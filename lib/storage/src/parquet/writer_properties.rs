use datafusion::arrow::datatypes::Schema;
use datafusion::common::config::TableParquetOptions;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::parquet::arrow::ArrowSchemaConverter;
use datafusion::parquet::basic::Type as PhysicalType;
use datafusion::parquet::file::metadata::SortingColumn;
use datafusion::parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use datafusion::parquet::schema::types::SchemaDescriptor;
use rdf_fusion_common::RdfSortOrder;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};

/// The columns that store RDF terms.
const TERM_COLUMNS: [&str; 4] = [COL_GRAPH, COL_SUBJECT, COL_PREDICATE, COL_OBJECT];

/// The writer configuration for a single Parquet write operation.
///
/// The configuration is derived from DataFusion's [`TableParquetOptions`]. On top of these, the
/// following RDF-specific overrides are applied (unless the user explicitly configured the
/// respective column or a global encoding):
/// - String columns use [`DELTA_BYTE_ARRAY`](datafusion::parquet::basic::Encoding::DELTA_BYTE_ARRAY)
///   as encoding. This encoding shares common prefixes between consecutive values (e.g., IRIs
///   with the same namespace). If dictionary encoding is enabled, this is the fallback encoding.
/// - Term columns that store object ids are written without compression. They are dictionary
///   encoded (RLE_DICTIONARY), which compresses runs of the same id in sorted data cheaply.
/// - String columns from the third sort column on are written without dictionary. Within the
///   runs of the first two sort columns, their values are mostly distinct (e.g., the subjects in
///   GPOS order), such that the dictionary only adds overhead before falling back.
/// - Bloom filters are disabled for the leading sort column, as the page index already allows
///   pruning on this column.
#[derive(Debug, Clone)]
pub struct RdfParquetWriterOptions {
    /// The DataFusion options, including the RDF-specific column options.
    options: TableParquetOptions,
    /// The sorting columns that are stored in the metadata of the Parquet files.
    sorting_columns: Option<Vec<SortingColumn>>,
}

impl RdfParquetWriterOptions {
    /// Creates a new [`RdfParquetWriterOptions`] for writing data with the given `schema`.
    ///
    /// - `base` are the Parquet options of the current session.
    /// - `sort_columns` are the names of the (top-level) columns the data is sorted by in
    ///   ascending order, with nulls first. Pass an empty slice if the data is not sorted.
    pub fn try_new(
        base: &TableParquetOptions,
        schema: &Schema,
        sort_columns: &[&str],
    ) -> DFResult<Self> {
        Self::try_new_impl(base, schema, sort_columns, sort_columns)
    }

    /// Creates a new [`RdfParquetWriterOptions`] for writing quads in the given `sort_order`.
    ///
    /// Only the native order corresponds to the order of the values in the Parquet file. Hence,
    /// other sort orders are not stored in the Parquet metadata. However, the data is still
    /// clustered by their columns.
    pub fn try_new_for_sort_order(
        base: &TableParquetOptions,
        schema: &Schema,
        sort_order: Option<&RdfSortOrder>,
    ) -> DFResult<Self> {
        let columns: Vec<_> = sort_order
            .map(|o| o.components().iter().map(|c| c.column_name()).collect())
            .unwrap_or_default();
        let is_native = matches!(sort_order, Some(RdfSortOrder::NativeOrder(_)));
        let sort_columns = if is_native { columns.as_slice() } else { &[] };
        Self::try_new_impl(base, schema, &columns, sort_columns)
    }

    /// - `clustering_columns` are the columns the data is clustered by, in order.
    /// - `sort_columns` are the columns that are stored as sorting columns in the metadata.
    fn try_new_impl(
        base: &TableParquetOptions,
        schema: &Schema,
        clustering_columns: &[&str],
        sort_columns: &[&str],
    ) -> DFResult<Self> {
        let mut options = base.clone();

        if options.global.created_by == TableParquetOptions::default().global.created_by {
            options.global.created_by =
                format!("rdf-fusion {}", env!("CARGO_PKG_VERSION"));
        }

        // parquet-rs cannot disable bloom filters for a single column if they are enabled
        // globally. Furthermore, setting the NDV or FPP implicitly enables bloom filters, even if
        // `bloom_filter_on_write` is disabled. Hence, we configure bloom filters per column.
        let bloom_filter_on_write = options.global.bloom_filter_on_write;
        let bloom_filter_ndv = options.global.bloom_filter_ndv.take();
        let bloom_filter_fpp = options.global.bloom_filter_fpp.take();
        options.global.bloom_filter_on_write = false;

        let parquet_schema = ArrowSchemaConverter::new()
            .convert(schema)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let global_encoding_set = base.global.encoding.is_some();

        for column in parquet_schema.columns() {
            let parts = column.path().parts();
            let top_level = parts[0].as_str();
            // Leaves of nested columns (e.g., struct-based term encodings) are not term ids.
            let is_term_id_column = parts.len() == 1 && TERM_COLUMNS.contains(&top_level);
            let clustering_position =
                clustering_columns.iter().position(|c| *c == top_level);
            let is_leading_sort_column = clustering_position == Some(0);
            let is_likely_not_clustered = clustering_position.is_some_and(|p| p >= 2);

            let column_options = options
                .column_specific_options
                .entry(column.path().string())
                .or_default();

            match column.physical_type() {
                PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
                    if !global_encoding_set =>
                {
                    column_options
                        .encoding
                        .get_or_insert_with(|| "delta_byte_array".to_owned());
                    if is_likely_not_clustered {
                        column_options.dictionary_enabled.get_or_insert(false);
                    }
                }
                PhysicalType::INT32 | PhysicalType::INT64 if is_term_id_column => {
                    column_options
                        .compression
                        .get_or_insert_with(|| "uncompressed".to_owned());
                }
                _ => {}
            }

            // The page index already allows pruning on the leading sort column.
            let bloom_filter_enabled = *column_options
                .bloom_filter_enabled
                .get_or_insert(bloom_filter_on_write && !is_leading_sort_column);
            if bloom_filter_enabled {
                column_options.bloom_filter_ndv =
                    column_options.bloom_filter_ndv.or(bloom_filter_ndv);
                column_options.bloom_filter_fpp =
                    column_options.bloom_filter_fpp.or(bloom_filter_fpp);
            }
        }

        let sorting_columns = create_sorting_columns(&parquet_schema, sort_columns);
        Ok(Self {
            options,
            sorting_columns,
        })
    }

    /// Returns the options for DataFusion's Parquet writer (e.g., `ParquetSink`).
    pub fn table_parquet_options(&self) -> &TableParquetOptions {
        &self.options
    }

    /// Returns the sorting columns that should be stored in the Parquet metadata.
    pub fn sorting_columns(&self) -> Option<Vec<SortingColumn>> {
        self.sorting_columns.clone()
    }

    /// Creates the [`WriterProperties`] for writers that directly use parquet-rs (e.g., the
    /// Delta Lake writers).
    pub fn to_writer_properties(&self) -> DFResult<WriterProperties> {
        let mut options = self.options.clone();
        // Writers that operate on WriterProperties add the Arrow schema themselves.
        options.global.skip_arrow_metadata = true;
        let builder = WriterPropertiesBuilder::try_from(&options)?
            .set_sorting_columns(self.sorting_columns.clone());
        Ok(builder.build())
    }
}

/// Creates the sorting columns for the Parquet metadata.
///
/// Parquet sorting columns refer to leaf columns. Hence, sort columns that are not a top-level
/// leaf column (e.g., struct-based term encodings) cannot be represented. In this case, only the
/// prefix of the sort order that can be represented is returned.
fn create_sorting_columns(
    parquet_schema: &SchemaDescriptor,
    sort_columns: &[&str],
) -> Option<Vec<SortingColumn>> {
    let result = sort_columns
        .iter()
        .map_while(|sort_column| {
            let column_idx = parquet_schema.columns().iter().position(
                |c| matches!(c.path().parts(), [name] if name == sort_column),
            )?;
            Some(SortingColumn {
                column_idx: column_idx as i32,
                descending: false,
                nulls_first: true,
            })
        })
        .collect::<Vec<_>>();
    (!result.is_empty()).then_some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::config::ParquetColumnOptions;
    use datafusion::parquet::basic::{Compression, Encoding};
    use rdf_fusion_common::QuadComponent;
    use rdf_fusion_encoding::QuadStorageEncoding;

    fn string_schema() -> Schema {
        QuadStorageEncoding::String
            .quad_schema()
            .inner()
            .as_ref()
            .clone()
    }

    fn bloom_enabled_options() -> TableParquetOptions {
        let mut options = TableParquetOptions::default();
        options.global.bloom_filter_on_write = true;
        options
    }

    /// Creates the options and the resulting [`WriterProperties`].
    fn create(
        base: &TableParquetOptions,
        schema: &Schema,
        sort_columns: &[&str],
    ) -> (RdfParquetWriterOptions, WriterProperties) {
        let options =
            RdfParquetWriterOptions::try_new(base, schema, sort_columns).unwrap();
        let props = options.to_writer_properties().unwrap();
        (options, props)
    }

    /// Creates the [`WriterProperties`] for writing string quads in the given `sort_order`.
    fn create_for_sort_order(
        base: &TableParquetOptions,
        sort_order: RdfSortOrder,
    ) -> (RdfParquetWriterOptions, WriterProperties) {
        let options = RdfParquetWriterOptions::try_new_for_sort_order(
            base,
            &string_schema(),
            Some(&sort_order),
        )
        .unwrap();
        let props = options.to_writer_properties().unwrap();
        (options, props)
    }

    fn has_bloom_filter(props: &WriterProperties, column: &str) -> bool {
        props.bloom_filter_properties(&column.into()).is_some()
    }

    #[test]
    fn string_columns_use_delta_byte_array() {
        let (_, props) = create(&TableParquetOptions::default(), &string_schema(), &[]);

        for column in TERM_COLUMNS {
            let path = column.into();
            assert_eq!(props.encoding(&path), Some(Encoding::DELTA_BYTE_ARRAY));
            // Dictionary encoding stays enabled (DataFusion default).
            assert!(props.dictionary_enabled(&path));
        }
    }

    #[test]
    fn plain_term_overrides_apply_to_leaf_columns() {
        let schema = QuadStorageEncoding::PlainTerm.quad_schema();
        let (options, props) =
            create(&bloom_enabled_options(), schema.inner(), &[COL_SUBJECT]);

        let parquet_schema = ArrowSchemaConverter::new().convert(schema.inner()).unwrap();
        let string_leaves = parquet_schema
            .columns()
            .iter()
            .filter(|c| c.physical_type() == PhysicalType::BYTE_ARRAY);
        let mut num_string_leaves = 0;
        for column in string_leaves {
            num_string_leaves += 1;
            assert_eq!(
                props.encoding(column.path()),
                Some(Encoding::DELTA_BYTE_ARRAY)
            );
        }
        assert!(num_string_leaves > 0);

        for column in parquet_schema.columns() {
            let is_subject = column.path().parts()[0] == COL_SUBJECT;
            assert_eq!(
                props.bloom_filter_properties(column.path()).is_some(),
                !is_subject
            );
        }

        // Sorting columns cannot be represented for struct-based columns.
        assert!(options.sorting_columns().is_none());
    }

    #[test]
    fn object_id_columns_use_dictionary_without_compression() {
        let schema = Schema::new(
            TERM_COLUMNS
                .map(|c| Field::new(c, DataType::Int64, true))
                .to_vec(),
        );
        let (options, props) = create(
            &bloom_enabled_options(),
            &schema,
            &[COL_PREDICATE, COL_OBJECT],
        );

        for column in TERM_COLUMNS {
            let path = column.into();
            assert!(props.dictionary_enabled(&path));
            assert_eq!(props.compression(&path), Compression::UNCOMPRESSED);
            assert_eq!(has_bloom_filter(&props, column), column != COL_PREDICATE);
        }

        let column_indices = options
            .sorting_columns()
            .unwrap()
            .iter()
            .map(|c| c.column_idx)
            .collect::<Vec<_>>();
        assert_eq!(column_indices, [2, 3]);
        assert_eq!(props.sorting_columns(), options.sorting_columns().as_ref());
    }

    #[test]
    fn sparql_order_only_clusters_leading_column() {
        let (options, props) = create_for_sort_order(
            &bloom_enabled_options(),
            RdfSortOrder::SparqlOrder(vec![
                QuadComponent::Predicate,
                QuadComponent::Subject,
            ]),
        );

        for column in TERM_COLUMNS {
            assert_eq!(has_bloom_filter(&props, column), column != COL_PREDICATE);
        }
        assert!(options.sorting_columns().is_none());
    }

    #[test]
    fn trailing_sort_columns_disable_dictionary() {
        let (_, props) = create_for_sort_order(
            &TableParquetOptions::default(),
            RdfSortOrder::NativeOrder(vec![
                QuadComponent::GraphName,
                QuadComponent::Predicate,
                QuadComponent::Object,
                QuadComponent::Subject,
            ]),
        );

        assert!(props.dictionary_enabled(&COL_GRAPH.into()));
        assert!(props.dictionary_enabled(&COL_PREDICATE.into()));
        assert!(!props.dictionary_enabled(&COL_OBJECT.into()));
        assert!(!props.dictionary_enabled(&COL_SUBJECT.into()));
    }

    #[test]
    fn session_options_are_respected() {
        let mut base = TableParquetOptions::default();
        base.global.max_row_group_size = 1234;
        base.global.statistics_truncate_length = Some(17);
        base.global.encoding = Some("plain".to_owned());
        base.column_specific_options.insert(
            COL_OBJECT.to_owned(),
            ParquetColumnOptions {
                encoding: Some("delta_length_byte_array".to_owned()),
                ..Default::default()
            },
        );

        let (_, props) = create(&base, &string_schema(), &[]);

        assert_eq!(props.max_row_group_row_count(), Some(1234));
        assert_eq!(props.statistics_truncate_length(), Some(17));
        assert_eq!(props.encoding(&COL_SUBJECT.into()), Some(Encoding::PLAIN));
        assert_eq!(
            props.encoding(&COL_OBJECT.into()),
            Some(Encoding::DELTA_LENGTH_BYTE_ARRAY)
        );
    }

    #[test]
    fn bloom_filter_ndv_does_not_enable_bloom_filters() {
        let mut base = TableParquetOptions::default();
        base.global.bloom_filter_on_write = false;
        base.global.bloom_filter_ndv = Some(1000);

        let (_, props) = create(&base, &string_schema(), &[]);

        for column in TERM_COLUMNS {
            assert!(!has_bloom_filter(&props, column));
        }
    }
}
