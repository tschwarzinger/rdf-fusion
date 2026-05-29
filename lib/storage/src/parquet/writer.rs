use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::metadata::SortingColumn;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::schema::types::ColumnPath;
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::EnabledStatistics;
use rdf_fusion_common::{QuadComponent, RdfSortOrder, RdfSortOrderName};
use rdf_fusion_encoding::QuadStorageEncoding;

/// TODO: Make this configurable
pub const PAGE_ROW_COUNT: usize = 1_024;
/// TODO: Make this configurable
pub const ROW_GROUP_ROW_COUNT: usize = PAGE_ROW_COUNT * 32;
/// TODO: Make this configurable
pub const FILE_ROW_COUNT: usize = ROW_GROUP_ROW_COUNT * 32;

/// Options for writing RDF data into Parquet files.
pub struct RdfFusionParquetWriterProperties {
    /// The encoding used for the quads.
    pub encoding: QuadStorageEncoding,
    /// The sort order of the quads.
    pub sort_order: Option<RdfSortOrderName>,
}

impl RdfFusionParquetWriterProperties {
    /// Creates a new [`RdfFusionParquetWriterProperties`].
    pub fn new(encoding: QuadStorageEncoding) -> Self {
        Self {
            encoding,
            sort_order: None,
        }
    }

    /// Sets the sort order that the writer can store in the Parquet file. This is only used for
    /// setting the metadata. This assumes that the sort options are ascending and nulls first.
    pub fn with_sort_order(mut self, sort_order: Option<RdfSortOrder>) -> Self {
        self.sort_order = sort_order.map(|s| s.name());
        self
    }

    /// Creates the Parquet writer properties.
    pub fn to_arrow(&self) -> WriterProperties {
        let sorting_columns = self.sort_order.as_ref().and_then(|order| match order {
            RdfSortOrderName::NativeOrder(order) => {
                if order.is_empty() {
                    None
                } else {
                    Some(
                        order
                            .iter()
                            .map(|c: &QuadComponent| SortingColumn {
                                column_idx: c.gspo_index() as i32,
                                descending: false,
                                nulls_first: true,
                            })
                            .collect::<Vec<_>>(),
                    )
                }
            }
            _ => None,
        });

        let non_clustered_columns = self.sort_order.as_ref().and_then(|order: &RdfSortOrderName| {
            if let Some(first) = order.components().first() {
                // We only assume that the first columns of the sort order exhibit
                // good-enough clustering.
                let column_paths = QuadComponent::all()
                    .iter()
                    .filter(|c| *c != first)
                    .map(|c| ColumnPath::new(vec![c.column_name().to_owned()]))
                    .collect::<Vec<_>>();
                Some(column_paths)
            } else {
                None
            }
        });

        let mut builder = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROW_GROUP_ROW_COUNT))
            .set_data_page_row_count_limit(PAGE_ROW_COUNT)
            .set_bloom_filter_enabled(false)
            .set_bloom_filter_ndv(ROW_GROUP_ROW_COUNT as u64)
            .set_sorting_columns(sorting_columns)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_statistics_truncate_length(Some(256)) // IRIs might be long
            .set_column_index_truncate_length(Some(256)); // IRIs might be long

        builder = if self.encoding.term_type().is_primitive() {
            builder
                .set_dictionary_enabled(false)
                .set_compression(Compression::UNCOMPRESSED) // Object IDs are small enough
        } else {
            builder.set_compression(Compression::ZSTD(ZstdLevel::default()))
        };

        if let Some(non_clustered_columns) = non_clustered_columns {
            for non_clustered_column in non_clustered_columns {
                builder = builder
                    .set_column_bloom_filter_enabled(non_clustered_column.clone(), true);
                builder = if self.encoding.term_type().is_primitive() {
                    builder.set_column_encoding(non_clustered_column.clone(), Encoding::PLAIN)
                } else {
                    builder
                        .set_column_dictionary_enabled(non_clustered_column.clone(), false)
                        .set_column_encoding(
                            non_clustered_column,
                            Encoding::DELTA_LENGTH_BYTE_ARRAY, // Good for common prefixes
                        )
                };
            }
        }

        builder.build()
    }
}
