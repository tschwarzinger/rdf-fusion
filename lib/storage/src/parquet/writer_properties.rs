use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::metadata::SortingColumn;
use datafusion::parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
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
pub const BLOOM_FILTER_NDV: usize = ROW_GROUP_ROW_COUNT / 4;
/// TODO: Make this configurable
#[allow(dead_code)]
pub const FILE_ROW_COUNT: usize = ROW_GROUP_ROW_COUNT * 32;

/// Options for writing RDF data into Parquet files.
#[derive(Debug)]
pub struct RdfFusionParquetWriterProperties {
    /// The encoding used for the quads.
    pub encoding: QuadStorageEncoding,
    /// The sort order of the quads.
    pub sort_order: Option<RdfSortOrderName>,
    /// The inner WriterPropertiesBuilder.
    builder: WriterPropertiesBuilder,
}

impl Clone for RdfFusionParquetWriterProperties {
    fn clone(&self) -> Self {
        Self {
            encoding: self.encoding.clone(),
            sort_order: self.sort_order.clone(),
            builder: self.builder.clone(),
        }
    }
}

impl RdfFusionParquetWriterProperties {
    /// Creates a new [`RdfFusionParquetWriterProperties`].
    pub fn new(encoding: QuadStorageEncoding) -> Self {
        let mut builder = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROW_GROUP_ROW_COUNT))
            .set_data_page_row_count_limit(PAGE_ROW_COUNT)
            .set_bloom_filter_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_statistics_truncate_length(Some(256)) // IRIs might be long
            .set_column_index_truncate_length(Some(256)); // IRIs might be long

        builder = if encoding.term_type().is_primitive() {
            builder
                .set_dictionary_enabled(false)
                .set_compression(Compression::UNCOMPRESSED) // Object IDs are small enough
        } else {
            builder.set_compression(Compression::ZSTD(ZstdLevel::default()))
        };

        Self {
            encoding,
            sort_order: None,
            builder,
        }
    }

    /// Sets the sort order that the writer can store in the Parquet file. This is only used for
    /// setting the metadata. This assumes that the sort options are ascending and nulls first.
    pub fn with_sort_order(mut self, sort_order: Option<RdfSortOrder>) -> Self {
        let name = sort_order.map(|s| s.name());

        let sorting_columns = name.as_ref().and_then(|order| match order {
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
        self.builder = self.builder.set_sorting_columns(sorting_columns);

        let all_columns = QuadComponent::all()
            .iter()
            .map(|c| ColumnPath::new(vec![c.column_name().to_owned()]))
            .collect::<Vec<_>>();
        let clustered_columns = name
            .as_ref()
            .and_then(|order: &RdfSortOrderName| {
                order.components().first().map(|first| {
                    // We only assume that the first columns of the sort order exhibit
                    // good-enough clustering.
                    vec![ColumnPath::new(vec![first.column_name().to_owned()])]
                })
            })
            .unwrap_or_else(|| all_columns.clone());

        for col_path in all_columns {
            let is_clustered = clustered_columns.contains(&col_path);

            self.builder = if !is_clustered {
                self.builder.set_column_bloom_filter_ndv(
                    col_path.clone(),
                    BLOOM_FILTER_NDV as u64,
                )
            } else {
                self.builder
                    .set_column_bloom_filter_enabled(col_path.clone(), false)
            };

            self.builder = self.builder.set_column_dictionary_enabled(
                col_path.clone(),
                !self.encoding.term_type().is_primitive(),
            );

            self.builder = if is_clustered {
                if self.encoding.term_type().is_primitive() {
                    self.builder
                        .set_column_encoding(col_path.clone(), Encoding::RLE)
                } else {
                    self.builder // Avoid overriding default
                }
            } else {
                if self.encoding.term_type().is_primitive() {
                    self.builder.set_column_encoding(
                        col_path.clone(),
                        Encoding::PLAIN, // Object IDs are small enough
                    )
                } else {
                    self.builder.set_column_encoding(
                        col_path.clone(),
                        Encoding::DELTA_LENGTH_BYTE_ARRAY, // Good for common prefixes
                    )
                }
            }
        }

        self.sort_order = name;
        self
    }

    /// Creates the Parquet writer properties.
    pub fn into_arrow(self) -> WriterProperties {
        self.builder.build()
    }

    /// Creates the Parquet writer properties by cloning the builder.
    pub fn to_arrow(&self) -> WriterProperties {
        self.builder.clone().build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdf_fusion_common::RdfSortOrder;

    #[test]
    fn test_override_and_reset_clustered_columns() {
        let encoding = QuadStorageEncoding::String;
        let path_subject = ColumnPath::new(vec!["subject".to_owned()]);
        let path_predicate = ColumnPath::new(vec!["predicate".to_owned()]);
        let path_object = ColumnPath::new(vec!["object".to_owned()]);
        let mut props = RdfFusionParquetWriterProperties::new(encoding);

        // 1. Set sort order: Subject (marked as clustered), Predicate, Object
        props = props.with_sort_order(Some(RdfSortOrder::NativeOrder(vec![
            QuadComponent::Subject,
            QuadComponent::Predicate,
            QuadComponent::Object,
        ])));

        let arrow_props = props.to_arrow();

        assert!(arrow_props.bloom_filter_properties(&path_subject).is_none());
        assert!(
            arrow_props
                .bloom_filter_properties(&path_predicate)
                .is_some()
        );
        assert!(arrow_props.bloom_filter_properties(&path_object).is_some());

        // 2. Change sort order: Predicate (marked as clustered), Object, Subject
        props = props.with_sort_order(Some(RdfSortOrder::NativeOrder(vec![
            QuadComponent::Predicate,
            QuadComponent::Object,
            QuadComponent::Subject,
        ])));

        let arrow_props2 = props.to_arrow();

        assert!(
            arrow_props2
                .bloom_filter_properties(&path_predicate)
                .is_none()
        );
        assert!(
            arrow_props2
                .bloom_filter_properties(&path_subject)
                .is_some()
        );
        assert!(arrow_props2.bloom_filter_properties(&path_object).is_some());
    }
}
