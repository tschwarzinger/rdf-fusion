use datafusion::arrow::datatypes::Schema;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::datasource::physical_plan::parquet::RowGroupAccess;
use datafusion::parquet::file::metadata::ParquetMetaData;

/// Estimates the scan statistics for a Parquet file based on the provided access plan.
/// It uses page-level `OffsetIndex` metadata if available to refine byte estimates.
pub fn estimate_scan_statistics(
    parquet_meta: &ParquetMetaData,
    access_plan: &ParquetAccessPlan,
    arrow_schema: &Schema,
) -> Statistics {
    let schema_descr = parquet_meta.file_metadata().schema_descr();
    let mut row_count = 0;
    let mut scanned_bytes: i64 = 0;
    let mut has_matching_row_group = false;

    let num_columns = schema_descr.num_columns();
    let mut column_bytes = vec![0i64; num_columns];
    let mut column_null_counts = vec![0i64; num_columns];
    let mut has_null_stats = false;

    let offset_index = parquet_meta.offset_index();

    for (i, rg) in parquet_meta.row_groups().iter().enumerate() {
        let access = &access_plan.inner()[i];
        if matches!(access, RowGroupAccess::Skip) {
            continue;
        }

        let rg_offset_index = offset_index.map(|oi| &oi[i]);
        let mut rg_scanned_bytes = 0i64;

        for (leaf_idx, col) in rg.columns().iter().enumerate() {
            let root = schema_descr.get_column_root_idx(leaf_idx);

            // Collect null statistics
            if let Some(stats) = col.statistics() {
                if let Some(null_count) = stats.null_count_opt() {
                    column_null_counts[root] += null_count as i64;
                    has_null_stats = true;
                }
            }

            // Calculate byte size based on access type and page index availability
            let col_bytes = match access {
                RowGroupAccess::Scan => col.compressed_size(),
                RowGroupAccess::Selection(selection) => {
                    if let Some(col_offsets) =
                        rg_offset_index.and_then(|oi| oi.get(leaf_idx))
                    {
                        // Map row selection to page index
                        let mut selected_bytes = 0i64;
                        let page_locations = col_offsets.page_locations();

                        // Convert RowSelection to absolute row intervals
                        let mut selected_ranges = Vec::new();
                        let mut current_row = 0;
                        for selector in selection.iter() {
                            if selector.skip {
                                current_row += selector.row_count as i64;
                            } else {
                                selected_ranges.push(
                                    current_row
                                        ..(current_row + selector.row_count as i64),
                                );
                                current_row += selector.row_count as i64;
                            }
                        }

                        // Check which pages overlap with the selected row ranges
                        for (page_idx, page) in page_locations.iter().enumerate() {
                            let page_start = page.first_row_index;
                            let page_end = if page_idx + 1 < page_locations.len() {
                                page_locations[page_idx + 1].first_row_index
                            } else {
                                rg.num_rows()
                            };

                            let overlaps = selected_ranges.iter().any(|range| {
                                range.start < page_end && range.end > page_start
                            });

                            if overlaps {
                                selected_bytes += page.compressed_page_size as i64;
                            }
                        }

                        // If any data pages are selected, the parquet reader must also fetch
                        // the dictionary page (if present) for this column chunk.
                        if selected_bytes > 0 {
                            if let Some(dict_offset) = col.dictionary_page_offset() {
                                if let Some(first_page) = page_locations.first() {
                                    if first_page.offset > dict_offset {
                                        selected_bytes += first_page.offset - dict_offset;
                                    }
                                }
                            }
                        }

                        selected_bytes
                    } else {
                        col.compressed_size()
                    }
                }
                RowGroupAccess::Skip => 0,
            };

            column_bytes[root] += col_bytes;
            rg_scanned_bytes += col_bytes;
        }

        scanned_bytes += rg_scanned_bytes;

        match access {
            RowGroupAccess::Scan => {
                if rg.num_rows() > 0 {
                    row_count += rg.num_rows();
                    has_matching_row_group = true;
                }
            }
            RowGroupAccess::Selection(selection) => {
                let count = selection.row_count();
                if count > 0 {
                    row_count += count as i64;
                    has_matching_row_group = true;
                }
            }
            RowGroupAccess::Skip => {}
        }
    }

    let num_bytes = if has_matching_row_group {
        Precision::Inexact(scanned_bytes as usize)
    } else {
        Precision::Exact(0)
    };
    let num_rows = if has_matching_row_group {
        Precision::Inexact(row_count as usize)
    } else {
        Precision::Exact(0)
    };

    let mut statistics = Statistics::default()
        .with_total_byte_size(num_bytes)
        .with_num_rows(num_rows);
    for (i, _field) in arrow_schema.fields().iter().enumerate() {
        statistics = statistics.add_column_statistics(ColumnStatistics {
            null_count: if has_null_stats {
                Precision::Exact(column_null_counts.get(i).copied().unwrap_or(0) as usize)
            } else {
                Precision::Absent
            },
            distinct_count: Precision::Absent,
            min_value: Precision::Absent,
            max_value: Precision::Absent,
            sum_value: Precision::Absent,
            byte_size: Precision::Inexact(
                column_bytes.get(i).copied().unwrap_or(0) as usize
            ),
        });
    }

    statistics
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::arrow::arrow_reader::{
        ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
    };
    use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::sync::Arc;

    fn create_test_file(enable_page_index: bool) -> (ParquetMetaData, Arc<Schema>) {
        let schema =
            Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
        // 100 rows to span across multiple pages if we set a tiny page size
        let array = Int64Array::from_iter_values(0..100);
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap();

        let mut buf = Vec::new();
        let mut props_builder = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(32)
            .set_data_page_row_count_limit(10) // Force 10 rows per page -> 10 pages
            .set_statistics_enabled(EnabledStatistics::Page);

        if enable_page_index {
            props_builder = props_builder.set_column_index_truncate_length(Some(64));
        }

        let props = props_builder.build();
        let mut writer =
            ArrowWriter::try_new(&mut buf, Arc::clone(&schema), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Load metadata with the page (offset) index so the estimator can use it. A plain
        // `SerializedFileReader` does not parse the offset index, which lives outside the footer.
        let options =
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
            bytes::Bytes::from(buf),
            options,
        )
        .unwrap();
        (reader.metadata().as_ref().clone(), schema)
    }

    #[test]
    fn test_estimation_with_page_index() {
        let (metadata, schema) = create_test_file(true);
        let num_row_groups = metadata.num_row_groups();

        // Select only rows 25-35. This crosses page 2 (rows 20-29) and page 3 (rows 30-39).
        let selection = RowSelection::from(vec![
            RowSelector::skip(25),
            RowSelector::select(10),
            RowSelector::skip(65),
        ]);

        let mut accesses = vec![RowGroupAccess::Scan; num_row_groups];
        accesses[0] = RowGroupAccess::Selection(selection);
        let access_plan = ParquetAccessPlan::new(accesses);

        let stats = estimate_scan_statistics(&metadata, &access_plan, &schema);

        // Total row count should be exactly 10, marked as Inexact because it's a partial selection
        assert_eq!(stats.num_rows, Precision::Inexact(10));

        let total_col_bytes = metadata.row_group(0).column(0).compressed_size();
        let estimated_bytes = match stats.total_byte_size {
            Precision::Inexact(b) => b as i64,
            _ => panic!("Expected inexact byte precision"),
        };

        // Because it only selected a subset of pages, the bytes should be significantly less than the whole column
        assert!(estimated_bytes > 0);
        assert!(
            estimated_bytes < total_col_bytes,
            "Expected page pruning to reduce scanned byte estimation"
        );
    }

    #[test]
    fn test_estimation_without_page_index() {
        let (metadata, schema) = create_test_file(false);
        // Wipe offset index to simulate missing page metadata
        let mut metadata_builder = metadata.into_builder();
        metadata_builder = metadata_builder.set_offset_index(None);
        let metadata_no_pages = metadata_builder.build();

        let num_row_groups = metadata_no_pages.num_row_groups();

        let selection = RowSelection::from(vec![
            RowSelector::skip(25),
            RowSelector::select(10),
            RowSelector::skip(65),
        ]);

        let mut accesses = vec![RowGroupAccess::Scan; num_row_groups];
        accesses[0] = RowGroupAccess::Selection(selection);
        let access_plan = ParquetAccessPlan::new(accesses);

        let stats = estimate_scan_statistics(&metadata_no_pages, &access_plan, &schema);

        let total_col_bytes = metadata_no_pages.row_group(0).column(0).compressed_size();
        let estimated_bytes = match stats.total_byte_size {
            Precision::Inexact(b) => b as i64,
            _ => panic!("Expected inexact byte precision"),
        };

        // With no page index, it must fall back to the entire column chunk size
        assert_eq!(estimated_bytes, total_col_bytes);
    }
}
