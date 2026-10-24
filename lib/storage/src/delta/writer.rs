use crate::delta::error::DeltaQuadsStorageError;
use crate::parquet::RdfParquetWriterOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::config::TableParquetOptions;
use deltalake::DeltaTable;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::table::config::TablePropertiesExt;

/// Creates a [`DeltaWriter`] for appending data with the given `schema` to `table`.
///
/// The Parquet writer properties are derived from the session's `parquet_options` (see
/// [`RdfParquetWriterOptions`]). The writer starts a new file once the table's target file size
/// is reached, while preserving the order of the written batches.
pub(crate) fn create_delta_writer(
    table: &DeltaTable,
    schema: SchemaRef,
    parquet_options: &TableParquetOptions,
    sort_columns: &[&str],
) -> Result<DeltaWriter, DeltaQuadsStorageError> {
    let writer_properties =
        RdfParquetWriterOptions::try_new(parquet_options, &schema, sort_columns)?
            .to_writer_properties()?;

    let table_state = table.snapshot()?;
    let table_properties = table_state.table_config();
    let config = WriterConfig::new(
        schema,
        table_state.metadata().partition_columns().to_vec(),
        Some(writer_properties),
        Some(table_properties.target_file_size()),
        None,
        table_properties.num_indexed_cols(),
        table_properties
            .data_skipping_stats_columns
            .as_ref()
            .map(|c| c.iter().map(|c| c.to_string()).collect()),
    );
    Ok(DeltaWriter::new(table.object_store(), config))
}
