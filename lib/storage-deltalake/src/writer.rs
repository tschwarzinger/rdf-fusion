use crate::error::DeltaQuadsStorageError;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::config::TableParquetOptions;
use deltalake::kernel::Add;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::table::config::TablePropertiesExt;
use deltalake::{DeltaResult, DeltaTable};
use rdf_fusion_encoding::without_rdf_term_extension_types;
use rdf_fusion_storage_parquet::RdfParquetWriterOptions;
use std::sync::Arc;

/// Creates a [`RdfDeltaWriter`] for appending data with the given `schema` to `table`.
///
/// The RDF term extension types are not persisted. Hence, they are removed from `schema` and from
/// the written batches (see [`RdfDeltaWriter`]).
///
/// The Parquet writer properties are derived from the session's `parquet_options` (see
/// [`RdfParquetWriterOptions`]). The writer starts a new file once the table's target file size
/// is reached, while preserving the order of the written batches.
pub(crate) fn create_delta_writer(
    table: &DeltaTable,
    schema: SchemaRef,
    parquet_options: &TableParquetOptions,
    sort_columns: &[&str],
) -> Result<RdfDeltaWriter, DeltaQuadsStorageError> {
    let schema: SchemaRef = Arc::new(without_rdf_term_extension_types(&schema));
    let writer_properties =
        RdfParquetWriterOptions::try_new(parquet_options, &schema, sort_columns)?
            .to_writer_properties()?;

    let table_state = table.snapshot()?;
    let table_properties = table_state.table_config();
    let config = WriterConfig::new(
        Arc::clone(&schema),
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
    Ok(RdfDeltaWriter {
        inner: DeltaWriter::new(table.object_store(), config),
        schema,
    })
}

/// Wraps a [`DeltaWriter`] that writes batches without the RDF term extension types.
///
/// The [`DeltaWriter`] requires that the schema of the written batches is equal to the schema of
/// the files. As the batches produced by the query engine carry the extension types, their schema
/// is replaced (zero-copy) before writing.
pub(crate) struct RdfDeltaWriter {
    inner: DeltaWriter,
    schema: SchemaRef,
}

impl RdfDeltaWriter {
    /// Writes the given `batch`.
    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            batch.columns().to_vec(),
            &options,
        )?;
        self.inner.write(&batch).await
    }

    /// Closes the writer and returns the add actions of the written files.
    pub(crate) async fn close(self) -> DeltaResult<Vec<Add>> {
        self.inner.close().await
    }
}
