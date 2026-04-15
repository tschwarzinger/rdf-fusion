use crate::delta::error::DeltaQuadStorageError;
use crate::delta::log::{COL_OPERATION, DeltaStorageLogOperation};
use datafusion::arrow::array::{RecordBatch, new_null_array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{ScalarValue, SchemaExt};
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionState;
use datafusion::prelude::{Expr, SessionContext, lit};
use deltalake::DeltaTable;
use deltalake::kernel::Action;
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use futures::StreamExt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::RwLock;

/// A transaction on a [`DeltaStorageLog`].
pub struct DeltaStorageLogTransaction {
    /// The session context.
    session_context: SessionContext,
    /// The target table of the transaction.
    table: Arc<RwLock<DeltaTable>>,
    /// The schema of the table.
    table_schema: SchemaRef,
    /// The individual parts of the transaction. When the transaction is executed, all parts are
    /// evaluated and their results are written to disk. Then, the resulting files are appended to
    /// the log table.
    parts: Vec<DataFrame>,
}

impl DeltaStorageLogTransaction {
    /// Creates a new [`DeltaStorageLogTransaction`].
    pub fn new(
        state: SessionState,
        table: Arc<RwLock<DeltaTable>>,
        table_schema: SchemaRef,
    ) -> Self {
        Self {
            session_context: SessionContext::new_with_state(state),
            table,
            table_schema,
            parts: vec![],
        }
    }

    /// Append a stream of quads to the log.
    ///
    /// This operation is lazy and executed during [`Self::execute`].
    pub fn append_quads(self, quads: DataFrame) -> Result<Self, DeltaQuadStorageError> {
        self.append_quads_with_operation(quads, DeltaStorageLogOperation::AddQuad)
    }

    /// Append the removal of a stream of quads to the log.
    ///
    /// This operation is lazy and executed during [`Self::execute`].
    pub fn remove_quads(self, quads: DataFrame) -> Result<Self, DeltaQuadStorageError> {
        self.append_quads_with_operation(quads, DeltaStorageLogOperation::RemoveQuad)
    }

    /// Implements the appending operation. This is used to implement both `append_quads` and
    /// `remove_quads`.
    ///
    /// This adds the [`COL_OPERATION`] based on the given operation and inserts it into the
    /// underlying delta table.
    ///
    /// This operation is lazy and executed during [`Self::execute`].
    fn append_quads_with_operation(
        mut self,
        quads: DataFrame,
        operation: DeltaStorageLogOperation,
    ) -> Result<Self, DeltaQuadStorageError> {
        validate_data_frame_schema(&self.table_schema, quads.schema().inner())?;

        let quads_with_operation = add_operation_to_quads(quads, operation);
        self.parts.push(quads_with_operation);

        return Ok(self);

        /// Validates that the stream schema matches the expected schema (which is based on the
        /// used encoding);
        fn validate_data_frame_schema(
            output_schema: &SchemaRef,
            actual: &SchemaRef,
        ) -> Result<(), DeltaQuadStorageError> {
            let expected_stream_schema = output_schema
                .project(&[1, 2, 3, 4])
                .expect("Valid projection");

            // Don't use equality because the expected_stream_schema is nullable
            if !expected_stream_schema.equivalent_names_and_types(actual.as_ref()) {
                return Err(DeltaQuadStorageError::InvalidSchema(Arc::clone(actual)));
            }

            Ok(())
        }

        /// Adds the [`COL_OPERATION`] for each record batch that is being streamed.
        fn add_operation_to_quads(
            quads: DataFrame,
            operation: DeltaStorageLogOperation,
        ) -> DataFrame {
            let mut exprs = Vec::new();
            exprs.push(lit(operation.as_stored()).alias(COL_OPERATION));
            exprs.extend(quads.schema().columns().into_iter().map(Expr::from));

            quads.select(exprs).expect("Valid projection")
        }
    }

    /// Append a graph-level operation to the log.
    pub fn append_graph_operation(
        mut self,
        graph: ScalarValue,
        operation: DeltaStorageLogOperation,
    ) -> Result<Self, DeltaQuadStorageError> {
        let encoding_type = graph.data_type();

        let batch = RecordBatch::try_new(
            Arc::clone(&self.table_schema),
            vec![
                ScalarValue::Int8(Some(operation.as_stored()))
                    .to_array_of_size(1)
                    .expect("Valid scalar value"),
                graph
                    .to_array_of_size(1)
                    .expect("Valid array representation"),
                new_null_array(&encoding_type, 1),
                new_null_array(&encoding_type, 1),
                new_null_array(&encoding_type, 1),
            ],
        )
        .expect("Valid batch");
        let data_frame = self.session_context.read_batch(batch)?;

        self.parts.push(data_frame);
        Ok(self)
    }

    /// Append a general operation (like Drop) to the log.
    pub fn append_general_operation(
        mut self,
        operation: DeltaStorageLogOperation,
    ) -> Result<Self, DeltaQuadStorageError> {
        let encoding_type = self.table_schema.field(1).data_type();

        let batch = RecordBatch::try_new(
            Arc::clone(&self.table_schema),
            vec![
                ScalarValue::Int8(Some(operation.as_stored()))
                    .to_array_of_size(1)
                    .expect("Valid array representation"),
                new_null_array(encoding_type, 1),
                new_null_array(encoding_type, 1),
                new_null_array(encoding_type, 1),
                new_null_array(encoding_type, 1),
            ],
        )
        .expect("Valid batch");
        let data_frame = self.session_context.read_batch(batch)?;

        self.parts.push(data_frame);
        Ok(self)
    }

    /// Executes the transaction, writing the commits to the storage backend and changing the table
    /// state.
    pub async fn execute(self) -> Result<(), DeltaQuadStorageError> {
        if self.parts.is_empty() {
            return Ok(());
        }

        let mut writer = self.create_record_batch_writer().await?;
        for part in self.parts {
            let mut batch_stream = part.execute_stream().await?;
            while let Some(batch) = batch_stream.next().await {
                // Project columns into the target schema (make subject etc. nullable)
                let batch = RecordBatch::try_new(
                    Arc::clone(&self.table_schema),
                    batch?.columns().to_vec(),
                )
                .expect("Failed to align schema nullability");

                writer.write(batch).await?;
            }
        }

        let add_actions = writer.flush().await?;
        let mut table = self.table.write().await;
        let table_state = table.state.as_ref().expect("Table loaded");
        let result = CommitBuilder::default()
            .with_actions(add_actions.into_iter().map(Action::Add).collect())
            .build(
                Some(table_state),
                table.log_store(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await?;

        table.state = Some(result.snapshot);

        Ok(())
    }

    /// Returns a new writer for the log table.
    ///
    /// Immediately drops the lock on [`Self::table`] after creating the writer.
    async fn create_record_batch_writer(
        &self,
    ) -> Result<RecordBatchWriter, DeltaQuadStorageError> {
        let table = self.table.read().await;
        let writer = RecordBatchWriter::for_table(&table)?;
        Ok(writer)
    }
}

impl Debug for DeltaStorageLogTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaStorageLogTransaction")
            .field("table", &self.table)
            .field("parts", &self.parts)
            .finish()
    }
}
