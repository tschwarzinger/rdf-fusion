use crate::delta::error::DeltaQuadStorageError;
use crate::delta::log::{
    COL_COMMIT_VERSION, COL_OPERATION, COL_OPERATION_SEQ_ID, DeltaStorageLogOperation,
    DeltaStorageLogVersionRange,
};
use crate::delta::snapshot::DeltaQuadStorageSnapshot;
use crate::delta::storage::DeltaQuadStorage;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{ScalarValue, SchemaExt};
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{ExprSchemable, Extension, LogicalPlan, col};
use datafusion::physical_plan::collect;
use datafusion::prelude::{Expr, SessionContext, lit};
use deltalake::DeltaTable;
use deltalake::kernel::Action;
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use futures::StreamExt;
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageGraphTarget, QuadStorageSnapshot, QuadStorageTransaction,
};
use rdf_fusion_logical::encoding::change::ChangeEncodingNode;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::{NamedOrBlankNodeRef, StorageError};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;
use tracing::info;

/// A transaction on a [`DeltaStorageLog`].
pub struct DeltaQuadStorageTransaction {
    /// The storage
    storage: Arc<DeltaQuadStorage>,
    /// The session context.
    state: SessionState,
    /// The target table of the transaction.
    table: Arc<RwLock<DeltaTable>>,
    /// The schema of the table.
    table_schema: SchemaRef,
    /// The base snapshot of the transaction.
    base_snapshot: Arc<DeltaQuadStorageSnapshot>,
    /// The individual parts of the transaction. When the transaction is executed, all parts are
    /// evaluated and their results are written to disk. Then, the resulting files are appended to
    /// the log table.
    parts: RwLock<Vec<DataFrame>>,
    /// Indicates whether the result of the transaction can depend on the database state. This flag
    /// is set to true if a snapshot from the transaction is obtained.
    may_depend_on_database_state: AtomicBool,
}

impl DeltaQuadStorageTransaction {
    /// Creates a new [`DeltaQuadStorageTransaction`].
    pub fn new(
        storage: Arc<DeltaQuadStorage>,
        state: SessionState,
        table: Arc<RwLock<DeltaTable>>,
        table_schema: SchemaRef,
        base_snapshot: Arc<DeltaQuadStorageSnapshot>,
    ) -> Self {
        Self {
            storage,
            state,
            table,
            table_schema,
            base_snapshot,
            parts: RwLock::new(vec![]),
            may_depend_on_database_state: AtomicBool::new(false),
        }
    }

    /// Append a stream of quads to the log.
    ///
    /// This operation is lazy and executed during [`Self::execute`].
    pub async fn append_quads(
        &self,
        quads: DataFrame,
    ) -> Result<(), DeltaQuadStorageError> {
        self.append_quads_with_operation(quads, DeltaStorageLogOperation::InsertQuad)
            .await
    }

    /// Append the removal of a stream of quads to the log.
    ///
    /// This operation is lazy and executed during [`Self::execute`].
    pub async fn remove_quads(
        &self,
        quads: DataFrame,
    ) -> Result<(), DeltaQuadStorageError> {
        self.append_quads_with_operation(quads, DeltaStorageLogOperation::RemoveQuad)
            .await
    }

    /// Implements the appending operation. This is used to implement both `append_quads` and
    /// `remove_quads`.
    ///
    /// This adds the [`COL_OPERATION`] based on the given operation and inserts it into the
    /// underlying delta table.
    ///
    /// This operation is lazy and executed during [`Self::execute`].
    async fn append_quads_with_operation(
        &self,
        quads: DataFrame,
        operation: DeltaStorageLogOperation,
    ) -> Result<(), DeltaQuadStorageError> {
        validate_data_frame_schema(&self.table_schema, quads.schema().inner())?;

        let mut parts = self.parts.write().await;
        let seq_id = parts.len() as i64;
        let quads_with_operation = add_operation_to_quads(quads, operation, seq_id);
        parts.push(quads_with_operation);

        return Ok(());

        /// Validates that the stream schema matches the expected schema (which is based on the
        /// used encoding);
        fn validate_data_frame_schema(
            output_schema: &SchemaRef,
            actual: &SchemaRef,
        ) -> Result<(), DeltaQuadStorageError> {
            let expected_stream_schema = output_schema
                .project(&[2, 3, 4, 5])
                .expect("Valid projection");

            // Don't use equality because the expected_stream_schema is nullable
            if !expected_stream_schema.equivalent_names_and_types(actual.as_ref()) {
                return Err(DeltaQuadStorageError::InvalidSchema(Arc::clone(actual)));
            }

            Ok(())
        }

        /// Adds the [`COL_OPERATION`] and [`COL_COMMIT_VERSION`] for each record batch that is being
        /// streamed.
        fn add_operation_to_quads(
            quads: DataFrame,
            operation: DeltaStorageLogOperation,
            seq_id: i64,
        ) -> DataFrame {
            let schema = quads.schema().clone();
            let mut exprs = Vec::new();
            exprs.push(
                lit(ScalarValue::Int64(None))
                    .cast_to(&DataType::Int64, &schema)
                    .unwrap()
                    .alias(COL_COMMIT_VERSION),
            );
            exprs.push(lit(seq_id).alias(COL_OPERATION_SEQ_ID));
            exprs.push(lit(operation.as_stored()).alias(COL_OPERATION));
            exprs.extend(schema.columns().into_iter().map(Expr::from));

            quads.select(exprs).expect("Valid projection")
        }
    }

    /// Append a graph-level operation to the log.
    pub async fn append_graph_operation(
        &self,
        operation: DeltaStorageLogOperation,
        graph: ScalarValue,
    ) -> Result<(), DeltaQuadStorageError> {
        let (index, _) = self
            .table_schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == COL_GRAPH)
            .expect("Schema validated");
        let batch = RecordBatch::try_new(
            Arc::new(
                self.table_schema
                    .project(&[index])
                    .expect("Valid projection"),
            ),
            vec![
                graph
                    .to_array_of_size(1)
                    .expect("Valid array representation"),
            ],
        )
        .expect("Valid batch");
        let context = SessionContext::new_with_state(self.state.clone());
        let data_frame = context.read_batch(batch)?;
        self.append_graph_operations(operation, data_frame).await
    }

    /// Append a graph-level operation to the log.
    pub async fn append_graph_operations(
        &self,
        operation: DeltaStorageLogOperation,
        graphs: DataFrame,
    ) -> Result<(), DeltaQuadStorageError> {
        let mut parts = self.parts.write().await;
        let seq_id = parts.len() as i64;
        let null_lit = self.storage.storage_encoding().create_null_scalar()?;
        let schema = graphs.schema().clone();
        let data_frame = graphs.select([
            lit(ScalarValue::Int64(None))
                .cast_to(&DataType::Int64, &schema)
                .unwrap()
                .alias(COL_COMMIT_VERSION),
            lit(seq_id).alias(COL_OPERATION_SEQ_ID),
            lit(ScalarValue::Int8(Some(operation.as_stored()))).alias(COL_OPERATION),
            col(COL_GRAPH),
            lit(null_lit.clone()).alias(COL_SUBJECT),
            lit(null_lit.clone()).alias(COL_PREDICATE),
            lit(null_lit).alias(COL_OBJECT),
        ])?;
        parts.push(data_frame);
        Ok(())
    }

    /// Executes the transaction, writing the commits to the storage backend and changing the table
    /// state.
    pub async fn execute(self) -> Result<(), DeltaQuadStorageError> {
        let DeltaQuadStorageTransaction {
            storage: _,
            base_snapshot: _,
            parts,
            table_schema,
            table,
            state: _,
            may_depend_on_database_state,
        } = self;

        let parts = parts.into_inner();
        if parts.is_empty() {
            return Ok(());
        }

        let mut writer = create_record_batch_writer(&table).await?;
        let aligned_schema = Arc::new(table_schema.project(&(0..6).collect::<Vec<_>>())?);

        let mut add_actions = Vec::new();

        let mut current_count = 0;
        for part in parts {
            let mut batch_stream = part.execute_stream().await?;
            while let Some(batch) = batch_stream.next().await {
                let batch = batch?;
                // Project columns into the target schema (make subject etc. nullable)
                // Use only columns 1..7 for writing (op, seq_id, g, s, p, o)
                // index 0 is _commit_version
                let batch = RecordBatch::try_new(
                    Arc::clone(&aligned_schema),
                    batch.columns()[1..7].to_vec(),
                )
                .expect("Failed to align schema nullability");

                current_count += batch.num_rows();
                writer.write(batch).await?;

                if current_count >= 10_000_000 {
                    info!("Flushing ~10M operations during large transaction ...");
                    let new_files = writer.flush().await?;
                    add_actions.extend(new_files);
                    current_count = 0;
                }
            }
        }

        let new_files = writer.flush().await?;
        add_actions.extend(new_files);

        let mut table = table.write().await;
        let table_state = table.state.as_ref().expect("Table loaded");
        let mut commit_builder = CommitBuilder::default()
            .with_actions(add_actions.into_iter().map(Action::Add).collect());
        if may_depend_on_database_state.load(Ordering::Relaxed) {
            commit_builder = commit_builder.with_max_retries(0);
        }
        let result = commit_builder
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

    /// Encodes the quads using object ids, if necessary.
    async fn encode_quads_if_necessary(
        &self,
        quads: DataFrame,
    ) -> Result<DataFrame, StorageError> {
        let quads_schema = quads.schema();

        // If schema matches, no encoding is necessary
        if quads_schema.inner().equivalent_names_and_types(
            self.storage
                .storage_encoding()
                .quad_schema()
                .inner()
                .as_ref(),
        ) {
            return Ok(quads);
        };

        let (state, logical_plan) = quads.into_parts();

        let encoded = ChangeEncodingNode::try_new(logical_plan, self.storage.encoding())?;
        Ok(DataFrame::new(
            state,
            LogicalPlan::Extension(Extension {
                node: Arc::new(encoded),
            }),
        ))
    }

    /// Handles clear or drop graph operation.
    async fn handle_clear_or_drop_graph(
        &self,
        graph: &QuadStorageGraphTarget,
        op: DeltaStorageLogOperation,
    ) -> Result<(), StorageError> {
        match graph {
            QuadStorageGraphTarget::NamedNode(graph_name) => {
                let scalar_value = self
                    .storage
                    .storage_encoding()
                    .encode_term_scalar(graph_name.as_ref().into())?;
                self.append_graph_operation(op, scalar_value)
                    .await
                    .map_err(|e| StorageError::Other(Box::new(e)))?;
            }
            QuadStorageGraphTarget::BlankNode(blank_node) => {
                let scalar_value = self
                    .storage
                    .storage_encoding()
                    .encode_term_scalar(blank_node.as_ref().into())?;
                self.append_graph_operation(op, scalar_value)
                    .await
                    .map_err(|e| StorageError::Other(Box::new(e)))?;
            }
            QuadStorageGraphTarget::DefaultGraph => {
                let scalar_value = self
                    .storage
                    .storage_encoding()
                    .create_null_scalar()
                    .map_err(|e| StorageError::Other(Box::new(e)))?;
                self.append_graph_operation(op, scalar_value)
                    .await
                    .map_err(|e| StorageError::Other(Box::new(e)))?;
            }
            QuadStorageGraphTarget::NamedGraphs | QuadStorageGraphTarget::AllGraphs => {
                let snapshot = self.snapshot().await?;
                let named_graphs = snapshot.named_graphs(&self.state).await?;
                let schema = named_graphs.schema();
                let mut batches = collect(named_graphs, self.state.task_ctx()).await?;
                if matches!(graph, QuadStorageGraphTarget::AllGraphs) {
                    let null_value =
                        self.storage.storage_encoding().create_null_scalar()?;
                    let default_graph_batch =
                        RecordBatch::try_new(schema, vec![null_value.to_array()?])
                            .expect("Schema should match");
                    batches.push(default_graph_batch);
                }

                // Nothing to clear or drop
                if batches.is_empty() {
                    return Ok(());
                }

                let context = SessionContext::new_with_state(self.state.clone());
                let data_frame = context.read_batches(batches)?;
                self.append_graph_operations(op, data_frame)
                    .await
                    .map_err(|e| StorageError::Other(Box::new(e)))?;
            }
        }
        Ok(())
    }
}

/// Returns a new writer for the log table.
///
/// Immediately drops the lock on `table` after creating the writer. This is necessary as the read
/// lock will not be automatically promoted to a write lock.
async fn create_record_batch_writer(
    table: &RwLock<DeltaTable>,
) -> Result<RecordBatchWriter, DeltaQuadStorageError> {
    let table = table.read().await;
    let writer = RecordBatchWriter::for_table(&table)?;
    Ok(writer)
}

#[async_trait]
impl QuadStorageTransaction for DeltaQuadStorageTransaction {
    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        self.may_depend_on_database_state
            .store(true, Ordering::Relaxed);
        let parts = self.parts.read().await.clone();
        if parts.is_empty() {
            return Ok(Arc::clone(&self.base_snapshot) as Arc<dyn QuadStorageSnapshot>);
        }

        // 1. Collect all pending parts (new operations) into RecordBatches
        let mut new_ops = Vec::new();
        for part in parts {
            let batches = part
                .collect()
                .await
                .map_err(|e| StorageError::Other(Box::new(e)))?;
            new_ops.extend(batches);
        }

        if new_ops.is_empty() {
            return Ok(Arc::clone(&self.base_snapshot) as Arc<dyn QuadStorageSnapshot>);
        }

        let context = SessionContext::new_with_state(self.state.clone());
        let mut fields = Vec::with_capacity(7);
        fields.push(Arc::new(Field::new(
            COL_COMMIT_VERSION,
            DataType::Int64,
            true,
        )));
        fields.extend(
            self.table_schema
                .fields()
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
        );
        let ops_schema = Arc::new(Schema::new(fields));

        let new_ops = new_ops
            .into_iter()
            .map(|batch| {
                let columns = batch.columns().to_vec();
                RecordBatch::try_new(Arc::clone(&ops_schema), columns)
                    .map_err(|e| StorageError::Other(Box::new(e)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let new_ops_plan = context
            .read_batches(new_ops)?
            .create_physical_plan()
            .await?;

        // 2. Compute the initial net changeset from the base snapshot
        let range =
            DeltaStorageLogVersionRange::new_unchecked(0, self.base_snapshot.version());
        let initial_changeset = self
            .storage
            .log()
            .compute_changeset(&self.state, range)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        let eager_changeset = initial_changeset.as_eager_changeset(&self.state).await?;

        // The new version range is just for metadata here, we use the base version + 1
        let new_range = DeltaStorageLogVersionRange::new_unchecked(
            0,
            self.base_snapshot.version() + 1,
        );
        let extended_changeset = eager_changeset
            .extend(&self.state, new_range, new_ops_plan)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        // 4. Return a new snapshot with the transactional changeset
        let new_snapshot = (*self.base_snapshot)
            .clone()
            .with_transactional_changeset(Arc::new(extended_changeset));

        Ok(Arc::new(new_snapshot) as Arc<dyn QuadStorageSnapshot>)
    }

    async fn insert(&self, quads: DataFrame) -> Result<Option<usize>, StorageError> {
        let quads = self.encode_quads_if_necessary(quads).await?;
        self.append_quads(quads)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(None)
    }

    async fn remove(&self, quads: DataFrame) -> Result<Option<bool>, StorageError> {
        let quads = self.encode_quads_if_necessary(quads).await?;
        self.remove_quads(quads)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(None)
    }

    async fn create_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<Option<bool>, StorageError> {
        let graph_batch = self
            .storage
            .storage_encoding()
            .encode_term_scalar(graph_name.into())?;
        self.append_graph_operation(DeltaStorageLogOperation::CreateGraph, graph_batch)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(None)
    }

    async fn clear_graph(
        &self,
        graph: &QuadStorageGraphTarget,
    ) -> Result<(), StorageError> {
        self.handle_clear_or_drop_graph(graph, DeltaStorageLogOperation::ClearGraph)
            .await?;
        Ok(())
    }

    async fn drop_graph(
        &self,
        graph: &QuadStorageGraphTarget,
    ) -> Result<(), StorageError> {
        self.handle_clear_or_drop_graph(graph, DeltaStorageLogOperation::DropGraph)
            .await?;
        Ok(())
    }

    async fn len(&self, state: &SessionState) -> Result<usize, StorageError> {
        let snapshot = self.snapshot().await?;
        snapshot.len(state).await
    }

    async fn commit(self: Box<Self>) -> Result<(), StorageError> {
        self.execute()
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))
    }
}

impl Debug for DeltaQuadStorageTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaStorageLogTransaction")
            .field("table", &self.table)
            .finish()
    }
}
