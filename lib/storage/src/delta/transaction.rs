use crate::delta::error::DeltaQuadStorageError;
use crate::delta::log::{
    COL_COMMIT_VERSION, COL_OPERATION, DeltaStorageLogOperation,
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
use tokio::sync::RwLock;

/// A transaction on a [`DeltaStorageLog`].
pub struct DeltaStorageTransaction {
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
}

impl DeltaStorageTransaction {
    /// Creates a new [`DeltaStorageTransaction`].
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
        }
    }

    /// Append a stream of quads to the log.
    ///
    /// This operation is lazy and executed during [`Self::execute`].
    pub async fn append_quads(
        &self,
        quads: DataFrame,
    ) -> Result<(), DeltaQuadStorageError> {
        self.append_quads_with_operation(quads, DeltaStorageLogOperation::AddQuad)
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

        let quads_with_operation = add_operation_to_quads(quads, operation);
        self.parts.write().await.push(quads_with_operation);

        return Ok(());

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

        /// Adds the [`COL_OPERATION`] and [`COL_COMMIT_VERSION`] for each record batch that is being
        /// streamed.
        fn add_operation_to_quads(
            quads: DataFrame,
            operation: DeltaStorageLogOperation,
        ) -> DataFrame {
            let schema = quads.schema().clone();
            let mut exprs = Vec::new();
            exprs.push(lit(operation.as_stored()).alias(COL_OPERATION));
            exprs.extend(schema.columns().into_iter().map(Expr::from));
            exprs.push(
                lit(ScalarValue::Int64(None))
                    .cast_to(&DataType::Int64, &schema)
                    .unwrap()
                    .alias(COL_COMMIT_VERSION),
            );

            quads.select(exprs).expect("Valid projection")
        }
    }

    /// Append a graph-level operation to the log.
    pub async fn append_graph_operation(
        &self,
        operation: DeltaStorageLogOperation,
        graph: ScalarValue,
    ) -> Result<(), DeltaQuadStorageError> {
        let batch = RecordBatch::try_new(
            Arc::new(self.table_schema.project(&[1]).expect("Valid projection")),
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
        let null_lit = self.storage.storage_encoding().create_null_scalar()?;
        let schema = graphs.schema().clone();
        let data_frame = graphs.select([
            lit(ScalarValue::Int8(Some(operation.as_stored()))).alias(COL_OPERATION),
            col(COL_GRAPH),
            lit(null_lit.clone()).alias(COL_SUBJECT),
            lit(null_lit.clone()).alias(COL_PREDICATE),
            lit(null_lit).alias(COL_OBJECT),
            lit(ScalarValue::Int64(None))
                .cast_to(&DataType::Int64, &schema)
                .unwrap()
                .alias(COL_COMMIT_VERSION),
        ])?;
        self.parts.write().await.push(data_frame);
        Ok(())
    }

    /// Executes the transaction, writing the commits to the storage backend and changing the table
    /// state.
    pub async fn execute(self) -> Result<(), DeltaQuadStorageError> {
        let DeltaStorageTransaction {
            storage: _,
            base_snapshot: _,
            parts,
            table_schema,
            table,
            state: _,
        } = self;

        let parts = parts.into_inner();
        if parts.is_empty() {
            return Ok(());
        }

        let mut writer = create_record_batch_writer(&table).await?;
        let aligned_schema = Arc::new(table_schema.project(&(0..5).collect::<Vec<_>>())?);

        for part in parts {
            let mut batch_stream = part.execute_stream().await?;
            while let Some(batch) = batch_stream.next().await {
                let batch = batch?;
                // Project columns into the target schema (make subject etc. nullable)
                // Use only first 5 columns for writing (op, g, s, p, o)
                let batch = RecordBatch::try_new(
                    Arc::clone(&aligned_schema),
                    batch.columns()[0..5].to_vec(),
                )
                .expect("Failed to align schema nullability");

                writer.write(batch).await?;
            }
        }

        let add_actions = writer.flush().await?;
        let mut table = table.write().await;
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
impl QuadStorageTransaction for DeltaStorageTransaction {
    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
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
        let mut fields = self
            .table_schema
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            COL_COMMIT_VERSION,
            DataType::Int64,
            true,
        )));
        let ops_schema = Arc::new(Schema::new(fields));

        let new_ops = new_ops
            .into_iter()
            .map(|batch| {
                let mut columns = batch.columns().to_vec();
                if columns.len() == 5 {
                    columns.push(Arc::new(
                        datafusion::arrow::array::Int64Array::from_value(
                            0,
                            batch.num_rows(),
                        ),
                    ));
                }
                RecordBatch::try_new(Arc::clone(&ops_schema), columns)
                    .map_err(|e| StorageError::Other(Box::new(e)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let df_schema = datafusion::common::DFSchema::try_from(Arc::clone(&ops_schema))
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        let new_ops_plan = context
            .read_batches(new_ops)?
            .select(
                ops_schema
                    .fields()
                    .iter()
                    .map(|f| col(f.name()).cast_to(f.data_type(), &df_schema).unwrap())
                    .collect::<Vec<_>>(),
            )?
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

        // 3. Extend the initial changeset with the new operations
        // We need to downcast initial_changeset to EagerDeltaQuadStorageChangeset
        // to call extend.
        let eager_changeset = initial_changeset
            .as_any()
            .downcast_ref::<crate::delta::log::EagerDeltaQuadStorageChangeset>()
            .ok_or_else(|| {
                StorageError::Other(
                    "Failed to downcast changeset to EagerDeltaQuadStorageChangeset"
                        .into(),
                )
            })?;

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

impl Debug for DeltaStorageTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaStorageLogTransaction")
            .field("table", &self.table)
            .finish()
    }
}
