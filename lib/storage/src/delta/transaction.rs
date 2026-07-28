use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::log::{
    COL_COMMIT_VERSION, COL_OPERATION, COL_OPERATION_SEQ_ID, DeltaStorageLogOperation,
    DeltaStorageLogVersionRange,
};
use crate::delta::snapshot::DeltaQuadsStorageSnapshot;
use crate::delta::storage::DeltaQuadsStorage;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{ScalarValue, SchemaExt};
use datafusion::dataframe::DataFrame;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{ExprSchemable, Extension, LogicalPlan, col};
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{Expr, SessionContext, lit};
use deltalake::DeltaTable;
use deltalake::kernel::Action;
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::protocol::{DeltaOperation, SaveMode};
use futures::StreamExt;
use rdf_fusion_common::StorageError;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::{QuadStorageEncoding, TermEncoding};
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use rdf_fusion_logical::encoding::object_id::EncodeAsObjectIdNode;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;
use tracing::info;

/// A transaction on a [`DeltaQuadsStorage`].
pub struct DeltaQuadsStorageTransaction {
    /// The storage
    storage: Arc<DeltaQuadsStorage>,
    /// The session context.
    state: SessionState,
    /// The target table of the transaction.
    table: Arc<RwLock<DeltaTable>>,
    /// The schema of the table.
    table_schema: SchemaRef,
    /// The base snapshot of the transaction.
    base_snapshot: Arc<DeltaQuadsStorageSnapshot>,
    /// The individual parts of the transaction. When the transaction is executed, all parts are
    /// evaluated and their results are written to disk. Then, the resulting files are appended to
    /// the log table.
    parts: RwLock<Vec<DataFrame>>,
    /// Indicates whether the result of the transaction can depend on the database state. This flag
    /// is set to true if a snapshot from the transaction is obtained.
    may_depend_on_database_state: AtomicBool,
}

impl DeltaQuadsStorageTransaction {
    /// Creates a new [`DeltaQuadsStorageTransaction`].
    pub fn new(
        storage: Arc<DeltaQuadsStorage>,
        state: SessionState,
        table: Arc<RwLock<DeltaTable>>,
        table_schema: SchemaRef,
        base_snapshot: Arc<DeltaQuadsStorageSnapshot>,
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
    ) -> Result<(), DeltaQuadsStorageError> {
        self.append_quads_with_operation(quads, DeltaStorageLogOperation::InsertQuad)
            .await
    }

    /// Append the removal of a stream of quads to the log.
    ///
    /// This operation is lazy and executed during [`Self::execute`].
    pub async fn remove_quads(
        &self,
        quads: DataFrame,
    ) -> Result<(), DeltaQuadsStorageError> {
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
    ) -> Result<(), DeltaQuadsStorageError> {
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
        ) -> Result<(), DeltaQuadsStorageError> {
            let expected_stream_schema = output_schema
                .project(&[2, 3, 4, 5])
                .expect("Valid projection");

            // Don't use equality because the expected_stream_schema is nullable
            if !expected_stream_schema.equivalent_names_and_types(actual.as_ref()) {
                return Err(DeltaQuadsStorageError::InvalidSchema(Arc::clone(actual)));
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
    pub async fn append_graph_operations(
        &self,
        operation: DeltaStorageLogOperation,
        graphs: DataFrame,
    ) -> Result<(), DeltaQuadsStorageError> {
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
    pub async fn execute(self) -> Result<(), DeltaQuadsStorageError> {
        let DeltaQuadsStorageTransaction {
            storage: _,
            base_snapshot: _,
            parts,
            table_schema,
            table,
            state,
            may_depend_on_database_state,
        } = self;

        let parts = parts.into_inner();
        if parts.is_empty() {
            return Ok(());
        }

        let aligned_schema = Arc::clone(&table_schema);
        let mut streams = Vec::new();

        for part in parts {
            let plan = part.create_physical_plan().await?;
            let partitions = plan.output_partitioning().partition_count();
            for i in 0..partitions {
                let stream = plan.execute(i, state.task_ctx())?;
                streams.push(stream);
            }
        }

        let streams = Arc::new(tokio::sync::Mutex::new(streams.into_iter()));
        let mut tasks = Vec::new();
        let target_partitions = state.config().target_partitions();
        let workers = std::cmp::max(1, target_partitions);

        for _ in 0..workers {
            let table = Arc::clone(&table);
            let aligned_schema = Arc::clone(&aligned_schema);
            let streams = Arc::clone(&streams);

            let task = datafusion::common::runtime::SpawnedTask::spawn(async move {
                let mut writer =
                    create_record_batch_writer(&table, Arc::clone(&aligned_schema))
                        .await?;
                let mut add_actions = Vec::new();
                let mut current_count = 0;

                loop {
                    let mut batch_stream = {
                        let mut lock = streams.lock().await;
                        match lock.next() {
                            Some(stream) => stream,
                            None => break,
                        }
                    };

                    while let Some(batch) = batch_stream.next().await {
                        let batch = batch?;
                        // Project columns into the target schema (make subject etc. nullable)
                        // Use only columns 1..7 for writing (op, seq_id, g, s, p, o)
                        // quad_table 0 is _commit_version
                        let batch = RecordBatch::try_new(
                            Arc::clone(&aligned_schema),
                            batch.columns()[1..7].to_vec(),
                        )
                        .expect("Failed to align schema nullability");

                        current_count += batch.num_rows();
                        writer
                            .write(&batch)
                            .await
                            .map_err(|e| DeltaQuadsStorageError::Other(e.to_string()))?;

                        if current_count >= 1_000_000 {
                            info!("Flushing ~1M operations during large transaction ...");
                            let new_files = writer.close().await.map_err(|e| {
                                DeltaQuadsStorageError::Other(e.to_string())
                            })?;
                            add_actions.extend(new_files);
                            current_count = 0;
                            writer = create_record_batch_writer(
                                &table,
                                Arc::clone(&aligned_schema),
                            )
                            .await?;
                        }
                    }
                }

                let new_files = writer
                    .close()
                    .await
                    .map_err(|e| DeltaQuadsStorageError::Other(e.to_string()))?;
                add_actions.extend(new_files);

                Ok::<Vec<deltalake::kernel::Add>, DeltaQuadsStorageError>(add_actions)
            });
            tasks.push(task);
        }

        let mut add_actions = Vec::new();
        for task in tasks {
            let actions = task.await.expect("Task panicked")?;
            add_actions.extend(actions.into_iter().map(Action::Add));
        }

        let mut table = table.write().await;
        let table_state = table.state.as_ref().expect("Table loaded");
        let mut commit_builder = CommitBuilder::default().with_actions(add_actions);
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
    async fn prepare_inserted_quads(
        &self,
        quads: DataFrame,
    ) -> Result<DataFrame, StorageError> {
        let quads_schema = quads.schema();

        let expected_stream_schema = self
            .table_schema
            .project(&[2, 3, 4, 5])
            .expect("Valid projection");

        let names_match = quads_schema.inner().fields().len()
            == expected_stream_schema.fields().len()
            && quads_schema
                .inner()
                .fields()
                .iter()
                .zip(expected_stream_schema.fields().iter())
                .all(|(f1, f2)| f1.name() == f2.name());

        if !names_match {
            return Err(DeltaQuadsStorageError::InvalidSchema(Arc::clone(
                quads_schema.inner(),
            ))
            .into());
        }

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

        self.encode_dataframe(quads)
    }

    fn encode_dataframe(&self, df: DataFrame) -> Result<DataFrame, StorageError> {
        let df_schema = df.schema();
        let target_encoding = self.storage.encoding();

        let is_encoded = df_schema.fields().iter().all(|f| match &target_encoding {
            QuadStorageEncoding::ObjectId(encoding) => {
                *f.data_type() == encoding.object_id_data_type().into()
            }
            QuadStorageEncoding::PlainTerm => {
                f.data_type() == PLAIN_TERM_ENCODING.data_type()
            }
            QuadStorageEncoding::String => f.data_type() == STRING_ENCODING.data_type(),
        });

        if is_encoded {
            return Ok(df);
        }

        match target_encoding {
            QuadStorageEncoding::ObjectId(encoding) => {
                let (state, logical_plan) = df.into_parts();
                let node = EncodeAsObjectIdNode::try_new(
                    logical_plan,
                    encoding.object_id_data_type(),
                )?;
                Ok(DataFrame::new(
                    state,
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(node),
                    }),
                ))
            }
            QuadStorageEncoding::PlainTerm => {
                let context = SessionContext::new_with_state(self.state.clone());
                let enc_pt_udf = context.udf("ENC_PT")?;
                let target_type = PLAIN_TERM_ENCODING.data_type();

                let mut decode_udf = None;
                let mut proj_exprs = Vec::new();
                for field in df_schema.fields() {
                    let col_expr = col(field.name().clone());
                    if matches!(
                        field.data_type(),
                        DataType::Int32 | DataType::Int64 | DataType::FixedSizeBinary(_)
                    ) {
                        let udf = match decode_udf.as_ref() {
                            Some(udf) => udf,
                            None => {
                                decode_udf = Some(context.udf("DECODE_PT")?);
                                decode_udf.as_ref().unwrap()
                            }
                        };
                        proj_exprs
                            .push(udf.call(vec![col_expr]).alias(field.name().clone()));
                    } else if field.data_type() != target_type {
                        proj_exprs.push(
                            enc_pt_udf.call(vec![col_expr]).alias(field.name().clone()),
                        );
                    } else {
                        proj_exprs.push(col_expr);
                    }
                }
                Ok(df.select(proj_exprs)?)
            }
            QuadStorageEncoding::String => {
                let context = SessionContext::new_with_state(self.state.clone());
                let enc_str_udf = context.udf("ENC_STR")?;
                let target_type = STRING_ENCODING.data_type();

                let mut decode_udf = None;
                let mut proj_exprs = Vec::new();
                for field in df_schema.fields() {
                    let col_expr = col(field.name().clone());
                    if matches!(
                        field.data_type(),
                        DataType::Int32 | DataType::Int64 | DataType::FixedSizeBinary(_)
                    ) {
                        let udf = match decode_udf.as_ref() {
                            Some(udf) => udf,
                            None => {
                                decode_udf = Some(context.udf("DECODE_PT")?);
                                decode_udf.as_ref().unwrap()
                            }
                        };
                        let decoded = udf.call(vec![col_expr]);
                        proj_exprs.push(
                            enc_str_udf.call(vec![decoded]).alias(field.name().clone()),
                        );
                    } else if field.data_type() != target_type {
                        proj_exprs.push(
                            enc_str_udf.call(vec![col_expr]).alias(field.name().clone()),
                        );
                    } else {
                        proj_exprs.push(col_expr);
                    }
                }
                Ok(df.select(proj_exprs)?)
            }
        }
    }
}

#[async_trait]
impl QuadStorageTransaction for DeltaQuadsStorageTransaction {
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
        let quads = self.prepare_inserted_quads(quads).await?;
        self.append_quads(quads)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(None)
    }

    async fn remove(&self, quads: DataFrame) -> Result<Option<bool>, StorageError> {
        let quads = self.prepare_inserted_quads(quads).await?;
        self.remove_quads(quads)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(None)
    }

    async fn create_named_graph(
        &self,
        graphs: DataFrame,
    ) -> Result<Option<bool>, StorageError> {
        let graphs = self.encode_dataframe(graphs)?;
        self.append_graph_operations(DeltaStorageLogOperation::CreateGraph, graphs)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(None)
    }

    async fn clear_graph(&self, graphs: DataFrame) -> Result<(), StorageError> {
        let graphs = self.encode_dataframe(graphs)?;
        self.append_graph_operations(DeltaStorageLogOperation::ClearGraph, graphs)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(())
    }

    async fn drop_graph(&self, graphs: DataFrame) -> Result<(), StorageError> {
        let graphs = self.encode_dataframe(graphs)?;
        self.append_graph_operations(DeltaStorageLogOperation::DropGraph, graphs)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
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

impl Debug for DeltaQuadsStorageTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaStorageLogTransaction")
            .field("table", &self.table)
            .finish()
    }
}

async fn create_record_batch_writer(
    table: &RwLock<DeltaTable>,
    schema: Arc<Schema>,
) -> Result<DeltaWriter, DeltaQuadsStorageError> {
    use deltalake::table::config::TablePropertiesExt;

    let table = table.read().await;
    let table_state = table.state.as_ref().unwrap();
    let table_props = table_state.table_config();

    let config = WriterConfig::new(
        schema,
        table_state.metadata().partition_columns().to_vec(),
        None,
        Some(table_props.target_file_size()),
        None,
        table_props.num_indexed_cols(),
        table_props
            .data_skipping_stats_columns
            .as_ref()
            .map(|c| c.iter().map(|c| c.to_string()).collect::<Vec<_>>()),
    );
    let writer = DeltaWriter::new(table.object_store(), config);
    Ok(writer)
}
