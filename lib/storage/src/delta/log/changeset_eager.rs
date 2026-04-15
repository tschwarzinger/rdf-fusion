use crate::delta::error::DeltaQuadStorageError;
use crate::delta::log::changeset::DeltaStorageLogChangeset;
use crate::delta::log::{
    COL_OPERATION, DeltaStorageLogOperation, DeltaStorageLogVersionRange,
};
use async_trait::async_trait;
use datafusion::arrow::array::{AsArray, RecordBatch, UInt64Builder};
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::execution::{SendableRecordBatchStream, SessionState};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use deltalake::arrow::compute::take_record_batch;
use deltalake::arrow::datatypes::Int8Type;
use futures::StreamExt;
use rdf_fusion_model::AResult;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::sync::Arc;

/// Represents a changeset between two versions of the [`DeltaStorageLog`].
pub struct EagerDeltaStorageLogChangeset {
    session_context: SessionContext,
    version_range: DeltaStorageLogVersionRange,
    is_clear_all: bool,
    cleared_graphs: Vec<RecordBatch>,
    removed_quads: Vec<RecordBatch>,
    added_quads: Vec<RecordBatch>,
    added_named_graphs: Vec<RecordBatch>,
    dropped_named_graphs: Vec<RecordBatch>,
}

impl EagerDeltaStorageLogChangeset {
    /// Partitions a stream of operations for creating an [`EagerDeltaStorageLogChangeset`].
    pub async fn partition_operations(
        state: &SessionState,
        version_range: DeltaStorageLogVersionRange,
        operations: SendableRecordBatchStream,
    ) -> Result<Self, DeltaQuadStorageError> {
        partition_changeset_operations(state, version_range, operations).await
    }
}

/// Partitions the operations for an index update dynamically from the stream.
async fn partition_changeset_operations(
    state: &SessionState,
    version_range: DeltaStorageLogVersionRange,
    mut operations: SendableRecordBatchStream,
) -> Result<EagerDeltaStorageLogChangeset, DeltaQuadStorageError> {
    let schema = operations.schema();

    // Map necessary columns and projection indices
    let op_idx = schema.index_of(COL_OPERATION).unwrap_or(0);
    let graph_idx = schema.index_of(COL_GRAPH).unwrap_or(1);
    let sub_idx = schema.index_of(COL_SUBJECT).unwrap_or(2);
    let pred_idx = schema.index_of(COL_PREDICATE).unwrap_or(3);
    let obj_idx = schema.index_of(COL_OBJECT).unwrap_or(4);

    let quad_proj = vec![graph_idx, sub_idx, pred_idx, obj_idx];
    let graph_proj = vec![graph_idx];

    let quad_schema = Arc::new(schema.project(&quad_proj)?);
    let graph_schema = Arc::new(schema.project(&graph_proj)?);

    let batch_size = state.config().batch_size();

    // Set up state
    let mut is_clear_all = false;
    let mut cleared_graphs_coal =
        BatchCoalescer::new(Arc::clone(&graph_schema), batch_size);
    let mut dropped_named_graphs_coal =
        BatchCoalescer::new(Arc::clone(&graph_schema), batch_size);
    let mut added_named_graphs_coal =
        BatchCoalescer::new(Arc::clone(&graph_schema), batch_size);

    let mut removed_quads_coal =
        BatchCoalescer::new(Arc::clone(&quad_schema), batch_size);
    let mut added_quads_coal = BatchCoalescer::new(Arc::clone(&quad_schema), batch_size);

    // --- Execute Streaming Processing ---
    while let Some(batch) = operations.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }

        let graph_col = batch.column(graph_idx);
        let sub_col = batch.column(sub_idx);
        let pred_col = batch.column(pred_idx);
        let obj_col = batch.column(obj_idx);

        let mut cleared_graphs_idx = UInt64Builder::new();
        let mut dropped_named_graphs_idx = UInt64Builder::new();
        let mut added_named_graphs_idx = UInt64Builder::new();
        let mut removed_quads_idx = UInt64Builder::new();
        let mut added_quads_idx = UInt64Builder::new();

        // Single linear pass over the batch rows
        let ops = batch.column(op_idx).as_primitive::<Int8Type>();
        for row in 0..batch.num_rows() {
            let op = DeltaStorageLogOperation::from_stored(ops.value(row)).ok_or_else(
                || {
                    DeltaQuadStorageError::Other(format!(
                        "Invalid operation: {}",
                        ops.value(row)
                    ))
                },
            )?;

            let graph_valid = !graph_col.is_null(row);
            let quad_valid =
                !sub_col.is_null(row) && !pred_col.is_null(row) && !obj_col.is_null(row);

            let row_u64 = u64::try_from(row).map_err(|_| {
                DeltaQuadStorageError::Other(
                    "Batch size could not be converted to u64.".to_string(),
                )
            })?;

            match op {
                DeltaStorageLogOperation::ClearDatabase => {
                    is_clear_all = true;
                }
                DeltaStorageLogOperation::DropGraph => {
                    cleared_graphs_idx.append_value(row_u64);
                    if graph_valid {
                        dropped_named_graphs_idx.append_value(row_u64);
                    }
                }
                DeltaStorageLogOperation::ClearGraph => {
                    cleared_graphs_idx.append_value(row_u64);
                }
                DeltaStorageLogOperation::CreateGraph => {
                    if graph_valid {
                        added_named_graphs_idx.append_value(row_u64);
                    }
                }
                DeltaStorageLogOperation::RemoveQuad => {
                    if !quad_valid {
                        return Err(DeltaQuadStorageError::Corruption("Invalid remove quad operation: missing subject, predicate, or object".to_string()));
                    }
                    removed_quads_idx.append_value(row_u64);
                }
                DeltaStorageLogOperation::AddQuad => {
                    if !quad_valid {
                        return Err(DeltaQuadStorageError::Corruption("Invalid remove quad operation: missing subject, predicate, or object".to_string()));
                    }

                    added_quads_idx.append_value(row_u64);
                    if graph_valid {
                        added_named_graphs_idx.append_value(row_u64);
                    }
                }
            }
        }

        let cleared_graphs = take_record_batch(
            &batch.project(&graph_proj)?,
            &cleared_graphs_idx.finish(),
        )?;
        cleared_graphs_coal.push_batch(cleared_graphs)?;

        let dropped_graphs = take_record_batch(
            &batch.project(&graph_proj)?,
            &dropped_named_graphs_idx.finish(),
        )?;
        dropped_named_graphs_coal.push_batch(dropped_graphs)?;

        let added_named_graphs = take_record_batch(
            &batch.project(&graph_proj)?,
            &added_named_graphs_idx.finish(),
        )?;
        added_named_graphs_coal.push_batch(added_named_graphs)?;

        let removed_quads =
            take_record_batch(&batch.project(&quad_proj)?, &removed_quads_idx.finish())?;
        removed_quads_coal.push_batch(removed_quads)?;

        let added_quads =
            take_record_batch(&batch.project(&quad_proj)?, &added_quads_idx.finish())?;
        added_quads_coal.push_batch(added_quads)?;
    }

    let cleared_graphs = drain_coalescer(cleared_graphs_coal)?;
    let dropped_named_graphs = drain_coalescer(dropped_named_graphs_coal)?;
    let added_named_graphs = drain_coalescer(added_named_graphs_coal)?;
    let removed_quads = drain_coalescer(removed_quads_coal)?;
    let added_quads = drain_coalescer(added_quads_coal)?;

    let session_context = SessionContext::new_with_state(state.clone());

    // Fulfill the distinct modifier natively on the collected subset
    let added_named_graphs = if added_named_graphs.is_empty() {
        vec![]
    } else {
        let df = session_context.read_batches(added_named_graphs)?;
        df.distinct()?.collect().await?
    };

    let removed_quads =
        enforce_non_nullable(removed_quads, &[COL_SUBJECT, COL_PREDICATE, COL_OBJECT])?;
    let added_quads =
        enforce_non_nullable(added_quads, &[COL_SUBJECT, COL_PREDICATE, COL_OBJECT])?;

    Ok(EagerDeltaStorageLogChangeset {
        session_context,
        version_range,
        is_clear_all,
        cleared_graphs,
        removed_quads,
        added_quads,
        added_named_graphs,
        dropped_named_graphs,
    })
}

/// Drains the coalescer and returns the completed batches.
fn drain_coalescer(mut coal: BatchCoalescer) -> AResult<Vec<RecordBatch>> {
    coal.finish_buffered_batch()?;
    let mut result = Vec::new();
    while let Some(batch) = coal.next_completed_batch() {
        result.push(batch);
    }
    Ok(result)
}

/// Rebuilds the RecordBatches with a strict, non-nullable schema for the specified columns.
fn enforce_non_nullable(
    batches: Vec<RecordBatch>,
    non_nullable_cols: &[&str],
) -> AResult<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(batches);
    }

    let old_schema = batches[0].schema();
    let mut new_fields = Vec::with_capacity(old_schema.fields().len());

    for field in old_schema.fields() {
        if non_nullable_cols.contains(&field.name().as_str()) {
            new_fields.push(Arc::new(Field::new(
                field.name(),
                field.data_type().clone(),
                false,
            )));
        } else {
            new_fields.push(Arc::clone(field));
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));

    batches
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(Arc::clone(&new_schema), batch.columns().to_vec())
        })
        .collect()
}

#[async_trait]
impl DeltaStorageLogChangeset for EagerDeltaStorageLogChangeset {
    fn version_range(&self) -> DeltaStorageLogVersionRange {
        self.version_range
    }

    async fn contains_clear_all(
        &self,
        _state: &SessionState,
    ) -> Result<bool, DeltaQuadStorageError> {
        Ok(self.is_clear_all)
    }

    async fn cleared_graphs(
        &self,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
        create_result(&self.session_context, &self.cleared_graphs).await
    }

    async fn removed_quads(
        &self,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
        create_result(&self.session_context, &self.removed_quads).await
    }

    async fn added_quads(
        &self,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
        create_result(&self.session_context, &self.added_quads).await
    }

    async fn added_named_graphs(
        &self,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
        create_result(&self.session_context, &self.added_named_graphs).await
    }

    async fn dropped_named_graphs(
        &self,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
        create_result(&self.session_context, &self.dropped_named_graphs).await
    }
}

/// Creates a [`DataFrame`] from the given batches.
async fn create_result(
    session_context: &SessionContext,
    batches: &[RecordBatch],
) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
    if batches.is_empty() {
        return Ok(None);
    }

    let result = session_context
        .read_batches(batches.iter().cloned())?
        .create_physical_plan()
        .await?;
    Ok(Some(result))
}
