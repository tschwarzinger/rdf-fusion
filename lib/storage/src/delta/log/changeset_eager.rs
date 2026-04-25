use crate::delta::error::DeltaQuadStorageError;
use crate::delta::log::changeset::DeltaQuadStorageLogChangeset;
use crate::delta::log::{
    COL_COMMIT_VERSION, COL_OPERATION, ComputeLogChangesetExec, DeltaStorageLogOperation,
    DeltaStorageLogVersionRange,
};
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, AsArray, Int8Array, Int64Array, RecordBatch, UInt64Builder, new_null_array,
};
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, SessionState};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect, execute_stream,
};
use datafusion::prelude::SessionContext;
use deltalake::arrow::compute::take_record_batch;
use deltalake::arrow::datatypes::Int8Type;
use futures::StreamExt;
use rdf_fusion_model::AResult;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::sync::Arc;

/// Represents a changeset between two versions of the [`DeltaStorageLog`].
pub struct EagerDeltaQuadStorageChangeset {
    session_context: SessionContext,
    version_range: DeltaStorageLogVersionRange,
    operation_schema: SchemaRef,
    cleared_graphs: Vec<RecordBatch>,
    removed_quads: Vec<RecordBatch>,
    added_quads: Vec<RecordBatch>,
    added_named_graphs: Vec<RecordBatch>,
    dropped_named_graphs: Vec<RecordBatch>,
}

impl EagerDeltaQuadStorageChangeset {
    /// Partitions a stream of operations for creating an [`EagerDeltaQuadStorageChangeset`].
    pub async fn partition_operations(
        state: &SessionState,
        version_range: DeltaStorageLogVersionRange,
        operations: SendableRecordBatchStream,
    ) -> Result<Self, DeltaQuadStorageError> {
        partition_changeset_operations(state, version_range, operations).await
    }

    /// Extends the changeset with new operations.
    pub async fn extend(
        &self,
        state: &SessionState,
        new_version_range: DeltaStorageLogVersionRange,
        new_operations_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self, DeltaQuadStorageError> {
        // 1. Collect existing and new operations in-memory
        let mut all_ops = self.to_operations()?;
        let new_ops = collect(new_operations_plan, state.task_ctx()).await?;
        all_ops.extend(new_ops);

        if all_ops.is_empty() {
            return Ok(EagerDeltaQuadStorageChangeset {
                session_context: self.session_context.clone(),
                version_range: DeltaStorageLogVersionRange::new_unchecked(
                    self.version_range.starting_version(),
                    new_version_range.ending_version(),
                ),
                operation_schema: Arc::clone(&self.operation_schema),
                cleared_graphs: vec![],
                removed_quads: vec![],
                added_quads: vec![],
                added_named_graphs: vec![],
                dropped_named_graphs: vec![],
            });
        }

        // 2. Create a physical plan from the combined batches
        let combined_plan = self
            .session_context
            .read_batches(all_ops)?
            .create_physical_plan()
            .await?;

        // 3. Ensure single partition for ComputeLogChangesetExec
        let combined_plan = if combined_plan.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(combined_plan)) as Arc<dyn ExecutionPlan>
        } else {
            combined_plan
        };

        // 4. Feed into change computation
        // Note: No SortExec needed here because we've appended new operations (which are chronologically
        // ordered) to the existing ones (which have version 0).
        let compute_plan = ComputeLogChangesetExec::try_new(combined_plan)?;
        let stream = execute_stream(Arc::new(compute_plan), state.task_ctx())?;

        let new_range = DeltaStorageLogVersionRange::new_unchecked(
            self.version_range.starting_version(),
            new_version_range.ending_version(),
        );

        partition_changeset_operations(state, new_range, stream).await
    }

    /// Reconstructs the original operations from the partitioned batches.
    pub fn to_operations(&self) -> Result<Vec<RecordBatch>, DeltaQuadStorageError> {
        let mut result = Vec::new();

        self.append_category_to_ops(
            &mut result,
            &self.cleared_graphs,
            DeltaStorageLogOperation::ClearGraph,
        )?;
        self.append_category_to_ops(
            &mut result,
            &self.dropped_named_graphs,
            DeltaStorageLogOperation::DropGraph,
        )?;
        self.append_category_to_ops(
            &mut result,
            &self.added_named_graphs,
            DeltaStorageLogOperation::CreateGraph,
        )?;
        self.append_category_to_ops(
            &mut result,
            &self.removed_quads,
            DeltaStorageLogOperation::RemoveQuad,
        )?;
        self.append_category_to_ops(
            &mut result,
            &self.added_quads,
            DeltaStorageLogOperation::AddQuad,
        )?;

        Ok(result)
    }

    fn append_category_to_ops(
        &self,
        ops: &mut Vec<RecordBatch>,
        batches: &[RecordBatch],
        operation: DeltaStorageLogOperation,
    ) -> Result<(), DeltaQuadStorageError> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = Arc::clone(&self.operation_schema);
        let op_val = operation.as_stored();

        for batch in batches {
            let num_rows = batch.num_rows();
            let op_array = Arc::new(Int8Array::from_value(op_val, num_rows)) as ArrayRef;
            let version_array = Arc::new(Int64Array::from_value(0, num_rows)) as ArrayRef;

            let mut columns = Vec::with_capacity(schema.fields().len());
            for field in schema.fields() {
                match field.name().as_str() {
                    COL_OPERATION => columns.push(Arc::clone(&op_array)),
                    COL_COMMIT_VERSION => columns.push(Arc::clone(&version_array)),
                    name => {
                        if let Ok(idx) = batch.schema().index_of(name) {
                            columns.push(Arc::clone(batch.column(idx)));
                        } else {
                            columns.push(new_null_array(field.data_type(), num_rows));
                        }
                    }
                }
            }
            ops.push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
        }
        Ok(())
    }
}

/// Partitions the operations for an index update dynamically from the stream.
async fn partition_changeset_operations(
    state: &SessionState,
    version_range: DeltaStorageLogVersionRange,
    mut operations: SendableRecordBatchStream,
) -> Result<EagerDeltaQuadStorageChangeset, DeltaQuadStorageError> {
    let operation_schema = operations.schema();

    // Map necessary columns and projection indices
    let op_idx = operation_schema.index_of(COL_OPERATION).unwrap_or(0);
    let graph_idx = operation_schema.index_of(COL_GRAPH).unwrap_or(1);
    let sub_idx = operation_schema.index_of(COL_SUBJECT).unwrap_or(2);
    let pred_idx = operation_schema.index_of(COL_PREDICATE).unwrap_or(3);
    let obj_idx = operation_schema.index_of(COL_OBJECT).unwrap_or(4);

    let quad_proj = vec![graph_idx, sub_idx, pred_idx, obj_idx];
    let graph_proj = vec![graph_idx];

    let quad_schema = Arc::new(operation_schema.project(&quad_proj)?);
    let graph_schema = Arc::new(operation_schema.project(&graph_proj)?);

    let batch_size = state.config().batch_size();

    // Set up state
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

    Ok(EagerDeltaQuadStorageChangeset {
        session_context,
        version_range,
        operation_schema,
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
impl DeltaQuadStorageLogChangeset for EagerDeltaQuadStorageChangeset {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn version_range(&self) -> DeltaStorageLogVersionRange {
        self.version_range
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::log::DeltaStorageLogOperation;
    use datafusion::arrow::array::Int64Builder;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::physical_plan::collect;
    use deltalake::arrow::util::pretty::pretty_format_batches;
    use rdf_fusion_encoding::plain_term::{
        PLAIN_TERM_ENCODING, PlainTermArrayElementBuilder,
    };
    use rdf_fusion_encoding::{EncodingArray, TermEncoding};
    use rdf_fusion_model::NamedNodeRef;

    #[tokio::test]
    async fn test_extend_eager_changeset() {
        let session = SessionContext::new();
        let state = session.state();
        let schema = create_operation_schema();

        // 1. Initial changeset: Add s1
        let batch1 = create_batch(
            &schema,
            vec![(
                None,
                "https://s1",
                "https://p1",
                "https://o1",
                DeltaStorageLogOperation::AddQuad,
                1,
            )],
        );
        let stream1 = session
            .read_batch(batch1)
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        let changeset = EagerDeltaQuadStorageChangeset::partition_operations(
            &state,
            DeltaStorageLogVersionRange::new_unchecked(0, 1),
            stream1,
        )
        .await
        .unwrap();

        // 2. Extend with: Remove s1, Add s2
        let batch2 = create_batch(
            &schema,
            vec![
                (
                    None,
                    "https://s1",
                    "https://p1",
                    "https://o1",
                    DeltaStorageLogOperation::RemoveQuad,
                    2,
                ),
                (
                    None,
                    "https://s2",
                    "https://p2",
                    "https://o2",
                    DeltaStorageLogOperation::AddQuad,
                    3,
                ),
            ],
        );
        let plan2 = session
            .read_batch(batch2)
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let extended = changeset
            .extend(
                &state,
                DeltaStorageLogVersionRange::new_unchecked(0, 3),
                plan2,
            )
            .await
            .unwrap();

        // 3. Verify
        let removed = collect_and_format(
            &state,
            extended.removed_quads(&state).await.unwrap().unwrap(),
        )
        .await;
        let added = collect_and_format(
            &state,
            extended.added_quads(&state).await.unwrap().unwrap(),
        )
        .await;

        // s1 should be in removed (because it was Add then Remove), s2 in added
        assert!(removed.contains("https://s1"));
        assert!(added.contains("https://s2"));
        assert!(!added.contains("https://s1"));
    }

    #[tokio::test]
    async fn test_extend_eager_changeset_with_barriers() {
        let session = SessionContext::new();
        let state = session.state();
        let schema = create_operation_schema();

        // 1. Initial changeset: Add s1 in g1
        let batch1 = create_batch(
            &schema,
            vec![(
                Some("https://g1"),
                "https://s1",
                "https://p1",
                "https://o1",
                DeltaStorageLogOperation::AddQuad,
                1,
            )],
        );
        let stream1 = session
            .read_batch(batch1)
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        let changeset = EagerDeltaQuadStorageChangeset::partition_operations(
            &state,
            DeltaStorageLogVersionRange::new_unchecked(0, 1),
            stream1,
        )
        .await
        .unwrap();

        // 2. Extend with: Clear g1, Add s2 in g1
        let mut graph_builder = PlainTermArrayElementBuilder::new();
        graph_builder.append_named_node(NamedNodeRef::new_unchecked("https://g1"));
        let g1_term = graph_builder.finish().into_array_ref();

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::clone(&g1_term),
                new_null_array(schema.field(1).data_type(), 1), // subject
                new_null_array(schema.field(2).data_type(), 1), // predicate
                new_null_array(schema.field(3).data_type(), 1), // object
                Arc::new(Int8Array::from(vec![
                    DeltaStorageLogOperation::ClearGraph.as_stored(),
                ])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();

        let batch3 = create_batch(
            &schema,
            vec![(
                Some("https://g1"),
                "https://s2",
                "https://p2",
                "https://o2",
                DeltaStorageLogOperation::AddQuad,
                3,
            )],
        );

        let plan2 = session
            .read_batches(vec![batch2, batch3])
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let extended = changeset
            .extend(
                &state,
                DeltaStorageLogVersionRange::new_unchecked(0, 3),
                plan2,
            )
            .await
            .unwrap();

        // 3. Verify
        let added = collect_and_format(
            &state,
            extended.added_quads(&state).await.unwrap().unwrap(),
        )
        .await;
        let cleared = collect_and_format(
            &state,
            extended.cleared_graphs(&state).await.unwrap().unwrap(),
        )
        .await;

        // s1 should be GONE (wiped by ClearGraph), s2 should be there, g1 should be in cleared
        assert!(added.contains("https://s2"));
        assert!(!added.contains("https://s1"));
        assert!(cleared.contains("https://g1"));
    }

    fn create_operation_schema() -> SchemaRef {
        let data_type = PLAIN_TERM_ENCODING.data_type().clone();
        Arc::new(Schema::new(vec![
            Field::new(COL_GRAPH, data_type.clone(), true),
            Field::new(COL_SUBJECT, data_type.clone(), true),
            Field::new(COL_PREDICATE, data_type.clone(), true),
            Field::new(COL_OBJECT, data_type, true),
            Field::new(COL_OPERATION, DataType::Int8, false),
            Field::new(COL_COMMIT_VERSION, DataType::Int64, false),
        ]))
    }

    fn create_batch(
        schema: &SchemaRef,
        rows: Vec<(
            Option<&str>,
            &str,
            &str,
            &str,
            DeltaStorageLogOperation,
            i64,
        )>,
    ) -> RecordBatch {
        let mut graph_builder = PlainTermArrayElementBuilder::new();
        let mut subject_builder = PlainTermArrayElementBuilder::new();
        let mut predicate_builder = PlainTermArrayElementBuilder::new();
        let mut object_builder = PlainTermArrayElementBuilder::new();
        let mut op_builder = Int8Array::builder(rows.len());
        let mut version_builder = Int64Builder::with_capacity(rows.len());

        for (g, s, p, o, op, v) in rows {
            if let Some(g) = g {
                graph_builder.append_named_node(NamedNodeRef::new_unchecked(g));
            } else {
                graph_builder.append_null();
            }
            subject_builder.append_named_node(NamedNodeRef::new_unchecked(s));
            predicate_builder.append_named_node(NamedNodeRef::new_unchecked(p));
            object_builder.append_named_node(NamedNodeRef::new_unchecked(o));
            op_builder.append_value(op.as_stored());
            version_builder.append_value(v);
        }

        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(graph_builder.finish().into_array_ref()),
                Arc::new(subject_builder.finish().into_array_ref()),
                Arc::new(predicate_builder.finish().into_array_ref()),
                Arc::new(object_builder.finish().into_array_ref()),
                Arc::new(op_builder.finish()),
                Arc::new(version_builder.finish()),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_changeset_execution_plan_schemas() {
        let session = SessionContext::new();
        let state = session.state();
        let schema = create_operation_schema();

        let batch = create_batch(
            &schema,
            vec![
                (
                    Some("https://g1"),
                    "https://s1",
                    "https://p1",
                    "https://o1",
                    DeltaStorageLogOperation::AddQuad,
                    1,
                ),
                (
                    Some("https://g1"),
                    "https://s1",
                    "https://p1",
                    "https://o1",
                    DeltaStorageLogOperation::RemoveQuad,
                    2,
                ),
                (
                    Some("https://g2"),
                    "https://s2",
                    "https://p2",
                    "https://o2",
                    DeltaStorageLogOperation::CreateGraph,
                    3,
                ),
                (
                    Some("https://g3"),
                    "https://s3",
                    "https://p3",
                    "https://o3",
                    DeltaStorageLogOperation::DropGraph,
                    4,
                ),
                (
                    None,
                    "https://s4",
                    "https://p4",
                    "https://o4",
                    DeltaStorageLogOperation::ClearGraph,
                    5,
                ),
            ],
        );
        let stream = session
            .read_batch(batch)
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        let changeset = EagerDeltaQuadStorageChangeset::partition_operations(
            &state,
            DeltaStorageLogVersionRange::new_unchecked(0, 5),
            stream,
        )
        .await
        .unwrap();

        // Check added_quads schema
        let added_plan = changeset.added_quads(&state).await.unwrap().unwrap();
        let added_schema = added_plan.schema();
        assert!(
            !added_schema
                .field_with_name(COL_SUBJECT)
                .unwrap()
                .is_nullable(),
            "Subject should be non-nullable in added_quads"
        );
        assert!(
            !added_schema
                .field_with_name(COL_PREDICATE)
                .unwrap()
                .is_nullable(),
            "Predicate should be non-nullable in added_quads"
        );
        assert!(
            !added_schema
                .field_with_name(COL_OBJECT)
                .unwrap()
                .is_nullable(),
            "Object should be non-nullable in added_quads"
        );

        // Check removed_quads schema
        let removed_plan = changeset.removed_quads(&state).await.unwrap().unwrap();
        let removed_schema = removed_plan.schema();
        assert!(
            !removed_schema
                .field_with_name(COL_SUBJECT)
                .unwrap()
                .is_nullable(),
            "Subject should be non-nullable in removed_quads"
        );
        assert!(
            !removed_schema
                .field_with_name(COL_PREDICATE)
                .unwrap()
                .is_nullable(),
            "Predicate should be non-nullable in removed_quads"
        );
        assert!(
            !removed_schema
                .field_with_name(COL_OBJECT)
                .unwrap()
                .is_nullable(),
            "Object should be non-nullable in removed_quads"
        );

        // Check added_named_graphs schema
        let added_graphs_plan =
            changeset.added_named_graphs(&state).await.unwrap().unwrap();
        let added_graphs_schema = added_graphs_plan.schema();
        assert!(
            added_graphs_schema
                .field_with_name(COL_GRAPH)
                .unwrap()
                .is_nullable(),
            "Graph should be nullable in added_named_graphs"
        );

        // Check dropped_named_graphs schema
        let dropped_graphs_plan = changeset
            .dropped_named_graphs(&state)
            .await
            .unwrap()
            .unwrap();
        let dropped_graphs_schema = dropped_graphs_plan.schema();
        assert!(
            dropped_graphs_schema
                .field_with_name(COL_GRAPH)
                .unwrap()
                .is_nullable(),
            "Graph should be nullable in dropped_named_graphs"
        );

        // Check cleared_graphs schema (should be nullable as it can include the default graph)
        let cleared_graphs_plan =
            changeset.cleared_graphs(&state).await.unwrap().unwrap();
        let cleared_graphs_schema = cleared_graphs_plan.schema();
        assert!(
            cleared_graphs_schema
                .field_with_name(COL_GRAPH)
                .unwrap()
                .is_nullable(),
            "Graph should be nullable in cleared_graphs"
        );
    }

    async fn collect_and_format(
        state: &SessionState,
        plan: Arc<dyn ExecutionPlan>,
    ) -> String {
        let batches = collect(plan, state.task_ctx()).await.unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }
}
