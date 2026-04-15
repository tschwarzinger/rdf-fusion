use crate::delta::log::{COL_COMMIT_VERSION, COL_OPERATION, DeltaStorageLogOperation};
use datafusion::arrow::array::{ArrayRef, Int8Builder, RecordBatch};
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::row::{Row, RowConverter, SortField};
use datafusion::common::{DataFusionError, exec_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexRequirement, OrderingRequirements,
    PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, Partitioning,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use deltalake::arrow::array::Int8Array;
use futures::{Stream, StreamExt, ready};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::any::Any;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Computes the net changeset from a CDF stream.
#[derive(Debug)]
pub struct ComputeLogChangesetExec {
    plan_properties: Arc<PlanProperties>,
    inner: Arc<dyn ExecutionPlan>,
}

impl ComputeLogChangesetExec {
    /// Creates a new [`ComputeLogChangesetExec`] validating the inner execution plan.
    pub fn try_new(inner: Arc<dyn ExecutionPlan>) -> DFResult<ComputeLogChangesetExec> {
        if inner.output_partitioning().partition_count() != 1 {
            return exec_err!("CDF stream must have a single partition.");
        }

        let inner_schema = inner.schema();
        validate_schema(inner_schema.as_ref())?;

        return Ok(ComputeLogChangesetExec {
            plan_properties: Arc::new(compute_properties(inner_schema.as_ref())),
            inner,
        });

        /// Validates that all necessary columns exist in the input schema.
        fn validate_schema(inner_schema: &Schema) -> DFResult<()> {
            let required_cols = [
                COL_GRAPH,
                COL_SUBJECT,
                COL_PREDICATE,
                COL_OBJECT,
                COL_OPERATION,
                COL_COMMIT_VERSION,
            ];

            for col in required_cols {
                if inner_schema.field_with_name(col).is_err() {
                    return exec_err!("Missing required column '{col}' in CDF stream");
                }
            }

            Ok(())
        }

        /// Computes the output schema and plan properties for the execution plan.
        fn compute_properties(inner_schema: &Schema) -> PlanProperties {
            let output_schema = Arc::new(Schema::new(vec![
                inner_schema
                    .field_with_name(COL_OPERATION)
                    .expect("validated schema")
                    .clone(),
                inner_schema
                    .field_with_name(COL_GRAPH)
                    .expect("validated schema")
                    .clone(),
                inner_schema
                    .field_with_name(COL_SUBJECT)
                    .expect("validated schema")
                    .clone(),
                inner_schema
                    .field_with_name(COL_PREDICATE)
                    .expect("validated schema")
                    .clone(),
                inner_schema
                    .field_with_name(COL_OBJECT)
                    .expect("validated schema")
                    .clone(),
            ]));

            let sort_expr = PhysicalSortExpr {
                expr: col(COL_OPERATION, output_schema.as_ref()).expect("Column exists"),
                options: SortOptions::default().asc(),
            };

            let mut equiv_props = EquivalenceProperties::new(Arc::clone(&output_schema));
            equiv_props.add_ordering([sort_expr]);

            PlanProperties::new(
                equiv_props,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )
        }
    }
}

impl ExecutionPlan for ComputeLogChangesetExec {
    fn name(&self) -> &str {
        "ComputeLogChangesetExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.plan_properties.eq_properties.schema())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let commit_version =
            col(COL_COMMIT_VERSION, self.inner.schema().as_ref()).expect("Column exists");
        let sort_expr = PhysicalSortRequirement::new(
            commit_version,
            Some(SortOptions::default().desc()),
        );
        let required_ordering =
            LexRequirement::new([sort_expr]).expect("Ordering not empty");
        vec![Some(OrderingRequirements::Hard(vec![required_ordering]))]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return exec_err!("ComputeLogChangesetExec must have exactly one child");
        }
        Ok(Arc::new(ComputeLogChangesetExec::try_new(Arc::clone(
            &children[0],
        ))?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!(
                "ComputeLogChangesetExec can only be executed on partition 0"
            );
        }

        let target_batch_size = context.session_config().batch_size();
        let inner_stream = self.inner.execute(partition, context)?;

        // Setup row converter for the quad columns
        let schema = self.inner.schema();
        let row_converter = RowConverter::new(vec![
            SortField::new(schema.field_with_name(COL_GRAPH)?.data_type().clone()),
            SortField::new(schema.field_with_name(COL_SUBJECT)?.data_type().clone()),
            SortField::new(schema.field_with_name(COL_PREDICATE)?.data_type().clone()),
            SortField::new(schema.field_with_name(COL_OBJECT)?.data_type().clone()),
        ])?;

        let graph_converter = RowConverter::new(vec![SortField::new(
            schema.field_with_name(COL_GRAPH)?.data_type().clone(),
        )])?;

        Ok(Box::pin(ComputeLogChangesetStream {
            inner: inner_stream,
            schema: self.schema(),
            row_converter,
            graph_converter,
            batch_coalescer: BatchCoalescer::new(self.schema(), target_batch_size),
            state: BTreeMap::new(),
            global_cleared: None,
            cleared_graphs: HashSet::new(),
            dropped_graphs: HashSet::default(),
            finished: false,
        }))
    }
}

impl DisplayAs for ComputeLogChangesetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ComputeLogChangesetExec")
    }
}

/// Implements the aggregation of the states. For each quad, the stream decides whether the last
/// operation is an insertion or removal.
struct ComputeLogChangesetStream {
    /// The inner stream of incoming batches.
    inner: SendableRecordBatchStream,
    /// The output schema of the stream.
    schema: SchemaRef,
    /// The row converter used to convert the quad columns into byte arrays.
    row_converter: RowConverter,
    /// The row converter used to convert only the graph column into byte arrays.
    graph_converter: RowConverter,
    /// The batch coalescer used to split the result into multiple batches.
    batch_coalescer: BatchCoalescer,
    /// Mapping of the converted quads/graph-ops to their final operation.
    state: BTreeMap<Box<[u8]>, i8>,
    /// Whether a global CLEAR has been encountered (indicated by [`Some`]). The row will contain
    /// all nulls, but it is necessary to use the [`RowConverter`].
    global_cleared: Option<Box<[u8]>>,
    /// Set of graphs that have been cleared.
    cleared_graphs: HashSet<Box<[u8]>>,
    /// Set of graphs that have been dropped.
    dropped_graphs: HashSet<Box<[u8]>>,
    /// Whether the stream has finished.
    finished: bool,
}

impl ComputeLogChangesetStream {
    /// Egests a single incoming record batch and updates the internal changeset state.
    fn process_batch(&mut self, batch: &RecordBatch) -> DFResult<()> {
        // Extract and downcast the operational columns
        let operations = get_downcast_array::<Int8Array>(batch, COL_OPERATION)?;

        let quad_rows = self.row_converter.convert_columns(&[
            Arc::clone(get_array(batch, COL_GRAPH)?),
            Arc::clone(get_array(batch, COL_SUBJECT)?),
            Arc::clone(get_array(batch, COL_PREDICATE)?),
            Arc::clone(get_array(batch, COL_OBJECT)?),
        ])?;

        let graph_rows = self
            .graph_converter
            .convert_columns(&[Arc::clone(get_array(batch, COL_GRAPH)?)])?;

        // Update the state
        for i in 0..batch.num_rows() {
            let op_val = operations.value(i);
            let operation =
                DeltaStorageLogOperation::from_stored(op_val).expect("stored op valid");

            let quad_row = quad_rows.row(i);
            let graph_row = graph_rows.row(i);

            match operation {
                DeltaStorageLogOperation::ClearDatabase => {
                    self.global_cleared = Some(quad_row.as_ref().into());
                    break;
                }
                DeltaStorageLogOperation::DropGraph => {
                    if !self.dropped_graphs.contains(graph_row.as_ref()) {
                        let mut key = quad_row.as_ref().to_vec();
                        key.push(op_val as u8);
                        self.state.insert(key.into_boxed_slice(), op_val);
                        self.dropped_graphs.insert(graph_row.as_ref().into());
                    }
                }
                DeltaStorageLogOperation::ClearGraph => {
                    if !self.cleared_graphs.contains(graph_row.as_ref())
                        && !self.dropped_graphs.contains(graph_row.as_ref())
                    {
                        let mut key = quad_row.as_ref().to_vec();
                        key.push(op_val as u8);
                        self.state.insert(key.into_boxed_slice(), op_val);
                        self.cleared_graphs.insert(graph_row.as_ref().into());
                    }
                }
                DeltaStorageLogOperation::CreateGraph => {
                    if !self.dropped_graphs.contains(graph_row.as_ref()) {
                        let mut key = quad_row.as_ref().to_vec();
                        key.push(op_val as u8);
                        self.state.insert(key.into_boxed_slice(), op_val);
                    }
                }
                DeltaStorageLogOperation::AddQuad => {
                    self.add_row_if_graph_not_cleared(op_val, quad_row, graph_row);
                    // TODO: Handle add quads that have created a new graph, which was cleared
                    // later.
                }
                DeltaStorageLogOperation::RemoveQuad => {
                    self.add_row_if_graph_not_cleared(op_val, quad_row, graph_row);
                }
            }
        }

        return Ok(());

        /// Helper to safely extract an array by name.
        fn get_array<'a>(
            batch: &'a RecordBatch,
            col_name: &str,
        ) -> DFResult<&'a ArrayRef> {
            batch.column_by_name(col_name).ok_or_else(|| {
                DataFusionError::Execution(format!("Missing column: {col_name}"))
            })
        }

        /// Helper to safely extract and downcast an array to a specific Arrow type.
        fn get_downcast_array<'a, T: 'static>(
            batch: &'a RecordBatch,
            col_name: &str,
        ) -> DFResult<&'a T> {
            let array = get_array(batch, col_name)?;
            array.as_any().downcast_ref::<T>().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Column '{col_name}' has unexpected type, failed to downcast to {}",
                    std::any::type_name::<T>()
                ))
            })
        }
    }

    fn add_row_if_graph_not_cleared(
        &mut self,
        op_val: i8,
        quad_row: Row,
        graph_row: Row,
    ) {
        if !self.cleared_graphs.contains(graph_row.as_ref())
            && !self.dropped_graphs.contains(graph_row.as_ref())
        {
            let quad_bytes: Box<[u8]> = quad_row.as_ref().into();
            match self.state.entry(quad_bytes) {
                Entry::Occupied(_) => {
                    // Ignore. The action already encountered happened later and thus
                    // overrules any older action.
                }
                Entry::Vacant(vac) => {
                    vac.insert(op_val);
                }
            }
        }
    }

    /// Consumes the final state map and materializes it into a single RecordBatch.
    fn push_to_batch_coalescer(&mut self) -> DFResult<()> {
        let state = std::mem::take(&mut self.state);

        let mut rows_to_convert = Vec::with_capacity(state.len());
        let mut operation_builder = Int8Builder::with_capacity(state.len());

        if let Some(row) = &self.global_cleared {
            rows_to_convert.push(row.clone());
            operation_builder
                .append_value(DeltaStorageLogOperation::ClearDatabase.as_stored());
        }

        for (row_bytes, op_val) in state {
            let operation =
                DeltaStorageLogOperation::from_stored(op_val).expect("valid op");
            let row_bytes =
                if operation.is_graph_operation() || operation.is_global_operation() {
                    row_bytes[..row_bytes.len() - 1].into()
                } else {
                    row_bytes
                };
            rows_to_convert.push(row_bytes);
            operation_builder.append_value(op_val);
        }

        // Parse bytes back into Arrow format
        let parser = self.row_converter.parser();
        let final_rows = rows_to_convert
            .iter()
            .map(|bytes| parser.parse(bytes.as_ref()));
        let mut arrays = self.row_converter.convert_rows(final_rows)?;

        // Attach the operation column
        arrays.insert(0, Arc::new(operation_builder.finish()) as ArrayRef);

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)?;
        self.batch_coalescer.push_batch(batch)?;
        self.batch_coalescer.finish_buffered_batch()?;

        Ok(())
    }
}

impl Stream for ComputeLogChangesetStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return if self.batch_coalescer.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(self.batch_coalescer.next_completed_batch().map(Ok))
            };
        }

        loop {
            match ready!(self.inner.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if let Err(e) = self.process_batch(&batch) {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    self.finished = true;
                    self.push_to_batch_coalescer()?;
                    return Poll::Ready(
                        self.batch_coalescer.next_completed_batch().map(Ok),
                    );
                }
            }
        }
    }
}

impl RecordBatchStream for ComputeLogChangesetStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Builder;
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::physical_expr::LexOrdering;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;
    use deltalake::arrow::util::pretty::pretty_format_batches;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::plain_term::{
        PLAIN_TERM_ENCODING, PlainTermArrayElementBuilder,
    };
    use rdf_fusion_encoding::{EncodingArray, TermEncoding};
    use rdf_fusion_model::NamedNodeRef;

    #[tokio::test]
    async fn test_compute_change_log_adding_and_removing() {
        let mut builder = TestChangesetBuilder::new();
        builder.add_quad(
            None,
            "https://s1",
            "https://p1",
            "https://o1",
            DeltaStorageLogOperation::AddQuad,
            1,
        );
        builder.add_quad(
            None,
            "https://s1",
            "https://p1",
            "https://o1",
            DeltaStorageLogOperation::RemoveQuad,
            2,
        );

        builder.add_quad(
            None,
            "https://s2",
            "https://p2",
            "https://o2",
            DeltaStorageLogOperation::RemoveQuad,
            3,
        );
        builder.add_quad(
            None,
            "https://s2",
            "https://p2",
            "https://o2",
            DeltaStorageLogOperation::AddQuad,
            4,
        );

        builder.add_quad(
            None,
            "https://s3",
            "https://p3",
            "https://o3",
            DeltaStorageLogOperation::AddQuad,
            5,
        );

        let result = builder.execute().await;
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+-------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | operation | graph | subject                                                        | predicate                                                      | object                                                         |
        +-----------+-------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | 20        |       | {term_type: 0, value: https://s1, data_type: , language_tag: } | {term_type: 0, value: https://p1, data_type: , language_tag: } | {term_type: 0, value: https://o1, data_type: , language_tag: } |
        | 21        |       | {term_type: 0, value: https://s2, data_type: , language_tag: } | {term_type: 0, value: https://p2, data_type: , language_tag: } | {term_type: 0, value: https://o2, data_type: , language_tag: } |
        | 21        |       | {term_type: 0, value: https://s3, data_type: , language_tag: } | {term_type: 0, value: https://p3, data_type: , language_tag: } | {term_type: 0, value: https://o3, data_type: , language_tag: } |
        +-----------+-------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_compute_change_log_clear_barrier() {
        let mut builder = TestChangesetBuilder::new();
        builder.add_quad(
            None,
            "https://s1",
            "https://p1",
            "https://o1",
            DeltaStorageLogOperation::AddQuad,
            1,
        );

        builder.add_clear(2);

        builder.add_quad(
            None,
            "https://s2",
            "https://p2",
            "https://o2",
            DeltaStorageLogOperation::AddQuad,
            3,
        );

        let result = builder.execute().await;
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+-------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | operation | graph | subject                                                        | predicate                                                      | object                                                         |
        +-----------+-------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | 0         |       |                                                                |                                                                |                                                                |
        | 21        |       | {term_type: 0, value: https://s2, data_type: , language_tag: } | {term_type: 0, value: https://p2, data_type: , language_tag: } | {term_type: 0, value: https://o2, data_type: , language_tag: } |
        +-----------+-------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_compute_change_log_clear_graph_barrier() {
        let mut builder = TestChangesetBuilder::new();
        builder.add_quad(
            Some("https://gA"),
            "https://s1",
            "https://p1",
            "https://o1",
            DeltaStorageLogOperation::AddQuad,
            1,
        );
        builder.add_quad(
            Some("https://gB"),
            "https://s3",
            "https://p3",
            "https://o3",
            DeltaStorageLogOperation::AddQuad,
            1,
        );

        builder.add_clear_graph("https://gA", 2);
        builder.add_quad(
            Some("https://gA"),
            "https://s2",
            "https://p2",
            "https://o2",
            DeltaStorageLogOperation::AddQuad,
            3,
        );

        let result = builder.execute().await;
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | operation | graph                                                          | subject                                                        | predicate                                                      | object                                                         |
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | 11        | {term_type: 0, value: https://gA, data_type: , language_tag: } |                                                                |                                                                |                                                                |
        | 21        | {term_type: 0, value: https://gA, data_type: , language_tag: } | {term_type: 0, value: https://s2, data_type: , language_tag: } | {term_type: 0, value: https://p2, data_type: , language_tag: } | {term_type: 0, value: https://o2, data_type: , language_tag: } |
        | 21        | {term_type: 0, value: https://gB, data_type: , language_tag: } | {term_type: 0, value: https://s3, data_type: , language_tag: } | {term_type: 0, value: https://p3, data_type: , language_tag: } | {term_type: 0, value: https://o3, data_type: , language_tag: } |
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_compute_change_log_drop_create_graph() {
        let mut builder = TestChangesetBuilder::new();
        builder.add_create_graph("https://g1", 1);
        builder.add_quad(
            Some("https://g1"),
            "https://s1",
            "https://p1",
            "https://o1",
            DeltaStorageLogOperation::AddQuad,
            2,
        );
        builder.add_drop_graph("https://g1", 3);
        builder.add_create_graph("https://g1", 4);
        builder.add_quad(
            Some("https://g1"),
            "https://s1",
            "https://p1",
            "https://o1",
            DeltaStorageLogOperation::AddQuad,
            5,
        );

        let result = builder.execute().await;
        // Result: Drop(g1), Create(g1), Add(s2)
        // v1-v2 are wiped by v3 Drop.
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | operation | graph                                                          | subject                                                        | predicate                                                      | object                                                         |
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | 10        | {term_type: 0, value: https://g1, data_type: , language_tag: } |                                                                |                                                                |                                                                |
        | 12        | {term_type: 0, value: https://g1, data_type: , language_tag: } |                                                                |                                                                |                                                                |
        | 21        | {term_type: 0, value: https://g1, data_type: , language_tag: } | {term_type: 0, value: https://s1, data_type: , language_tag: } | {term_type: 0, value: https://p1, data_type: , language_tag: } | {term_type: 0, value: https://o1, data_type: , language_tag: } |
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_compute_change_log_drop_graph_barrier_different_quad() {
        let mut builder = TestChangesetBuilder::new();
        builder.add_quad(
            Some("https://g1"),
            "https://s1",
            "https://p1",
            "https://o1",
            DeltaStorageLogOperation::AddQuad,
            1,
        );
        builder.add_drop_graph("https://g1", 2);
        builder.add_quad(
            Some("https://g1"),
            "https://s2",
            "https://p2",
            "https://o2",
            DeltaStorageLogOperation::AddQuad,
            3,
        );

        let result = builder.execute().await;
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | operation | graph                                                          | subject                                                        | predicate                                                      | object                                                         |
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | 10        | {term_type: 0, value: https://g1, data_type: , language_tag: } |                                                                |                                                                |                                                                |
        | 21        | {term_type: 0, value: https://g1, data_type: , language_tag: } | {term_type: 0, value: https://s2, data_type: , language_tag: } | {term_type: 0, value: https://p2, data_type: , language_tag: } | {term_type: 0, value: https://o2, data_type: , language_tag: } |
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_compute_change_log_drop_graph_barrier_clear_graph() {
        let mut builder = TestChangesetBuilder::new();
        builder.add_clear_graph("https://g1", 1);
        builder.add_drop_graph("https://g1", 2);

        let result = builder.execute().await;
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        | operation | graph                                                          | subject | predicate | object |
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        | 10        | {term_type: 0, value: https://g1, data_type: , language_tag: } |         |           |        |
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        ");
    }

    #[tokio::test]
    async fn test_compute_change_log_drop_graph_barrier_create_graph() {
        let mut builder = TestChangesetBuilder::new();
        builder.add_create_graph("https://g1", 1);
        builder.add_drop_graph("https://g1", 2);

        let result = builder.execute().await;
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        | operation | graph                                                          | subject | predicate | object |
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        | 10        | {term_type: 0, value: https://g1, data_type: , language_tag: } |         |           |        |
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        ");
    }

    #[tokio::test]
    async fn test_compute_change_log_clear_graph_still_shows_create_graph() {
        let mut builder = TestChangesetBuilder::new();
        builder.add_create_graph("https://g1", 1);
        builder.add_clear_graph("https://g1", 2);

        let result = builder.execute().await;
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        | operation | graph                                                          | subject | predicate | object |
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        | 11        | {term_type: 0, value: https://g1, data_type: , language_tag: } |         |           |        |
        | 12        | {term_type: 0, value: https://g1, data_type: , language_tag: } |         |           |        |
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        ");
    }

    #[tokio::test]
    async fn test_compute_change_log_clear_graph_barrier_quads() {
        let mut builder = TestChangesetBuilder::new();
        builder.add_quad(
            Some("https://g1"),
            "https://s1",
            "https://p1",
            "https://o1",
            DeltaStorageLogOperation::AddQuad,
            1,
        );
        builder.add_clear_graph("https://g1", 2);
        builder.add_quad(
            Some("https://g1"),
            "https://s2",
            "https://p2",
            "https://o2",
            DeltaStorageLogOperation::AddQuad,
            3,
        );

        let result = builder.execute().await;
        // Result: Clear(g1) and Add(s2). s1 should be wiped.
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | operation | graph                                                          | subject                                                        | predicate                                                      | object                                                         |
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        | 11        | {term_type: 0, value: https://g1, data_type: , language_tag: } |                                                                |                                                                |                                                                |
        | 21        | {term_type: 0, value: https://g1, data_type: , language_tag: } | {term_type: 0, value: https://s2, data_type: , language_tag: } | {term_type: 0, value: https://p2, data_type: , language_tag: } | {term_type: 0, value: https://o2, data_type: , language_tag: } |
        +-----------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+----------------------------------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_compute_change_log_clear_all_barrier_graph_ops() {
        let mut builder = TestChangesetBuilder::new();
        // v1: Create G1
        builder.add_create_graph("https://g1", 1);
        // v2: ClearAll
        builder.add_clear(2);
        // v3: Create G2
        builder.add_create_graph("https://g2", 3);

        let result = builder.execute().await;
        // Result: ClearAll and Create(g2). Create(g1) should be wiped.
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        | operation | graph                                                          | subject | predicate | object |
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        | 0         |                                                                |         |           |        |
        | 12        | {term_type: 0, value: https://g2, data_type: , language_tag: } |         |           |        |
        +-----------+----------------------------------------------------------------+---------+-----------+--------+
        ");
    }

    #[tokio::test]
    async fn test_compute_change_log_clear_all_as_last_operation() {
        let mut builder = TestChangesetBuilder::new();
        builder.add_create_graph("https://g1", 1);
        builder.add_clear(2);

        let result = builder.execute().await;
        assert_snapshot!(pretty_format_batches(&[result]).unwrap(), @"
        +-----------+-------+---------+-----------+--------+
        | operation | graph | subject | predicate | object |
        +-----------+-------+---------+-----------+--------+
        | 0         |       |         |           |        |
        +-----------+-------+---------+-----------+--------+
        ");
    }

    struct TestChangesetBuilder {
        schema: SchemaRef,
        rows: Vec<(
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            i8,
            i64,
        )>,
    }

    impl TestChangesetBuilder {
        fn new() -> Self {
            let data_type = PLAIN_TERM_ENCODING.data_type().clone();
            let schema = Arc::new(Schema::new(vec![
                Field::new(COL_GRAPH, data_type.clone(), true),
                Field::new(COL_SUBJECT, data_type.clone(), true),
                Field::new(COL_PREDICATE, data_type.clone(), true),
                Field::new(COL_OBJECT, data_type, true),
                Field::new(COL_OPERATION, DataType::Int8, false),
                Field::new(COL_COMMIT_VERSION, DataType::Int64, false),
            ]));
            Self {
                schema,
                rows: Vec::new(),
            }
        }

        fn add_quad(
            &mut self,
            g: Option<&str>,
            s: &str,
            p: &str,
            o: &str,
            op: DeltaStorageLogOperation,
            v: i64,
        ) {
            self.rows.push((
                g.map(|s| s.to_string()),
                Some(s.to_string()),
                Some(p.to_string()),
                Some(o.to_string()),
                op.as_stored(),
                v,
            ));
        }

        fn add_clear(&mut self, v: i64) {
            self.rows.push((
                None,
                None,
                None,
                None,
                DeltaStorageLogOperation::ClearDatabase.as_stored(),
                v,
            ));
        }

        fn add_clear_graph(&mut self, g: &str, v: i64) {
            self.rows.push((
                Some(g.to_string()),
                None,
                None,
                None,
                DeltaStorageLogOperation::ClearGraph.as_stored(),
                v,
            ));
        }

        fn add_drop_graph(&mut self, g: &str, v: i64) {
            self.rows.push((
                Some(g.to_string()),
                None,
                None,
                None,
                DeltaStorageLogOperation::DropGraph.as_stored(),
                v,
            ));
        }

        fn add_create_graph(&mut self, g: &str, v: i64) {
            self.rows.push((
                Some(g.to_string()),
                None,
                None,
                None,
                DeltaStorageLogOperation::CreateGraph.as_stored(),
                v,
            ));
        }

        async fn execute(mut self) -> RecordBatch {
            // Sort rows by version DESC to simulate what CDF scan + SortExec does
            self.rows.sort_by(|a, b| b.5.cmp(&a.5));

            let mut graph_builder = PlainTermArrayElementBuilder::new();
            let mut subject_builder = PlainTermArrayElementBuilder::new();
            let mut predicate_builder = PlainTermArrayElementBuilder::new();
            let mut object_builder = PlainTermArrayElementBuilder::new();
            let mut op_builder = Int8Builder::with_capacity(self.rows.len());
            let mut version_builder = Int64Builder::with_capacity(self.rows.len());

            for (g, s, p, o, op, v) in self.rows {
                if let Some(g) = g {
                    graph_builder.append_named_node(NamedNodeRef::new_unchecked(&g));
                } else {
                    graph_builder.append_null();
                }
                if let Some(s) = s {
                    subject_builder.append_named_node(NamedNodeRef::new_unchecked(&s));
                } else {
                    subject_builder.append_null();
                }
                if let Some(p) = p {
                    predicate_builder.append_named_node(NamedNodeRef::new_unchecked(&p));
                } else {
                    predicate_builder.append_null();
                }
                if let Some(o) = o {
                    object_builder.append_named_node(NamedNodeRef::new_unchecked(&o));
                } else {
                    object_builder.append_null();
                }
                op_builder.append_value(op);
                version_builder.append_value(v);
            }

            let batch = RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![
                    Arc::new(graph_builder.finish().into_array_ref()),
                    Arc::new(subject_builder.finish().into_array_ref()),
                    Arc::new(predicate_builder.finish().into_array_ref()),
                    Arc::new(object_builder.finish().into_array_ref()),
                    Arc::new(op_builder.finish()),
                    Arc::new(version_builder.finish()),
                ],
            )
            .unwrap();

            let ctx = SessionContext::new();
            let df = ctx.read_batch(batch).unwrap();
            let exec = df.create_physical_plan().await.unwrap();

            let single_partition_exec =
                if exec.output_partitioning().partition_count() > 1 {
                    Arc::new(CoalescePartitionsExec::new(exec)) as Arc<dyn ExecutionPlan>
                } else {
                    exec
                };

            // Add the sort ordering property
            let commit_version_col =
                col(COL_COMMIT_VERSION, self.schema.as_ref()).unwrap();
            let sort_expr =
                PhysicalSortExpr::new(commit_version_col, SortOptions::default().desc());
            let sort_exec = Arc::new(SortExec::new(
                LexOrdering::new(vec![sort_expr]).unwrap(),
                single_partition_exec,
            ));

            let compute_exec: Arc<dyn ExecutionPlan> =
                Arc::new(ComputeLogChangesetExec::try_new(sort_exec).unwrap());
            let results = collect(Arc::clone(&compute_exec), ctx.task_ctx())
                .await
                .unwrap();

            concat_batches(&compute_exec.schema(), &results).unwrap()
        }
    }
}
