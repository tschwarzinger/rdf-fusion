use datafusion::arrow::array::Array;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{ColumnStatistics, Result as DFResult, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::coop::make_cooperative;
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, SchedulingType,
};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream, StatisticsArgs, collect,
};
use std::fmt::Formatter;
use std::sync::Arc;

/// The result of a quad pattern scan that has been executed eagerly during planning.
///
/// Small scans are executed while planning so that the planner has exact statistics (e.g., it can
/// distinguish between 1 and 200 matching quads) and so that they do not delay the execution of
/// the query. The original scan is kept for displaying the plan and for reporting its metrics.
#[derive(Debug)]
pub struct MaterializedQuadPatternExec {
    /// The scan that has been executed.
    source: Arc<dyn ExecutionPlan>,
    /// The results of the scan. Empty if the scan produced no rows.
    batch: Option<RecordBatch>,
    /// The statistics of the result.
    statistics: Arc<Statistics>,
    properties: Arc<PlanProperties>,
}

impl MaterializedQuadPatternExec {
    /// Executes `source` and materializes its results.
    pub async fn materialize(
        source: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> DFResult<Self> {
        let schema = source.schema();
        let batches = collect(Arc::clone(&source), context).await?;

        // Concatenating the batches also compacts them. Batches produced by a scan often reference
        // the buffers of larger (decoded) pages.
        let batch = concat_batches(&schema, &batches)?;
        let batch = (batch.num_rows() > 0).then_some(batch);
        Ok(Self::new(source, batch))
    }

    fn new(source: Arc<dyn ExecutionPlan>, batch: Option<RecordBatch>) -> Self {
        let schema = source.schema();
        let statistics = Arc::new(compute_statistics(&schema, batch.as_ref()));

        // A single partition of the source is read in order. Hence, the ordering is retained.
        let eq_properties = if source.output_partitioning().partition_count() <= 1 {
            source.equivalence_properties().clone()
        } else {
            EquivalenceProperties::new(Arc::clone(&schema))
        };
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);

        Self {
            source,
            batch,
            statistics,
            properties: Arc::new(properties),
        }
    }

    /// Returns the materialized results. `None` if the scan produced no rows.
    pub fn batch(&self) -> Option<&RecordBatch> {
        self.batch.as_ref()
    }

    /// Returns the number of materialized rows.
    pub fn num_rows(&self) -> usize {
        self.batch.as_ref().map_or(0, RecordBatch::num_rows)
    }
}

impl DisplayAs for MaterializedQuadPatternExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MaterializedQuadPatternExec: rows={}, source=[",
                    self.num_rows()
                )?;
                self.source.fmt_as(t, f)?;
                write!(f, "]")
            }
            DisplayFormatType::TreeRender => writeln!(f, "rows={}", self.num_rows()),
        }
    }
}

impl ExecutionPlan for MaterializedQuadPatternExec {
    fn name(&self) -> &str {
        "MaterializedQuadPatternExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let batches = self.batch.iter().cloned().collect();
        let stream = MemoryStream::try_new(batches, self.schema(), None)?;
        Ok(make_cooperative(Box::pin(stream)))
    }

    /// Returns the metrics of the scan that has been executed during planning.
    fn metrics(&self) -> Option<MetricsSet> {
        self.source.metrics()
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> DFResult<Arc<Statistics>> {
        Ok(Arc::clone(&self.statistics))
    }
}

/// Computes the statistics of the materialized results.
///
/// Non-empty results report inexact statistics, although they are exact. Otherwise, DataFusion's
/// `AggregateStatistics` rule replaces aggregates (e.g., `COUNT`) with plain literals that are not
/// RDF terms.
fn compute_statistics(schema: &SchemaRef, batch: Option<&RecordBatch>) -> Statistics {
    let Some(batch) = batch else {
        return Statistics::new_unknown(schema)
            .with_num_rows(Precision::Exact(0))
            .with_total_byte_size(Precision::Exact(0));
    };

    let mut statistics = Statistics::default()
        .with_num_rows(Precision::Inexact(batch.num_rows()))
        .with_total_byte_size(Precision::Inexact(batch.get_array_memory_size()));
    for column in batch.columns() {
        statistics = statistics.add_column_statistics(ColumnStatistics {
            null_count: Precision::Inexact(column.logical_null_count()),
            byte_size: Precision::Inexact(column.get_array_memory_size()),
            ..ColumnStatistics::new_unknown()
        });
    }
    statistics
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::StatisticsContext;
    use datafusion::prelude::SessionContext;

    fn source(num_rows: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let values = (0..num_rows)
            .map(|i| format!("value{i}"))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(values))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn task_ctx() -> Arc<TaskContext> {
        SessionContext::new().task_ctx()
    }

    #[tokio::test]
    async fn test_materialize() {
        let exec = MaterializedQuadPatternExec::materialize(source(3), task_ctx())
            .await
            .unwrap();
        assert_eq!(exec.num_rows(), 3);

        let stats = StatisticsContext::new()
            .compute(&exec, &StatisticsArgs::new())
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(3));

        let batches = collect(Arc::new(exec), task_ctx()).await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }

    #[tokio::test]
    async fn test_materialize_empty() {
        let exec = MaterializedQuadPatternExec::materialize(source(0), task_ctx())
            .await
            .unwrap();
        assert_eq!(exec.num_rows(), 0);
        assert!(exec.batch().is_none());

        let stats = StatisticsContext::new()
            .compute(&exec, &StatisticsArgs::new())
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(0));
    }
}
