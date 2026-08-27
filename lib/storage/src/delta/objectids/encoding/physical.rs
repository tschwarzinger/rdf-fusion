use crate::delta::objectids::DeltaObjectIdDictionary;
use crate::delta::objectids::encoding::stream::ObjectIdEncodingStream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, Partitioning, PhysicalExpr,
};
use datafusion::physical_expr_common::metrics::MetricBuilder;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::config::RdfFusionSessionConfigExt;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncodeAsObjectIdDeltaExec {
    /// The physical plan of the input DataFrame
    input: Arc<dyn ExecutionPlan>,
    /// The mapping used for encoding
    mapping: Arc<DeltaObjectIdDictionary>,
    /// The schema of the result
    output_schema: SchemaRef,
    /// The properties of the plan
    properties: Arc<PlanProperties>,
    /// Configured max buffered rows
    max_buffered_rows: Option<usize>,
    /// Configured max buffered IDs
    max_buffered_ids: Option<usize>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl EncodeAsObjectIdDeltaExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        mapping: Arc<DeltaObjectIdDictionary>,
        output_schema: SchemaRef,
    ) -> DFResult<Self> {
        let eq_properties = EquivalenceProperties::new(Arc::clone(&output_schema));
        let properties = input
            .properties()
            .as_ref()
            .clone()
            .with_eq_properties(eq_properties)
            .with_partitioning(Partitioning::UnknownPartitioning(
                input.output_partitioning().partition_count(),
            ));

        Ok(Self {
            input,
            mapping,
            output_schema,
            properties: Arc::new(properties),
            max_buffered_rows: None,
            max_buffered_ids: None,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn with_buffering_options(
        mut self,
        max_buffered_rows: Option<usize>,
        max_buffered_ids: Option<usize>,
    ) -> Self {
        self.max_buffered_rows = max_buffered_rows;
        self.max_buffered_ids = max_buffered_ids;
        self
    }
}

impl DisplayAs for EncodeAsObjectIdDeltaExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "EncodeAsObjectIdDeltaExec:")
    }
}

impl ExecutionPlan for EncodeAsObjectIdDeltaExec {
    fn name(&self) -> &str {
        "EncodeAsObjectIdDeltaExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut new_plan = Self::try_new(
            Arc::clone(&children[0]),
            Arc::clone(&self.mapping),
            Arc::clone(&self.schema()),
        )?;
        new_plan.max_buffered_rows = self.max_buffered_rows;
        new_plan.max_buffered_ids = self.max_buffered_ids;
        new_plan.metrics = self.metrics.clone();
        Ok(Arc::new(new_plan))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let options = context.session_config().rdf_fusion_options_or_from_env()?;
        let max_buffered_rows = self
            .max_buffered_rows
            .or(options.storage.delta.max_buffered_rows)
            .unwrap_or(100_000);
        let max_buffered_ids = self
            .max_buffered_ids
            .or(options.storage.delta.max_buffered_ids)
            .unwrap_or(10_000);
        let _commit_failures =
            MetricBuilder::new(&self.metrics).counter("commit_failures", partition);

        let session_ctx = SessionContext::new_with_config_rt(
            context.session_config().clone(),
            context.runtime_env(),
        );
        let session_state = session_ctx.state();

        Ok(Box::pin(ObjectIdEncodingStream::new(
            input_stream,
            Arc::clone(&self.mapping),
            max_buffered_rows,
            max_buffered_ids,
            session_state,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
