use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
#[allow(clippy::struct_field_names)]
pub struct QueryExplanation {
    /// The latency of the query planning. This can differ from the planning time which is th.
    pub planning_latency: std::time::Duration,
    /// The compute time it took to compute the query plan.
    pub planning_compute: std::time::Duration,
    /// The initial logical plan created from the SPARQL query.
    pub initial_logical_plan: LogicalPlan,
    /// The optimized logical plan.
    pub optimized_logical_plan: LogicalPlan,
    /// A reference to the root node of the plan that was actually executed.
    pub execution_plan: Arc<dyn ExecutionPlan>,
}
