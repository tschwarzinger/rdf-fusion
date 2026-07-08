use crate::paths::kleene_plus::KleenePlusClosureExec;
use async_trait::async_trait;
use datafusion::common::plan_err;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_logical::paths::KleenePlusClosureNode;
use std::sync::Arc;

/// Planner for KleenePlusPath nodes
pub struct KleenePlusPathPlanner;

impl Default for KleenePlusPathPlanner {
    fn default() -> Self {
        Self
    }
}

#[async_trait]
impl ExtensionPlanner for KleenePlusPathPlanner {
    /// Converts a logical KleenePlusPath node into its physical execution plan
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<KleenePlusClosureNode>() else {
            return Ok(None);
        };

        if logical_inputs.len() != 1 || physical_inputs.len() != 1 {
            return plan_err!("KleenePlusPath node must have exactly one input");
        }

        let physical_plan = KleenePlusClosureExec::try_new(
            Arc::clone(&physical_inputs[0]),
            !node.disallow_cross_graph_paths(),
        )?;

        Ok(Some(Arc::new(physical_plan)))
    }
}
