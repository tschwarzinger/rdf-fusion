use crate::rdf_files::rdf::RdfParserExec;
use crate::rdf_files::rdf::logical_node::ParseRdfFileNode;
use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::DFResult;
use std::sync::Arc;

pub struct RdfFilePlanner;

#[async_trait]
impl ExtensionPlanner for RdfFilePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<ParseRdfFileNode>() else {
            return Ok(None);
        };

        let reader = node.source.stream(session_state).await?;
        let parser = node.options.create_parser(reader);

        Ok(Some(Arc::new(RdfParserExec::new(
            parser,
            Arc::clone(node.schema.inner()),
        ))))
    }
}
