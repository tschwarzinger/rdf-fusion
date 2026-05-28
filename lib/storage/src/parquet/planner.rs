use crate::parquet::snapshot::ParquetQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::DFResult;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use std::sync::Arc;

pub struct ParquetQuadStoragePlanner {
    snapshot: Arc<ParquetQuadStorageSnapshot>,
}

impl ParquetQuadStoragePlanner {
    pub fn new(snapshot: Arc<ParquetQuadStorageSnapshot>) -> Self {
        Self { snapshot }
    }
}
#[async_trait]
impl ExtensionPlanner for ParquetQuadStoragePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<QuadPatternNode>() else {
            return Ok(None);
        };

        Ok(Some(self.snapshot.plan_quad_pattern(
            node.quad_pattern(),
            node.schema(),
            session_state,
        )?))
    }
}
