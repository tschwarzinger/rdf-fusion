use crate::delta::objectids::EncodeAsObjectIdDeltaExec;
use crate::delta::snapshot::DeltaQuadsStorageSnapshot;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::plan_err;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::DFResult;
use rdf_fusion_logical::encoding::object_id::EncodeAsObjectIdNode;
use std::sync::Arc;

/// A planner for converting logical nodes that require access to the storage layer into physical
/// plans that are realized with the [`DeltaQuadsStorageSnapshot`].
pub struct DeltaQuadsStoragePlanner {
    /// The storage snapshot
    snapshot: DeltaQuadsStorageSnapshot,
}

impl DeltaQuadsStoragePlanner {
    /// Creates a new [`DeltaQuadsStoragePlanner`].
    pub fn new(snapshot: DeltaQuadsStorageSnapshot) -> Self {
        Self { snapshot }
    }

    /// Tries to plan a [`EncodeAsObjectIdNode`].
    async fn try_plan_encode_as_object_id(
        &self,
        node: &dyn UserDefinedLogicalNode,
        physical_inputs: &[Arc<dyn ExecutionPlan>],
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<EncodeAsObjectIdNode>() else {
            return Ok(None);
        };

        let Some(mapping) = self.snapshot.object_id_mapping() else {
            return plan_err!("Object ID mapping is not available for this storage");
        };

        let physical_plan = EncodeAsObjectIdDeltaExec::try_new(
            Arc::clone(&physical_inputs[0]),
            Arc::clone(mapping),
            Arc::clone(node.schema().inner()),
        )?
        .with_buffering_options(
            self.snapshot.options().max_buffered_rows,
            self.snapshot.options().max_buffered_ids,
        );

        Ok(Some(Arc::new(physical_plan)))
    }
}

#[async_trait]
impl ExtensionPlanner for DeltaQuadsStoragePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(planned) = self
            .try_plan_encode_as_object_id(node, physical_inputs)
            .await?
        {
            return Ok(Some(planned));
        }

        Ok(None)
    }
}
