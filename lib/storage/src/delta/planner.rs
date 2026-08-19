use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::objectids::EncodeAsObjectIdDeltaExec;
use crate::delta::scan_plan_builder::DeltaQuadsStorageScanPlanBuilder;
use crate::delta::snapshot::DeltaQuadsStorageSnapshot;
use async_trait::async_trait;
use datafusion::common::plan_err;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::DFResult;
use rdf_fusion_logical::encoding::object_id::EncodeAsObjectIdNode;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use std::sync::Arc;

/// A planner for converting logical quad scans into physical plans that are realized with the
/// [`DeltaQuadsStorageSnapshot`].
pub struct DeltaQuadsStoragePlanner {
    /// The storage snapshot
    snapshot: DeltaQuadsStorageSnapshot,
}

impl DeltaQuadsStoragePlanner {
    /// Creates a new [`DeltaQuadsStoragePlanner`].
    pub fn new(snapshot: DeltaQuadsStorageSnapshot) -> Self {
        Self { snapshot }
    }

    /// Implements the plan building process.
    async fn plan_scan(
        &self,
        session_state: &SessionState,
        node: &QuadPatternNode,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadsStorageError> {
        let mut builder = DeltaQuadsStorageScanPlanBuilder::new(
            session_state.clone(),
            node.quad_pattern().clone(),
            self.snapshot.encoding().clone(),
        )
        .with_cache(Arc::clone(self.snapshot.cache()))
        .with_best_quad_table(self.snapshot.quad_tables())?
        .with_changeset_for_log(self.snapshot.log(), Some(self.snapshot.version()))
        .await?
        .with_projection_indices(node.projection.clone());

        if let Some(transactional) = self.snapshot.transactional_changeset() {
            builder = builder.with_changeset(Arc::clone(transactional));
        }

        builder.build().await.map(|r| r.scan)
    }

    /// Tries to plan a [`QuadPatternNode`].
    async fn try_plan_quad_pattern_scan(
        &self,
        session_state: &SessionState,
        node: &dyn UserDefinedLogicalNode,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<QuadPatternNode>() else {
            return Ok(None);
        };

        let scan_plan = self
            .plan_scan(session_state, node)
            .await
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;

        Ok(Some(scan_plan))
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
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(planned) =
            self.try_plan_quad_pattern_scan(session_state, node).await?
        {
            return Ok(Some(planned));
        }

        if let Some(planned) = self
            .try_plan_encode_as_object_id(node, physical_inputs)
            .await?
        {
            return Ok(Some(planned));
        }

        Ok(None)
    }
}
