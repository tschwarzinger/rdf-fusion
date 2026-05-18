use crate::rdf_files::manager::RdfFileManager;
use crate::rdf_files::rdf::RdfFileSourceConfig;
use crate::rdf_files::scan::RdfFileQuadPatternScanExec;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::config::RdfFileStorageOptions;
use rdf_fusion_common::{DFResult, GraphName};
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use std::sync::Arc;

/// A planner for converting logical quad scans into physical plans that are realized with the
/// [`RdfFileQuadStorage`](crate::rdf_files::RdfFileQuadStorage).
pub struct RdfFileQuadStoragePlanner {
    manager: RdfFileManager,
    sources: Vec<(GraphName, RdfFileSourceConfig)>,
    options: RdfFileStorageOptions,
}

impl RdfFileQuadStoragePlanner {
    /// Creates a new [`RdfFileQuadStoragePlanner`].
    pub fn new(
        manager: RdfFileManager,
        sources: Vec<(GraphName, RdfFileSourceConfig)>,
        options: RdfFileStorageOptions,
    ) -> Self {
        Self {
            manager,
            sources,
            options,
        }
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

        Ok(Some(Arc::new(
            RdfFileQuadPatternScanExec::new(
                node.quad_pattern().clone(),
                self.manager.clone(),
                self.sources.clone(),
                node.storage_encoding().clone(),
                Arc::new(node.schema().as_arrow().clone()),
                session_state,
                self.options.clone(),
            )
            .await?,
        )))
    }
}

#[async_trait]
impl ExtensionPlanner for RdfFileQuadStoragePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        self.try_plan_quad_pattern_scan(session_state, node).await
    }
}
