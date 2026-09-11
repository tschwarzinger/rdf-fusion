use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::buffer::BufferExec;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs, StatisticsContext};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::config::RdfFusionSessionConfigExt;
use rdf_fusion_extensions::storage::QuadStorageSnapshot;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use std::sync::Arc;

/// A generic planner that matches a [`QuadPatternNode`] against a [`QuadStorageSnapshot`].
///
/// The actual matching of a quad pattern is delegated to the snapshot's
/// [`QuadStorageSnapshot::scan_quad_pattern`] implementation. This guarantees that all quad
/// patterns of a query are matched against the same snapshot of the storage layer.
///
/// In addition, scans that are estimated to be small (below the configured buffering threshold) are
/// eagerly buffered so that they can be evaluated in parallel instead of sequentially waiting on
/// dynamic filters.
pub struct QuadPatternPlanner {
    snapshot: Arc<dyn QuadStorageSnapshot>,
}

impl QuadPatternPlanner {
    /// Creates a new [`QuadPatternPlanner`] for the given snapshot.
    pub fn new(snapshot: Arc<dyn QuadStorageSnapshot>) -> Self {
        Self { snapshot }
    }
}

#[async_trait]
impl ExtensionPlanner for QuadPatternPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<QuadPatternNode>() else {
            return Ok(None);
        };

        let session_state = session
            .as_any()
            .downcast_ref::<SessionState>()
            .expect("session must be a SessionState");

        let scan = self
            .snapshot
            .scan_quad_pattern(
                node.quad_pattern(),
                node.projection.clone(),
                session_state,
            )
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let buffering_threshold = session
            .config()
            .rdf_fusion_options_or_default()
            .execution
            .small_scan_buffering_threshold;

        let exec = if buffering_threshold > 0 {
            let stats = StatisticsContext::new()
                .compute(scan.as_ref(), &StatisticsArgs::new())?;
            let bytes = stats
                .total_byte_size
                .get_value()
                .cloned()
                .unwrap_or(usize::MAX);

            if bytes < buffering_threshold {
                Arc::new(BufferExec::new(scan, buffering_threshold))
            } else {
                scan
            }
        } else {
            scan
        };

        Ok(Some(exec))
    }
}
