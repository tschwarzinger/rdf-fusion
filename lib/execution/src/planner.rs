use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{
    DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner,
};
use rdf_fusion_common::{DFResult, StorageError};
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::{QuadStorage, QuadStorageSnapshot};
use rdf_fusion_physical::paths::KleenePlusPathPlanner;
use rdf_fusion_storage::rdf_files::RdfFilePlanner;
use std::fmt::Debug;
use std::sync::Arc;

/// A planner that uses the quad storage for planning quad scans.
pub struct RdfFusionPlanner {
    /// The RdfFusion context.
    context: RdfFusionContextView,
    /// The storage that is used to execute the query.
    snapshot: UsedSnapshot,
}

impl RdfFusionPlanner {
    /// Creates a new [`RdfFusionPlanner`] that will obtain a new snapshot from the underlying
    /// quad storage.
    pub fn new(context: RdfFusionContextView, storage: Arc<dyn QuadStorage>) -> Self {
        Self {
            context,
            snapshot: UsedSnapshot::Dynamic(storage),
        }
    }

    /// Creates a new [`RdfFusionPlanner`] that always queries the given snapshot.
    pub fn new_with_snapshot(
        context: RdfFusionContextView,
        snapshot: Arc<dyn QuadStorageSnapshot>,
    ) -> Self {
        Self {
            context,
            snapshot: UsedSnapshot::Static(snapshot),
        }
    }
}

impl Debug for RdfFusionPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RdfFusionPlanner")
    }
}

#[async_trait]
impl QueryPlanner for RdfFusionPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let snapshot = self
            .snapshot
            .get_snapshot()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let mut planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(KleenePlusPathPlanner), Arc::new(RdfFilePlanner)];
        planners.extend(snapshot.planners(&self.context).await);

        let planner = DefaultPhysicalPlanner::with_extension_planners(planners);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Stores the type of quad storage snapshot that is used by the [`RdfFusionPlanner`].
pub enum UsedSnapshot {
    /// The snapshot is re-created for each new query.
    Dynamic(Arc<dyn QuadStorage>),
    /// A static snapshot is used.
    Static(Arc<dyn QuadStorageSnapshot>),
}

impl UsedSnapshot {
    /// Gets a snapshot, either using the pre-determined one or creating one from the underlying
    /// storage layer.
    pub async fn get_snapshot(
        &self,
    ) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        match self {
            UsedSnapshot::Dynamic(storage) => storage.snapshot().await,
            UsedSnapshot::Static(snapshot) => Ok(Arc::clone(snapshot)),
        }
    }
}
