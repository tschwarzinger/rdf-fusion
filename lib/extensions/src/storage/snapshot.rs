use crate::RdfFusionContextView;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::ExtensionPlanner;
use rdf_fusion_common::QuadPattern;
use rdf_fusion_common::StorageError;
use std::sync::Arc;

/// Represents a snapshot of a [`QuadStorage`](crate::storage::QuadStorage).
#[async_trait]
pub trait QuadStorageSnapshot: Send + Sync {
    /// Returns a list of planners that support planning logical nodes requiring access to the
    /// storage layer.
    ///
    /// # Consistency
    ///
    /// A query plan must often evaluate multiple quad patterns that have access to the same
    /// storage. It is the responsibility of the storage layer to ensure that the quad patterns use
    /// the same snapshot of the storage layer.
    ///
    /// # Default
    ///
    /// The default implementation returns an empty list of planners. Storage layers can override
    /// this to provide storage-specific planners. Planners returned here are registered *before*
    /// the generic quad pattern planner so that they are given priority when planning quad patterns.
    async fn planners(
        &self,
        _context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        vec![]
    }

    /// Matches a quad pattern against this snapshot and returns a physical plan over the matching
    /// quads.
    async fn scan_quad_pattern(
        &self,
        pattern: &QuadPattern,
        projection: Option<Vec<usize>>,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError>;

    /// Returns the list of named graphs in the storage.
    ///
    /// The resulting [`ExecutionPlan`]'s schema must have a single column for the graphs.
    async fn named_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError>;

    /// Returns the number of quads in the storage.
    async fn len(&self, state: &SessionState) -> Result<usize, StorageError>;
}
