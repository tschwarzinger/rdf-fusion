use crate::RdfFusionContextView;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::ExtensionPlanner;
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
    async fn planners(
        &self,
        context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>>;

    /// Returns the list of named graphs in the storage.
    ///
    /// The resulting [`SendableRecordBatchStream`] must have a single column for the graphs.
    async fn named_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError>;

    /// Returns the number of quads in the storage.
    async fn len(&self, state: &SessionState) -> Result<usize, StorageError>;
}
