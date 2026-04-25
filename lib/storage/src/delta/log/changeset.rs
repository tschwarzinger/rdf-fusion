use crate::delta::error::DeltaQuadStorageError;
use crate::delta::log::DeltaStorageLogVersionRange;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// A reference to a [`DeltaQuadStorageLogChangeset`].
pub type DeltaQuadStorageLogChangesetRef = Arc<dyn DeltaQuadStorageLogChangeset>;

/// Trait for a changeset between two versions of the [`DeltaStorageLog`].
///
/// This behavior is encapsulated in a trait to allow for two implementations:
/// - An eagerly compute changeset that is held in-memory and can be shared by multiple requests
/// - A lazily computed changeset that is computed on-demand and is always recomputed
///
/// The first implementation is used for "small" changesets. For such changesets, we want to
/// amortize the cost of pre-computing the changeset by sharing it for multiple consumers (e.g.,
/// index updaters, queries). However, if the changeset is huge, it can be that the available memory
/// cannot hold the entire changeset (e.g., on the initial insert of a dataset). Then, we fall back
/// to a lazily computed changeset which directly accesses the log table.
///
/// All functions return the *effective change* between two versions. For example, adding a quad and
/// removing the same quad only contains an entry in the removed quads list.
#[async_trait]
pub trait DeltaQuadStorageLogChangeset: Send + Sync {
    /// Returns the changeset as [`Any`].
    fn as_any(&self) -> &dyn std::any::Any;

    /// Returns the version range that this changeset reflects.
    fn version_range(&self) -> DeltaStorageLogVersionRange;

    /// Returns the list of cleared graphs.
    ///
    /// The data frame should have one column [`COL_GRAPH`](rdf_fusion_model::quads::COL_GRAPH).
    async fn cleared_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError>;

    /// Returns a list of removed quads.
    async fn removed_quads(
        &self,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError>;

    /// Returns a list of added quads.
    async fn added_quads(
        &self,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError>;

    /// Returns a list of (explicitly or implicitly) added named graphs.
    ///
    /// The data frame should have one column [`COL_GRAPH`](rdf_fusion_model::quads::COL_GRAPH).
    async fn added_named_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError>;

    /// Returns a list of dropped named graphs.
    ///
    /// The data frame should have one column [`COL_GRAPH`](rdf_fusion_model::quads::COL_GRAPH).
    async fn dropped_named_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError>;
}
