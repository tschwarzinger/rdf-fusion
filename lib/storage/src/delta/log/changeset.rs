use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::log::{DeltaStorageLogVersionRange, EagerChangeset};
use crate::index::IndexComponents;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct ChangesetContext {
    pub intended_sort_order: Option<IndexComponents>,
}

/// A reference to a [`DeltaQuadsStorageLogChangeset`].
pub type DeltaQuadsStorageLogChangesetRef = Arc<dyn DeltaQuadsStorageLogChangeset>;

/// Trait for a changeset between two versions of the [`DeltaQuadsStorageLog`].
///
/// This behavior is encapsulated in a trait to allow for two implementations:
/// - [`EagerChangeset`]: An eagerly compute changeset that is held in-memory and can be shared by
///   multiple requests.
/// - [`LazyInsertionOnlyChangeset`]: A lazily computed changeset that is computed on-demand and is
/// always recomputed. This changeset only supports transactions that only insert
/// quads (e.g., bulk insertions).
///
/// The first implementation is used for "small" changesets. For such changesets, we want to
/// amortize the cost of pre-computing the changeset by sharing it for multiple consumers (e.g.,
/// index updaters, queries). However, if the changeset is huge, it can be that the available memory
/// cannot hold the entire changeset (e.g., on the initial insert of a dataset). Then, if possible,
/// we fall back to a lazily computed changeset which directly accesses the log table.
///
/// All functions return the *effective change* between two versions. For example, adding a quad and
/// removing the same quad only contains an entry in the removed quads list.
///
/// [`DeltaQuadsStorageLog`]: crate::delta::log::DeltaQuadsStorageLog
/// [`LazyInsertionOnlyChangeset`]: crate::delta::log::LazyInsertionOnlyChangeset
#[async_trait]
pub trait DeltaQuadsStorageLogChangeset: Send + Sync {
    /// Returns the version range that this changeset reflects.
    fn version_range(&self) -> DeltaStorageLogVersionRange;

    /// Returns the list of cleared graphs.
    ///
    /// The data frame should have one column [`COL_GRAPH`](rdf_fusion_common::quads::COL_GRAPH).
    async fn cleared_graphs(
        &self,
        context: &ChangesetContext,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError>;

    /// Returns a list of removed quads.
    async fn removed_quads(
        &self,
        context: &ChangesetContext,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError>;

    /// Returns a list of added quads.
    async fn added_quads(
        &self,
        context: &ChangesetContext,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError>;

    /// Returns a list of (explicitly or implicitly) added named graphs.
    ///
    /// The data frame should have one column [`COL_GRAPH`](rdf_fusion_common::quads::COL_GRAPH).
    async fn added_named_graphs(
        &self,
        context: &ChangesetContext,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError>;

    /// Returns a list of dropped named graphs.
    ///
    /// The data frame should have one column [`COL_GRAPH`](rdf_fusion_common::quads::COL_GRAPH).
    async fn dropped_named_graphs(
        &self,
        context: &ChangesetContext,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError>;

    /// Returns the current changeset as an [`EagerChangeset`]. This is necessary for updating the
    /// changeset during transactions.
    async fn as_eager_changeset(
        &self,
        state: &SessionState,
    ) -> Result<EagerChangeset, DeltaQuadsStorageError>;

    /// Returns the size of the changeset in bytes.
    fn size(&self) -> usize;
}
