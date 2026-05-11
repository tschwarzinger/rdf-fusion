use crate::delta::log::DeltaQuadStorageLogChangesetRef;
use crate::delta::log::DeltaStorageLogVersionRange;
use moka::future::Cache;

/// Manages changesets for the [`DeltaQuadStorageLog`](crate::delta::log::DeltaQuadStorageLog).
pub(crate) struct ChangesetManager {
    cache: Cache<DeltaStorageLogVersionRange, DeltaQuadStorageLogChangesetRef>,
}

impl ChangesetManager {
    /// Creates a new [`ChangesetManager`] with the given maximum capacity in bytes.
    pub fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .weigher(|_key, value: &DeltaQuadStorageLogChangesetRef| value.size() as u32)
            .max_capacity(max_capacity_bytes)
            .build();
        Self { cache }
    }

    /// Returns the changeset for the given version range if it is cached.
    pub async fn get(
        &self,
        version_range: &DeltaStorageLogVersionRange,
    ) -> Option<DeltaQuadStorageLogChangesetRef> {
        self.cache.get(version_range).await
    }

    /// Inserts a changeset into the cache.
    pub async fn insert(
        &self,
        version_range: DeltaStorageLogVersionRange,
        changeset: DeltaQuadStorageLogChangesetRef,
    ) {
        self.cache.insert(version_range, changeset).await;
    }
}
