use crate::delta::log::DeltaQuadStorageLogChangesetRef;
use crate::delta::log::DeltaStorageLogVersionRange;
use quick_cache::Weighter;
use quick_cache::sync::Cache;

#[derive(Clone, Copy, Debug, Default)]
struct ChangeSetWeighter;

impl Weighter<DeltaStorageLogVersionRange, DeltaQuadStorageLogChangesetRef>
    for ChangeSetWeighter
{
    fn weight(
        &self,
        _key: &DeltaStorageLogVersionRange,
        val: &DeltaQuadStorageLogChangesetRef,
    ) -> u64 {
        val.size() as u64
    }
}

/// Manages changesets for the [`DeltaQuadStorageLog`](crate::delta::log::DeltaQuadStorageLog).
pub struct ChangesetManager {
    cache: Cache<
        DeltaStorageLogVersionRange,
        DeltaQuadStorageLogChangesetRef,
        ChangeSetWeighter,
    >,
}

impl ChangesetManager {
    pub fn new(max_capacity_bytes: u64) -> Self {
        let estimated_items = (max_capacity_bytes / 1_000_000).max(10) as usize;
        let cache =
            Cache::with_weighter(estimated_items, max_capacity_bytes, ChangeSetWeighter);
        Self { cache }
    }

    /// Returns the changeset for the given version range if it is cached.
    pub async fn get(
        &self,
        version_range: &DeltaStorageLogVersionRange,
    ) -> Option<DeltaQuadStorageLogChangesetRef> {
        self.cache.get(version_range)
    }

    /// Inserts a changeset into the cache.
    pub async fn insert(
        &self,
        version_range: DeltaStorageLogVersionRange,
        changeset: DeltaQuadStorageLogChangesetRef,
    ) {
        self.cache.insert(version_range, changeset);
    }
}
