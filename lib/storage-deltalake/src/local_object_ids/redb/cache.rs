use crate::local_object_ids::LocalDictionaryTerm;
use crate::local_object_ids::redb::RedbTerm;
use quick_cache::sync::Cache;
use std::sync::Arc;

/// A cache used in [`LocalObjectIdDictionary`](crate::local_object_ids::LocalObjectIdDictionary)
/// that keeps recently used mappings in-memory.
#[derive(Clone, Debug)]
pub struct RedbObjectIdCache {
    id_to_term_cache: Arc<Cache<i64, Arc<LocalDictionaryTerm>>>,
    term_to_id_cache: Arc<Cache<Arc<LocalDictionaryTerm>, i64>>,
}

impl RedbObjectIdCache {
    /// Creates a new [`RedbObjectIdCache`] with the given size.
    pub fn new(size: usize) -> Self {
        Self {
            id_to_term_cache: Arc::new(Cache::new(size)),
            term_to_id_cache: Arc::new(Cache::new(size)),
        }
    }

    /// Inserts a new term <-> id mapping into the cache.
    pub(crate) fn insert(&self, term: Arc<LocalDictionaryTerm>, id: i64) {
        self.term_to_id_cache.insert(Arc::clone(&term), id);
        self.id_to_term_cache.insert(id, term);
    }

    pub(crate) fn get_by_term(&self, term: &RedbTerm<'_>) -> Option<i64> {
        self.term_to_id_cache.get(term)
    }

    pub(crate) fn get_by_id(&self, id: i64) -> Option<Arc<LocalDictionaryTerm>> {
        self.id_to_term_cache.get(&id)
    }
}
