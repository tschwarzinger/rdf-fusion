use crate::local_object_ids::redb::cache::RedbObjectIdCache;
use crate::local_object_ids::{
    LocalObjectIdError, ObjectIdClaimer, RedbLocalObjectIdDictionary,
};
use redb::Database;
use redb::backends::InMemoryBackend;
use std::path::Path;
use std::sync::Arc;

/// A builder for a [`RedbLocalObjectIdDictionary`].
pub struct RedbObjectIdDictionaryBuilder {
    /// The storage backend to use.
    backend: RedbBackend,
    /// The size of the cache. If `None`, the cache will be disabled.
    cache_size: Option<usize>,
    /// The object id claimer to use. If `None`, the claimer will be disabled.
    claimer: Option<Arc<dyn ObjectIdClaimer>>,
}

enum RedbBackend {
    InMemory,
    OnDisk(std::path::PathBuf),
}

impl RedbObjectIdDictionaryBuilder {
    /// Creates a new [`RedbObjectIdDictionaryBuilder`] with a new in-memory storage backend.
    pub fn new_in_memory() -> Self {
        Self {
            backend: RedbBackend::InMemory,
            cache_size: None,
            claimer: None,
        }
    }

    /// Creates a new [`RedbObjectIdDictionaryBuilder`] with a new on-disk storage backend.
    pub fn new_on_disk(path: impl AsRef<Path>) -> Self {
        Self {
            backend: RedbBackend::OnDisk(path.as_ref().to_path_buf()),
            cache_size: None,
            claimer: None,
        }
    }

    /// Sets the cache size for this builder.
    pub fn with_cache_size(mut self, cache_size: Option<usize>) -> Self {
        self.cache_size = cache_size;
        self
    }

    /// Sets the object id claimer for this builder.
    pub fn with_claimer(mut self, claimer: Option<Arc<dyn ObjectIdClaimer>>) -> Self {
        self.claimer = claimer;
        self
    }

    /// Creates the [`RedbLocalObjectIdDictionary`].
    pub fn finish(self) -> Result<RedbLocalObjectIdDictionary, LocalObjectIdError> {
        let database = match self.backend {
            RedbBackend::InMemory => Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .map_err(|e| LocalObjectIdError::Storage(e.to_string()))?,
            RedbBackend::OnDisk(path) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| LocalObjectIdError::Storage(e.to_string()))?;
                }

                Database::create(path)
                    .map_err(|e| LocalObjectIdError::Storage(e.to_string()))?
            }
        };

        let cache = self.cache_size.map(RedbObjectIdCache::new);
        RedbLocalObjectIdDictionary::try_new(Arc::new(database), cache, self.claimer)
    }
}
