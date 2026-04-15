use crate::index::IndexComponents;
use deltalake::kernel::{Add, EagerSnapshot};
use deltalake::logstore::LogStoreRef;
use rdf_fusion_encoding::QuadStorageEncoding;
use std::sync::Arc;

/// Represents an immutable snapshot of the index at a specific Delta commit version.
///
/// This guarantees that readers see a consistent state of the data and the application log version.
/// Note that we assume that no cleanup job (i.e., vacuuming) is cleaning up the files that are
/// referenced by this snapshot.
#[derive(Debug, Clone)]
pub struct DeltaStorageQuadIndexSnapshot {
    /// The encoding used for storing quads.
    storage_encoding: QuadStorageEncoding,
    /// The log store of the index table.
    log_store: LogStoreRef,
    /// The snapshot of the index table.
    snapshot: EagerSnapshot,
    /// The active files of the index table.
    active_files: Arc<Vec<Add>>,
    /// The components of the index.
    components: IndexComponents,
    /// The log version that this snapshot represents.
    log_version: u64,
}

impl DeltaStorageQuadIndexSnapshot {
    /// Creates a new [`DeltaStorageQuadIndexSnapshot`]. The snapshot and the log store are
    /// expected to belong to the same Delta table.
    pub fn new(
        storage_encoding: QuadStorageEncoding,
        snapshot: EagerSnapshot,
        log_store: LogStoreRef,
        active_files: Arc<Vec<Add>>,
        components: IndexComponents,
        log_version: u64,
    ) -> Self {
        Self {
            storage_encoding,
            snapshot,
            active_files,
            log_store,
            components,
            log_version,
        }
    }

    /// Returns the encoding used for storing quads.
    pub fn encoding(&self) -> QuadStorageEncoding {
        self.storage_encoding.clone()
    }

    /// Returns the current version of the quad storage database that this index snapshot reflects.
    pub fn log_transaction_version(&self) -> u64 {
        self.log_version
    }

    /// Returns the underlying delta table snapshot.
    pub fn snapshot(&self) -> &EagerSnapshot {
        &self.snapshot
    }

    /// Returns the cached active files for this snapshot.
    pub fn active_files(&self) -> &Arc<Vec<Add>> {
        &self.active_files
    }

    /// Returns the underlying log store.
    pub fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }

    /// Returns the components of the index.
    pub fn components(&self) -> IndexComponents {
        self.components
    }
}
