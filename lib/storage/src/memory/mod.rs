//! A simple in-memory quad storage.
//!
//! The [`MemoryQuadStorage`] is meant for tests, examples, and small datasets. Scans use the
//! indexes of the [`Dataset`] to find the matching quads and convert them to Arrow while the query
//! is executed.

mod changes;
mod scan;
mod snapshot;
mod transaction;

#[cfg(test)]
mod tests;

pub use snapshot::MemoryQuadStorageSnapshot;
pub use transaction::MemoryQuadStorageTransaction;

use crate::memory::changes::Changes;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use rdf_fusion_common::{Dataset, NamedOrBlankNode, StorageError};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::object_id::ObjectIdDictionary;
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// A simple quad storage that keeps all quads in memory.
///
/// The quads are kept in an [`oxrdf::Dataset`](Dataset). Scans look up the quads that match the
/// constants of the pattern in the indexes of the dataset and convert them to Arrow (in the plain
/// term encoding) during execution, one batch at a time.
///
/// A named graph exists once it is created explicitly (e.g., with `CREATE GRAPH`) or a quad is
/// inserted into it. It continues to exist when its quads are removed (e.g., with `CLEAR GRAPH`)
/// and only ceases to exist when it is dropped (e.g., with `DROP GRAPH`).
///
/// Transactions are serialized: while a transaction is open, [`QuadStorage::begin_transaction`]
/// waits until the transaction is committed or dropped. Dropping a transaction discards its
/// changes. Snapshots are unaffected by open transactions.
///
/// A transaction records its changes on top of the committed state and only applies them on
/// commit. Neither snapshots nor transactions copy the quads. Only a commit while a snapshot of the
/// committed state is still alive copies the quads, such that the snapshot remains unaffected.
#[derive(Debug, Clone, Default)]
pub struct MemoryQuadStorage {
    /// The current (committed) state.
    state: Arc<RwLock<Arc<MemoryState>>>,
    /// Held by the open transaction, if any.
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl MemoryQuadStorage {
    /// Creates a new empty [`MemoryQuadStorage`].
    pub fn new() -> Self {
        Self::default()
    }

    fn current_state(&self) -> Arc<MemoryState> {
        Arc::clone(&self.state.read().expect("Lock is not poisoned"))
    }
}

#[async_trait]
impl QuadStorage for MemoryQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        QuadStorageEncoding::PlainTerm
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdDictionary>> {
        None
    }

    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        Ok(Arc::new(MemoryQuadStorageSnapshot::new(
            self.current_state(),
            Arc::new(Changes::default()),
        )))
    }

    async fn begin_transaction(
        &self,
        _session: &SessionState,
    ) -> Result<Box<dyn QuadStorageTransaction>, StorageError> {
        let write_guard = Arc::clone(&self.write_lock).lock_owned().await;
        Ok(Box::new(MemoryQuadStorageTransaction::new(
            Arc::clone(&self.state),
            self.current_state(),
            write_guard,
        )))
    }

    async fn optimize(&self, _state: &SessionState) -> Result<(), StorageError> {
        Ok(())
    }

    async fn validate(&self, _state: &SessionState) -> Result<(), StorageError> {
        Ok(())
    }
}

/// The committed state of a [`MemoryQuadStorage`].
///
/// Shared states are immutable. A commit modifies the state in place if it is not shared (i.e., no
/// snapshot uses it) and copies it otherwise (see [`Arc::make_mut`]).
#[derive(Debug, Clone, Default)]
struct MemoryState {
    /// All quads.
    quads: Dataset,
    /// The named graphs. Contains the graphs of all quads and the graphs without quads.
    named_graphs: HashSet<NamedOrBlankNode>,
}
