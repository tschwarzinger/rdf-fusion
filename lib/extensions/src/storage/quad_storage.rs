use crate::storage::{QuadStorageSnapshot, QuadStorageTransaction};
use async_trait::async_trait;
use datafusion::execution::SessionState;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::object_id::ObjectIdMapping;
use rdf_fusion_model::StorageError;
use std::sync::Arc;

#[async_trait]
#[allow(clippy::len_without_is_empty)]
pub trait QuadStorage: Send + Sync {
    /// Returns the quad storage encoding.
    fn encoding(&self) -> QuadStorageEncoding;

    /// Returns a reference to the used [ObjectIdMapping].
    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>>;

    /// Returns a snapshot reflecting the current version of this storage.
    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError>;

    /// Starts a new transaction against the current version of this quad storage.
    async fn begin_transaction(
        &self,
        session: &SessionState,
    ) -> Result<Box<dyn QuadStorageTransaction>, StorageError>;

    /// Optimizes the storage (e.g., building indices).
    async fn optimize(&self, state: &SessionState) -> Result<(), StorageError>;

    /// Validates invariants in the store
    async fn validate(&self, state: &SessionState) -> Result<(), StorageError>;
}
