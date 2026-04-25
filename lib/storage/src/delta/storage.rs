use crate::delta::DeltaStorageTransaction;
use crate::delta::error::DeltaQuadStorageError;
use crate::delta::index::{DeltaQuadStorageIndex, DeltaQuadStorageIndexSnapshot};
use crate::delta::log::{DeltaStorageLog, DeltaStorageLogVersionRange};
use crate::delta::objectids::DeltaObjectIdMapping;
use crate::delta::snapshot::DeltaQuadStorageSnapshot;
use crate::index::IndexComponents;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use rdf_fusion_encoding::object_id::{ObjectIdEncoding, ObjectIdMapping};
use rdf_fusion_encoding::typed_family::TypedFamilyEncodingRef;
use rdf_fusion_encoding::{QuadStorageEncoding, QuadStorageEncodingName};
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use rdf_fusion_model::StorageError;
use std::sync::Arc;

/// A quad storage that uses Delta Lake tables for storing quads.
#[derive(Clone)]
pub struct DeltaQuadStorage {
    /// The log that records the changes made to the storage
    log: Arc<DeltaStorageLog>,
    /// The encodings used for storing quads
    storage_encoding: QuadStorageEncoding,
    /// The indexes of the storage
    indexes: Vec<Arc<DeltaQuadStorageIndex>>,
    /// The object id mapping used for encoding object ids, if necessary.
    object_id_mapping: Option<Arc<DeltaObjectIdMapping>>,
}

impl DeltaQuadStorage {
    /// Creates a new [`DeltaQuadStorage`] at the given `base_location`.
    pub async fn new_at_location(
        encoding: QuadStorageEncodingName,
        index_configurations: Vec<IndexComponents>,
        base_location: &str,
        typed_family_encoding: TypedFamilyEncodingRef,
    ) -> Result<Self, DeltaQuadStorageError> {
        let (object_id_mapping, storage_encoding) = match encoding {
            QuadStorageEncodingName::PlainTerm => (None, QuadStorageEncoding::PlainTerm),
            QuadStorageEncodingName::String => (None, QuadStorageEncoding::String),
            QuadStorageEncodingName::ObjectId => {
                let mapping = Arc::new(
                    DeltaObjectIdMapping::try_new_at_location(
                        &format!("{base_location}/object_ids",),
                        typed_family_encoding,
                    )
                    .await?,
                );
                let encoding = ObjectIdEncoding::new(
                    Arc::clone(&mapping) as Arc<dyn ObjectIdMapping>
                );

                (
                    Some(mapping),
                    QuadStorageEncoding::ObjectId(Arc::new(encoding)),
                )
            }
        };

        let log = DeltaStorageLog::try_new_at_location(
            storage_encoding.clone(),
            &format!("{base_location}/log"),
        )
        .await
        .expect("TODO");

        let mut indexes = Vec::new();
        for index in index_configurations {
            let new_index = DeltaQuadStorageIndex::try_new(
                storage_encoding.clone(),
                &format!("{base_location}/{index}"),
                index,
            )
            .await
            .unwrap();
            indexes.push(Arc::new(new_index));
        }

        Ok(Self {
            log: Arc::new(log),
            storage_encoding,
            indexes,
            object_id_mapping,
        })
    }

    /// Creates a new [`DeltaQuadStorage`] with default settings (ObjectId encoding) at the given `base_location`.
    pub async fn new_default_at_location(
        index_configurations: Vec<IndexComponents>,
        base_location: &str,
        typed_family_encoding: TypedFamilyEncodingRef,
    ) -> Result<Self, DeltaQuadStorageError> {
        Self::new_at_location(
            QuadStorageEncodingName::ObjectId,
            index_configurations,
            base_location,
            typed_family_encoding,
        )
        .await
    }

    /// Creates a new [`DeltaQuadStorage`] in memory.
    pub async fn new_in_memory(
        encoding: QuadStorageEncodingName,
        index_configurations: Vec<IndexComponents>,
        typed_family_encoding: TypedFamilyEncodingRef,
    ) -> Self {
        Self::new_at_location(
            encoding,
            index_configurations,
            "memory://",
            typed_family_encoding,
        )
        .await
        .expect("In Memory should always initialize successfully")
    }

    /// Creates a new [`DeltaQuadStorage`] in memory with default settings (ObjectId encoding).
    pub async fn new_default_in_memory(
        index_configurations: Vec<IndexComponents>,
        typed_family_encoding: TypedFamilyEncodingRef,
    ) -> Self {
        Self::new_in_memory(
            QuadStorageEncodingName::ObjectId,
            index_configurations,
            typed_family_encoding,
        )
        .await
    }

    /// Returns the log that records the changes made to the storage.
    pub fn log(&self) -> &Arc<DeltaStorageLog> {
        &self.log
    }

    /// Returns the indexes of the storage.
    pub fn indexes(&self) -> &[Arc<DeltaQuadStorageIndex>] {
        &self.indexes
    }

    /// Returns the indexes of the storage.
    pub async fn index_snapshots(
        &self,
    ) -> Result<Vec<DeltaQuadStorageIndexSnapshot>, DeltaQuadStorageError> {
        let mut result = Vec::new();

        for index in &self.indexes {
            let snapshot = index.snapshot().await?;
            result.push(snapshot);
        }

        Ok(result)
    }

    /// Returns the encodings used by this storage.
    pub fn storage_encoding(&self) -> &QuadStorageEncoding {
        &self.storage_encoding
    }

    /// Returns the object id mapping used by this storage, if any.
    pub fn delta_object_id_mapping(&self) -> Option<Arc<DeltaObjectIdMapping>> {
        self.object_id_mapping.clone()
    }

    /// Takes a snapshot of the storage (indexes + logs).
    pub(crate) async fn snapshot_impl(
        &self,
    ) -> Result<DeltaQuadStorageSnapshot, StorageError> {
        let index_snapshots = self.index_snapshots().await?;

        // Get the log version after the index versions so that the index versions are always equal
        // or smaller than the log version.
        let version = self.log.version().await;

        Ok(DeltaQuadStorageSnapshot::new(
            Arc::clone(&self.log),
            index_snapshots,
            self.storage_encoding.clone(),
            self.object_id_mapping.clone(),
            version,
        ))
    }
}

#[async_trait]
impl QuadStorage for DeltaQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        self.storage_encoding.clone()
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        self.storage_encoding
            .object_id_encoding()
            .map(|enc| Arc::clone(enc.mapping()))
    }

    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        let snapshot = self.snapshot_impl().await?;
        Ok(Arc::new(snapshot))
    }

    async fn begin_transaction(
        &self,
        session: &SessionState,
    ) -> Result<Box<dyn QuadStorageTransaction>, StorageError> {
        let snapshot = self.snapshot_impl().await?;
        Ok(Box::new(DeltaStorageTransaction::new(
            Arc::new(self.clone()),
            session.clone(),
            Arc::clone(self.log.table()),
            Arc::clone(self.log.schema()),
            Arc::new(snapshot),
        )))
    }

    async fn optimize(&self, state: &SessionState) -> Result<(), StorageError> {
        if self.indexes.is_empty() {
            return Ok(());
        }

        let any_index = &self.indexes()[0];
        let snapshot = any_index.snapshot().await?;
        let current_index_version = snapshot.log_transaction_version();
        let current_log_version = self.log.version().await;

        if current_log_version < current_index_version {
            return Err(DeltaQuadStorageError::VersionError(format!(
                "Index is already at version {current_index_version}. Cannot downgrade to version {current_log_version}.",
            )).into());
        }

        if current_log_version == current_index_version {
            return Ok(());
        }

        let version_range = DeltaStorageLogVersionRange::new_unchecked(
            current_index_version,
            current_log_version,
        );
        let changeset = self.log.compute_changeset(state, version_range).await?;

        for index in &self.indexes {
            index
                .update(state, Arc::clone(&changeset))
                .await
                .map_err(|e| StorageError::Other(Box::new(e)))?;
        }

        Ok(())
    }

    async fn validate(&self, state: &SessionState) -> Result<(), StorageError> {
        // TODO: Validate the log

        for index in &self.indexes {
            index
                .validate(state)
                .await
                .map_err(|e| StorageError::Other(Box::new(e)))?;
        }

        Ok(())
    }
}
