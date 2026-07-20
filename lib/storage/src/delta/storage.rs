use crate::delta::DeltaQuadsStorageBuilder;
use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::index::{DeltaQuadsStorageIndex, DeltaQuadsStorageIndexSnapshot};
use crate::delta::log::{DeltaQuadsStorageLog, DeltaStorageLogVersionRange};
use crate::delta::objectids::DeltaObjectIdDictionary;
use crate::delta::refresh::DeltaTableRefresher;
use crate::delta::snapshot::DeltaQuadsStorageSnapshot;
use crate::delta::transaction::DeltaQuadsStorageTransaction;
use crate::index::IndexComponents;
use crate::parquet::reader::ChunkCache;
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::instant::Instant;
use datafusion::execution::SessionState;
use deltalake::logstore::{LogStoreRef, logstore_with};
use futures::StreamExt;
use object_store::path::Path;
use rdf_fusion_common::StorageError;
use rdf_fusion_common::config::{DeltaStorageOptions, RdfFusionOptions};
use rdf_fusion_common::quads::COL_GRAPH;
use rdf_fusion_encoding::object_id::{ObjectIdDictionary, ObjectIdEncoding};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::{QuadStorageEncoding, QuadStorageEncodingName, TermEncoding};
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// A quad storage that uses Delta Lake tables for storing quads.
///
/// DeltaQuads is another governance layer that combines multiple Delta Lake tables in such a way
/// that they can be used to implement an RDF store.
#[derive(Clone)]
pub struct DeltaQuadsStorage {
    /// The log that records the changes made to the storage
    log: Arc<DeltaQuadsStorageLog>,
    /// The encodings used for storing quads
    storage_encoding: QuadStorageEncoding,
    /// The indexes of the storage
    indexes: Vec<Arc<DeltaQuadsStorageIndex>>,
    /// The object id mapping used for encoding object ids, if necessary.
    object_id_mapping: Option<Arc<DeltaObjectIdDictionary>>,
    /// Manages periodic refreshes of the delta table.
    refresher: Arc<DeltaTableRefresher>,
    /// Options
    options: DeltaStorageOptions,
    /// Chunked cache for object store reads
    chunk_cache: Arc<ChunkCache>,
}

impl DeltaQuadsStorage {
    /// Creates a new [`DeltaQuadsStorage`] at the given `base_location`.
    pub async fn new_at_location(
        options: &RdfFusionOptions,
        encoding: QuadStorageEncodingName,
        index_configurations: Vec<IndexComponents>,
        base_log_store: LogStoreRef,
    ) -> Result<Self, DeltaQuadsStorageError> {
        let storage_config = base_log_store.config().options().clone();
        let base_url = base_log_store.config().location().clone();

        let (object_id_mapping, storage_encoding) = match encoding {
            QuadStorageEncodingName::PlainTerm => (None, QuadStorageEncoding::PlainTerm),
            QuadStorageEncodingName::String => (None, QuadStorageEncoding::String),
            QuadStorageEncodingName::ObjectId => {
                let mapping_url = base_url.join("dictionary/").unwrap();
                let mapping_log_store = logstore_with(
                    base_log_store.root_object_store(None),
                    &mapping_url,
                    storage_config.clone(),
                )
                .map_err(DeltaQuadsStorageError::from)?;

                let mapping = Arc::new(
                    DeltaObjectIdDictionary::try_new_at_location(
                        options,
                        mapping_log_store,
                    )
                    .await?,
                );
                let encoding = ObjectIdEncoding::new(
                    Arc::clone(&mapping) as Arc<dyn ObjectIdDictionary>
                );

                (
                    Some(mapping),
                    QuadStorageEncoding::ObjectId(Arc::new(encoding)),
                )
            }
        };

        let log_url = base_url.join("log/").unwrap();
        let log_log_store = logstore_with(
            base_log_store.root_object_store(None),
            &log_url,
            storage_config.clone(),
        )
        .map_err(DeltaQuadsStorageError::from)?;

        let log = DeltaQuadsStorageLog::try_new_at_location(
            storage_encoding.clone(),
            log_log_store,
        )
        .await?;

        let mut indexes = Vec::new();
        for index in index_configurations {
            let index_url = base_url.join(&format!("indexes/{index}/")).unwrap();
            let index_log_store = logstore_with(
                base_log_store.root_object_store(None),
                &index_url,
                storage_config.clone(),
            )
            .map_err(DeltaQuadsStorageError::from)?;

            let new_index = DeltaQuadsStorageIndex::try_new(
                storage_encoding.clone(),
                index_log_store,
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
            refresher: Arc::new(DeltaTableRefresher::new(None)),
            options: options.storage.delta.clone(),
            chunk_cache: Arc::new(ChunkCache::with_weighter(
                100_000, // estimated number of items
                options.storage.delta.page_cache_size as u64,
                crate::parquet::reader::BytesWeighter,
            )),
        })
    }

    /// Creates a new [`DeltaQuadsStorage`] in memory.
    pub async fn new_in_memory(
        encoding: QuadStorageEncodingName,
        index_configurations: Vec<IndexComponents>,
    ) -> Self {
        DeltaQuadsStorageBuilder::new()
            .with_encoding(encoding)
            .with_indexes(index_configurations)
            .build()
            .await
            .expect("Failed to build in-memory DeltaQuadsStorage")
    }

    /// Tries to load an existing [`DeltaQuadsStorage`] based on the given `base_location`.
    pub async fn try_load(
        state: &SessionState,
        options: &RdfFusionOptions,
        base_log_store: LogStoreRef,
    ) -> Result<Self, DeltaQuadsStorageError> {
        let log_storage_config = base_log_store.config().options().clone();
        let base_url = base_log_store.config().location().clone();

        let log_url = base_url.join("log/").unwrap();
        let log_log_store = logstore_with(
            base_log_store.root_object_store(None),
            &log_url,
            log_storage_config.clone(),
        )
        .map_err(DeltaQuadsStorageError::from)?;

        let log = DeltaQuadsStorageLog::try_load(log_log_store).await?;

        let graph_column = log.schema().column_with_name(COL_GRAPH).ok_or_else(|| {
            DeltaQuadsStorageError::Corruption(
                "Graph column not found in log schema".to_string(),
            )
        })?;
        let data_type = graph_column.1.data_type();

        let (storage_encoding, object_id_mapping) =
            if data_type == PLAIN_TERM_ENCODING.data_type() {
                (QuadStorageEncoding::PlainTerm, None)
            } else if data_type == STRING_ENCODING.data_type() {
                (QuadStorageEncoding::String, None)
            } else if data_type == &DataType::Int64 {
                let mapping_url = base_url.join("dictionary/").unwrap();
                let mapping_log_store = logstore_with(
                    base_log_store.root_object_store(None),
                    &mapping_url,
                    log_storage_config.clone(),
                )
                .map_err(DeltaQuadsStorageError::from)?;

                let mapping =
                    DeltaObjectIdDictionary::try_load(state, mapping_log_store).await?;
                let mapping = Arc::new(mapping);
                let encoding = ObjectIdEncoding::new(
                    Arc::clone(&mapping) as Arc<dyn ObjectIdDictionary>
                );

                (
                    QuadStorageEncoding::ObjectId(Arc::new(encoding)),
                    Some(mapping),
                )
            } else {
                return Err(DeltaQuadsStorageError::Other(format!(
                    "Loading for data type {data_type} not supported."
                )));
            };

        let mut indexes = Vec::new();
        let object_store = base_log_store.root_object_store(None);
        for index in IndexComponents::list_all() {
            let index_url = base_url.join(&format!("indexes/{index}/")).unwrap();
            let prefix_path = Path::from(index_url.path());

            let mut list_stream = object_store.list(Some(&prefix_path));
            let exists = list_stream.next().await.is_some();
            if !exists {
                continue;
            }

            let index_log_store = logstore_with(
                base_log_store.root_object_store(None),
                &index_url,
                log_storage_config.clone(),
            )
            .map_err(DeltaQuadsStorageError::from)?;

            let new_index = DeltaQuadsStorageIndex::try_load(
                storage_encoding.clone(),
                index_log_store,
                *index,
            )
            .await?;
            indexes.push(Arc::new(new_index));
        }

        Ok(Self {
            log: Arc::new(log),
            storage_encoding,
            indexes,
            object_id_mapping,
            refresher: Arc::new(DeltaTableRefresher::new(None)),
            options: options.storage.delta.clone(),
            chunk_cache: Arc::new(ChunkCache::with_weighter(
                100_000, // estimated number of items
                options.storage.delta.page_cache_size as u64,
                crate::parquet::reader::BytesWeighter,
            )),
        })
    }

    /// Returns the log that records the changes made to the storage.
    pub fn log(&self) -> &Arc<DeltaQuadsStorageLog> {
        &self.log
    }

    /// Returns the indexes of the storage.
    pub fn indexes(&self) -> &[Arc<DeltaQuadsStorageIndex>] {
        &self.indexes
    }

    /// Returns the indexes of the storage.
    pub async fn index_snapshots(
        &self,
    ) -> Result<Vec<DeltaQuadsStorageIndexSnapshot>, DeltaQuadsStorageError> {
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
    pub fn delta_object_id_mapping(&self) -> Option<Arc<DeltaObjectIdDictionary>> {
        self.object_id_mapping.clone()
    }

    /// Sets the maximum age of the transaction log before it is refreshed.
    pub async fn set_transaction_max_age(&self, max_age: Option<Duration>) {
        self.refresher.set_max_age(max_age).await;
    }

    /// Takes a snapshot of the storage (indexes + logs).
    pub(crate) async fn snapshot_impl(
        &self,
    ) -> Result<DeltaQuadsStorageSnapshot, StorageError> {
        let arrival_time = Instant::now();
        self.refresher
            .ensure_fresh(arrival_time, self.log.table())
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        let index_snapshots = self.index_snapshots().await?;

        // Get the log version after the index versions so that the index versions are always equal
        // or smaller than the log version.
        let version = self.log.version().await;

        Ok(DeltaQuadsStorageSnapshot::new(
            Arc::clone(&self.log),
            index_snapshots,
            self.storage_encoding.clone(),
            self.object_id_mapping.clone(),
            self.options.clone(),
            version,
            Arc::clone(&self.chunk_cache),
        ))
    }
}

#[async_trait]
impl QuadStorage for DeltaQuadsStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        self.storage_encoding.clone()
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdDictionary>> {
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
        state: &SessionState,
    ) -> Result<Box<dyn QuadStorageTransaction>, StorageError> {
        let snapshot = self.snapshot_impl().await?;
        Ok(Box::new(DeltaQuadsStorageTransaction::new(
            Arc::new(self.clone()),
            state.clone(),
            Arc::clone(self.log.table()),
            Arc::clone(self.log.schema()),
            Arc::new(snapshot),
        )))
    }

    async fn optimize(&self, state: &SessionState) -> Result<(), StorageError> {
        if self.indexes.is_empty() {
            info!("Database has no indexes.");
            return Ok(());
        }

        let mut snapshots = Vec::new();
        for index in self.indexes() {
            let snapshot = index.snapshot().await?;
            snapshots.push(snapshot)
        }

        let mut min_version = u64::MAX;
        for snapshot in &snapshots {
            let current_index_version = snapshot.log_transaction_version();
            if current_index_version < min_version {
                min_version = current_index_version;
            }
        }

        let current_index_version = min_version;
        let current_log_version = self.log.version().await;

        if current_log_version < current_index_version {
            return Err(DeltaQuadsStorageError::VersionError(format!(
                "Index is already at version {current_index_version}. Cannot downgrade to version {current_log_version}.",
            )).into());
        }

        if current_log_version == current_index_version {
            info!(
                "All indexes are up-to-date (version {}).",
                current_log_version
            );
            return Ok(());
        }

        let version_range = DeltaStorageLogVersionRange::new_unchecked(
            current_index_version,
            current_log_version,
        );
        let changeset = self.log.compute_changeset(state, version_range).await?;

        info!(
            "Updating indexes from version {} to version {}",
            current_index_version, current_log_version
        );

        for (index, snapshot) in self.indexes.iter().zip(snapshots.iter()) {
            if snapshot.log_transaction_version() == current_log_version {
                info!(
                    "Skipping updating index {} with matching version ({}).",
                    snapshot.components(),
                    snapshot.log_transaction_version()
                );
                continue;
            }

            info!("Updating index {} ...", index.components().to_string());
            Box::pin(index.update(state, Arc::clone(&changeset)))
                .await
                .map_err(|e| StorageError::Other(Box::new(e)))?;
            info!("Index {} updated.", index.components().to_string());
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
