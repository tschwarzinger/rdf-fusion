use crate::delta::DeltaQuadsStorageBuilder;
use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::log::{DeltaQuadsStorageLog, DeltaStorageLogVersionRange};
use crate::delta::objectids::DeltaObjectIdDictionary;
use crate::delta::quad_table::{DeltaQuadsQuadTable, DeltaQuadsQuadTableSnapshot};
use crate::delta::refresh::DeltaTableRefresher;
use crate::delta::snapshot::DeltaQuadsStorageSnapshot;
use crate::delta::transaction::DeltaQuadsStorageTransaction;
use crate::object_store::CachedObjectStore;
use crate::quad_tables::QuadTableName;
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
    /// The quad tables of the storage
    quad_tables: Vec<Arc<DeltaQuadsQuadTable>>,
    /// The object id mapping used for encoding object ids, if necessary.
    object_id_mapping: Option<Arc<DeltaObjectIdDictionary>>,
    /// Manages periodic refreshes of the delta table.
    refresher: Arc<DeltaTableRefresher>,
    /// Options
    options: DeltaStorageOptions,
    /// Cached object store
    cached_store: Arc<CachedObjectStore>,
}

impl DeltaQuadsStorage {
    /// Creates a new [`DeltaQuadsStorage`] at the given `base_location`.
    pub async fn new_at_location(
        options: &RdfFusionOptions,
        encoding: QuadStorageEncodingName,
        quad_table_configurations: Vec<QuadTableName>,
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

        let mut quad_tables = Vec::new();
        for quad_table in quad_table_configurations {
            let quad_table_url = base_url
                .join(&format!("quad-tables/{quad_table}/"))
                .unwrap();
            let quad_table_log_store = logstore_with(
                base_log_store.root_object_store(None),
                &quad_table_url,
                storage_config.clone(),
            )
            .map_err(DeltaQuadsStorageError::from)?;

            let new_quad_table = DeltaQuadsQuadTable::try_new(
                storage_encoding.clone(),
                quad_table_log_store,
                quad_table,
            )
            .await
            .unwrap();
            quad_tables.push(Arc::new(new_quad_table));
        }

        Ok(Self {
            log: Arc::new(log),
            storage_encoding,
            quad_tables,
            object_id_mapping,
            refresher: Arc::new(DeltaTableRefresher::new(None)),
            options: options.storage.delta.clone(),
            cached_store: Arc::new(CachedObjectStore::new(
                base_log_store.root_object_store(None),
                options.storage.delta.data_cache_block_size,
                options.storage.delta.data_cache_num_blocks,
            )),
        })
    }

    /// Creates a new [`DeltaQuadsStorage`] in memory.
    pub async fn new_in_memory(
        encoding: QuadStorageEncodingName,
        quad_table_configurations: Vec<QuadTableName>,
    ) -> Self {
        DeltaQuadsStorageBuilder::new()
            .with_encoding(encoding)
            .with_quad_tables(quad_table_configurations)
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

        let mut quad_tables = Vec::new();
        let object_store = base_log_store.root_object_store(None);
        for quad_table in QuadTableName::list_all() {
            let quad_table_url = base_url
                .join(&format!("quad-tables/{quad_table}/"))
                .unwrap();
            let prefix_path = Path::from(quad_table_url.path());

            let mut list_stream = object_store.list(Some(&prefix_path));
            let exists = list_stream.next().await.is_some();
            if !exists {
                continue;
            }

            let quad_table_log_store = logstore_with(
                base_log_store.root_object_store(None),
                &quad_table_url,
                log_storage_config.clone(),
            )
            .map_err(DeltaQuadsStorageError::from)?;

            let new_quad_table = DeltaQuadsQuadTable::try_load(
                storage_encoding.clone(),
                quad_table_log_store,
                *quad_table,
            )
            .await?;
            quad_tables.push(Arc::new(new_quad_table));
        }

        Ok(Self {
            log: Arc::new(log),
            storage_encoding,
            quad_tables,
            object_id_mapping,
            refresher: Arc::new(DeltaTableRefresher::new(None)),
            options: options.storage.delta.clone(),
            cached_store: Arc::new(CachedObjectStore::new(
                base_log_store.root_object_store(None),
                options.storage.delta.data_cache_block_size,
                options.storage.delta.data_cache_num_blocks,
            )),
        })
    }

    /// Returns the log that records the changes made to the storage.
    pub fn log(&self) -> &Arc<DeltaQuadsStorageLog> {
        &self.log
    }

    /// Returns the quad tables of the storage.
    pub fn quad_tables(&self) -> &[Arc<DeltaQuadsQuadTable>] {
        &self.quad_tables
    }

    /// Returns the quad tables of the storage.
    pub async fn quad_table_snapshots(
        &self,
    ) -> Result<Vec<DeltaQuadsQuadTableSnapshot>, DeltaQuadsStorageError> {
        let mut result = Vec::new();

        for quad_table in &self.quad_tables {
            let snapshot = quad_table.snapshot().await?;
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

    /// Takes a snapshot of the storage (quad tables + logs).
    pub(crate) async fn snapshot_impl(
        &self,
    ) -> Result<DeltaQuadsStorageSnapshot, StorageError> {
        let arrival_time = Instant::now();
        self.refresher
            .ensure_fresh(arrival_time, self.log.table())
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        let quad_table_snapshots = self.quad_table_snapshots().await?;

        // Get the log version after the quad_table versions so that the quad_table versions are always equal
        // or smaller than the log version.
        let version = self.log.version().await;

        Ok(DeltaQuadsStorageSnapshot::new(
            Arc::clone(&self.log),
            quad_table_snapshots,
            self.storage_encoding.clone(),
            self.object_id_mapping.clone(),
            self.options.clone(),
            version,
            Arc::clone(&self.cached_store),
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
        if self.quad_tables.is_empty() {
            info!("Database has no quad tables.");
            return Ok(());
        }

        let mut snapshots = Vec::new();
        for quad_table in self.quad_tables() {
            let snapshot = quad_table.snapshot().await?;
            snapshots.push(snapshot)
        }

        let mut min_version = u64::MAX;
        for snapshot in &snapshots {
            let current_quad_table_version = snapshot.log_transaction_version();
            if current_quad_table_version < min_version {
                min_version = current_quad_table_version;
            }
        }

        let current_quad_table_version = min_version;
        let current_log_version = self.log.version().await;

        if current_log_version < current_quad_table_version {
            return Err(DeltaQuadsStorageError::VersionError(format!(
                "QuadTable is already at version {current_quad_table_version}. Cannot downgrade to version {current_log_version}.",
            )).into());
        }

        if current_log_version == current_quad_table_version {
            info!(
                "All quad tables are up-to-date (version {}).",
                current_log_version
            );
            return Ok(());
        }

        let version_range = DeltaStorageLogVersionRange::new_unchecked(
            current_quad_table_version,
            current_log_version,
        );
        let changeset = self.log.compute_changeset(state, version_range).await?;

        info!(
            "Updating quad tables from version {} to version {}",
            current_quad_table_version, current_log_version
        );

        for (quad_table, snapshot) in self.quad_tables.iter().zip(snapshots.iter()) {
            if snapshot.log_transaction_version() == current_log_version {
                info!(
                    "Skipping updating quad table {} with matching version ({}).",
                    snapshot.components(),
                    snapshot.log_transaction_version()
                );
                continue;
            }

            info!(
                "Updating quad table {} ...",
                quad_table.components().to_string()
            );
            Box::pin(quad_table.update(state, Arc::clone(&changeset)))
                .await
                .map_err(|e| StorageError::Other(Box::new(e)))?;
            info!(
                "Quad table {} updated.",
                quad_table.components().to_string()
            );
        }

        Ok(())
    }

    async fn validate(&self, state: &SessionState) -> Result<(), StorageError> {
        // TODO: Validate the log

        for quad_table in &self.quad_tables {
            quad_table
                .validate(state)
                .await
                .map_err(|e| StorageError::Other(Box::new(e)))?;
        }

        Ok(())
    }
}
