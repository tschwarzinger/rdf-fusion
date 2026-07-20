use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::objectids::{DeltaObjectIdClaimer, ObjectIdClaimerPutMode};
use crate::local_object_ids::{
    InMemoryObjectIdDictionary, LocalObjectIdDictionary, LocalObjectIdTransaction,
    RocksDBObjectIdDictionary,
};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, Int64Builder, RecordBatch};
use datafusion::arrow::datatypes::{Field, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionContext;
use deltalake::arrow::datatypes::Schema;
use deltalake::kernel::Action;
use deltalake::kernel::engine::arrow_conversion::{TryFromArrow, TryFromKernel};
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties, TransactionError};
use deltalake::logstore::{LogStore, LogStoreRef};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{
    DataType as DeltaDataType, DeltaTable, DeltaTableConfig, DeltaTableError, StructField,
};
use futures::StreamExt;
use md5::{Digest, Md5};
use rdf_fusion_common::config::{RdfFusionOptions, RdfFusionSessionConfigExt};
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::object_id::{
    ObjectIdDataType, ObjectIdDictionary, ObjectIdDictionaryError,
};
use rdf_fusion_encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArray, PlainTermArrayElementBuilder, PlainTermScalar,
};
use rdf_fusion_encoding::typed_family::{TypedFamilyArray, TypedFamilyEncodingRef};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

fn get_rocksdb_path(
    options: &RdfFusionOptions,
    location: &url::Url,
) -> Option<std::path::PathBuf> {
    if location.scheme() == "memory" {
        return None;
    }

    if let Some(ref work_dir) = options.local.work_dir {
        let mut hasher = Md5::new();
        hasher.update(location.as_str().as_bytes());
        let result = hasher.finalize();

        let mut hash = String::with_capacity(32);
        for byte in result {
            use std::fmt::Write;
            write!(&mut hash, "{byte:02x}").unwrap();
        }

        let file_name = hash.to_string();

        info!("Using rocksdb directory '{file_name}' for location '{location}'.");

        Some(
            std::path::PathBuf::from(work_dir)
                .join("dictionaries")
                .join(file_name),
        )
    } else {
        None
    }
}

/// Implements [ObjectIdDictionary] using a [LocalObjectIdDictionary] backed by Delta Lake.
#[derive(Debug)]
pub struct DeltaObjectIdDictionary {
    /// The in-memory mapping.
    local_mapping: Arc<dyn LocalObjectIdDictionary>,
    /// The durable Delta table storing the mapping
    table: Arc<RwLock<DeltaTable>>,
    /// The schema of the table.
    table_schema: SchemaRef,
}

impl DeltaObjectIdDictionary {
    /// Creates a new [DeltaObjectIdDictionary] from a dictionary and a table.
    pub async fn try_new_at_location(
        options: &RdfFusionOptions,
        log_store: LogStoreRef,
    ) -> Result<Self, DeltaQuadsStorageError> {
        let delta_columns = vec![
            StructField::new("id", DeltaDataType::LONG, false),
            StructField::new(
                "term",
                DeltaDataType::try_from_arrow(PLAIN_TERM_ENCODING.data_type()).unwrap(),
                true,
            ),
        ];
        let arrow_columns = delta_columns
            .iter()
            .map(|c| Field::try_from_kernel(c).expect("Valid field"))
            .collect::<Vec<_>>();

        let table = CreateBuilder::new()
            .with_log_store(Arc::clone(&log_store))
            .with_columns(delta_columns)
            .await?;
        let table_schema = Arc::new(Schema::new(arrow_columns));

        let db_path = get_rocksdb_path(options, table.log_store().config().location());
        let put_mode = match options.storage.delta.assume_single_node {
            true => ObjectIdClaimerPutMode::AlwaysOverwrite,
            false => ObjectIdClaimerPutMode::EnsureVersion,
        };
        let claimer = Arc::new(DeltaObjectIdClaimer::new(
            Arc::clone(&log_store),
            options.storage.delta.object_id_claim_size,
            put_mode,
        ));
        let local_mapping: Arc<dyn LocalObjectIdDictionary> = if let Some(path) = db_path
        {
            info!(
                "Creating rocksdb local dictionary at directory '{}'.",
                path.display()
            );
            Arc::new(RocksDBObjectIdDictionary::try_new(
                path,
                options.storage.delta.object_id_cache_size,
                claimer,
            )?)
        } else {
            info!("Creating in-memory local dictionary at directory.");
            Arc::new(InMemoryObjectIdDictionary::new(claimer))
        };

        Ok(Self {
            local_mapping,
            table: Arc::new(RwLock::new(table)),
            table_schema,
        })
    }

    pub async fn try_load(
        session: &SessionState,
        log_store: LogStoreRef,
    ) -> Result<Self, DeltaQuadsStorageError> {
        let mut table =
            DeltaTable::new(Arc::clone(&log_store), DeltaTableConfig::default());
        table.load().await?;

        let delta_columns = [
            StructField::new("id", DeltaDataType::LONG, false),
            StructField::new(
                "term",
                DeltaDataType::try_from_arrow(PLAIN_TERM_ENCODING.data_type()).unwrap(),
                true,
            ),
        ];
        let arrow_columns = delta_columns
            .iter()
            .map(|c| Field::try_from_kernel(c).expect("Valid field"))
            .collect::<Vec<_>>();
        let table_schema = Arc::new(Schema::new(arrow_columns));

        info!("Loaded global object id mapping table state.");

        let options = session.config().rdf_fusion_options_or_from_env()?;
        let put_mode = match options.storage.delta.assume_single_node {
            true => ObjectIdClaimerPutMode::AlwaysOverwrite,
            false => ObjectIdClaimerPutMode::EnsureVersion,
        };
        let claimer = Arc::new(DeltaObjectIdClaimer::new(
            Arc::clone(&log_store),
            options.storage.delta.object_id_claim_size,
            put_mode,
        ));

        let db_path = get_rocksdb_path(&options, table.log_store().config().location());
        let local_dictionary: Arc<dyn LocalObjectIdDictionary> =
            if let Some(path) = db_path {
                Arc::new(RocksDBObjectIdDictionary::try_new(
                    path,
                    options.storage.delta.object_id_cache_size,
                    claimer,
                )?)
            } else {
                Arc::new(InMemoryObjectIdDictionary::new(claimer))
            };

        let mapping = Self {
            local_mapping: local_dictionary,
            table: Arc::new(RwLock::new(table)),
            table_schema,
        };

        info!("Trying to update local dictionary ...");
        mapping.update_local_dictionary().await?;
        info!("Local dictionary up-to-date.");

        Ok(mapping)
    }

    /// Returns a reference to the underlying dictionary.
    pub fn dictionary(&self) -> Arc<dyn LocalObjectIdDictionary> {
        Arc::clone(&self.local_mapping)
    }

    /// Returns the current version of the Delta Table.
    pub async fn delta_version(&self) -> u64 {
        self.table.read().await.version().unwrap_or(0) as u64
    }

    /// Synchronizes the local object ID dictionary with the global Delta table.
    ///
    /// This method checks the version of the local dictionary against the global table.
    /// - If the local dictionary is up-to-date, it does nothing.
    /// - If the local dictionary has no version (first run), it performs a full sync.
    /// - If the local dictionary is behind, it performs an incremental sync, fetching
    ///   only the new Parquet files added since the last sync.
    pub async fn update_local_dictionary(&self) -> Result<(), DeltaQuadsStorageError> {
        let mut table = self.table.write().await;
        table.load().await?;

        let local_version = self
            .local_mapping
            .snapshot()
            .await?
            .get_synced_version()?
            .unwrap_or(0);

        let table_version = table.version().unwrap_or(0);

        if local_version == table_version as u64 {
            return Ok(());
        }

        if local_version > table_version as u64 {
            return Err(DeltaQuadsStorageError::Corruption(format!(
                "The local dictionary has a higher version than the global dictionary (local: {local_version}, global: {table_version})."
            )));
        }

        let session_ctx = SessionContext::new();
        let mut txn = self.local_mapping.transaction().await?;

        self.sync_incrementally(
            &session_ctx,
            &table,
            &mut *txn,
            local_version,
            table_version as u64,
        )
        .await?;

        txn.commit(table_version as u64).await?;

        Ok(())
    }

    /// Performs an incremental sync by reading only the newly added Parquet files.
    ///
    /// Instead of loading the entire Delta snapshot, this registers the Delta object store
    /// directly with DataFusion and feeds it exactly the URLs of the new Parquet files.
    async fn sync_incrementally(
        &self,
        session_ctx: &SessionContext,
        table: &DeltaTable,
        txn: &mut dyn LocalObjectIdTransaction,
        v_on_disk: u64,
        table_version: u64,
    ) -> Result<(), DeltaQuadsStorageError> {
        info!(
            "Syncing local dictionary incrementally from version {} to Delta table version {}...",
            v_on_disk, table_version
        );

        let log_store = table.log_store();
        let added_files =
            get_added_files_from_commits(log_store, v_on_disk + 1, table_version).await?;

        info!(
            "Adding {} files from the global dictionary to the local dictionary ...",
            added_files.len()
        );
        if !added_files.is_empty() {
            let df = session_ctx
                .read_parquet(
                    added_files,
                    datafusion::prelude::ParquetReadOptions::default(),
                )
                .await?;

            let mut stream = df.execute_stream().await?;
            while let Some(batch) = stream.next().await {
                txn.add_global_batch(&batch?).await?;
            }
        }

        info!("Local dictionary sync complete.");

        Ok(())
    }

    /// Flushes the object id table to disk.
    pub async fn flush(&self) -> Result<(), DeltaQuadsStorageError> {
        Ok(())
    }

    /// Commits the current transaction to Delta Lake. Returns Ok(true) if successful, Ok(false) on
    /// conflict.
    pub async fn commit_dictionary_transaction_to_delta(
        &self,
        txn: &dyn LocalObjectIdTransaction,
    ) -> Result<bool, DeltaQuadsStorageError> {
        let pending_count = txn.pending_ids().len();
        if pending_count == 0 {
            return Ok(true);
        }

        let mut table = self.table.write().await;

        let mut id_builder = Int64Builder::with_capacity(pending_count);
        let mut term_builder = PlainTermArrayElementBuilder::new();

        for (id, term) in txn.pending_ids() {
            id_builder.append_value(*id);
            term_builder.append_raw(
                term.0,
                &term.1,
                term.2.as_deref(),
                term.3.as_deref(),
            );
        }

        let term_array = term_builder.finish().into_array_ref();
        let id_array = Arc::new(id_builder.finish()) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::clone(&self.table_schema),
            vec![id_array, term_array],
        )?;

        let mut writer = RecordBatchWriter::for_table(&table)?;
        writer.write(batch).await?;
        let add_actions = writer.flush().await?;

        let table_state = table.state.as_ref().expect("Table state loaded");
        let current_version = table_state.version();
        let should_checkpoint = current_version > 0 && current_version % 10 == 0;

        let commit_properties = CommitProperties::default()
            .with_create_checkpoint(should_checkpoint)
            .with_max_retries(0);

        let commit_result = CommitBuilder::from(commit_properties)
            .with_actions(add_actions.into_iter().map(Action::Add).collect())
            .build(
                Some(table_state),
                table.log_store(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await;

        match commit_result {
            Ok(state) => {
                table.state = Some(state.snapshot);
                Ok(true)
            }
            Err(DeltaTableError::VersionAlreadyExists(_)) => Ok(false),
            Err(DeltaTableError::Transaction {
                source: TransactionError::MaxCommitAttempts(_),
            }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl ObjectIdDictionary for DeltaObjectIdDictionary {
    fn object_id_data_type(&self) -> ObjectIdDataType {
        ObjectIdDataType::Int64
    }

    async fn try_get_object_id(
        &self,
        term: &PlainTermScalar,
    ) -> Result<Option<ScalarValue>, ObjectIdDictionaryError> {
        let snapshot = self
            .local_mapping
            .snapshot()
            .await
            .map_err(|e| ObjectIdDictionaryError::Storage(Box::new(e)))?;
        if let Some(id) = snapshot.get_id_by_term(term) {
            Ok(Some(ScalarValue::Int64(Some(id))))
        } else {
            Ok(None)
        }
    }

    async fn encode_array(
        &self,
        array: &PlainTermArray,
    ) -> Result<ArrayRef, ObjectIdDictionaryError> {
        let mut txn = self
            .local_mapping
            .transaction()
            .await
            .map_err(|e| ObjectIdDictionaryError::Storage(Box::new(e)))?;
        let ids_array = txn
            .encode_array(array)
            .await
            .map_err(|e| ObjectIdDictionaryError::Storage(Box::new(e)))?;
        txn.commit(self.delta_version().await)
            .await
            .map_err(|e| ObjectIdDictionaryError::Storage(Box::new(e)))?;

        Ok(Arc::new(ids_array) as ArrayRef)
    }

    async fn decode_array(
        &self,
        array: &ArrayRef,
    ) -> Result<PlainTermArray, ObjectIdDictionaryError> {
        let id_array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            ObjectIdDictionaryError::UnexpectedObjectIdFormat(format!(
                "Expected Int64Array, got {:?}",
                array.data_type()
            ))
        })?;

        let term_col = self
            .local_mapping
            .snapshot()
            .await
            .map_err(|e| ObjectIdDictionaryError::Storage(Box::new(e)))?
            .resolve_plain_terms(id_array)
            .map_err(|e| ObjectIdDictionaryError::Storage(Box::new(e)))?;

        let result =
            PlainTermArray::try_from(term_col).expect("Should be valid PlainTermArray");
        Ok(result)
    }

    async fn decode_array_to_typed_family(
        &self,
        encoding: &TypedFamilyEncodingRef,
        array: &ArrayRef,
    ) -> Result<TypedFamilyArray, ObjectIdDictionaryError> {
        let id_array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            ObjectIdDictionaryError::UnexpectedObjectIdFormat(format!(
                "Expected Int64Array, got {:?}",
                array.data_type()
            ))
        })?;

        let typed_value_col = self
            .local_mapping
            .snapshot()
            .await
            .map_err(|e| ObjectIdDictionaryError::Storage(Box::new(e)))?
            .resolve_plain_terms(id_array)
            .map_err(|e| ObjectIdDictionaryError::Storage(Box::new(e)))?;

        let plain_terms = PLAIN_TERM_ENCODING
            .try_new_array(typed_value_col)
            .expect("Decoded Plain Term Array");
        let result = encoding.cast_from_plain_term_array(&plain_terms)?;

        Ok(result)
    }
}

/// Extracts the relative paths of newly added Parquet files from the Delta commit logs.
///
/// Scans the JSONL transaction logs between `start_version` and `end_version` (inclusive)
/// and filters for `Action::Add` entries to identify exactly what data was appended.
async fn get_added_files_from_commits(
    log_store: LogStoreRef,
    start_version: u64,
    end_version: u64,
) -> Result<Vec<String>, DeltaQuadsStorageError> {
    let mut added_files = Vec::new();

    for v in start_version..=end_version {
        let commit_bytes = log_store
            .read_commit_entry(v)
            .await
            .map_err(|e| {
                DeltaQuadsStorageError::Corruption(format!(
                    "Failed to read commit {v}: {e}"
                ))
            })?
            .ok_or_else(|| {
                DeltaQuadsStorageError::Corruption(format!(
                    "Missing commit entry for version {v}"
                ))
            })?;

        use std::io::BufRead;
        let cursor = std::io::Cursor::new(commit_bytes.as_ref());

        // Parse the JSONL transaction log to find newly added files
        for line in cursor.lines() {
            let line =
                line.map_err(|e| DeltaQuadsStorageError::Corruption(e.to_string()))?;

            if line.trim().is_empty() {
                continue;
            }

            let action: Action = serde_json::from_str(&line).map_err(|e| {
                DeltaQuadsStorageError::Corruption(format!(
                    "Failed to parse commit log {v}: {e}"
                ))
            })?;

            if let Action::Add(add) = action {
                added_files.push(add.path);
            }
        }
    }

    Ok(added_files)
}
