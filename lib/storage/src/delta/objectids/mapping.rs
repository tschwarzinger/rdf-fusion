use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::objectids::encoding::writer::DeltaObjectIdDictionaryWriter;
use crate::delta::objectids::{DeltaObjectIdClaimer, ObjectIdClaimerPutMode};
use crate::local_object_ids::{
    InMemoryObjectIdDictionary, LmdbObjectIdDictionary, LocalObjectIdDictionary,
    LocalObjectIdTransaction,
};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, Int64Builder, RecordBatch};
use datafusion::arrow::datatypes::{Field, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionContext;
use deltalake::arrow::datatypes::Schema;
use deltalake::kernel::Action;
use deltalake::kernel::Add;
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

fn get_lmdb_path(
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

        let dir_name = hash.to_string();

        info!(
            "Using lmdb environment in directory '{dir_name}' for location '{location}'."
        );

        Some(
            std::path::PathBuf::from(work_dir)
                .join("dictionaries")
                .join(dir_name),
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
    /// The shared writer for parallel encoding.
    writer: Arc<DeltaObjectIdDictionaryWriter>,
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

        let db_path = get_lmdb_path(options, table.log_store().config().location());
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
                "Creating lmdb local dictionary at directory '{}'.",
                path.display()
            );
            Arc::new(LmdbObjectIdDictionary::try_new(
                path,
                options.storage.delta.object_id_cache_size,
                claimer,
            )?)
        } else {
            info!("Creating in-memory local dictionary at directory.");
            Arc::new(InMemoryObjectIdDictionary::new(claimer))
        };

        let table = Arc::new(RwLock::new(table));
        let delta_version = table.read().await.version().unwrap_or(0) as u64;
        let writer = Arc::new(DeltaObjectIdDictionaryWriter::new(
            Arc::clone(&local_mapping),
            delta_version + 1,
        ));

        Ok(Self {
            local_mapping,
            table,
            table_schema,
            writer,
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

        let db_path = get_lmdb_path(&options, table.log_store().config().location());
        let local_dictionary: Arc<dyn LocalObjectIdDictionary> =
            if let Some(path) = db_path {
                Arc::new(LmdbObjectIdDictionary::try_new(
                    path,
                    options.storage.delta.object_id_cache_size,
                    claimer,
                )?)
            } else {
                Arc::new(InMemoryObjectIdDictionary::new(claimer))
            };

        let table = Arc::new(RwLock::new(table));
        let delta_version = table.read().await.version().unwrap_or(0) as u64;
        let writer = Arc::new(DeltaObjectIdDictionaryWriter::new(
            Arc::clone(&local_dictionary),
            delta_version + 1,
        ));

        let mapping = Self {
            local_mapping: local_dictionary,
            table,
            table_schema,
            writer,
        };

        info!("Trying to update local dictionary ...");
        mapping.update_local_dictionary(session).await?;
        info!("Local dictionary up-to-date.");

        Ok(mapping)
    }

    /// Returns a reference to the underlying dictionary.
    pub fn dictionary(&self) -> Arc<dyn LocalObjectIdDictionary> {
        Arc::clone(&self.local_mapping)
    }

    /// Returns the shared writer for parallel encoding.
    pub fn shared_writer(&self) -> Arc<DeltaObjectIdDictionaryWriter> {
        Arc::clone(&self.writer)
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
    pub async fn update_local_dictionary(
        &self,
        session: &SessionState,
    ) -> Result<(), DeltaQuadsStorageError> {
        let mut table = self.table.write().await;
        table.load().await?;

        let local_version = self
            .local_mapping
            .snapshot()
            .await?
            .get_synced_version()
            .await?
            .unwrap_or(0);

        let global_version = table.version().unwrap_or(0);

        if local_version == global_version as u64 {
            return Ok(());
        }

        if local_version > global_version as u64 {
            return Err(DeltaQuadsStorageError::Corruption(format!(
                "The local dictionary has a higher version than the global dictionary (local: {local_version}, global: {global_version})."
            )));
        }

        let session_ctx = SessionContext::new_with_state(session.clone());

        info!(
            "Syncing local dictionary incrementally from version {} to Delta table version {}...",
            local_version, global_version
        );
        self.sync_incrementally(
            &session_ctx,
            &table,
            local_version,
            global_version as u64,
        )
        .await?;
        info!("Local dictionary updated.");

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
        v_on_disk: u64,
        table_version: u64,
    ) -> Result<(), DeltaQuadsStorageError> {
        let log_store = table.log_store();
        let transactions =
            get_added_files_from_commits(log_store, v_on_disk + 1, table_version).await?;

        let table_url = table.table_url();
        let base_url = table_url.as_str().trim_end_matches('/');

        info!(
            "Processing {} delta transactions to sync global dictionary...",
            transactions.len()
        );

        for tx in transactions {
            let file_paths: Vec<String> = tx
                .added_files
                .into_iter()
                .map(|add| format!("{}/{}", base_url, add.path))
                .collect();

            let mut txn = self.local_mapping.transaction().await?;

            if !file_paths.is_empty() {
                info!(
                    "Adding {} files from Delta version {} to the local dictionary...",
                    file_paths.len(),
                    tx.version
                );

                let df = session_ctx
                    .read_parquet(
                        file_paths,
                        datafusion::prelude::ParquetReadOptions::default(),
                    )
                    .await?;

                let mut stream = df.execute_stream().await?;
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    txn.add_global_batch(&batch).await?;
                }

                info!(
                    "Committing {} entries to dictionary for version {}...",
                    txn.pending_ids().len(),
                    tx.version
                );
            }

            txn.commit(tx.version).await?;
        }

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
                term.term_type,
                &term.value,
                term.data_type.as_deref(),
                term.language.as_deref(),
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
        if let Some(id) = snapshot.get_id_by_term(term).await {
            Ok(Some(ScalarValue::Int64(Some(id))))
        } else {
            Ok(None)
        }
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
            .await
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
            .await
            .map_err(|e| ObjectIdDictionaryError::Storage(Box::new(e)))?;

        let plain_terms = PLAIN_TERM_ENCODING
            .try_new_array(typed_value_col)
            .expect("Decoded Plain Term Array");
        let result = encoding.cast_from_plain_term_array(&plain_terms)?;

        Ok(result)
    }
}

struct DeltaTransactionFiles {
    version: u64,
    added_files: Vec<Add>,
}

/// Extracts the relative paths of newly added Parquet files from the Delta commit logs.
///
/// Scans the JSONL transaction logs between `start_version` and `end_version` (inclusive)
/// and filters for `Action::Add` entries to identify exactly what data was appended.
async fn get_added_files_from_commits(
    log_store: LogStoreRef,
    start_version: u64,
    end_version: u64,
) -> Result<Vec<DeltaTransactionFiles>, DeltaQuadsStorageError> {
    let mut transactions = Vec::new();

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

        let mut added_files = Vec::new();

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
                added_files.push(add);
            }
        }

        transactions.push(DeltaTransactionFiles {
            version: v,
            added_files,
        });
    }

    Ok(transactions)
}
