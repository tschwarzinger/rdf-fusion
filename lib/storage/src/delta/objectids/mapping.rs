use crate::delta::error::DeltaQuadStorageError;
use crate::delta::objectids::{DeltaObjectIdClaimer, ObjectIdClaimerPutMode};
use crate::local_object_ids::{LocalObjectIdDictionary, LocalObjectIdTransaction};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, Int64Builder, RecordBatch};
use datafusion::arrow::datatypes::{Field, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::execution::SessionState;
use datafusion::logical_expr::col;
use datafusion::prelude::SessionContext;
use deltalake::arrow::datatypes::Schema;
use deltalake::delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
use deltalake::kernel::Action;
use deltalake::kernel::engine::arrow_conversion::{TryFromArrow, TryFromKernel};
use deltalake::kernel::transaction::{CommitBuilder, TableReference, TransactionError};
use deltalake::logstore::LogStoreRef;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{
    DataType as DeltaDataType, DeltaTable, DeltaTableConfig, DeltaTableError, StructField,
};
use futures::StreamExt;
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
use tracing::info;

fn get_redb_path(
    options: &RdfFusionOptions,
    location: &url::Url,
) -> Option<std::path::PathBuf> {
    if location.scheme() == "memory" {
        return None;
    }

    if let Some(ref work_dir) = options.local.work_dir {
        let mut safe_name = String::new();
        let mut last_was_underscore = false;
        for c in location.as_str().chars() {
            if c.is_alphanumeric() {
                safe_name.push(c);
                last_was_underscore = false;
            } else if !last_was_underscore {
                safe_name.push('_');
                last_was_underscore = true;
            }
        }
        let safe_name = safe_name.trim_end_matches('_');
        let file_name = format!("{safe_name}.redb");
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
    local_mapping: Arc<LocalObjectIdDictionary>,
    /// The durable Delta table storing the mapping
    table: Arc<tokio::sync::RwLock<DeltaTable>>,
    /// The schema of the table.
    table_schema: SchemaRef,
}

impl DeltaObjectIdDictionary {
    /// Creates a new [DeltaObjectIdDictionary] from a dictionary and a table.
    pub async fn try_new_at_location(
        options: &RdfFusionOptions,
        log_store: LogStoreRef,
    ) -> Result<Self, DeltaQuadStorageError> {
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

        let db_path = get_redb_path(options, table.log_store().config().location());
        let put_mode = match options.storage.delta.assume_single_node {
            true => ObjectIdClaimerPutMode::AlwaysOverwrite,
            false => ObjectIdClaimerPutMode::EnsureVersion,
        };
        let claimer = Arc::new(DeltaObjectIdClaimer::new(
            Arc::clone(&log_store),
            options.storage.delta.object_id_claim_size,
            put_mode,
        ));
        let in_memory_mapping = LocalObjectIdDictionary::try_new(
            db_path,
            options.storage.delta.object_id_cache_size,
            claimer,
        )?;

        let mut txn = in_memory_mapping.transaction().await?;
        txn.set_synced_version(table.version().unwrap_or(0))?;
        txn.commit()?;

        Ok(Self {
            local_mapping: Arc::new(in_memory_mapping),
            table: Arc::new(tokio::sync::RwLock::new(table)),
            table_schema,
        })
    }

    pub async fn try_load(
        session: &SessionState,
        log_store: LogStoreRef,
    ) -> Result<Self, DeltaQuadStorageError> {
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

        info!("Loaded object id mapping state. Rebuilding in-memory dictionary ...");

        let options = session.config().rdf_fusion_options_or_from_env()?;
        let db_path = get_redb_path(&options, table.log_store().config().location());
        let put_mode = match options.storage.delta.assume_single_node {
            true => ObjectIdClaimerPutMode::AlwaysOverwrite,
            false => ObjectIdClaimerPutMode::EnsureVersion,
        };
        let claimer = Arc::new(DeltaObjectIdClaimer::new(
            Arc::clone(&log_store),
            options.storage.delta.object_id_claim_size,
            put_mode,
        ));
        let in_memory_mapping = LocalObjectIdDictionary::try_new(
            db_path,
            options.storage.delta.object_id_cache_size,
            claimer,
        )?;

        let mapping = Self {
            local_mapping: Arc::new(in_memory_mapping),
            table: Arc::new(tokio::sync::RwLock::new(table)),
            table_schema,
        };

        mapping.update_local_dictionary().await?;

        Ok(mapping)
    }

    /// Returns a reference to the underlying dictionary.
    pub fn dictionary(&self) -> Arc<LocalObjectIdDictionary> {
        Arc::clone(&self.local_mapping)
    }

    pub async fn update_local_dictionary(&self) -> Result<(), DeltaQuadStorageError> {
        let mut table = self.table.write().await;
        table.load().await?;

        let version_on_disk = self.local_mapping.snapshot()?.get_synced_version()?;
        let table_version = table.version().unwrap_or(0);

        if let Some(v_on_disk) = version_on_disk {
            if v_on_disk >= table_version {
                return Ok(());
            }
        }

        info!(
            "Syncing local dictionary from version {:?} to Delta table version {}...",
            version_on_disk, table_version
        );

        let session_ctx = SessionContext::new();
        let table_provider = DeltaTableProvider::try_new(
            table.snapshot()?.eager_snapshot().clone(),
            table.log_store(),
            DeltaScanConfig::default(),
        )?;

        let df = session_ctx.read_table(Arc::new(table_provider))?;
        let df = df.sort(vec![col("id").sort(true, false)])?;

        let mut stream = df.execute_stream().await?;
        let mut txn = self.local_mapping.transaction().await?;
        while let Some(batch) = stream.next().await {
            txn.add_global_batch(&batch?).await?;
        }

        txn.set_synced_version(table_version as u64)?;
        txn.commit()?;

        Ok(())
    }

    /// Flushes the object id table to disk.
    pub async fn flush(&self) -> Result<(), DeltaQuadStorageError> {
        Ok(())
    }

    /// Flushes the current transaction to Delta Lake. Returns Ok(true) if successful, Ok(false) on
    /// conflict.
    pub async fn commit_dictionary_transaction_to_delta(
        &self,
        txn: &LocalObjectIdTransaction,
    ) -> Result<bool, DeltaQuadStorageError> {
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

        let commit_result = CommitBuilder::default()
            .with_actions(add_actions.into_iter().map(Action::Add).collect())
            .with_max_retries(0)
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
            Ok(_) => {
                table.load().await?;
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
        txn.commit()
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
