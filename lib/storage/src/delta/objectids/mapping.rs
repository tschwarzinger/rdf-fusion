use crate::delta::error::DeltaQuadStorageError;
use crate::local_object_ids::LocalObjectIdDictionary;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{Field, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::execution::SessionState;
use datafusion::logical_expr::col;
use datafusion::prelude::{SessionContext, lit};
use deltalake::arrow::datatypes::Schema;
use deltalake::delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
use deltalake::kernel::Action;
use deltalake::kernel::engine::arrow_conversion::{TryFromArrow, TryFromKernel};
use deltalake::kernel::transaction::{CommitBuilder, TableReference};
use deltalake::logstore::LogStoreRef;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{DataType as DeltaDataType, DeltaTable, DeltaTableConfig, StructField};
use futures::StreamExt;
use rdf_fusion_common::config::{RdfFusionOptions, RdfFusionSessionConfigExt};
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::object_id::{
    ObjectIdDataType, ObjectIdDictionary, ObjectIdDictionaryError,
};
use rdf_fusion_encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArray, PlainTermScalar,
};
use rdf_fusion_encoding::typed_family::{TypedFamilyArray, TypedFamilyEncodingRef};
use std::sync::Arc;
use tokio::sync::Mutex;
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
    /// Tracks the highest ID that has been durably written to Delta Table.
    flush_lock: Arc<Mutex<i64>>,
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
            .with_log_store(log_store)
            .with_columns(delta_columns)
            .await?;
        let table_schema = Arc::new(Schema::new(arrow_columns));

        let db_path = get_redb_path(options, table.log_store().config().location());
        let in_memory_mapping = LocalObjectIdDictionary::try_new(db_path)?;

        in_memory_mapping
            .set_synced_version(table.version().unwrap_or(0))
            .await?;

        Ok(Self {
            local_mapping: Arc::new(in_memory_mapping),
            table: Arc::new(tokio::sync::RwLock::new(table)),
            table_schema,
            flush_lock: Arc::new(Mutex::new(0)),
        })
    }

    pub async fn try_load(
        session: &SessionState,
        log_store: LogStoreRef,
    ) -> Result<Self, DeltaQuadStorageError> {
        let mut table = DeltaTable::new(log_store, DeltaTableConfig::default());
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
        let in_memory_mapping = LocalObjectIdDictionary::try_new(db_path)?;

        let version_on_disk = in_memory_mapping.get_synced_version().await?;
        let table_version = table.version().unwrap_or(0) as i64;

        let mut needs_sync = true;
        if let Some(v_on_disk) = version_on_disk {
            if v_on_disk as i64 >= table_version {
                info!(
                    "Local object ID dictionary is already synced to version {}",
                    table_version
                );
                needs_sync = false;
            }
        }

        if needs_sync {
            let start_id = in_memory_mapping.next_id();
            info!(
                "Syncing local dictionary from version {:?} to Delta table version {} (loading IDs >= {})...",
                version_on_disk, table_version, start_id
            );

            let session_ctx = SessionContext::new_with_state(session.clone());
            let table_provider = DeltaTableProvider::try_new(
                table.snapshot()?.eager_snapshot().clone(),
                table.log_store(),
                DeltaScanConfig::default(),
            )?;

            if let Some(stats) = table_provider.statistics() {
                if let Precision::Exact(num_rows) = stats.num_rows {
                    info!("Length of dictionary: {} rows", num_rows)
                }
            }

            let df = session_ctx.read_table(Arc::new(table_provider))?;
            let df = df.filter(col("id").gt_eq(lit(start_id)))?;
            let df = df.sort(vec![col("id").sort(true, false)])?;

            let mut stream = df.execute_stream().await?;
            while let Some(batch) = stream.next().await {
                in_memory_mapping.add_batch(&batch?).await?;
            }

            in_memory_mapping
                .set_synced_version(table_version as u64)
                .await?;
        }

        let highest_flushed_id = in_memory_mapping.next_id().saturating_sub(1);
        Ok(Self {
            local_mapping: Arc::new(in_memory_mapping),
            table: Arc::new(tokio::sync::RwLock::new(table)),
            table_schema,
            flush_lock: Arc::new(Mutex::new(highest_flushed_id)),
        })
    }

    /// Returns a reference to the underlying dictionary.
    pub fn dictionary(&self) -> Arc<LocalObjectIdDictionary> {
        Arc::clone(&self.local_mapping)
    }

    /// Flushes the object id table to disk.
    pub async fn flush(&self) -> Result<(), DeltaQuadStorageError> {
        let mut guard = self.flush_lock.lock().await;
        let last_flushed = *guard;

        let (batches, new_flushed) = {
            let current_id = self.local_mapping.next_id();

            // Nothing to flush, we're done
            if current_id <= last_flushed {
                return Ok(());
            }

            let b = self
                .local_mapping
                .read_batches_since_id(last_flushed, &self.table_schema)
                .await?;
            (b, current_id)
        };

        let table = self.table.read().await;

        let mut actions = Vec::new();
        let mut pending_rows = 0;
        let mut writer = RecordBatchWriter::for_table(&table)?;
        for batch in batches {
            pending_rows += batch.num_rows();
            writer.write(batch).await?;

            if pending_rows >= 1_000_000 {
                info!("Flushing ~1M object ids ...");
                actions.extend(writer.flush().await?);
                pending_rows = 0;
            }
        }
        actions.extend(writer.flush().await?);
        info!("Object id data files flushed.");

        let result = CommitBuilder::default()
            .with_actions(actions.into_iter().map(Action::Add).collect())
            .build(
                Some(table.snapshot()?),
                table.log_store(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await?;
        drop(table);

        let mut table = self.table.write().await;
        table.state = Some(result.snapshot);

        info!(
            "New object id table version committed. Txn id: {}",
            result.version
        );

        self.local_mapping
            .set_synced_version(result.version as u64)
            .await?;

        *guard = new_flushed;
        Ok(())
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
        if let Some(id) = self.local_mapping.get_id_by_term(term).await {
            Ok(Some(ScalarValue::Int64(Some(id))))
        } else {
            Ok(None)
        }
    }

    async fn encode_array(
        &self,
        array: &PlainTermArray,
    ) -> Result<ArrayRef, ObjectIdDictionaryError> {
        let ids_array = self
            .local_mapping
            .encode_array(array)
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
