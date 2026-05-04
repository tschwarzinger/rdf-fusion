use crate::delta::error::DeltaQuadStorageError;
use crate::delta::objectids::mapping_in_memory::ObjectIdInMemoryMapping;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{Field, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::execution::SessionState;
use datafusion::logical_expr::col;
use datafusion::prelude::SessionContext;
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
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::object_id::{
    ObjectIdDataType, ObjectIdMapping, ObjectIdMappingError,
};
use rdf_fusion_encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArray, PlainTermScalar,
};
use rdf_fusion_encoding::typed_family::{TypedFamilyArray, TypedFamilyEncodingRef};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tracing::info;

/// Implements [ObjectIdMapping] using a [ObjectIdInMemoryMapping] backed by Delta Lake.
#[derive(Debug)]
pub struct DeltaObjectIdMapping {
    /// The in-memory mapping.
    ///
    /// We use a synchronous lock to avoid async in the encoding/decoding functions.
    in_memory_mapping: Arc<RwLock<ObjectIdInMemoryMapping>>,
    /// The durable Delta table storing the mapping
    table: Arc<tokio::sync::RwLock<DeltaTable>>,
    /// The schema of the table.
    table_schema: SchemaRef,
    /// Tracks the highest ID that has been durably written to Delta Table. We are using a Mutex
    /// to avoid race conditions between multiple inserts. In the future, we may want an improved
    /// version that allows multiple concurrent (non-overlapping) flushes, but the future only
    /// resolves once all flushes that came before are also resolved.
    flush_lock: Arc<Mutex<i64>>,
}

impl DeltaObjectIdMapping {
    /// Creates a new [DeltaObjectIdMapping] from a dictionary and a table.
    pub async fn try_new_at_location(
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

        Ok(Self {
            in_memory_mapping: Arc::new(RwLock::new(ObjectIdInMemoryMapping::empty())),
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

        let mut in_memory_mapping = ObjectIdInMemoryMapping::empty();
        let session = SessionContext::new_with_state(session.clone());
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

        // Build a DataFrame, sort by `id` ascending, and execute
        let df = session.read_table(Arc::new(table_provider))?;
        let df = df.sort(vec![col("id").sort(true, false)])?;

        let mut stream = df.execute_stream().await?;
        while let Some(batch) = stream.next().await {
            in_memory_mapping.add_batch(&batch?)?;
        }

        let highest_flushed_id = in_memory_mapping.next_id().saturating_sub(1);
        Ok(Self {
            in_memory_mapping: Arc::new(RwLock::new(in_memory_mapping)),
            table: Arc::new(tokio::sync::RwLock::new(table)),
            table_schema,
            flush_lock: Arc::new(Mutex::new(highest_flushed_id)),
        })
    }

    /// Returns a reference to the underlying dictionary.
    pub fn dictionary(&self) -> Arc<RwLock<ObjectIdInMemoryMapping>> {
        Arc::clone(&self.in_memory_mapping)
    }

    /// Flushes the object id table to disk.
    pub async fn flush(&self) -> Result<(), DeltaQuadStorageError> {
        let guard = self.flush_lock.lock().await;
        let last_flushed = *guard;

        let batches = {
            let dictionary = self
                .in_memory_mapping
                .read()
                .expect("In-memory mapping lock is poisoned");
            let current_id = dictionary.next_id();

            // Nothing to flush, we're done
            if current_id <= last_flushed {
                return Ok(());
            }

            dictionary.read_batches_since_id(last_flushed, &self.table_schema)?
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

        Ok(())
    }
}

impl ObjectIdMapping for DeltaObjectIdMapping {
    fn object_id_data_type(&self) -> ObjectIdDataType {
        ObjectIdDataType::Int64
    }

    fn try_get_object_id(
        &self,
        term: &PlainTermScalar,
    ) -> Result<Option<ScalarValue>, ObjectIdMappingError> {
        let dict = self
            .in_memory_mapping
            .read()
            .expect("In-memory mapping lock is poisoned");
        if let Some(id) = dict.get_id_by_term(term) {
            Ok(Some(ScalarValue::Int64(Some(id))))
        } else {
            Ok(None)
        }
    }

    fn encode_array(
        &self,
        array: &PlainTermArray,
    ) -> Result<ArrayRef, ObjectIdMappingError> {
        let ids_array = self
            .in_memory_mapping
            .write()
            .expect("In-memory mapping lock is poisoned")
            .encode_array(array)
            .map_err(ObjectIdMappingError::from)?;

        Ok(Arc::new(ids_array) as ArrayRef)
    }

    fn decode_array(
        &self,
        array: &ArrayRef,
    ) -> Result<PlainTermArray, ObjectIdMappingError> {
        let id_array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            ObjectIdMappingError::UnexpectedObjectIdFormat(format!(
                "Expected Int64Array, got {:?}",
                array.data_type()
            ))
        })?;

        let dict = self
            .in_memory_mapping
            .read()
            .expect("In-memory mapping lock is poisoned");
        let term_col = dict
            .resolve_plain_terms(id_array)
            .map_err(ObjectIdMappingError::from)?;

        let result =
            PlainTermArray::try_from(term_col).expect("Should be valid PlainTermArray");
        Ok(result)
    }

    fn decode_array_to_typed_family(
        &self,
        encoding: &TypedFamilyEncodingRef,
        array: &ArrayRef,
    ) -> Result<TypedFamilyArray, ObjectIdMappingError> {
        let id_array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            ObjectIdMappingError::UnexpectedObjectIdFormat(format!(
                "Expected Int64Array, got {:?}",
                array.data_type()
            ))
        })?;

        let dict = self
            .in_memory_mapping
            .read()
            .expect("In-memory mapping lock is poisoned");
        let typed_value_col = dict
            .resolve_plain_terms(id_array)
            .map_err(ObjectIdMappingError::from)?;

        let plain_terms = PLAIN_TERM_ENCODING
            .try_new_array(typed_value_col)
            .expect("Decoded Plain Term Array");
        let result = encoding.cast_from_plain_term_array(&plain_terms)?;

        Ok(result)
    }
}
