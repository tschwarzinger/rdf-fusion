use crate::delta::error::DeltaQuadStorageError;
use crate::delta::objectids::mapping_in_memory::ObjectIdInMemoryMapping;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{Field, SchemaRef};
use datafusion::common::ScalarValue;
use deltalake::arrow::datatypes::Schema;
use deltalake::kernel::Action;
use deltalake::kernel::engine::arrow_conversion::{TryFromArrow, TryFromKernel};
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{DataType as DeltaDataType, DeltaTable, StructField};
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
        location: &str,
        typed_family_encoding: TypedFamilyEncodingRef,
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
            .with_location(location)
            .with_columns(delta_columns)
            .await?;
        let table_schema = Arc::new(Schema::new(arrow_columns));

        Ok(Self {
            in_memory_mapping: Arc::new(RwLock::new(ObjectIdInMemoryMapping::empty(
                typed_family_encoding,
            ))),
            table: Arc::new(tokio::sync::RwLock::new(table)),
            table_schema,
            flush_lock: Arc::new(Mutex::new(0)),
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

        let mut writer = RecordBatchWriter::for_table(&table)?;
        for batch in batches {
            writer.write(batch).await?;
        }
        let actions = writer.flush().await?;

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
            .resolve_typed_values(id_array)
            .map_err(ObjectIdMappingError::from)?;

        TypedFamilyArray::try_new(Arc::clone(encoding), Arc::clone(&typed_value_col))
            .map_err(|e| ObjectIdMappingError::IllegalArgument(e.to_string()))
    }
}
