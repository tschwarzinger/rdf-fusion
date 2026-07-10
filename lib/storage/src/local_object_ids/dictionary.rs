pub use crate::local_object_ids::error::LocalObjectIdError;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{Int64Type, SchemaRef};
use datafusion::common::runtime::SpawnedTask;
use deltalake::arrow::array::Int64Builder;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{
    PlainTermArray, PlainTermArrayElementBuilder, PlainTermScalar,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

// Use a tuple format based on BorrowedDictionaryTerm that can be directly used in redb.
type TermTuple<'a> = (i8, &'a str, Option<&'a str>, Option<&'a str>);

const ID_TO_TERM: TableDefinition<i64, TermTuple<'static>> =
    TableDefinition::new("id_to_term");
const TERM_TO_ID: TableDefinition<TermTuple<'static>, i64> =
    TableDefinition::new("term_to_id");
const METADATA: TableDefinition<&str, &str> = TableDefinition::new("metadata");

const SYNCED_VERSION_KEY: &str = "synced_version";
const NEXT_ID_KEY: &str = "next_id";

/// Implements a mapping for ObjectIds using redb as a backend, optimized for Tokio.
#[derive(Debug, Clone)]
pub struct LocalObjectIdDictionary {
    // Wrapped in Arc to safely pass across spawn_blocking boundaries
    db: Arc<Database>,
    next_id: Arc<AtomicI64>,
}

impl LocalObjectIdDictionary {
    /// Creates a new object id dictionary (file-backed or in-memory).
    pub fn try_new(path: Option<std::path::PathBuf>) -> Result<Self, LocalObjectIdError> {
        let db = if let Some(path) = &path {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    LocalObjectIdError::Storage(redb::StorageError::Io(e))
                })?;
            }
            Database::create(path)?
        } else {
            Database::builder()
                .create_with_backend(redb::backends::InMemoryBackend::new())?
        };

        let mut next_id_val = 0;
        {
            let write_txn = db.begin_write()?;
            write_txn.open_table(ID_TO_TERM)?;
            write_txn.open_table(TERM_TO_ID)?;
            {
                let metadata_table = write_txn.open_table(METADATA)?;
                if let Some(val) = metadata_table.get(NEXT_ID_KEY)? {
                    next_id_val = val
                        .value()
                        .parse::<i64>()
                        .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))?;
                }
            }
            write_txn.commit()?;
        }

        Ok(Self {
            db: Arc::new(db),
            next_id: Arc::new(AtomicI64::new(next_id_val)),
        })
    }

    /// Creates a new in-memory object id dictionary.
    pub fn try_new_in_memory() -> Result<Self, LocalObjectIdError> {
        Self::try_new(None)
    }

    /// Encodes RDF Terms into Object IDs, assigning new IDs if necessary.
    pub async fn encode_array(
        &self,
        array: &PlainTermArray,
    ) -> Result<Int64Array, LocalObjectIdError> {
        let array_clone = array.clone();
        let db = Arc::clone(&self.db);
        let next_id_ref = Arc::clone(&self.next_id);

        SpawnedTask::spawn_blocking(move || {
            let array_parts = array_clone.as_parts();
            let mut result_ids = Int64Builder::with_capacity(array_clone.len());

            let write_txn = db.begin_write()?;
            {
                let mut id_to_term_table = write_txn.open_table(ID_TO_TERM)?;
                let mut term_to_id_table = write_txn.open_table(TERM_TO_ID)?;

                for idx in 0..array_clone.len() {
                    if array_clone.inner().is_null(idx) {
                        result_ids.append_null();
                        continue;
                    }

                    let term_type = array_parts.term_type.value(idx);
                    let value = array_parts.value.value(idx);
                    let data_type = array_parts
                        .data_type
                        .is_valid(idx)
                        .then(|| array_parts.data_type.value(idx));
                    let language = array_parts
                        .language_tag
                        .is_valid(idx)
                        .then(|| array_parts.language_tag.value(idx));

                    let term_tuple = (term_type, value, data_type, language);

                    if let Some(id_val) = term_to_id_table.get(term_tuple)? {
                        let id = id_val.value();
                        result_ids.append_value(id);
                    } else {
                        let id = next_id_ref.fetch_add(1, Ordering::Relaxed);

                        term_to_id_table.insert(term_tuple, id)?;
                        id_to_term_table.insert(id, term_tuple)?;

                        result_ids.append_value(id);
                    }
                }

                let current_next_id = next_id_ref.load(Ordering::Relaxed).to_string();
                let mut metadata_table = write_txn.open_table(METADATA)?;
                metadata_table.insert(NEXT_ID_KEY, current_next_id.as_str())?;
            }
            write_txn.commit()?;

            Ok(result_ids.finish())
        })
        .await
        .expect("spawn_blocking panicked")
    }

    /// Resolves a bulk of Object IDs into their corresponding RDF Plain Terms
    pub async fn resolve_plain_terms(
        &self,
        ids: &Int64Array,
    ) -> Result<ArrayRef, LocalObjectIdError> {
        let ids_clone = ids.clone();
        let db = Arc::clone(&self.db);

        SpawnedTask::spawn_blocking(move || {
            let len = ids_clone.len();
            let mut builder = PlainTermArrayElementBuilder::with_capacity(len);

            let mut resolved_terms = Vec::new();
            let mut resolved_terms_indices: Vec<usize> = Vec::with_capacity(len);

            let read_txn = db.begin_read()?;
            let id_to_term_table = read_txn.open_table(ID_TO_TERM)?;

            for idx in 0..len {
                if ids_clone.is_null(idx) {
                    resolved_terms_indices.push(usize::MAX);
                    continue;
                };
                let id = ids_clone.value(idx);

                if let Some(db_term_val) = id_to_term_table.get(id)? {
                    let access_guard_idx = resolved_terms.len();
                    resolved_terms.push(db_term_val);
                    resolved_terms_indices.push(access_guard_idx);
                } else {
                    return Err(LocalObjectIdError::NotFound(id));
                }
            }

            let mut i = 0;
            while i < len {
                let resolved_term_idx = resolved_terms_indices[i];
                if resolved_term_idx == usize::MAX {
                    builder.append_null();
                    i += 1;
                    continue;
                }

                let resolved_term = &resolved_terms[resolved_term_idx];
                let (term_type, value, data_type, language) = resolved_term.value();
                builder.append_raw(term_type, value, data_type, language);

                i += 1;

                // Fast path for repeated terms.
                let mut j = i;
                while j < len {
                    if resolved_terms_indices[j] == resolved_term_idx {
                        builder.append_raw(term_type, value, data_type, language);
                        j += 1;
                    } else {
                        i = j;
                        break;
                    }
                }
            }

            Ok(builder.finish().into_array_ref())
        })
        .await
        .expect("spawn_blocking panicked")
    }

    /// Returns a list of RecordBatches for terms between start_id and self.next_id.
    pub async fn read_batches_since_id(
        &self,
        start_id: i64,
        schema: &SchemaRef,
    ) -> Result<Vec<RecordBatch>, LocalObjectIdError> {
        let current_next_id = self.next_id.load(Ordering::Relaxed);
        if start_id >= current_next_id {
            return Ok(vec![]);
        }

        let db = Arc::clone(&self.db);
        let schema_clone = Arc::clone(schema);

        SpawnedTask::spawn_blocking(move || {
            const CHUNK_SIZE: usize = 8192;
            let read_txn = db.begin_read()?;
            let id_to_term_table = read_txn.open_table(ID_TO_TERM)?;

            let mut batches = Vec::new();
            let mut current_id = start_id;

            while current_id < current_next_id {
                let total_remaining = (current_next_id - current_id) as usize;
                let take = total_remaining.min(CHUNK_SIZE);

                let mut builder = PlainTermArrayElementBuilder::new();
                let ids_slice = Arc::new(Int64Array::from_iter_values(
                    current_id..(current_id + take as i64),
                ));

                for id in current_id..(current_id + take as i64) {
                    if let Some(db_term_val) = id_to_term_table.get(&id)? {
                        let (term_type, value, data_type, language) = db_term_val.value();
                        builder.append_raw(term_type, value, data_type, language);
                    } else {
                        return Err(LocalObjectIdError::NotFound(id));
                    }
                }

                let terms_array = builder.finish().into_array_ref();
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema_clone),
                    vec![ids_slice, terms_array],
                )?;

                batches.push(batch);
                current_id += take as i64;
            }

            Ok(batches)
        })
        .await
        .expect("spawn_blocking panicked")
    }

    pub fn next_id(&self) -> i64 {
        self.next_id.load(Ordering::Relaxed)
    }

    pub async fn get_id_by_term(&self, term: &PlainTermScalar) -> Option<i64> {
        let term = term.clone();
        let db = Arc::clone(&self.db);

        SpawnedTask::spawn_blocking(move || {
            let parts = term.as_parts()?;

            let read_txn = db.begin_read().ok()?;
            let term_to_id_table = read_txn.open_table(TERM_TO_ID).ok()?;

            let term_tuple = (
                parts.term_type,
                parts.value,
                parts.data_type,
                parts.language_tag,
            );

            let id = term_to_id_table.get(term_tuple).ok()??.value();
            Some(id)
        })
        .await
        .ok()
        .flatten()
    }

    /// Loads a RecordBatch of `(id, term)` into the mapping.
    pub async fn add_batch(&self, batch: &RecordBatch) -> Result<(), LocalObjectIdError> {
        let batch_clone = batch.clone();
        let db = Arc::clone(&self.db);
        let next_id_ref = Arc::clone(&self.next_id);

        SpawnedTask::spawn_blocking(move || {
            let id_col = batch_clone
                .column_by_name("id")
                .expect("Missing 'id' column")
                .as_primitive::<Int64Type>();

            let term_col = batch_clone
                .column_by_name("term")
                .expect("Missing 'term' column");
            let plain_term_array = PlainTermArray::try_from(Arc::clone(term_col))?;
            let array_parts = plain_term_array.as_parts();

            let write_txn = db.begin_write()?;
            {
                let mut id_to_term_table = write_txn.open_table(ID_TO_TERM)?;
                let mut term_to_id_table = write_txn.open_table(TERM_TO_ID)?;

                let mut max_id = -1i64;

                for i in 0..batch_clone.num_rows() {
                    let id = id_col.value(i);
                    max_id = max_id.max(id);

                    let term_type = array_parts.term_type.value(i);
                    let value = array_parts.value.value(i);
                    let data_type = array_parts
                        .data_type
                        .is_valid(i)
                        .then(|| array_parts.data_type.value(i));
                    let language = array_parts
                        .language_tag
                        .is_valid(i)
                        .then(|| array_parts.language_tag.value(i));

                    let term_tuple = (term_type, value, data_type, language);

                    id_to_term_table.insert(id, term_tuple)?;
                    term_to_id_table.insert(term_tuple, id)?;
                }

                if max_id >= 0 {
                    let new_next_id = max_id + 1;
                    let mut current = next_id_ref.load(Ordering::Relaxed);
                    while new_next_id > current {
                        match next_id_ref.compare_exchange_weak(
                            current,
                            new_next_id,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(actual) => current = actual,
                        }
                    }
                }

                let current_next_id = next_id_ref.load(Ordering::Relaxed).to_string();
                let mut metadata_table = write_txn.open_table(METADATA)?;
                metadata_table.insert(NEXT_ID_KEY, current_next_id.as_str())?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .expect("spawn_blocking panicked")
    }

    pub async fn set_synced_version(
        &self,
        version: u64,
    ) -> Result<(), LocalObjectIdError> {
        let db = Arc::clone(&self.db);
        SpawnedTask::spawn_blocking(move || {
            let write_txn = db.begin_write()?;
            {
                let mut metadata_table = write_txn.open_table(METADATA)?;
                let version = version.to_string();
                metadata_table.insert(SYNCED_VERSION_KEY, version.as_str())?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await
        .expect("spawn_blocking panicked")
    }

    pub async fn get_synced_version(&self) -> Result<Option<u64>, LocalObjectIdError> {
        let db = Arc::clone(&self.db);
        SpawnedTask::spawn_blocking(move || {
            let read_txn = db.begin_read()?;
            let metadata_table = read_txn.open_table(METADATA)?;
            let version = metadata_table.get(SYNCED_VERSION_KEY)?;

            match version {
                None => Ok(None),
                Some(str_value) => str_value
                    .value()
                    .parse::<u64>()
                    .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))
                    .map(Some),
            }
        })
        .await
        .expect("spawn_blocking panicked")
    }
}
