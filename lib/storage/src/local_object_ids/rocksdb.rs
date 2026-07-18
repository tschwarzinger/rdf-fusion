use super::{
    CF_ID_TO_TERM, CF_METADATA, CF_TERM_TO_ID, LAST_FREE_ID_KEY, NEXT_FREE_ID_KEY,
    ObjectIdClaimer, OwnedTermTuple, SYNCED_VERSION_KEY,
};
use crate::local_object_ids::claim::ObjectIdClaim;
use crate::local_object_ids::error::LocalObjectIdError;
use crate::local_object_ids::traits::{
    LocalObjectIdDictionary, LocalObjectIdDictionarySnapshot, LocalObjectIdTransaction,
};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::Int64Type;
use deltalake::arrow::array::Int64Builder;
use quick_cache::sync::Cache;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{
    PlainTermArray, PlainTermArrayElementBuilder, PlainTermScalar,
};
use rocksdb::{DB, Options, WriteBatch};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct RocksDBObjectIdDictionary {
    db: Arc<DB>,
    claimer: Arc<dyn ObjectIdClaimer>,
    id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
    term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,
}

impl RocksDBObjectIdDictionary {
    pub fn try_new(
        path: PathBuf,
        cache_size: usize,
        claimer: Arc<dyn ObjectIdClaimer>,
    ) -> Result<Self, LocalObjectIdError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cf_names = vec![CF_ID_TO_TERM, CF_TERM_TO_ID, CF_METADATA];
        let db = DB::open_cf(&opts, &path, cf_names)
            .map_err(|e| LocalObjectIdError::Corruption(e.to_string()))?;

        Ok(Self {
            db: Arc::new(db),
            claimer,
            id_to_term_cache: Arc::new(Cache::new(cache_size)),
            term_to_id_cache: Arc::new(Cache::new(cache_size)),
        })
    }
}

#[async_trait]
impl LocalObjectIdDictionary for RocksDBObjectIdDictionary {
    async fn snapshot(
        &self,
    ) -> Result<Arc<dyn LocalObjectIdDictionarySnapshot>, LocalObjectIdError> {
        Ok(Arc::new(RocksDBLocalObjectIdDictionarySnapshot {
            db: Arc::clone(&self.db),
            id_to_term_cache: Arc::clone(&self.id_to_term_cache),
            term_to_id_cache: Arc::clone(&self.term_to_id_cache),
        }))
    }

    async fn transaction(
        &self,
    ) -> Result<Box<dyn LocalObjectIdTransaction>, LocalObjectIdError> {
        let initial_claim = try_load_initial_claim_rocksdb(&self.db)?;
        Ok(Box::new(RocksDBLocalObjectIdTransaction {
            db: Arc::clone(&self.db),
            claimed_ids: ObjectIdClaim::new(
                initial_claim,
                Some(Arc::clone(&self.claimer)),
            ),
            id_to_term_cache: Arc::clone(&self.id_to_term_cache),
            term_to_id_cache: Arc::clone(&self.term_to_id_cache),
            reset_state_on_conflict: initial_claim,
            pending_terms: HashMap::new(),
            pending_ids: HashMap::new(),
        }))
    }
}

pub struct RocksDBLocalObjectIdDictionarySnapshot {
    db: Arc<DB>,
    id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
    term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,
}

impl LocalObjectIdDictionarySnapshot for RocksDBLocalObjectIdDictionarySnapshot {
    fn resolve_plain_terms(
        &self,
        ids: &Int64Array,
    ) -> Result<ArrayRef, LocalObjectIdError> {
        let len = ids.len();
        let mut builder = PlainTermArrayElementBuilder::with_capacity(len);
        let mut last_id: Option<i64> = None;
        let mut last_term: Option<Arc<OwnedTermTuple>> = None;

        let cf = self.db.cf_handle(CF_ID_TO_TERM).unwrap();

        for idx in 0..len {
            if ids.is_null(idx) {
                builder.append_null();
                continue;
            };
            let id = ids.value(idx);

            if Some(id) == last_id {
                let (term_type, value, data_type, language) =
                    last_term.as_ref().unwrap().as_ref();
                builder.append_raw(
                    *term_type,
                    value,
                    data_type.as_deref(),
                    language.as_deref(),
                );
                continue;
            }

            if let Some(cached_term) = self.id_to_term_cache.get(&id) {
                let (term_type, value, data_type, language) = cached_term.as_ref();
                builder.append_raw(
                    *term_type,
                    value,
                    data_type.as_deref(),
                    language.as_deref(),
                );
                last_id = Some(id);
                last_term = Some(cached_term);
            } else {
                if let Some(db_term_val) = self.db.get_cf(&cf, id.to_le_bytes())? {
                    let owned_tuple = super::decode_term_tuple(&db_term_val);
                    let (term_type, value, data_type, language) = &owned_tuple;
                    builder.append_raw(
                        *term_type,
                        value,
                        data_type.as_deref(),
                        language.as_deref(),
                    );
                    let arc = Arc::new(owned_tuple.clone());
                    self.id_to_term_cache.insert(id, Arc::clone(&arc));
                    last_id = Some(id);
                    last_term = Some(arc);
                } else {
                    return Err(LocalObjectIdError::NotFound(id));
                }
            }
        }
        Ok(builder.finish().into_array_ref())
    }

    fn len(&self) -> Result<u64, LocalObjectIdError> {
        let cf = self.db.cf_handle(CF_ID_TO_TERM).unwrap();
        Ok(self
            .db
            .property_int_value_cf(&cf, "rocksdb.estimate-num-keys")?
            .unwrap_or(0))
    }

    fn read_claimed_object_ids(&self) -> Result<Option<(i64, i64)>, LocalObjectIdError> {
        let cf = self.db.cf_handle(CF_METADATA).unwrap();
        let next = self
            .db
            .get_cf(&cf, NEXT_FREE_ID_KEY)?
            .map(|b| String::from_utf8(b).unwrap());
        let last = self
            .db
            .get_cf(&cf, LAST_FREE_ID_KEY)?
            .map(|b| String::from_utf8(b).unwrap());

        match (next, last) {
            (Some(next_val), Some(last_val)) => {
                let next_free_id = next_val.parse::<i64>().map_err(|err| {
                    LocalObjectIdError::Corruption(format!(
                        "Invalid next free id found: {err}"
                    ))
                })?;
                let last_free_value = last_val.parse::<i64>().map_err(|err| {
                    LocalObjectIdError::Corruption(format!(
                        "Invalid last free id found: {err}"
                    ))
                })?;
                Ok(Some((next_free_id, last_free_value)))
            }
            (None, None) => Ok(None),
            _ => Err(LocalObjectIdError::Corruption(
                "Found only one of next and last free id.".to_string(),
            )),
        }
    }

    fn is_empty(&self) -> Result<bool, LocalObjectIdError> {
        let cf = self.db.cf_handle(CF_ID_TO_TERM).unwrap();
        Ok(self
            .db
            .iterator_cf(&cf, rocksdb::IteratorMode::Start)
            .next()
            .is_none())
    }

    fn get_id_by_term(&self, term: &PlainTermScalar) -> Option<i64> {
        let parts = term.as_parts()?;
        let term_tuple = (
            parts.term_type,
            parts.value,
            parts.data_type,
            parts.language_tag,
        );
        let owned_tuple: OwnedTermTuple = (
            parts.term_type,
            parts.value.into(),
            parts.data_type.map(|s| s.into()),
            parts.language_tag.map(|s| s.into()),
        );

        if let Some(id) = self.term_to_id_cache.get(&owned_tuple) {
            return Some(id);
        }

        let cf = self.db.cf_handle(CF_TERM_TO_ID).unwrap();
        let encoded_term = super::encode_term_tuple_ref(&term_tuple);
        if let Some(id_bytes) = self.db.get_cf(&cf, encoded_term).ok().flatten() {
            let id = i64::from_le_bytes(id_bytes.try_into().unwrap());
            self.term_to_id_cache.insert(owned_tuple, id);
            Some(id)
        } else {
            None
        }
    }

    fn get_synced_version(&self) -> Result<Option<u64>, LocalObjectIdError> {
        let cf = self.db.cf_handle(CF_METADATA).unwrap();
        if let Some(b) = self.db.get_cf(&cf, SYNCED_VERSION_KEY)? {
            let s = String::from_utf8(b).unwrap();
            s.parse::<u64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))
                .map(Some)
        } else {
            Ok(None)
        }
    }
}

pub struct RocksDBLocalObjectIdTransaction {
    db: Arc<DB>,
    claimed_ids: ObjectIdClaim,
    id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
    term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,
    reset_state_on_conflict: Option<(i64, i64)>,
    pending_terms: HashMap<OwnedTermTuple, i64>,
    pending_ids: HashMap<i64, Arc<OwnedTermTuple>>,
}

#[async_trait]
impl LocalObjectIdTransaction for RocksDBLocalObjectIdTransaction {
    async fn encode_array(
        &mut self,
        array: &PlainTermArray,
    ) -> Result<Int64Array, LocalObjectIdError> {
        let array_parts = array.as_parts();
        let mut result_ids = Int64Builder::with_capacity(array.len());
        let mut last_term_tuple: Option<(i8, &str, Option<&str>, Option<&str>)> = None;
        let mut last_id = i64::MIN;

        let cf_term_to_id = self.db.cf_handle(CF_TERM_TO_ID).unwrap();

        for idx in 0..array.len() {
            if array.inner().is_null(idx) {
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

            if Some(term_tuple) == last_term_tuple {
                result_ids.append_value(last_id);
                continue;
            }

            let owned_tuple: OwnedTermTuple = (
                term_type,
                value.into(),
                data_type.map(|s| s.into()),
                language.map(|s| s.into()),
            );

            if let Some(&id) = self.pending_terms.get(&owned_tuple) {
                result_ids.append_value(id);
                last_term_tuple = Some(term_tuple);
                last_id = id;
            } else if let Some(id) = self.term_to_id_cache.get(&owned_tuple) {
                result_ids.append_value(id);
                last_term_tuple = Some(term_tuple);
                last_id = id;
            } else {
                let encoded_term = super::encode_term_tuple_ref(&term_tuple);
                if let Some(id_bytes) = self.db.get_cf(&cf_term_to_id, encoded_term)? {
                    let id = i64::from_le_bytes(id_bytes.try_into().unwrap());
                    self.term_to_id_cache.insert(owned_tuple.clone(), id);
                    result_ids.append_value(id);
                    last_term_tuple = Some(term_tuple);
                    last_id = id;
                } else {
                    let next_id = self.claimed_ids.acquire_next_id().await?;
                    let arc_tuple = Arc::new(owned_tuple.clone());
                    self.pending_terms.insert(owned_tuple, next_id.id);
                    self.pending_ids.insert(next_id.id, arc_tuple);
                    result_ids.append_value(next_id.id);
                    last_term_tuple = Some(term_tuple);
                    last_id = next_id.id;
                    if next_id.newly_claimed.is_some() {
                        self.reset_state_on_conflict = next_id.newly_claimed;
                    }
                }
            }
        }
        Ok(result_ids.finish())
    }

    async fn add_global_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), LocalObjectIdError> {
        let id_col = batch
            .column_by_name("id")
            .expect("Missing 'id' column")
            .as_primitive::<Int64Type>();
        let term_col = batch.column_by_name("term").expect("Missing 'term' column");
        let plain_term_array = PlainTermArray::try_from(Arc::clone(term_col))?;
        let array_parts = plain_term_array.as_parts();

        let current_claim = self.claimed_ids.peek_current_claim();
        let mut max_id: Option<i64> = None;

        for i in 0..batch.num_rows() {
            let id = id_col.value(i);
            max_id = Some(max_id.map_or(id, |m| m.max(id)));

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

            let owned_tuple: OwnedTermTuple = (
                term_type,
                value.into(),
                data_type.map(|s| s.into()),
                language.map(|s| s.into()),
            );

            let arc_tuple = Arc::new(owned_tuple.clone());
            self.pending_terms.insert(owned_tuple, id);
            self.pending_ids.insert(id, arc_tuple);
        }

        let mut claim_changed = false;
        if let (Some(max_id), Some((next_free, last_free))) = (max_id, current_claim) {
            if max_id >= next_free {
                claim_changed = true;
                if max_id >= last_free {
                    self.reset_state_on_conflict = None;
                } else {
                    self.reset_state_on_conflict = Some((max_id + 1, last_free));
                }
            }
        }

        if claim_changed {
            self.claimed_ids
                .set_claim_state(self.reset_state_on_conflict);
        }

        Ok(())
    }

    async fn commit(
        self: Box<Self>,
        delta_version: u64,
    ) -> Result<(), LocalObjectIdError> {
        let current_claim = self.claimed_ids.peek_current_claim();
        let mut batch = WriteBatch::default();
        let cf_id_to_term = self.db.cf_handle(CF_ID_TO_TERM).unwrap();
        let cf_term_to_id = self.db.cf_handle(CF_TERM_TO_ID).unwrap();
        let cf_metadata = self.db.cf_handle(CF_METADATA).unwrap();

        for (term_tuple, id) in &self.pending_terms {
            let tuple_ref = (
                term_tuple.0,
                term_tuple.1.as_ref(),
                term_tuple.2.as_deref(),
                term_tuple.3.as_deref(),
            );
            let encoded_term = super::encode_term_tuple_ref(&tuple_ref);
            let encoded_id = id.to_le_bytes();
            batch.put_cf(&cf_term_to_id, &encoded_term, encoded_id);
            batch.put_cf(&cf_id_to_term, encoded_id, &encoded_term);
        }

        let delta_version_str = delta_version.to_string();
        batch.put_cf(
            &cf_metadata,
            SYNCED_VERSION_KEY,
            delta_version_str.as_bytes(),
        );
        write_claim_rocksdb(&mut batch, cf_metadata, current_claim);
        self.db.write(batch)?;

        for (term_tuple, id) in &self.pending_terms {
            self.term_to_id_cache.insert(term_tuple.clone(), *id);
            self.id_to_term_cache
                .insert(*id, Arc::new(term_tuple.clone()));
        }

        Ok(())
    }

    fn pending_ids(&self) -> &HashMap<i64, Arc<OwnedTermTuple>> {
        &self.pending_ids
    }

    async fn abort(mut self: Box<Self>) -> Result<(), LocalObjectIdError> {
        let mut batch = WriteBatch::default();
        let cf_metadata = self.db.cf_handle(CF_METADATA).unwrap();
        write_claim_rocksdb(&mut batch, cf_metadata, self.reset_state_on_conflict);
        self.db.write(batch)?;
        self.claimed_ids
            .set_claim_state(self.reset_state_on_conflict);
        Ok(())
    }
}

fn try_load_initial_claim_rocksdb(
    db: &DB,
) -> Result<Option<(i64, i64)>, LocalObjectIdError> {
    let cf_metadata = db.cf_handle(CF_METADATA).unwrap();

    let mut next_free_id = None;
    if let Some(val) = db.get_cf(&cf_metadata, NEXT_FREE_ID_KEY)? {
        next_free_id = Some(
            std::str::from_utf8(&val)
                .unwrap()
                .parse::<i64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))?,
        );
    }

    let mut last_free_id = None;
    if let Some(val) = db.get_cf(&cf_metadata, LAST_FREE_ID_KEY)? {
        last_free_id = Some(
            std::str::from_utf8(&val)
                .unwrap()
                .parse::<i64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))?,
        );
    }

    super::validate_initial_claim(next_free_id, last_free_id)
}

fn write_claim_rocksdb(
    batch: &mut WriteBatch,
    cf_metadata: &rocksdb::ColumnFamily,
    claim: Option<(i64, i64)>,
) {
    match claim {
        None => {
            batch.delete_cf(cf_metadata, NEXT_FREE_ID_KEY);
            batch.delete_cf(cf_metadata, LAST_FREE_ID_KEY);
        }
        Some((next_free_id, last_free_id)) => {
            let next = next_free_id.to_string();
            let last = last_free_id.to_string();
            batch.put_cf(cf_metadata, NEXT_FREE_ID_KEY, next.as_bytes());
            batch.put_cf(cf_metadata, LAST_FREE_ID_KEY, last.as_bytes());
        }
    }
}
