use super::{
    LAST_FREE_ID_KEY, NEXT_FREE_ID_KEY, ObjectIdClaimer, OwnedTermTuple,
    SYNCED_VERSION_KEY,
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
use heed::types::Str;
use heed::{BytesDecode, BytesEncode, Database, Env, EnvOpenOptions};
use md5::{Digest, Md5};
use quick_cache::sync::Cache;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{
    PlainTermArray, PlainTermArrayElementBuilder, PlainTermScalar,
};
use std::borrow::Cow;
use std::collections::HashMap;

fn compute_hash(term: &OwnedTermTuple) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update([term.term_type as u8]);
    hasher.update(term.value.as_bytes());
    if let Some(ref dt) = term.data_type {
        hasher.update([1]);
        hasher.update(dt.as_bytes());
    } else {
        hasher.update([0]);
    }
    if let Some(ref lang) = term.language {
        hasher.update([1]);
        hasher.update(lang.as_bytes());
    } else {
        hasher.update([0]);
    }
    hasher.finalize().into()
}

use std::path::PathBuf;
use std::sync::Arc;

pub struct I64Encoding;

impl<'a> BytesEncode<'a> for I64Encoding {
    type EItem = i64;
    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'a, [u8]>, heed::BoxedError> {
        Ok(Cow::Owned(item.to_le_bytes().to_vec()))
    }
}

impl<'a> BytesDecode<'a> for I64Encoding {
    type DItem = i64;
    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, heed::BoxedError> {
        Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
    }
}

pub struct TermTupleEncoding;

impl<'a> BytesEncode<'a> for TermTupleEncoding {
    type EItem = OwnedTermTuple;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'a, [u8]>, heed::BoxedError> {
        let mut bytes = Vec::new();
        bytes.push(item.term_type as u8);
        bytes.extend_from_slice(&(item.value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(item.value.as_bytes());
        if let Some(ref dt) = item.data_type {
            bytes.push(1);
            bytes.extend_from_slice(&(dt.len() as u32).to_le_bytes());
            bytes.extend_from_slice(dt.as_bytes());
        } else {
            bytes.push(0);
        }
        if let Some(ref lang) = item.language {
            bytes.push(1);
            bytes.extend_from_slice(&(lang.len() as u32).to_le_bytes());
            bytes.extend_from_slice(lang.as_bytes());
        } else {
            bytes.push(0);
        }
        Ok(Cow::Owned(bytes))
    }
}

impl<'a> BytesDecode<'a> for TermTupleEncoding {
    type DItem = OwnedTermTuple;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, heed::BoxedError> {
        let term_type = bytes[0] as i8;
        let mut offset = 1;

        let len =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let value = std::str::from_utf8(&bytes[offset..offset + len]).unwrap();
        offset += len;

        let data_type = if bytes[offset] == 1 {
            offset += 1;
            let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
                as usize;
            offset += 4;
            let dt = std::str::from_utf8(&bytes[offset..offset + len]).unwrap();
            offset += len;
            Some(dt)
        } else {
            offset += 1;
            None
        };

        let language = if bytes[offset] == 1 {
            offset += 1;
            let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
                as usize;
            offset += 4;
            let lang = std::str::from_utf8(&bytes[offset..offset + len]).unwrap();
            Some(lang)
        } else {
            None
        };

        Ok(OwnedTermTuple {
            term_type,
            value: value.to_string(),
            data_type: data_type.map(|s| s.to_string()),
            language: language.map(|s| s.to_string()),
        })
    }
}

pub struct HashEncoding;
impl<'a> BytesEncode<'a> for HashEncoding {
    type EItem = [u8; 16];
    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'a, [u8]>, heed::BoxedError> {
        Ok(Cow::Owned(item.to_vec()))
    }
}
impl<'a> BytesDecode<'a> for HashEncoding {
    type DItem = [u8; 16];
    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, heed::BoxedError> {
        Ok(bytes.try_into().unwrap())
    }
}

type IdToTermDb = Database<I64Encoding, TermTupleEncoding>;
type TermToIdDb = Database<HashEncoding, I64Encoding>;
type MetadataDb = Database<Str, Str>;

#[derive(Clone, Debug)]
pub struct LmdbObjectIdDictionary {
    env: Env,
    id_to_term_db: IdToTermDb,
    term_to_id_db: TermToIdDb,
    metadata_db: MetadataDb,
    claimer: Arc<dyn ObjectIdClaimer>,
    id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
    term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,
}

impl LmdbObjectIdDictionary {
    pub fn try_new(
        path: PathBuf,
        cache_size: usize,
        claimer: Arc<dyn ObjectIdClaimer>,
    ) -> Result<Self, LocalObjectIdError> {
        std::fs::create_dir_all(&path)
            .map_err(|e| LocalObjectIdError::Storage(e.to_string()))?;

        let mut env_builder = EnvOpenOptions::new();
        env_builder.max_dbs(3);
        env_builder.map_size(100 * 1024 * 1024 * 1024); // 100 GiB
        let env = unsafe { env_builder.open(&path)? };

        let mut write_txn = env.write_txn()?;
        let id_to_term_db = env.create_database(&mut write_txn, Some("id_to_term"))?;
        let term_to_id_db = env.create_database(&mut write_txn, Some("term_to_id"))?;
        let metadata_db = env.create_database(&mut write_txn, Some("metadata"))?;
        write_txn.commit()?;

        Ok(Self {
            env,
            id_to_term_db,
            term_to_id_db,
            metadata_db,
            claimer,
            id_to_term_cache: Arc::new(Cache::new(cache_size)),
            term_to_id_cache: Arc::new(Cache::new(cache_size)),
        })
    }
}

#[async_trait]
impl LocalObjectIdDictionary for LmdbObjectIdDictionary {
    async fn snapshot(
        &self,
    ) -> Result<Arc<dyn LocalObjectIdDictionarySnapshot>, LocalObjectIdError> {
        Ok(Arc::new(LmdbLocalObjectIdDictionarySnapshot {
            env: self.env.clone(),
            id_to_term_db: self.id_to_term_db,
            term_to_id_db: self.term_to_id_db,
            metadata_db: self.metadata_db,
            id_to_term_cache: Arc::clone(&self.id_to_term_cache),
            term_to_id_cache: Arc::clone(&self.term_to_id_cache),
        }))
    }

    async fn transaction(
        &self,
    ) -> Result<Box<dyn LocalObjectIdTransaction>, LocalObjectIdError> {
        let initial_claim = try_load_initial_claim_lmdb(&self.env, self.metadata_db)?;
        Ok(Box::new(LmdbLocalObjectIdTransaction {
            env: self.env.clone(),
            id_to_term_db: self.id_to_term_db,
            term_to_id_db: self.term_to_id_db,
            metadata_db: self.metadata_db,
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

#[derive(Clone)]
pub struct LmdbLocalObjectIdDictionarySnapshot {
    env: Env,
    id_to_term_db: IdToTermDb,
    term_to_id_db: TermToIdDb,
    metadata_db: MetadataDb,
    id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
    term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,
}

#[async_trait]
impl LocalObjectIdDictionarySnapshot for LmdbLocalObjectIdDictionarySnapshot {
    async fn resolve_plain_terms(
        &self,
        ids: &Int64Array,
    ) -> Result<ArrayRef, LocalObjectIdError> {
        let this = self.clone();
        let ids = ids.clone();
        datafusion::common::runtime::SpawnedTask::spawn_blocking(move || {
            let len = ids.len();
            let mut builder = PlainTermArrayElementBuilder::with_capacity(len);
            let mut last_id: Option<i64> = None;
            let mut last_term: Option<Arc<OwnedTermTuple>> = None;

            let read_txn = this.env.read_txn()?;

            for idx in 0..len {
                if ids.is_null(idx) {
                    builder.append_null();
                    continue;
                };
                let id = ids.value(idx);

                if Some(id) == last_id {
                    let term = last_term.as_ref().unwrap().as_ref();
                    builder.append_raw(
                        term.term_type,
                        &term.value,
                        term.data_type.as_deref(),
                        term.language.as_deref(),
                    );
                    continue;
                }

                if let Some(cached_term) = this.id_to_term_cache.get(&id) {
                    let term = cached_term.as_ref();
                    builder.append_raw(
                        term.term_type,
                        &term.value,
                        term.data_type.as_deref(),
                        term.language.as_deref(),
                    );
                    last_id = Some(id);
                    last_term = Some(cached_term);
                } else {
                    if let Some(tuple_val) = this.id_to_term_db.get(&read_txn, &id)? {
                        let owned_tuple = tuple_val;
                        let term = &owned_tuple;
                        builder.append_raw(
                            term.term_type,
                            &term.value,
                            term.data_type.as_deref(),
                            term.language.as_deref(),
                        );
                        let arc = Arc::new(owned_tuple.clone());
                        this.id_to_term_cache.insert(id, Arc::clone(&arc));
                        last_id = Some(id);
                        last_term = Some(arc);
                    } else {
                        return Err(LocalObjectIdError::NotFound(id));
                    }
                }
            }
            Ok(builder.finish().into_array_ref())
        })
        .await
        .unwrap()
    }

    async fn len(&self) -> Result<u64, LocalObjectIdError> {
        let this = self.clone();
        datafusion::common::runtime::SpawnedTask::spawn_blocking(move || {
            let read_txn = this.env.read_txn()?;
            Ok(this.id_to_term_db.len(&read_txn)?)
        })
        .await
        .unwrap()
    }

    async fn read_claimed_object_ids(
        &self,
    ) -> Result<Option<(i64, i64)>, LocalObjectIdError> {
        let this = self.clone();
        datafusion::common::runtime::SpawnedTask::spawn_blocking(move || {
            let read_txn = this.env.read_txn()?;

            let next = this.metadata_db.get(&read_txn, NEXT_FREE_ID_KEY)?;
            let last = this.metadata_db.get(&read_txn, LAST_FREE_ID_KEY)?;

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
        })
        .await
        .unwrap()
    }

    async fn is_empty(&self) -> Result<bool, LocalObjectIdError> {
        let this = self.clone();
        datafusion::common::runtime::SpawnedTask::spawn_blocking(move || {
            let read_txn = this.env.read_txn()?;
            Ok(this.id_to_term_db.is_empty(&read_txn)?)
        })
        .await
        .unwrap()
    }

    async fn get_id_by_term(&self, term: &PlainTermScalar) -> Option<i64> {
        let parts = term.as_parts()?;

        let owned_tuple = OwnedTermTuple {
            term_type: parts.term_type,
            value: parts.value.into(),
            data_type: parts.data_type.map(|s| s.into()),
            language: parts.language_tag.map(|s| s.into()),
        };

        if let Some(id) = self.term_to_id_cache.get(&owned_tuple) {
            return Some(id);
        }

        let this = self.clone();
        datafusion::common::runtime::SpawnedTask::spawn_blocking(move || {
            let read_txn = this.env.read_txn().ok()?;
            let hash = compute_hash(&owned_tuple);
            if let Some(id) = this.term_to_id_db.get(&read_txn, &hash).ok().flatten() {
                this.term_to_id_cache.insert(owned_tuple, id);
                Some(id)
            } else {
                None
            }
        })
        .await
        .unwrap()
    }

    async fn get_synced_version(&self) -> Result<Option<u64>, LocalObjectIdError> {
        let this = self.clone();
        datafusion::common::runtime::SpawnedTask::spawn_blocking(move || {
            let read_txn = this.env.read_txn()?;

            if let Some(s) = this.metadata_db.get(&read_txn, SYNCED_VERSION_KEY)? {
                s.parse::<u64>()
                    .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))
                    .map(Some)
            } else {
                Ok(None)
            }
        })
        .await
        .unwrap()
    }
}

pub struct LmdbLocalObjectIdTransaction {
    env: Env,
    id_to_term_db: IdToTermDb,
    term_to_id_db: TermToIdDb,
    metadata_db: MetadataDb,
    claimed_ids: ObjectIdClaim,
    id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
    term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,
    reset_state_on_conflict: Option<(i64, i64)>,
    pending_terms: HashMap<OwnedTermTuple, i64>,
    pending_ids: HashMap<i64, Arc<OwnedTermTuple>>,
}

#[async_trait]
impl LocalObjectIdTransaction for LmdbLocalObjectIdTransaction {
    async fn encode_array(
        &mut self,
        array: &PlainTermArray,
    ) -> Result<Int64Array, LocalObjectIdError> {
        let array_parts = array.as_parts();
        let mut result_ids = Int64Builder::with_capacity(array.len());
        let mut last_term_tuple: Option<OwnedTermTuple> = None;
        let mut last_id = i64::MIN;

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
            let term_tuple = OwnedTermTuple {
                term_type,
                value: value.into(),
                data_type: data_type.map(|s| s.into()),
                language: language.map(|s| s.into()),
            };

            if Some(&term_tuple) == last_term_tuple.as_ref() {
                result_ids.append_value(last_id);
                continue;
            }

            let owned_tuple = term_tuple.clone();

            if let Some(&id) = self.pending_terms.get(&owned_tuple) {
                result_ids.append_value(id);
                last_term_tuple = Some(term_tuple);
                last_id = id;
            } else if let Some(id) = self.term_to_id_cache.get(&owned_tuple) {
                result_ids.append_value(id);
                last_term_tuple = Some(term_tuple);
                last_id = id;
            } else {
                let id_opt = {
                    let read_txn = self.env.read_txn()?;
                    let hash = compute_hash(&term_tuple);
                    self.term_to_id_db.get(&read_txn, &hash)?
                };
                if let Some(id) = id_opt {
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

            let owned_tuple = OwnedTermTuple {
                term_type,
                value: value.into(),
                data_type: data_type.map(|s| s.into()),
                language: language.map(|s| s.into()),
            };

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
        let mut write_txn = self.env.write_txn()?;

        for (term_tuple, id) in &self.pending_terms {
            let hash = compute_hash(term_tuple);
            self.term_to_id_db.put(&mut write_txn, &hash, id)?;
            self.id_to_term_db.put(&mut write_txn, id, term_tuple)?;
        }

        let delta_version_str = delta_version.to_string();
        self.metadata_db.put(
            &mut write_txn,
            SYNCED_VERSION_KEY,
            delta_version_str.as_str(),
        )?;
        write_claim_lmdb(&mut write_txn, self.metadata_db, current_claim)?;

        write_txn.commit()?;

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
        let mut write_txn = self.env.write_txn()?;
        write_claim_lmdb(
            &mut write_txn,
            self.metadata_db,
            self.reset_state_on_conflict,
        )?;
        write_txn.commit()?;

        self.claimed_ids
            .set_claim_state(self.reset_state_on_conflict);
        Ok(())
    }
}

fn try_load_initial_claim_lmdb(
    env: &Env,
    metadata_db: MetadataDb,
) -> Result<Option<(i64, i64)>, LocalObjectIdError> {
    let read_txn = env.read_txn()?;

    let mut next_free_id = None;
    if let Some(val) = metadata_db.get(&read_txn, NEXT_FREE_ID_KEY)? {
        next_free_id = Some(
            val.parse::<i64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))?,
        );
    }

    let mut last_free_id = None;
    if let Some(val) = metadata_db.get(&read_txn, LAST_FREE_ID_KEY)? {
        last_free_id = Some(
            val.parse::<i64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))?,
        );
    }

    super::validate_initial_claim(next_free_id, last_free_id)
}

fn write_claim_lmdb(
    write_txn: &mut heed::RwTxn,
    metadata_db: MetadataDb,
    claim: Option<(i64, i64)>,
) -> Result<(), LocalObjectIdError> {
    match claim {
        None => {
            metadata_db.delete(write_txn, NEXT_FREE_ID_KEY)?;
            metadata_db.delete(write_txn, LAST_FREE_ID_KEY)?;
        }
        Some((next_free_id, last_free_id)) => {
            let next = next_free_id.to_string();
            let last = last_free_id.to_string();
            metadata_db.put(write_txn, NEXT_FREE_ID_KEY, next.as_str())?;
            metadata_db.put(write_txn, LAST_FREE_ID_KEY, last.as_str())?;
        }
    }
    Ok(())
}
