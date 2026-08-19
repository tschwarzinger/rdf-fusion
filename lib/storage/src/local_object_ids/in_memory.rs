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
use datafusion::arrow::array::Int64Builder;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::Int64Type;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{
    PlainTermArray, PlainTermArrayElementBuilder, PlainTermScalar,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

#[derive(Debug, Default)]
pub struct InMemoryBackend {
    pub id_to_term: HashMap<i64, Arc<OwnedTermTuple>>,
    pub term_to_id: HashMap<OwnedTermTuple, i64>,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct InMemoryObjectIdDictionary {
    backend: Arc<RwLock<InMemoryBackend>>,
    claimer: Arc<dyn ObjectIdClaimer>,
}

impl InMemoryObjectIdDictionary {
    pub fn new(claimer: Arc<dyn ObjectIdClaimer>) -> Self {
        Self {
            backend: Arc::new(RwLock::new(InMemoryBackend::default())),
            claimer,
        }
    }
}

#[async_trait]
impl LocalObjectIdDictionary for InMemoryObjectIdDictionary {
    async fn snapshot(
        &self,
    ) -> Result<Arc<dyn LocalObjectIdDictionarySnapshot>, LocalObjectIdError> {
        let guard = Arc::clone(&self.backend).read_owned().await;
        Ok(Arc::new(InMemoryLocalObjectIdDictionarySnapshot { guard }))
    }

    async fn transaction(
        &self,
    ) -> Result<Box<dyn LocalObjectIdTransaction>, LocalObjectIdError> {
        let backend = self.backend.read().await;
        let initial_claim = try_load_initial_claim_in_memory(&backend)?;
        drop(backend);
        Ok(Box::new(InMemoryLocalObjectIdTransaction {
            backend: Arc::clone(&self.backend),
            claimed_ids: ObjectIdClaim::new(
                initial_claim,
                Some(Arc::clone(&self.claimer)),
            ),
            reset_state_on_conflict: initial_claim,
            pending_terms: HashMap::new(),
            pending_ids: HashMap::new(),
        }))
    }
}

pub struct InMemoryLocalObjectIdDictionarySnapshot {
    guard: OwnedRwLockReadGuard<InMemoryBackend>,
}

#[async_trait]
impl LocalObjectIdDictionarySnapshot for InMemoryLocalObjectIdDictionarySnapshot {
    async fn resolve_plain_terms(
        &self,
        ids: &Int64Array,
    ) -> Result<ArrayRef, LocalObjectIdError> {
        let len = ids.len();
        let mut builder = PlainTermArrayElementBuilder::with_capacity(len);
        let mut last_id: Option<i64> = None;
        let mut last_term: Option<Arc<OwnedTermTuple>> = None;

        let id_to_term = &self.guard.id_to_term;

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

            if let Some(arc) = id_to_term.get(&id) {
                let term = arc.as_ref();
                builder.append_raw(
                    term.term_type,
                    &term.value,
                    term.data_type.as_deref(),
                    term.language.as_deref(),
                );
                last_id = Some(id);
                last_term = Some(Arc::clone(arc));
            } else {
                return Err(LocalObjectIdError::NotFound(id));
            }
        }
        Ok(builder.finish().into_array_ref())
    }

    async fn len(&self) -> Result<u64, LocalObjectIdError> {
        Ok(self.guard.id_to_term.len() as u64)
    }

    async fn read_claimed_object_ids(
        &self,
    ) -> Result<Option<(i64, i64)>, LocalObjectIdError> {
        let meta = &self.guard.metadata;
        let next = meta.get(NEXT_FREE_ID_KEY).cloned();
        let last = meta.get(LAST_FREE_ID_KEY).cloned();

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

    async fn is_empty(&self) -> Result<bool, LocalObjectIdError> {
        Ok(self.guard.id_to_term.is_empty())
    }

    async fn get_id_by_term(&self, term: &PlainTermScalar) -> Option<i64> {
        let parts = term.as_parts()?;
        let owned_tuple = OwnedTermTuple {
            term_type: parts.term_type,
            value: parts.value.into(),
            data_type: parts.data_type.map(|s| s.into()),
            language: parts.language_tag.map(|s| s.into()),
        };

        self.guard.term_to_id.get(&owned_tuple).copied()
    }

    async fn get_synced_version(&self) -> Result<Option<u64>, LocalObjectIdError> {
        let meta = &self.guard.metadata;
        if let Some(s) = meta.get(SYNCED_VERSION_KEY) {
            s.parse::<u64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))
                .map(Some)
        } else {
            Ok(None)
        }
    }
}

pub struct InMemoryLocalObjectIdTransaction {
    backend: Arc<RwLock<InMemoryBackend>>,
    claimed_ids: ObjectIdClaim,
    reset_state_on_conflict: Option<(i64, i64)>,
    pending_terms: HashMap<OwnedTermTuple, i64>,
    pending_ids: HashMap<i64, Arc<OwnedTermTuple>>,
}

#[async_trait]
impl LocalObjectIdTransaction for InMemoryLocalObjectIdTransaction {
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
            } else if let Some(id) = {
                self.backend
                    .read()
                    .await
                    .term_to_id
                    .get(&owned_tuple)
                    .copied()
            } {
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

        let mut backend = self.backend.write().await;
        let InMemoryBackend {
            ref mut id_to_term,
            ref mut term_to_id,
            ref mut metadata,
        } = *backend;

        for (term_tuple, id) in self.pending_terms {
            term_to_id.insert(term_tuple.clone(), id);
            id_to_term.insert(id, Arc::new(term_tuple));
        }

        metadata.insert(SYNCED_VERSION_KEY.to_string(), delta_version.to_string());
        write_claim_in_memory(metadata, current_claim);

        Ok(())
    }

    fn pending_ids(&self) -> &HashMap<i64, Arc<OwnedTermTuple>> {
        &self.pending_ids
    }

    async fn abort(mut self: Box<Self>) -> Result<(), LocalObjectIdError> {
        let mut backend = self.backend.write().await;
        let metadata = &mut backend.metadata;
        write_claim_in_memory(metadata, self.reset_state_on_conflict);
        self.claimed_ids
            .set_claim_state(self.reset_state_on_conflict);
        Ok(())
    }
}

fn try_load_initial_claim_in_memory(
    mem: &InMemoryBackend,
) -> Result<Option<(i64, i64)>, LocalObjectIdError> {
    let meta = &mem.metadata;
    let next_free_id = meta
        .get(NEXT_FREE_ID_KEY)
        .map(|s| {
            s.parse::<i64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))
        })
        .transpose()?;
    let last_free_id = meta
        .get(LAST_FREE_ID_KEY)
        .map(|s| {
            s.parse::<i64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))
        })
        .transpose()?;

    super::validate_initial_claim(next_free_id, last_free_id)
}

fn write_claim_in_memory(
    metadata: &mut HashMap<String, String>,
    claim: Option<(i64, i64)>,
) {
    match claim {
        None => {
            metadata.remove(NEXT_FREE_ID_KEY);
            metadata.remove(LAST_FREE_ID_KEY);
        }
        Some((next_free_id, last_free_id)) => {
            metadata.insert(NEXT_FREE_ID_KEY.to_string(), next_free_id.to_string());
            metadata.insert(LAST_FREE_ID_KEY.to_string(), last_free_id.to_string());
        }
    }
}
