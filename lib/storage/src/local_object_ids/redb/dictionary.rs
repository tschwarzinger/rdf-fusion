use crate::local_object_ids::redb::cache::RedbObjectIdCache;
use crate::local_object_ids::redb::{
    ID_TO_TERM_TABLE, LAST_FREE_ID_KEY, METADATA_TABLE, NEXT_FREE_ID_KEY, RedbTerm,
    SYNCED_VERSION_KEY, TERM_TO_ID_TABLE, try_load_initial_claim_redb,
};
use crate::local_object_ids::{
    LocalDictionaryTerm, LocalObjectIdDictionary, LocalObjectIdDictionarySnapshot,
    LocalObjectIdError, LocalObjectIdTransaction, ObjectIdClaim, ObjectIdClaimer,
};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, Int64Builder, RecordBatch,
};
use datafusion::arrow::datatypes::Int64Type;
use datafusion::common::runtime::SpawnedTask;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{
    PlainTermArray, PlainTermArrayElementBuilder, PlainTermScalar,
};
use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, Table,
    WriteTransaction,
};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct RedbLocalObjectIdDictionary {
    db: Arc<Database>,
    claimer: Option<Arc<dyn ObjectIdClaimer>>,
    cache: Option<RedbObjectIdCache>,
}

impl std::fmt::Debug for RedbLocalObjectIdDictionary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbObjectIdDictionary")
            .field("cache", &self.cache)
            .finish_non_exhaustive()
    }
}

impl RedbLocalObjectIdDictionary {
    /// Creates a new [`RedbLocalObjectIdDictionary`], given an already instantiated database.
    ///
    /// Most users should use the [`RedbLocalObjectIdDictionary`] for conveniently creating instances of
    /// this dictionary.
    pub fn try_new(
        db: Arc<Database>,
        cache: Option<RedbObjectIdCache>,
        claimer: Option<Arc<dyn ObjectIdClaimer>>,
    ) -> redb::Result<Self, LocalObjectIdError> {
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(ID_TO_TERM_TABLE)?;
            let _ = write_txn.open_table(TERM_TO_ID_TABLE)?;
            let _ = write_txn.open_table(METADATA_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db, claimer, cache })
    }
}

#[async_trait]
impl LocalObjectIdDictionary for RedbLocalObjectIdDictionary {
    async fn snapshot(
        &self,
    ) -> redb::Result<Arc<dyn LocalObjectIdDictionarySnapshot>, LocalObjectIdError> {
        Ok(Arc::new(RedbObjectIdDictionarySnapshot {
            db: Arc::clone(&self.db),
            cache: self.cache.clone(),
        }))
    }

    async fn transaction(
        &self,
    ) -> redb::Result<Box<dyn LocalObjectIdTransaction>, LocalObjectIdError> {
        let write_txn = self.db.begin_write()?;
        let initial_claim = try_load_initial_claim_redb(&write_txn)?;
        Ok(Box::new(RedbObjectIdDictionaryTransaction {
            write_txn,
            claimed_ids: ObjectIdClaim::new(initial_claim, self.claimer.clone()),
            cache: self.cache.clone(),
            reset_state_on_conflict: initial_claim,
            pending_terms: HashMap::new(),
            pending_ids: HashMap::new(),
        }))
    }
}

#[derive(Clone)]
pub struct RedbObjectIdDictionarySnapshot {
    db: Arc<Database>,
    cache: Option<RedbObjectIdCache>,
}

#[async_trait]
impl LocalObjectIdDictionarySnapshot for RedbObjectIdDictionarySnapshot {
    async fn resolve_plain_terms(
        &self,
        ids: &Int64Array,
    ) -> redb::Result<ArrayRef, LocalObjectIdError> {
        let this = self.clone();
        let ids = ids.clone();
        SpawnedTask::spawn_blocking(move || {
            let len = ids.len();
            let mut builder = PlainTermArrayElementBuilder::with_capacity(len);
            let mut last_id: Option<i64> = None;
            let mut last_term: Option<Arc<LocalDictionaryTerm>> = None;

            let read_txn = this.db.begin_read()?;
            let id_to_term_table = read_txn.open_table(ID_TO_TERM_TABLE)?;

            let append_term = |builder: &mut PlainTermArrayElementBuilder,
                               term: &RedbTerm| {
                builder.append_raw(
                    term.term_type,
                    term.value,
                    term.data_type,
                    term.language,
                );
            };

            for idx in ids.iter() {
                let Some(id) = idx else {
                    builder.append_null();
                    continue;
                };

                if Some(id) == last_id {
                    if let Some(term) = &last_term {
                        append_term(&mut builder, &RedbTerm::from(term.as_ref()));
                        continue;
                    }
                }

                if let Some(cached_term) =
                    this.cache.as_ref().and_then(|c| c.get_by_id(id))
                {
                    append_term(&mut builder, &RedbTerm::from(cached_term.as_ref()));
                    last_id = Some(id);
                    last_term = Some(cached_term);
                } else if let Some(guard) = id_to_term_table.get(id)? {
                    let redb_term = guard.value();
                    append_term(&mut builder, &redb_term);
                    let arc = Arc::new(redb_term.as_local_dictionary_term());
                    if let Some(cache) = &this.cache {
                        cache.insert(Arc::clone(&arc), id);
                    }
                    last_id = Some(id);
                    last_term = Some(arc);
                } else {
                    return Err(LocalObjectIdError::NotFound(id));
                }
            }
            Ok(builder.finish().into_array_ref())
        })
        .await
        .map_err(|e| LocalObjectIdError::Storage(format!("Join Error:: {e}")))?
    }

    async fn len(&self) -> redb::Result<u64, LocalObjectIdError> {
        let this = self.clone();
        SpawnedTask::spawn_blocking(move || {
            let read_txn = this.db.begin_read()?;
            let table = read_txn.open_table(ID_TO_TERM_TABLE)?;
            Ok(table.len()?)
        })
        .await
        .map_err(|e| LocalObjectIdError::Storage(format!("Join Error:: {e}")))?
    }

    async fn read_claimed_object_ids(
        &self,
    ) -> redb::Result<Option<(i64, i64)>, LocalObjectIdError> {
        let this = self.clone();
        SpawnedTask::spawn_blocking(move || {
            let read_txn = this.db.begin_read()?;
            let metadata_table = read_txn.open_table(METADATA_TABLE)?;

            let next = metadata_table.get(NEXT_FREE_ID_KEY)?;
            let last = metadata_table.get(LAST_FREE_ID_KEY)?;

            match (next, last) {
                (Some(next_val), Some(last_val)) => {
                    let next_free_id =
                        next_val.value().parse::<i64>().map_err(|err| {
                            LocalObjectIdError::Corruption(format!(
                                "Invalid next free id found: {err}"
                            ))
                        })?;
                    let last_free_value =
                        last_val.value().parse::<i64>().map_err(|err| {
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
        .map_err(|e| LocalObjectIdError::Storage(format!("Join Error:: {e}")))?
    }

    async fn is_empty(&self) -> redb::Result<bool, LocalObjectIdError> {
        let this = self.clone();
        SpawnedTask::spawn_blocking(move || {
            let read_txn = this.db.begin_read()?;
            let table = read_txn.open_table(ID_TO_TERM_TABLE)?;
            Ok(table.is_empty()?)
        })
        .await
        .map_err(|e| LocalObjectIdError::Storage(format!("Join Error:: {e}")))?
    }

    async fn get_id_by_term(
        &self,
        term: &PlainTermScalar,
    ) -> redb::Result<Option<i64>, LocalObjectIdError> {
        let Some(parts) = term.as_parts() else {
            return Ok(None);
        };

        // Use borrowed version to check cache without allocations
        let redb_term = RedbTerm {
            term_type: parts.term_type,
            value: parts.value,
            data_type: parts.data_type,
            language: parts.language_tag,
        };

        if let Some(id) = self.cache.as_ref().and_then(|c| c.get_by_term(&redb_term)) {
            return Ok(Some(id));
        }

        let owned_term = redb_term.as_local_dictionary_term();
        let this = self.clone();
        SpawnedTask::spawn_blocking(move || {
            let read_txn = this.db.begin_read()?;
            let term_to_id_table = read_txn.open_table(TERM_TO_ID_TABLE)?;
            if let Some(guard) = term_to_id_table.get(&RedbTerm::from(&owned_term))? {
                let id = guard.value();
                if let Some(cache) = &this.cache {
                    cache.insert(Arc::new(owned_term), id);
                }
                Ok(Some(id))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|e| LocalObjectIdError::Storage(format!("Join Error:: {e}")))?
    }

    // Removed SpawnedTask for fast reads
    async fn get_synced_version(&self) -> redb::Result<Option<u64>, LocalObjectIdError> {
        let this = self.clone();
        SpawnedTask::spawn_blocking(move || {
            let read_txn = this.db.begin_read()?;
            let metadata_table = read_txn.open_table(METADATA_TABLE)?;

            if let Some(guard) = metadata_table.get(SYNCED_VERSION_KEY)? {
                guard
                    .value()
                    .parse::<u64>()
                    .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))
                    .map(Some)
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|e| LocalObjectIdError::Storage(format!("Join Error:: {e}")))?
    }
}

/// A transaction that updates a [`RedbLocalObjectIdDictionary`].
pub struct RedbObjectIdDictionaryTransaction {
    write_txn: WriteTransaction,
    claimed_ids: ObjectIdClaim,
    cache: Option<RedbObjectIdCache>,
    reset_state_on_conflict: Option<(i64, i64)>,
    pending_terms: HashMap<Arc<LocalDictionaryTerm>, i64>,
    pending_ids: HashMap<i64, Arc<LocalDictionaryTerm>>,
}

#[async_trait]
impl LocalObjectIdTransaction for RedbObjectIdDictionaryTransaction {
    async fn encode_array(
        &mut self,
        array: &PlainTermArray,
    ) -> Result<Int64Array, LocalObjectIdError> {
        let array_parts = array.as_parts();
        let mut result_ids = Int64Builder::with_capacity(array.len());
        let mut last_redb_term: Option<RedbTerm<'_>> = None;
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

            let redb_term = RedbTerm {
                term_type,
                value,
                data_type,
                language,
            };

            if Some(redb_term) == last_redb_term {
                result_ids.append_value(last_id);
                continue;
            }

            if let Some(id) = self.cache.as_ref().and_then(|c| c.get_by_term(&redb_term))
            {
                result_ids.append_value(id);
                last_redb_term = Some(redb_term);
                last_id = id;
            } else {
                let id_opt = {
                    let term_to_id_table = self.write_txn.open_table(TERM_TO_ID_TABLE)?;
                    term_to_id_table.get(redb_term)?.map(|g| g.value())
                };
                if let Some(id) = id_opt {
                    if let Some(cache) = &self.cache {
                        cache.insert(Arc::new(redb_term.as_local_dictionary_term()), id);
                    }
                    result_ids.append_value(id);
                    last_redb_term = Some(redb_term);
                    last_id = id;
                } else {
                    let owned_tuple = Arc::new(redb_term.as_local_dictionary_term());
                    if let Some(&id) = self.pending_terms.get(&owned_tuple) {
                        result_ids.append_value(id);
                        last_redb_term = Some(redb_term);
                        last_id = id;
                    } else {
                        let next_id = self.claimed_ids.acquire_next_id().await?;
                        self.pending_terms
                            .insert(Arc::clone(&owned_tuple), next_id.id);
                        self.pending_ids.insert(next_id.id, owned_tuple);
                        result_ids.append_value(next_id.id);
                        last_redb_term = Some(redb_term);
                        last_id = next_id.id;
                        if next_id.newly_claimed.is_some() {
                            self.reset_state_on_conflict = next_id.newly_claimed;
                        }
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
            .ok_or_else(|| {
                LocalObjectIdError::Corruption("Missing 'id' column".to_string())
            })?
            .as_primitive::<Int64Type>();
        let term_col = batch.column_by_name("term").ok_or_else(|| {
            LocalObjectIdError::Corruption("Missing 'term' column".to_string())
        })?;

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

            let owned_term = Arc::new(LocalDictionaryTerm {
                term_type,
                value: value.into(),
                data_type: data_type.map(|s| s.into()),
                language: language.map(|s| s.into()),
            });

            self.pending_terms.insert(Arc::clone(&owned_term), id);
            self.pending_ids.insert(id, owned_term);
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
        {
            let mut id_to_term_table = self.write_txn.open_table(ID_TO_TERM_TABLE)?;
            let mut term_to_id_table = self.write_txn.open_table(TERM_TO_ID_TABLE)?;
            let mut metadata_table = self.write_txn.open_table(METADATA_TABLE)?;

            for (term_tuple, id) in &self.pending_terms {
                let redb_term = RedbTerm::from(term_tuple.as_ref());
                term_to_id_table.insert(redb_term, *id)?;
                id_to_term_table.insert(*id, redb_term)?;
            }

            let delta_version_str = delta_version.to_string();
            metadata_table.insert(SYNCED_VERSION_KEY, delta_version_str.as_str())?;
            write_claim(&mut metadata_table, current_claim)?;
        }

        self.write_txn.commit()?;

        if let Some(cache) = self.cache {
            for (term_tuple, id) in &self.pending_terms {
                cache.insert(Arc::clone(term_tuple), *id);
            }
        }

        Ok(())
    }

    async fn abort(mut self: Box<Self>) -> Result<(), LocalObjectIdError> {
        {
            let mut metadata_table = self.write_txn.open_table(METADATA_TABLE)?;
            write_claim(&mut metadata_table, self.reset_state_on_conflict)?;
        }
        self.write_txn.commit()?;

        self.claimed_ids
            .set_claim_state(self.reset_state_on_conflict);
        Ok(())
    }

    fn pending_ids(&self) -> &HashMap<i64, Arc<LocalDictionaryTerm>> {
        &self.pending_ids
    }
}

/// Writes the new claim to the dictionary. If the claim is `None`, the claim is removed.
fn write_claim(
    metadata_table: &mut Table<&str, &str>,
    claim: Option<(i64, i64)>,
) -> redb::Result<(), LocalObjectIdError> {
    match claim {
        None => {
            metadata_table.remove(NEXT_FREE_ID_KEY)?;
            metadata_table.remove(LAST_FREE_ID_KEY)?;
        }
        Some((next_free_id, last_free_id)) => {
            let next = next_free_id.to_string();
            let last = last_free_id.to_string();
            metadata_table.insert(NEXT_FREE_ID_KEY, next.as_str())?;
            metadata_table.insert(LAST_FREE_ID_KEY, last.as_str())?;
        }
    }
    Ok(())
}
