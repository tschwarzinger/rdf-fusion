use super::{
    LAST_FREE_ID_KEY, NEXT_FREE_ID_KEY, ObjectIdClaimer, OwnedTermTuple,
    SYNCED_VERSION_KEY, TABLE_ID_TO_TERM, TABLE_METADATA, TABLE_TERM_TO_ID,
};
use crate::local_object_ids::claim::ObjectIdClaim;
use crate::local_object_ids::error::LocalObjectIdError;
use datafusion::arrow::array::{Array, AsArray, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::Int64Type;
use deltalake::arrow::array::Int64Builder;
use quick_cache::sync::Cache;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::PlainTermArray;
use redb::ReadableTable;
use std::collections::HashMap;
use std::sync::Arc;

///
///
/// **CAVEAT**: this is a synchronous API which does disk access because redb only exposes a
/// synchronous API.
pub struct LocalObjectIdTransaction {
    write_txn: redb::WriteTransaction,
    claimed_ids: ObjectIdClaim,
    id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
    term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,

    reset_state_on_conflict: Option<(i64, i64)>,
    pending_terms: HashMap<OwnedTermTuple, i64>,
    pending_ids: HashMap<i64, Arc<OwnedTermTuple>>,
}

impl LocalObjectIdTransaction {
    /// Creates a new [`LocalObjectIdTransaction`].
    pub fn try_new(
        write_txn: redb::WriteTransaction,
        claimer: Arc<dyn ObjectIdClaimer>,
        id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
        term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,
    ) -> Result<Self, LocalObjectIdError> {
        let initial_claim = try_load_initial_claim(&write_txn)?;
        Ok(Self {
            write_txn,
            claimed_ids: ObjectIdClaim::new(initial_claim, Some(claimer)),
            id_to_term_cache,
            term_to_id_cache,
            reset_state_on_conflict: initial_claim,
            pending_terms: HashMap::new(),
            pending_ids: HashMap::new(),
        })
    }

    pub fn pending_ids(&self) -> &HashMap<i64, Arc<OwnedTermTuple>> {
        &self.pending_ids
    }

    pub async fn encode_array(
        &mut self,
        array: &PlainTermArray,
    ) -> Result<Int64Array, LocalObjectIdError> {
        let array_parts = array.as_parts();
        let mut result_ids = Int64Builder::with_capacity(array.len());

        let term_to_id_table = self.write_txn.open_table(TABLE_TERM_TO_ID)?;

        let mut last_term_tuple: Option<(i8, &str, Option<&str>, Option<&str>)> = None;
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
            } else if let Some(id_val) = term_to_id_table.get(term_tuple)? {
                let id = id_val.value();
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

        Ok(result_ids.finish())
    }

    /// Add a batch from the global dictionary.
    ///
    /// Note: Altering the `claimed_id` is only necessary if in an earlier dictionary transaction,
    /// the global commit succeeded but the local did not.
    pub async fn add_global_batch(
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

        let mut max_id = -1i64;

        for i in 0..batch.num_rows() {
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
        if let Some((next_free, last_free)) = current_claim {
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

    pub fn set_synced_version(&mut self, version: u64) -> Result<(), LocalObjectIdError> {
        let mut metadata_table = self.write_txn.open_table(TABLE_METADATA)?;
        let version_str = version.to_string();
        metadata_table.insert(SYNCED_VERSION_KEY, version_str.as_str())?;
        Ok(())
    }

    pub fn commit(self) -> Result<(), LocalObjectIdError> {
        {
            let mut id_to_term_table = self.write_txn.open_table(TABLE_ID_TO_TERM)?;
            let mut term_to_id_table = self.write_txn.open_table(TABLE_TERM_TO_ID)?;

            for (term_tuple, id) in &self.pending_terms {
                let tuple_ref = (
                    term_tuple.0,
                    term_tuple.1.as_ref(),
                    term_tuple.2.as_deref(),
                    term_tuple.3.as_deref(),
                );
                term_to_id_table.insert(tuple_ref, *id)?;
                id_to_term_table.insert(*id, tuple_ref)?;
                self.term_to_id_cache.insert(term_tuple.clone(), *id);
                self.id_to_term_cache
                    .insert(*id, Arc::new(term_tuple.clone()));
            }

            let current_claim = self.claimed_ids.peek_current_claim();
            let mut metadata_table = self.write_txn.open_table(TABLE_METADATA)?;

            match current_claim {
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
        }

        self.write_txn.commit()?;

        Ok(())
    }

    pub fn abort(mut self) -> Result<(), LocalObjectIdError> {
        {
            let mut metadata_table = self.write_txn.open_table(TABLE_METADATA)?;
            match &self.reset_state_on_conflict {
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
        }

        self.claimed_ids
            .set_claim_state(self.reset_state_on_conflict);
        self.write_txn.commit()?;
        Ok(())
    }
}

/// Initializes the tables and loads the metadata.
fn try_load_initial_claim(
    txn: &redb::WriteTransaction,
) -> Result<Option<(i64, i64)>, LocalObjectIdError> {
    let metadata_table = txn.open_table(TABLE_METADATA)?;

    let mut next_free_id = None;
    if let Some(val) = metadata_table.get(NEXT_FREE_ID_KEY)? {
        next_free_id = Some(
            val.value()
                .parse::<i64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))?,
        );
    }

    let mut last_free_id = None;
    if let Some(val) = metadata_table.get(LAST_FREE_ID_KEY)? {
        last_free_id = Some(
            val.value()
                .parse::<i64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))?,
        );
    }

    match (next_free_id, last_free_id) {
        (Some(next), Some(last)) => {
            if next > last {
                return Err(LocalObjectIdError::Corruption(format!(
                    "Invalid object id claim when loading local object id dictionary ([{next}, {last}])"
                )));
            }
            Ok(Some((next, last)))
        }
        (None, None) => Ok(None),
        _ => Err(LocalObjectIdError::Corruption(
            "Only one claim tracking value was found in the local object id mapping."
                .to_string(),
        )),
    }
}
