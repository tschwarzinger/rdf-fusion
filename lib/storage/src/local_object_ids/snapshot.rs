use super::{
    OwnedTermTuple, SYNCED_VERSION_KEY, TABLE_ID_TO_TERM, TABLE_METADATA,
    TABLE_TERM_TO_ID,
};
use crate::local_object_ids::error::LocalObjectIdError;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use quick_cache::sync::Cache;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{PlainTermArrayElementBuilder, PlainTermScalar};
use redb::ReadableTableMetadata;
use std::sync::Arc;

pub struct LocalObjectIdDictionarySnapshot {
    read_txn: redb::ReadTransaction,
    id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
    term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,
}

impl LocalObjectIdDictionarySnapshot {
    /// Creates a new [`LocalObjectIdDictionarySnapshot`].
    pub fn new(
        read_txn: redb::ReadTransaction,
        id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
        term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,
    ) -> Self {
        Self {
            read_txn,
            id_to_term_cache,
            term_to_id_cache,
        }
    }

    pub fn resolve_plain_terms(
        &self,
        ids: &Int64Array,
    ) -> Result<ArrayRef, LocalObjectIdError> {
        let len = ids.len();
        let mut builder = PlainTermArrayElementBuilder::with_capacity(len);

        let id_to_term_table = self.read_txn.open_table(TABLE_ID_TO_TERM)?;

        let mut last_id = i64::MIN;
        let mut last_term: Option<Arc<OwnedTermTuple>> = None;

        for idx in 0..len {
            if ids.is_null(idx) {
                builder.append_null();
                continue;
            };
            let id = ids.value(idx);

            if id == last_id {
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
                last_id = id;
                last_term = Some(cached_term);
            } else if let Some(db_term_val) = id_to_term_table.get(id)? {
                let (term_type, value, data_type, language) = db_term_val.value();
                let owned_tuple: OwnedTermTuple = (
                    term_type,
                    value.into(),
                    data_type.map(|s| s.into()),
                    language.map(|s| s.into()),
                );

                let (term_type, value, data_type, language) = &owned_tuple;
                builder.append_raw(
                    *term_type,
                    value,
                    data_type.as_deref(),
                    language.as_deref(),
                );

                let arc = Arc::new(owned_tuple.clone());
                self.id_to_term_cache.insert(id, Arc::clone(&arc));
                last_id = id;
                last_term = Some(arc);
            } else {
                return Err(LocalObjectIdError::NotFound(id));
            }
        }

        Ok(builder.finish().into_array_ref())
    }

    pub fn len(&self) -> Result<u64, LocalObjectIdError> {
        let table = self.read_txn.open_table(TABLE_ID_TO_TERM)?;
        Ok(table.len()?)
    }

    pub fn read_claimed_object_ids(
        &self,
    ) -> Result<Option<(i64, i64)>, LocalObjectIdError> {
        let Ok(metadata_table) = self.read_txn.open_table(TABLE_METADATA) else {
            return Err(LocalObjectIdError::Corruption(
                "No metadata table found".to_string(),
            ));
        };

        let next_free_str_value = metadata_table.get(super::NEXT_FREE_ID_KEY)?;
        let last_free_str_value = metadata_table.get(super::LAST_FREE_ID_KEY)?;
        let (next_free_str_value, last_free_str_value) =
            match (next_free_str_value, last_free_str_value) {
                (Some(next_free_str_value), Some(last_free_str_value)) => {
                    (next_free_str_value, last_free_str_value)
                }
                (None, None) => return Ok(None),
                _ => {
                    return Err(LocalObjectIdError::Corruption(
                        "Found only one of next and last free id.".to_string(),
                    ));
                }
            };

        let next_free_id = next_free_str_value.value().parse::<i64>().map_err(|err| {
            LocalObjectIdError::Corruption(format!("Invalid next free id found: {err}"))
        })?;
        let last_free_value =
            last_free_str_value.value().parse::<i64>().map_err(|err| {
                LocalObjectIdError::Corruption(format!(
                    "Invalid last free id found: {err}"
                ))
            })?;

        Ok(Some((next_free_id, last_free_value)))
    }

    pub fn is_empty(&self) -> Result<bool, LocalObjectIdError> {
        let table = self.read_txn.open_table(TABLE_ID_TO_TERM)?;
        Ok(table.is_empty()?)
    }

    pub fn get_id_by_term(&self, term: &PlainTermScalar) -> Option<i64> {
        let parts = term.as_parts()?;
        let term_to_id_table = self.read_txn.open_table(TABLE_TERM_TO_ID).ok()?;

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

        let id = term_to_id_table.get(term_tuple).ok()??.value();
        self.term_to_id_cache.insert(owned_tuple, id);
        Some(id)
    }

    pub fn get_synced_version(&self) -> Result<Option<u64>, LocalObjectIdError> {
        let metadata_table = self.read_txn.open_table(TABLE_METADATA)?;
        let version = metadata_table.get(SYNCED_VERSION_KEY)?;

        match version {
            None => Ok(None),
            Some(str_value) => str_value
                .value()
                .parse::<u64>()
                .map_err(|err| LocalObjectIdError::Corruption(err.to_string()))
                .map(Some),
        }
    }
}
