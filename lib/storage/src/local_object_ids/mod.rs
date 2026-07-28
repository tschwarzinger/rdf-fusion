mod claim;
mod error;
mod in_memory;
mod lmdb;
mod traits;

pub use claim::{ObjectIdClaim, ObjectIdClaimer, StaticObjectIdClaimer};
pub use error::LocalObjectIdError;
pub use in_memory::InMemoryObjectIdDictionary;
pub use lmdb::LmdbObjectIdDictionary;
pub use quick_cache::sync::Cache;
pub use traits::{
    LocalObjectIdDictionary, LocalObjectIdDictionarySnapshot, LocalObjectIdTransaction,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OwnedTermTuple {
    pub term_type: i8,
    pub value: String,
    pub data_type: Option<String>,
    pub language: Option<String>,
}

pub(crate) const SYNCED_VERSION_KEY: &str = "synced_version";
pub(crate) const NEXT_FREE_ID_KEY: &str = "next_free_id";
pub(crate) const LAST_FREE_ID_KEY: &str = "last_free_id";

pub(crate) fn validate_initial_claim(
    next_free_id: Option<i64>,
    last_free_id: Option<i64>,
) -> Result<Option<(i64, i64)>, LocalObjectIdError> {
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
