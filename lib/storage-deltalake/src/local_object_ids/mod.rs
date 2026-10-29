mod claim;
mod error;
mod redb;
mod traits;

pub use claim::{ObjectIdClaim, ObjectIdClaimer, StaticObjectIdClaimer};
pub use error::LocalObjectIdError;
pub use redb::*;
pub use traits::{
    LocalObjectIdDictionary, LocalObjectIdDictionarySnapshot, LocalObjectIdTransaction,
};

/// Represents a term stored in the redb database.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LocalDictionaryTerm {
    pub term_type: i8,
    pub value: String,
    pub data_type: Option<String>,
    pub language: Option<String>,
}

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
