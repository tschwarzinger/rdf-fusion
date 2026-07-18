mod claim;
mod error;
mod in_memory;
mod rocksdb;
mod traits;

pub use claim::{ObjectIdClaim, ObjectIdClaimer, StaticObjectIdClaimer};
pub use error::LocalObjectIdError;
pub use in_memory::InMemoryObjectIdDictionary;
pub use quick_cache::sync::Cache;
pub use rocksdb::RocksDBObjectIdDictionary;
pub use traits::{
    LocalObjectIdDictionary, LocalObjectIdDictionarySnapshot, LocalObjectIdTransaction,
};

type OwnedTermTuple = (i8, String, Option<String>, Option<String>);

pub(crate) const CF_ID_TO_TERM: &str = "id_to_term";
pub(crate) const CF_TERM_TO_ID: &str = "term_to_id";
pub(crate) const CF_METADATA: &str = "metadata";

pub(crate) const SYNCED_VERSION_KEY: &str = "synced_version";
pub(crate) const NEXT_FREE_ID_KEY: &str = "next_free_id";
pub(crate) const LAST_FREE_ID_KEY: &str = "last_free_id";

pub(crate) fn encode_term_tuple_ref(
    tuple: &(i8, &str, Option<&str>, Option<&str>),
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(tuple.0 as u8);
    bytes.extend_from_slice(&(tuple.1.len() as u32).to_le_bytes());
    bytes.extend_from_slice(tuple.1.as_bytes());
    if let Some(dt) = tuple.2 {
        bytes.push(1);
        bytes.extend_from_slice(&(dt.len() as u32).to_le_bytes());
        bytes.extend_from_slice(dt.as_bytes());
    } else {
        bytes.push(0);
    }
    if let Some(lang) = tuple.3 {
        bytes.push(1);
        bytes.extend_from_slice(&(lang.len() as u32).to_le_bytes());
        bytes.extend_from_slice(lang.as_bytes());
    } else {
        bytes.push(0);
    }
    bytes
}

pub(crate) fn decode_term_tuple(bytes: &[u8]) -> OwnedTermTuple {
    let term_type = bytes[0] as i8;
    let mut offset = 1;

    let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let value = std::str::from_utf8(&bytes[offset..offset + len])
        .unwrap()
        .to_string();
    offset += len;

    let data_type = if bytes[offset] == 1 {
        offset += 1;
        let len =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let dt = std::str::from_utf8(&bytes[offset..offset + len])
            .unwrap()
            .to_string();
        offset += len;
        Some(dt)
    } else {
        offset += 1;
        None
    };

    let language = if bytes[offset] == 1 {
        offset += 1;
        let len =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let lang = std::str::from_utf8(&bytes[offset..offset + len])
            .unwrap()
            .to_string();
        Some(lang)
    } else {
        None
    };

    (term_type, value, data_type, language)
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
