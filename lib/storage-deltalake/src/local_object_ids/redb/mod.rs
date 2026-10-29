mod builder;
mod cache;
mod dictionary;
mod term;

pub use builder::RedbObjectIdDictionaryBuilder;
pub use cache::RedbObjectIdCache;
pub use dictionary::RedbLocalObjectIdDictionary;

use crate::local_object_ids::error::LocalObjectIdError;
use redb::{ReadableTable, TableDefinition, WriteTransaction};
use term::RedbTerm;

/// The redb table name that maps object ids to terms.
const ID_TO_TERM_TABLE: TableDefinition<i64, RedbTerm<'_>> =
    TableDefinition::new("id_to_term");

/// The redb table name that maps terms to object ids.
const TERM_TO_ID_TABLE: TableDefinition<RedbTerm<'_>, i64> =
    TableDefinition::new("term_to_id");

/// The redb table name that stores the metadata.
const METADATA_TABLE: TableDefinition<&str, &str> = TableDefinition::new("metadata");

/// The name of the synced version key in the metadata table.
const SYNCED_VERSION_KEY: &str = "synced_version";

/// The name of the next free id key in the metadata table.
const NEXT_FREE_ID_KEY: &str = "next_free_id";

/// The name of the last free id key in the metadata table.
const LAST_FREE_ID_KEY: &str = "last_free_id";

/// Tries to extract the initial claim from the metadata table.
fn try_load_initial_claim_redb(
    write_txn: &WriteTransaction,
) -> Result<Option<(i64, i64)>, LocalObjectIdError> {
    let metadata_table = write_txn.open_table(METADATA_TABLE)?;

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

    super::validate_initial_claim(next_free_id, last_free_id)
}
