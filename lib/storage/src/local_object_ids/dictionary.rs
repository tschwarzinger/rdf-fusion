use crate::local_object_ids::OwnedTermTuple;
use crate::local_object_ids::claim::ObjectIdClaimer;
use crate::local_object_ids::error::LocalObjectIdError;
use crate::local_object_ids::snapshot::LocalObjectIdDictionarySnapshot;
use crate::local_object_ids::transaction::LocalObjectIdTransaction;
use quick_cache::sync::Cache;
use redb::{Database, ReadableDatabase};
use std::path::PathBuf;
use std::sync::Arc;

/// Implements a mapping for ObjectIds using redb as a backend.
///
/// **CAVEAT**: this is a synchronous API which does disk access because redb only exposes a
/// synchronous API.
#[derive(Debug, Clone)]
pub struct LocalObjectIdDictionary {
    db: Arc<Database>,
    claimer: Arc<dyn ObjectIdClaimer>,
    id_to_term_cache: Arc<Cache<i64, Arc<OwnedTermTuple>>>,
    term_to_id_cache: Arc<Cache<OwnedTermTuple, i64>>,
}

impl LocalObjectIdDictionary {
    /// Creates a new object id dictionary (file-backed or in-memory).
    pub fn try_new(
        path: Option<PathBuf>,
        cache_size: usize,
        claimer: Arc<dyn ObjectIdClaimer>,
    ) -> Result<Self, LocalObjectIdError> {
        let (db, is_existing) = setup_database(path.as_ref())?;
        if !is_existing {
            initialize_database(&db)?;
        };

        let id_to_term_cache = Arc::new(Cache::new(cache_size));
        let term_to_id_cache = Arc::new(Cache::new(cache_size));

        Ok(Self {
            db: Arc::new(db),
            claimer,
            id_to_term_cache,
            term_to_id_cache,
        })
    }

    pub fn snapshot(
        &self,
    ) -> Result<LocalObjectIdDictionarySnapshot, LocalObjectIdError> {
        let read_txn = self.db.begin_read()?;
        Ok(LocalObjectIdDictionarySnapshot::new(
            read_txn,
            Arc::clone(&self.id_to_term_cache),
            Arc::clone(&self.term_to_id_cache),
        ))
    }

    pub async fn transaction(
        &self,
    ) -> Result<LocalObjectIdTransaction, LocalObjectIdError> {
        let write_txn = self.db.begin_write()?;

        let txn = LocalObjectIdTransaction::try_new(
            write_txn,
            Arc::clone(&self.claimer),
            Arc::clone(&self.id_to_term_cache),
            Arc::clone(&self.term_to_id_cache),
        )?;
        Ok(txn)
    }
}

/// Creates the database for the given path and returns a bool that indicates whether it was newly
/// created.
fn setup_database(
    path: Option<&PathBuf>,
) -> Result<(Database, bool), LocalObjectIdError> {
    if let Some(path) = path {
        if path.exists() {
            Ok((Database::create(path)?, true))
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    LocalObjectIdError::Storage(redb::StorageError::Io(e))
                })?;
            }
            Ok((Database::create(path)?, false))
        }
    } else {
        let backend = redb::backends::InMemoryBackend::new();
        Ok((Database::builder().create_with_backend(backend)?, false))
    }
}

/// Initializes the tables.
fn initialize_database(db: &Database) -> Result<(), LocalObjectIdError> {
    let write_txn = db.begin_write()?;
    write_txn.open_table(super::TABLE_ID_TO_TERM)?;
    write_txn.open_table(super::TABLE_TERM_TO_ID)?;
    write_txn.open_table(super::TABLE_METADATA)?;
    write_txn.commit()?;
    Ok(())
}
