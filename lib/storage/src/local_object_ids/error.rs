use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use rdf_fusion_common::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LocalObjectIdError {
    #[error("Database error: {0}")]
    Database(#[from] redb::DatabaseError),
    #[error("Transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("Table error: {0}")]
    Table(#[from] redb::TableError),
    #[error("Storage error: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("Commit error: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("Object ID {0} not found")]
    NotFound(i64),
    #[error("{0}")]
    Corruption(String),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
}

impl From<LocalObjectIdError> for DataFusionError {
    fn from(error: LocalObjectIdError) -> Self {
        DataFusionError::External(Box::new(error))
    }
}

impl From<LocalObjectIdError> for StorageError {
    fn from(error: LocalObjectIdError) -> Self {
        StorageError::Other(Box::new(error))
    }
}
