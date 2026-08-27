use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use rdf_fusion_common::StorageError;
use rdf_fusion_encoding::object_id::ObjectIdDictionaryError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LocalObjectIdError {
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Object ID {0} not found")]
    NotFound(i64),
    #[error("{0}")]
    ObjectIdClaimer(String),
    #[error("{0}")]
    Corruption(String),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
}

impl From<redb::Error> for LocalObjectIdError {
    fn from(error: redb::Error) -> Self {
        LocalObjectIdError::Storage(error.to_string())
    }
}

impl From<redb::DatabaseError> for LocalObjectIdError {
    fn from(error: redb::DatabaseError) -> Self {
        LocalObjectIdError::Storage(error.to_string())
    }
}

impl From<redb::TransactionError> for LocalObjectIdError {
    fn from(error: redb::TransactionError) -> Self {
        LocalObjectIdError::Storage(error.to_string())
    }
}

impl From<redb::TableError> for LocalObjectIdError {
    fn from(error: redb::TableError) -> Self {
        LocalObjectIdError::Storage(error.to_string())
    }
}

impl From<redb::CommitError> for LocalObjectIdError {
    fn from(error: redb::CommitError) -> Self {
        LocalObjectIdError::Storage(error.to_string())
    }
}

impl From<redb::StorageError> for LocalObjectIdError {
    fn from(error: redb::StorageError) -> Self {
        LocalObjectIdError::Storage(error.to_string())
    }
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

impl From<LocalObjectIdError> for ObjectIdDictionaryError {
    fn from(value: LocalObjectIdError) -> Self {
        ObjectIdDictionaryError::Storage(Box::new(value))
    }
}
