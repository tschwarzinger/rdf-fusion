use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use rdf_fusion_common::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LocalObjectIdError {
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),
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
