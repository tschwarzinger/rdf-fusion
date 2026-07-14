use crate::index::IndexComponents;
use crate::local_object_ids::LocalObjectIdError;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use deltalake::DeltaTableError;
use rdf_fusion_common::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("Error while interacting with the delta storage: {0}")]
pub enum DeltaQuadsStorageError {
    #[error(transparent)]
    DeltaError(#[from] DeltaTableError),
    #[error(transparent)]
    LocalObjectIdDictionary(#[from] LocalObjectIdError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error("The given stream has an invalid schema. Found schema: {0}")]
    InvalidSchema(SchemaRef),
    #[error("The arrow type '{0}' is not supported by the delta storage.")]
    UnsupportedArrowType(DataType),
    #[error("The index '{0}' is not maintained by the delta storage.")]
    IndexNotFound(IndexComponents),
    #[error("{0}")]
    VersionError(String),
    #[error("An invariant was violated in the storage layer. {0}")]
    Corruption(String),
    #[error("{0}")]
    Other(String),
}

impl From<StorageError> for DeltaQuadsStorageError {
    fn from(value: StorageError) -> Self {
        DeltaQuadsStorageError::Other(value.to_string())
    }
}

impl From<String> for DeltaQuadsStorageError {
    fn from(value: String) -> Self {
        DeltaQuadsStorageError::Other(value)
    }
}

impl From<DeltaQuadsStorageError> for StorageError {
    fn from(value: DeltaQuadsStorageError) -> Self {
        StorageError::Other(Box::new(value))
    }
}
