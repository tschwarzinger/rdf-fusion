use crate::index::IndexComponents;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use deltalake::DeltaTableError;
use rdf_fusion_model::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("Error while interacting with the delta storage: {0}")]
pub enum DeltaQuadStorageError {
    #[error(transparent)]
    DeltaError(#[from] DeltaTableError),
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

impl From<DeltaQuadStorageError> for StorageError {
    fn from(value: DeltaQuadStorageError) -> Self {
        StorageError::Other(Box::new(value))
    }
}
