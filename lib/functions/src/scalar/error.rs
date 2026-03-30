use datafusion::common::DataFusionError;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("Could not create SparqlOp implementation. {0}")]
pub struct SparqlUDFCreationError(String);

impl SparqlUDFCreationError {
    /// Creates a new [`SparqlUDFCreationError`].
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl From<SparqlUDFCreationError> for DataFusionError {
    fn from(value: SparqlUDFCreationError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}
