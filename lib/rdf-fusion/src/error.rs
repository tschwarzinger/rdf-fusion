use oxrdfio::{RdfFormat, RdfParseError};
use rdf_fusion_common::IriParseError;
use rdf_fusion_common::StorageError;
use rdf_fusion_execution::sparql::error::QueryEvaluationError;
use std::io;

/// An error raised while loading a file into a [`Store`](crate::store::Store).
#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    /// An error raised while reading the file.
    #[error(transparent)]
    Parsing(#[from] RdfParseError),
    /// An error raised during the insertion in the store.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The base IRI is invalid.
    #[error("Invalid base IRI '{iri}': {error}")]
    InvalidBaseIri {
        /// The IRI itself.
        iri: String,
        /// The parsing error.
        #[source]
        error: IriParseError,
    },
}

impl From<LoaderError> for io::Error {
    #[inline]
    fn from(error: LoaderError) -> Self {
        match error {
            LoaderError::Storage(error) => error.into(),
            LoaderError::Parsing(error) => error.into(),
            LoaderError::InvalidBaseIri { .. } => {
                Self::new(io::ErrorKind::InvalidInput, error.to_string())
            }
        }
    }
}

/// An error raised while writing a file from a [`Store`](crate::store::Store).

#[derive(Debug, thiserror::Error)]

pub enum SerializerError {
    /// An error raised while writing the content.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// An error raised during accessing the quads in the [`Store`](crate::store::Store).
    #[error(transparent)]
    Evaluation(#[from] QueryEvaluationError),

    /// A format compatible with [RDF dataset](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-dataset) is required.
    #[error("A RDF format supporting datasets was expected, {0} found")]
    DatasetFormatExpected(RdfFormat),
}
