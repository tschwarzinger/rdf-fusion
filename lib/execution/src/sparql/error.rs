use datafusion::error::DataFusionError;
use oxrdfio::RdfParseError;
use rdf_fusion_model::StorageError;
use rdf_fusion_model::sparql::SparqlSyntaxError;
use rdf_fusion_model::{NamedNode, Term};
use sparesults::QueryResultsParseError;
use std::convert::Infallible;
use std::error::Error;
use std::io;

/// A SPARQL evaluation error
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SparqlEvaluationError {
    /// Error from the underlying RDF dataset
    #[error(transparent)]
    Dataset(Box<dyn Error + Send + Sync>),
    /// Error during `SERVICE` evaluation
    #[error("{0}")]
    Service(#[source] Box<dyn Error + Send + Sync>),
    /// Error if the dataset returns the default graph even if a named graph is expected
    #[error(
        "The SPARQL dataset returned the default graph even if a named graph is expected"
    )]
    UnexpectedDefaultGraph,
    /// The variable storing the `SERVICE` name is unbound
    #[error("The variable encoding the service name is unbound")]
    UnboundService,
    /// Invalid service name
    #[error("{0} is not a valid service name")]
    InvalidServiceName(Term),
    /// The given `SERVICE` is not supported
    #[error("The service {0} is not supported")]
    UnsupportedService(NamedNode),
    #[error("The storage provided a triple term that is not a valid RDF-star term")]
    InvalidStorageTripleTerm,
}

impl From<Infallible> for SparqlEvaluationError {
    #[inline]
    fn from(error: Infallible) -> Self {
        match error {}
    }
}

/// A SPARQL evaluation error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum QueryEvaluationError {
    /// An error in SPARQL parsing.
    #[error(transparent)]
    Parsing(#[from] SparqlSyntaxError),
    /// An error from the storage.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// An error while parsing an external RDF file.
    #[error(transparent)]
    GraphParsing(#[from] RdfParseError),
    /// An error while parsing an external result file (likely from a federated query).
    #[error(transparent)]
    ResultsParsing(#[from] QueryResultsParseError),
    /// An error returned during results serialization.
    #[error(transparent)]
    ResultsSerialization(io::Error),
    /// Error during `SERVICE` evaluation
    #[error("{0}")]
    Service(#[source] Box<dyn Error + Send + Sync + 'static>),
    /// Error when `CREATE` tries to create an already existing graph
    #[error("The graph {0} already exists")]
    GraphAlreadyExists(NamedNode),
    /// Error when `DROP` or `CLEAR` tries to remove a not existing graph
    #[error("The graph {0} does not exist")]
    GraphDoesNotExist(NamedNode),
    /// The variable storing the `SERVICE` name is unbound
    #[error("The variable encoding the service name is unbound")]
    UnboundService,
    /// The given `SERVICE` is not supported
    #[error("The service {0} is not supported")]
    UnsupportedService(NamedNode),
    /// The given content media type returned from an HTTP response is not supported (`SERVICE` and `LOAD`)
    #[error("The content media type {0} is not supported")]
    UnsupportedContentType(String),
    /// The `SERVICE` call has not returns solutions
    #[error("The service is not returning solutions but a boolean or a graph")]
    ServiceDoesNotReturnSolutions,
    /// The results are not a RDF graph
    #[error("The query results are not a RDF graph")]
    NotAGraph,
    #[error("An error returned from the query engine: {0}")]
    Engine(DataFusionError),
    #[error("A feature has not yet been implemented: {0}")]
    NotImplemented(String),
    #[error("An internal error that likely indicates towards a bug in RdfFusion: {0}")]
    InternalError(String),
}

impl QueryEvaluationError {
    pub fn internal<T>(cause: String) -> Result<T, Self> {
        Err(QueryEvaluationError::InternalError(cause))
    }
}

impl From<Infallible> for QueryEvaluationError {
    #[inline]
    fn from(error: Infallible) -> Self {
        match error {}
    }
}

impl From<SparqlEvaluationError> for QueryEvaluationError {
    fn from(error: SparqlEvaluationError) -> Self {
        match error {
            SparqlEvaluationError::Dataset(error) => match error.downcast() {
                Ok(error) => Self::Storage(*error),
                Err(error) => Self::InternalError(error.to_string()),
            },
            SparqlEvaluationError::Service(error) => Self::Service(error),
            #[allow(clippy::todo, reason = "Not production ready")]
            _ => todo!("Integrate error"),
        }
    }
}

impl From<DataFusionError> for QueryEvaluationError {
    fn from(error: DataFusionError) -> Self {
        Self::Engine(error)
    }
}
