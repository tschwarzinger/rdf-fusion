/// An error returned while parsing and planning a SPARQL query or update.
#[derive(Debug, thiserror::Error)]
pub enum SparqlParseError {
    /// A syntax error returned by the underlying parser.
    #[error(transparent)]
    Syntax(#[from] rdf_fusion_common::sparql::SparqlSyntaxError),

    /// An error while resolving the base IRI.
    #[error(transparent)]
    Iri(#[from] rdf_fusion_common::IriParseError),

    /// An error while creating the LogicalPlan from the parsed algebra.
    #[error(transparent)]
    PlanCreation(#[from] datafusion::error::DataFusionError),
}
