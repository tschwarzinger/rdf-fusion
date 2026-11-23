use rdf_fusion_common::sparql::QueryDataset;
use rdf_fusion_common::{DateTime, Iri, NamedNode};
use rdf_fusion_encoding::EncodingName;

pub use rdf_fusion_common::config::OptimizationLevel;

/// Options for a SPARQL query dataset specification.
///
/// If an override is `None`, the corresponding part of the query's own dataset specification is
/// left untouched.
#[derive(Clone, Debug, Default)]
pub struct DatasetOptions {
    /// Use the union of all graphs in the store as the default graph.
    pub default_graph_as_union: bool,
    /// The graphs to use as the default graph.
    pub default_graphs: Option<Vec<NamedNode>>,
    /// The named graphs that are available to the query.
    pub named_graphs: Option<Vec<NamedNode>>,
}

impl DatasetOptions {
    /// Creates a new [`QueryDataset`] based on these options.
    pub fn as_query_dataset(&self) -> QueryDataset {
        // TODO: default_graph_as_union
        QueryDataset::new(
            self.default_graphs
                .as_ref()
                .map(|gs| gs.iter().map(|g| g.clone().into()).collect()),
            self.named_graphs
                .as_ref()
                .map(|gs| gs.iter().map(|g| g.clone().into()).collect()),
        )
    }
}

/// Options for SPARQL query evaluation.
#[derive(Clone, Default)]
pub struct QueryOptions {
    /// The encoding to use for output terms
    pub output_encoding_name: Option<EncodingName>,
    /// The base IRI used to resolve relative IRIs in the query.
    pub base_iri: Option<Iri<String>>,
    /// Overrides applied to the query dataset specification.
    pub dataset: DatasetOptions,
    /// The time used to evaluate `NOW()`. When `None`, the system clock is queried.
    pub now: Option<DateTime>,
}

/// Options for SPARQL update evaluation.
#[derive(Clone, Default)]
pub struct UpdateOptions {
    /// Overrides applied to the datasets of the update operations.
    pub dataset: DatasetOptions,
    /// The time used to evaluate `NOW()`. When `None`, the system clock is queried.
    pub now: Option<DateTime>,
}
