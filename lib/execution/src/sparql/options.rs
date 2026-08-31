use rdf_fusion_common::{Iri, NamedNode};
use rdf_fusion_encoding::EncodingName;

/// Defines how many optimizations the query optimizer should apply.
///
/// Currently, the default value is [OptimizationLevel::Full], as we are still searching for a
/// subset that performs well on many queries. Once this subset has been identified, the default
/// value will be [OptimizationLevel::Default].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OptimizationLevel {
    /// No optimizations, except rewrites that are necessary for a working query.
    None,
    /// A balanced default optimization level. Suitable for simple queries or those handling modest
    /// data volumes.
    Default,
    /// Runs all optimizations. Ideal for complex queries or those processing large datasets.
    #[default]
    Full,
}

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

/// Options for SPARQL query evaluation.
#[derive(Clone, Default)]
pub struct QueryOptions {
    /// The defined optimization level
    pub optimization_level: OptimizationLevel,
    /// The encoding to use for output terms
    pub output_encoding_name: Option<EncodingName>,
    /// The base IRI used to resolve relative IRIs in the query.
    pub base_iri: Option<Iri<String>>,
    /// Overrides applied to the query dataset specification.
    pub dataset: DatasetOptions,
}

/// Options for SPARQL update evaluation.
#[derive(Clone, Default)]
pub struct UpdateOptions {
    /// Overrides applied to the datasets of the update operations.
    pub dataset: DatasetOptions,
}
