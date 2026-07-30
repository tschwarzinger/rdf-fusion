use rdf_fusion_common::sparql::algebra::GraphTarget;
use rdf_fusion_common::{BlankNode, NamedNode};

/// Represents a graph target for the quad storage implementation.
pub enum QuadStorageGraphTarget {
    /// A named graph.
    NamedNode(NamedNode),
    /// A blank node within the scope of the RDF store.
    BlankNode(BlankNode),
    /// The default graph.
    DefaultGraph,
    /// All graphs.
    NamedGraphs,
    /// All graphs (named graphs including the default graph).
    AllGraphs,
}

impl From<GraphTarget> for QuadStorageGraphTarget {
    fn from(value: GraphTarget) -> Self {
        match value {
            GraphTarget::NamedNode(nn) => QuadStorageGraphTarget::NamedNode(nn),
            GraphTarget::DefaultGraph => QuadStorageGraphTarget::DefaultGraph,
            GraphTarget::NamedGraphs => QuadStorageGraphTarget::NamedGraphs,
            GraphTarget::AllGraphs => QuadStorageGraphTarget::AllGraphs,
        }
    }
}
