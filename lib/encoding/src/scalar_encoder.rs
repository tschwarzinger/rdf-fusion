use rdf_fusion_common::{BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef, TermRef};

/// Is responsible for encoding RDF terms (or NULL) in a particular encoding.
pub trait ScalarEncoder {
    type Scalar;

    /// Encodes a [TermRef] as a scalar in a particular encoding.
    fn encode_scalar_term(term: TermRef<'_>) -> Self::Scalar {
        match term {
            TermRef::NamedNode(nn) => Self::encode_scalar_named_node(nn),
            TermRef::BlankNode(bnode) => Self::encode_scalar_blank_node(bnode),
            TermRef::Literal(lit) => Self::encode_scalar_literal(lit),
        }
    }

    /// Encodes a [GraphNameRef] as a scalar in a particular encoding.
    fn encode_scalar_graph(graph: GraphNameRef<'_>) -> Self::Scalar;

    /// Encodes NULL as a scalar in a particular encoding.
    fn encode_scalar_null() -> Self::Scalar;

    /// Encodes a [NamedNodeRef] as a scalar in a particular encoding.
    fn encode_scalar_named_node(node: NamedNodeRef<'_>) -> Self::Scalar;

    /// Encodes a [BlankNodeRef] as a scalar in a particular encoding.
    fn encode_scalar_blank_node(node: BlankNodeRef<'_>) -> Self::Scalar;

    /// Encodes a [LiteralRef] as a scalar in a particular encoding.
    fn encode_scalar_literal(literal: LiteralRef<'_>) -> Self::Scalar;
}
