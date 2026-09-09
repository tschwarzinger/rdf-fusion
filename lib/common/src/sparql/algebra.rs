//! This is based on [spargebra](https://crates.io/crates/spargebra).

use oxrdf::NamedNode;
use std::fmt;

/// A target RDF graph for update operations.
///
/// Could be a specific graph, all named graphs or the complete dataset.
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum GraphTarget {
    NamedNode(NamedNode),
    DefaultGraph,
    NamedGraphs,
    AllGraphs,
}

impl fmt::Display for GraphTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NamedNode(node) => write!(f, "GRAPH {node}"),
            Self::DefaultGraph => f.write_str("DEFAULT"),
            Self::NamedGraphs => f.write_str("NAMED"),
            Self::AllGraphs => f.write_str("ALL"),
        }
    }
}

impl From<NamedNode> for GraphTarget {
    fn from(node: NamedNode) -> Self {
        Self::NamedNode(node)
    }
}

impl From<oxrdf::GraphName> for GraphTarget {
    fn from(graph_name: oxrdf::GraphName) -> Self {
        match graph_name {
            oxrdf::GraphName::NamedNode(node) => Self::NamedNode(node),
            oxrdf::GraphName::DefaultGraph => Self::DefaultGraph,
            oxrdf::GraphName::BlankNode(_) => {
                unimplemented!("Blank node graph names are not supported in GraphTarget")
            }
        }
    }
}

/// A [property path expression](https://www.w3.org/TR/sparql11-query/#defn_PropertyPathExpr).
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum PropertyPathExpression {
    NamedNode(NamedNode),
    Reverse(Box<Self>),
    Sequence(Box<Self>, Box<Self>),
    Alternative(Box<Self>, Box<Self>),
    ZeroOrMore(Box<Self>),
    OneOrMore(Box<Self>),
    ZeroOrOne(Box<Self>),
    NegatedPropertySet(Vec<NamedNode>),
}

impl fmt::Display for PropertyPathExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NamedNode(p) => p.fmt(f),
            Self::Reverse(p) => write!(f, "^({p})"),
            Self::Sequence(a, b) => write!(f, "({a} / {b})"),
            Self::Alternative(a, b) => write!(f, "({a} | {b})"),
            Self::ZeroOrMore(p) => write!(f, "({p})*"),
            Self::OneOrMore(p) => write!(f, "({p})+"),
            Self::ZeroOrOne(p) => write!(f, "({p})?"),
            Self::NegatedPropertySet(p) => {
                f.write_str("!(")?;
                for (i, c) in p.iter().enumerate() {
                    if i > 0 {
                        f.write_str(" | ")?;
                    }
                    write!(f, "{c}")?;
                }
                f.write_str(")")
            }
        }
    }
}

impl From<NamedNode> for PropertyPathExpression {
    fn from(p: NamedNode) -> Self {
        Self::NamedNode(p)
    }
}
