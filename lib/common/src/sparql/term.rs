//! This is based on [spargebra](https://crates.io/crates/spargebra).

use oxrdf::{BlankNode, Literal, NamedNode, NamedOrBlankNode, Term, Triple, Variable};
use std::fmt;

/// The union of [IRIs](https://www.w3.org/TR/rdf11-concepts/#dfn-iri) and [literals](https://www.w3.org/TR/rdf11-concepts/#dfn-literal).
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum GroundTerm {
    NamedNode(NamedNode),
    Literal(Literal),
}

impl fmt::Display for GroundTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NamedNode(node) => node.fmt(f),
            Self::Literal(literal) => literal.fmt(f),
        }
    }
}

impl From<NamedNode> for GroundTerm {
    fn from(node: NamedNode) -> Self {
        Self::NamedNode(node)
    }
}

impl From<Literal> for GroundTerm {
    fn from(literal: Literal) -> Self {
        Self::Literal(literal)
    }
}

impl TryFrom<Term> for GroundTerm {
    type Error = ();

    fn try_from(term: Term) -> Result<Self, Self::Error> {
        match term {
            Term::NamedNode(t) => Ok(t.into()),
            Term::BlankNode(_) => Err(()),
            Term::Literal(t) => Ok(t.into()),
        }
    }
}

impl From<GroundTerm> for Term {
    fn from(term: GroundTerm) -> Self {
        match term {
            GroundTerm::NamedNode(t) => t.into(),
            GroundTerm::Literal(l) => l.into(),
        }
    }
}

/// The union of [IRIs](https://www.w3.org/TR/rdf11-concepts/#dfn-iri) and [variables](https://www.w3.org/TR/sparql11-query/#sparqlQueryVariables).
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum NamedNodePattern {
    NamedNode(NamedNode),
    Variable(Variable),
}

impl fmt::Display for NamedNodePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NamedNode(node) => node.fmt(f),
            Self::Variable(var) => var.fmt(f),
        }
    }
}

impl From<NamedNode> for NamedNodePattern {
    fn from(node: NamedNode) -> Self {
        Self::NamedNode(node)
    }
}

impl From<Variable> for NamedNodePattern {
    fn from(var: Variable) -> Self {
        Self::Variable(var)
    }
}

impl TryFrom<NamedNodePattern> for NamedNode {
    type Error = ();

    fn try_from(pattern: NamedNodePattern) -> Result<Self, Self::Error> {
        match pattern {
            NamedNodePattern::NamedNode(t) => Ok(t),
            NamedNodePattern::Variable(_) => Err(()),
        }
    }
}

/// The union of [terms](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-term) and [variables](https://www.w3.org/TR/sparql11-query/#sparqlQueryVariables).
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum TermPattern {
    NamedNode(NamedNode),
    BlankNode(BlankNode),
    Literal(Literal),
    Variable(Variable),
}

impl fmt::Display for TermPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NamedNode(term) => term.fmt(f),
            Self::BlankNode(term) => term.fmt(f),
            Self::Literal(term) => term.fmt(f),
            Self::Variable(var) => var.fmt(f),
        }
    }
}

impl From<NamedNode> for TermPattern {
    fn from(node: NamedNode) -> Self {
        Self::NamedNode(node)
    }
}

impl From<BlankNode> for TermPattern {
    fn from(node: BlankNode) -> Self {
        Self::BlankNode(node)
    }
}

impl From<Literal> for TermPattern {
    fn from(literal: Literal) -> Self {
        Self::Literal(literal)
    }
}

impl From<Variable> for TermPattern {
    fn from(var: Variable) -> Self {
        Self::Variable(var)
    }
}

impl From<NamedOrBlankNode> for TermPattern {
    fn from(subject: NamedOrBlankNode) -> Self {
        match subject {
            NamedOrBlankNode::NamedNode(node) => node.into(),
            NamedOrBlankNode::BlankNode(node) => node.into(),
        }
    }
}

impl From<Term> for TermPattern {
    fn from(term: Term) -> Self {
        match term {
            Term::NamedNode(node) => node.into(),
            Term::BlankNode(node) => node.into(),
            Term::Literal(literal) => literal.into(),
        }
    }
}

impl From<NamedNodePattern> for TermPattern {
    fn from(element: NamedNodePattern) -> Self {
        match element {
            NamedNodePattern::NamedNode(node) => node.into(),
            NamedNodePattern::Variable(var) => var.into(),
        }
    }
}

impl From<GroundTermPattern> for TermPattern {
    fn from(element: GroundTermPattern) -> Self {
        match element {
            GroundTermPattern::NamedNode(node) => node.into(),
            GroundTermPattern::Literal(literal) => literal.into(),
            GroundTermPattern::Variable(variable) => variable.into(),
        }
    }
}

impl TryFrom<TermPattern> for NamedOrBlankNode {
    type Error = ();

    fn try_from(term: TermPattern) -> Result<Self, Self::Error> {
        match term {
            TermPattern::NamedNode(t) => Ok(t.into()),
            TermPattern::BlankNode(t) => Ok(t.into()),
            TermPattern::Literal(_) | TermPattern::Variable(_) => Err(()),
        }
    }
}

impl TryFrom<TermPattern> for Term {
    type Error = ();

    fn try_from(pattern: TermPattern) -> Result<Self, Self::Error> {
        match pattern {
            TermPattern::NamedNode(t) => Ok(t.into()),
            TermPattern::BlankNode(t) => Ok(t.into()),
            TermPattern::Literal(t) => Ok(t.into()),
            TermPattern::Variable(_) => Err(()),
        }
    }
}

/// The union of [terms](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-term) and [variables](https://www.w3.org/TR/sparql11-query/#sparqlQueryVariables) without blank nodes.
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum GroundTermPattern {
    NamedNode(NamedNode),
    Literal(Literal),
    Variable(Variable),
}

impl fmt::Display for GroundTermPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NamedNode(term) => term.fmt(f),
            Self::Literal(term) => term.fmt(f),
            Self::Variable(var) => var.fmt(f),
        }
    }
}

impl From<NamedNode> for GroundTermPattern {
    fn from(node: NamedNode) -> Self {
        Self::NamedNode(node)
    }
}

impl From<Literal> for GroundTermPattern {
    fn from(literal: Literal) -> Self {
        Self::Literal(literal)
    }
}

impl From<Variable> for GroundTermPattern {
    fn from(var: Variable) -> Self {
        Self::Variable(var)
    }
}

impl From<GroundTerm> for GroundTermPattern {
    fn from(term: GroundTerm) -> Self {
        match term {
            GroundTerm::NamedNode(node) => node.into(),
            GroundTerm::Literal(literal) => literal.into(),
        }
    }
}

impl From<NamedNodePattern> for GroundTermPattern {
    fn from(element: NamedNodePattern) -> Self {
        match element {
            NamedNodePattern::NamedNode(node) => node.into(),
            NamedNodePattern::Variable(var) => var.into(),
        }
    }
}

impl TryFrom<TermPattern> for GroundTermPattern {
    type Error = ();

    fn try_from(pattern: TermPattern) -> Result<Self, Self::Error> {
        Ok(match pattern {
            TermPattern::NamedNode(named_node) => named_node.into(),
            TermPattern::BlankNode(_) => return Err(()),
            TermPattern::Literal(literal) => literal.into(),
            TermPattern::Variable(variable) => variable.into(),
        })
    }
}

/// The union of [IRIs](https://www.w3.org/TR/rdf11-concepts/#dfn-iri), [default graph name](https://www.w3.org/TR/rdf11-concepts/#dfn-default-graph) and [variables](https://www.w3.org/TR/sparql11-query/#sparqlQueryVariables).
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum GraphNamePattern {
    NamedNode(NamedNode),
    DefaultGraph,
    Variable(Variable),
}

impl fmt::Display for GraphNamePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NamedNode(node) => node.fmt(f),
            Self::DefaultGraph => f.write_str("DEFAULT"),
            Self::Variable(var) => var.fmt(f),
        }
    }
}

impl From<NamedNode> for GraphNamePattern {
    fn from(node: NamedNode) -> Self {
        Self::NamedNode(node)
    }
}

impl From<Variable> for GraphNamePattern {
    fn from(var: Variable) -> Self {
        Self::Variable(var)
    }
}

impl From<oxrdf::GraphName> for GraphNamePattern {
    fn from(graph_name: oxrdf::GraphName) -> Self {
        match graph_name {
            oxrdf::GraphName::NamedNode(node) => node.into(),
            oxrdf::GraphName::DefaultGraph => Self::DefaultGraph,
            oxrdf::GraphName::BlankNode(_) => unimplemented!(
                "Blank node graph names are not supported in GraphNamePattern"
            ),
        }
    }
}

impl From<NamedNodePattern> for GraphNamePattern {
    fn from(graph_name: NamedNodePattern) -> Self {
        match graph_name {
            NamedNodePattern::NamedNode(node) => node.into(),
            NamedNodePattern::Variable(var) => var.into(),
        }
    }
}

/// A [triple pattern](https://www.w3.org/TR/sparql11-query/#defn_TriplePattern)
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct TriplePattern {
    pub subject: TermPattern,
    pub predicate: NamedNodePattern,
    pub object: TermPattern,
}

impl TriplePattern {
    pub fn new(
        subject: impl Into<TermPattern>,
        predicate: impl Into<NamedNodePattern>,
        object: impl Into<TermPattern>,
    ) -> Self {
        Self {
            subject: subject.into(),
            predicate: predicate.into(),
            object: object.into(),
        }
    }
}

impl fmt::Display for TriplePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.subject, self.predicate, self.object)
    }
}

impl From<Triple> for TriplePattern {
    fn from(triple: Triple) -> Self {
        Self {
            subject: triple.subject.into(),
            predicate: triple.predicate.into(),
            object: triple.object.into(),
        }
    }
}

impl TryFrom<TriplePattern> for Triple {
    type Error = ();

    fn try_from(triple: TriplePattern) -> Result<Self, Self::Error> {
        Ok(Self {
            subject: triple.subject.try_into()?,
            predicate: triple.predicate.try_into()?,
            object: triple.object.try_into()?,
        })
    }
}

/// A [triple pattern](https://www.w3.org/TR/sparql11-query/#defn_TriplePattern) in a specific graph
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct QuadPattern {
    pub subject: TermPattern,
    pub predicate: NamedNodePattern,
    pub object: TermPattern,
    pub graph_name: GraphNamePattern,
}

impl QuadPattern {
    pub fn new(
        subject: impl Into<TermPattern>,
        predicate: impl Into<NamedNodePattern>,
        object: impl Into<TermPattern>,
        graph_name: impl Into<GraphNamePattern>,
    ) -> Self {
        Self {
            subject: subject.into(),
            predicate: predicate.into(),
            object: object.into(),
            graph_name: graph_name.into(),
        }
    }
}

impl fmt::Display for QuadPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.graph_name == GraphNamePattern::DefaultGraph {
            write!(f, "{} {} {}", self.subject, self.predicate, self.object)
        } else {
            write!(
                f,
                "GRAPH {} {{ {} {} {} }}",
                self.graph_name, self.subject, self.predicate, self.object
            )
        }
    }
}

/// A [triple pattern](https://www.w3.org/TR/sparql11-query/#defn_TriplePattern) in a specific graph without blank nodes.
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct GroundQuadPattern {
    pub subject: GroundTermPattern,
    pub predicate: NamedNodePattern,
    pub object: GroundTermPattern,
    pub graph_name: GraphNamePattern,
}

impl GroundQuadPattern {
    pub fn new(
        subject: impl Into<GroundTermPattern>,
        predicate: impl Into<NamedNodePattern>,
        object: impl Into<GroundTermPattern>,
        graph_name: impl Into<GraphNamePattern>,
    ) -> Self {
        Self {
            subject: subject.into(),
            predicate: predicate.into(),
            object: object.into(),
            graph_name: graph_name.into(),
        }
    }
}

impl fmt::Display for GroundQuadPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.graph_name == GraphNamePattern::DefaultGraph {
            write!(f, "{} {} {}", self.subject, self.predicate, self.object)
        } else {
            write!(
                f,
                "GRAPH {} {{ {} {} {} }}",
                self.graph_name, self.subject, self.predicate, self.object
            )
        }
    }
}

impl TryFrom<QuadPattern> for GroundQuadPattern {
    type Error = ();

    fn try_from(pattern: QuadPattern) -> Result<Self, Self::Error> {
        Ok(Self {
            subject: pattern.subject.try_into()?,
            predicate: pattern.predicate,
            object: pattern.object.try_into()?,
            graph_name: pattern.graph_name,
        })
    }
}
