use crate::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::fmt::{Display, Formatter};

/// Represents what part of an RDF quad is index at the given position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum QuadComponent {
    /// The graph name
    GraphName,
    /// The subject
    Subject,
    /// The predicate
    Predicate,
    /// The object
    Object,
}

impl Display for QuadComponent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QuadComponent::GraphName => write!(f, "G"),
            QuadComponent::Subject => write!(f, "S"),
            QuadComponent::Predicate => write!(f, "P"),
            QuadComponent::Object => write!(f, "O"),
        }
    }
}

impl QuadComponent {
    /// Returns all components of a quad.
    pub fn all() -> [QuadComponent; 4] {
        [
            QuadComponent::GraphName,
            QuadComponent::Subject,
            QuadComponent::Predicate,
            QuadComponent::Object,
        ]
    }

    /// Returns the index of the component in an GSPO quad pattern.
    pub fn gspo_index(&self) -> usize {
        match self {
            QuadComponent::GraphName => 0,
            QuadComponent::Subject => 1,
            QuadComponent::Predicate => 2,
            QuadComponent::Object => 3,
        }
    }

    /// Returns the column name for the component.
    pub fn column_name(&self) -> &'static str {
        match self {
            QuadComponent::GraphName => COL_GRAPH,
            QuadComponent::Subject => COL_SUBJECT,
            QuadComponent::Predicate => COL_PREDICATE,
            QuadComponent::Object => COL_OBJECT,
        }
    }

    /// Returns the component for the given character (G, S, P, O).
    pub fn from_char(c: char) -> Option<Self> {
        match c.to_ascii_uppercase() {
            'G' => Some(QuadComponent::GraphName),
            'S' => Some(QuadComponent::Subject),
            'P' => Some(QuadComponent::Predicate),
            'O' => Some(QuadComponent::Object),
            _ => None,
        }
    }
}
