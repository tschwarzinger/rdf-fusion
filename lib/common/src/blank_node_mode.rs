use std::fmt::{Display, Formatter};

/// Specifies how blank nodes should be matched.
///
/// Blank nodes are scoped to a single graph. Users must choose whether a blank node in a pattern
/// should only match blank nodes with the same label in the graph or match any node (like a
/// variable).
///
/// In SPARQL queries, this strict matching is generally not desired because the blank nodes in the
/// query are different from the blank nodes in the graph (even if they have the same label!). In
/// this case, blank nodes in the query should be treated as variables and can therefore match any
/// node in the graph.
///
/// However, if the goal is to retrieve all quads with a specific blank node as the subject, it
/// may be appropriate to only match the blank node with that exact label in the graph.
///
/// Examples:
///
/// | Mode     | To Match | In Graph | Matches? |
/// |----------|----------|----------|----------|
/// | Variable | `_:a`    | `_:a`    | Yes      |
/// | Variable | `_:a`    | `_:b`    | Yes      |
/// | Filter   | `_:a`    | `_:a`    | Yes      |
/// | Filter   | `_:a`    | `_:b`    | No       |
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum BlankNodeMatchingMode {
    /// Treat blank nodes as variables.
    #[default]
    Variable,
    /// Treat blank nodes as a specific, constant filter (exact label match).
    Filter,
}

impl Display for BlankNodeMatchingMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BlankNodeMatchingMode::Variable => write!(f, "Variable"),
            BlankNodeMatchingMode::Filter => write!(f, "Filter"),
        }
    }
}
