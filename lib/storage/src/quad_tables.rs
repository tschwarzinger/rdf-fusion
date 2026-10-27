//! General-purpose types for quad tables

use rdf_fusion_common::{
    BlankNodeMatchingMode, NamedNodePattern, QuadComponent, TermPattern, TriplePattern,
};
use rdf_fusion_logical::ActiveGraph;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// Represents a list of *disjunct* quad table components that represents the sort order of a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuadTableName([QuadComponent; 4]);

impl Display for QuadTableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for component in self.0.iter() {
            write!(f, "{component}")?;
        }
        Ok(())
    }
}

// Allows concisely generating all 24 constants
macro_rules! define_quad_tables {
    ($($name:ident => [$a:ident, $b:ident, $c:ident, $d:ident]),* $(,)?) => {
        $(
            #[doc = concat!("A ", stringify!($name), " quad_table.")]
            pub const $name: QuadTableName = QuadTableName([
                QuadComponent::$a,
                QuadComponent::$b,
                QuadComponent::$c,
                QuadComponent::$d,
            ]);
        )*

        /// Returns a list of all 24 valid [`QuadTableName`] permutations.
        pub const fn list_all() -> &'static [QuadTableName; 24] {
            &[$(Self::$name),*]
        }
    };
}

impl QuadTableName {
    define_quad_tables! {
        // Subject First
        SPOG => [Subject, Predicate, Object, GraphName],
        SPGO => [Subject, Predicate, GraphName, Object],
        SOPG => [Subject, Object, Predicate, GraphName],
        SOGP => [Subject, Object, GraphName, Predicate],
        SGPO => [Subject, GraphName, Predicate, Object],
        SGOP => [Subject, GraphName, Object, Predicate],

        // Predicate First
        PSOG => [Predicate, Subject, Object, GraphName],
        PSGO => [Predicate, Subject, GraphName, Object],
        POSG => [Predicate, Object, Subject, GraphName],
        POGS => [Predicate, Object, GraphName, Subject],
        PGSO => [Predicate, GraphName, Subject, Object],
        PGOS => [Predicate, GraphName, Object, Subject],

        // Object First
        OSPG => [Object, Subject, Predicate, GraphName],
        OSGP => [Object, Subject, GraphName, Predicate],
        OPSG => [Object, Predicate, Subject, GraphName],
        OPGS => [Object, Predicate, GraphName, Subject],
        OGSP => [Object, GraphName, Subject, Predicate],
        OGPS => [Object, GraphName, Predicate, Subject],

        // GraphName First
        GSPO => [GraphName, Subject, Predicate, Object],
        GSOP => [GraphName, Subject, Object, Predicate],
        GPSO => [GraphName, Predicate, Subject, Object],
        GPOS => [GraphName, Predicate, Object, Subject],
        GOSP => [GraphName, Object, Subject, Predicate],
        GOPS => [GraphName, Object, Predicate, Subject],
    }

    /// Returns a reference to the inner array.
    pub const fn inner(&self) -> &[QuadComponent; 4] {
        &self.0
    }

    /// Tries to create a new [QuadTableName].
    ///
    /// Returns an error if a [QuadComponent] appears more than once.
    pub fn try_new(
        components: [QuadComponent; 4],
    ) -> Result<Self, QuadTableNameCreationError> {
        let distinct = components.iter().collect::<HashSet<_>>();
        if distinct.len() != components.len() {
            return Err(QuadTableNameCreationError);
        }

        Ok(QuadTableName(components))
    }
}

#[derive(Debug, Error)]
#[error("Duplicate quad table component given.")]
pub struct QuadTableNameCreationError;

/// Computes how well a quad table sorted by `components` supports scanning the `pattern`. A
/// higher score is better.
///
/// Each bound component of the sort order adds to the score, weighted by its position. Hence, a
/// sort order that starts with the bound components is preferred, as the scan only covers a
/// contiguous slice of the table.
pub fn compute_scan_score(
    components: &[QuadComponent],
    active_graph: &ActiveGraph,
    pattern: &TriplePattern,
    blank_node_mode: BlankNodeMatchingMode,
) -> usize {
    let mut score = 0;
    for (i, component) in components.iter().enumerate() {
        let is_bound =
            is_component_bound(*component, active_graph, pattern, blank_node_mode);

        // We don't stop after finding a non-bound component, because components that are part of
        // the sort order should still exhibit a better clustering, even though the scan is no
        // longer restricted to a slice of the quad table.
        if is_bound {
            let position = 3usize.saturating_sub(i);
            score += 1 << position;
        }
    }
    score
}

/// Returns whether the pattern restricts the `component` to constants (e.g., a named node or the
/// default graph).
pub fn is_component_bound(
    component: QuadComponent,
    active_graph: &ActiveGraph,
    pattern: &TriplePattern,
    blank_node_mode: BlankNodeMatchingMode,
) -> bool {
    match component {
        QuadComponent::GraphName => matches!(
            active_graph,
            ActiveGraph::DefaultGraph | ActiveGraph::Union(_)
        ),
        QuadComponent::Subject => is_term_bound(&pattern.subject, blank_node_mode),
        QuadComponent::Predicate => {
            matches!(pattern.predicate, NamedNodePattern::NamedNode(_))
        }
        QuadComponent::Object => is_term_bound(&pattern.object, blank_node_mode),
    }
}

fn is_term_bound(pattern: &TermPattern, mode: BlankNodeMatchingMode) -> bool {
    match pattern {
        TermPattern::NamedNode(_) | TermPattern::Literal(_) => true,
        TermPattern::BlankNode(_) => mode == BlankNodeMatchingMode::Filter,
        TermPattern::Variable(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::quad_tables::{QuadComponent, QuadTableName};

    #[test]
    fn quad_table_configuration_accepts_unique_components() {
        let ok = QuadTableName::try_new([
            QuadComponent::GraphName,
            QuadComponent::Subject,
            QuadComponent::Predicate,
            QuadComponent::Object,
        ]);
        assert!(ok.is_ok());
    }

    #[test]
    fn quad_table_configuration_rejects_duplicate_components() {
        let err = QuadTableName::try_new([
            QuadComponent::GraphName,
            QuadComponent::Subject,
            QuadComponent::Subject,
            QuadComponent::Object,
        ]);
        assert!(err.is_err());
    }
}
