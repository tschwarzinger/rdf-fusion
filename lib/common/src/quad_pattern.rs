use crate::active_graph::ActiveGraph;
use crate::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use crate::{
    BlankNodeMatchingMode, NamedNodePattern, QuadComponent, TermPattern, TriplePattern,
    Variable,
};
use datafusion::logical_expr::{Expr, col};
use std::collections::HashSet;

/// A pattern that matches quads in a storage layer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuadPattern {
    /// The active graph to query.
    pub active_graph: ActiveGraph,
    /// Whether to project the graph to a variable.
    pub graph_variable: Option<Variable>,
    /// The triple pattern to match.
    pub triple_pattern: TriplePattern,
    /// How to handle blank nodes in the pattern.
    pub blank_node_mode: BlankNodeMatchingMode,
}

impl QuadPattern {
    /// Creates a new [QuadPattern].
    pub fn new(
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
    ) -> Self {
        Self {
            active_graph,
            graph_variable,
            triple_pattern: pattern,
            blank_node_mode,
        }
    }

    /// Returns a quad pattern that matches all quads and binds them to the canonical name of the
    /// components.
    ///
    /// In other words (`?graph` also matches the default graph):
    /// ```sparql
    /// GRAPH ?graph { ?subject ?predicate ?object }
    /// ```
    pub fn all_quads() -> Self {
        Self::new(
            ActiveGraph::AllGraphs,
            Some(Variable::new_unchecked(COL_GRAPH)),
            TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked(COL_SUBJECT)),
                predicate: NamedNodePattern::Variable(Variable::new_unchecked(
                    COL_PREDICATE,
                )),
                object: TermPattern::Variable(Variable::new_unchecked(COL_OBJECT)),
            },
            BlankNodeMatchingMode::Variable,
        )
    }

    /// Returns which components of the pattern are bound to a single element (i.e., fixed by the
    /// query).
    pub fn bound_components(&self) -> Vec<QuadComponent> {
        let mut result = Vec::new();

        if self.active_graph.is_bound_to_single_graph() {
            result.push(QuadComponent::GraphName);
        }

        if is_bound_to_single_value(&self.triple_pattern.subject, self.blank_node_mode) {
            result.push(QuadComponent::Subject);
        }

        let predicate = TermPattern::from(self.triple_pattern.predicate.clone());
        if is_bound_to_single_value(&predicate, self.blank_node_mode) {
            result.push(QuadComponent::Predicate);
        }

        if is_bound_to_single_value(&self.triple_pattern.object, self.blank_node_mode) {
            result.push(QuadComponent::Object);
        }

        result
    }

    /// Computes the projections from a quad table for this pattern.
    pub fn compute_projected_components(&self) -> Vec<(QuadComponent, String)> {
        let patterns = [
            self.graph_variable
                .as_ref()
                .map(|v| TermPattern::Variable(v.clone())),
            Some(self.triple_pattern.subject.clone()),
            Some(self.triple_pattern.predicate.clone().into()),
            Some(self.triple_pattern.object.clone()),
        ];

        let mut seen = HashSet::new();
        patterns
            .into_iter()
            .zip(QuadComponent::all())
            .filter_map(|(pattern, component)| pattern.map(|p| (p, component)))
            .filter_map(|(pattern, component)| {
                let name = match pattern {
                    TermPattern::BlankNode(blank_node)
                        if self.blank_node_mode == BlankNodeMatchingMode::Variable =>
                    {
                        blank_node.as_str().to_string()
                    }
                    TermPattern::Variable(variable) => variable.as_str().to_string(),
                    _ => return None,
                };

                if seen.contains(&name) {
                    return None;
                }
                seen.insert(name.clone());
                Some((component, name))
            })
            .collect()
    }

    /// Computes the projections from a quad table for this pattern.
    ///
    /// Calls [`Self::compute_projected_components`] and presents the component as a DataFusion
    /// expression.
    pub fn compute_projection(&self) -> Vec<(Expr, String)> {
        self.compute_projected_components()
            .into_iter()
            .map(|(component, name)| (col(component.column_name()), name))
            .collect()
    }

    /// Returns the number of variables in the pattern.
    pub fn number_of_unique_variables(&self) -> usize {
        let set = self
            .compute_projected_components()
            .into_iter()
            .map(|(component, _)| component)
            .collect::<HashSet<_>>();
        set.len()
    }
}

/// Returns whether the given pattern is bound to a single value.
fn is_bound_to_single_value(
    pattern: &TermPattern,
    blank_node_matching_mode: BlankNodeMatchingMode,
) -> bool {
    match pattern {
        TermPattern::NamedNode(_) => true,
        TermPattern::BlankNode(_) => {
            blank_node_matching_mode == BlankNodeMatchingMode::Filter
        }
        TermPattern::Literal(_) => true,
        TermPattern::Variable(_) => false,
    }
}
