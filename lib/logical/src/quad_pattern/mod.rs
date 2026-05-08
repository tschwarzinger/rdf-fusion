mod logical;

pub use logical::*;

use crate::active_graph::ActiveGraph;
use crate::patterns::compute_schema_for_triple_pattern;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Expr, col, lit};
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{
    BlankNodeMatchingMode, DFResult, GraphNameRef, NamedNodePattern, QuadComponent,
    TermPattern, TriplePattern, Variable,
};
use rdf_fusion_encoding::QuadStorageEncoding;
use std::collections::{HashMap, HashSet};

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
    pub fn for_all_quads() -> Self {
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

    /// Computes the schema for this [QuadPattern].
    pub fn compute_schema(&self, storage_encoding: &QuadStorageEncoding) -> DFSchemaRef {
        compute_schema_for_triple_pattern(
            storage_encoding,
            self.graph_variable.as_ref().map(|v| v.as_ref()),
            &self.triple_pattern,
            self.blank_node_mode,
        )
    }

    /// Computes the filter expressions over a quad table for this pattern.
    pub fn compute_filters(
        &self,
        storage_encoding: &QuadStorageEncoding,
    ) -> DFResult<Vec<Expr>> {
        let mut filters = Vec::new();

        let patterns = [
            self.graph_variable
                .as_ref()
                .map(|v| TermPattern::Variable(v.clone())),
            Some(self.triple_pattern.subject.clone()),
            Some(self.triple_pattern.predicate.clone().into()),
            Some(self.triple_pattern.object.clone()),
        ];

        if let Some(active_graph_filter) = self.filter_active_graph(storage_encoding)? {
            filters.push(active_graph_filter);
        }

        let term_filters = self.filter_by_terms(storage_encoding, &patterns)?;
        filters.extend(term_filters);

        let variable_filters = self.filters_on_repeated_variables(patterns)?;
        filters.extend(variable_filters);

        Ok(filters)
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

        patterns
            .into_iter()
            .zip(QuadComponent::all())
            .filter_map(|(pattern, component)| pattern.map(|p| (p, component)))
            .filter_map(|(pattern, component)| match pattern {
                TermPattern::BlankNode(blank_node)
                    if self.blank_node_mode == BlankNodeMatchingMode::Variable =>
                {
                    Some((component, blank_node.as_str().to_string()))
                }
                TermPattern::Variable(variable) => {
                    Some((component, variable.as_str().to_string()))
                }
                _ => None,
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

    /// Computes the filters for the active graph.
    fn filter_active_graph(
        &self,
        storage_encoding: &QuadStorageEncoding,
    ) -> DFResult<Option<Expr>> {
        let graph_col = col(COL_GRAPH);
        match &self.active_graph {
            ActiveGraph::DefaultGraph => Ok(Some(graph_col.is_null())),
            ActiveGraph::AnyNamedGraph => Ok(Some(graph_col.is_not_null())),
            ActiveGraph::Union(graphs) => {
                if graphs.is_empty() {
                    return Ok(Some(lit(false)));
                }

                let mut literals = Vec::new();
                let mut include_default = false;
                for g in graphs {
                    match g.as_ref() {
                        GraphNameRef::NamedNode(nn) => {
                            literals.push(lit(
                                storage_encoding.encode_term_scalar(nn.into())?
                            ));
                        }
                        GraphNameRef::BlankNode(bn) => {
                            literals.push(lit(
                                storage_encoding.encode_term_scalar(bn.into())?
                            ));
                        }
                        GraphNameRef::DefaultGraph => include_default = true,
                    };
                }

                let filter = if literals.is_empty() {
                    if include_default {
                        graph_col.is_null()
                    } else {
                        lit(false)
                    }
                } else if include_default {
                    graph_col
                        .clone()
                        .in_list(literals, false)
                        .or(graph_col.is_null())
                } else {
                    graph_col.in_list(literals, false)
                };
                Ok(Some(filter))
            }
            ActiveGraph::AllGraphs => Ok(None),
        }
    }

    /// Computes filter expressions for fixed terms in a quad pattern.
    fn filter_by_terms(
        &self,
        storage_encoding: &QuadStorageEncoding,
        patterns: &[Option<TermPattern>; 4],
    ) -> DFResult<Vec<Expr>> {
        let mut result = Vec::new();
        let quad_columns = [COL_GRAPH, COL_SUBJECT, COL_PREDICATE, COL_OBJECT];
        for (col_name, p) in quad_columns.iter().zip(patterns.iter()) {
            if let Some(p) = p {
                let term = match p {
                    TermPattern::NamedNode(nn) => Some(nn.as_ref().into()),
                    TermPattern::Literal(lit) => Some(lit.as_ref().into()),
                    TermPattern::BlankNode(bn)
                        if self.blank_node_mode == BlankNodeMatchingMode::Filter =>
                    {
                        Some(bn.as_ref().into())
                    }
                    _ => None,
                };

                if let Some(term) = term {
                    result.push(
                        col(*col_name)
                            .eq(lit(storage_encoding.encode_term_scalar(term)?)),
                    );
                }
            }
        }
        Ok(result)
    }

    /// Computes filter expressions for repeated variables in a quad patterns.
    fn filters_on_repeated_variables(
        &self,
        patterns: [Option<TermPattern>; 4],
    ) -> DFResult<Vec<Expr>> {
        let mut mappings = HashMap::new();
        let quad_columns = [COL_GRAPH, COL_SUBJECT, COL_PREDICATE, COL_OBJECT];
        for (col_name, pattern) in quad_columns.iter().zip(patterns.iter()) {
            let var = match pattern {
                Some(TermPattern::Variable(v)) => Some(v.as_str()),
                Some(TermPattern::BlankNode(bn))
                    if self.blank_node_mode == BlankNodeMatchingMode::Variable =>
                {
                    Some(bn.as_str())
                }
                _ => None,
            };

            if let Some(var) = var {
                mappings
                    .entry(var.to_string())
                    .or_insert_with(Vec::new)
                    .push(*col_name);
            }
        }

        let mut result = Vec::new();
        for columns in mappings.into_values() {
            if columns.len() > 1 {
                let first_col = col(columns[0]);
                for other_col in columns.iter().skip(1) {
                    result.push(first_col.clone().eq(col(*other_col)));
                }
            }
        }
        Ok(result)
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
