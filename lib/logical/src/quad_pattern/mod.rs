mod logical;

pub use logical::*;
pub use rdf_fusion_common::QuadPattern;

use crate::patterns::compute_schema_for_triple_pattern;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Expr, col, lit};
use rdf_fusion_common::quads::COL_GRAPH;
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult, GraphNameRef, TermPattern};
use rdf_fusion_encoding::QuadStorageEncoding;

/// Computes the DataFusion schema for a [`QuadPattern`].
pub fn compute_quad_pattern_schema(
    pattern: &QuadPattern,
    storage_encoding: &QuadStorageEncoding,
) -> DFSchemaRef {
    compute_schema_for_triple_pattern(
        storage_encoding,
        pattern.graph_variable.as_ref().map(|v| v.as_ref()),
        &pattern.triple_pattern,
        pattern.blank_node_mode,
    )
}

/// Computes the filter expressions over a quad table for a [`QuadPattern`].
pub async fn compute_quad_pattern_filters(
    pattern: &QuadPattern,
    storage_encoding: &QuadStorageEncoding,
) -> DFResult<Vec<Expr>> {
    let mut filters = Vec::new();

    let patterns = [
        pattern
            .graph_variable
            .as_ref()
            .map(|v| TermPattern::Variable(v.clone())),
        Some(pattern.triple_pattern.subject.clone()),
        Some(pattern.triple_pattern.predicate.clone().into()),
        Some(pattern.triple_pattern.object.clone()),
    ];

    if let Some(active_graph_filter) =
        filter_active_graph(pattern, storage_encoding).await?
    {
        filters.push(active_graph_filter);
    }

    let term_filters = filter_by_terms(pattern, storage_encoding, &patterns).await?;
    filters.extend(term_filters);

    let variable_filters = filters_on_repeated_variables(pattern, patterns)?;
    filters.extend(variable_filters);

    Ok(filters)
}

/// Computes the filters for the active graph.
async fn filter_active_graph(
    pattern: &QuadPattern,
    storage_encoding: &QuadStorageEncoding,
) -> DFResult<Option<Expr>> {
    let graph_col = col(COL_GRAPH);
    match &pattern.active_graph {
        rdf_fusion_common::ActiveGraph::DefaultGraph => Ok(Some(graph_col.is_null())),
        rdf_fusion_common::ActiveGraph::AnyNamedGraph => {
            Ok(Some(graph_col.is_not_null()))
        }
        rdf_fusion_common::ActiveGraph::Union(graphs) => {
            if graphs.is_empty() {
                return Ok(Some(lit(false)));
            }

            let mut literals = Vec::new();
            let mut include_default = false;
            for g in graphs {
                match g.as_ref() {
                    GraphNameRef::NamedNode(nn) => {
                        let term = storage_encoding.try_encode_term(nn.into()).await?;
                        if let Some(term) = term {
                            literals.push(lit(term))
                        };
                    }
                    GraphNameRef::BlankNode(bn) => {
                        let term = storage_encoding.try_encode_term(bn.into()).await?;
                        if let Some(term) = term {
                            literals.push(lit(term))
                        };
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
        rdf_fusion_common::ActiveGraph::AllGraphs => Ok(None),
    }
}

/// Computes filter expressions for fixed terms in a quad pattern.
async fn filter_by_terms(
    pattern: &QuadPattern,
    storage_encoding: &QuadStorageEncoding,
    patterns: &[Option<TermPattern>; 4],
) -> DFResult<Vec<Expr>> {
    use rdf_fusion_common::quads::{COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
    let mut result = Vec::new();
    let quad_columns = [COL_GRAPH, COL_SUBJECT, COL_PREDICATE, COL_OBJECT];
    for (col_name, p) in quad_columns.iter().zip(patterns.iter()) {
        if let Some(p) = p {
            let term = match p {
                TermPattern::NamedNode(nn) => Some(nn.as_ref().into()),
                TermPattern::Literal(lit) => Some(lit.as_ref().into()),
                TermPattern::BlankNode(bn)
                    if pattern.blank_node_mode == BlankNodeMatchingMode::Filter =>
                {
                    Some(bn.as_ref().into())
                }
                _ => None,
            };

            if let Some(term) = term {
                let term = storage_encoding.try_encode_term(term).await?;
                let expr = if let Some(term) = term {
                    col(*col_name).eq(lit(term))
                } else {
                    lit(false)
                };
                result.push(expr);
            }
        }
    }
    Ok(result)
}

/// Computes filter expressions for repeated variables in a quad pattern.
fn filters_on_repeated_variables(
    pattern: &QuadPattern,
    patterns: [Option<TermPattern>; 4],
) -> DFResult<Vec<Expr>> {
    use rdf_fusion_common::quads::{COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
    use std::collections::HashMap;
    let mut mappings = HashMap::new();
    let quad_columns = [COL_GRAPH, COL_SUBJECT, COL_PREDICATE, COL_OBJECT];
    for (col_name, quad_pattern) in quad_columns.iter().zip(patterns.iter()) {
        let var = match quad_pattern {
            Some(TermPattern::Variable(v)) => Some(v.as_str()),
            Some(TermPattern::BlankNode(bn))
                if pattern.blank_node_mode == BlankNodeMatchingMode::Variable =>
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
