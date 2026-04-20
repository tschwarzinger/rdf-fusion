mod logical;
mod rewrite;

use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use logical::*;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_model::BlankNodeMatchingMode;
use rdf_fusion_model::{TermPattern, TriplePattern, VariableRef};
pub use rewrite::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Computes a DataFusion schema for a SPARQL triple pattern.
///
/// This function creates a schema that includes fields for all variables in the triple pattern.
/// The schema is used to represent the structure of the data that will be produced when
/// evaluating the triple pattern against an RDF dataset.
///
/// # Arguments
/// * `graph_variable` - An optional variable representing the graph name
/// * `pattern` - The triple pattern to compute the schema for
/// * `blank_node_mode` - How blank nodes in the pattern should be treated
///
/// # Returns
/// A reference-counted schema containing fields for all variables in the pattern
///
/// # Additional Resources
/// - [SPARQL 1.1 Query Language - Triple Patterns](https://www.w3.org/TR/sparql11-query/#QSynTriples)
pub fn compute_schema_for_triple_pattern(
    storage_encoding: &QuadStorageEncoding,
    graph_variable: Option<VariableRef<'_>>,
    pattern: &TriplePattern,
    blank_node_mode: BlankNodeMatchingMode,
) -> DFSchemaRef {
    compute_schema_for_pattern(
        &storage_encoding.quad_schema(),
        &[
            graph_variable
                .as_ref()
                .map(|v| TermPattern::Variable(v.into_owned())),
            Some(pattern.subject.clone()),
            Some(pattern.predicate.clone().into()),
            Some(pattern.object.clone()),
        ],
        blank_node_mode,
    )
}

/// Computes a DataFusion schema for a general pattern of RDF terms.
///
/// This lower-level function creates a schema that includes fields for all variables
/// in the given pattern array. It extracts variable names from the pattern elements
/// and creates corresponding fields in the output schema.
///
/// # Arguments
/// * `inner_schema` - The base schema containing field types
/// * `patterns` - An array of optional term patterns (variables, constants, or blank nodes)
/// * `blank_node_mode` - How blank nodes in the pattern should be treated
///
/// # Returns
/// A reference-counted schema containing fields for all variables in the pattern
///
/// # Implementation Note
/// This function assumes that variable names do not clash, which is enforced by
/// the SPARQL syntax.
#[allow(clippy::expect_used, reason = "Variables should not clash")]
pub fn compute_schema_for_pattern(
    inner_schema: &DFSchema,
    patterns: &[Option<TermPattern>],
    blank_node_mode: BlankNodeMatchingMode,
) -> DFSchemaRef {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut fields: Vec<(&str, &Field)> = Vec::new();

    for (pattern, field) in patterns.iter().zip(inner_schema.fields()) {
        match pattern {
            Some(TermPattern::Variable(variable))
                if !seen.contains(variable.as_str()) =>
            {
                seen.insert(variable.as_str());
                fields.push((variable.as_str(), field));
            }
            // A blank node only leads to an output variable if it is matched like a variable
            Some(TermPattern::BlankNode(bnode))
                if blank_node_mode == BlankNodeMatchingMode::Variable
                    && !seen.contains(bnode.as_str()) =>
            {
                seen.insert(bnode.as_str());
                fields.push((bnode.as_str(), field));
            }
            _ => {}
        }
    }

    let fields = fields
        .into_iter()
        .map(|(name, field)| {
            Field::new(name, field.data_type().clone(), field.is_nullable())
        })
        .collect::<Fields>();
    Arc::new(
        DFSchema::from_unqualified_fields(fields, HashMap::new())
            .expect("Fields already deduplicated."),
    )
}
