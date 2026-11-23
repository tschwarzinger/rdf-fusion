use crate::ActiveGraph;
use crate::quad_pattern::{QuadPattern, compute_quad_pattern_schema};
use datafusion::arrow::datatypes::Fields;
use datafusion::common::{DFSchema, DFSchemaRef, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_common::{NamedNodePattern, Term, TermPattern, TriplePattern, Variable};
use rdf_fusion_encoding::QuadStorageEncoding;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

/// A logical node that represents a scan of quads matching a pattern.
///
/// This node is the main entry point for accessing RDF data in the query plan.
/// It is responsible for retrieving quads from the underlying storage that match
/// the given `active_graph` and `pattern`.
///
/// ### Blank Node Matching
///
/// The `blank_node_mode` determines how blank nodes in the pattern are handled.
/// See [BlankNodeMatchingMode] for more details.
///
/// ### Planning [QuadPatternNode]
///
/// Planning the [QuadPatternNode] requires users to define a specialized planner for the used
/// storage layer. This is because the planner should consider storage-specific problems like
/// sharing a snapshot across multiple scans of the quads table in a single query. The built-in
/// storage layers of RDF Fusion provide examples.
#[derive(PartialEq, Eq, Clone)]
pub struct QuadPatternNode {
    /// The encoding of the storage layer.
    storage_encoding: QuadStorageEncoding,
    /// The pattern to match.
    pattern: QuadPattern,
    /// The schema of the result.
    schema: DFSchemaRef,
    /// The projection to apply.
    pub projection: Option<Vec<usize>>,
}

// The schema is derived from the other fields. Hence, we do not hash it as hashing the (nested)
// schema is expensive and DataFusion hashes the plan in every optimizer pass.
impl std::hash::Hash for QuadPatternNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.storage_encoding.hash(state);
        self.pattern.hash(state);
        self.projection.hash(state);
    }
}

impl QuadPatternNode {
    /// Creates a new [QuadPatternNode].
    pub fn new(
        storage_encoding: QuadStorageEncoding,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
    ) -> Self {
        let pattern = QuadPattern::new(
            active_graph,
            graph_variable,
            pattern,
            BlankNodeMatchingMode::Variable,
        );
        let schema = compute_quad_pattern_schema(&pattern, &storage_encoding);
        Self {
            storage_encoding,
            pattern,
            schema,
            projection: None,
        }
    }

    /// Creates a new [QuadPatternNode].
    ///
    /// Contrary to [Self::new], blank nodes are not treated as a variable. They are used for
    /// filtering the quad set.
    pub fn new_with_blank_nodes_as_filter(
        storage_encoding: QuadStorageEncoding,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
    ) -> Self {
        let pattern = QuadPattern::new(
            active_graph,
            graph_variable,
            pattern,
            BlankNodeMatchingMode::Filter,
        );
        let schema = compute_quad_pattern_schema(&pattern, &storage_encoding);
        Self {
            storage_encoding,
            pattern,
            schema,
            projection: None,
        }
    }

    /// Creates a new [QuadPatternNode] that returns all quads in `active_graph` using the default
    /// quads schema.
    pub fn new_all_quads(
        storage_encoding: QuadStorageEncoding,
        active_graph: ActiveGraph,
    ) -> Self {
        let pattern = QuadPattern::new(
            active_graph,
            Some(Variable::new_unchecked(COL_GRAPH)),
            TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked(COL_SUBJECT)),
                predicate: NamedNodePattern::Variable(Variable::new_unchecked(
                    COL_PREDICATE,
                )),
                object: TermPattern::Variable(Variable::new_unchecked(COL_OBJECT)),
            },
            BlankNodeMatchingMode::Filter, // Doesn't matter here
        );
        let schema = storage_encoding.quad_schema();
        Self {
            storage_encoding,
            pattern,
            schema,
            projection: None,
        }
    }

    /// Returns a new [QuadPatternNode] with the given projection.
    pub fn with_projection(&self, projection: Vec<usize>) -> DFResult<Self> {
        let mut fields = Vec::new();
        let arrow_schema = self.schema.as_arrow();
        for &i in &projection {
            fields.push(arrow_schema.field(i).clone());
        }
        let fields = Fields::from(fields);
        let projected_schema = DFSchema::from_unqualified_fields(fields, HashMap::new())?;

        Ok(Self {
            storage_encoding: self.storage_encoding.clone(),
            pattern: self.pattern.clone(),
            schema: Arc::new(projected_schema),
            projection: Some(projection),
        })
    }

    /// Returns a new [QuadPatternNode] in which the variables of the triple pattern are replaced by
    /// their terms in `bindings`. Bound variables are removed from the schema.
    ///
    /// Bound blank nodes match exactly their label (see [`BlankNodeMatchingMode::Filter`]). Hence,
    /// they are only bound if the pattern does not use blank nodes as variables. Only IRIs are
    /// bound in the predicate position.
    ///
    /// Returns `None` if no variable is bound or if all projected variables are bound.
    pub fn bind_variables(
        &self,
        bindings: &HashMap<String, Term>,
    ) -> DFResult<Option<Self>> {
        let triple = &self.pattern.triple_pattern;
        let has_blank_node_variables = self.pattern.blank_node_mode
            == BlankNodeMatchingMode::Variable
            && [&triple.subject, &triple.object]
                .iter()
                .any(|term| matches!(term, TermPattern::BlankNode(_)));

        let bind = |pattern: &TermPattern| match pattern {
            TermPattern::Variable(variable) => match bindings.get(variable.as_str()) {
                Some(Term::NamedNode(node)) => Some(TermPattern::NamedNode(node.clone())),
                Some(Term::BlankNode(node)) if !has_blank_node_variables => {
                    Some(TermPattern::BlankNode(node.clone()))
                }
                Some(Term::Literal(literal)) => {
                    Some(TermPattern::Literal(literal.clone()))
                }
                _ => None,
            },
            _ => None,
        };
        let bind_predicate = |pattern: &NamedNodePattern| match pattern {
            NamedNodePattern::Variable(variable) => match bindings.get(variable.as_str())
            {
                Some(Term::NamedNode(node)) => {
                    Some(NamedNodePattern::NamedNode(node.clone()))
                }
                _ => None,
            },
            NamedNodePattern::NamedNode(_) => None,
        };

        let subject = bind(&triple.subject);
        let predicate = bind_predicate(&triple.predicate);
        let object = bind(&triple.object);
        if subject.is_none() && predicate.is_none() && object.is_none() {
            return Ok(None);
        }

        let pattern = QuadPattern {
            triple_pattern: TriplePattern {
                subject: subject.unwrap_or_else(|| triple.subject.clone()),
                predicate: predicate.unwrap_or_else(|| triple.predicate.clone()),
                object: object.unwrap_or_else(|| triple.object.clone()),
            },
            blank_node_mode: match has_blank_node_variables {
                true => BlankNodeMatchingMode::Variable,
                false => BlankNodeMatchingMode::Filter,
            },
            ..self.pattern.clone()
        };
        let unprojected = Self {
            storage_encoding: self.storage_encoding.clone(),
            schema: compute_quad_pattern_schema(&pattern, &self.storage_encoding),
            pattern,
            projection: None,
        };

        // Retain the projected variables that are still unbound (in their order).
        let projection = self
            .schema
            .fields()
            .iter()
            .filter_map(|field| {
                unprojected
                    .schema
                    .index_of_column_by_name(None, field.name())
            })
            .collect::<Vec<_>>();
        if projection.is_empty() {
            return Ok(None);
        }
        unprojected.with_projection(projection).map(Some)
    }

    /// The storage encoding of the [QuadPatternNode].
    pub fn storage_encoding(&self) -> &QuadStorageEncoding {
        &self.storage_encoding
    }

    /// The quad pattern.
    pub fn quad_pattern(&self) -> &QuadPattern {
        &self.pattern
    }
}

impl fmt::Debug for QuadPatternNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for QuadPatternNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for QuadPatternNode {
    fn name(&self) -> &str {
        "QuadPattern"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "QuadPattern: ")?;

        if let Some(graph_variable) = &self.pattern.graph_variable {
            write!(f, "graph_variable={graph_variable} ")?;
        }
        write!(f, "triple_pattern=[{}]", self.pattern.triple_pattern)?;

        if self.pattern.active_graph != ActiveGraph::DefaultGraph {
            write!(f, ", active_graph: {} ", self.pattern.active_graph)?;
        }

        if let Some(projection) = &self.projection {
            write!(f, ", projection={projection:?}")?;
        }

        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        if !inputs.is_empty() {
            return plan_err!("QuadPatternNode has no inputs, got {}.", inputs.len());
        }

        if !exprs.is_empty() {
            return plan_err!("QuadPatternNode has no expressions, got {}.", exprs.len());
        }

        Ok(Self {
            storage_encoding: self.storage_encoding.clone(),
            pattern: self.pattern.clone(),
            schema: Arc::clone(&self.schema),
            projection: self.projection.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdf_fusion_common::{BlankNode, Literal, NamedNode};

    /// The pattern `?s ?p ?o`.
    fn spo() -> QuadPatternNode {
        QuadPatternNode::new(
            QuadStorageEncoding::String,
            ActiveGraph::DefaultGraph,
            None,
            TriplePattern {
                subject: Variable::new_unchecked("s").into(),
                predicate: Variable::new_unchecked("p").into(),
                object: Variable::new_unchecked("o").into(),
            },
        )
    }

    fn iri(iri: &str) -> Term {
        NamedNode::new_unchecked(iri).into()
    }

    /// Binds the variables in `node` and returns the bound triple pattern and its schema.
    fn bind<'a>(
        node: &QuadPatternNode,
        bindings: impl IntoIterator<Item = (&'a str, Term)>,
    ) -> Option<(String, Vec<String>)> {
        let bindings = bindings
            .into_iter()
            .map(|(name, term)| (name.to_owned(), term))
            .collect();
        let bound = node.bind_variables(&bindings).unwrap()?;
        let fields = bound.schema().fields().iter().map(|f| f.name().clone());
        Some((
            bound.quad_pattern().triple_pattern.to_string(),
            fields.collect(),
        ))
    }

    #[test]
    fn test_bind_variables() {
        let bound = bind(
            &spo(),
            [
                ("p", iri("http://example.com/p")),
                ("o", Literal::new_simple_literal("o").into()),
            ],
        );
        let expected = ("?s <http://example.com/p> \"o\"", vec!["s"]);
        assert_eq!(bound, Some((expected.0.into(), to_strings(&expected.1))));
    }

    #[test]
    fn test_bind_variables_retains_projection() {
        let node = spo().with_projection(vec![2, 0]).unwrap(); // [o, s]
        let bound = bind(&node, [("s", iri("http://example.com/s"))]);
        let expected = ("<http://example.com/s> ?p ?o", vec!["o"]);
        assert_eq!(bound, Some((expected.0.into(), to_strings(&expected.1))));
    }

    #[test]
    fn test_bind_variables_without_bound_variable() {
        assert_eq!(bind(&spo(), [("x", iri("http://example.com/x"))]), None);
    }

    #[test]
    fn test_bind_variables_with_all_projected_variables_bound() {
        let node = spo().with_projection(vec![0]).unwrap(); // [s]
        assert_eq!(bind(&node, [("s", iri("http://example.com/s"))]), None);
    }

    #[test]
    fn test_bind_variables_ignores_literal_predicates() {
        assert_eq!(bind(&spo(), [("p", Literal::from(1).into())]), None);
    }

    #[test]
    fn test_bind_variables_binds_blank_nodes_as_filter() {
        let blank_node = BlankNode::new_unchecked("b1");
        let bound = spo()
            .bind_variables(&HashMap::from([("s".to_owned(), blank_node.into())]))
            .unwrap()
            .unwrap();

        let pattern = bound.quad_pattern();
        assert_eq!(pattern.triple_pattern.to_string(), "_:b1 ?p ?o");
        assert_eq!(pattern.blank_node_mode, BlankNodeMatchingMode::Filter);
    }

    #[test]
    fn test_bind_variables_ignores_blank_nodes_if_blank_nodes_are_variables() {
        // The pattern `_:x ?p ?o`, in which `_:x` acts as a variable.
        let node = QuadPatternNode::new(
            QuadStorageEncoding::String,
            ActiveGraph::DefaultGraph,
            None,
            TriplePattern {
                subject: BlankNode::new_unchecked("x").into(),
                predicate: Variable::new_unchecked("p").into(),
                object: Variable::new_unchecked("o").into(),
            },
        );
        let bound = bind(&node, [("o", BlankNode::new_unchecked("b1").into())]);
        assert_eq!(bound, None);
    }

    fn to_strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| (*v).to_owned()).collect()
    }
}
