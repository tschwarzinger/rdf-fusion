use crate::active_graph::ActiveGraph;
use crate::quad_pattern::QuadPattern;
use datafusion::arrow::datatypes::Fields;
use datafusion::common::{DFSchema, DFSchemaRef, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_common::{NamedNodePattern, TermPattern, TriplePattern, Variable};
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
#[derive(PartialEq, Eq, Hash)]
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
        let schema = pattern.compute_schema(&storage_encoding);
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
        let schema = pattern.compute_schema(&storage_encoding);
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
        write!(f, "triple_pattern=[{}]", &self.pattern.triple_pattern)?;

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
