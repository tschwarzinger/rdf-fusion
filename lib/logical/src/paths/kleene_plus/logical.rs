use crate::paths::PATH_TABLE_DFSCHEMA;
use datafusion::common::{DFSchemaRef, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_common::DFResult;
use std::cmp::Ordering;
use std::fmt;

/// Represents a Kleene-plus path closure node. This node computes the Kleene plus closure
/// of the inner paths. This closure is the result of the `+` operator in SPARQL property paths.
#[derive(PartialEq, Eq, Hash)]
pub struct KleenePlusClosureNode {
    /// The inner path node.
    inner: LogicalPlan,
    /// The schema of this node.
    schema: DFSchemaRef,
    /// This setting indicates whether a single path can span multiple graphs. While usually this
    /// is allowed (as the entire RDF dataset is queries), given a GRAPH ?x { ... } pattern, each
    /// named node is evaluated individually.
    disallow_cross_graph_paths: bool,
}

impl KleenePlusClosureNode {
    /// Tries to create a new [KleenePlusClosureNode].
    ///
    /// See [KleenePlusClosureNode::disallow_cross_graph_paths] for details on
    /// `allow_cross_graph_paths`.
    ///
    /// # Errors
    ///
    /// Returns an error if `inner` does not have the expected schema.
    pub fn try_new(
        inner: LogicalPlan,
        disallow_cross_graph_paths: bool,
    ) -> DFResult<Self> {
        let matches_path_schema = inner
            .schema()
            .logically_equivalent_names_and_types(PATH_TABLE_DFSCHEMA.as_ref());
        if !matches_path_schema {
            return plan_err!(
                "Unexpected schema for inner path node. Expected: {:?} Schema: {:?}",
                PATH_TABLE_DFSCHEMA.as_ref(),
                inner.schema()
            );
        }

        Ok(Self {
            inner,
            schema: PATH_TABLE_DFSCHEMA.clone(),
            disallow_cross_graph_paths,
        })
    }

    pub fn inner(&self) -> &LogicalPlan {
        &self.inner
    }

    /// Indicates whether paths can cross graphs.
    pub fn disallow_cross_graph_paths(&self) -> bool {
        self.disallow_cross_graph_paths
    }
}

impl fmt::Debug for KleenePlusClosureNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for KleenePlusClosureNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for KleenePlusClosureNode {
    fn name(&self) -> &str {
        "KleenePlusPath"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.inner()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KleenePlusPath:",)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        if inputs.len() != 1 {
            return plan_err!("Expected 1 input but got {}", inputs.len());
        }
        if !exprs.is_empty() {
            return plan_err!("Expected 0 expressions but got {}", exprs.len());
        }
        Self::try_new(inputs[0].clone(), self.disallow_cross_graph_paths())
    }
}
