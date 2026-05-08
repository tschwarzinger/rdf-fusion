use crate::ActiveGraph;
use crate::paths::PATH_TABLE_DFSCHEMA;
use crate::patterns::compute_schema_for_pattern;
use datafusion::common::{DFSchemaRef, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_common::{PropertyPathExpression, TermPattern, Variable};
use std::cmp::Ordering;
use std::fmt;

#[derive(PartialEq, Eq, Hash)]
pub struct PropertyPathNode {
    active_graph: ActiveGraph,
    graph_name_var: Option<Variable>,
    subject: TermPattern,
    path: PropertyPathExpression,
    object: TermPattern,
    schema: DFSchemaRef,
}

impl PropertyPathNode {
    pub fn new(
        active_graph: ActiveGraph,
        graph_name_var: Option<Variable>,
        subject: TermPattern,
        path: PropertyPathExpression,
        object: TermPattern,
    ) -> Self {
        let schema = compute_schema(graph_name_var.as_ref(), &subject, &object);
        Self {
            active_graph,
            graph_name_var,
            subject,
            path,
            object,
            schema,
        }
    }

    pub fn active_graph(&self) -> &ActiveGraph {
        &self.active_graph
    }

    pub fn graph_name_var(&self) -> Option<&Variable> {
        self.graph_name_var.as_ref()
    }

    pub fn subject(&self) -> &TermPattern {
        &self.subject
    }

    pub fn path(&self) -> &PropertyPathExpression {
        &self.path
    }

    pub fn object(&self) -> &TermPattern {
        &self.object
    }
}

impl fmt::Debug for PropertyPathNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for PropertyPathNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for PropertyPathNode {
    fn name(&self) -> &str {
        "PropertyPath"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let graph_name = self
            .graph_name_var
            .as_ref()
            .map(|v| v.to_string() + " ")
            .unwrap_or_default();
        write!(
            f,
            "PropertyPath: {}{} {} {}",
            &graph_name, &self.subject, &self.path, &self.object
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        if !inputs.is_empty() {
            return plan_err!("Expected 0 inputs but got {}", inputs.len());
        }
        if !exprs.is_empty() {
            return plan_err!("Expected 0 expressions but got {}", exprs.len());
        }
        Ok(Self::new(
            self.active_graph.clone(),
            self.graph_name_var.clone(),
            self.subject.clone(),
            self.path.clone(),
            self.object.clone(),
        ))
    }
}

fn compute_schema(
    graph: Option<&Variable>,
    subject: &TermPattern,
    object: &TermPattern,
) -> DFSchemaRef {
    let patterns = vec![
        graph.map(|v| TermPattern::Variable(v.clone())),
        Some(subject.clone()),
        Some(object.clone()),
    ];

    compute_schema_for_pattern(
        &PATH_TABLE_DFSCHEMA,
        &patterns,
        BlankNodeMatchingMode::Variable,
    )
}
