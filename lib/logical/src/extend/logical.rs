use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::{Column, DFSchema, DFSchemaRef, plan_err};
use datafusion::logical_expr::{
    Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::Variable;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// A logical node that represents the SPARQL `Extend` operator.
///
/// This node is used to add a new variable to the solution sequence, bound to the
/// result of evaluating an expression.
#[derive(PartialEq, Eq, Hash)]
pub struct ExtendNode {
    /// The input plan.
    inner: LogicalPlan,
    /// The variable to bind.
    variable: Variable,
    /// The expression to evaluate.
    expression: Expr,
    /// The schema of the output.
    schema: DFSchemaRef,
}

impl ExtendNode {
    /// Creates a new [ExtendNode].
    pub fn try_new(
        inner: LogicalPlan,
        variable: Variable,
        expression: Expr,
    ) -> DFResult<Self> {
        let column = Column::new_unqualified(variable.as_str());
        if inner.schema().has_column(&column) {
            return plan_err!("Variable {} already exists in schema.", variable);
        }

        let schema = compute_schema(&inner.clone(), &variable, &expression)?;
        Ok(Self {
            inner,
            variable,
            expression,
            schema,
        })
    }

    /// Returns the input plan.
    pub fn inner(&self) -> &LogicalPlan {
        &self.inner
    }

    /// Returns the variable to bind.
    pub fn variable(&self) -> &Variable {
        &self.variable
    }

    /// Returns the expression to evaluate.
    pub fn expression(&self) -> &Expr {
        &self.expression
    }
}

impl fmt::Debug for ExtendNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for ExtendNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for ExtendNode {
    fn name(&self) -> &str {
        "Extend"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.inner()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.expression.clone()]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Extend: {} {}", &self.variable, &self.expression)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if inputs.len() != 1 {
            return plan_err!(
                "ExtendNode must have exactly one input, got {}.",
                inputs.len()
            );
        }

        if exprs.len() != 1 {
            return plan_err!("ExtendNode must have exactly one expression.");
        }

        Self::try_new(inputs[0].clone(), self.variable.clone(), exprs[0].clone())
    }
}

fn compute_schema(
    inner: &LogicalPlan,
    variable: &Variable,
    expression: &Expr,
) -> DFResult<DFSchemaRef> {
    let column = Column::new_unqualified(variable.as_str());
    let field = expression.to_field(inner.schema())?.1;

    let mut fields = inner
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new(
        column.name,
        field.data_type().clone(),
        field.is_nullable(),
    ));

    let fields = fields.into_iter().collect::<Fields>();
    let schema = DFSchema::from_unqualified_fields(fields, HashMap::new())?;
    Ok(Arc::new(schema))
}
