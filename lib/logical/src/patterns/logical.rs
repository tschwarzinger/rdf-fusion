use crate::patterns::compute_schema_for_pattern;
use datafusion::common::{DFSchemaRef, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_common::TermPattern;
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use std::cmp::Ordering;
use std::fmt;

/// A logical node that represents a pattern match on a generic input.
#[derive(PartialEq, Eq, Hash)]
pub struct PatternNode {
    /// The input plan
    input: LogicalPlan,
    /// The patterns to match. Each pattern corresponds to a column in the input plan.
    /// A `None` value means that the column is not part of the output and should not partake
    /// in the matching process.
    patterns: Vec<Option<TermPattern>>,
    /// The schema of the output.
    schema: DFSchemaRef,
}

impl PatternNode {
    /// Creates a new [PatternNode].
    ///
    /// # Errors
    ///
    /// Returns an error if the length of the input schema does not match the length of the
    /// patterns.
    pub fn try_new(
        input: LogicalPlan,
        patterns: Vec<Option<TermPattern>>,
    ) -> DFResult<Self> {
        if input.schema().columns().len() != patterns.len() {
            return plan_err!("Patterns must match the number of column of inner.");
        }

        // TODO: Check type

        let schema = compute_schema_for_pattern(
            input.schema(),
            &patterns,
            BlankNodeMatchingMode::Variable,
        );
        Ok(Self {
            input,
            patterns,
            schema,
        })
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    pub fn patterns(&self) -> &[Option<TermPattern>] {
        &self.patterns
    }
}

impl fmt::Debug for PatternNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for PatternNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for PatternNode {
    fn name(&self) -> &str {
        "Pattern"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let patterns = self
            .patterns
            .iter()
            .map(|opt| {
                opt.as_ref()
                    .map_or_else(|| "-".to_owned(), ToString::to_string)
            })
            .collect::<Vec<_>>()
            .join(" ");
        write!(f, "Pattern: {patterns}",)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if inputs.len() != 1 {
            return plan_err!(
                "PatternNode must have exactly one input, got {}",
                inputs.len()
            );
        }

        if !exprs.is_empty() {
            return plan_err!("PatternNode must have no expressions");
        }

        Self::try_new(inputs[0].clone(), self.patterns.clone())
    }
}
