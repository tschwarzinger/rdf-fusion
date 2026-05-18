use datafusion::common::{DFSchema, DFSchemaRef, Result as DFResult, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_encoding::QuadStorageEncoding;
use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

/// Changes the encoding of all terms to the given target encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChangeEncodingNode {
    input: LogicalPlan,
    target_encoding: QuadStorageEncoding,
    output_schema: DFSchemaRef,
}

impl ChangeEncodingNode {
    /// Creates a new [`ChangeEncodingNode`].
    pub fn try_new(
        input: LogicalPlan,
        target_encoding: QuadStorageEncoding,
    ) -> DFResult<Self> {
        // TODO: Validate that the inputs are String or Plain Term encoded

        let encoded_type = target_encoding.term_type().clone();
        let df_schema = input
            .schema()
            .iter()
            .map(|(t, f)| {
                (
                    t.cloned(),
                    Arc::new(f.as_ref().clone().with_data_type(encoded_type.clone())),
                )
            })
            .collect::<Vec<_>>();
        let df_schema =
            DFSchema::new_with_metadata(df_schema, input.schema().metadata().clone())?;
        Ok(Self {
            input,
            target_encoding,
            output_schema: Arc::new(df_schema),
        })
    }

    /// Returns the target encoding of the [`ChangeEncodingNode`].
    pub fn target_encoding(&self) -> &QuadStorageEncoding {
        &self.target_encoding
    }
}

impl UserDefinedLogicalNodeCore for ChangeEncodingNode {
    fn name(&self) -> &str {
        "ChangeEncoding"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ChangeEncoding: target_encoding={}",
            self.target_encoding
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return plan_err!("ChangeEncoding takes a single input plan");
        }

        Self::try_new(inputs[0].clone(), self.target_encoding.clone())
    }
}

impl PartialOrd for ChangeEncodingNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input.partial_cmp(&other.input)
    }
}
