use datafusion::common::{DFSchema, DFSchemaRef, Result as DFResult, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::object_id::ObjectIdDataType;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

/// A special case of [`ChangeEncodingNode`](crate::encoding::change::ChangeEncodingNode) that
/// only supports the object id encoding as target encoding.
///
/// Quad storage implementation that support the object id encoding must be able to translate this
/// logical node to a physical execution plan. This node will be used for, for example, inserting
/// plain term quads into the database. This is also the reason why this encoding change is a
/// separate node. Otherwise, implementors of storage layer would also need to handle the conversion
/// to other encodings.  
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EncodeAsObjectIdNode {
    input: LogicalPlan,
    object_id_type: ObjectIdDataType,
    output_schema: DFSchemaRef,
}

impl EncodeAsObjectIdNode {
    /// Creates a new [`EncodeAsObjectIdNode`].
    pub fn try_new(
        input: LogicalPlan,
        object_id_type: ObjectIdDataType,
    ) -> DFResult<Self> {
        let any_unexpected_data_type = input
            .schema()
            .fields()
            .iter()
            .any(|f| f.data_type() != PLAIN_TERM_ENCODING.data_type());
        if any_unexpected_data_type {
            return plan_err!(
                "EncodeAsObjectId only supports columns with a valid encoding"
            );
        }

        let encoded_type = object_id_type.term_type();
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
            object_id_type,
            output_schema: Arc::new(df_schema),
        })
    }
}

impl UserDefinedLogicalNodeCore for EncodeAsObjectIdNode {
    fn name(&self) -> &str {
        "EncodeAsObjectId"
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
        write!(f, "EncodeAsObjectId:")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return plan_err!("EncodeAsObjectId takes a single input plan");
        }

        Self::try_new(inputs[0].clone(), self.object_id_type)
    }
}

impl PartialOrd for EncodeAsObjectIdNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input.partial_cmp(&other.input)
    }
}
