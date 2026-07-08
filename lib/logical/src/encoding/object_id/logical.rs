use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, DFSchema, DFSchemaRef, plan_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use itertools::Itertools;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::object_id::ObjectIdDataType;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use thiserror::Error;

/// A logical node that only supports the object id encoding as target encoding.
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
        let any_unexpected_data_type = input.schema().fields().iter().any(|f| {
            f.data_type() != PLAIN_TERM_ENCODING.data_type()
                && f.data_type() != STRING_ENCODING.data_type()
        });
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

/// A logical node that decodes one or more columns from object IDs to plain terms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DecodeObjectIdsNode {
    schema: DFSchemaRef,
    input: LogicalPlan,
    columns: Vec<Column>,
}

impl DecodeObjectIdsNode {
    /// Creates a new [`DecodeObjectIdsNode`].
    pub fn try_new(input: LogicalPlan, columns_to_decode: Vec<Column>) -> DFResult<Self> {
        validate_columns_to_decode(input.schema(), &columns_to_decode)?;

        let fields = input
            .schema()
            .columns()
            .into_iter()
            .enumerate()
            .map(|(idx, column)| {
                let input_field = Arc::clone(input.schema().field(idx));
                let output_field = if columns_to_decode.contains(&column) {
                    let field = input_field
                        .as_ref()
                        .clone()
                        .with_data_type(PLAIN_TERM_ENCODING.data_type().clone());
                    Arc::new(field)
                } else {
                    input_field
                };

                Ok((column.relation, output_field))
            })
            .collect::<DFResult<Vec<_>>>()?;

        Ok(Self {
            schema: Arc::new(DFSchema::new_with_metadata(
                fields,
                input.schema().metadata().clone(),
            )?),
            input,
            columns: columns_to_decode,
        })
    }

    /// Gets the columns to decode.
    pub fn columns_to_decode(&self) -> &[Column] {
        &self.columns
    }
}

impl UserDefinedLogicalNodeCore for DecodeObjectIdsNode {
    fn name(&self) -> &str {
        "DecodeObjectIds"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DecodeObjectIds: columns=[{}]",
            self.columns.iter().format(", ")
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        if inputs.len() != 1 {
            return plan_err!("DecodeObjectIds takes a single input plan");
        }

        if !exprs.is_empty() {
            return plan_err!("DecodeObjectIds takes no expressions");
        }

        let mut inputs = inputs;
        let input = inputs.pop().expect("Checked above");
        Self::try_new(input, self.columns.clone())
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

impl PartialOrd for DecodeObjectIdsNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

#[derive(Debug, Error)]
pub(crate) enum InvalidDecodingColumnError {
    #[error("The decoding column {0} does not exist in the schema.")]
    DoesNotExist(Column),
    #[error(
        "Invalid data type for decoding column '{0}'. Expected Int32, Int64, or FixedSizeBinary, but got {1}."
    )]
    InvalidDataType(Column, Box<DataType>),
}

impl From<InvalidDecodingColumnError> for DataFusionError {
    fn from(value: InvalidDecodingColumnError) -> Self {
        DataFusionError::Plan(value.to_string())
    }
}

/// Checks that all columns_to_decode exist and that they are either an int32, an int64, or a
/// fixed-size-binary type.
pub(crate) fn validate_columns_to_decode(
    schema: &DFSchema,
    columns_to_decode: &[Column],
) -> Result<(), InvalidDecodingColumnError> {
    for column in columns_to_decode {
        let field = datafusion::common::ExprSchema::field_from_column(schema, column)
            .map_err(|_| InvalidDecodingColumnError::DoesNotExist(column.clone()))?;

        match field.data_type() {
            DataType::Int32 | DataType::Int64 | DataType::FixedSizeBinary(_) => {
                // Valid Object ID types
            }
            dt => {
                return Err(InvalidDecodingColumnError::InvalidDataType(
                    column.clone(),
                    Box::new(dt.clone()),
                ));
            }
        }
    }

    Ok(())
}
