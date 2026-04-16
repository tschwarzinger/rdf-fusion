use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use arrow_string::like::{ends_with, starts_with};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::{BooleanFamilyArray, DowncastTypedFamilyArray};
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum StringAffixOperation {
    Starts,
    Ends,
}

/// Returns true if the first literal starts with the second literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STRSTARTS](https://www.w3.org/TR/sparql11-query/#func-strstarts)
pub fn str_starts_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StringAffixSparqlOp::new(
        encodings,
        StringAffixOperation::Starts,
    )))
}

/// Returns true if the first literal ends with the second literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STRENDS](https://www.w3.org/TR/sparql11-query/#func-strends)
pub fn str_ends_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StringAffixSparqlOp::new(
        encodings,
        StringAffixOperation::Ends,
    )))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct StringAffixSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
    op: StringAffixOperation,
}

impl Debug for StringAffixSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringAffixSparqlOp")
            .field("encodings", &self.encodings)
            .field("op", &self.op)
            .finish()
    }
}

impl StringAffixSparqlOp {
    /// Create a new [`StringAffixSparqlOp`].
    fn new(encodings: RdfFusionEncodings, op: StringAffixOperation) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_binary_arity()
            .build();

        let name = match op {
            StringAffixOperation::Starts => BuiltinName::StrStarts.to_string(),
            StringAffixOperation::Ends => BuiltinName::StrEnds.to_string(),
        };

        Self {
            encodings,
            name,
            signature: Signature::new(type_signature, Volatility::Immutable),
            op,
        }
    }
}

impl ScalarUDFImpl for StringAffixSparqlOp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let op_name = match self.op {
            StringAffixOperation::Starts => "STRSTARTS",
            StringAffixOperation::Ends => "STRENDS",
        };

        match detect_encoding_from_types(&self.encodings, arg_types)? {
            Some(EncodingName::TypedFamily) => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            Some(EncodingName::PlainTerm) => {
                Ok(self.encodings.plain_term().data_type().clone())
            }
            _ => exec_err!("Unsupported encoding for {} return type", op_name),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let op = self.op;

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => tf_args
                .map_children_tf_binary(|lhs, rhs| {
                    match (lhs.as_downcast_array(), rhs.as_downcast_array()) {
                        (
                            DowncastTypedFamilyArray::String(l),
                            DowncastTypedFamilyArray::String(r),
                        ) => {
                            let res = l.apply_binary_boolean(&r, |a, b| match op {
                                StringAffixOperation::Starts => starts_with(a, b),
                                StringAffixOperation::Ends => ends_with(a, b),
                            })?;
                            self.encodings
                                .typed_family()
                                .create_array_from_family(BooleanFamilyArray::new(res))
                        }
                        _ => self
                            .encodings
                            .typed_family()
                            .create_null_array(lhs.to_array().len()),
                    }
                })?
                .into_array_ref(),
            _ => {
                let op_name = match self.op {
                    StringAffixOperation::Starts => "STRSTARTS",
                    StringAffixOperation::Ends => "STRENDS",
                };
                exec_err!("{} is only supported for TypedFamily encoding", op_name)?
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_default_encodings, create_typed_family_strings_array,
        evaluate_binary_function_for_test,
    };

    use insta::assert_snapshot;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_str_starts_typed_family() {
        let encodings = create_default_encodings();

        let left = create_typed_family_strings_array(
            &encodings,
            vec!["foobar", "foobar", "foobar", "foobar", "foobar"],
            vec![None, Some("en"), Some("en"), Some("de"), None],
        );

        let right = create_typed_family_strings_array(
            &encodings,
            vec!["foo", "foo", "foo", "foo", "foo"],
            vec![None, None, Some("en"), Some("en"), Some("en")],
        );

        let udf = Arc::new(str_starts_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------+-------------------------------------------------+---------------------------------------+
        | left                                               | right                                           | STRSTARTS(?table?.left,?table?.right) |
        +----------------------------------------------------+-------------------------------------------------+---------------------------------------+
        | {rdf-fusion.strings={value: foobar, language: }}   | {rdf-fusion.strings={value: foo, language: }}   | {rdf-fusion.boolean=true}             |
        | {rdf-fusion.strings={value: foobar, language: en}} | {rdf-fusion.strings={value: foo, language: }}   | {rdf-fusion.boolean=true}             |
        | {rdf-fusion.strings={value: foobar, language: en}} | {rdf-fusion.strings={value: foo, language: en}} | {rdf-fusion.boolean=true}             |
        | {rdf-fusion.strings={value: foobar, language: de}} | {rdf-fusion.strings={value: foo, language: en}} | {rdf-fusion.null=}                    |
        | {rdf-fusion.strings={value: foobar, language: }}   | {rdf-fusion.strings={value: foo, language: en}} | {rdf-fusion.boolean=true}             |
        +----------------------------------------------------+-------------------------------------------------+---------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_str_ends_typed_family() {
        let encodings = create_default_encodings();

        let left = create_typed_family_strings_array(
            &encodings,
            vec!["foobar", "foobar", "foobar", "foobar", "foobar"],
            vec![None, Some("en"), Some("en"), Some("de"), None],
        );

        let right = create_typed_family_strings_array(
            &encodings,
            vec!["bar", "bar", "bar", "bar", "bar"],
            vec![None, None, Some("en"), Some("en"), Some("en")],
        );

        let udf = Arc::new(str_ends_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------+-------------------------------------------------+-------------------------------------+
        | left                                               | right                                           | STRENDS(?table?.left,?table?.right) |
        +----------------------------------------------------+-------------------------------------------------+-------------------------------------+
        | {rdf-fusion.strings={value: foobar, language: }}   | {rdf-fusion.strings={value: bar, language: }}   | {rdf-fusion.boolean=true}           |
        | {rdf-fusion.strings={value: foobar, language: en}} | {rdf-fusion.strings={value: bar, language: }}   | {rdf-fusion.boolean=true}           |
        | {rdf-fusion.strings={value: foobar, language: en}} | {rdf-fusion.strings={value: bar, language: en}} | {rdf-fusion.boolean=true}           |
        | {rdf-fusion.strings={value: foobar, language: de}} | {rdf-fusion.strings={value: bar, language: en}} | {rdf-fusion.null=}                  |
        | {rdf-fusion.strings={value: foobar, language: }}   | {rdf-fusion.strings={value: bar, language: en}} | {rdf-fusion.boolean=true}           |
        +----------------------------------------------------+-------------------------------------------------+-------------------------------------+
        "
        );
    }
}
