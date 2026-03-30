use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::DowncastTypedFamilyArray;
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, EncodingName, RdfFusionEncodings,
    TermEncoding, detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum StringSplitOperation {
    After,
    Before,
}

/// Returns the part of the first literal that follows the first occurrence of the second literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STRAFTER](https://www.w3.org/TR/sparql11-query/#func-strafter)
pub fn str_after_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StringSplitSparqlOp::new(
        encodings,
        StringSplitOperation::After,
    )))
}

/// Returns the part of the first literal that precedes the first occurrence of the second literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STRBEFORE](https://www.w3.org/TR/sparql11-query/#func-strbefore)
pub fn str_before_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StringSplitSparqlOp::new(
        encodings,
        StringSplitOperation::Before,
    )))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct StringSplitSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
    op: StringSplitOperation,
}

impl Debug for StringSplitSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringSplitSparqlOp")
            .field("encodings", &self.encodings)
            .field("op", &self.op)
            .finish()
    }
}

impl StringSplitSparqlOp {
    /// Create a new [`StringSplitSparqlOp`].
    fn new(encodings: RdfFusionEncodings, op: StringSplitOperation) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_binary_arity()
            .build();

        let name = match op {
            StringSplitOperation::After => BuiltinName::StrAfter.to_string(),
            StringSplitOperation::Before => BuiltinName::StrBefore.to_string(),
        };

        Self {
            encodings,
            name,
            signature: Signature::new(type_signature, Volatility::Immutable),
            op,
        }
    }
}

impl ScalarUDFImpl for StringSplitSparqlOp {
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
            StringSplitOperation::After => "STRAFTER",
            StringSplitOperation::Before => "STRBEFORE",
        };

        match detect_encoding_from_types(&self.encodings, arg_types)? {
            Some(EncodingName::TypedFamily) => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            _ => exec_err!("Unsupported encoding for {} return type", op_name),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args_wrapped =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();
        let op = self.op;

        let result = match args_wrapped.downcast_arrays() {
            Some(DowncastEncodingArrays::TypedFamily(tf_args)) => tf_args
                .map_children_tf_binary(|lhs, rhs| {
                    match (lhs.downcast(), rhs.downcast()) {
                        (
                            DowncastTypedFamilyArray::String(l),
                            DowncastTypedFamilyArray::String(r),
                        ) => {
                            let res = match op {
                                StringSplitOperation::After => {
                                    l.apply_binary_string_full(
                                        &r,
                                        |a, a_lang, b, b_lang| {
                                            if b_lang.is_none() || a_lang == b_lang {
                                                if let Some(pos) = a.find(b) {
                                                    Some((
                                                        a[pos + b.len()..].to_string(),
                                                        a_lang.map(|s| s.to_string()),
                                                    ))
                                                } else {
                                                    // If not found, must return a simple literal empty string
                                                    Some(("".to_string(), None))
                                                }
                                            } else {
                                                None
                                            }
                                        },
                                    )
                                }
                                StringSplitOperation::Before => {
                                    l.apply_binary_string_full(
                                        &r,
                                        |a, a_lang, b, b_lang| {
                                            if b_lang.is_none() || a_lang == b_lang {
                                                if let Some(pos) = a.find(b) {
                                                    Some((
                                                        a[..pos].to_string(),
                                                        a_lang.map(|s| s.to_string()),
                                                    ))
                                                } else {
                                                    // If not found, must return a simple literal empty string
                                                    Some(("".to_string(), None))
                                                }
                                            } else {
                                                None
                                            }
                                        },
                                    )
                                }
                            };
                            tf_encoding.create_array_from_family(res)
                        }
                        _ => tf_encoding.create_null_array(lhs.array().len()),
                    }
                })?
                .into_array_ref(),
            _ => {
                let op_name = match self.op {
                    StringSplitOperation::After => "STRAFTER",
                    StringSplitOperation::Before => "STRBEFORE",
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
    async fn test_str_after_typed_family() {
        let encodings = create_default_encodings();

        let left = create_typed_family_strings_array(
            &encodings,
            vec!["foobar", "foobar", "foobar", "foobar", "foobar"],
            vec![None, Some("en"), Some("en"), Some("de"), None],
        );

        let right = create_typed_family_strings_array(
            &encodings,
            vec!["oo", "oo", "oo", "oo", "oo"],
            vec![None, None, Some("en"), Some("en"), Some("en")],
        );

        let udf = Arc::new(str_after_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------+------------------------------------------------+-------------------------------------------------+
        | left                                               | right                                          | STRAFTER(?table?.left,?table?.right)            |
        +----------------------------------------------------+------------------------------------------------+-------------------------------------------------+
        | {rdf-fusion.strings={value: foobar, language: }}   | {rdf-fusion.strings={value: oo, language: }}   | {rdf-fusion.strings={value: bar, language: }}   |
        | {rdf-fusion.strings={value: foobar, language: en}} | {rdf-fusion.strings={value: oo, language: }}   | {rdf-fusion.strings={value: bar, language: en}} |
        | {rdf-fusion.strings={value: foobar, language: en}} | {rdf-fusion.strings={value: oo, language: en}} | {rdf-fusion.strings={value: bar, language: en}} |
        | {rdf-fusion.strings={value: foobar, language: de}} | {rdf-fusion.strings={value: oo, language: en}} | {rdf-fusion.null=}                              |
        | {rdf-fusion.strings={value: foobar, language: }}   | {rdf-fusion.strings={value: oo, language: en}} | {rdf-fusion.null=}                              |
        +----------------------------------------------------+------------------------------------------------+-------------------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_str_before_typed_family() {
        let encodings = create_default_encodings();

        let left = create_typed_family_strings_array(
            &encodings,
            vec!["foobar", "foobar", "foobar", "foobar", "foobar"],
            vec![None, Some("en"), Some("en"), Some("de"), None],
        );

        let right = create_typed_family_strings_array(
            &encodings,
            vec!["oo", "oo", "oo", "oo", "oo"],
            vec![None, None, Some("en"), Some("en"), Some("en")],
        );

        let udf = Arc::new(str_before_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------+------------------------------------------------+-----------------------------------------------+
        | left                                               | right                                          | STRBEFORE(?table?.left,?table?.right)         |
        +----------------------------------------------------+------------------------------------------------+-----------------------------------------------+
        | {rdf-fusion.strings={value: foobar, language: }}   | {rdf-fusion.strings={value: oo, language: }}   | {rdf-fusion.strings={value: f, language: }}   |
        | {rdf-fusion.strings={value: foobar, language: en}} | {rdf-fusion.strings={value: oo, language: }}   | {rdf-fusion.strings={value: f, language: en}} |
        | {rdf-fusion.strings={value: foobar, language: en}} | {rdf-fusion.strings={value: oo, language: en}} | {rdf-fusion.strings={value: f, language: en}} |
        | {rdf-fusion.strings={value: foobar, language: de}} | {rdf-fusion.strings={value: oo, language: en}} | {rdf-fusion.null=}                            |
        | {rdf-fusion.strings={value: foobar, language: }}   | {rdf-fusion.strings={value: oo, language: en}} | {rdf-fusion.null=}                            |
        +----------------------------------------------------+------------------------------------------------+-----------------------------------------------+
        "
        );
    }
}
