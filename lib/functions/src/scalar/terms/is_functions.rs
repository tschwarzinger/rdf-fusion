use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use itertools::repeat_n;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_family::{
    BooleanFamilyArray, DowncastTypedFamilyArray, TypedFamilyArray,
};
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, EncodingName, RdfFusionEncodings,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::{AResult, DFResult};
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// Returns true if the argument is a blank node.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - ISBLANK](https://www.w3.org/TR/sparql11-query/#func-isBlank)
pub fn is_blank_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(IsSparqlOp::new(
        IsOpType::Blank,
        encodings,
    )))
}

/// Returns true if the argument is a blank node.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - isIRI](https://www.w3.org/TR/sparql11-query/#func-isIRI)
pub fn is_iri_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(IsSparqlOp::new(
        IsOpType::Iri,
        encodings,
    )))
}

/// Returns true if the argument is a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - ISLITERAL](https://www.w3.org/TR/sparql11-query/#func-isLiteral)
pub fn is_literal_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(IsSparqlOp::new(
        IsOpType::Literal,
        encodings,
    )))
}

/// Returns true if the argument is a numeric literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - ISNUMERIC](https://www.w3.org/TR/sparql11-query/#func-isNumeric)
pub fn is_numeric_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(IsSparqlOp::new(
        IsOpType::Numeric,
        encodings,
    )))
}

/// Represents the type of SPARQL `is*` check to perform.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum IsOpType {
    Blank,
    Iri,
    Literal,
    Numeric,
}

impl IsOpType {
    fn builtin_name(&self) -> BuiltinName {
        match self {
            IsOpType::Blank => BuiltinName::IsBlank,
            IsOpType::Iri => BuiltinName::IsIri,
            IsOpType::Literal => BuiltinName::IsLiteral,
            IsOpType::Numeric => BuiltinName::IsNumeric,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct IsSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
    op_type: IsOpType,
}

impl Debug for IsSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IsSparqlOp")
            .field("op_type", &self.op_type)
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl IsSparqlOp {
    /// Create a new generic `is*` op.
    fn new(op_type: IsOpType, encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name: op_type.builtin_name().to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
            op_type,
        }
    }

    /// Dispatches the evaluation logic based on the operator type and the downcasted child array.
    fn evaluate_child(
        &self,
        downcasted: &DowncastTypedFamilyArray,
        child_len: usize,
    ) -> AResult<TypedFamilyArray> {
        let tf_encoding = self.encodings.typed_family();

        // Nulls always propagate as nulls in SPARQL functions
        if let DowncastTypedFamilyArray::Null(_) = downcasted {
            return tf_encoding.create_null_array(child_len);
        }

        let bool_array = match self.op_type {
            IsOpType::Blank => {
                if let DowncastTypedFamilyArray::Resource(res) = downcasted {
                    res.is_blank_node()
                } else {
                    BooleanArray::from_iter(repeat_n(false, child_len))
                }
            }
            IsOpType::Iri => {
                if let DowncastTypedFamilyArray::Resource(res) = downcasted {
                    res.is_named_node() // Assuming this exists on your Resource array
                } else {
                    BooleanArray::from_iter(repeat_n(false, child_len))
                }
            }
            IsOpType::Literal => {
                // Everything that isn't a Resource (and isn't explicitly null) is a literal
                if let DowncastTypedFamilyArray::Resource(_) = downcasted {
                    BooleanArray::from_iter(repeat_n(false, child_len))
                } else {
                    BooleanArray::from_iter(repeat_n(true, child_len))
                }
            }
            IsOpType::Numeric => {
                if let DowncastTypedFamilyArray::Numeric(_) = downcasted {
                    BooleanArray::from_iter(repeat_n(true, child_len))
                } else {
                    BooleanArray::from_iter(repeat_n(false, child_len))
                }
            }
        };

        tf_encoding.create_array_from_family(BooleanFamilyArray::new(bool_array))
    }
}

impl ScalarUDFImpl for IsSparqlOp {
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
        match detect_encoding_from_types(&self.encodings, arg_types)? {
            Some(EncodingName::TypedFamily) => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            _ => exec_err!("{} only supports the TypedFamily encoding", self.name),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("{} expects a single argument.", self.name);
        }

        let args_wrapped =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let _tf_encoding = self.encodings.typed_family();

        match args_wrapped.downcast_arrays() {
            Some(DowncastEncodingArrays::TypedFamily(array)) => {
                let result = array.map_children_tf_unary(|child| {
                    let child_len = child.array().len();
                    self.evaluate_child(&child.downcast(), child_len)
                })?;

                Ok(ColumnarValue::Array(result.into_array_ref()))
            }
            _ => exec_err!("{} was called with an unsupported encoding.", self.name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_default_encodings, create_standard_test_vector, evaluate_function_for_test,
    };
    use insta::assert_snapshot;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_is_blank_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(is_blank_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------+
        | input                                                                                        | isBLANK(?table?.input)     |
        +----------------------------------------------------------------------------------------------+----------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}         |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.boolean=false} |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.boolean=false} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.boolean=false} |
        +----------------------------------------------------------------------------------------------+----------------------------+
        ");
    }

    #[tokio::test]
    async fn test_is_iri_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(is_iri_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------+
        | input                                                                                        | isIRI(?table?.input)       |
        +----------------------------------------------------------------------------------------------+----------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}         |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.boolean=false} |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.boolean=false} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.boolean=false} |
        +----------------------------------------------------------------------------------------------+----------------------------+
        ");
    }

    #[tokio::test]
    async fn test_is_literal_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(is_literal_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------+
        | input                                                                                        | isLITERAL(?table?.input)   |
        +----------------------------------------------------------------------------------------------+----------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}         |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.boolean=false} |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.boolean=false} |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.boolean=true}  |
        +----------------------------------------------------------------------------------------------+----------------------------+
        ");
    }

    #[tokio::test]
    async fn test_is_numeric_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(is_numeric_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------+
        | input                                                                                        | isNUMERIC(?table?.input)   |
        +----------------------------------------------------------------------------------------------+----------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}         |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.boolean=false} |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.boolean=false} |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.boolean=false} |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.boolean=false} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.boolean=false} |
        +----------------------------------------------------------------------------------------------+----------------------------+
        ");
    }
}
