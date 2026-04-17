use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use crate::scalar::ScalarSparqlFunctionArgs;
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_compute::numeric::cast_numeric;
use rdf_fusion_encoding::typed_family::{DowncastTypedFamilyArray, NumericFamilyArray};
use rdf_fusion_encoding::{
    detect_encoding_from_types, DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings,
    TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::{DFResult, Decimal};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Implementation of the SPARQL `xsd:int()` cast function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - CAST](https://www.w3.org/TR/sparql11-query/#func-cast)
pub fn cast_int_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CastNumericSparqlUdf::new(
        encodings,
        BuiltinName::CastInt.to_string(),
        DataType::Int32,
    )))
}

/// Implementation of the SPARQL `xsd:integer()` cast function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - CAST](https://www.w3.org/TR/sparql11-query/#func-cast)
pub fn cast_integer_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CastNumericSparqlUdf::new(
        encodings,
        BuiltinName::CastInteger.to_string(),
        DataType::Int64,
    )))
}

/// Implementation of the SPARQL `xsd:float()` cast function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - CAST](https://www.w3.org/TR/sparql11-query/#func-cast)
pub fn cast_float_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CastNumericSparqlUdf::new(
        encodings,
        BuiltinName::CastFloat.to_string(),
        DataType::Float32,
    )))
}

/// Implementation of the SPARQL `xsd:double()` cast function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - CAST](https://www.w3.org/TR/sparql11-query/#func-cast)
pub fn cast_double_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CastNumericSparqlUdf::new(
        encodings,
        BuiltinName::CastDouble.to_string(),
        DataType::Float64,
    )))
}

/// Implementation of the SPARQL `xsd:decimal()` cast function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - CAST](https://www.w3.org/TR/sparql11-query/#func-cast)
pub fn cast_decimal_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CastNumericSparqlUdf::new(
        encodings,
        BuiltinName::CastDecimal.to_string(),
        DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
    )))
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CastNumericSparqlUdf {
    encodings: RdfFusionEncodings,
    name: String,
    target_type: DataType,
    signature: Signature,
}

impl Debug for CastNumericSparqlUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CastNumericSparqlUdf")
            .field("name", &self.name)
            .field("target_type", &self.target_type)
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl CastNumericSparqlUdf {
    /// Create a new [`CastNumericSparqlUdf`].
    pub fn new(
        encodings: RdfFusionEncodings,
        name: String,
        target_type: DataType,
    ) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name,
            target_type,
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CastNumericSparqlUdf {
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
            _ => exec_err!("Numeric cast is only supported for TypedFamily encoding"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => {
                let encoding = self.encodings.typed_family();
                tf_args
                    .map_children_tf_unary(|child| match child.as_downcast_array() {
                        DowncastTypedFamilyArray::Numeric(array) => {
                            let cast = cast_numeric(&array, &self.target_type)?;
                            encoding.create_array_from_family(
                                NumericFamilyArray::try_from_primitive(cast)?,
                            )
                        }
                        DowncastTypedFamilyArray::Boolean(array) => {
                            let cast = if matches!(self.target_type, DataType::Decimal128(_, _))
                            {
                                let mut builder = datafusion::arrow::array::Decimal128Builder::with_capacity(
                                    array.inner().len(),
                                )
                                    .with_precision_and_scale(
                                        Decimal::PRECISION,
                                        Decimal::SCALE,
                                    )?;

                                for i in 0..array.inner().len() {
                                    if array.inner().is_null(i) {
                                        builder.append_null();
                                    } else if array.inner().value(i) {
                                        builder.append_value(
                                            10_i128.pow(Decimal::SCALE as u32),
                                        );
                                    } else {
                                        builder.append_value(0);
                                    }
                                }

                                Arc::new(builder.finish()) as ArrayRef
                            } else {
                                cast(array.inner(), &self.target_type)?
                            };
                            encoding.create_array_from_family(
                                NumericFamilyArray::try_from_primitive(cast)?,
                            )
                        }
                        DowncastTypedFamilyArray::String(array) => {
                            let cast = array.cast(&self.target_type)?;
                            encoding.create_array_from_family(
                                NumericFamilyArray::try_from_primitive(cast)?,
                            )
                        }
                        _ => encoding.create_null_array(child.to_array().len()),
                    })?
                    .into_array_ref()
            }
            _ => exec_err!("Numeric cast is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
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
    async fn test_cast_int_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(cast_int_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+--------------------------------+
        | input                                                                                        | xsd:int(?table?.input)         |
        +----------------------------------------------------------------------------------------------+--------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}             |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}             |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}             |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}             |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={int=10}}  |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={int=10}}  |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={int=0}}   |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={int=20}}  |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={int=30}}  |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={int=40}}  |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}             |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}             |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}             |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.numeric={int=123}} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}             |
        +----------------------------------------------------------------------------------------------+--------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_cast_integer_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(cast_integer_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+------------------------------------+
        | input                                                                                        | xsd:integer(?table?.input)         |
        +----------------------------------------------------------------------------------------------+------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                 |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                 |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                 |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                 |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}  |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={integer=10}}  |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={integer=0}}   |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={integer=20}}  |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={integer=30}}  |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={integer=40}}  |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}                 |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                 |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                 |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.numeric={integer=123}} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                 |
        +----------------------------------------------------------------------------------------------+------------------------------------+
        ");
    }
    #[tokio::test]
    async fn test_cast_float_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(cast_float_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+------------------------------------+
        | input                                                                                        | xsd:float(?table?.input)           |
        +----------------------------------------------------------------------------------------------+------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                 |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                 |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                 |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                 |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={float=10.0}}  |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=10.0}}  |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={float=0.0}}   |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={float=20.0}}  |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={float=30.0}}  |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={float=40.0}}  |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}                 |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                 |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                 |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.numeric={float=123.0}} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                 |
        +----------------------------------------------------------------------------------------------+------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_cast_double_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(cast_double_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+-------------------------------------+
        | input                                                                                        | xsd:double(?table?.input)           |
        +----------------------------------------------------------------------------------------------+-------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                  |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                  |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                  |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                  |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={double=10.0}}  |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={double=10.0}}  |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={double=0.0}}   |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={double=20.0}}  |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={double=30.0}}  |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={double=40.0}}  |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}                  |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                  |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                  |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.numeric={double=123.0}} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                  |
        +----------------------------------------------------------------------------------------------+-------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_cast_decimal_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(cast_decimal_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------+
        | input                                                                                        | xsd:decimal(?table?.input)                            |
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                    |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                    |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                    |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                    |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={decimal=10.000000000000000000}}  |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={decimal=10.000000000000000000}}  |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={decimal=0.000000000000000000}}   |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={decimal=20.000000000000000000}}  |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={decimal=30.000000000000000000}}  |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={decimal=40.000000000000000000}}  |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}                                    |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                                    |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                                    |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.numeric={decimal=123.000000000000000000}} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                    |
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------+
        ");
    }
}
