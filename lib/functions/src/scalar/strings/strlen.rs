use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::{DowncastTypedFamilyArray, NumericFamilyArray};
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// Returns the length of a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STRLEN](https://www.w3.org/TR/sparql11-query/#func-strlen)
pub fn strlen_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StrLenSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct StrLenSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for StrLenSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrLenSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl StrLenSparqlOp {
    /// Create a new [`StrLenSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::StrLen.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StrLenSparqlOp {
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
            _ => exec_err!("Unsupported encoding for STRLEN return type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => tf_args
                .map_children_tf_unary(|child| match child.as_downcast_array() {
                    DowncastTypedFamilyArray::String(array) => {
                        let values = array.value_array();
                        let lengths = Int64Array::from_iter(values.iter().map(|val| {
                            val.map(|val| val.chars().count() as i64).unwrap_or(0)
                        }));
                        let numeric_array = NumericFamilyArray::new_integers(lengths);
                        tf_encoding.create_array_from_family(numeric_array)
                    }
                    _ => tf_encoding.create_null_array(child.to_array().len()),
                })?
                .into_array_ref(),
            _ => exec_err!(
                "STRLEN is only supported for TypedFamily and PlainTerm encoding"
            )?,
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
    use datafusion::arrow::array::StringArray;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::typed_family::StringFamilyArray;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_strlen_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(strlen_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+-----------------------------------+
        | input                                                                                        | STRLEN(?table?.input)             |
        +----------------------------------------------------------------------------------------------+-----------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.numeric={integer=2}}  |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.numeric={integer=13}} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.numeric={integer=5}}  |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.numeric={integer=3}}  |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                |
        +----------------------------------------------------------------------------------------------+-----------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_strlen_multibyte() {
        let encodings = create_default_encodings();
        let tf_encoding = encodings.typed_family();
        let strings_array =
            StringFamilyArray::new_simple(StringArray::from(vec!["食べ物", "👪", "👨‍👩‍👧‍👦"]));
        let input = tf_encoding
            .create_array_from_family(strings_array)
            .unwrap()
            .into_array_ref();

        let udf = Arc::new(strlen_udf(encodings).unwrap());
        let result = evaluate_function_for_test(input, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r#"
        +--------------------------------------------------+----------------------------------+
        | input                                            | STRLEN(?table?.input)            |
        +--------------------------------------------------+----------------------------------+
        | {rdf-fusion.strings={value: 食べ物, language: }} | {rdf-fusion.numeric={integer=3}} |
        | {rdf-fusion.strings={value: 👪, language: }}     | {rdf-fusion.numeric={integer=1}} |
        | {rdf-fusion.strings={value: 👨‍👩‍👧‍👦, language: }}     | {rdf-fusion.numeric={integer=7}} |
        +--------------------------------------------------+----------------------------------+
        "#
        );
    }
}
