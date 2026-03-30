use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, StringFamily, TypedFamily,
};
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, EncodingName, RdfFusionEncodings,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Returns the URI-encoded value of a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - ENCODE_FOR_URI](https://www.w3.org/TR/sparql11-query/#func-encodeForURI)
pub fn encode_for_uri_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(EncodeForUriSparqlOp::new(
        encodings,
    )))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct EncodeForUriSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for EncodeForUriSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodeForUriSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl EncodeForUriSparqlOp {
    /// Create a new [`EncodeForUriSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::EncodeForUri.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for EncodeForUriSparqlOp {
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
        let encoding_name = detect_encoding_from_types(&self.encodings, arg_types)?;

        match encoding_name {
            Some(EncodingName::TypedFamily) => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            Some(EncodingName::PlainTerm) => {
                Ok(self.encodings.plain_term().data_type().clone())
            }
            _ => exec_err!("Unsupported encoding for ENCODE_FOR_URI return type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArrays::TypedFamily(tf_args)) => tf_args
                .map_children_tf_unary(|child| match child.downcast() {
                    DowncastTypedFamilyArray::String(array) => {
                        let values = array.value_array();
                        let encoded = (0..values.len())
                            .map(|i| encode_for_uri_impl(values.value(i)));

                        let encoded_values =
                            Arc::new(StringArray::from_iter_values(encoded));
                        let family_array =
                            StringFamily::create_simple_strings_array(encoded_values);

                        tf_encoding.create_array_with_single_family(
                            StringFamily::FAMILY_ID,
                            family_array,
                        )
                    }
                    _ => tf_encoding.create_null_array(child.array().len()),
                })?
                .into_array_ref(),
            _ => exec_err!("ENCODE_FOR_URI is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
    }
}

/// Implements the URI encoding
fn encode_for_uri_impl(string: &str) -> String {
    let mut result = Vec::with_capacity(string.len());
    for c in string.bytes() {
        match c {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(c)
            }
            _ => {
                result.push(b'%');
                let high = c / 16;
                let low = c % 16;
                result.push(if high < 10 {
                    b'0' + high
                } else {
                    b'A' + (high - 10)
                });
                result.push(if low < 10 {
                    b'0' + low
                } else {
                    b'A' + (low - 10)
                });
            }
        }
    }
    String::from_utf8(result).unwrap()
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
    async fn test_encode_for_uri_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(encode_for_uri_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------------+
        | input                                                                                        | ENCODE_FOR_URI(?table?.input)                               |
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                          |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                          |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                          |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                          |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                                          |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                                          |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                                          |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                                          |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                                          |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                                          |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: just%20a%20string, language: }} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: }}             |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 123, language: }}               |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                          |
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------------+
        "
        );
    }
}
