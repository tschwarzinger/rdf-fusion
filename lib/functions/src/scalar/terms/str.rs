use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::internal_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use itertools::repeat_n;
use rdf_fusion_common::DFResult;
use rdf_fusion_common::vocab::xsd;
use rdf_fusion_encoding::plain_term::PlainTermArray;
use rdf_fusion_encoding::typed_family::StringFamilyArray;
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;

/// Implementation of the SPARQL `STR` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STR](https://www.w3.org/TR/sparql11-query/#func-str)
pub fn str_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StrSparqlOp::new(encodings)))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StrSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl StrSparqlOp {
    /// Create a new [`StrSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.plain_term().as_ref())
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::Str.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StrSparqlOp {
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
        if arg_types.is_empty() {
            return Ok(self.encodings.typed_family().data_type().clone());
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let input = args.args[0].to_array(args.number_rows)?;
        let data_type = input.data_type();

        if data_type == self.encodings.plain_term().data_type() {
            let array = self.encodings.plain_term().try_new_array(input)?;
            let result = impl_str_plain_term(&array);
            Ok(ColumnarValue::Array(result.into_array_ref()))
        } else if data_type == self.encodings.typed_family().data_type() {
            let tf_encoding = self.encodings.typed_family();
            let tf_array = tf_encoding.try_new_array(input)?;
            let printed = tf_array.pretty_print()?;
            let result = tf_encoding
                .create_array_from_family(StringFamilyArray::new_simple(printed))?;
            Ok(ColumnarValue::Array(result.into_array_ref()))
        } else {
            internal_err!("Unsupported data type for STR: {:?}", data_type)
        }
    }
}

fn impl_str_plain_term(array: &PlainTermArray) -> PlainTermArray {
    let parts = array.as_parts();
    let len = array.len();
    let nulls = array.inner().nulls().cloned();

    let value = parts.value.clone();
    let data_types = StringArray::from_iter_values(repeat_n(xsd::STRING.as_str(), len));
    let languages = StringArray::new_null(len);

    PlainTermArray::try_new_literals(value, data_types, languages, nulls).unwrap()
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
    async fn test_str_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(str_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------------------+
        | input                                                                                        | STR(?table?.input)                                                |
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.strings={value: http://example.com/test, language: }} |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.strings={value: my-blank-node, language: }}           |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.strings={value: 123456, language: }}                  |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.strings={value: 10, language: }}                      |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.strings={value: 10, language: }}                      |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.strings={value: 0, language: }}                       |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.strings={value: 20, language: }}                      |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.strings={value: 30, language: }}                      |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.strings={value: 40, language: }}                      |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                      |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: just a string, language: }}           |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: }}                   |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 123, language: }}                     |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.strings={value: 2023-01-01T12:00:00Z, language: }}    |
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------------------+
        "
        )
    }
}
