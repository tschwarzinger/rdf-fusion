use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlUDFTypeSignatureBuilder;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::FieldRef;
use datafusion::common::internal_err;
use datafusion::logical_expr::ReturnFieldArgs;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use itertools::repeat_n;
use rdf_fusion_common::DFResult;
use rdf_fusion_common::vocab::xsd;
use rdf_fusion_encoding::EncodingName;
use rdf_fusion_encoding::plain_term::PlainTermArray;
use rdf_fusion_encoding::rdf_term_field_ref;
use rdf_fusion_encoding::typed_family::StringFamilyArray;
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use std::sync::Arc;

/// Implementation of the SPARQL `STR` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STR](https://www.w3.org/TR/sparql11-query/#func-str)
pub fn str_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StrSparqlUDF::new(encodings)))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StrSparqlUDF {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl StrSparqlUDF {
    /// Create a new [`StrSparqlUDF`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlUDFTypeSignatureBuilder::new()
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

impl ScalarUDFImpl for StrSparqlUDF {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        let arg_fields = args.arg_fields;
        if arg_fields.is_empty() {
            return Ok(rdf_term_field_ref(
                self.name(),
                self.encodings.typed_family().as_ref(),
                true,
            ));
        }
        Ok(Arc::new(
            arg_fields[0].as_ref().clone().with_name(self.name()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let input = args.args[0].to_array(args.number_rows)?;
        let encoding = self.encodings.try_get_encoding_name(&args.arg_fields[0]);

        if encoding == Some(EncodingName::PlainTerm) {
            let array = self.encodings.plain_term().try_new_array(input)?;
            let result = impl_str_plain_term(&array);
            Ok(ColumnarValue::Array(result.into_array_ref()))
        } else if encoding == Some(EncodingName::TypedFamily) {
            let tf_encoding = self.encodings.typed_family();
            let tf_array = tf_encoding.try_new_array(input)?;
            let printed = tf_array.pretty_print()?;
            let result = tf_encoding
                .create_array_from_family(StringFamilyArray::new_simple(printed))?;
            Ok(ColumnarValue::Array(result.into_array_ref()))
        } else {
            internal_err!("Unsupported encoding for STR: {:?}", encoding)
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
        | {rdf-fusion.iri=http://example.com/test}                                                     | {rdf-fusion.strings={value: http://example.com/test, language: }} |
        | {rdf-fusion.blank-node=my-blank-node}                                                        | {rdf-fusion.strings={value: my-blank-node, language: }}           |
        | {rdf-fusion.blank-node=123456}                                                               | {rdf-fusion.strings={value: 123456, language: }}                  |
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
