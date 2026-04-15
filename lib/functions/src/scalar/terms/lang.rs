use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::plain_term::PlainTermType;
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, StringFamily, TypedFamily,
};
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, EncodingName, RdfFusionEncodings,
    TermEncoding, detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Returns the language tag of a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - LANG](https://www.w3.org/TR/sparql11-query/#func-lang)
pub fn lang_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(LangSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct LangSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for LangSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LangSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl LangSparqlOp {
    /// Create a new [`LangSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_supported_encoding(encodings.plain_term().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::Lang.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LangSparqlOp {
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
            Some(EncodingName::PlainTerm) => {
                Ok(self.encodings.plain_term().data_type().clone())
            }
            _ => datafusion::common::exec_err!(
                "LANG is only supported for TypedFamily and PlainTerm encoding"
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArrays::TypedFamily(tf_args)) => tf_args
                .map_children_tf_unary(|child| match child.downcast() {
                    DowncastTypedFamilyArray::String(array) => {
                        let langs =
                            array.language_array().iter().map(|i| i.unwrap_or(""));
                        let family_array = StringFamily::create_simple_strings_array(
                            Arc::new(StringArray::from_iter_values(langs)),
                        );
                        self.encodings
                            .typed_family()
                            .create_array_with_single_family(
                                StringFamily::FAMILY_ID,
                                family_array,
                            )
                    }
                    DowncastTypedFamilyArray::Null(_)
                    | DowncastTypedFamilyArray::Resource(_) => self
                        .encodings
                        .typed_family()
                        .create_null_array(child.array().len()),
                    _ => {
                        let len = child.array().len();
                        let res = Arc::new(StringArray::from(vec![""; len]));
                        let sf_array = StringFamily::create_simple_strings_array(res);
                        self.encodings
                            .typed_family()
                            .create_array_with_single_family(
                                StringFamily::FAMILY_ID,
                                sf_array,
                            )
                    }
                })?
                .into_array_ref(),
            Some(DowncastEncodingArrays::PlainTerm(pt_args)) => {
                let array = pt_args.get(0);
                let parts = array.as_parts();
                let len = array.len();

                let mut langs = StringBuilder::with_capacity(len, len);
                for i in 0..len {
                    if parts.struct_array.is_null(i) {
                        langs.append_null();
                    } else {
                        let term_type =
                            PlainTermType::try_from(parts.term_type.value(i)).unwrap();
                        if term_type != PlainTermType::Literal {
                            langs.append_null();
                        } else if parts.language_tag.is_null(i) {
                            langs.append_value("");
                        } else {
                            langs.append_value(parts.language_tag.value(i));
                        }
                    }
                }

                self.encodings
                    .plain_term()
                    .create_string_array(langs.finish())
            }
            _ => {
                return exec_err!(
                    "LANG is only supported for TypedFamily and PlainTerm encoding"
                );
            }
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
    use datafusion::dataframe::DataFrame;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::plain_term::PlainTermArrayElementBuilder;
    use rdf_fusion_model::vocab::xsd;
    use rdf_fusion_model::{LiteralRef, NamedNodeRef};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_lang_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(lang_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------+
        | input                                                                                        | LANG(?table?.input)                          |
        +----------------------------------------------------------------------------------------------+----------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                           |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                           |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                           |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                           |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.strings={value: , language: }}   |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.strings={value: , language: }}   |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.strings={value: , language: }}   |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.strings={value: , language: }}   |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.strings={value: , language: }}   |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.strings={value: , language: }}   |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: , language: }}   |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: , language: }}   |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: en, language: }} |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: , language: }}   |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.strings={value: , language: }}   |
        +----------------------------------------------------------------------------------------------+----------------------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_lang_plain_term() {
        let encodings = create_default_encodings();
        let mut plain_terms = PlainTermArrayElementBuilder::new();
        plain_terms.append_literal(LiteralRef::new_language_tagged_literal_unchecked(
            "hello", "en",
        ));
        plain_terms.append_literal(LiteralRef::new_simple_literal("world"));
        plain_terms.append_literal(LiteralRef::new_typed_literal("42", xsd::INTEGER));
        plain_terms.append_named_node(NamedNodeRef::new_unchecked("http://example.com"));
        plain_terms.append_null();
        let plain_terms = plain_terms.finish();

        let udf = Arc::new(lang_udf(encodings).unwrap());

        let input =
            DataFrame::from_columns(vec![("input", plain_terms.into_array_ref())])
                .unwrap();
        let result = input
            .select([col("input"), udf.call(vec![col("input")])])
            .unwrap();

        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r"
        +------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------+
        | input                                                                                                            | LANG(?table?.input)                                                                           |
        +------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------+
        | {term_type: 2, value: hello, data_type: http://www.w3.org/1999/02/22-rdf-syntax-ns#langString, language_tag: en} | {term_type: 2, value: en, data_type: http://www.w3.org/2001/XMLSchema#string, language_tag: } |
        | {term_type: 2, value: world, data_type: http://www.w3.org/2001/XMLSchema#string, language_tag: }                 | {term_type: 2, value: , data_type: http://www.w3.org/2001/XMLSchema#string, language_tag: }   |
        | {term_type: 2, value: 42, data_type: http://www.w3.org/2001/XMLSchema#integer, language_tag: }                   | {term_type: 2, value: , data_type: http://www.w3.org/2001/XMLSchema#string, language_tag: }   |
        | {term_type: 0, value: http://example.com, data_type: , language_tag: }                                           |                                                                                               |
        |                                                                                                                  |                                                                                               |
        +------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------+
        "
        );
    }
}
