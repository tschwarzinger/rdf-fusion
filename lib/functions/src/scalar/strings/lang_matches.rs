use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
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

/// Returns true if the language tag matches the language range.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - LANGMATCHES](https://www.w3.org/TR/sparql11-query/#func-langMatches)
pub fn lang_matches_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(LangMatchesSparqlOp::new(
        encodings,
    )))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct LangMatchesSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for LangMatchesSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LangMatchesSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl LangMatchesSparqlOp {
    /// Create a new [`LangMatchesSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_binary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::LangMatches.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LangMatchesSparqlOp {
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
            _ => exec_err!("Unsupported encoding for LANGMATCHES return type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args_wrapped =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args_wrapped.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => tf_args
                .map_children_tf_binary(|lhs, rhs| {
                    match (lhs.as_downcast_array(), rhs.as_downcast_array()) {
                        (
                            DowncastTypedFamilyArray::String(l),
                            DowncastTypedFamilyArray::String(r),
                        ) => {
                            let res =
                                l.apply_binary_boolean_element_wise(&r, |tag, range| {
                                    if range == "*" {
                                        !tag.is_empty()
                                    } else {
                                        tag.to_lowercase() == range.to_lowercase()
                                            || tag.to_lowercase().starts_with(&format!(
                                                "{}-",
                                                range.to_lowercase()
                                            ))
                                    }
                                });
                            tf_encoding
                                .create_array_from_family(BooleanFamilyArray::new(res))
                        }
                        _ => tf_encoding.create_null_array(lhs.to_array().len()),
                    }
                })?
                .into_array_ref(),
            _ => exec_err!("LANGMATCHES is only supported for TypedFamily encoding")?,
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
    async fn test_lang_matches_typed_family() {
        let encodings = create_default_encodings();

        let left = create_typed_family_strings_array(
            &encodings,
            vec!["en", "en-US", "en-GB", "de", ""],
            vec![None, None, None, None, None],
        );

        let right = create_typed_family_strings_array(
            &encodings,
            vec!["en", "en", "en-GB", "en", "*"],
            vec![None, None, None, None, None],
        );

        let udf = Arc::new(lang_matches_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r"
        +-------------------------------------------------+-------------------------------------------------+-----------------------------------------+
        | left                                            | right                                           | LANGMATCHES(?table?.left,?table?.right) |
        +-------------------------------------------------+-------------------------------------------------+-----------------------------------------+
        | {rdf-fusion.strings={value: en, language: }}    | {rdf-fusion.strings={value: en, language: }}    | {rdf-fusion.boolean=true}               |
        | {rdf-fusion.strings={value: en-US, language: }} | {rdf-fusion.strings={value: en, language: }}    | {rdf-fusion.boolean=true}               |
        | {rdf-fusion.strings={value: en-GB, language: }} | {rdf-fusion.strings={value: en-GB, language: }} | {rdf-fusion.boolean=true}               |
        | {rdf-fusion.strings={value: de, language: }}    | {rdf-fusion.strings={value: en, language: }}    | {rdf-fusion.boolean=false}              |
        | {rdf-fusion.strings={value: , language: }}      | {rdf-fusion.strings={value: *, language: }}     | {rdf-fusion.boolean=false}              |
        +-------------------------------------------------+-------------------------------------------------+-----------------------------------------+
        "
        );
    }
}
