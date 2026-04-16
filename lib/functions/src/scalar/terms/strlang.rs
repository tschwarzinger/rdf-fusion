use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermType};
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use rdf_fusion_model::vocab::rdf::LANG_STRING;
use rdf_fusion_model::vocab::xsd;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// Creates a new RDF term from a plain literal and a language tag.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STRLANG](https://www.w3.org/TR/sparql11-query/#func-strlang)
pub fn strlang_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StrLangSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct StrLangSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for StrLangSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrLangSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl StrLangSparqlOp {
    /// Create a new [`StrLangSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_binary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::StrLang.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StrLangSparqlOp {
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
            _ => exec_err!("STRLANG only supports the TypedFamily encoding"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let num_rows = args.number_rows;
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => {
                let lhs = tf_args.get(0);
                let rhs = tf_args.get(1);

                let lhs_pt = lhs.as_plain_term_array()?;
                let rhs_pt = rhs.as_plain_term_array()?;

                let res_pt = self.compute_plain_term(&lhs_pt, &rhs_pt, num_rows)?;
                tf_encoding
                    .cast_from_plain_term_array(&res_pt)?
                    .into_array_ref()
            }
            _ => exec_err!("STRLANG is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
    }
}

impl StrLangSparqlOp {
    fn compute_plain_term(
        &self,
        lhs: &PlainTermArray,
        rhs: &PlainTermArray,
        num_rows: usize,
    ) -> DFResult<PlainTermArray> {
        let lhs_parts = lhs.as_parts();
        let rhs_parts = rhs.as_parts();

        let mut result_values = Vec::with_capacity(num_rows);
        let mut result_langs = Vec::with_capacity(num_rows);
        let mut is_valid = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            let lhs_type = lhs_parts.term_type.value(i);
            let rhs_type = rhs_parts.term_type.value(i);

            if lhs_type == i8::from(PlainTermType::Literal)
                && rhs_type == i8::from(PlainTermType::Literal)
            {
                let lhs_lang = lhs_parts.language_tag.value(i);
                let lhs_dt = lhs_parts.data_type.value(i);
                let rhs_lang_val = rhs_parts.value.value(i).to_lowercase();
                let rhs_lang_tag = rhs_parts.language_tag.value(i);
                let rhs_dt = rhs_parts.data_type.value(i);

                if lhs_lang.is_empty()
                    && (lhs_dt.is_empty() || lhs_dt == xsd::STRING.as_str())
                    && rhs_lang_tag.is_empty()
                    && (rhs_dt.is_empty() || rhs_dt == xsd::STRING.as_str())
                {
                    result_values.push(lhs_parts.value.value(i).to_string());
                    result_langs.push(rhs_lang_val);
                    is_valid.push(true);
                    continue;
                }
            }
            result_values.push(String::new());
            result_langs.push(String::new());
            is_valid.push(false);
        }

        let validity = NullBuffer::from(is_valid);
        let mut result_datatypes = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            if validity.is_valid(i) {
                result_datatypes.push(Some(LANG_STRING.as_str()));
            } else {
                result_datatypes.push(None);
            }
        }

        let result = PlainTermArray::try_new_literals(
            StringArray::from(result_values),
            StringArray::from(result_datatypes),
            StringArray::from(result_langs),
            Some(validity),
        )?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_default_encodings, evaluate_function_with_args_for_test,
    };
    use datafusion::logical_expr::{col, lit};
    use insta::assert_snapshot;
    use rdf_fusion_encoding::typed_family::{
        FamilyArray, StringFamily, StringFamilyArray, TypedFamilyArrayBuilder,
        TypedFamilyEncoding, TypedFamilyId,
    };
    use rdf_fusion_encoding::{EncodingArray, EncodingScalar};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_strlang_typed_family() {
        let encoding = Arc::new(TypedFamilyEncoding::default());

        let strings_array =
            StringFamily::create_simple_strings_array(Arc::new(StringArray::from(vec![
                "abc",
            ])));
        let string_type_id = encoding
            .find_typed_family_type_id(TypedFamilyId::String)
            .unwrap();
        let test_vector = TypedFamilyArrayBuilder::new(
            Arc::clone(&encoding),
            vec![string_type_id],
            vec![0],
        )
        .unwrap()
        .with_family_array::<StringFamilyArray>(Some(
            StringFamilyArray::from_array_unchecked(strings_array.clone()),
        ))
        .unwrap()
        .finish()
        .unwrap()
        .into_array_ref();

        let encodings = create_default_encodings();
        let udf = Arc::new(strlang_udf(encodings).unwrap());

        let lang_array = encoding
            .create_array_from_family(StringFamilyArray::new_simple(StringArray::from(
                vec!["en"],
            )))
            .unwrap();
        let lang_scalar = lang_array.try_as_scalar(0).unwrap();

        let result = evaluate_function_with_args_for_test(
            test_vector,
            udf,
            vec![col("input"), lit(lang_scalar.into_scalar_value())],
        );
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r"
        +-----------------------------------------------+-----------------------------------------------------+
        | input                                         | STRLANG(?table?.input,Union 2:{value:en,language:}) |
        +-----------------------------------------------+-----------------------------------------------------+
        | {rdf-fusion.strings={value: abc, language: }} | {rdf-fusion.strings={value: abc, language: en}}     |
        +-----------------------------------------------+-----------------------------------------------------+
        "
        )
    }
}
