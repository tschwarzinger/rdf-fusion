use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, BooleanBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::{
    BooleanFamilyArray, DowncastTypedFamilyArray, StringFamilyArray, TypedFamilyArray,
    TypedFamilyEncodingRef,
};
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::{AResult, DFResult, ThinError, ThinResult};
use regex::{Regex, RegexBuilder};
use std::any::Any;
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};

/// Implementation of the SPARQL `regex` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - REGEX](https://www.w3.org/TR/sparql11-query/#func-regex)
pub fn regex_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(RegexSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct RegexSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for RegexSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegexSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl RegexSparqlOp {
    /// Create a new [`RegexSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_binary_arity()
            .with_ternary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::Regex.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RegexSparqlOp {
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
            _ => exec_err!("Unsupported encoding for REGEX return type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args_wrapped =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args_wrapped.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => tf_args
                .map_children_tf(|children| {
                    let family_len = children[0].to_array().len();
                    let children = children
                        .iter()
                        .map(|c| c.as_downcast_array())
                        .collect::<Vec<_>>();

                    match children.as_slice() {
                        [
                            DowncastTypedFamilyArray::String(t_arr),
                            DowncastTypedFamilyArray::String(p_arr),
                        ] => regex_impl(tf_encoding, t_arr, p_arr, None),
                        [
                            DowncastTypedFamilyArray::String(t_arr),
                            DowncastTypedFamilyArray::String(p_arr),
                            DowncastTypedFamilyArray::String(f_arr),
                        ] => regex_impl(tf_encoding, t_arr, p_arr, Some(f_arr)),
                        [
                            DowncastTypedFamilyArray::String(t_arr),
                            DowncastTypedFamilyArray::String(p_arr),
                            DowncastTypedFamilyArray::Null(_),
                        ] => regex_impl(tf_encoding, t_arr, p_arr, None),
                        _ => tf_encoding.create_null_array(family_len),
                    }
                })?
                .into_array_ref(),
            _ => exec_err!("REGEX is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
    }
}

fn regex_impl(
    encoding: &TypedFamilyEncodingRef,
    t_arr: &StringFamilyArray,
    p_arr: &StringFamilyArray,
    f_arr: Option<&StringFamilyArray>,
) -> AResult<TypedFamilyArray> {
    let text = t_arr.value_array();
    let pattern = p_arr.value_array();
    let flags = f_arr.map(|f| f.value_array());

    let mut res = BooleanBuilder::with_capacity(text.len());
    for i in 0..text.len() {
        let is_flag_null = flags.as_ref().is_some_and(|f| f.is_null(i));

        if text.is_null(i) || pattern.is_null(i) || is_flag_null {
            res.append_null();
        } else {
            let flag_str = flags.as_ref().map(|f| f.value(i));
            let regex = compile_pattern(pattern.value(i), flag_str);
            if let Ok(regex) = regex {
                res.append_value(regex.is_match(text.value(i)));
            } else {
                res.append_null();
            }
        }
    }
    encoding.create_array_from_family(BooleanFamilyArray::new(res.finish()))
}

pub(super) fn compile_pattern(pattern: &str, flags: Option<&str>) -> ThinResult<Regex> {
    const REGEX_SIZE_LIMIT: usize = 1_000_000;

    let mut pattern = Cow::Borrowed(pattern);
    let flags = flags.unwrap_or_default();
    if flags.contains('q') {
        pattern = regex::escape(&pattern).into();
    }
    let mut regex_builder = RegexBuilder::new(&pattern);
    regex_builder.size_limit(REGEX_SIZE_LIMIT);
    for flag in flags.chars() {
        match flag {
            's' => {
                regex_builder.dot_matches_new_line(true);
            }
            'm' => {
                regex_builder.multi_line(true);
            }
            'i' => {
                regex_builder.case_insensitive(true);
            }
            'x' => {
                regex_builder.ignore_whitespace(true);
            }
            'q' => (),                         // Already supported
            _ => return ThinError::expected(), // invalid option
        }
    }
    regex_builder.build().map_err(|_| ThinError::ExpectedError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_default_encodings;
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::typed_family::{StringFamily, TypedFamily};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_regex_typed_family() {
        let encodings = create_default_encodings();
        let encoding = encodings.typed_family();

        let text_array = Arc::new(StringArray::from(vec![
            "foobar", "FOOBAR", "foobar", "foobar",
        ])) as ArrayRef;
        let text = encoding
            .create_array_with_single_family(
                StringFamily::FAMILY_ID,
                StringFamily::create_simple_strings_array(text_array),
            )
            .unwrap()
            .into_array_ref();

        let pattern_array =
            Arc::new(StringArray::from(vec!["foo", "foo", "foo", "foo"])) as ArrayRef;
        let pattern = encoding
            .create_array_with_single_family(
                StringFamily::FAMILY_ID,
                StringFamily::create_simple_strings_array(pattern_array),
            )
            .unwrap()
            .into_array_ref();

        let flags_array = Arc::new(StringArray::from(vec!["", "", "i", "s"])) as ArrayRef;
        let flags = encoding
            .create_array_with_single_family(
                StringFamily::FAMILY_ID,
                StringFamily::create_simple_strings_array(flags_array),
            )
            .unwrap()
            .into_array_ref();

        let udf = Arc::new(regex_udf(encodings).unwrap());

        let input = datafusion::prelude::DataFrame::from_columns(vec![
            ("text", text),
            ("pattern", pattern),
            ("flags", flags),
        ])
        .unwrap();

        let result = input
            .select(vec![udf.call(vec![
                col("text"),
                col("pattern"),
                col("flags"),
            ])])
            .unwrap();

        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r#"
        +---------------------------------------------------+
        | REGEX(?table?.text,?table?.pattern,?table?.flags) |
        +---------------------------------------------------+
        | {rdf-fusion.boolean=true}                         |
        | {rdf-fusion.boolean=false}                        |
        | {rdf-fusion.boolean=true}                         |
        | {rdf-fusion.boolean=true}                         |
        +---------------------------------------------------+
        "#
        );
    }
}
