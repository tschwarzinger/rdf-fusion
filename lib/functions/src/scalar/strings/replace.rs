use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpArity;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, StringFamily, StringFamilyArray, TypedFamily,
    TypedFamilyArray, TypedFamilyEncodingRef,
};
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, EncodingName, RdfFusionEncodings,
    TermEncoding, detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::{AResult, DFResult};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Implementation of the SPARQL `replace` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - REPLACE](https://www.w3.org/TR/sparql11-query/#func-replace)
pub fn replace_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(ReplaceSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ReplaceSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for ReplaceSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaceSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl ReplaceSparqlOp {
    /// Create a new [`ReplaceSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_ternary_arity()
            .with_arity(SparqlOpArity::Fixed(NonZeroUsize::new(4).unwrap()))
            .build();
        Self {
            encodings,
            name: BuiltinName::Replace.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ReplaceSparqlOp {
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
            _ => exec_err!("Unsupported encoding for REPLACE return type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args_wrapped =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args_wrapped.downcast_arrays() {
            Some(DowncastEncodingArrays::TypedFamily(tf_args)) => tf_args
                .map_children_tf(|children| {
                    let family_len = children[0].array().len();
                    let children =
                        children.iter().map(|c| c.downcast()).collect::<Vec<_>>();

                    match children.as_slice() {
                        [
                            DowncastTypedFamilyArray::String(t_arr),
                            DowncastTypedFamilyArray::String(p_arr),
                            DowncastTypedFamilyArray::String(r_arr),
                        ] => replace_impl(tf_encoding, t_arr, p_arr, r_arr, None),
                        [
                            DowncastTypedFamilyArray::String(t_arr),
                            DowncastTypedFamilyArray::String(p_arr),
                            DowncastTypedFamilyArray::String(r_arr),
                            DowncastTypedFamilyArray::String(f_arr),
                        ] => replace_impl(tf_encoding, t_arr, p_arr, r_arr, Some(f_arr)),
                        [
                            DowncastTypedFamilyArray::String(t_arr),
                            DowncastTypedFamilyArray::String(p_arr),
                            DowncastTypedFamilyArray::String(r_arr),
                            DowncastTypedFamilyArray::Null(_),
                        ] => replace_impl(tf_encoding, t_arr, p_arr, r_arr, None),
                        _ => tf_encoding.create_null_array(family_len),
                    }
                })?
                .into_array_ref(),
            _ => exec_err!("REPLACE is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
    }
}

fn replace_impl(
    encoding: &TypedFamilyEncodingRef,
    t_arr: &StringFamilyArray,
    p_arr: &StringFamilyArray,
    r_arr: &StringFamilyArray,
    f_arr: Option<&StringFamilyArray>,
) -> AResult<TypedFamilyArray> {
    let text = t_arr.value_array();
    let pattern = p_arr.value_array();
    let replacement = r_arr.value_array();
    let flags = f_arr.map(|f| f.value_array());

    let mut res_values = StringBuilder::with_capacity(text.len(), text.len() * 10);
    for i in 0..text.len() {
        let is_flag_null = flags.as_ref().is_some_and(|f| f.is_null(i));

        if pattern.is_null(i) || replacement.is_null(i) || is_flag_null {
            res_values.append_null();
        } else {
            let flag_str = flags.as_ref().map(|f| f.value(i));
            let regex = super::regex::compile_pattern(pattern.value(i), flag_str);
            if let Ok(regex) = regex {
                res_values.append_value(
                    &regex.replace_all(text.value(i), replacement.value(i)),
                );
            } else {
                res_values.append_null();
            }
        }
    }

    let sf_array = StringFamily::create_strings_array(
        Arc::new(res_values.finish()),
        Arc::new(t_arr.language_array().clone()),
    );
    encoding.create_array_with_single_family(StringFamily::FAMILY_ID, sf_array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;

    use crate::test_utils::create_default_encodings;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_replace_typed_family() {
        let encodings = create_default_encodings();
        let encoding = encodings.typed_family();

        let text_array = Arc::new(StringArray::from(vec![
            "foobar", "foobar", "foobar", "foobar",
        ])) as ArrayRef;
        let text = encoding
            .create_array_with_single_family(
                StringFamily::FAMILY_ID,
                StringFamily::create_simple_strings_array(text_array),
            )
            .unwrap()
            .into_array_ref();

        let pattern_array =
            Arc::new(StringArray::from(vec!["foo", "bar", "o", "o"])) as ArrayRef;
        let pattern = encoding
            .create_array_with_single_family(
                StringFamily::FAMILY_ID,
                StringFamily::create_simple_strings_array(pattern_array),
            )
            .unwrap()
            .into_array_ref();

        let replacement_array =
            Arc::new(StringArray::from(vec!["baz", "qux", "a", ""])) as ArrayRef;
        let replacement = encoding
            .create_array_with_single_family(
                StringFamily::FAMILY_ID,
                StringFamily::create_simple_strings_array(replacement_array),
            )
            .unwrap()
            .into_array_ref();

        let udf = Arc::new(replace_udf(encodings).unwrap());

        let input = datafusion::prelude::DataFrame::from_columns(vec![
            ("text", text),
            ("pattern", pattern),
            ("replacement", replacement),
        ])
        .unwrap();

        let result = input
            .select(vec![udf.call(vec![
                col("text"),
                col("pattern"),
                col("replacement"),
            ])])
            .unwrap();

        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r"
        +-----------------------------------------------------------+
        | REPLACE(?table?.text,?table?.pattern,?table?.replacement) |
        +-----------------------------------------------------------+
        | {rdf-fusion.strings={value: bazbar, language: }}          |
        | {rdf-fusion.strings={value: fooqux, language: }}          |
        | {rdf-fusion.strings={value: faabar, language: }}          |
        | {rdf-fusion.strings={value: fbar, language: }}            |
        +-----------------------------------------------------------+
        "
        );
    }
}
