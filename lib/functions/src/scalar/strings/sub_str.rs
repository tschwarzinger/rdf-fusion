use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, NumericFamilyArray, StringFamily, StringFamilyArray,
    TypedFamily, TypedFamilyArray, TypedFamilyEncodingRef,
};
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, EncodingName, RdfFusionEncodings,
    TermEncoding, detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::{AResult, DFResult};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Implementation of the SPARQL `substr` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - SUBSTR](https://www.w3.org/TR/sparql11-query/#func-substr)
pub fn sub_str_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(SubStrSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct SubStrSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for SubStrSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubStrSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl SubStrSparqlOp {
    /// Create a new [`SubStrSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_binary_arity()
            .with_ternary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::SubStr.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SubStrSparqlOp {
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
            _ => exec_err!("Unsupported encoding for SUBSTR return type"),
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
                            DowncastTypedFamilyArray::String(s_arr),
                            DowncastTypedFamilyArray::Numeric(st_arr),
                        ] => substr_impl(tf_encoding, s_arr, st_arr, None),
                        [
                            DowncastTypedFamilyArray::String(s_arr),
                            DowncastTypedFamilyArray::Numeric(st_arr),
                            DowncastTypedFamilyArray::Numeric(l_arr),
                        ] => substr_impl(tf_encoding, s_arr, st_arr, Some(l_arr)),
                        [
                            DowncastTypedFamilyArray::String(s_arr),
                            DowncastTypedFamilyArray::Numeric(st_arr),
                            DowncastTypedFamilyArray::Null(_),
                        ] => substr_impl(tf_encoding, s_arr, st_arr, None),
                        _ => tf_encoding.create_null_array(family_len),
                    }
                })?
                .into_array_ref(),
            _ => exec_err!("SUBSTR is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
    }
}

fn substr_impl(
    encoding: &TypedFamilyEncodingRef,
    s_arr: &StringFamilyArray,
    st_arr: &NumericFamilyArray,
    l_arr: Option<&NumericFamilyArray>,
) -> AResult<TypedFamilyArray> {
    let source = s_arr.value_array();

    // Cast to Int64 to match our indexing expectations
    let start_raw = st_arr.cast(&DataType::Int64)?;
    let start = start_raw.as_primitive::<Int64Type>();

    let length = match l_arr {
        Some(l) => {
            let length_raw = l.cast(&DataType::Int64)?;
            Some(length_raw.as_primitive::<Int64Type>().clone())
        }
        None => None,
    };

    let mut res_values = StringBuilder::with_capacity(source.len(), source.len() * 10);

    for i in 0..source.len() {
        let is_length_null = length.as_ref().is_some_and(|l| l.is_null(i));

        if source.is_null(i) || start.is_null(i) || is_length_null {
            res_values.append_null();
        } else {
            let s_val = source.value(i);
            // SPARQL strings are 1-indexed. Convert to 0-indexed for rust/arrow.
            let st_val = start.value(i);
            let rust_start = std::cmp::max(0, st_val - 1) as usize;

            let len_val = length.as_ref().map(|l| l.value(i));

            // Use character iteration to correctly handle multi-byte characters (like emojis)
            let chars: Vec<char> = s_val.chars().collect();

            if rust_start >= chars.len() {
                res_values.append_value("");
            } else {
                let end = match len_val {
                    Some(l) => std::cmp::min(
                        chars.len(),
                        rust_start + std::cmp::max(0, l) as usize,
                    ),
                    None => chars.len(),
                };

                let sub: String = chars[rust_start..end].iter().collect();
                res_values.append_value(&sub);
            }
        }
    }

    let sf_array = StringFamily::create_strings_array(
        Arc::new(res_values.finish()),
        Arc::new(s_arr.language_array().clone()),
    );
    encoding.create_array_with_single_family(StringFamily::FAMILY_ID, sf_array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_default_encodings;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::dataframe::DataFrame;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::typed_family::{NumericFamilyArray, StringFamilyArray};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_substr_typed_family() {
        let encodings = create_default_encodings();
        let encoding = encodings.typed_family();

        let source_array = Arc::new(StringArray::from(vec![
            "foobar", "foobar", "foobar", "foobar",
        ])) as ArrayRef;
        let source = encoding
            .create_array_with_single_family(
                StringFamily::FAMILY_ID,
                StringFamily::create_simple_strings_array(source_array),
            )
            .unwrap()
            .into_array_ref();

        let start_array =
            NumericFamilyArray::new_integers(Int64Array::from(vec![1, 4, 1, 4]));
        let start = encoding
            .create_array_from_family(start_array)
            .unwrap()
            .into_array_ref();

        let length_array =
            NumericFamilyArray::new_integers(Int64Array::from(vec![3, 2, 10, 10]));
        let length = encoding
            .create_array_from_family(length_array)
            .unwrap()
            .into_array_ref();

        let udf = Arc::new(sub_str_udf(encodings).unwrap());

        let input = DataFrame::from_columns(vec![
            ("source", source),
            ("start", start),
            ("length", length),
        ])
        .unwrap();

        let result = input
            .select(vec![udf.call(vec![
                col("source"),
                col("start"),
                col("length"),
            ])])
            .unwrap();

        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r"
        +-----------------------------------------------------+
        | SUBSTR(?table?.source,?table?.start,?table?.length) |
        +-----------------------------------------------------+
        | {rdf-fusion.strings={value: foo, language: }}       |
        | {rdf-fusion.strings={value: ba, language: }}        |
        | {rdf-fusion.strings={value: foobar, language: }}    |
        | {rdf-fusion.strings={value: bar, language: }}       |
        +-----------------------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_substr_multibyte() {
        let encodings = create_default_encodings();
        let encoding = encodings.typed_family();

        let source_array =
            StringFamilyArray::new_simple(StringArray::from(vec!["食べ物", "👪", "👨‍👩‍👧‍👦"]));
        let source = encoding
            .create_array_from_family(source_array)
            .unwrap()
            .into_array_ref();

        let start_array =
            NumericFamilyArray::new_integers(Int64Array::from(vec![1, 1, 1]));
        let start = encoding
            .create_array_from_family(start_array)
            .unwrap()
            .into_array_ref();

        let length_array =
            NumericFamilyArray::new_integers(Int64Array::from(vec![1, 1, 1]));
        let length = encoding
            .create_array_from_family(length_array)
            .unwrap()
            .into_array_ref();

        let udf = Arc::new(sub_str_udf(encodings).unwrap());

        let input = DataFrame::from_columns(vec![
            ("source", source),
            ("start", start),
            ("length", length),
        ])
        .unwrap();

        let result = input
            .select(vec![udf.call(vec![
                col("source"),
                col("start"),
                col("length"),
            ])])
            .unwrap();

        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r"
        +-----------------------------------------------------+
        | SUBSTR(?table?.source,?table?.start,?table?.length) |
        +-----------------------------------------------------+
        | {rdf-fusion.strings={value: 食, language: }}        |
        | {rdf-fusion.strings={value: 👪, language: }}        |
        | {rdf-fusion.strings={value: 👨, language: }}        |
        +-----------------------------------------------------+
        "
        );
    }
}
