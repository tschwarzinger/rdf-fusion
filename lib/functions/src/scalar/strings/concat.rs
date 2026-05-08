use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use arrow_string::concat_elements::concat_elements_utf8_many;
use datafusion::arrow::array::{Array, BooleanArray, StringArray};
use datafusion::arrow::compute::kernels::cmp::distinct;
use datafusion::arrow::compute::{nullif, or};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use itertools::repeat_n;
use rdf_fusion_common::{AResult, DFResult};
use rdf_fusion_encoding::typed_family::{
    FamilyArray, StringFamily, StringFamilyArray, TypedFamily, TypedFamilyArray,
    TypedFamilyChild, TypedFamilyId,
};
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Concatenates multiple RDF terms into a single string literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - CONCAT](https://www.w3.org/TR/sparql11-query/#func-concat)
pub fn concat_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(ConcatSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ConcatSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for ConcatSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcatSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl ConcatSparqlOp {
    /// Create a new [`ConcatSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_variadic_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::Concat.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ConcatSparqlOp {
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
            Some(EncodingName::TypedFamily) | None => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            _ => exec_err!("Unsupported encoding for CONCAT return type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let num_rows = args.number_rows;
        let args_wrapped =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args_wrapped.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => {
                tf_args.map_children_tf(|cs| self.map_children_typed_family(cs))
            }
            None => {
                let result_lexical = StringArray::from(vec![""; num_rows]);
                let result_tags = StringArray::new_null(num_rows);

                let sf_array = StringFamily::create_strings_array(
                    Arc::new(result_lexical),
                    Arc::new(result_tags),
                );

                tf_encoding
                    .create_array_with_single_family(StringFamily::FAMILY_ID, sf_array)
            }
            _ => return exec_err!("CONCAT is only supported for TypedFamily encoding"),
        };

        Ok(ColumnarValue::Array(result?.into_array_ref()))
    }
}

impl ConcatSparqlOp {
    /// Implements the actual logic of handling typed family arrays.
    fn map_children_typed_family(
        &self,
        children: &[TypedFamilyChild],
    ) -> AResult<TypedFamilyArray> {
        let len = children[0].to_array().len();
        let any_non_string = children
            .iter()
            .any(|c| c.family().family_id() != TypedFamilyId::String);
        if any_non_string {
            return self.encodings.typed_family().create_null_array(len);
        }

        let string_arrays = children
            .iter()
            .map(|c| StringFamilyArray::from_array_unchecked(c.to_array()))
            .collect::<Vec<_>>();

        let lexical_forms = string_arrays
            .iter()
            .map(|arr| arr.value_array())
            .collect::<Vec<_>>();
        let tags = string_arrays
            .iter()
            .map(|arr| arr.language_array())
            .collect::<Vec<_>>();

        let lexical_results = concat_elements_utf8_many(&lexical_forms)?;

        let first_tag = tags[0];
        let mismatches = tags
            .iter()
            .skip(1)
            .map(|arr| distinct(first_tag, arr))
            .reduce(|lhs, rhs| or(&lhs?, &rhs?))
            .unwrap_or(Ok(BooleanArray::from_iter(repeat_n(
                false,
                first_tag.len(),
            ))))?;
        let result_tags = nullif(first_tag, &mismatches)?;

        let sf_array = StringFamily::create_strings_array(
            Arc::new(lexical_results),
            Arc::new(result_tags),
        );

        self.encodings
            .typed_family()
            .create_array_with_single_family(StringFamily::FAMILY_ID, sf_array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_default_encodings;
    use datafusion::arrow::array::{AsArray, Int64Array, StringArray};
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::typed_family::{
        NumericFamilyArray, StringFamily, StringFamilyArray,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_concat_typed_family() {
        let encodings = create_default_encodings();
        let encoding = encodings.typed_family();

        let s1_array = StringFamilyArray::new_simple(StringArray::from(vec![
            "foo", "foo", "foo", "foo",
        ]));
        let s1 = encoding
            .create_array_from_family(s1_array)
            .unwrap()
            .into_array_ref();

        let s2_array = StringFamily::create_strings_array(
            Arc::new(StringArray::from(vec!["bar", "bar", "bar", "bar"])),
            Arc::new(StringArray::from(vec![
                None,
                Some("en"),
                Some("en"),
                Some("de"),
            ])),
        );
        let s2 = encoding
            .create_array_with_single_family(StringFamily::FAMILY_ID, s2_array)
            .unwrap()
            .into_array_ref();

        let s3_array = StringFamily::create_strings_array(
            Arc::new(StringArray::from(vec!["baz", "baz", "baz", "baz"])),
            Arc::new(StringArray::from(vec![None, None, Some("en"), Some("de")])),
        );
        let s3 = encoding
            .create_array_with_single_family(StringFamily::FAMILY_ID, s3_array)
            .unwrap()
            .into_array_ref();

        let udf = Arc::new(concat_udf(encodings).unwrap());

        let input = datafusion::prelude::DataFrame::from_columns(vec![
            ("s1", s1),
            ("s2", s2),
            ("s3", s3),
        ])
        .unwrap();

        let result = input
            .select(vec![
                col("s1"),
                col("s2"),
                col("s3"),
                udf.call(vec![col("s1"), col("s2"), col("s3")]),
            ])
            .unwrap();

        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r"
        +-----------------------------------------------+-------------------------------------------------+-------------------------------------------------+-----------------------------------------------------+
        | s1                                            | s2                                              | s3                                              | CONCAT(?table?.s1,?table?.s2,?table?.s3)            |
        +-----------------------------------------------+-------------------------------------------------+-------------------------------------------------+-----------------------------------------------------+
        | {rdf-fusion.strings={value: foo, language: }} | {rdf-fusion.strings={value: bar, language: }}   | {rdf-fusion.strings={value: baz, language: }}   | {rdf-fusion.strings={value: foobarbaz, language: }} |
        | {rdf-fusion.strings={value: foo, language: }} | {rdf-fusion.strings={value: bar, language: en}} | {rdf-fusion.strings={value: baz, language: }}   | {rdf-fusion.strings={value: foobarbaz, language: }} |
        | {rdf-fusion.strings={value: foo, language: }} | {rdf-fusion.strings={value: bar, language: en}} | {rdf-fusion.strings={value: baz, language: en}} | {rdf-fusion.strings={value: foobarbaz, language: }} |
        | {rdf-fusion.strings={value: foo, language: }} | {rdf-fusion.strings={value: bar, language: de}} | {rdf-fusion.strings={value: baz, language: de}} | {rdf-fusion.strings={value: foobarbaz, language: }} |
        +-----------------------------------------------+-------------------------------------------------+-------------------------------------------------+-----------------------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn reproduce_concat_issue() {
        let encodings = create_default_encodings();
        let encoding = encodings.typed_family();

        // s1: "123" (simple literal)
        let s1_array = StringFamilyArray::new_simple(StringArray::from(vec!["123"]));
        let s1 = encoding
            .create_array_from_family(s1_array)
            .unwrap()
            .into_array_ref();

        // s7: 7 (integer)
        let s7_val_array = Int64Array::from(vec![7]);
        let s7_numeric = NumericFamilyArray::new_integers(s7_val_array);
        let s7 = encoding
            .create_array_from_family(s7_numeric)
            .unwrap()
            .into_array_ref();

        let udf = Arc::new(concat_udf(encodings).unwrap());

        let input =
            datafusion::prelude::DataFrame::from_columns(vec![("s1", s1), ("s7", s7)])
                .unwrap();

        let result = input
            .select(vec![udf.call(vec![col("s1"), col("s7")])])
            .unwrap();

        let batches = result.collect().await.unwrap();

        let val = batches[0].column(0);
        let union_val = val.as_union();
        let tid = union_val.type_id(0);
        assert_eq!(tid, 0, "CONCAT('123', 7) should be NULL (type ID 0)");
    }

    #[tokio::test]
    async fn test_concat_simple_literals_repro() {
        let encodings = create_default_encodings();
        let encoding = encodings.typed_family();

        // s1: "123" (simple literal)
        let s1_array = StringFamilyArray::new_simple(StringArray::from(vec!["123"]));
        let s1 = encoding
            .create_array_from_family(s1_array)
            .unwrap()
            .into_array_ref();

        let udf = Arc::new(concat_udf(encodings).unwrap());

        let input = datafusion::prelude::DataFrame::from_columns(vec![
            ("s1", s1.clone()),
            ("s2", s1),
        ])
        .unwrap();

        let result = input
            .select(vec![udf.call(vec![col("s1"), col("s2")])])
            .unwrap();

        let batches = result.collect().await.unwrap();
        let val = batches[0].column(0);
        assert!(!val.is_null(0), "CONCAT('123', '123') should NOT be NULL");
    }
}
