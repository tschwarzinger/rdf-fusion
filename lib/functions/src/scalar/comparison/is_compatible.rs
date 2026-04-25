use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, BooleanArray, make_comparator};
use datafusion::arrow::compute::kernels::cmp::not_distinct;
use datafusion::arrow::compute::{SortOptions, is_null, or};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use rdf_fusion_encoding::{RdfFusionEncodings, detect_encoding_from_types};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Implementation of the `IS_COMPATIBLE` function.
pub fn is_compatible_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(IsCompatibleSparqlOp::new(
        encodings,
    )))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct IsCompatibleSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for IsCompatibleSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IsCompatibleSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl IsCompatibleSparqlOp {
    /// Create a new [`IsCompatibleSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.plain_term().as_ref())
            .with_supported_encoding_opt(encodings.object_id().map(|e| e.as_ref()))
            .with_supported_encoding(encodings.string_encoding().as_ref())
            .with_binary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::IsCompatible.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsCompatibleSparqlOp {
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
        let _ = detect_encoding_from_types(&self.encodings, arg_types)?;
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        let data_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type())
            .cloned()
            .collect::<Vec<_>>();
        let return_type = self.return_type(&data_types)?;
        Ok(Arc::new(Field::new(self.name(), return_type, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let lhs = args.args[0].to_array(args.number_rows)?;
        let rhs = args.args[1].to_array(args.number_rows)?;

        let is_null_lhs = is_null(&lhs)?;
        let is_null_rhs = is_null(&rhs)?;
        let any_null = or(&is_null_lhs, &is_null_rhs)?;
        let is_equal = if lhs.data_type().is_nested() {
            let comparator =
                make_comparator(lhs.as_ref(), rhs.as_ref(), SortOptions::default())?;
            (0..args.number_rows)
                .map(|i| comparator(i, i) == Ordering::Equal)
                .collect::<BooleanArray>()
        } else {
            not_distinct(&lhs, &rhs)?
        };
        let result = or(&is_equal, &any_null)?;

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_default_encodings;
    use datafusion::dataframe::DataFrame;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::plain_term::PlainTermArrayElementBuilder;
    use rdf_fusion_model::NamedNodeRef;

    #[tokio::test]
    async fn test_is_compatible_mixed() {
        let encodings = create_default_encodings();
        let mut lhs_builder = PlainTermArrayElementBuilder::new();
        let mut rhs_builder = PlainTermArrayElementBuilder::new();

        let iri_a = NamedNodeRef::new_unchecked("http://example.org/a");
        let iri_b = NamedNodeRef::new_unchecked("http://example.org/b");

        // Pair 1: Same IRI
        lhs_builder.append_named_node(iri_a);
        rhs_builder.append_named_node(iri_a);

        // Pair 2: Different IRI
        lhs_builder.append_named_node(iri_a);
        rhs_builder.append_named_node(iri_b);

        // Pair 3: IRI + Null
        lhs_builder.append_null();
        rhs_builder.append_named_node(iri_a);

        // Pair 3: Null + Null
        lhs_builder.append_null();
        rhs_builder.append_null();

        let lhs = lhs_builder.finish();
        let rhs = rhs_builder.finish();
        let udf = Arc::new(is_compatible_udf(encodings).unwrap());

        let df = DataFrame::from_columns(vec![
            ("lhs", lhs.into_array_ref()),
            ("rhs", rhs.into_array_ref()),
        ])
        .unwrap();

        let result = df
            .select([
                col("lhs"),
                col("rhs"),
                udf.call(vec![col("lhs"), col("rhs")]),
            ])
            .unwrap();

        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +--------------------------------------------------------------------------+--------------------------------------------------------------------------+----------------------------------------+
        | lhs                                                                      | rhs                                                                      | IS_COMPATIBLE(?table?.lhs,?table?.rhs) |
        +--------------------------------------------------------------------------+--------------------------------------------------------------------------+----------------------------------------+
        | {term_type: 0, value: http://example.org/a, data_type: , language_tag: } | {term_type: 0, value: http://example.org/a, data_type: , language_tag: } | true                                   |
        | {term_type: 0, value: http://example.org/a, data_type: , language_tag: } | {term_type: 0, value: http://example.org/b, data_type: , language_tag: } | false                                  |
        |                                                                          | {term_type: 0, value: http://example.org/a, data_type: , language_tag: } | true                                   |
        |                                                                          |                                                                          | true                                   |
        +--------------------------------------------------------------------------+--------------------------------------------------------------------------+----------------------------------------+
        "
        );
    }
}
