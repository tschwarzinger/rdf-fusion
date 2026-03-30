use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingName, RdfFusionEncodings, detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Implementation of the SPARQL effective boolean value (EBV) function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - Effective Boolean Value](https://www.w3.org/TR/sparql11-query/#ebv)
pub fn effective_boolean_value_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(
        EffectiveBooleanValueSparqlOp::new(encodings),
    ))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct EffectiveBooleanValueSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for EffectiveBooleanValueSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectiveBooleanValueSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl EffectiveBooleanValueSparqlOp {
    /// Create a new [`EffectiveBooleanValueSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::EffectiveBooleanValue.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for EffectiveBooleanValueSparqlOp {
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
        let encoding_name = detect_encoding_from_types(&self.encodings, arg_types)?;

        match encoding_name {
            Some(EncodingName::TypedFamily) => Ok(DataType::Boolean),
            _ => {
                exec_err!("Unsupported encoding for EBV return type")
            }
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArrays::TypedFamily(tf_args)) => {
                let tf_array = tf_args.get(0);
                tf_array.effective_boolean_value()?
            }
            _ => exec_err!("EBV is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::conversion::encoding::with_typed_family_encoding;
    use crate::test_utils::{
        create_default_encodings, create_standard_test_vector, evaluate_function_for_test,
    };
    use datafusion::dataframe::DataFrame;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::plain_term::PlainTermArrayElementBuilder;
    use rdf_fusion_model::LiteralRef;
    use rdf_fusion_model::vocab::xsd;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_ebv_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(effective_boolean_value_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+--------------------+
        | input                                                                                        | EBV(?table?.input) |
        +----------------------------------------------------------------------------------------------+--------------------+
        | {rdf-fusion.null=}                                                                           |                    |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  |                    |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            |                    |
        | {rdf-fusion.resources={blank_node=123456}}                                                   |                    |
        | {rdf-fusion.numeric={integer=10}}                                                            | true               |
        | {rdf-fusion.numeric={float=10.0}}                                                            | true               |
        | {rdf-fusion.numeric={float=0.0}}                                                             | false              |
        | {rdf-fusion.numeric={double=20.0}}                                                           | true               |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | true               |
        | {rdf-fusion.numeric={int=40}}                                                                | true               |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | true               |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | true               |
        | {rdf-fusion.strings={value: hello, language: en}}                                            |                    |
        | {rdf-fusion.strings={value: 123, language: }}                                                | true               |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} |                    |
        +----------------------------------------------------------------------------------------------+--------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_ebv_numeric_ill_formed() {
        let encodings = create_default_encodings();
        let mut plain_terms = PlainTermArrayElementBuilder::new(1);
        plain_terms.append_literal(LiteralRef::new_typed_literal("xyz", xsd::INTEGER));
        let plain_terms = plain_terms.finish();

        let with_encoding_udf = Arc::new(with_typed_family_encoding(encodings.clone()));
        let ebv_udf = Arc::new(effective_boolean_value_udf(encodings).unwrap());

        let input =
            DataFrame::from_columns(vec![("input", plain_terms.into_array_ref())])
                .unwrap();
        let result = input
            .select([
                col("input"),
                ebv_udf.call(vec![with_encoding_udf.call(vec![col("input")])]),
            ])
            .unwrap();

        assert_snapshot!(result.to_string().await.unwrap(), @"
        +-------------------------------------------------------------------------------------------------+----------------------------+
        | input                                                                                           | EBV(ENC_TF(?table?.input)) |
        +-------------------------------------------------------------------------------------------------+----------------------------+
        | {term_type: 2, value: xyz, data_type: http://www.w3.org/2001/XMLSchema#integer, language_tag: } |                            |
        +-------------------------------------------------------------------------------------------------+----------------------------+
        ");
    }
}
