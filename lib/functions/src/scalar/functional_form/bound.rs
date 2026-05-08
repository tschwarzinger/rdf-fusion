use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::compute::is_not_null;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_family::BooleanFamilyArray;
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;

/// Implementation of the SPARQL `BOUND` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - BOUND](https://www.w3.org/TR/sparql11-query/#func-bound)
pub fn bound_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(BoundSparqlOp::new(encodings)))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BoundSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl BoundSparqlOp {
    /// Create a new [`BoundSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let mut type_signature_builder = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.plain_term().as_ref())
            .with_supported_encoding(encodings.typed_family().as_ref());

        if let Some(oid_encoding) = encodings.object_id() {
            type_signature_builder =
                type_signature_builder.with_supported_encoding(oid_encoding.as_ref());
        }

        let type_signature = type_signature_builder.with_unary_arity().build();

        Self {
            encodings,
            name: BuiltinName::Bound.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BoundSparqlOp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(self.encodings.typed_family().data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let input = args.args[0].to_array(args.number_rows)?;
        let bound = BooleanFamilyArray::from(is_not_null(&input)?);
        let result = self
            .encodings
            .typed_family()
            .create_array_from_family(bound)?;
        Ok(ColumnarValue::Array(result.into_array_ref()))
    }
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
    async fn test_bound_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(bound_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+----------------------------+
        | input                                                                                        | BOUND(?table?.input)       |
        +----------------------------------------------------------------------------------------------+----------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.boolean=false} |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.boolean=true}  |
        +----------------------------------------------------------------------------------------------+----------------------------+
        "
        )
    }
}
