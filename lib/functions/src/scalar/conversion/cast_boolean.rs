use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::{
    BooleanFamily, BooleanFamilyArray, DowncastTypedFamilyArray, TypedFamily,
};
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, EncodingName, RdfFusionEncodings,
    TermEncoding, detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// Implementation of the SPARQL `xsd:boolean()` cast function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - CAST](https://www.w3.org/TR/sparql11-query/#func-cast)
pub fn cast_boolean_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CastBooleanSparqlOp::new(
        encodings,
    )))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CastBooleanSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for CastBooleanSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CastBooleanSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl CastBooleanSparqlOp {
    /// Create a new [`CastBooleanSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::CastBoolean.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CastBooleanSparqlOp {
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
            _ => exec_err!("xsd:boolean is only supported for TypedFamily encoding"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArrays::TypedFamily(tf_args)) => {
                let encoding = self.encodings.typed_family();
                tf_args
                    .map_children_tf_unary(|child| match child.downcast() {
                        DowncastTypedFamilyArray::Numeric(array) => {
                            let bools = array.is_not_zero()?;
                            encoding
                                .create_array_from_family(BooleanFamilyArray::from(bools))
                        }
                        DowncastTypedFamilyArray::Boolean(array) => {
                            encoding.create_array_from_family(array)
                        }
                        DowncastTypedFamilyArray::String(array) => {
                            let cast = array.cast(&DataType::Boolean)?;
                            encoding.create_array_with_single_family(
                                BooleanFamily::FAMILY_ID,
                                cast,
                            )
                        }
                        _ => encoding.create_null_array(child.array().len()),
                    })?
                    .into_array_ref()
            }
            _ => exec_err!("xsd:boolean is only supported for TypedFamily encoding")?,
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
    use insta::assert_snapshot;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_cast_boolean_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(cast_boolean_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------+
        | input                                                                                        | xsd:boolean(?table?.input) |
        +----------------------------------------------------------------------------------------------+----------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}         |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}         |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}         |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}         |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=false} |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}         |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}         |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}         |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.null=}         |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}         |
        +----------------------------------------------------------------------------------------------+----------------------------+
        ");
    }
}
