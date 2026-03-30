use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::ResourceFamily;
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, EncodingName, RdfFusionEncodings,
    TermEncoding, detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// Returns the datatype of a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - DATATYPE](https://www.w3.org/TR/sparql11-query/#func-datatype)
pub fn datatype_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(DatatypeSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct DatatypeSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for DatatypeSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatatypeSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl DatatypeSparqlOp {
    /// Create a new [`DatatypeSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_supported_encoding(encodings.plain_term().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::Datatype.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DatatypeSparqlOp {
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
            Some(EncodingName::PlainTerm) => {
                Ok(self.encodings.plain_term().data_type().clone())
            }
            _ => datafusion::common::exec_err!(
                "DATATYPE is only supported for TypedFamily and PlainTerm encoding"
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArrays::TypedFamily(tf_args)) => {
                let array = tf_args.get(0);
                let iris = ResourceFamily::create_named_nodes_array(
                    array.literal_data_types()?,
                )?;

                self.encodings
                    .typed_family()
                    .create_array_from_family(iris)?
                    .into_array_ref()
            }
            Some(DowncastEncodingArrays::PlainTerm(pt_args)) => {
                let array = pt_args.get(0);
                let parts = array.as_parts();
                let new_values = parts.data_type.clone();
                self.encodings
                    .plain_term()
                    .create_named_nodes_array(new_values)
            }
            _ => exec_err!(
                "DATATYPE is only supported for TypedFamily and PlainTerm encoding"
            )?,
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
    async fn test_datatype_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(datatype_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------+
        | input                                                                                        | DATATYPE(?table?.input)                                                                   |
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                        |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                                                        |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                                                        |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                                                        |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.resources={named_node=http://www.w3.org/2001/XMLSchema#integer}}              |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.resources={named_node=http://www.w3.org/2001/XMLSchema#float}}                |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.resources={named_node=http://www.w3.org/2001/XMLSchema#float}}                |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.resources={named_node=http://www.w3.org/2001/XMLSchema#double}}               |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.resources={named_node=http://www.w3.org/2001/XMLSchema#decimal}}              |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.resources={named_node=http://www.w3.org/2001/XMLSchema#int}}                  |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.resources={named_node=http://www.w3.org/2001/XMLSchema#string}}               |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.resources={named_node=http://www.w3.org/2001/XMLSchema#string}}               |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.resources={named_node=http://www.w3.org/1999/02/22-rdf-syntax-ns#langString}} |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.resources={named_node=http://www.w3.org/2001/XMLSchema#string}}               |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.resources={named_node=http://www.w3.org/2001/XMLSchema#dateTime}}             |
        +----------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------+
        "
        )
    }
}
