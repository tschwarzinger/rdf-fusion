use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, ResourceFamily, TypedFamilyEncodingRef,
};
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::{BlankNode, BlankNodeRef, DFResult};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Implementation of the SPARQL `BNODE` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - BNODE](https://www.w3.org/TR/sparql11-query/#func-bnode)
pub fn bnode_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(BNodeSparqlOp::new(Arc::clone(
        encodings.typed_family(),
    ))))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct BNodeSparqlOp {
    encoding: TypedFamilyEncodingRef,
    name: String,
    signature: Signature,
}

impl Debug for BNodeSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BNodeSparqlOp")
            .field("encoding", &self.encoding)
            .finish()
    }
}

impl BNodeSparqlOp {
    /// Create a new [`BNodeSparqlOp`].
    fn new(encoding: TypedFamilyEncodingRef) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encoding.as_ref())
            .with_nullary_arity()
            .with_unary_arity()
            .build();
        Self {
            encoding,
            name: BuiltinName::BNode.to_string(),
            signature: Signature::new(type_signature, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for BNodeSparqlOp {
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
        Ok(self.encoding.data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let num_rows = args.number_rows;
        if args.args.is_empty() {
            let bnodes = (0..num_rows).map(|_| BlankNode::default().to_string());
            let bnodes_array = StringArray::from_iter_values(bnodes);
            let resource_array = ResourceFamily::create_blank_nodes_array(bnodes_array)?;
            let result = self.encoding.create_array_from_family(resource_array)?;
            return Ok(ColumnarValue::Array(result.into_array_ref()));
        }

        let input_raw = args.args[0].to_array(num_rows)?;
        let input = self.encoding.try_new_array(input_raw)?;

        let result = input.map_unary_tf(|child| match child.downcast() {
            DowncastTypedFamilyArray::String(array) => {
                let values = array.value_array();
                let languages = array.language_array();

                let mut bnodes = StringBuilder::new();
                for i in 0..values.len() {
                    if languages.is_null(i) {
                        let val = values.value(i);
                        if let Ok(b) = BlankNodeRef::new(val) {
                            bnodes.append_value(b.as_str())
                        } else {
                            bnodes.append_null();
                        }
                    } else {
                        bnodes.append_null();
                    }
                }

                let resource_array =
                    ResourceFamily::create_blank_nodes_array(bnodes.finish())?;
                self.encoding.create_array_from_family(resource_array)
            }
            _ => self.encoding.create_null_array(child.array().len()),
        })?;

        Ok(ColumnarValue::Array(result.into_array_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_default_encodings, create_standard_test_vector,
        evaluate_function_for_test, evaluate_function_with_args_for_test,
    };
    use insta::assert_snapshot;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_bnode_unary_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(bnode_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+-----------------------------------------+
        | input                                                                                        | BNODE(?table?.input)                    |
        +----------------------------------------------------------------------------------------------+-----------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                      |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                      |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                      |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                      |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                      |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                      |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                      |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                      |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                      |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                      |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.resources={blank_node=b1}}  |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                      |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                      |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.resources={blank_node=123}} |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                      |
        +----------------------------------------------------------------------------------------------+-----------------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_bnode_nullary_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(bnode_udf(encodings).unwrap());
        let result = evaluate_function_with_args_for_test(
            test_vector,
            udf,
            vec![], // Nullary
        );
        let result_str = result.to_string().await.unwrap();
        // Since BNODE() is volatile, we just check that it produces results.
        assert!(result_str.contains("blank_node"));
    }
}
