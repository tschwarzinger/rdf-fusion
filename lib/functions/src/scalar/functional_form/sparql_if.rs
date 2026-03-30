use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, new_null_array};
use datafusion::arrow::compute::interleave;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::TypedFamilyEncodingRef;
use rdf_fusion_encoding::{RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Implementation of the SPARQL `IF` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - IF](https://www.w3.org/TR/sparql11-query/#func-if)
pub fn sparql_if_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(SparqlIfSparqlOp::new(Arc::clone(
        encodings.typed_family(),
    ))))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct SparqlIfSparqlOp {
    encoding: TypedFamilyEncodingRef,
    name: String,
    signature: Signature,
}

impl Debug for SparqlIfSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparqlIfSparqlOp")
            .field("encoding", &self.encoding)
            .finish()
    }
}

impl SparqlIfSparqlOp {
    /// Create a new [`SparqlIfSparqlOp`].
    fn new(encoding: TypedFamilyEncodingRef) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encoding.as_ref())
            .with_ternary_arity()
            .build();
        Self {
            encoding,
            name: BuiltinName::If.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparqlIfSparqlOp {
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
        let test = self
            .encoding
            .try_new_array(args.args[0].to_array(num_rows)?)?;
        let if_true = args.args[1].to_array(num_rows)?;
        let if_false = args.args[2].to_array(num_rows)?;

        let ebv = test.effective_boolean_value()?;
        let null_array = new_null_array(self.encoding.data_type(), 1);

        let mut indices = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            if ebv.is_null(i) {
                indices.push((0, 0));
            } else if ebv.value(i) {
                indices.push((1, i));
            } else {
                indices.push((2, i));
            }
        }

        let arrays = vec![null_array.as_ref(), if_true.as_ref(), if_false.as_ref()];
        let result = interleave(&arrays, &indices)?;
        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_default_encodings, create_standard_test_vector, evaluate_function,
    };
    use datafusion::arrow::array::{RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::typed_family::StringFamilyArray;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_if_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());

        let true_val = encodings
            .typed_family()
            .create_array_from_family(StringFamilyArray::new_simple(
                StringArray::new_repeated("TRUE", test_vector.len()),
            ))
            .unwrap();
        let false_val = encodings
            .typed_family()
            .create_array_from_family(StringFamilyArray::new_simple(
                StringArray::new_repeated("FALSE", test_vector.len()),
            ))
            .unwrap();

        let args = vec![
            test_vector.clone(),
            true_val.into_array_ref(),
            false_val.into_array_ref(),
        ];

        let arg_fields = vec![
            Arc::new(Field::new(
                "test",
                encodings.typed_family().data_type().clone(),
                true,
            )),
            Arc::new(Field::new(
                "if_true",
                encodings.typed_family().data_type().clone(),
                true,
            )),
            Arc::new(Field::new(
                "if_false",
                encodings.typed_family().data_type().clone(),
                true,
            )),
        ];
        let input_schema = Schema::new(arg_fields);
        let input = RecordBatch::try_new(Arc::new(input_schema), args).unwrap();

        let encodings = create_default_encodings();
        let udf = Arc::new(sparql_if_udf(encodings).unwrap());
        let result = evaluate_function(
            input,
            udf,
            vec![col("test"), col("if_true"), col("if_false")],
        );

        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+------------------------------------------------+-------------------------------------------------+---------------------------------------------------+
        | test                                                                                         | if_true                                        | if_false                                        | IF(?table?.test,?table?.if_true,?table?.if_false) |
        +----------------------------------------------------------------------------------------------+------------------------------------------------+-------------------------------------------------+---------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.null=}                                |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.null=}                                |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.null=}                                |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.null=}                                |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.strings={value: TRUE, language: }}    |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.strings={value: TRUE, language: }}    |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.strings={value: FALSE, language: }}   |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.strings={value: TRUE, language: }}    |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.strings={value: TRUE, language: }}    |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.strings={value: TRUE, language: }}    |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.strings={value: TRUE, language: }}    |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.strings={value: TRUE, language: }}    |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.null=}                                |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.strings={value: TRUE, language: }}    |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.strings={value: TRUE, language: }} | {rdf-fusion.strings={value: FALSE, language: }} | {rdf-fusion.null=}                                |
        +----------------------------------------------------------------------------------------------+------------------------------------------------+-------------------------------------------------+---------------------------------------------------+
        "
        );
    }
}
