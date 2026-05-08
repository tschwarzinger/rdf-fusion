use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, StringFamily, TypedFamily,
};
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// Returns the lower-case of a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - LCASE](https://www.w3.org/TR/sparql11-query/#func-lcase)
pub fn lcase_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StringCaseSparqlUDF::new(
        encodings,
        BuiltinName::LCase.to_string(),
        StringCaseOp::Lower,
    )))
}

/// Returns the upper-case of a literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - LCASE](https://www.w3.org/TR/sparql11-query/#func-ucase)
pub fn ucase_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StringCaseSparqlUDF::new(
        encodings,
        BuiltinName::UCase.to_string(),
        StringCaseOp::Upper,
    )))
}

/// Defines the specific casing operation to apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum StringCaseOp {
    Lower,
    Upper,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct StringCaseSparqlUDF {
    encodings: RdfFusionEncodings,
    name: String,
    op: StringCaseOp,
    signature: Signature,
}

impl Debug for StringCaseSparqlUDF {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringCaseSparqlUdf")
            .field("name", &self.name)
            .field("op", &self.op)
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl StringCaseSparqlUDF {
    /// Create a new [`StringCaseSparqlUDF`].
    pub fn new(encodings: RdfFusionEncodings, name: String, op: StringCaseOp) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name,
            op,
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StringCaseSparqlUDF {
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
        let encoding_name = self
            .encodings
            .try_get_encoding_name(&arg_types[0])
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    "Unsupported input encoding".to_string(),
                )
            })?;

        match encoding_name {
            EncodingName::TypedFamily | EncodingName::PlainTerm => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            _ => exec_err!("Unsupported encoding for LCASE/UCASE"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        let tf_args = match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => tf_args.clone(),
            _ => exec_err!("String casing is only supported for TypedFamily encoding")?,
        };

        let result = tf_args
            .map_children_tf_unary(
                |child: rdf_fusion_encoding::typed_family::TypedFamilyChild| match child
                    .as_downcast_array()
                {
                    DowncastTypedFamilyArray::String(array) => {
                        let string_array = match self.op {
                            StringCaseOp::Lower => {
                                array.apply_unary(|v| v.to_lowercase())
                            }
                            StringCaseOp::Upper => {
                                array.apply_unary(|v| v.to_uppercase())
                            }
                        };
                        self.encodings
                            .typed_family()
                            .create_array_with_single_family(
                                StringFamily::FAMILY_ID,
                                string_array,
                            )
                    }
                    _ => self
                        .encodings
                        .typed_family()
                        .create_null_array(child.to_array().len()),
                },
            )?
            .into_array_ref();

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
    async fn test_lcase_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(lcase_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+---------------------------------------------------------+
        | input                                                                                        | LCASE(?table?.input)                                    |
        +----------------------------------------------------------------------------------------------+---------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                      |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                      |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                      |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                                      |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}            |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: just a string, language: }} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: en}}       |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 123, language: }}           |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                      |
        +----------------------------------------------------------------------------------------------+---------------------------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_ucase_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(ucase_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+---------------------------------------------------------+
        | input                                                                                        | UCASE(?table?.input)                                    |
        +----------------------------------------------------------------------------------------------+---------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                      |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                      |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                      |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                                      |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                                      |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: B1, language: }}            |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: JUST A STRING, language: }} |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: HELLO, language: en}}       |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 123, language: }}           |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                      |
        +----------------------------------------------------------------------------------------------+---------------------------------------------------------+
        "
        );
    }
}
