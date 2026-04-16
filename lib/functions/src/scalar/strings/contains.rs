use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use arrow_string::like::contains;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::{
    BooleanFamilyArray, DowncastTypedFamilyArray, TypedFamilyChild,
};
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// Returns true if the first literal contains the second literal.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - CONTAINS](https://www.w3.org/TR/sparql11-query/#func-contains)
pub fn contains_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(ContainsSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ContainsSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for ContainsSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContainsSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl ContainsSparqlOp {
    /// Create a new [`ContainsSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_binary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::Contains.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ContainsSparqlOp {
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
            _ => exec_err!("Unsupported encoding for CONTAINS return type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args_wrapped =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let tf_args = match args_wrapped.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => tf_args.clone(),
            _ => exec_err!(
                "CONTAINS is only supported for TypedFamily or PlainTerm encoding"
            )?,
        };

        let result = tf_args
            .map_children_tf_binary(
                |lhs: TypedFamilyChild, rhs: TypedFamilyChild| match (
                    lhs.as_downcast_array(),
                    rhs.as_downcast_array(),
                ) {
                    (
                        DowncastTypedFamilyArray::String(l),
                        DowncastTypedFamilyArray::String(r),
                    ) => {
                        let res = l.apply_binary_boolean(&r, |a, b| contains(a, b))?;
                        tf_encoding.create_array_from_family(BooleanFamilyArray::new(res))
                    }
                    _ => tf_encoding.create_null_array(lhs.to_array().len()),
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
        create_default_encodings, evaluate_binary_function_for_test,
    };
    use insta::assert_snapshot;
    use rdf_fusion_encoding::typed_family::{StringFamily, TypedFamily};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_contains_typed_family() {
        let encodings = create_default_encodings();

        let left_values = vec!["foobar", "foobar", "foobar", "foobar", "foobar"];
        let left_langs = vec![None, Some("en"), Some("en"), Some("de"), None];
        let strings_array = StringFamily::create_strings_array(
            Arc::new(datafusion::arrow::array::StringArray::from(left_values)),
            Arc::new(datafusion::arrow::array::StringArray::from(left_langs)),
        );
        let left = encodings
            .typed_family()
            .create_array_with_single_family(StringFamily::FAMILY_ID, strings_array)
            .unwrap()
            .into_array_ref();

        let right_values = vec!["oob", "oob", "oob", "oob", "oob"];
        let right_langs = vec![None, None, Some("en"), Some("en"), Some("en")];
        let search_array = StringFamily::create_strings_array(
            Arc::new(datafusion::arrow::array::StringArray::from(right_values)),
            Arc::new(datafusion::arrow::array::StringArray::from(right_langs)),
        );
        let right = encodings
            .typed_family()
            .create_array_with_single_family(StringFamily::FAMILY_ID, search_array)
            .unwrap()
            .into_array_ref();

        let udf = Arc::new(contains_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------+-------------------------------------------------+--------------------------------------+
        | left                                               | right                                           | CONTAINS(?table?.left,?table?.right) |
        +----------------------------------------------------+-------------------------------------------------+--------------------------------------+
        | {rdf-fusion.strings={value: foobar, language: }}   | {rdf-fusion.strings={value: oob, language: }}   | {rdf-fusion.boolean=true}            |
        | {rdf-fusion.strings={value: foobar, language: en}} | {rdf-fusion.strings={value: oob, language: }}   | {rdf-fusion.boolean=true}            |
        | {rdf-fusion.strings={value: foobar, language: en}} | {rdf-fusion.strings={value: oob, language: en}} | {rdf-fusion.boolean=true}            |
        | {rdf-fusion.strings={value: foobar, language: de}} | {rdf-fusion.strings={value: oob, language: en}} | {rdf-fusion.null=}                   |
        | {rdf-fusion.strings={value: foobar, language: }}   | {rdf-fusion.strings={value: oob, language: en}} | {rdf-fusion.boolean=true}            |
        +----------------------------------------------------+-------------------------------------------------+--------------------------------------+
        "
        );
    }
}
