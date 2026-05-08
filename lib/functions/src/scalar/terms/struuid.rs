use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_family::{StringFamilyArray, TypedFamilyEncodingRef};
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use uuid::Uuid;

/// Implementation of the SPARQL `STRUUID` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STRUUID](https://www.w3.org/TR/sparql11-query/#func-struuid)
pub fn struuid_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StrUuidSparqlOp::new(Arc::clone(
        encodings.typed_family(),
    ))))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct StrUuidSparqlOp {
    encoding: TypedFamilyEncodingRef,
    name: String,
    signature: Signature,
}

impl Debug for StrUuidSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrUuidSparqlOp")
            .field("encoding", &self.encoding)
            .finish()
    }
}

impl StrUuidSparqlOp {
    /// Create a new [`StrUuidSparqlOp`].
    fn new(encoding: TypedFamilyEncodingRef) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encoding.as_ref())
            .with_nullary_arity()
            .build();
        Self {
            encoding,
            name: BuiltinName::StrUuid.to_string(),
            signature: Signature::new(type_signature, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for StrUuidSparqlOp {
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
        let uuids = (0..num_rows).map(|_| Uuid::new_v4().to_string());
        let values_array =
            StringFamilyArray::new_simple(StringArray::from_iter_values(uuids));
        let result = self.encoding.create_array_from_family(values_array)?;
        Ok(ColumnarValue::Array(result.into_array_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_default_encodings, create_standard_test_vector,
        evaluate_function_with_args_for_test,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_struuid_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(struuid_udf(encodings).unwrap());
        let result = evaluate_function_with_args_for_test(test_vector, udf, vec![]);
        let result_str = result.to_string().await.unwrap();
        assert!(result_str.contains("rdf-fusion.strings={value: "));
    }
}
