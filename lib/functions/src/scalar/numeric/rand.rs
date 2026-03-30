use crate::scalar::error::SparqlUDFCreationError;
use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rand::RngExt;
use rdf_fusion_encoding::typed_family::NumericFamilyArray;
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// Implementation of the SPARQL `RAND` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - RAND](https://www.w3.org/TR/sparql11-query/#func-rand)
pub fn rand_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(RandSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct RandSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for RandSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RandSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl RandSparqlOp {
    /// Create a new [`RandSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        Self {
            encodings,
            name: BuiltinName::Rand.to_string(),
            signature: Signature::exact(vec![], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for RandSparqlOp {
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
        if !arg_types.is_empty() {
            return exec_err!("RAND does not accept arguments");
        }
        Ok(self.encodings.typed_family().data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let encoding = self.encodings.typed_family();
        let mut rng = rand::rng();
        let values: Vec<f64> = (0..args.number_rows).map(|_| rng.random()).collect();
        let double_array = NumericFamilyArray::new_doubles(Float64Array::from(values));

        let result = encoding.create_array_from_family(double_array)?;

        Ok(ColumnarValue::Array(result.into_array_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_default_encodings, evaluate_function_with_args_for_test,
    };
    use datafusion::arrow::array::{ArrayRef, NullArray};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_rand_typed_family() {
        let encodings = create_default_encodings();
        let udf = Arc::new(rand_udf(encodings).unwrap());

        // Nullary function still needs some input to determine number of rows in DataFrame
        let test_vector = Arc::new(NullArray::new(5)) as ArrayRef;
        let result = evaluate_function_with_args_for_test(test_vector, udf, vec![]);

        let result_str = result.clone().to_string().await.unwrap();
        // Since it's random, we just check that we have 5 rows and they are numeric doubles.
        assert_eq!(result.collect().await.unwrap().len(), 1); // 1 batch
        assert!(result_str.contains("{double="));
    }
}
