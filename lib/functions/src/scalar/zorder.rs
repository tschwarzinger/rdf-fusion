use datafusion::arrow::array::{Array, ArrayRef, AsArray, GenericBinaryBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

pub fn zorder_udf(encodings: RdfFusionEncodings) -> ScalarUDF {
    ScalarUDF::new_from_impl(ZOrderSparqlOp::new(encodings))
}

/// A UDF that computes the Z-Order curve value for multiple RDF terms.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ZOrderSparqlOp {
    name: String,
    signature: Signature,
    encodings: RdfFusionEncodings,
}

impl ZOrderSparqlOp {
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        Self {
            name: BuiltinName::ZOrder.to_string(),
            signature: Signature::variadic(
                vec![
                    encodings.typed_family().data_type().clone(),
                    DataType::Binary,
                ],
                Volatility::Immutable,
            ),
            encodings,
        }
    }
}

impl ScalarUDFImpl for ZOrderSparqlOp {
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
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.is_empty() {
            return plan_err!("ZOrder requires at least one argument");
        }

        let num_rows = args.number_rows;
        let mut sortable_byte_arrays = Vec::with_capacity(args.args.len());

        for arg in &args.args {
            let array = arg.to_array(num_rows)?;
            let data_type = array.data_type();

            if data_type == self.encodings.typed_family().data_type() {
                let tf_encoding = self.encodings.typed_family();
                let tf_array = tf_encoding.try_new_array(array)?;
                sortable_byte_arrays.push(tf_array.as_sortable_bytes()?);
            } else if let DataType::Binary = data_type {
                sortable_byte_arrays.push(array.as_binary::<i32>().clone());
            } else {
                return exec_err!(
                    "ZOrder only supports TypedFamily or Binary encoding, got {:?}",
                    data_type
                );
            }
        }

        let mut builder = GenericBinaryBuilder::<i32>::new();
        for i in 0..num_rows {
            let inputs: Vec<&[u8]> =
                sortable_byte_arrays.iter().map(|a| a.value(i)).collect();
            let interleaved = interleave_bits(&inputs);
            builder.append_value(&interleaved);
        }

        let result_array = Arc::new(builder.finish()) as ArrayRef;

        // Handle scalar result if all inputs were scalars
        let all_scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        if all_scalar {
            let scalar_value =
                datafusion::common::ScalarValue::try_from_array(&result_array, 0)?;
            Ok(ColumnarValue::Scalar(scalar_value))
        } else {
            Ok(ColumnarValue::Array(result_array))
        }
    }
}

fn interleave_bits(inputs: &[&[u8]]) -> Vec<u8> {
    let n = inputs.len();
    if n == 0 {
        return Vec::new();
    }
    let max_len = inputs.iter().map(|b| b.len()).max().unwrap_or(0);
    let mut result = vec![0u8; max_len * n];

    for i in 0..(max_len * 8) {
        let byte_idx = i / 8;
        let bit_in_byte_idx = 7 - (i % 8);
        for (j, input) in inputs.iter().enumerate().take(n) {
            let bit = if byte_idx < input.len() {
                (input[byte_idx] >> bit_in_byte_idx) & 1
            } else {
                0
            };

            let total_bit_idx = i * n + j;
            let res_byte_idx = total_bit_idx / 8;
            let res_bit_in_byte_idx = 7 - (total_bit_idx % 8);
            result[res_byte_idx] |= bit << res_bit_in_byte_idx;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interleave_bits_simple() {
        let a = [0xAAu8];
        let b = [0xCCu8];
        let result = interleave_bits(&[&a, &b]);
        assert_eq!(result, vec![0xD8, 0xD8]);
    }

    #[test]
    fn test_interleave_bits_different_lengths() {
        let a = [0xFFu8];
        let b = [0x00u8, 0xFFu8];
        let result = interleave_bits(&[&a, &b]);
        assert_eq!(result, vec![0xAA, 0xAA, 0x55, 0x55]);
    }
}
