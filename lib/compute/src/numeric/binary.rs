use crate::numeric::cast_numeric;
use datafusion::arrow::array::{Datum, Scalar};
use datafusion::arrow::compute::kernels::numeric::{add, div, mul, sub};
use datafusion::arrow::datatypes::DataType;
use rdf_fusion_encoding::typed_family::{
    FamilyArray, FamilyDatum, FamilyDatumExt, NumericFamily, NumericFamilyArray,
    NumericFamilyArrayElementBuilder,
};
use rdf_fusion_model::{Decimal, Numeric, ThinResult};
use std::cmp::max;

/// Adds together two [`NumericFamilyArray`]s.
pub fn add_numeric_family(
    lhs: &dyn FamilyDatum<NumericFamilyArray>,
    rhs: &dyn FamilyDatum<NumericFamilyArray>,
) -> NumericFamilyArray {
    apply_numeric_binary(lhs, rhs, NumericBinaryOp::Add)
}

/// An operation that can be executed by [`apply_numeric_binary`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NumericBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Applies the given operation to the two input arrays.
pub fn apply_numeric_binary(
    lhs: &dyn FamilyDatum<NumericFamilyArray>,
    rhs: &dyn FamilyDatum<NumericFamilyArray>,
    op: NumericBinaryOp,
) -> NumericFamilyArray {
    let (lhs_is_scalar, lhs_array) = lhs.get();
    let (rhs_is_scalar, rhs_array) = rhs.get();

    if let Some(cast_target) = try_detect_fast_path(lhs_array, rhs_array) {
        let target_data_type = match cast_target {
            NumericFamily::FLOAT_TYPE_ID => DataType::Float32,
            NumericFamily::DOUBLE_TYPE_ID => DataType::Float64,
            NumericFamily::DECIMAL_TYPE_ID => {
                DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE)
            }
            NumericFamily::INT_TYPE_ID => DataType::Int32,
            NumericFamily::INTEGER_TYPE_ID => DataType::Int64,
            _ => unreachable!("Invalid target cast type"),
        };

        // Cast both arrays to the target homogenous type
        let lhs_casted = cast_numeric(lhs, &target_data_type).expect("Valid LHS cast");
        let rhs_casted = cast_numeric(rhs, &target_data_type).expect("Valid RHS cast");

        let result_array = match (lhs_is_scalar, rhs_is_scalar) {
            (false, false) => {
                try_apply_numeric_binary_fast_path(&lhs_casted, &rhs_casted, op)
            }
            (false, true) => try_apply_numeric_binary_fast_path(
                &lhs_casted,
                &Scalar::new(rhs_casted),
                op,
            ),
            (true, false) => try_apply_numeric_binary_fast_path(
                &Scalar::new(lhs_casted),
                &rhs_casted,
                op,
            ),
            (true, true) => try_apply_numeric_binary_fast_path(
                &Scalar::new(lhs_casted),
                &Scalar::new(rhs_casted),
                op,
            ),
        };

        return match result_array {
            // An overflow occurred, fall back to the slow path
            None => apply_numeric_binary_slow_path(lhs, rhs, op),
            Some(result_array) => result_array,
        };
    };

    return apply_numeric_binary_slow_path(lhs, rhs, op);

    /// Detects, for each row, what the output type of the operation is.
    fn try_detect_fast_path(
        lhs_type: &NumericFamilyArray,
        rhs_type: &NumericFamilyArray,
    ) -> Option<i8> {
        let lhs_type = lhs_type.try_get_homogenous_type_id_for_fast_path()?;
        let rhs_type = rhs_type.try_get_homogenous_type_id_for_fast_path()?;

        let result =
            match (lhs_type, rhs_type) {
                (NumericFamily::DOUBLE_TYPE_ID, _)
                | (_, NumericFamily::DOUBLE_TYPE_ID) => NumericFamily::DOUBLE_TYPE_ID,
                (NumericFamily::FLOAT_TYPE_ID, _) | (_, NumericFamily::FLOAT_TYPE_ID) => {
                    NumericFamily::FLOAT_TYPE_ID
                }
                (NumericFamily::DECIMAL_TYPE_ID, _)
                | (_, NumericFamily::DECIMAL_TYPE_ID) => NumericFamily::DECIMAL_TYPE_ID,
                (NumericFamily::INTEGER_TYPE_ID, _)
                | (_, NumericFamily::INTEGER_TYPE_ID) => NumericFamily::INTEGER_TYPE_ID,
                (NumericFamily::INT_TYPE_ID, _) | (_, NumericFamily::INT_TYPE_ID) => {
                    NumericFamily::INT_TYPE_ID
                }
                _ => panic!("Invalid numeric array combination: {lhs_type} {rhs_type}",),
            };
        Some(result)
    }

    /// Tries to calculate the result of the operation using the fast path.
    fn try_apply_numeric_binary_fast_path(
        lhs: &dyn Datum,
        rhs: &dyn Datum,
        op: NumericBinaryOp,
    ) -> Option<NumericFamilyArray> {
        let result_array = match op {
            NumericBinaryOp::Add => add(lhs, rhs),
            NumericBinaryOp::Sub => sub(lhs, rhs),
            NumericBinaryOp::Mul => mul(lhs, rhs),
            NumericBinaryOp::Div => div(lhs, rhs),
        }
        .ok()?;

        let result = NumericFamilyArray::try_from_primitive(result_array)
            .expect("Valid primitive array conversion");
        Some(result)
    }

    /// Uses [`apply_binary_row_wise`] for the operation.
    fn apply_numeric_binary_slow_path(
        lhs: &dyn FamilyDatum<NumericFamilyArray>,
        rhs: &dyn FamilyDatum<NumericFamilyArray>,
        op: NumericBinaryOp,
    ) -> NumericFamilyArray {
        match op {
            NumericBinaryOp::Add => {
                apply_binary_row_wise(lhs, rhs, |lhs, rhs| lhs.checked_add(rhs))
            }
            NumericBinaryOp::Sub => {
                apply_binary_row_wise(lhs, rhs, |lhs, rhs| lhs.checked_sub(rhs))
            }
            NumericBinaryOp::Mul => {
                apply_binary_row_wise(lhs, rhs, |lhs, rhs| lhs.checked_mul(rhs))
            }
            NumericBinaryOp::Div => {
                apply_binary_row_wise(lhs, rhs, |lhs, rhs| lhs.div(rhs))
            }
        }
    }
}

/// Implements the binary operation row-wise on two arrays.
fn apply_binary_row_wise<F>(
    lhs: &dyn FamilyDatum<NumericFamilyArray>,
    rhs: &dyn FamilyDatum<NumericFamilyArray>,
    f: F,
) -> NumericFamilyArray
where
    F: Fn(Numeric, Numeric) -> ThinResult<Numeric>,
{
    let (_, lhs_array) = lhs.get();
    let (_, rhs_array) = rhs.get();

    let result_len = max(lhs_array.len(), rhs_array.len());
    let lhs_is_null = lhs_array.null_buffer();
    let rhs_is_null = rhs_array.null_buffer();
    let mut builder = NumericFamilyArrayElementBuilder::with_capacity(result_len);

    let lhs_indices = lhs
        .indices_of_length(result_len)
        .expect("Valid number of rows if both regular arrays have the same length");
    let rhs_indices = rhs
        .indices_of_length(result_len)
        .expect("Valid number of rows if both regular arrays have the same length");

    for (lhs_index, rhs_index) in lhs_indices.into_iter().zip(rhs_indices.into_iter()) {
        if lhs_is_null.is_null(lhs_index) || rhs_is_null.is_null(rhs_index) {
            builder.append_null();
        } else {
            let res = f(
                lhs_array.get_numeric(lhs_index),
                rhs_array.get_numeric(rhs_index),
            );
            match res {
                Ok(v) => builder.append_numeric(v),
                Err(_) => builder.append_null(),
            }
        }
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float32Array, Int32Array};
    use rdf_fusion_model::Numeric;

    #[test]
    fn test_add_fast_path_homogenous() {
        // Fast path: Int32 + Int32
        let lhs =
            NumericFamilyArray::new_ints(Int32Array::from(vec![Some(1), Some(2), None]));
        let rhs = NumericFamilyArray::new_ints(Int32Array::from(vec![
            Some(10),
            None,
            Some(30),
        ]));

        let result = add_numeric_family(&lhs, &rhs);

        assert_eq!(result.len(), 3);
        assert_eq!(result.get_numeric_opt(0), Some(Numeric::Int(11.into())));
        assert_eq!(result.get_numeric_opt(1), None);
        assert_eq!(result.get_numeric_opt(2), None);
    }

    #[test]
    fn test_add_fast_path_type_promotion() {
        // Fast path with promotion: Int32 + Float32 -> Float32
        let lhs = NumericFamilyArray::new_ints(Int32Array::from(vec![1, 2, 3]));
        let rhs = NumericFamilyArray::new_floats(Float32Array::from(vec![1.5, 2.5, 3.5]));

        let result = apply_numeric_binary(&lhs, &rhs, NumericBinaryOp::Add);

        assert_eq!(result.len(), 3);
        assert_eq!(result.get_numeric_opt(0), Some(Numeric::Float(2.5.into())));
        assert_eq!(result.get_numeric_opt(1), Some(Numeric::Float(4.5.into())));
        assert_eq!(result.get_numeric_opt(2), Some(Numeric::Float(6.5.into())));
    }

    #[test]
    fn test_sub_fast_path_homogenous() {
        // Fast path: Float32 - Float32
        let lhs = NumericFamilyArray::new_floats(Float32Array::from(vec![10.0, 20.0]));
        let rhs = NumericFamilyArray::new_floats(Float32Array::from(vec![3.0, 5.5]));

        let result = apply_numeric_binary(&lhs, &rhs, NumericBinaryOp::Sub);

        assert_eq!(result.len(), 2);
        assert_eq!(result.get_numeric_opt(0), Some(Numeric::Float(7.0.into())));
        assert_eq!(result.get_numeric_opt(1), Some(Numeric::Float(14.5.into())));
    }

    #[test]
    fn test_add_slow_path_mixed_union() {
        let mut lhs_builder = NumericFamilyArrayElementBuilder::with_capacity(2);
        lhs_builder.append_numeric(Numeric::Int(5.into()));
        lhs_builder.append_numeric(Numeric::Float(10.5.into()));
        let lhs = lhs_builder.finish();

        let mut rhs_builder = NumericFamilyArrayElementBuilder::with_capacity(2);
        rhs_builder.append_numeric(Numeric::Int(10.into()));
        rhs_builder.append_numeric(Numeric::Float(20.0.into()));
        let rhs = rhs_builder.finish();

        let result = apply_numeric_binary(&lhs, &rhs, NumericBinaryOp::Add);

        assert_eq!(result.len(), 2);
        // Row 1: Int + Int
        assert_eq!(result.get_numeric_opt(0), Some(Numeric::Int(15.into())));
        // Row 2: Float + Float
        assert_eq!(result.get_numeric_opt(1), Some(Numeric::Float(30.5.into())));
    }

    #[test]
    fn test_slow_path_null_handling() {
        let mut lhs_builder = NumericFamilyArrayElementBuilder::with_capacity(2);
        lhs_builder.append_numeric(Numeric::Int(5.into()));
        lhs_builder.append_null(); // Null Float
        let lhs = lhs_builder.finish();

        let mut rhs_builder = NumericFamilyArrayElementBuilder::with_capacity(2);
        rhs_builder.append_null(); // Null Int
        rhs_builder.append_numeric(Numeric::Float(20.0.into()));
        let rhs = rhs_builder.finish();

        let result = apply_numeric_binary(&lhs, &rhs, NumericBinaryOp::Sub);

        assert_eq!(result.len(), 2);
        // Row 0: 5 - Null -> Null
        assert_eq!(result.get_numeric_opt(0), None);
        // Row 1: Null - 20.0 -> Null
        assert_eq!(result.get_numeric_opt(1), None);
    }
}
