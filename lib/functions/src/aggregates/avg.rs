use crate::aggregates::SparqlSumAccumulator;
use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::compute::sum;
use datafusion::arrow::datatypes::{DataType, UInt64Type};
use datafusion::arrow::error::ArrowError;
use datafusion::logical_expr::{AggregateUDF, Volatility, create_udaf};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, NumericFamily, NumericFamilyScalar, TypedFamilyEncodingRef,
};
use rdf_fusion_encoding::{EncodingScalar, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::sync::Arc;

pub fn avg_typed_family(encoding: TypedFamilyEncodingRef) -> AggregateUDF {
    let data_type = encoding.data_type().clone();
    create_udaf(
        &BuiltinName::Avg.to_string(),
        vec![data_type.clone()],
        Arc::new(data_type.clone()),
        Volatility::Immutable,
        Arc::new(move |_| Ok(Box::new(SparqlTypedFamilyAvg::new(Arc::clone(&encoding))))),
        Arc::new(vec![data_type, DataType::UInt64]),
    )
}

#[derive(Debug)]
struct SparqlTypedFamilyAvg {
    encoding: TypedFamilyEncodingRef,
    sum_acc: SparqlSumAccumulator,
    count: u64,
}

impl SparqlTypedFamilyAvg {
    /// Creates a new [`SparqlTypedFamilyAvg`].
    pub fn new(encoding: TypedFamilyEncodingRef) -> Self {
        let sum_acc = SparqlSumAccumulator::new(Arc::clone(&encoding));
        SparqlTypedFamilyAvg {
            encoding,
            sum_acc,
            count: 0,
        }
    }
}

impl Accumulator for SparqlTypedFamilyAvg {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.sum_acc.update_batch(values)?;

        let arr = self.encoding.try_new_array(Arc::clone(&values[0]))?;
        for child in arr.non_empty_children() {
            if let DowncastTypedFamilyArray::Numeric(numeric_child) = child.downcast() {
                let len = numeric_child.len().try_into().map_err(|_| {
                    ArrowError::ArithmeticOverflow("Child length too big".to_owned())
                })?;
                self.count = self.count.checked_add(len).ok_or_else(|| {
                    ArrowError::ArithmeticOverflow("Average count overflow".to_owned())
                })?;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.count == 0 {
            return self
                .encoding
                .create_scalar_from_family::<NumericFamily>(
                    NumericFamilyScalar::Integer(0.into()).to_scalar_value(),
                )
                .map(|v| v.into_scalar_value());
        }

        let count = i64::try_from(self.count).map_err(|_| {
            ArrowError::ArithmeticOverflow("Average count overflow".to_string())
        })?;
        let avg = self
            .sum_acc
            .sum()
            .and_then(|sum| sum.div(NumericFamilyScalar::Integer(count.into())));

        match avg {
            Ok(scalar) => Ok(self
                .encoding
                .create_scalar_from_family::<NumericFamily>(scalar.to_scalar_value())?
                .into_scalar_value()),
            Err(_) => Ok(self.encoding.create_scalar_null().into_scalar_value()),
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let mut state = self.sum_acc.state()?;
        state.push(ScalarValue::UInt64(Some(self.count)));
        Ok(state)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.sum_acc.merge_batch(&states[0..states.len() - 1])?;

        let counts = states[states.len() - 1].as_primitive::<UInt64Type>();
        let count = sum(counts).ok_or_else(|| {
            ArrowError::ArithmeticOverflow("Average count overflow".to_string())
        })?;
        self.count += count;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::evaluate_aggregate_with_args_for_test;
    use datafusion::arrow::array::Int64Array;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::typed_family::{
        NumericFamilyArray, TypedFamilyArray, TypedFamilyEncoding,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_avg_typed_family() -> Result<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let values = NumericFamilyArray::new_integers(Int64Array::from(vec![1, 2, 3]));
        let typed_array = encoding.create_array_from_family(values)?;

        assert_snapshot!(
            run_test(typed_array).await,
            @"
        +-----------------------------------------------------+
        | AVG(?table?.a)                                      |
        +-----------------------------------------------------+
        | {rdf-fusion.numeric={decimal=2.000000000000000000}} |
        +-----------------------------------------------------+");

        Ok(())
    }

    #[tokio::test]
    async fn test_avg_typed_family_with_null() -> Result<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let values = Int64Array::from(vec![Some(1), None, Some(2)]);
        let typed_array = encoding
            .create_array_from_family(NumericFamilyArray::new_integers(values))?;

        assert_snapshot!(
            run_test(typed_array).await,
            @"
        +-----------------------------------------------------+
        | AVG(?table?.a)                                      |
        +-----------------------------------------------------+
        | {rdf-fusion.numeric={decimal=1.500000000000000000}} |
        +-----------------------------------------------------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_avg_typed_family_promotion() -> Result<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let numeric_array =
            NumericFamilyArray::new_integers(Int64Array::from(vec![10, 20]));
        let typed_array = encoding.create_array_from_family(numeric_array)?;

        assert_snapshot!(
            run_test(typed_array).await,
            @"
        +------------------------------------------------------+
        | AVG(?table?.a)                                       |
        +------------------------------------------------------+
        | {rdf-fusion.numeric={decimal=15.000000000000000000}} |
        +------------------------------------------------------+
        ");

        Ok(())
    }

    /// Executes a test and returns the pretty-printed result.
    async fn run_test(typed_array: TypedFamilyArray) -> String {
        let encoding = Arc::clone(typed_array.encoding());
        let df = evaluate_aggregate_with_args_for_test(
            typed_array.into_array_ref(),
            Arc::new(avg_typed_family(encoding)),
            vec![col("a")],
        );
        df.to_string().await.unwrap()
    }
}
