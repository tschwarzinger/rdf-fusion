use datafusion::arrow::array::ArrayRef;
use datafusion::logical_expr::{AggregateUDF, Volatility, create_udaf};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, NumericFamily, NumericFamilyScalar, TypedFamilyEncodingRef,
    TypedFamilyScalar,
};
use rdf_fusion_encoding::{EncodingScalar, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::ThinResult;
use rdf_fusion_model::{DFResult, ThinError};
use std::sync::Arc;

pub fn sum_typed_family(encoding: TypedFamilyEncodingRef) -> AggregateUDF {
    let data_type = encoding.data_type().clone();
    create_udaf(
        &BuiltinName::Sum.to_string(),
        vec![data_type.clone()],
        Arc::new(data_type.clone()),
        Volatility::Immutable,
        Arc::new(move |_| Ok(Box::new(SparqlSumAccumulator::new(Arc::clone(&encoding))))),
        Arc::new(vec![data_type.clone()]),
    )
}

#[derive(Debug)]
pub(crate) struct SparqlSumAccumulator {
    encoding: TypedFamilyEncodingRef,
    sum: ThinResult<NumericFamilyScalar>,
}

impl SparqlSumAccumulator {
    /// Creates a new [`SparqlSumAccumulator`].
    pub fn new(encoding: TypedFamilyEncodingRef) -> Self {
        SparqlSumAccumulator {
            encoding,
            sum: Ok(NumericFamilyScalar::Integer(0.into())),
        }
    }

    /// Returns the current sum.
    pub fn sum(&self) -> ThinResult<NumericFamilyScalar> {
        self.sum
    }

    fn evaluate_typed_family(&self) -> DFResult<TypedFamilyScalar> {
        match &self.sum {
            Ok(sum) => self
                .encoding
                .create_scalar_from_family::<NumericFamily>(sum.to_scalar_value()),
            Err(_) => Ok(self.encoding.create_scalar_null()),
        }
    }
}

impl Accumulator for SparqlSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let Ok(old_sum) = &self.sum else {
            return Ok(());
        };
        let old_sum = *old_sum;

        let arr = self.encoding.try_new_array(Arc::clone(&values[0]))?;
        let typed_arrays = arr.non_empty_children();

        for child in typed_arrays {
            match child.downcast() {
                DowncastTypedFamilyArray::Null(_) => continue,
                DowncastTypedFamilyArray::Numeric(numeric_child) => {
                    let new_sum = numeric_child.sum();
                    let new_sum = new_sum.and_then(|sum| old_sum.checked_add(sum));
                    self.sum = new_sum;
                }
                _ => {
                    self.sum = Err(ThinError::ExpectedError);
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.evaluate_typed_family()?.into_scalar_value())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![self.evaluate_typed_family()?.into_scalar_value()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
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
        NumericFamilyArray, NumericFamilyArrayElementBuilder, TypedFamilyArray,
        TypedFamilyEncoding,
    };
    use rdf_fusion_model::Numeric;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_sum_typed_family() -> Result<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let values = NumericFamilyArray::new_integers(Int64Array::from(vec![1, 3, 2]));
        let typed_array = encoding.create_array_from_family(values)?;

        assert_snapshot!(
            run_test(typed_array).await,
            @"
        +----------------------------------+
        | SUM(?table?.a)                   |
        +----------------------------------+
        | {rdf-fusion.numeric={integer=6}} |
        +----------------------------------+");

        Ok(())
    }

    #[tokio::test]
    async fn test_sum_typed_family_with_null() -> Result<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let values = NumericFamilyArray::new_integers(Int64Array::from(vec![
            Some(1),
            None,
            Some(2),
        ]));
        let typed_array = encoding.create_array_from_family(values)?;

        assert_snapshot!(
            run_test(typed_array).await,
            @"
        +----------------------------------+
        | SUM(?table?.a)                   |
        +----------------------------------+
        | {rdf-fusion.numeric={integer=3}} |
        +----------------------------------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_sum_typed_family_promotion() -> Result<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());

        // 10 (integer) + 1.5 (float) -> 11.5 (float)
        let mut builder = NumericFamilyArrayElementBuilder::with_capacity(2);
        builder.append_numeric(Numeric::Integer(10.into()));
        builder.append_numeric(Numeric::Float(1.5.into()));
        let numeric_array = builder.finish();
        let typed_array = encoding.create_array_from_family(numeric_array)?;

        assert_snapshot!(
            run_test(typed_array).await,
            @"
        +-----------------------------------+
        | SUM(?table?.a)                    |
        +-----------------------------------+
        | {rdf-fusion.numeric={float=11.5}} |
        +-----------------------------------+
        ");

        Ok(())
    }

    /// Executes a test and returns the pretty-printed result.
    async fn run_test(typed_array: TypedFamilyArray) -> String {
        let encoding = Arc::clone(typed_array.encoding());
        let df = evaluate_aggregate_with_args_for_test(
            typed_array.into_array_ref(),
            Arc::new(sum_typed_family(encoding)),
            vec![col("a")],
        );
        df.to_string().await.unwrap()
    }
}
