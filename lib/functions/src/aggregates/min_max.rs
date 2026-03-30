use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{AggregateUDF, Volatility, create_udaf};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use rdf_fusion_encoding::typed_family::{
    FamilyComparator, TypedFamilyArray, TypedFamilyEncodingRef, TypedFamilyId,
    TypedFamilyScalar,
};
use rdf_fusion_encoding::{EncodingArray, EncodingScalar, TermEncoding};
use rdf_fusion_model::DFResult;
use std::cmp::Ordering;
use std::sync::Arc;

pub fn max_typed_family(encoding: TypedFamilyEncodingRef) -> AggregateUDF {
    create_min_max_udf(encoding, "MAX", MinMaxOperator::Max)
}

pub fn min_typed_family(encoding: TypedFamilyEncodingRef) -> AggregateUDF {
    create_min_max_udf(encoding, "MIN", MinMaxOperator::Min)
}

fn create_min_max_udf(
    encoding: TypedFamilyEncodingRef,
    name: &str,
    operator: MinMaxOperator,
) -> AggregateUDF {
    let data_type = encoding.data_type().clone();
    create_udaf(
        name,
        vec![data_type.clone()],
        Arc::new(data_type.clone()),
        Volatility::Immutable,
        Arc::new(move |_| {
            Ok(Box::new(SparqlMinMaxAccumulator::new(
                Arc::clone(&encoding),
                operator,
            )))
        }),
        Arc::new(vec![DataType::Boolean, data_type]),
    )
}

/// Defines the operator of [`SparqlMinMaxAccumulator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MinMaxOperator {
    Min,
    Max,
}

impl MinMaxOperator {
    /// Determines if the `new` value is "better" (more extreme) than the `current` value
    /// based on the ordering of `new.cmp(&current)`.
    fn is_better(&self, cmp: Ordering) -> bool {
        match self {
            MinMaxOperator::Min => cmp == Ordering::Less,
            MinMaxOperator::Max => cmp == Ordering::Greater,
        }
    }

    /// Checks if the family can be completely skipped due to the new type being "worse" than the
    /// current one.
    fn can_skip_family(&mut self, current_type_id: Option<i8>, new_type_id: i8) -> bool {
        if let Some(old_type_id) = current_type_id {
            let type_cmp = new_type_id.cmp(&old_type_id);
            if type_cmp != Ordering::Equal && !self.is_better(type_cmp) {
                return true;
            }
        }
        false
    }
}

#[derive(Debug)]
struct SparqlMinMaxAccumulator {
    encoding: TypedFamilyEncodingRef,
    operator: MinMaxOperator,
    executed_once: bool,
    extreme_family: Option<i8>,
    extreme_value: Option<ScalarValue>,
    error: bool,
}

impl SparqlMinMaxAccumulator {
    /// Creates a new [`SparqlMinMaxAccumulator`].
    pub fn new(encoding: TypedFamilyEncodingRef, operator: MinMaxOperator) -> Self {
        Self {
            encoding,
            operator,
            executed_once: false,
            extreme_family: None,
            extreme_value: None,
            error: false,
        }
    }

    /// Encodes the current extrema as a [`TypedFamilyScalar`].
    fn encode_extreme(&self) -> DFResult<TypedFamilyScalar> {
        if self.error || !self.executed_once {
            return Ok(self.encoding.create_scalar_null());
        }
        self.encoding
            .try_new_scalar(self.extreme_value.clone().unwrap())
    }

    /// Sets the state to given family and value.
    fn set_extreme(
        &mut self,
        type_id: i8,
        family_id: TypedFamilyId,
        inner_scalar: ScalarValue,
    ) -> DFResult<()> {
        self.extreme_family = Some(type_id);
        self.extreme_value = Some(
            self.encoding
                .create_array_with_single_family(family_id, inner_scalar.to_array()?)?
                .try_as_scalar(0)?
                .into_scalar_value(),
        );
        self.executed_once = true;
        Ok(())
    }

    /// Helper to find the extrema within a single child array.
    fn find_child_extreme(
        &self,
        array: &ArrayRef,
        comparator: &FamilyComparator,
    ) -> Option<ScalarValue> {
        let mut extreme_idx = 0;
        for i in 1..array.len() {
            match comparator(i, extreme_idx) {
                Some(cmp) => {
                    if self.operator.is_better(cmp) {
                        extreme_idx = i;
                    }
                }
                None => {
                    // For now, we simply ignore elements that cannot be compared to the current
                    // extreme.
                }
            }
        }
        Some(ScalarValue::try_from_array(array, extreme_idx).expect("Index exists"))
    }
}

impl Accumulator for SparqlMinMaxAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
    ) -> Result<(), datafusion::error::DataFusionError> {
        if values.is_empty() || self.error {
            return Ok(());
        }

        let arr = self.encoding.try_new_array(Arc::clone(&values[0]))?;

        // Process every family in the batch (including Null)
        for child in arr.non_empty_children() {
            let new_family_id = child.family().family_id();

            let new_type_id = self
                .encoding
                .find_typed_family_type_id(new_family_id)
                .expect("Family must be part of the encoding");

            if self
                .operator
                .can_skip_family(self.extreme_family, new_type_id)
            {
                continue;
            }

            let Some(comparator) = child
                .family()
                .comparator(Arc::clone(child.array()), Arc::clone(child.array()))
            else {
                let scalar = ScalarValue::try_from_array(child.array(), 0)?;
                self.set_extreme(new_type_id, new_family_id, scalar)?;
                return Ok(());
            };

            let Some(batch_extreme_inner) =
                self.find_child_extreme(child.array(), &comparator)
            else {
                self.error = true;
                return Ok(());
            };

            if !self.executed_once {
                self.set_extreme(new_type_id, new_family_id, batch_extreme_inner)?;
                continue;
            }

            let old_type_id = self.extreme_family.unwrap();
            let type_cmp = new_type_id.cmp(&old_type_id);

            if type_cmp != Ordering::Equal {
                // Must be better due to the skip check above
                self.set_extreme(new_type_id, new_family_id, batch_extreme_inner)?;
            } else {
                // 4. Otherwise compare_extrema with existing and choose the best
                let current_extreme = self.extreme_value.as_ref().unwrap();
                let current_tf_arr = TypedFamilyArray::new_unchecked(
                    Arc::clone(&self.encoding),
                    current_extreme.to_array()?,
                );
                let current_inner_arr = current_tf_arr.child_for_family_id(new_family_id);

                let Some(comp) = child.family().comparator(
                    Arc::clone(current_inner_arr),
                    batch_extreme_inner.to_array()?,
                ) else {
                    self.error = true;
                    return Ok(());
                };

                match comp(0, 0) {
                    Some(cmp) => {
                        // Reverse because comp(0, 0) compares `current` to `batch`
                        if self.operator.is_better(cmp.reverse()) {
                            self.set_extreme(
                                new_type_id,
                                new_family_id,
                                batch_extreme_inner,
                            )?;
                        }
                    }
                    None => {
                        self.error = true;
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.encode_extreme()?.into_scalar_value())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Boolean(Some(self.executed_once)),
            self.encode_extreme()?.into_scalar_value(),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.is_empty() {
            return Ok(());
        }

        let executed_once_arr = states[0].as_boolean();
        for (i, executed_once) in executed_once_arr.iter().enumerate() {
            if executed_once == Some(true) {
                let state_val = ScalarValue::try_from_array(&states[1], i)?;
                let state_arr = state_val.to_array()?;
                self.update_batch(&[state_arr])?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::evaluate_aggregate_for_test;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::typed_family::{
        NumericFamily, NumericFamilyArray, StringFamily, StringFamilyArray, TypedFamily,
        TypedFamilyArrayBuilder, TypedFamilyEncoding,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_max_typed_family() -> DFResult<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let values = NumericFamilyArray::new_integers(Int64Array::from(vec![1, 3, 2]));
        let typed_array = encoding.create_array_from_family(values)?;

        assert_snapshot!(
            run_test(MinMaxOperator::Max, typed_array).await,
            @"
        +----------------------------------+
        | MAX(?table?.a)                   |
        +----------------------------------+
        | {rdf-fusion.numeric={integer=3}} |
        +----------------------------------+");

        Ok(())
    }

    #[tokio::test]
    async fn test_min_typed_family() -> DFResult<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let values = NumericFamilyArray::new_integers(Int64Array::from(vec![1, 3, 2]));
        let typed_array = encoding.create_array_from_family(values)?;

        assert_snapshot!(
            run_test(MinMaxOperator::Min, typed_array).await,
            @r"
        +----------------------------------+
        | MIN(?table?.a)                   |
        +----------------------------------+
        | {rdf-fusion.numeric={integer=1}} |
        +----------------------------------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_max_typed_family_with_nulls() -> DFResult<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let values = NumericFamilyArray::new_integers(Int64Array::from(vec![
            Some(1),
            None,
            Some(2),
        ]));
        let typed_array = encoding.create_array_from_family(values)?;

        assert_snapshot!(
            run_test(MinMaxOperator::Max, typed_array).await,
            @r"
        +--------------------+
        | MAX(?table?.a)     |
        +--------------------+
        | {rdf-fusion.null=} |
        +--------------------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_min_empty_batch() -> DFResult<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let typed_array = encoding.create_null_array(0)?;

        assert_snapshot!(
            run_test(MinMaxOperator::Max, typed_array).await,
            @r"
        +--------------------+
        | MAX(?table?.a)     |
        +--------------------+
        | {rdf-fusion.null=} |
        +--------------------+
        "
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cross_family_sorting() -> DFResult<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());

        let numeric_values =
            NumericFamilyArray::new_integers(Int64Array::from(vec![10, 20]));
        let string_values =
            StringFamilyArray::new_simple(StringArray::from(vec!["apple", "zebra"]));

        let numeric_tid = encoding
            .find_typed_family_type_id(NumericFamily::FAMILY_ID)
            .unwrap();
        let string_tid = encoding
            .find_typed_family_type_id(StringFamily::FAMILY_ID)
            .unwrap();
        let typed_array = TypedFamilyArrayBuilder::new(
            encoding,
            vec![numeric_tid, string_tid, numeric_tid, string_tid],
            vec![0, 0, 1, 1],
        )?
        .with_family_array(Some(numeric_values))?
        .with_family_array(Some(string_values))?
        .finish()?;

        assert_snapshot!(
            run_test(MinMaxOperator::Min, typed_array).await,
            @r"
        +-------------------------------------------------+
        | MIN(?table?.a)                                  |
        +-------------------------------------------------+
        | {rdf-fusion.strings={value: apple, language: }} |
        +-------------------------------------------------+
        "
        );

        Ok(())
    }

    /// Executes the test and returns the serialized DataFrame.
    async fn run_test(op: MinMaxOperator, typed_array: TypedFamilyArray) -> String {
        let udf = match op {
            MinMaxOperator::Min => min_typed_family(Arc::clone(&typed_array.encoding())),
            MinMaxOperator::Max => max_typed_family(Arc::clone(&typed_array.encoding())),
        };
        let df = evaluate_aggregate_for_test(typed_array.into_array_ref(), Arc::new(udf));
        df.to_string().await.unwrap()
    }
}
