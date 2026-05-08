use crate::aggregates::numeric_state::NumericState;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, exec_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use rdf_fusion_common::Decimal;
use rdf_fusion_common::{DFResult, Numeric};
use rdf_fusion_encoding::typed_family::{
    FamilyArray, NumericFamily, NumericFamilyArray, NumericFamilyArrayElementBuilder,
    NumericFamilyArrayParts, TypedFamilyEncodingRef, TypedFamilyId, TypedFamilyScalar,
};
use rdf_fusion_encoding::{EncodingArray, EncodingScalar, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;
use std::sync::Arc;

/// Creates a new [AggregateUDF] for the SPARQL `SUM` aggregate function.
///
/// Relevant Resources:
/// - [SPARQL 1.1 - SUM](https://www.w3.org/TR/sparql11-query/#defn_aggSum)
pub fn sparql_sum(encoding: TypedFamilyEncodingRef) -> AggregateUDF {
    AggregateUDF::new_from_impl(SumSparqlUDAF::new(encoding))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SumSparqlUDAF {
    name: String,
    signature: Signature,
    encoding: TypedFamilyEncodingRef,
    data_type: DataType,
}

impl SumSparqlUDAF {
    pub fn new(encoding: TypedFamilyEncodingRef) -> Self {
        let data_type = encoding.data_type().clone();
        Self {
            name: BuiltinName::Sum.to_string(),
            signature: Signature::exact(vec![data_type.clone()], Volatility::Immutable),
            encoding,
            data_type,
        }
    }
}

impl AggregateUDFImpl for SumSparqlUDAF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(self.data_type.clone())
    }

    fn accumulator(
        &self,
        _acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        Ok(Box::new(SparqlSumAccumulator::new(Arc::clone(
            &self.encoding,
        ))))
    }

    fn state_fields(
        &self,
        args: StateFieldsArgs,
    ) -> Result<Vec<Arc<Field>>, DataFusionError> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "sum"),
            self.data_type.clone(),
            true,
        ))])
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>, DataFusionError> {
        Ok(Box::new(SparqlSumGroupsAccumulator::new(Arc::clone(
            &self.encoding,
        ))))
    }
}

#[derive(Debug)]
pub(crate) struct SparqlSumAccumulator {
    encoding: TypedFamilyEncodingRef,
    sum: NumericState,
}

impl SparqlSumAccumulator {
    /// Creates a new [`SparqlSumAccumulator`].
    pub fn new(encoding: TypedFamilyEncodingRef) -> Self {
        SparqlSumAccumulator {
            encoding,
            sum: NumericState::new_untouched_integer(0),
        }
    }

    /// Evaluates the sum.
    fn evaluate_sum(&self) -> DFResult<TypedFamilyScalar> {
        match self.sum.to_numeric() {
            Ok(numeric) => {
                let numeric_scalar = NumericFamilyArray::new_scalar_from_numeric(numeric);
                self.encoding.create_scalar_from_family::<NumericFamily>(
                    numeric_scalar.to_scalar_value(),
                )
            }
            Err(_) => Ok(self.encoding.create_scalar_null()),
        }
    }
}

impl Accumulator for SparqlSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.sum.acc_sum(&self.encoding, &values[0], true)?;
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.evaluate_sum()?.into_scalar_value())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![self.evaluate_sum()?.into_scalar_value()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.sum.acc_sum(&self.encoding, &states[0], false)?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct SparqlSumGroupsAccumulator {
    encoding: TypedFamilyEncodingRef,
    /// Holds the running sum for each group
    sums: Vec<NumericState>,
}

impl SparqlSumGroupsAccumulator {
    /// Creates a new [`SparqlSumGroupsAccumulator`].
    pub fn new(encoding: TypedFamilyEncodingRef) -> Self {
        Self {
            encoding,
            sums: Vec::new(),
        }
    }

    fn update_batch_impl<const IGNORE_NULLS: bool>(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.sums
            .resize(total_num_groups, NumericState::new_untouched_integer(0));

        let arr = self.encoding.try_new_array(Arc::clone(&values[0]))?;

        let null_type_id = self.encoding.null_type_id();
        let numeric_type_id = self
            .encoding
            .find_typed_family_type_id(TypedFamilyId::Numeric)
            .expect("Numeric family should always be present");

        let type_ids = arr.type_ids();
        let offsets = arr.inner().as_union().offsets().expect("Dense union");

        let numeric_family_array = NumericFamilyArray::from_array_unchecked(Arc::clone(
            arr.inner().as_union().child(numeric_type_id),
        ));
        let numeric_family_parts = numeric_family_array.as_parts();

        match opt_filter {
            None => {
                for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                    process_numeric_row::<IGNORE_NULLS>(
                        &mut self.sums,
                        row_idx,
                        group_idx,
                        type_ids,
                        offsets,
                        null_type_id,
                        numeric_type_id,
                        &numeric_family_parts,
                    )?;
                }
            }
            Some(filter) => {
                for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                    // Combine the validity and boolean check for cleaner logic
                    if filter.is_valid(row_idx) && filter.value(row_idx) {
                        process_numeric_row::<IGNORE_NULLS>(
                            &mut self.sums,
                            row_idx,
                            group_idx,
                            type_ids,
                            offsets,
                            null_type_id,
                            numeric_type_id,
                            &numeric_family_parts,
                        )?;
                    }
                }
            }
        }

        return Ok(());

        /// Implements the processing logic for a single row.
        #[inline(always)]
        #[allow(clippy::too_many_arguments)]
        fn process_numeric_row<const IGNORE_NULLS: bool>(
            sums: &mut [NumericState],
            row_idx: usize,
            group_idx: usize,
            type_ids: &[i8],
            offsets: &[i32],
            null_type_id: i8,
            numeric_type_id: i8,
            numeric_family_parts: &NumericFamilyArrayParts<'_>,
        ) -> Result<(), DataFusionError> {
            let top_type_id = type_ids[row_idx];

            if top_type_id == null_type_id {
                if !IGNORE_NULLS {
                    sums[group_idx] = NumericState::error();
                }
            } else if top_type_id == numeric_type_id {
                let offset = offsets[row_idx] as usize;
                let num_type_id = numeric_family_parts.type_ids[offset];
                let num_offset = numeric_family_parts.offsets[offset] as usize;

                match num_type_id {
                    NumericFamily::FLOAT_TYPE_ID => {
                        let value = numeric_family_parts.floats.value(num_offset);
                        sums[group_idx].acc_sum_single(Numeric::Float(value.into()));
                    }
                    NumericFamily::DOUBLE_TYPE_ID => {
                        let value = numeric_family_parts.doubles.value(num_offset);
                        sums[group_idx].acc_sum_single(Numeric::Double(value.into()));
                    }
                    NumericFamily::DECIMAL_TYPE_ID => {
                        let value = numeric_family_parts.decimals.value(num_offset);
                        let value = Decimal::from_be_bytes(value.to_be_bytes());
                        sums[group_idx].acc_sum_single(Numeric::Decimal(value));
                    }
                    NumericFamily::INT_TYPE_ID => {
                        let value = numeric_family_parts.ints.value(num_offset);
                        sums[group_idx].acc_sum_single(Numeric::Int(value.into()));
                    }
                    NumericFamily::INTEGER_TYPE_ID => {
                        let value = numeric_family_parts.integers.value(num_offset);
                        sums[group_idx].acc_sum_single(Numeric::Integer(value.into()));
                    }
                    _ => {
                        return exec_err!("Invalid Numeric type id: {}", num_type_id);
                    }
                }
            } else {
                // Set an error if the type is not numeric or ignored
                sums[group_idx] = NumericState::error();
            }

            Ok(())
        }
    }
}

impl GroupsAccumulator for SparqlSumGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<(), DataFusionError> {
        self.update_batch_impl::<true>(
            values,
            group_indices,
            opt_filter,
            total_num_groups,
        )
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef, DataFusionError> {
        let states = emit_to.take_needed(&mut self.sums);
        let mut builder = NumericFamilyArrayElementBuilder::with_capacity(states.len());

        for state in states {
            if state.is_error {
                builder.append_null();
            } else {
                builder.append_numeric(state.value);
            }
        }

        let numeric_array = builder.finish();
        let typed_array = self.encoding.create_array_from_family(numeric_array)?;

        Ok(typed_array.into_array_ref())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>, DataFusionError> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<(), DataFusionError> {
        self.update_batch_impl::<false>(
            states,
            group_indices,
            opt_filter,
            total_num_groups,
        )
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.sums.capacity() * size_of::<NumericState>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::evaluate_aggregate_with_args_for_test;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::util::pretty::pretty_format_columns;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_common::Numeric;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::typed_family::{
        NumericFamilyArray, NumericFamilyArrayElementBuilder, TypedFamilyArray,
        TypedFamilyEncoding,
    };
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

    #[tokio::test]
    async fn test_groups_accumulator_direct() -> Result<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let mut acc = SparqlSumGroupsAccumulator::new(Arc::clone(&encoding));

        let values = NumericFamilyArray::new_integers(Int64Array::from(vec![
            Some(10),
            Some(20),
            Some(30),
            None,
            Some(50),
        ]));

        let typed_array = encoding.create_array_from_family(values)?;
        let array_ref = typed_array.into_array_ref();

        // Map rows to groups:
        // Group 0: 10, 30 -> Sum: 40
        // Group 1: 20, Null -> Sum: 20 (Null ignored)
        // Group 2: 50 -> Sum: 50
        let group_indices = vec![0, 1, 0, 1, 2];
        let total_num_groups = 3;

        acc.update_batch(&[array_ref], &group_indices, None, total_num_groups)?;
        let result_array = acc.evaluate(EmitTo::All)?;

        assert_snapshot!(
            pretty_format_columns("result", &[result_array])?,
            @"
        +-----------------------------------+
        | result                            |
        +-----------------------------------+
        | {rdf-fusion.numeric={integer=40}} |
        | {rdf-fusion.numeric={integer=20}} |
        | {rdf-fusion.numeric={integer=50}} |
        +-----------------------------------+
        "
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_groups_accumulator_merge() -> Result<()> {
        let encoding = Arc::new(TypedFamilyEncoding::default());

        // Partition 1
        let mut acc1 = SparqlSumGroupsAccumulator::new(Arc::clone(&encoding));
        let values1 =
            NumericFamilyArray::new_integers(Int64Array::from(vec![Some(10), Some(20)]));
        let array_ref1 = encoding.create_array_from_family(values1)?.into_array_ref();
        acc1.update_batch(&[array_ref1], &[0, 1], None, 2)?;
        let state1 = acc1.state(EmitTo::All)?;

        // Partition 2
        let mut acc2 = SparqlSumGroupsAccumulator::new(Arc::clone(&encoding));
        let values2 =
            NumericFamilyArray::new_integers(Int64Array::from(vec![Some(30), Some(40)]));
        let array_ref2 = encoding.create_array_from_family(values2)?.into_array_ref();
        acc2.update_batch(&[array_ref2], &[0, 1], None, 2)?;
        let state2 = acc2.state(EmitTo::All)?;

        // Final Aggregation (Merge)
        let mut final_acc = SparqlSumGroupsAccumulator::new(Arc::clone(&encoding));
        final_acc.merge_batch(&state1, &[0, 1], None, 2)?;
        final_acc.merge_batch(&state2, &[0, 1], None, 2)?;

        let final_result = final_acc.evaluate(EmitTo::All)?;

        // Group 0 should be 40 (10 + 30)
        // Group 1 should be 60 (20 + 40)
        assert_snapshot!(
            pretty_format_columns("result", &[final_result])?,
            @"
        +-----------------------------------+
        | result                            |
        +-----------------------------------+
        | {rdf-fusion.numeric={integer=40}} |
        | {rdf-fusion.numeric={integer=60}} |
        +-----------------------------------+
        "
        );
        Ok(())
    }

    /// Executes a test and returns the pretty-printed result.
    async fn run_test(typed_array: TypedFamilyArray) -> String {
        let encoding = Arc::clone(typed_array.encoding());
        let df = evaluate_aggregate_with_args_for_test(
            typed_array.into_array_ref(),
            Arc::new(AggregateUDF::new_from_impl(SumSparqlUDAF::new(encoding))),
            vec![col("a")],
        );
        df.to_string().await.unwrap()
    }
}
