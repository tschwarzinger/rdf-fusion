use crate::aggregates::{sparql_count, sparql_sum};
use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use rdf_fusion_common::DFResult;
use rdf_fusion_compute::numeric::{NumericBinaryOp, apply_typed_family_binary};
use rdf_fusion_encoding::typed_family::{
    NumericFamily, NumericFamilyArray, TypedFamilyArgs, TypedFamilyEncodingRef,
};
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;
use std::sync::Arc;

/// Creates a new [AggregateUDF] for the SPARQL `AVG` aggregate function.
///
/// Relevant Resources:
/// - [SPARQL 1.1 - AVG](https://www.w3.org/TR/sparql11-query/#defn_aggAvg)
pub fn sparql_avg(encoding: TypedFamilyEncodingRef) -> AggregateUDF {
    AggregateUDF::new_from_impl(SparqlAvgUDAF::new(encoding))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparqlAvgUDAF {
    name: String,
    signature: Signature,
    encoding: TypedFamilyEncodingRef,
    data_type: DataType,
    sum_inner: AggregateUDF,
    count_inner: AggregateUDF,
}

impl SparqlAvgUDAF {
    pub fn new(encoding: TypedFamilyEncodingRef) -> Self {
        let data_type = encoding.data_type().clone();

        let sum_inner = sparql_sum(Arc::clone(&encoding));
        let count_inner = sparql_count(Arc::clone(&encoding));

        Self {
            name: BuiltinName::Avg.to_string(),
            signature: Signature::exact(vec![data_type.clone()], Volatility::Immutable),
            encoding,
            data_type,
            sum_inner,
            count_inner,
        }
    }
}

impl AggregateUDFImpl for SparqlAvgUDAF {
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
        Ok(self.data_type.clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let sum_acc = self.sum_inner.inner().accumulator(acc_args.clone())?;
        let count_acc = self.count_inner.inner().accumulator(acc_args)?;
        Ok(Box::new(SparqlAvgAccumulator::new(
            Arc::clone(&self.encoding),
            sum_acc,
            count_acc,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Arc<Field>>> {
        let args_clone = StateFieldsArgs {
            name: args.name,
            input_fields: args.input_fields,
            return_field: Arc::clone(&args.return_field),
            ordering_fields: args.ordering_fields,
            is_distinct: args.is_distinct,
        };

        let mut fields = self.sum_inner.inner().state_fields(args)?;
        let mut count_fields = self.count_inner.inner().state_fields(args_clone)?;
        fields.append(&mut count_fields);

        Ok(fields)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.sum_inner
            .inner()
            .groups_accumulator_supported(args.clone())
            && self.count_inner.inner().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        let sum_acc = self
            .sum_inner
            .inner()
            .create_groups_accumulator(args.clone())?;
        let count_acc = self.count_inner.inner().create_groups_accumulator(args)?;

        Ok(Box::new(SparqlAvgGroupsAccumulator::new(
            Arc::clone(&self.encoding),
            sum_acc,
            count_acc,
        )))
    }

    fn default_value(&self, _data_type: &DataType) -> DFResult<ScalarValue> {
        let zero = NumericFamilyArray::new_integer_scalar(0);
        self.encoding
            .create_scalar_from_family::<NumericFamily>(zero.to_scalar_value())
            .map(|tf| tf.into_scalar_value())
    }
}

#[derive(Debug)]
pub(crate) struct SparqlAvgAccumulator {
    encoding: TypedFamilyEncodingRef,
    sum_acc: Box<dyn Accumulator>,
    count_acc: Box<dyn Accumulator>,
}

impl SparqlAvgAccumulator {
    pub fn new(
        encoding: TypedFamilyEncodingRef,
        sum_acc: Box<dyn Accumulator>,
        count_acc: Box<dyn Accumulator>,
    ) -> Self {
        Self {
            encoding,
            sum_acc,
            count_acc,
        }
    }
}

impl Accumulator for SparqlAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.sum_acc.update_batch(values)?;
        self.count_acc.update_batch(values)?;
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let sum_res = self.sum_acc.evaluate()?;
        let count_res = self.count_acc.evaluate()?;

        // Construct the expected '0' scalar for comparison and fallback
        let zero = NumericFamilyArray::new_integer_scalar(0);
        let zero_scalar = self
            .encoding
            .create_scalar_from_family::<NumericFamily>(zero.to_scalar_value())
            .map(|tf| tf.into_scalar_value())?;

        // If the multiset is empty (Count == 0), AVG is 0.
        if count_res == zero_scalar {
            return Ok(zero_scalar);
        }

        let sum_res_tf = self.encoding.try_new_scalar(sum_res)?;
        let count_res_tf = self.encoding.try_new_scalar(count_res)?;

        let args = TypedFamilyArgs::new_unchecked(
            1,
            vec![
                EncodingDatum::Scalar(sum_res_tf),
                EncodingDatum::Scalar(count_res_tf),
            ],
        );
        let div = apply_typed_family_binary(&args, NumericBinaryOp::Div)?;
        let scalar = div.try_as_scalar(0).expect("length is valid");
        Ok(scalar.into_scalar_value())
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + (self.sum_acc.size() - size_of_val(&self.sum_acc))
            + (self.count_acc.size() - size_of_val(&self.count_acc))
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let mut state = self.sum_acc.state()?;
        state.append(&mut self.count_acc.state()?);
        Ok(state)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let sum_states = &states[0..1];
        let count_states = &states[1..];

        self.sum_acc.merge_batch(sum_states)?;
        self.count_acc.merge_batch(count_states)?;
        Ok(())
    }
}

pub(crate) struct SparqlAvgGroupsAccumulator {
    encoding: TypedFamilyEncodingRef,
    sum_acc: Box<dyn GroupsAccumulator>,
    count_acc: Box<dyn GroupsAccumulator>,
}

impl SparqlAvgGroupsAccumulator {
    pub fn new(
        encoding: TypedFamilyEncodingRef,
        sum_acc: Box<dyn GroupsAccumulator>,
        count_acc: Box<dyn GroupsAccumulator>,
    ) -> Self {
        Self {
            encoding,
            sum_acc,
            count_acc,
        }
    }
}

impl GroupsAccumulator for SparqlAvgGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.sum_acc
            .update_batch(values, group_indices, opt_filter, total_num_groups)?;
        self.count_acc.update_batch(
            values,
            group_indices,
            opt_filter,
            total_num_groups,
        )?;
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let sum_res = self.sum_acc.evaluate(emit_to)?;
        let count_res = self.count_acc.evaluate(emit_to)?;

        assert_eq!(
            sum_res.len(),
            count_res.len(),
            "Both accumulators must return the same length."
        );
        let len = sum_res.len();

        let sum_res = self.encoding.try_new_array(sum_res)?;
        let count_res = self.encoding.try_new_array(count_res)?;

        let args = TypedFamilyArgs::new_unchecked(
            len,
            vec![
                EncodingDatum::Array(sum_res),
                EncodingDatum::Array(count_res),
            ],
        );
        let div = apply_typed_family_binary(&args, NumericBinaryOp::Div)?;
        Ok(div.into_array_ref())
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let mut state = self.sum_acc.state(emit_to)?;
        state.append(&mut self.count_acc.state(emit_to)?);
        Ok(state)
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<(), DataFusionError> {
        let sum_states = &states[0..1];
        let count_states = &states[1..];

        self.sum_acc.merge_batch(
            sum_states,
            group_indices,
            opt_filter,
            total_num_groups,
        )?;
        self.count_acc.merge_batch(
            count_states,
            group_indices,
            opt_filter,
            total_num_groups,
        )?;
        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.sum_acc.size() + self.count_acc.size()
    }
}
