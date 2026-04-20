use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::exec_err;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF,
    SetMonotonicity, Signature, StatisticsArgs, Volatility,
};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use rdf_fusion_encoding::typed_family::{
    NumericFamily, NumericFamilyArray, TypedFamilyEncodingRef,
};
use rdf_fusion_encoding::{EncodingArray, EncodingScalar, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::sync::Arc;

/// Creates a new [AggregateUDF] for the SPARQL `COUNT` aggregate function.
///
/// Relevant Resources:
/// - [SPARQL 1.1 - COUNT](https://www.w3.org/TR/sparql11-query/#defn_aggCount)
pub fn sparql_count(encoding: TypedFamilyEncodingRef) -> AggregateUDF {
    AggregateUDF::new_from_impl(SparqlCount::new(encoding))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparqlCount {
    inner: Arc<AggregateUDF>,
    encoding: TypedFamilyEncodingRef,
    signature: Signature,
    name: String,
}

impl SparqlCount {
    pub fn new(encoding: TypedFamilyEncodingRef) -> Self {
        let inner = datafusion::functions_aggregate::count::count_udaf();
        Self {
            inner,
            encoding,
            signature: Signature::any(1, Volatility::Immutable),
            name: BuiltinName::Count.to_string(),
        }
    }
}

impl AggregateUDFImpl for SparqlCount {
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

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let inner = self.inner.inner().accumulator(acc_args)?;
        Ok(Box::new(SparqlCountAccumulator::new(
            inner,
            Arc::clone(&self.encoding),
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Arc<Field>>> {
        self.inner.inner().state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.inner().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        let inner = self.inner.inner().create_groups_accumulator(args)?;
        Ok(Box::new(SparqlCountGroupsAccumulator::new(
            inner,
            Arc::clone(&self.encoding),
        )))
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.inner().set_monotonicity(data_type)
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner.inner().value_from_stats(statistics_args)
    }

    fn default_value(&self, _data_type: &DataType) -> DFResult<ScalarValue> {
        let count = NumericFamilyArray::new_integer_scalar(0);
        self.encoding
            .create_scalar_from_family::<NumericFamily>(count.to_scalar_value())
            .map(|tf| tf.into_scalar_value())
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.inner().reverse_expr()
    }
}

/// A wrapper around an SQL [`Accumulator`] that implements COUNT.
#[derive(Debug)]
pub(crate) struct SparqlCountAccumulator {
    inner: Box<dyn Accumulator>,
    encoding: TypedFamilyEncodingRef,
}

impl SparqlCountAccumulator {
    pub fn new(inner: Box<dyn Accumulator>, encoding: TypedFamilyEncodingRef) -> Self {
        Self { inner, encoding }
    }
}

impl Accumulator for SparqlCountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.inner.update_batch(values)
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let value = self.inner.evaluate()?;
        #[allow(clippy::cast_possible_wrap)]
        let count_val = match value {
            ScalarValue::Int64(Some(v)) => v,
            _ => return exec_err!("Unexpected value from count"),
        };

        let count = NumericFamilyArray::new_integer_scalar(count_val);
        self.encoding
            .create_scalar_from_family::<NumericFamily>(count.to_scalar_value())
            .map(|tf| tf.into_scalar_value())
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.inner.size()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.inner.merge_batch(states)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.inner.retract_batch(values)
    }
}

/// A wrapper around an SQL [`GroupsAccumulator`] that implements COUNT.
pub(crate) struct SparqlCountGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
    encoding: TypedFamilyEncodingRef,
}

impl SparqlCountGroupsAccumulator {
    pub fn new(
        inner: Box<dyn GroupsAccumulator>,
        encoding: TypedFamilyEncodingRef,
    ) -> Self {
        Self { inner, encoding }
    }
}

impl GroupsAccumulator for SparqlCountGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.inner
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let inner = self.inner.evaluate(emit_to)?;
        if inner.data_type() != &DataType::Int64 {
            return exec_err!("Unexpected data type from count");
        }

        let count = NumericFamilyArray::new_integers(inner.as_primitive().clone());
        let result = self.encoding.create_array_from_family(count)?;
        Ok(result.into_array_ref())
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.inner
            .merge_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}
