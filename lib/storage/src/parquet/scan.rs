use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Statistics, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    Distribution, OrderingRequirements, PhysicalSortExpr, ScalarFunctionExpr,
};
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    SortOrderPushdownResult,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// A physical execution plan for scanning a Parquet database.
///
/// This wraps a `DataSourceExec` and prevents pushing down expensive UDF filters into Parquet
/// scanning while allowing cheap filters (e.g., term equality) to be pushed down.
#[derive(Debug, Clone)]
pub struct ParquetQuadScanExec {
    quad_pattern: QuadPattern,
    inner: Arc<DataSourceExec>,
}

impl ParquetQuadScanExec {
    pub fn try_new(
        quad_pattern: QuadPattern,
        inner: Arc<DataSourceExec>,
    ) -> DFResult<Self> {
        Ok(Self {
            quad_pattern,
            inner,
        })
    }

    /// Provides access to the underlying execution plan that implements the actual scan.
    #[allow(dead_code)]
    pub(crate) fn inner_scan(&self) -> &Arc<DataSourceExec> {
        &self.inner
    }

    fn wrap_inner(&self, inner: Arc<dyn ExecutionPlan>) -> Option<Arc<Self>> {
        let downcast = inner.as_any().downcast_ref::<DataSourceExec>()?;
        Some(Arc::new(Self {
            quad_pattern: self.quad_pattern.clone(),
            inner: Arc::new(downcast.clone()),
        }))
    }
}

impl ExecutionPlan for ParquetQuadScanExec {
    fn name(&self) -> &str {
        "ParquetQuadScanExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "ParquetQuadScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn check_invariants(&self, check: InvariantLevel) -> DFResult<()> {
        self.inner.check_invariants(check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.inner.required_input_distribution()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.inner.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("ParquetQuadScanExec must have no children");
        }
        Ok(self)
    }

    fn reset_state(self: Arc<Self>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let new_plan = Arc::clone(&self.inner).reset_state()?;
        let wrapped = self.wrap_inner(new_plan).expect("must be DataSourceExec");
        Ok(wrapped as Arc<dyn ExecutionPlan>)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let inner = self.inner.repartitioned(target_partitions, config)?;
        Ok(inner.and_then(|new_inner| {
            self.wrap_inner(new_inner)
                .map(|p| p as Arc<dyn ExecutionPlan>)
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        self.inner.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.with_fetch(limit)?;
        self.wrap_inner(inner).map(|p| p as Arc<dyn ExecutionPlan>)
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.inner.cardinality_effect()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let inner = self.inner.try_swapping_with_projection(projection)?;
        Ok(inner.and_then(|new_inner| {
            self.wrap_inner(new_inner)
                .map(|p| p as Arc<dyn ExecutionPlan>)
        }))
    }

    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> DFResult<FilterDescription> {
        self.inner
            .gather_filters_for_pushdown(phase, parent_filters, config)
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let parent_filters: Vec<_> = child_pushdown_result
            .parent_filters
            .iter()
            .map(|f| Arc::clone(&f.filter))
            .collect();
        if parent_filters.is_empty() {
            return Ok(FilterPushdownPropagation {
                filters: vec![],
                updated_node: None,
            });
        }

        let is_pushable: Vec<bool> = parent_filters
            .iter()
            .map(|expr| !contains_udf(expr))
            .collect();

        if !is_pushable.iter().any(|&p| p) {
            return Ok(FilterPushdownPropagation {
                filters: vec![PushedDown::No; parent_filters.len()],
                updated_node: None,
            });
        }

        let mut child_pushdown_result = child_pushdown_result;
        child_pushdown_result
            .parent_filters
            .retain(|f| !contains_udf(&f.filter));
        let inner_propagation = self.inner.handle_child_pushdown_result(
            phase,
            child_pushdown_result,
            config,
        )?;

        let mut inner_filters_iter = inner_propagation.filters.into_iter();
        let final_filters = is_pushable
            .into_iter()
            .map(|pushable| {
                if pushable {
                    inner_filters_iter
                        .next()
                        .expect("inner filters length mismatch")
                } else {
                    PushedDown::No
                }
            })
            .collect();

        let updated_node = match inner_propagation.updated_node {
            None => None,
            Some(node) => {
                let wrapped = self.wrap_inner(node).expect("must be DataSourceExec");
                Some(wrapped as Arc<dyn ExecutionPlan>)
            }
        };

        Ok(FilterPushdownPropagation {
            filters: final_filters,
            updated_node,
        })
    }

    fn with_new_state(
        &self,
        state: Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let new_inner = self.inner.with_new_state(state)?;
        self.wrap_inner(new_inner)
            .map(|p| p as Arc<dyn ExecutionPlan>)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> DFResult<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let result = self.inner.try_pushdown_sort(order)?;
        Ok(result.map(|new_inner| {
            let wrapped = self.wrap_inner(new_inner).expect("must be DataSourceExec");
            wrapped as Arc<dyn ExecutionPlan>
        }))
    }

    fn with_preserve_order(
        &self,
        preserve_order: bool,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let new_inner = self.inner.with_preserve_order(preserve_order)?;
        self.wrap_inner(new_inner)
            .map(|p| p as Arc<dyn ExecutionPlan>)
    }
}

impl DisplayAs for ParquetQuadScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ParquetQuadScanExec:")?;
        write!(f, " active_graph={}", self.quad_pattern.active_graph)?;

        if let Some(var) = &self.quad_pattern.graph_variable {
            write!(f, ", graph_variable={var}")?;
        }

        write!(f, ", triple_pattern=[{}]", self.quad_pattern.triple_pattern)?;
        write!(f, ", blank_node_mode={}", self.quad_pattern.blank_node_mode)?;
        write!(f, ", ")?;

        self.inner.data_source().fmt_as(t, f)?;

        Ok(())
    }
}

/// Helper function to check if a physical expression contains a Scalar UDF.
fn contains_udf(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if expr.as_any().is::<ScalarFunctionExpr>() {
        return true;
    }
    for child in expr.children() {
        if contains_udf(child) {
            return true;
        }
    }
    false
}
