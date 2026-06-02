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
pub struct ParquetQuadStorageScanExec {
    quad_pattern: QuadPattern,
    inner: Arc<DataSourceExec>,
}

impl ParquetQuadStorageScanExec {
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

impl ExecutionPlan for ParquetQuadStorageScanExec {
    fn name(&self) -> &str {
        "ParquetQuadStorageScanExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "ParquetQuadStorageScanExec"
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
            return plan_err!("ParquetQuadStorageScanExec must have no children");
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

impl DisplayAs for ParquetQuadStorageScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ParquetQuadStorageScanExec:")?;
        write!(f, ", active_graph={}", self.quad_pattern.active_graph)?;

        if let Some(var) = &self.quad_pattern.graph_variable {
            write!(f, ", graph_variable={var}")?;
        }

        write!(
            f,
            ", triple_pattern=[{}]",
            &self.quad_pattern.triple_pattern
        )?;
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

#[cfg(test)]
mod tests {
    use crate::parquet::ParquetQuadStorage;
    use datafusion::dataframe::DataFrameWriteOptions;
    use datafusion::physical_plan::displayable;
    use insta::assert_snapshot;
    use object_store::memory::InMemory;
    use rdf_fusion_common::{NamedNode, Quad};
    use rdf_fusion_encoding::QuadStorageEncodingName;
    use rdf_fusion_encoding::string::StringQuadsBuilder;
    use rdf_fusion_execution::RdfFusionContext;
    use rdf_fusion_execution::RdfFusionContextBuilder;
    use rdf_fusion_execution::sparql::QueryOptions;
    use rdf_fusion_execution::sparql::RdfFusionQuery;
    use std::sync::Arc;
    use url::Url;

    #[tokio::test]
    async fn test_parquet_scan_filter_pushdown_with_equality_with_named_node() {
        let context = prepare_test_store().await;

        let query_pushed: RdfFusionQuery = "SELECT ?s WHERE { ?s <http://example.org/p1> ?o . FILTER(?o = <http://example.org/o1>) }".try_into().unwrap();
        let (_, explanation_pushed) = context
            .execute_query(&query_pushed, QueryOptions::default())
            .await
            .unwrap();
        let plan_pushed = explanation_pushed.execution_plan;

        assert_snapshot!(
            displayable(plan_pushed.as_ref()).indent(true),
            @"ParquetQuadStorageScanExec:, active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> ?o], blank_node_mode=Variable, file_groups={1 group: [[test.parquet]]}, projection=[ENC_PT(subject@1) as s], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = <http://example.org/p1> AND object@3 = <http://example.org/o1>, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= <http://example.org/p1> AND <http://example.org/p1> <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= <http://example.org/o1> AND <http://example.org/o1> <= object_max@6, required_guarantees=[object in (<http://example.org/o1>), predicate in (<http://example.org/p1>)]"
        );
    }

    #[tokio::test]
    async fn test_parquet_scan_filter_pushdown_with_function_prevented() {
        let context = prepare_test_store().await;

        let query_not_pushed: RdfFusionQuery = "SELECT ?s WHERE { ?s <http://example.org/p1> ?o . FILTER(LCASE(STR(?o)) = \"http://example.org/o1\") }".try_into().unwrap();
        let (_, explanation_not_pushed) = context
            .execute_query(&query_not_pushed, QueryOptions::default())
            .await
            .unwrap();
        let plan_not_pushed = explanation_not_pushed.execution_plan;

        assert_snapshot!(displayable(plan_not_pushed.as_ref()).indent(true), @"
        ProjectionExec: expr=[ENC_PT(subject@0) as s]
          FilterExec: EBV(EQ(LCASE(ENC_TF(STR(ENC_PT(object@3)))), 2:{value:http://example.org/o1,language:})), projection=[subject@1]
            ParquetQuadStorageScanExec:, active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> ?o], blank_node_mode=Variable, file_groups={1 group: [[test.parquet]]}, projection=[graph, subject, predicate, object], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = <http://example.org/p1>, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= <http://example.org/p1> AND <http://example.org/p1> <= predicate_max@2, required_guarantees=[predicate in (<http://example.org/p1>)]
        ");
    }

    async fn prepare_test_store() -> RdfFusionContext {
        let context = datafusion::prelude::SessionContext::new();
        context.runtime_env().object_store_registry.register_store(
            &Url::parse("memory:///").unwrap(),
            Arc::new(InMemory::new()),
        );

        let mut builder = StringQuadsBuilder::with_capacity(1);
        builder.append_quad(
            Quad::new(
                NamedNode::new_unchecked("http://example.org/s1"),
                NamedNode::new_unchecked("http://example.org/p1"),
                NamedNode::new_unchecked("http://example.org/o1"),
                rdf_fusion_common::GraphNameRef::DefaultGraph,
            )
            .as_ref(),
        );
        let batch = builder.finish().into_record_batch();
        context
            .read_batch(batch)
            .unwrap()
            .write_parquet(
                "memory:///test.parquet",
                DataFrameWriteOptions::new().with_single_file_output(true),
                None,
            )
            .await
            .unwrap();

        let storage = ParquetQuadStorage::try_load(
            Url::parse("memory:///test.parquet").unwrap(),
            QuadStorageEncodingName::String,
            context.runtime_env().object_store_registry.as_ref(),
        )
        .await
        .unwrap();

        RdfFusionContextBuilder::new(Arc::new(storage))
            .with_single_partition_session_config()
            .build()
            .unwrap()
    }
}
