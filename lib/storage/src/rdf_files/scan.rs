use crate::exec::{
    QuadStorageScanStream, extract_and_alias_inner_metrics, is_cooperative_on_all_paths,
};
use crate::rdf_files::detect_encoding_from_schema;
use crate::rdf_files::manager::RdfFileManager;
use crate::rdf_files::rdf::RdfFileSourceConfig;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Statistics, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, SessionState};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr_common::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use rdf_fusion_common::config::RdfFileStorageOptions;
use rdf_fusion_common::{DFResult, GraphName};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::any::Any;
use std::sync::Arc;

/// A physical execution plan for scanning a [`RdfFileQuadStorage`].
///
/// This plan wraps the underlying scans (e.g., `DataSourceExec`) to provide a cleaner
/// representation in the query plan.
///
/// [`RdfFileQuadStorage`]: crate::rdf_files::RdfFileQuadStorage
#[derive(Debug, Clone)]
pub struct RdfFileQuadPatternScanExec {
    quad_pattern: QuadPattern,
    properties: Arc<PlanProperties>,
    inner: Arc<dyn ExecutionPlan>,
    target_encoding: QuadStorageEncoding,
    metrics: ExecutionPlanMetricsSet,
    pushed_down_filters: Vec<Arc<dyn PhysicalExpr>>,
    fetch: Option<usize>,
    options: RdfFileStorageOptions,
}

impl RdfFileQuadPatternScanExec {
    /// Creates a new [`RdfFileQuadPatternScanExec`].
    pub async fn new(
        quad_pattern: QuadPattern,
        manager: RdfFileManager,
        sources: Vec<(GraphName, RdfFileSourceConfig)>,
        target_encoding: QuadStorageEncoding,
        _schema: SchemaRef,
        session_state: &SessionState,
        options: RdfFileStorageOptions,
    ) -> DFResult<Self> {
        let inner = Self::build_inner_plan(
            &quad_pattern,
            &manager,
            &sources,
            &target_encoding,
            session_state,
            &options,
        )
        .await?;

        let scheduling = if is_cooperative_on_all_paths(&inner) {
            SchedulingType::Cooperative
        } else {
            SchedulingType::NonCooperative
        };

        let properties = inner
            .properties()
            .as_ref()
            .clone()
            .with_scheduling_type(scheduling);

        Ok(Self {
            quad_pattern,
            properties: Arc::new(properties),
            inner,
            target_encoding,
            metrics: ExecutionPlanMetricsSet::new(),
            pushed_down_filters: vec![],
            fetch: None,
            options,
        })
    }

    /// Returns the inner execution plan.
    ///
    /// This is hidden from DataFusion's optimizer to declutter execution plans.
    #[cfg(test)]
    pub fn inner_plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }

    /// Tries to create a new [`RdfFileQuadPatternScanExec`] with the given inner plan.
    fn try_new_internal(
        quad_pattern: QuadPattern,
        inner: Arc<dyn ExecutionPlan>,
        target_encoding: QuadStorageEncoding,
        options: RdfFileStorageOptions,
        fetch: Option<usize>,
        pushed_down_filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Self> {
        let scheduling = if is_cooperative_on_all_paths(&inner) {
            SchedulingType::Cooperative
        } else {
            SchedulingType::NonCooperative
        };

        let properties = inner
            .properties()
            .as_ref()
            .clone()
            .with_scheduling_type(scheduling);

        Ok(Self {
            quad_pattern,
            properties: Arc::new(properties),
            inner,
            target_encoding,
            metrics: ExecutionPlanMetricsSet::new(),
            pushed_down_filters,
            fetch,
            options,
        })
    }

    /// Builds the inner execution plan.
    async fn build_inner_plan(
        quad_pattern: &QuadPattern,
        manager: &RdfFileManager,
        sources: &[(GraphName, RdfFileSourceConfig)],
        target_encoding: &QuadStorageEncoding,
        state: &SessionState,
        options: &RdfFileStorageOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        use crate::rdf_files::rdf::RdfFileScanOptions;
        use datafusion::common::DataFusionError;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::projection::ProjectionExec;
        use datafusion::physical_plan::union::UnionExec;

        let mut plans = Vec::new();
        for (graph_name, source) in sources {
            if quad_pattern.active_graph.matches(graph_name) {
                let options = RdfFileScanOptions::with_format(source.format)
                    .with_base_iri(source.url.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .with_default_graph(graph_name.clone())
                    .with_rename_blank_nodes(true);

                let provider = manager
                    .get_scan_plan(source.url.clone(), options, state)
                    .await?;
                let mut plan = provider.scan(state, None, &[], None).await?;

                let source_encoding = detect_encoding_from_schema(&plan.schema())?;
                if &source_encoding != target_encoding {
                    // Inject casting projection
                    let mut exprs = Vec::new();
                    for (i, field) in plan.schema().fields().iter().enumerate() {
                        let target_field =
                            Arc::clone(target_encoding.quad_schema().field(i));
                        let expr = Arc::new(Column::new(field.name(), i))
                            as Arc<dyn PhysicalExpr>;
                        let cast_expr = datafusion::physical_expr::expressions::cast(
                            expr,
                            &plan.schema(),
                            target_field.data_type().clone(),
                        )?;
                        exprs.push((cast_expr, field.name().to_string()));
                    }
                    plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
                }

                plans.push(plan);
            }
        }

        let plan_count = plans.len();
        let inner_plan: Arc<dyn ExecutionPlan>;

        if plans.is_empty() {
            inner_plan = Arc::new(EmptyExec::new(Arc::clone(
                target_encoding.quad_schema().inner(),
            )));
        } else {
            let mut plan = UnionExec::try_new(plans)?;

            let filters = quad_pattern.compute_filters(target_encoding)?;
            if !filters.is_empty() {
                let filter_expr =
                    datafusion::logical_expr::utils::conjunction(filters).unwrap();
                let physical_filter_expr = state
                    .create_physical_expr(filter_expr, &target_encoding.quad_schema())?;
                plan = Arc::new(FilterExec::try_new(physical_filter_expr, plan)?);

                // Apply filter pushdown immediately to try pushing filters into DataSourceExec (e.g. Parquet)
                plan = FilterPushdown::new().optimize(plan, state.config_options())?;
            }

            // De-duplicate quads, if necessary
            if plan_count > 1 || !options.assume_quads_unique_in_single_file {
                plan = if plan.properties().output_partitioning().partition_count() > 1 {
                    Arc::new(CoalescePartitionsExec::new(plan))
                } else {
                    plan
                };

                let group_exprs = plan
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        (
                            Arc::new(Column::new(field.name(), i))
                                as Arc<dyn PhysicalExpr>,
                            field.name().clone(),
                        )
                    })
                    .collect::<Vec<_>>();

                plan = Arc::new(AggregateExec::try_new(
                    AggregateMode::Single,
                    PhysicalGroupBy::new_single(group_exprs),
                    vec![],
                    vec![],
                    plan,
                    Arc::clone(target_encoding.quad_schema().inner()),
                )?);
            }
            inner_plan = plan;
        }

        // We must project the inner plan to match the logical node's schema.
        // The inner plan has the GSPO schema (graph, subject, predicate, object).
        let mut physical_exprs = Vec::new();
        for (component, name) in quad_pattern.compute_projected_components() {
            physical_exprs.push((
                Arc::new(Column::new(component.column_name(), component.gspo_index()))
                    as Arc<dyn PhysicalExpr>,
                name,
            ));
        }

        let mut plan: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(physical_exprs, inner_plan)?);

        // Apply pushdowns to the entire unoptimized plan
        plan = ProjectionPushdown::new().optimize(plan, state.config_options())?;
        plan = FilterPushdown::new().optimize(plan, state.config_options())?;

        Ok(plan)
    }
}

impl DisplayAs for RdfFileQuadPatternScanExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RdfFileQuadPatternScanExec:")?;
        write!(f, " active_graph={}", self.quad_pattern.active_graph)?;

        if let Some(var) = &self.quad_pattern.graph_variable {
            write!(f, ", graph_variable={var}")?;
        }

        write!(
            f,
            ", triple_pattern=[{} {} {}]",
            &self.quad_pattern.triple_pattern.subject,
            &self.quad_pattern.triple_pattern.predicate,
            &self.quad_pattern.triple_pattern.object
        )?;
        write!(f, ", blank_node_mode={}", self.quad_pattern.blank_node_mode)?;

        let schema = self.schema();
        if schema.fields().len() != self.quad_pattern.number_of_unique_variables() {
            let col_names: Vec<&str> = schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            write!(f, ", projection=[{}]", col_names.as_slice().join(", "))?;
        }

        if !self.pushed_down_filters.is_empty() {
            let filter_strings: Vec<String> = self
                .pushed_down_filters
                .iter()
                .map(|expr| format!("{expr}"))
                .collect();
            write!(f, ", pushed_filters=[{}]", filter_strings.join(", "))?;
        }

        if let Some(fetch) = self.fetch() {
            write!(f, ", fetch={fetch}")?;
        }

        Ok(())
    }
}

impl ExecutionPlan for RdfFileQuadPatternScanExec {
    fn name(&self) -> &str {
        "RdfFileQuadPatternScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties.eq_properties.schema())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!(
                "RdfFileQuadPatternScanExec is opaque and cannot accept new children"
            );
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(QuadStorageScanStream::new(
            inner_stream,
            baseline_metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let mut set = self.metrics.clone_inner();

        extract_and_alias_inner_metrics(&self.inner, &mut set);

        Some(set)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        self.inner.partition_statistics(partition)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let fetch = limit?;

        let limited = Arc::new(GlobalLimitExec::new(
            Arc::clone(&self.inner),
            0,
            Some(fetch),
        ));
        let optimized = LimitPushdown::new()
            .optimize(limited, &ConfigOptions::default())
            .ok()?;

        let new_plan = Self::try_new_internal(
            self.quad_pattern.clone(),
            optimized,
            self.target_encoding.clone(),
            self.options.clone(),
            limit,
            self.pushed_down_filters.clone(),
        )
        .ok()?;

        Some(Arc::new(new_plan))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let parent_filters: Vec<_> = child_pushdown_result
            .parent_filters
            .into_iter()
            .map(|f| f.filter)
            .collect();
        if parent_filters.is_empty() {
            return Ok(FilterPushdownPropagation {
                filters: vec![],
                updated_node: None,
            });
        }

        let pushable = parent_filters
            .iter()
            .filter(|expr| !contains_udf(expr))
            .cloned()
            .collect::<Vec<_>>();
        if pushable.is_empty() {
            return Ok(FilterPushdownPropagation {
                filters: vec![PushedDown::No; parent_filters.len()],
                updated_node: None,
            });
        }

        let combined_expr = datafusion::physical_expr::conjunction(pushable.clone());
        let filter_exec =
            Arc::new(FilterExec::try_new(combined_expr, Arc::clone(&self.inner))?);
        let rule = match phase {
            FilterPushdownPhase::Pre => FilterPushdown::new(),
            FilterPushdownPhase::Post => FilterPushdown::new_post_optimization(),
        };
        let optimized_inner = rule.optimize(filter_exec, config)?;

        let new_pushed_down_filters =
            [self.pushed_down_filters.clone(), pushable.clone()].concat();
        let final_plan = match optimized_inner.as_any().downcast_ref::<FilterExec>() {
            None => {
                let new_plan = Self::try_new_internal(
                    self.quad_pattern.clone(),
                    optimized_inner,
                    self.target_encoding.clone(),
                    self.options.clone(),
                    self.fetch,
                    new_pushed_down_filters,
                )?;
                Arc::new(new_plan) as Arc<dyn ExecutionPlan>
            }
            Some(inner_filter) => {
                let remaining_predicate = Arc::clone(inner_filter.predicate());
                let inner_filter_child = Arc::clone(inner_filter.children()[0]);
                let new_quad_scan = Self::try_new_internal(
                    self.quad_pattern.clone(),
                    inner_filter_child,
                    self.target_encoding.clone(),
                    self.options.clone(),
                    self.fetch,
                    new_pushed_down_filters,
                )?;

                let new_filter =
                    FilterExec::try_new(remaining_predicate, Arc::new(new_quad_scan))?;
                Arc::new(new_filter) as Arc<dyn ExecutionPlan>
            }
        };

        let filter_info = parent_filters
            .iter()
            .map(|expr| {
                if contains_udf(expr) {
                    PushedDown::No
                } else {
                    PushedDown::Yes
                }
            })
            .collect();
        Ok(FilterPushdownPropagation {
            filters: filter_info,
            updated_node: Some(final_plan),
        })
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(inner) = self.inner.try_swapping_with_projection(projection)? else {
            return Ok(None);
        };

        let new_plan = Self::try_new_internal(
            self.quad_pattern.clone(),
            inner,
            self.target_encoding.clone(),
            self.options.clone(),
            self.fetch,
            self.pushed_down_filters.clone(),
        )?;
        Ok(Some(Arc::new(new_plan) as Arc<dyn ExecutionPlan>))
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
    use super::*;
    use crate::rdf_files::RdfFileManager;
    use datafusion::dataframe::DataFrameWriteOptions;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::expressions::Literal;
    use datafusion::physical_plan::projection::ProjectionExpr;
    use datafusion::prelude::SessionContext;
    use insta::assert_snapshot;
    use object_store::memory::InMemory;
    use rdf_fusion_common::{
        NamedNode, RdfFormat, TermPattern, TermRef, TriplePattern, Variable,
    };
    use rdf_fusion_encoding::string::{STRING_ENCODING, StringQuadsBuilder};
    use rdf_fusion_logical::ActiveGraph;
    use rdf_fusion_logical::quad_pattern::QuadPattern;
    use url::Url;

    #[tokio::test]
    async fn test_data_dump_quad_pattern_scan_exec_inner() {
        let manager = RdfFileManager::new();
        let sources = vec![];
        let quad_pattern = QuadPattern::all_quads();
        let schema = QuadStorageEncoding::String.quad_schema().inner().clone();

        let ctx = SessionContext::new();
        let scan = RdfFileQuadPatternScanExec::new(
            quad_pattern,
            manager,
            sources,
            QuadStorageEncoding::String,
            schema.into(),
            &ctx.state(),
            RdfFileStorageOptions::default(),
        )
        .await
        .unwrap();

        assert!(scan.inner.as_any().is::<EmptyExec>());
    }

    #[tokio::test]
    async fn test_rdf_file_scan_basic() {
        let (ctx, scan) = setup_in_memory_parquet_scan().await;

        assert_snapshot!(
            displayable(scan.as_ref()).indent(true),
            @"RdfFileQuadPatternScanExec: active_graph=Default Graph, triple_pattern=[?s ?p ?o], blank_node_mode=Variable"
        );
        assert_snapshot!(
            displayable(scan.inner_plan().as_ref()).indent(true),
            @"
        ProjectionExec: expr=[subject@1 as s, predicate@2 as p, object@3 as o]
          AggregateExec: mode=Single, gby=[graph@0 as graph, subject@1 as subject, predicate@2 as predicate, object@3 as object], aggr=[]
            FilterExec: graph@0 IS NULL
              DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[graph, subject, predicate, object], file_type=parquet, predicate=graph@0 IS NULL AND graph@0 IS NULL, pruning_predicate=graph_null_count@0 > 0 AND graph_null_count@0 > 0, required_guarantees=[]
        "
        );

        let results = datafusion::physical_plan::collect(scan, ctx.task_ctx())
            .await
            .unwrap();
        let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&results)
            .unwrap()
            .to_string();
        assert_snapshot!(formatted, @r"
        +-------------------------+-------------------------+-------------------------+
        | s                       | p                       | o                       |
        +-------------------------+-------------------------+-------------------------+
        | <http://example.org/s1> | <http://example.org/p1> | <http://example.org/o1> |
        | <http://example.org/s2> | <http://example.org/p2> | <http://example.org/o2> |
        +-------------------------+-------------------------+-------------------------+
        ");
    }

    #[tokio::test]
    async fn test_rdf_file_scan_limit_pushdown() {
        let (_ctx, scan) = setup_in_memory_parquet_scan().await;

        let pushed_scan_arc = scan.with_fetch(Some(1)).expect("Should return Some plan");
        let pushed_scan = pushed_scan_arc
            .as_any()
            .downcast_ref::<RdfFileQuadPatternScanExec>()
            .unwrap();

        assert_eq!(pushed_scan_arc.fetch(), Some(1));

        assert_snapshot!(
            displayable(pushed_scan_arc.as_ref()).indent(true),
            @"RdfFileQuadPatternScanExec: active_graph=Default Graph, triple_pattern=[?s ?p ?o], blank_node_mode=Variable, fetch=1"
        );
        assert_snapshot!(
            displayable(pushed_scan.inner_plan().as_ref()).indent(true),
            @"
        ProjectionExec: expr=[subject@1 as s, predicate@2 as p, object@3 as o]
          GlobalLimitExec: skip=0, fetch=1
            AggregateExec: mode=Single, gby=[graph@0 as graph, subject@1 as subject, predicate@2 as predicate, object@3 as object], aggr=[]
              FilterExec: graph@0 IS NULL
                DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[graph, subject, predicate, object], file_type=parquet, predicate=graph@0 IS NULL AND graph@0 IS NULL, pruning_predicate=graph_null_count@0 > 0 AND graph_null_count@0 > 0, required_guarantees=[]
        "
        );
    }

    #[tokio::test]
    async fn test_rdf_file_scan_filter_pushdown() {
        let (ctx, scan) = setup_in_memory_parquet_scan().await;

        let filter_expr =
            make_string_filter_expr(&scan.schema(), "s", "http://example.org/s1");
        let filter_exec =
            Arc::new(FilterExec::try_new(filter_expr, scan.clone()).unwrap());

        let optimized = FilterPushdown::new()
            .optimize(filter_exec, ctx.state().config_options())
            .expect("Filter pushdown should succeed");

        assert_snapshot!(
            displayable(optimized.as_ref()).indent(true),
            @"RdfFileQuadPatternScanExec: active_graph=Default Graph, triple_pattern=[?s ?p ?o], blank_node_mode=Variable, pushed_filters=[s@0 = <http://example.org/s1>]"
        );

        let pushed_scan = optimized
            .as_any()
            .downcast_ref::<RdfFileQuadPatternScanExec>()
            .unwrap();

        assert_snapshot!(
            displayable(pushed_scan.inner_plan().as_ref()).indent(true),
            @"
        ProjectionExec: expr=[subject@1 as s, predicate@2 as p, object@3 as o]
          AggregateExec: mode=Single, gby=[graph@0 as graph, subject@1 as subject, predicate@2 as predicate, object@3 as object], aggr=[], ordering_mode=PartiallySorted([1])
            FilterExec: subject@1 = <http://example.org/s1> AND graph@0 IS NULL
              DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[graph, subject, predicate, object], file_type=parquet, predicate=graph@0 IS NULL AND graph@0 IS NULL AND graph@0 IS NULL AND subject@1 = <http://example.org/s1>, pruning_predicate=graph_null_count@0 > 0 AND graph_null_count@0 > 0 AND graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= <http://example.org/s1> AND <http://example.org/s1> <= subject_max@2, required_guarantees=[subject in (<http://example.org/s1>)]
        "
        );
    }

    #[tokio::test]
    async fn test_rdf_file_scan_projection_pushdown() {
        let (ctx, scan) = setup_in_memory_parquet_scan().await;

        let schema = scan.schema();
        let s_idx = schema.index_of("s").unwrap();
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![ProjectionExpr::new(
                    Arc::new(Column::new("s", s_idx)),
                    "s".to_string(),
                )],
                scan.clone(),
            )
            .unwrap(),
        );

        let optimized = ProjectionPushdown::new()
            .optimize(projection, ctx.state().config_options())
            .expect("Projection pushdown should succeed");

        assert_snapshot!(
            displayable(optimized.as_ref()).indent(true),
            @"RdfFileQuadPatternScanExec: active_graph=Default Graph, triple_pattern=[?s ?p ?o], blank_node_mode=Variable, projection=[s]"
        );

        let pushed_scan = optimized
            .as_any()
            .downcast_ref::<RdfFileQuadPatternScanExec>()
            .unwrap();

        assert_snapshot!(
            displayable(pushed_scan.inner_plan().as_ref()).indent(true),
            @"
        ProjectionExec: expr=[subject@1 as s]
          AggregateExec: mode=Single, gby=[graph@0 as graph, subject@1 as subject, predicate@2 as predicate, object@3 as object], aggr=[]
            FilterExec: graph@0 IS NULL
              DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[graph, subject, predicate, object], file_type=parquet, predicate=graph@0 IS NULL AND graph@0 IS NULL, pruning_predicate=graph_null_count@0 > 0 AND graph_null_count@0 > 0, required_guarantees=[]
        "
        );
    }

    #[tokio::test]
    async fn test_rdf_file_scan_multi_source() {
        let ctx = setup_test_context();
        let manager = RdfFileManager::new();

        let url1 = "memory://mem/test1.parquet";
        let url2 = "memory://mem/test2.parquet";

        write_quads_to_parquet(
            &ctx,
            url1,
            vec![make_named_quad(
                "http://example.org/g1",
                "http://example.org/s1",
                "http://example.org/p1",
                "http://example.org/o1",
            )],
        )
        .await;

        write_quads_to_parquet(
            &ctx,
            url2,
            vec![make_named_quad(
                "http://example.org/g2",
                "http://example.org/s2",
                "http://example.org/p2",
                "http://example.org/o2",
            )],
        )
        .await;

        let sources = vec![
            (
                GraphName::NamedNode(NamedNode::new_unchecked("http://example.org/g1")),
                RdfFileSourceConfig {
                    url: url1.to_string(),
                    format: RdfFormat::Parquet,
                },
            ),
            (
                GraphName::NamedNode(NamedNode::new_unchecked("http://example.org/g2")),
                RdfFileSourceConfig {
                    url: url2.to_string(),
                    format: RdfFormat::Parquet,
                },
            ),
        ];

        let quad_pattern = QuadPattern::all_quads();
        let schema = quad_pattern
            .compute_schema(&QuadStorageEncoding::String)
            .as_arrow()
            .clone();

        let scan = RdfFileQuadPatternScanExec::new(
            quad_pattern,
            manager,
            sources,
            QuadStorageEncoding::String,
            schema.into(),
            &ctx.state(),
            RdfFileStorageOptions::default(),
        )
        .await
        .unwrap();

        assert_snapshot!(
            displayable(&scan).indent(true),
            @"RdfFileQuadPatternScanExec: active_graph=All Graphs, graph_variable=?graph, triple_pattern=[?subject ?predicate ?object], blank_node_mode=Variable"
        );
        assert_snapshot!(
            displayable(scan.inner_plan().as_ref()).indent(true),
            @"
        AggregateExec: mode=Single, gby=[graph@0 as graph, subject@1 as subject, predicate@2 as predicate, object@3 as object], aggr=[]
          CoalescePartitionsExec
            UnionExec
              DataSourceExec: file_groups={1 group: [[test1.parquet]]}, projection=[graph, subject, predicate, object], file_type=parquet
              DataSourceExec: file_groups={1 group: [[test2.parquet]]}, projection=[graph, subject, predicate, object], file_type=parquet
        "
        );

        let results = datafusion::physical_plan::collect(Arc::new(scan), ctx.task_ctx())
            .await
            .unwrap();
        let schema = results[0].schema();
        let combined =
            datafusion::arrow::compute::concat_batches(&schema, &results).unwrap();

        let formatted =
            datafusion::arrow::util::pretty::pretty_format_batches(&[combined])
                .unwrap()
                .to_string();
        assert_snapshot!(formatted, @r"
        +-------------------------+-------------------------+-------------------------+-------------------------+
        | graph                   | subject                 | predicate               | object                  |
        +-------------------------+-------------------------+-------------------------+-------------------------+
        | <http://example.org/g1> | <http://example.org/s1> | <http://example.org/p1> | <http://example.org/o1> |
        | <http://example.org/g2> | <http://example.org/s2> | <http://example.org/p2> | <http://example.org/o2> |
        +-------------------------+-------------------------+-------------------------+-------------------------+
        ");
    }

    #[tokio::test]
    async fn test_rdf_file_scan_assume_unique() {
        let mut options = RdfFileStorageOptions::default();
        options.assume_quads_unique_in_single_file = true;

        let (_ctx, scan) =
            setup_custom_parquet_scan(QuadPattern::all_quads(), options).await;

        assert_snapshot!(
            displayable(scan.inner_plan().as_ref()).indent(true),
            @"DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[graph, subject, predicate, object], file_type=parquet"
        );
    }

    #[tokio::test]
    async fn test_rdf_file_scan_limit_pushdown_full_optimizer() {
        let (ctx, scan) = setup_in_memory_parquet_scan().await;

        let filter_expr =
            make_string_filter_expr(&scan.schema(), "s", "http://example.org/s1");
        let filter_exec =
            Arc::new(FilterExec::try_new(filter_expr, scan.clone()).unwrap());
        let limit_exec = Arc::new(GlobalLimitExec::new(filter_exec, 0, Some(1)));

        let optimized = optimize_plan(&ctx, limit_exec);

        assert_snapshot!(
            displayable(optimized.as_ref()).indent(true),
            @"RdfFileQuadPatternScanExec: active_graph=Default Graph, triple_pattern=[?s ?p ?o], blank_node_mode=Variable, pushed_filters=[s@0 = <http://example.org/s1>], fetch=1"
        );

        let pushed_scan = optimized
            .as_any()
            .downcast_ref::<RdfFileQuadPatternScanExec>()
            .unwrap();

        assert_snapshot!(
            displayable(pushed_scan.inner_plan().as_ref()).indent(true),
            @"
        ProjectionExec: expr=[subject@1 as s, predicate@2 as p, object@3 as o]
          GlobalLimitExec: skip=0, fetch=1
            AggregateExec: mode=Single, gby=[graph@0 as graph, subject@1 as subject, predicate@2 as predicate, object@3 as object], aggr=[], ordering_mode=PartiallySorted([1])
              FilterExec: subject@1 = <http://example.org/s1> AND graph@0 IS NULL
                DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[graph, subject, predicate, object], file_type=parquet, predicate=graph@0 IS NULL AND graph@0 IS NULL AND graph@0 IS NULL AND subject@1 = <http://example.org/s1>, pruning_predicate=graph_null_count@0 > 0 AND graph_null_count@0 > 0 AND graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= <http://example.org/s1> AND <http://example.org/s1> <= subject_max@2, required_guarantees=[subject in (<http://example.org/s1>)]
        "
        );
    }

    #[tokio::test]
    async fn test_rdf_file_scan_limit_pushdown_full_optimizer_unique() {
        let mut options = RdfFileStorageOptions::default();
        options.assume_quads_unique_in_single_file = true;

        let quad_pattern = QuadPattern::new(
            ActiveGraph::DefaultGraph,
            None,
            TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked("s")),
                predicate: rdf_fusion_common::NamedNodePattern::Variable(
                    Variable::new_unchecked("p"),
                ),
                object: TermPattern::Variable(Variable::new_unchecked("o")),
            },
            rdf_fusion_common::BlankNodeMatchingMode::Variable,
        );

        let (ctx, scan) = setup_custom_parquet_scan(quad_pattern, options).await;

        let filter_expr =
            make_string_filter_expr(&scan.schema(), "s", "http://example.org/s1");
        let filter_exec =
            Arc::new(FilterExec::try_new(filter_expr, scan.clone()).unwrap());
        let limit_exec = Arc::new(GlobalLimitExec::new(filter_exec, 0, Some(1)));

        let optimized = optimize_plan(&ctx, limit_exec);

        assert_snapshot!(
            displayable(optimized.as_ref()).indent(true),
            @"RdfFileQuadPatternScanExec: active_graph=Default Graph, triple_pattern=[?s ?p ?o], blank_node_mode=Variable, pushed_filters=[s@0 = <http://example.org/s1>], fetch=1"
        );

        let pushed_scan = optimized
            .as_any()
            .downcast_ref::<RdfFileQuadPatternScanExec>()
            .unwrap();

        assert_snapshot!(
            displayable(pushed_scan.inner_plan().as_ref()).indent(true),
            @"
        ProjectionExec: expr=[subject@0 as s, predicate@1 as p, object@2 as o]
          FilterExec: subject@1 = <http://example.org/s1> AND graph@0 IS NULL, projection=[subject@1, predicate@2, object@3], fetch=1
            DataSourceExec: file_groups={1 group: [[test.parquet]]}, projection=[graph, subject, predicate, object], file_type=parquet, predicate=graph@0 IS NULL AND graph@0 IS NULL AND graph@0 IS NULL AND subject@1 = <http://example.org/s1>, pruning_predicate=graph_null_count@0 > 0 AND graph_null_count@0 > 0 AND graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= <http://example.org/s1> AND <http://example.org/s1> <= subject_max@2, required_guarantees=[subject in (<http://example.org/s1>)]
        "
        );
    }

    /// Creates a mock named RDF quad.
    fn make_named_quad(
        g_uri: &str,
        s_uri: &str,
        p_uri: &str,
        o_uri: &str,
    ) -> rdf_fusion_common::Quad {
        let graph_node = NamedNode::new_unchecked(g_uri);
        rdf_fusion_common::Quad::new(
            NamedNode::new_unchecked(s_uri),
            NamedNode::new_unchecked(p_uri),
            NamedNode::new_unchecked(o_uri),
            rdf_fusion_common::GraphNameRef::NamedNode(graph_node.as_ref()),
        )
    }

    /// Generates a binary equality physical expression filtering a string-encoded column by a URI value.
    fn make_string_filter_expr(
        schema: &SchemaRef,
        col_name: &str,
        uri: &str,
    ) -> Arc<dyn PhysicalExpr> {
        use rdf_fusion_encoding::EncodingScalar;
        let idx = schema.index_of(col_name).unwrap();
        let scalar = STRING_ENCODING
            .encode_term(Ok(TermRef::NamedNode(
                NamedNode::new_unchecked(uri).as_ref(),
            )))
            .unwrap()
            .into_scalar_value();

        Arc::new(BinaryExpr::new(
            Arc::new(Column::new(col_name, idx)),
            Operator::Eq,
            Arc::new(Literal::new(scalar)),
        ))
    }

    /// Iteratively executes all configured physical optimizer rules on the provided plan.
    fn optimize_plan(
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let state = ctx.state();
        let mut optimized = plan;
        for rule in state.physical_optimizers() {
            optimized = rule.optimize(optimized, state.config_options()).unwrap();
        }
        optimized
    }

    /// Universal environment visualizer generator for custom execution-plan environments.
    async fn setup_custom_parquet_scan(
        quad_pattern: QuadPattern,
        options: RdfFileStorageOptions,
    ) -> (SessionContext, Arc<RdfFileQuadPatternScanExec>) {
        let ctx = setup_test_context();
        let manager = RdfFileManager::new();
        let url = "memory://mem/test.parquet";

        write_quads_to_parquet(
            &ctx,
            url,
            vec![
                rdf_fusion_common::Quad::new(
                    NamedNode::new_unchecked("http://example.org/s1"),
                    NamedNode::new_unchecked("http://example.org/p1"),
                    NamedNode::new_unchecked("http://example.org/o1"),
                    rdf_fusion_common::GraphNameRef::DefaultGraph,
                ),
                rdf_fusion_common::Quad::new(
                    NamedNode::new_unchecked("http://example.org/s2"),
                    NamedNode::new_unchecked("http://example.org/p2"),
                    NamedNode::new_unchecked("http://example.org/o2"),
                    rdf_fusion_common::GraphNameRef::DefaultGraph,
                ),
            ],
        )
        .await;

        let sources = vec![(
            GraphName::DefaultGraph,
            RdfFileSourceConfig {
                url: url.to_string(),
                format: RdfFormat::Parquet,
            },
        )];

        let schema = quad_pattern
            .compute_schema(&QuadStorageEncoding::String)
            .as_arrow()
            .clone();

        let scan = RdfFileQuadPatternScanExec::new(
            quad_pattern,
            manager,
            sources,
            QuadStorageEncoding::String,
            schema.into(),
            &ctx.state(),
            options,
        )
        .await
        .unwrap();

        (ctx, Arc::new(scan))
    }

    /// Sets up a standard default graph parquet scan environment for tracking basic scenarios.
    async fn setup_in_memory_parquet_scan()
    -> (SessionContext, Arc<RdfFileQuadPatternScanExec>) {
        let quad_pattern = QuadPattern::new(
            ActiveGraph::DefaultGraph,
            None,
            TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked("s")),
                predicate: rdf_fusion_common::NamedNodePattern::Variable(
                    Variable::new_unchecked("p"),
                ),
                object: TermPattern::Variable(Variable::new_unchecked("o")),
            },
            rdf_fusion_common::BlankNodeMatchingMode::Variable,
        );
        setup_custom_parquet_scan(quad_pattern, RdfFileStorageOptions::default()).await
    }

    fn setup_test_context() -> SessionContext {
        let ctx = SessionContext::new();
        let object_store = Arc::new(InMemory::new());
        ctx.runtime_env().register_object_store(
            &Url::parse("memory://mem").unwrap(),
            Arc::clone(&object_store) as _,
        );
        ctx
    }

    async fn write_quads_to_parquet(
        ctx: &SessionContext,
        url: &str,
        quads: Vec<rdf_fusion_common::Quad>,
    ) {
        let mut builder = StringQuadsBuilder::with_capacity(quads.len());
        for quad in quads {
            builder.append_quad(quad.as_ref());
        }
        let batch = builder.finish().into_record_batch();

        ctx.read_batch(batch)
            .unwrap()
            .write_parquet(
                url,
                DataFrameWriteOptions::new().with_single_file_output(true),
                None,
            )
            .await
            .unwrap();
    }
}
