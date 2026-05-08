use crate::delta::log::{DeltaQuadStorageLog, DeltaStorageLogVersionRange};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Statistics, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{ScalarFunctionExpr, conjunction};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, SchedulingType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::metrics::{Metric, MetricValue};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use futures::Stream;
use rdf_fusion_common::DFResult;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::any::Any;
use std::borrow::Cow;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A physical execution plan for scanning a [`DeltaQuadStorage`].
///
/// For now, this is mostly a marker in the query plan that helps with debugging, and most of its
/// methods simply delegate to the inner plan.
#[derive(Debug)]
pub struct DeltaQuadStorageScanExec {
    log: Arc<DeltaQuadStorageLog>,
    quad_pattern: QuadPattern,
    changeset_version: Option<DeltaStorageLogVersionRange>,
    inner: Arc<dyn ExecutionPlan>,
    index_name: Option<String>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    pushed_down_filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl DeltaQuadStorageScanExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        log: Arc<DeltaQuadStorageLog>, // TODO Snapshot
        quad_pattern: QuadPattern,
        changeset_version: Option<DeltaStorageLogVersionRange>,
        inner: Arc<dyn ExecutionPlan>,
        index_name: Option<String>,
    ) -> DFResult<Self> {
        // TODO: Ensure that all plans are cooperative (Parquet Scan is but maybe)
        let properties = inner
            .properties()
            .as_ref()
            .clone()
            .with_scheduling_type(SchedulingType::Cooperative);

        Ok(Self {
            log: Arc::clone(&log),
            quad_pattern,
            changeset_version,
            inner,
            index_name,
            properties: Arc::new(properties),
            metrics: ExecutionPlanMetricsSet::new(),
            pushed_down_filters: vec![],
        })
    }

    /// Builder method to easily clone and attach new pushed filters
    pub fn with_pushed_filters(mut self, filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        self.pushed_down_filters = filters;
        self
    }

    /// Provides access to the underlying execution plan that implements the actual scan. This
    /// should mostly be used for debugging purposes.
    #[cfg(test)]
    pub(crate) fn inner_scan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }
}

impl ExecutionPlan for DeltaQuadStorageScanExec {
    fn name(&self) -> &str {
        "DeltaQuadStorageScanExec"
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
                "DeltaQuadStorageScanExec is opaque and cannot accept new children"
            );
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(DeltaQuadStorageScanStream {
            inner: inner_stream,
            baseline_metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let mut set = self.metrics.clone_inner();

        extract_inner_metrics(&self.inner, &mut set);

        return Some(set);

        /// Recursively find and alias target metrics
        fn extract_inner_metrics(plan: &Arc<dyn ExecutionPlan>, set: &mut MetricsSet) {
            if let Some(metrics) = plan.metrics() {
                for metric in metrics.iter() {
                    let name_opt = match metric.value() {
                        MetricValue::Count { name, .. } => Some(name.as_ref()),
                        MetricValue::Time { name, .. } => Some(name.as_ref()),
                        MetricValue::Gauge { name, .. } => Some(name.as_ref()),
                        MetricValue::PruningMetrics { name, .. } => Some(name.as_ref()),
                        _ => None,
                    };

                    if let Some(name) = name_opt {
                        // Using `starts_with` handles DataFusion's implicit `_matched`
                        // and `_total` suffixes for pruning metrics transparently.
                        let target_prefixes = [
                            "time_elapsed_processing",
                            "time_elapsed_opening",
                            "files_pruned",
                            "files_scanned",
                            "row_groups_pruned_statistics",
                            "page_index_rows_pruned",
                        ];

                        if target_prefixes
                            .iter()
                            .any(|prefix| name.starts_with(prefix))
                        {
                            let new_name: Cow<'static, str> =
                                format!("index_{name}").into();

                            // Clone the underlying atomic references so the new metric updates automatically
                            let new_value = match metric.value() {
                                MetricValue::Count { count, .. } => MetricValue::Count {
                                    name: new_name,
                                    count: count.clone(),
                                },
                                MetricValue::Time { time, .. } => MetricValue::Time {
                                    name: new_name,
                                    time: time.clone(),
                                },
                                MetricValue::Gauge { gauge, .. } => MetricValue::Gauge {
                                    name: new_name,
                                    gauge: gauge.clone(),
                                },
                                MetricValue::PruningMetrics {
                                    pruning_metrics, ..
                                } => MetricValue::PruningMetrics {
                                    name: new_name,
                                    pruning_metrics: pruning_metrics.clone(),
                                },
                                _ => unreachable!(),
                            };

                            // Push the newly aliased metric
                            set.push(Arc::new(Metric::new(
                                new_value,
                                metric.partition(),
                            )));
                        }
                    }
                }
            }

            // Recurse down the execution plan tree
            for child in plan.children() {
                extract_inner_metrics(child, set);
            }
        }
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        self.inner.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let new_scan = self.inner.with_fetch(limit)?;
        Some(Arc::new(Self {
            log: Arc::clone(&self.log),
            quad_pattern: self.quad_pattern.clone(),
            changeset_version: self.changeset_version,
            inner: new_scan,
            index_name: self.index_name.clone(),
            properties: Arc::clone(&self.properties),
            metrics: ExecutionPlanMetricsSet::new(),
            pushed_down_filters: self.pushed_down_filters.clone(),
        }))
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
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

        // Only push down filters that don't contain UDFs (e.g., no pushdown of encoding changes)
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

        // Use the resulting filter exec as the new node for running the filter pushdown rule
        let combined_expr = conjunction(pushable.clone());
        let filter_exec =
            Arc::new(FilterExec::try_new(combined_expr, Arc::clone(&self.inner))?);
        let rule = match phase {
            FilterPushdownPhase::Pre => FilterPushdown::new(),
            FilterPushdownPhase::Post => FilterPushdown::new_post_optimization(),
        };
        let optimized_inner = rule.optimize(filter_exec, config)?;

        // Build the new plan. If there is still a filter at the top of this plan, hoist it above
        // the new scan.
        let new_pushed_down_filters =
            [self.pushed_down_filters.clone(), pushable.clone()].concat();
        let final_plan = match optimized_inner.as_any().downcast_ref::<FilterExec>() {
            None => {
                let new_plan = Self::try_new(
                    Arc::clone(&self.log),
                    self.quad_pattern.clone(),
                    self.changeset_version,
                    optimized_inner,
                    self.index_name.clone(),
                )?
                .with_pushed_filters(new_pushed_down_filters);
                Arc::new(new_plan) as Arc<dyn ExecutionPlan>
            }
            Some(inner_filter) => {
                let inner_filter_child = Arc::clone(inner_filter.children()[0]);
                let new_quad_scan = Self::try_new(
                    Arc::clone(&self.log),
                    self.quad_pattern.clone(),
                    self.changeset_version,
                    inner_filter_child,
                    self.index_name.clone(),
                )?
                .with_pushed_filters(new_pushed_down_filters);

                // Hoist the filter above the new scan if the filter has not been pushed down.
                // This can be affected by the `options.execution.parquet.pushdown_filters` option.
                let remaining_predicate = Arc::clone(inner_filter.predicate());
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

        let new_plan = Self::try_new(
            Arc::clone(&self.log),
            self.quad_pattern.clone(),
            self.changeset_version,
            inner,
            self.index_name.clone(),
        )?
        .with_pushed_filters(self.pushed_down_filters.clone());
        Ok(Some(Arc::new(new_plan) as Arc<dyn ExecutionPlan>))
    }
}

impl DisplayAs for DeltaQuadStorageScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DeltaQuadStorageScanExec:")?;
        write!(f, " index={}", self.index_name.as_deref().unwrap_or("None"))?;
        write!(f, ", active_graph={}", self.quad_pattern.active_graph)?;
        write!(
            f,
            ", triple_pattern=[{} {} {}]",
            &self.quad_pattern.triple_pattern.subject,
            &self.quad_pattern.triple_pattern.predicate,
            &self.quad_pattern.triple_pattern.object
        )?;
        write!(f, ", blank_node_mode={}", self.quad_pattern.blank_node_mode)?;

        if let Some(changeset_version) = self.changeset_version {
            write!(f, ", changeset_version={changeset_version}")?;
        }

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

        if let Some(fetch) = self.inner.fetch() {
            write!(f, ", fetch={fetch}")?;
        }

        Ok(())
    }
}

/// A wrapping stream that records the metrics for the scan.
struct DeltaQuadStorageScanStream {
    inner: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl Stream for DeltaQuadStorageScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        let mut timer = elapsed_compute.timer();
        let poll = self.inner.as_mut().poll_next(cx);
        timer.stop();

        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for DeltaQuadStorageScanStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
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
    use crate::delta::DeltaQuadStorage;
    use crate::delta::scan_plan_builder::DeltaQuadStorageScanPlanBuilder;
    use crate::index::IndexComponents;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{
        ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    };
    use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion::scalar::ScalarValue;
    use deltalake::arrow::datatypes::Field;
    use insta::assert_snapshot;
    use rdf_fusion_common::{
        BlankNodeMatchingMode, NamedNode, TermPattern, TriplePattern, Variable,
    };
    use rdf_fusion_encoding::QuadStorageEncodingName;
    use rdf_fusion_extensions::storage::QuadStorage;
    use rdf_fusion_logical::ActiveGraph;
    use std::sync::OnceLock;

    #[tokio::test]
    async fn test_pushdown_successful_on_index_scan() {
        let (session, scan) = setup_quad_scan(true).await;

        let filter_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("p", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(123)))), // Object ID encoding
        ));

        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            FilterExec::try_new(filter_expr, Arc::clone(&scan) as Arc<dyn ExecutionPlan>)
                .unwrap(),
        );

        let rule = FilterPushdown::new();
        let optimized_filter = rule
            .optimize(plan, session.state().config_options())
            .unwrap();

        let optimized_scan = optimized_filter
            .as_any()
            .downcast_ref::<DeltaQuadStorageScanExec>()
            .expect("top-level node should be a DeltaQuadStorageScanExec");

        assert_snapshot!(
            displayable(optimized_filter.as_ref()).indent(false),
            @"DeltaQuadStorageScanExec: index=GSPO, active_graph=Default Graph, triple_pattern=[<https://my.at/> ?p <https://my.at/>], blank_node_mode=Variable, pushed_filters=[p@0 = 123]"
        );

        // Assert the inner scan that the filter exists (see `predicate@2 = 123`)
        assert_snapshot!(
            displayable(optimized_scan.inner_scan().as_ref()).indent(false),
            @r"
        ProjectionExec: expr=[predicate@0 as p]
          DeltaScan
            DataSourceExec: file_groups={1 group: [[]]}, projection=[predicate], file_type=parquet, predicate=graph@1 IS NULL AND subject@3 = 0 AND object@2 = 0 AND predicate@2 = 123, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= 0 AND 0 <= subject_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= 0 AND 0 <= object_max@6 AND predicate_null_count@10 != row_count@4 AND predicate_min@8 <= 123 AND 123 <= predicate_max@9, required_guarantees=[object in (0), predicate in (123), subject in (0)]
        "
        );
    }

    #[tokio::test]
    async fn test_index_scan_without_parquet_pushdown_enabled() {
        let (_, scan) = setup_quad_scan(false).await;
        assert_snapshot!(
            displayable(scan.inner_scan().as_ref()).indent(false),
            @"
        ProjectionExec: expr=[predicate@0 as p]
          ProjectionExec: expr=[predicate@2 as predicate]
            FilterExec: graph@0 IS NULL AND subject@1 = 0 AND object@3 = 0
              DeltaScan
                DataSourceExec: file_groups={1 group: [[]]}, projection=[graph, subject, predicate, object], file_type=parquet
        "
        );
    }

    #[tokio::test]
    async fn test_pushdown_fails_with_udf() {
        let (session, scan) = setup_quad_scan(true).await;

        let udf = ScalarUDF::new_from_impl(MockUDF);
        let filter_expr = Arc::new(ScalarFunctionExpr::new(
            "mock_udf",
            Arc::new(udf),
            vec![Arc::new(Column::new("p", 0))],
            Arc::new(Field::new("result", DataType::Boolean, false)),
            Arc::new(ConfigOptions::default()),
        ));

        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(filter_expr, scan).unwrap());

        let rule = FilterPushdown::new();
        let optimized = rule
            .optimize(plan, session.state().config_options())
            .unwrap();

        assert_snapshot!(
            displayable(optimized.as_ref()).indent(false),
            @"
        FilterExec: mock_udf(p@0)
          DeltaQuadStorageScanExec: index=GSPO, active_graph=Default Graph, triple_pattern=[<https://my.at/> ?p <https://my.at/>], blank_node_mode=Variable
        "
        );
    }

    #[tokio::test]
    async fn test_projection_pushdown() {
        let quad_pattern = QuadPattern::new(
            ActiveGraph::DefaultGraph,
            None,
            TriplePattern {
                subject: Variable::new_unchecked("s").into(),
                predicate: Variable::new_unchecked("p").into(),
                object: Variable::new_unchecked("o").into(),
            },
            BlankNodeMatchingMode::Variable,
        );
        let (session, scan) = setup_quad_scan_with_pattern(quad_pattern, true).await;

        let exprs = vec![(
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
            "s".to_string(),
        )];

        let projection_plan: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(exprs, Arc::clone(&scan) as Arc<dyn ExecutionPlan>)
                .unwrap(),
        );
        let rule = ProjectionPushdown::new();
        let optimized_plan = rule
            .optimize(projection_plan, session.state().config_options())
            .unwrap();

        let optimized_scan = optimized_plan
            .as_any()
            .downcast_ref::<DeltaQuadStorageScanExec>()
            .expect("The outer ProjectionExec should have been absorbed by the DeltaQuadStorageScanExec");

        assert_snapshot!(
            displayable(optimized_scan).indent(false),
            @"DeltaQuadStorageScanExec: index=GSPO, active_graph=Default Graph, triple_pattern=[?s ?p ?o], blank_node_mode=Variable, projection=[s]"
        );

        // Delta does not (yet) pushdown these kind of projections, so we do not yet have any
        // performance benefits. But we are prepared to handle this case in the future.
        assert_snapshot!(
            displayable(optimized_scan.inner_scan().as_ref()).indent(false),
            @r"
        ProjectionExec: expr=[subject@0 as s]
          DeltaScan
            DataSourceExec: file_groups={1 group: [[]]}, projection=[subject, predicate, object], file_type=parquet, predicate=graph@3 IS NULL, pruning_predicate=graph_null_count@0 > 0, required_guarantees=[]
        "
        );
    }

    /// Common setup to create an opaque DeltaQuadStorageScanExec using the real scan builder
    async fn setup_quad_scan(
        parquet_pushdown: bool,
    ) -> (SessionContext, Arc<DeltaQuadStorageScanExec>) {
        let quad_pattern = QuadPattern::new(
            ActiveGraph::DefaultGraph,
            None,
            TriplePattern {
                subject: TermPattern::NamedNode(NamedNode::new_unchecked(
                    "https://my.at/",
                )),
                predicate: Variable::new_unchecked("p").into(),
                object: TermPattern::NamedNode(NamedNode::new_unchecked(
                    "https://my.at/",
                )),
            },
            BlankNodeMatchingMode::Variable,
        );
        setup_quad_scan_with_pattern(quad_pattern, parquet_pushdown).await
    }

    async fn setup_quad_scan_with_pattern(
        quad_pattern: QuadPattern,
        parquet_pushdown: bool,
    ) -> (SessionContext, Arc<DeltaQuadStorageScanExec>) {
        let mut config = SessionConfig::new().with_target_partitions(1);
        let options = config.options_mut();
        options.execution.parquet.pushdown_filters = parquet_pushdown;

        let session = SessionContext::new_with_config(config);
        let storage = Arc::new(
            DeltaQuadStorage::new_in_memory(
                QuadStorageEncodingName::ObjectId,
                vec![IndexComponents::GSPO],
            )
            .await,
        );

        let builder = DeltaQuadStorageScanPlanBuilder::new(
            session.state(),
            quad_pattern.clone(),
            storage.encoding().clone(),
        );
        let plan_result = builder
            .with_best_index(&storage.index_snapshots().await.unwrap())
            .expect("Failed to apply best index")
            .build()
            .await
            .expect("Failed to build scan plan");

        let scan = DeltaQuadStorageScanExec::try_new(
            storage.log().clone(),
            quad_pattern,
            plan_result.changeset_version_range,
            plan_result.scan,
            plan_result.chosen_index.map(|idx| idx.to_string()),
        )
        .unwrap();

        (session, Arc::new(scan))
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct MockUDF;
    impl ScalarUDFImpl for MockUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "mock_udf"
        }
        fn signature(&self) -> &Signature {
            static SIG: OnceLock<Signature> = OnceLock::new();
            SIG.get_or_init(|| Signature::any(1, Volatility::Immutable))
        }
        fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
            Ok(DataType::Boolean)
        }
        fn invoke_with_args(
            &self,
            _args: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            unimplemented!()
        }
    }
}
