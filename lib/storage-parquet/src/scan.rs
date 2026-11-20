use crate::switch::{QuadTableSwitch, SwitchDecision};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Statistics, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{
    OrderingRequirements, PhysicalSortExpr, ScalarFunctionExpr,
};
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Metric, MetricValue, MetricsSet,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan,
    InputDistributionRequirements, PhysicalExpr, PlanProperties, SortOrderPushdownResult,
    StatisticsArgs, StatisticsContext,
};
use futures::{StreamExt, TryStreamExt};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::config::RdfFusionSessionConfigExt;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_encoding::{QuadStorageEncoding, with_rdf_term_extension_type};
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// The bytes read while planning the scan.
pub(crate) const BYTES_SCANNED_PLANNING: &str = "bytes_scanned_planning";
/// The bytes read while planning the alternative scan of a quad table switch.
const SWITCH_BYTES_SCANNED_PLANNING: &str = "switch_bytes_scanned_planning";

/// A physical execution plan for scanning quads in Parquet files.
///
/// This wraps a `DataSourceExec` and prevents pushing down expensive UDF filters into Parquet
/// scanning while allowing inexpensive filters (e.g., term equality) to be pushed down.
///
/// In addition to the metrics of the inner scan, this reports the metrics recorded while planning
/// the scan (e.g., `bytes_scanned_planning` for eager pruning).
///
/// If the storage has multiple quad tables, the scan may switch to another quad table at execution
/// time, once the dynamic filter of a hash join restricts a column to a few values (see
/// `QuadTableSwitch`). Then, the execution metrics of the alternative scan are reported instead
/// of the ones of the planned scan. In addition, `switch_bytes_scanned_planning` reports the bytes
/// read while planning the alternative scan.
///
/// # RDF Term Extension Types
///
/// The Parquet files do not store the RDF term extension types and the inner scan reads them with
/// their plain schema. This wrapper validates that every output column has the storage type of the
/// configured encoding and attaches the extension type of the encoding (zero-copy). The types of
/// the files are validated when the storage is opened (see `validate_file_term_types`).
///
/// As the inner scan does not carry the extension types, no functions on RDF terms are pushed into
/// it (neither filters nor projections).
#[derive(Debug, Clone)]
pub struct ParquetQuadScanExec {
    quad_pattern: QuadPattern,
    encoding: QuadStorageEncoding,
    inner: Arc<DataSourceExec>,
    properties: Arc<PlanProperties>,
    planning_metrics: ExecutionPlanMetricsSet,
    quad_table_switch: Option<QuadTableSwitch>,
}

impl ParquetQuadScanExec {
    pub(crate) fn try_new(
        quad_pattern: QuadPattern,
        encoding: QuadStorageEncoding,
        inner: Arc<DataSourceExec>,
        planning_metrics: ExecutionPlanMetricsSet,
        quad_table_switch: Option<QuadTableSwitch>,
    ) -> DFResult<Self> {
        let properties = Arc::new(annotate_properties(&encoding, inner.as_ref())?);
        Ok(Self {
            quad_pattern,
            encoding,
            inner,
            properties,
            planning_metrics,
            quad_table_switch,
        })
    }

    /// Returns the scanned quad pattern.
    pub fn quad_pattern(&self) -> &QuadPattern {
        &self.quad_pattern
    }

    /// Provides access to the underlying execution plan that implements the actual scan.
    ///
    /// Note that the inner scan does not carry the RDF term extension types.
    pub(crate) fn inner_scan(&self) -> &Arc<DataSourceExec> {
        &self.inner
    }

    /// Returns the decision whether the scan switched to another quad table, if it has been made.
    pub(crate) fn switch_decision(&self) -> Option<&Arc<SwitchDecision>> {
        self.quad_table_switch.as_ref()?.current_decision()
    }

    /// Wraps a new inner plan. Returns `None` if `inner` is not a [`DataSourceExec`] or does not
    /// produce RDF terms of the configured encoding (e.g., after pushing down a projection that
    /// computes another value).
    fn wrap_inner(&self, inner: Arc<dyn ExecutionPlan>) -> Option<Arc<Self>> {
        let downcast = inner.downcast_ref::<DataSourceExec>()?;
        Self::try_new(
            self.quad_pattern.clone(),
            self.encoding.clone(),
            Arc::new(downcast.clone()),
            self.planning_metrics.clone(),
            self.quad_table_switch.as_ref().map(QuadTableSwitch::reset),
        )
        .ok()
        .map(Arc::new)
    }
}

impl ParquetQuadScanExec {
    /// Pushes the columns referenced by `projection` into the scan and evaluates the
    /// `projection` above it. Returns `None` if the projection references all columns.
    fn push_referenced_columns(
        &self,
        projection: &ProjectionExec,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let schema = self.schema();
        let mut referenced = projection
            .expr()
            .iter()
            .flat_map(|e| collect_columns(&e.expr))
            .map(|c| c.index())
            .collect::<Vec<_>>();
        referenced.sort_unstable();
        referenced.dedup();
        if referenced.len() == schema.fields().len() {
            return Ok(None);
        }

        let column_projection = ProjectionExec::try_new(
            referenced.iter().map(|&idx| {
                let name = schema.field(idx).name();
                ProjectionExpr::new(
                    Arc::new(Column::new(name, idx)) as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            }),
            Arc::new(self.clone()) as Arc<dyn ExecutionPlan>,
        )?;
        let Some(scan) = self.try_swapping_with_projection(&column_projection)? else {
            return Ok(None);
        };

        // Re-bind the columns of the projection to the schema of the new scan.
        let scan_schema = scan.schema();
        let exprs = projection
            .expr()
            .iter()
            .map(|e| {
                let expr = Arc::clone(&e.expr)
                    .transform(|expr| {
                        let Some(column) = expr.downcast_ref::<Column>() else {
                            return Ok(Transformed::no(expr));
                        };
                        let index = scan_schema.index_of(column.name())?;
                        Ok(Transformed::yes(
                            Arc::new(Column::new(column.name(), index))
                                as Arc<dyn PhysicalExpr>,
                        ))
                    })?
                    .data;
                Ok(ProjectionExpr::new(expr, e.alias.clone()))
            })
            .collect::<DFResult<Vec<_>>>()?;
        Ok(Some(Arc::new(ProjectionExec::try_new(exprs, scan)?)))
    }
}

/// Computes the [`PlanProperties`] of the `inner` scan with the RDF term extension types of the
/// `encoding` attached to the schema.
///
/// Returns an error if a column of the `inner` scan does not have the storage type of `encoding`.
fn annotate_properties(
    encoding: &QuadStorageEncoding,
    inner: &DataSourceExec,
) -> DFResult<PlanProperties> {
    let inner_schema = inner.schema();
    let expected_type = encoding.term_type();
    let fields = inner_schema
        .fields()
        .iter()
        .map(|field| {
            if field.data_type() != expected_type {
                return plan_err!(
                    "Column '{}' of the Parquet quad scan has data type {}, but the {} encoding \
                    stores terms as {}.",
                    field.name(),
                    field.data_type(),
                    encoding,
                    expected_type
                );
            }
            Ok(with_rdf_term_extension_type(
                field.as_ref().clone(),
                encoding.encoding_name(),
            ))
        })
        .collect::<DFResult<Vec<_>>>()?;
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        inner_schema.metadata().clone(),
    ));

    let eq_properties = inner
        .properties()
        .eq_properties
        .clone()
        .with_new_schema(schema)?;
    Ok(inner
        .properties()
        .as_ref()
        .clone()
        .with_eq_properties(eq_properties))
}

/// Validates that the term columns of a Parquet file (described by its `footer`) have the storage
/// type of the `encoding`. The file at `location` is only used for the error message.
///
/// As the extension types are not stored in the files, this ensures that the files can be read as
/// RDF terms of the `encoding`. This also accepts files written by other tools, as long as they use
/// the same storage types.
pub(crate) fn validate_file_term_types(
    encoding: &QuadStorageEncoding,
    footer: &ParquetMetaData,
    location: &str,
) -> DFResult<()> {
    let file_metadata = footer.file_metadata();
    let file_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;
    let expected_type = encoding.term_type();
    for column in [COL_GRAPH, COL_SUBJECT, COL_PREDICATE, COL_OBJECT] {
        let Ok(field) = file_schema.field_with_name(column) else {
            return plan_err!("The Parquet file {location} has no column '{column}'.");
        };
        if !field.data_type().equals_datatype(expected_type) {
            return plan_err!(
                "Column '{column}' of the Parquet file {location} has data type {}, but the {} \
                encoding stores terms as {}.",
                field.data_type(),
                encoding,
                expected_type
            );
        }
    }
    Ok(())
}

/// Replaces the schema of the batches in `stream` with `schema`, which must only differ in the
/// field metadata. This is a zero-copy operation.
fn with_schema(
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
) -> SendableRecordBatchStream {
    let stream_schema = Arc::clone(&schema);
    let stream = stream.map(move |batch| {
        let batch = batch?;
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&stream_schema),
            batch.columns().to_vec(),
            &options,
        )?)
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
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

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties.eq_properties.schema())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn check_invariants(&self, check: InvariantLevel) -> DFResult<()> {
        self.inner.check_invariants(check)
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.inner.input_distribution_requirements()
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

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
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
        let switching_enabled = context
            .session_config()
            .rdf_fusion_options_or_default()
            .execution
            .quad_table_switching;
        let Some(switch) = self.quad_table_switch.clone().filter(|_| switching_enabled)
        else {
            return Ok(with_schema(
                self.inner.execute(partition, context)?,
                self.schema(),
            ));
        };

        // The dynamic filters are only populated once the build side of the join is complete.
        // Hence, the decision is made on the first poll of any partition, which a hash join only
        // does after completing its filter.
        let planned = self.clone();
        let stream = futures::stream::once(async move {
            let decision = switch.decision(&planned, &context).await;
            match decision.as_ref() {
                SwitchDecision::Switched(switched) => {
                    let plan = switched.scan();
                    if partition
                        < plan.properties().output_partitioning().partition_count()
                    {
                        plan.execute(partition, context)
                    } else {
                        Ok(Box::pin(EmptyRecordBatchStream::new(planned.schema()))
                            as SendableRecordBatchStream)
                    }
                }
                _ => Ok(with_schema(
                    planned.inner.execute(partition, context)?,
                    planned.schema(),
                )),
            }
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let decision = self.switch_decision().map(Arc::as_ref);
        // Only the scan that has been executed reports execution metrics.
        let executed = match decision {
            Some(SwitchDecision::Switched(switched)) => &switched.scan().inner,
            _ => &self.inner,
        };
        let mut metrics = executed.metrics().unwrap_or_default();
        for metric in self.planning_metrics.clone_inner().iter() {
            metrics.push(Arc::clone(metric));
        }

        if let Some(alternative) = decision.and_then(SwitchDecision::alternative_scan) {
            let bytes = alternative
                .planning_metrics
                .clone_inner()
                .sum_by_name(BYTES_SCANNED_PLANNING)
                .map_or(0, |value| value.as_usize());
            metrics.push(count_metric(SWITCH_BYTES_SCANNED_PLANNING, bytes));
        }
        Some(metrics)
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> DFResult<Arc<Statistics>> {
        StatisticsContext::new()
            .compute(self, &StatisticsArgs::new().with_partition(partition))
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> DFResult<Arc<Statistics>> {
        self.inner.statistics_from_inputs(input_stats, args)
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.inner.child_stats_requests(partition)
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
        // The inner scan does not carry the RDF term extension types. Hence, functions on RDF
        // terms cannot be evaluated within the inner scan. Instead, only the referenced columns
        // are pushed into the scan and the functions are evaluated above it.
        if projection.expr().iter().any(|e| contains_udf(&e.expr)) {
            return self.push_referenced_columns(projection);
        }

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

        // By default, only a switch is shown. The verbose format also explains why a scan kept
        // the planned quad table.
        match (t, self.switch_decision().map(Arc::as_ref)) {
            (_, None | Some(SwitchDecision::NoDynamicFilter)) => {}
            (DisplayFormatType::Verbose, Some(decision)) => {
                write!(f, ", quad_table_switch={decision}")?;
            }
            (_, Some(SwitchDecision::Switched(switched))) => {
                write!(f, ", quad_table_switch={switched}")?;
            }
            (_, Some(SwitchDecision::Kept { .. })) => {}
        }

        Ok(())
    }
}

/// Helper function to check if a physical expression contains a Scalar UDF.
fn contains_udf(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if expr.is::<ScalarFunctionExpr>() {
        return true;
    }
    for child in expr.children() {
        if contains_udf(child) {
            return true;
        }
    }
    false
}

/// Creates a global count metric with the given `name` and `value`.
fn count_metric(name: &'static str, value: usize) -> Arc<Metric> {
    let count = Count::new();
    count.add(value);
    Arc::new(Metric::new(
        MetricValue::Count {
            name: name.into(),
            count,
        },
        None,
    ))
}
