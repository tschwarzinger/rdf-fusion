use crate::memory::storage::predicate_pushdown::MemStoragePredicateExpr;
use crate::memory::storage::scan::PlannedPatternScan;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Statistics, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSource;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

/// The physical operator for evaluating a quad pattern against a [MemQuadStorage](crate::memory::MemQuadStorage).
#[derive(Debug, Clone)]
pub struct MemQuadPatternDataSource {
    /// The schema of the data source.
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// A [PlannedPatternScan] that represents a scan that is about to be executed.
    planned_scan: PlannedPatternScan,
}

impl MemQuadPatternDataSource {
    /// Creates a new [MemQuadPatternDataSource].
    pub fn new(schema: SchemaRef, stream: PlannedPatternScan) -> Self {
        Self {
            schema,
            planned_scan: stream,
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }
}

impl DataSource for MemQuadPatternDataSource {
    fn open(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("Only partition 0 is supported for now.");
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let result = self.planned_scan.clone().create_stream(baseline_metrics);
        if result.schema() != self.schema {
            return exec_err!("Unexpected schema for quad pattern stream.");
        }

        Ok(result)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.planned_scan)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(Arc::clone(&self.schema))
    }

    fn scheduling_type(&self) -> SchedulingType {
        SchedulingType::Cooperative
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.metrics.clone()
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> DFResult<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        let parent_filters = filters
            .clone()
            .into_iter()
            .map(|f| {
                let rewritten = MemStoragePredicateExpr::try_from(&f);
                (f, rewritten)
            })
            .collect::<Vec<_>>();

        let filter_pushdowns = parent_filters
            .iter()
            .map(|(_, rewritten)| match rewritten {
                None => PushedDown::No,
                Some(_) => PushedDown::Yes,
            })
            .collect::<Vec<_>>();

        // Don't create a new node if no filters were pushed down
        if filter_pushdowns.iter().all(|r| matches!(r, PushedDown::No)) {
            return Ok(FilterPushdownPropagation {
                filters: filter_pushdowns,
                updated_node: None,
            });
        }

        let filters = parent_filters
            .iter()
            .filter_map(|(_, f)| f.clone())
            .collect::<Vec<_>>();
        let updated_scan = apply_pushdown_filters(&self.planned_scan, &filters)?;
        let updated_node = Arc::new(MemQuadPatternDataSource::new(
            Arc::clone(&self.schema),
            updated_scan,
        ));

        Ok(FilterPushdownPropagation {
            filters: filter_pushdowns,
            updated_node: Some(updated_node),
        })
    }
}

/// Applies the given filters to the given pattern by extending the filters
fn apply_pushdown_filters(
    original: &PlannedPatternScan,
    filters: &[MemStoragePredicateExpr],
) -> DFResult<PlannedPatternScan> {
    let mut scan = original.clone();
    for filter in filters {
        scan = scan.apply_filter(filter)?;
    }

    scan.try_find_better_index()
}

#[cfg(test)]
mod test {
    use crate::memory::storage::MemQuadPatternDataSource;
    use crate::memory::storage::snapshot::PlanPatternScanResult;
    use crate::memory::{MemObjectIdMapping, MemQuadStorage};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::memory::DataSourceExec;
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::source::DataSource;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_plan::filter_pushdown::{
        FilterPushdownPropagation, PushedDown,
    };
    use datafusion::physical_plan::{PhysicalExpr, displayable};
    use datafusion::scalar::ScalarValue;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::object_id::{ObjectIdEncoding, ObjectIdMapping};
    use rdf_fusion_logical::ActiveGraph;
    use rdf_fusion_model::{
        BlankNodeMatchingMode, NamedNode, NamedNodePattern, TermPattern,
        TriplePattern, Variable,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_filter_pushdown_binds_variable_eq() {
        let exec = create_test_pattern().await;
        let filter_expr = create_object_filter_expr(Operator::Eq, 1);
        let result = execute_filter_pushdown(exec, filter_expr);

        assert!(result.filters.iter().all(|f| matches!(f, PushedDown::Yes)));
        assert!(result.updated_node.is_some());
        assert_snapshot!(
            format_quad_pattern(result.updated_node.unwrap()),
            @"DataSourceExec: [GPOS] subject=?subject, predicate=<http://example.com/test>, object=?object, additional_filters=[object == [0, 0, 0, 1]]"
        )
    }

    #[tokio::test]
    async fn test_filter_pushdown_binds_variable_comparison() {
        let exec = create_test_pattern().await;
        let filter_expr = create_object_filter_expr(Operator::Gt, 1);
        let result = execute_filter_pushdown(exec, filter_expr);

        assert!(result.filters.iter().all(|f| matches!(f, PushedDown::Yes)));
        assert!(result.updated_node.is_some());
        assert_snapshot!(
            format_quad_pattern(result.updated_node.unwrap()),
            @"DataSourceExec: [GPOS] subject=?subject, predicate=<http://example.com/test>, object=?object, additional_filters=[object in ([0, 0, 0, 2]..[FF, FF, FF, FF])]"
        )
    }

    #[tokio::test]
    async fn test_filter_pushdown_binds_variable_between() {
        let exec = create_test_pattern().await;
        let gt = create_object_filter_expr(Operator::Gt, 1);
        let lt = create_object_filter_expr(Operator::Lt, 10);
        let filter_expr = Arc::new(BinaryExpr::new(gt, Operator::And, lt));
        let result = execute_filter_pushdown(exec, filter_expr);

        assert!(result.filters.iter().all(|f| matches!(f, PushedDown::Yes)));
        assert!(result.updated_node.is_some());
        assert_snapshot!(
            format_quad_pattern(result.updated_node.unwrap()),
            @"DataSourceExec: [GPOS] subject=?subject, predicate=<http://example.com/test>, object=?object, additional_filters=[object in ([0, 0, 0, 2]..[0, 0, 0, 9])]"
        )
    }

    /// Creates a new [MemQuadPatternDataSource] for the pattern (?subject <...> ?object) and no graph
    /// variable.
    async fn create_test_pattern() -> MemQuadPatternDataSource {
        let schema = Arc::new(Schema::new(vec![
            Field::new("subject", DataType::FixedSizeBinary(4), false),
            Field::new("object", DataType::FixedSizeBinary(4), false),
        ]));
        let pattern = TriplePattern {
            subject: TermPattern::Variable(Variable::new_unchecked("subject")),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                "http://example.com/test",
            )),
            object: TermPattern::Variable(Variable::new_unchecked("object")),
        };

        let object_id_mapping = Arc::new(MemObjectIdMapping::default());
        object_id_mapping
            .encode_scalar(&rdf_fusion_encoding::plain_term::PlainTermScalar::from(
                rdf_fusion_model::NamedNodeRef::new_unchecked("http://example.com/test"),
            ))
            .unwrap();
        let encoding = Arc::new(ObjectIdEncoding::new(object_id_mapping));

        let index = MemQuadStorage::try_new(encoding, 10).unwrap();
        let planned_scan = index
            .snapshot()
            .await
            .plan_pattern_evaluation(
                ActiveGraph::DefaultGraph,
                None,
                pattern,
                BlankNodeMatchingMode::Filter,
            )
            .await
            .unwrap();

        match planned_scan {
            PlanPatternScanResult::Empty(_) => unreachable!("Unexpected empty result"),
            PlanPatternScanResult::PatternScan(planned_scan) => {
                MemQuadPatternDataSource::new(
                    Arc::new(schema.as_ref().clone()),
                    planned_scan,
                )
            }
        }
    }

    /// Creates a filter operation on the `object` column with the given `operator` and `value`.
    fn create_object_filter_expr(
        operator: Operator,
        value: u32,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("object", 1)),
            operator,
            Arc::new(Literal::new(ScalarValue::FixedSizeBinary(4, Some(value.to_be_bytes().to_vec())))),
        ))
    }

    /// Runs the filter push down on `exec` with the given `filter_expr`.
    fn execute_filter_pushdown(
        exec: MemQuadPatternDataSource,
        filter_expr: Arc<dyn PhysicalExpr>,
    ) -> FilterPushdownPropagation<Arc<dyn DataSource>> {
        exec.try_pushdown_filters(vec![filter_expr], &ConfigOptions::default())
            .unwrap()
    }

    /// Formats `data_source` as a string.
    fn format_quad_pattern(data_source: Arc<dyn DataSource>) -> String {
        displayable(&DataSourceExec::new(data_source))
            .indent(false)
            .to_string()
    }
}
