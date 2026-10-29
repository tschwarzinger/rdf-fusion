use crate::PushdownProjection;
use crate::reader::ParquetMetadataFetcher;
use crate::scan::ParquetQuadScanExec;
use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{ScalarValue, internal_datafusion_err};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SessionState, SessionStateBuilder, TaskContext};
use datafusion::logical_expr::{Expr, Operator, col, lit};
use datafusion::parquet::arrow::parquet_column;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, Column, DynamicFilterPhysicalExpr, InListExpr, Literal,
};
use datafusion::physical_plan::{
    ExecutionPlan, PhysicalExpr, StatisticsArgs, StatisticsContext,
};
use futures::FutureExt;
use rdf_fusion_common::{DFResult, QuadComponent, RdfSortOrder};
use rdf_fusion_logical::quad_pattern::QuadPattern;
use rdf_fusion_storage::quad_tables::is_component_bound;
use std::cmp::Reverse;
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tracing::debug;

/// The factor by which the alternative scan must read fewer rows than the planned scan.
///
/// The factor accounts for the inaccuracy of the estimates and for the cost of planning the
/// alternative scan at execution time.
const SWITCH_FACTOR: usize = 4;

/// The quad tables that a [`QuadTableSwitch`] can switch between. All quad tables contain the same
/// quads.
#[async_trait]
pub trait SwitchableQuadTables: Debug + Send + Sync {
    /// Returns the components by which the quads of each quad table are natively sorted, or
    /// [`None`] if the quad table is not natively sorted.
    fn native_sort_orders(&self) -> Vec<Option<Vec<QuadComponent>>>;

    /// Builds the scan of the `pattern` from the quad table at `idx`, applying the `filter` in
    /// addition to the filters of the pattern. The files are split into (at most)
    /// `num_partitions` groups. The scan must not switch itself.
    async fn build_scan(
        &self,
        idx: usize,
        pattern: &QuadPattern,
        projection: PushdownProjection,
        state: &SessionState,
        filter: Expr,
        num_partitions: usize,
    ) -> DFResult<Arc<dyn ExecutionPlan>>;
}

/// Allows a `ParquetQuadScanExec` to switch to another quad table at execution time.
///
/// # Decision
///
/// The decision is made once, on the first poll of any partition, and shared by all partitions:
///
/// 1. **Find a lookup.** The scan inspects the complete dynamic filters that have been pushed into
///    it. HashJoinExec (`CollectLeft` and `Partitioned`) completes its filter before it polls the
///    probe side, so the scan never waits for a filter. If a filter requires
///    `column IN (v1, ..., vk)` for a column that is not bound in the pattern, and another quad
///    table sorts this column right after the bound columns, this quad table can look up the `k`
///    values (see `Self::lookup_table`).
/// 2. **Check a cheap bound.** A lookup reads about `k` times the rows per page of the planned
///    files. If this is not `SWITCH_FACTOR` times fewer than the planned scan's estimated rows,
///    the scan keeps the planned quad table without loading any metadata of the other quad table.
/// 3. **Plan the alternative scan.** The alternative scan is planned with eager pruning, using the
///    IN list as an additional filter. This loads the metadata of the other quad table and yields
///    an accurate estimate. The bound of step 2 may be too low, for example, if a value has more
///    rows than fit into a page.
/// 4. **Compare.** The scan switches if the alternative scan's estimated rows are `SWITCH_FACTOR`
///    times fewer than the planned scan's.
///
/// # Equivalence
///
/// The alternative scan uses the same pattern, projection, and number of partitions. Partition `i`
/// of the switched scan executes partition `i` of the alternative scan. As the alternative scan is
/// ordered differently, a scan that declares an output ordering never switches.
#[derive(Clone)]
pub struct QuadTableSwitch {
    quad_tables: Arc<dyn SwitchableQuadTables>,
    /// The index of the planned quad table in `quad_tables`.
    planned_table: usize,
    projection: PushdownProjection,
    decision: Arc<OnceCell<Arc<SwitchDecision>>>,
}

/// A quad table that can look up the values of an IN list of a dynamic filter.
struct Lookup {
    component: QuadComponent,
    values: Vec<ScalarValue>,
    /// The index of the quad table in [`QuadTableSwitch::quad_tables`].
    table: usize,
}

impl QuadTableSwitch {
    pub fn new(
        quad_tables: Arc<dyn SwitchableQuadTables>,
        planned_table: usize,
        projection: PushdownProjection,
    ) -> Self {
        Self {
            quad_tables,
            planned_table,
            projection,
            decision: Arc::new(OnceCell::new()),
        }
    }

    /// Returns a copy without a decision, for a new instance of the scan.
    pub(crate) fn reset(&self) -> Self {
        Self {
            decision: Arc::new(OnceCell::new()),
            ..self.clone()
        }
    }

    /// Returns the decision, if it has already been made.
    pub(crate) fn current_decision(&self) -> Option<&Arc<SwitchDecision>> {
        self.decision.get()
    }

    /// Returns the decision for the `planned` scan, making it if necessary.
    pub(crate) async fn decision(
        &self,
        planned: &ParquetQuadScanExec,
        context: &TaskContext,
    ) -> Arc<SwitchDecision> {
        let decision = self
            .decision
            .get_or_init(|| {
                Box::pin(async {
                    let decision = self.decide(planned, context).await;
                    debug!(
                        "Quad table switch for [{}]: {decision}",
                        planned.quad_pattern().triple_pattern
                    );
                    Arc::new(decision)
                })
            })
            .await;
        Arc::clone(decision)
    }

    async fn decide(
        &self,
        planned: &ParquetQuadScanExec,
        context: &TaskContext,
    ) -> SwitchDecision {
        let inner = planned.inner_scan();
        let filters = dynamic_filters(inner);
        if filters.is_empty() {
            return SwitchDecision::NoDynamicFilter;
        }
        if inner.fetch().is_some() || planned.properties().output_ordering().is_some() {
            return SwitchDecision::kept("the scan has a limit or an output ordering");
        }

        // Step 1: Find a lookup.
        let pattern = planned.quad_pattern();
        let mut candidates = filters
            .iter()
            .filter_map(|filter| filter.downcast_ref::<DynamicFilterPhysicalExpr>())
            // A complete filter does not change anymore. Waiting for an incomplete filter (e.g.,
            // from a TopK) could deadlock.
            .filter(|filter| filter.wait_complete().now_or_never().is_some())
            .filter_map(|filter| filter.current().ok())
            .flat_map(|filter| in_list_conditions(&filter))
            .filter_map(|condition| {
                let component = component_of_column(&condition.column)?;
                let bound = is_component_bound(
                    component,
                    &pattern.active_graph,
                    &pattern.triple_pattern,
                    pattern.blank_node_mode,
                );
                (!bound).then_some((component, condition.values))
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return SwitchDecision::kept("the dynamic filter has no IN list");
        }
        // The fewest values promise the most selective lookup.
        candidates.sort_by_key(|(_, values)| values.len());
        let Some(lookup) = candidates.into_iter().find_map(|(component, values)| {
            let table = self.lookup_table(pattern, component)?;
            Some(Lookup {
                component,
                values,
                table,
            })
        }) else {
            return SwitchDecision::kept(
                "no other quad table supports lookups of the IN list",
            );
        };

        match self.try_switch(planned, context, lookup).await {
            Ok(decision) => decision,
            Err(error) => SwitchDecision::kept(format!(
                "planning the alternative scan failed: {error}"
            )),
        }
    }

    /// Performs steps 2 to 4 of the decision for the `lookup`.
    async fn try_switch(
        &self,
        planned: &ParquetQuadScanExec,
        context: &TaskContext,
        lookup: Lookup,
    ) -> DFResult<SwitchDecision> {
        let inner = planned.inner_scan();
        let num_values = lookup.values.len();
        let column = lookup.component.column_name();
        let Some(planned_rows) = estimated_rows(inner.as_ref())? else {
            return Ok(SwitchDecision::kept("the planned scan has no row estimate"));
        };

        // Step 2: Check a cheap bound.
        let state = session_state(context);
        let rows_per_page = rows_per_page(&state, inner, column).await?;
        if num_values
            .saturating_mul(rows_per_page)
            .saturating_mul(SWITCH_FACTOR)
            > planned_rows
        {
            return Ok(SwitchDecision::kept(format!(
                "{num_values} values x {rows_per_page} rows per page are not clearly fewer \
                 than {planned_rows} rows"
            )));
        }

        // Step 3: Plan the alternative scan.
        let num_partitions = planned.properties().output_partitioning().partition_count();
        let filter = lookup_filter(column, lookup.values);
        let alternative = self
            .quad_tables
            .build_scan(
                lookup.table,
                planned.quad_pattern(),
                self.projection.clone(),
                &state,
                filter,
                num_partitions,
            )
            .await?;
        // If the filter cannot be pushed into the scan, the scan is wrapped (e.g., in a FilterExec).
        let Some(alternative) = alternative.downcast_ref::<ParquetQuadScanExec>() else {
            return Ok(SwitchDecision::kept(
                "the alternative scan cannot push down the pattern",
            ));
        };
        let alternative = Arc::new(alternative.clone());
        if alternative.schema() != planned.schema()
            || alternative
                .properties()
                .output_partitioning()
                .partition_count()
                > num_partitions
        {
            return Err(internal_datafusion_err!(
                "The alternative scan has a different schema or more partitions"
            ));
        }

        // Step 4: Compare.
        // Only natively sorted quad tables support lookups (see `lookup_table`).
        let table = self.quad_tables.native_sort_orders()[lookup.table]
            .clone()
            .map(|components| RdfSortOrder::NativeOrder(components).to_string())
            .unwrap_or_default();
        let estimated_rows = estimated_rows(alternative.as_ref())?.unwrap_or(usize::MAX);
        if estimated_rows.saturating_mul(SWITCH_FACTOR) > planned_rows {
            return Ok(SwitchDecision::Kept {
                reason: format!(
                    "{table} estimates {estimated_rows} rows for {num_values} values, \
                     which are not clearly fewer than {planned_rows} rows"
                ),
                alternative: Some(alternative),
            });
        }
        Ok(SwitchDecision::Switched(SwitchedScan {
            table,
            column,
            num_values,
            planned_rows,
            estimated_rows,
            scan: alternative,
        }))
    }

    /// Returns the quad table that can look up values of the `component`, in addition to the bound
    /// components of the `pattern`. This is a natively sorted quad table whose sort order starts
    /// with bound components and the `component`. On ties, the first quad table is chosen.
    ///
    /// Returns [`None`] if the planned quad table already supports these lookups.
    fn lookup_table(
        &self,
        pattern: &QuadPattern,
        component: QuadComponent,
    ) -> Option<usize> {
        // The number of leading components of the sort order that are bound or the component, if
        // the component is among them.
        let lookup_prefix = |components: Option<&[QuadComponent]>| {
            let prefix = components?
                .iter()
                .take_while(|c| {
                    **c == component
                        || is_component_bound(
                            **c,
                            &pattern.active_graph,
                            &pattern.triple_pattern,
                            pattern.blank_node_mode,
                        )
                })
                .collect::<Vec<_>>();
            prefix.contains(&&component).then_some(prefix.len())
        };

        let sort_orders = self.quad_tables.native_sort_orders();
        if lookup_prefix(sort_orders[self.planned_table].as_deref()).is_some() {
            return None;
        }
        sort_orders
            .into_iter()
            .enumerate()
            .filter_map(|(idx, components)| {
                Some((lookup_prefix(components.as_deref())?, Reverse(idx)))
            })
            .max()
            .map(|(_, Reverse(idx))| idx)
    }
}

impl Debug for QuadTableSwitch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuadTableSwitch")
            .field("planned_table", &self.planned_table)
            .field("projection", &self.projection)
            .field("decision", &self.decision.get())
            .finish()
    }
}

/// Whether a scan switched to another quad table.
#[derive(Debug)]
pub enum SwitchDecision {
    /// The scan has no dynamic filter. Hence, the planned scan is used.
    NoDynamicFilter,
    /// The planned scan is used.
    Kept {
        reason: String,
        /// The alternative scan, if it has been planned. Its planning metrics are reported.
        alternative: Option<Arc<ParquetQuadScanExec>>,
    },
    /// The scan switched to another quad table.
    Switched(SwitchedScan),
}

impl SwitchDecision {
    fn kept(reason: impl Into<String>) -> Self {
        Self::Kept {
            reason: reason.into(),
            alternative: None,
        }
    }

    /// Returns the alternative scan, if it has been planned.
    pub(crate) fn alternative_scan(&self) -> Option<&Arc<ParquetQuadScanExec>> {
        match self {
            SwitchDecision::NoDynamicFilter => None,
            SwitchDecision::Kept { alternative, .. } => alternative.as_ref(),
            SwitchDecision::Switched(switched) => Some(&switched.scan),
        }
    }
}

impl Display for SwitchedScan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}, estimated_rows={}]",
            self.table, self.estimated_rows
        )
    }
}

impl Display for SwitchDecision {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SwitchDecision::NoDynamicFilter => write!(f, "no dynamic filter"),
            SwitchDecision::Kept { reason, .. } => write!(f, "[kept: {reason}]"),
            SwitchDecision::Switched(switched) => {
                write!(
                    f,
                    "[switched to {} for {} values of {}, estimated_rows={} instead of {}]",
                    switched.table,
                    switched.num_values,
                    switched.column,
                    switched.estimated_rows,
                    switched.planned_rows,
                )
            }
        }
    }
}

/// A scan that switched to another quad table.
///
/// The display only shows the quad table and the estimated rows (see [`SwitchDecision`] for
/// the details).
#[derive(Debug)]
pub struct SwitchedScan {
    /// The name of the quad table (e.g., `Native(GSPO)`).
    table: String,
    /// The column that is looked up.
    column: &'static str,
    num_values: usize,
    planned_rows: usize,
    estimated_rows: usize,
    /// The alternative scan.
    scan: Arc<ParquetQuadScanExec>,
}

impl SwitchedScan {
    /// Returns the scan that is executed instead of the planned scan. It has at most as many
    /// partitions as the planned scan. The remaining partitions are empty.
    pub(crate) fn scan(&self) -> &Arc<ParquetQuadScanExec> {
        &self.scan
    }
}

/// Creates a [`SessionState`] for planning the alternative scan during execution. The state has
/// the scalar functions of the `context`, as the filters of the pattern may refer to them.
fn session_state(context: &TaskContext) -> SessionState {
    SessionStateBuilder::new()
        .with_config(context.session_config().clone())
        .with_runtime_env(context.runtime_env())
        .with_scalar_functions(context.scalar_functions().values().cloned().collect())
        .build()
}

/// Returns the dynamic filters that have been pushed into the Parquet source of the `scan`.
///
/// The filters are expressed in terms of the columns of the quad table, as the source maps the
/// pushed filters through its projection.
fn dynamic_filters(scan: &DataSourceExec) -> Vec<Arc<dyn PhysicalExpr>> {
    let Some(predicate) = scan
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .and_then(|config| config.file_source().filter())
    else {
        return Vec::new();
    };

    let mut filters = Vec::new();
    // The closure never fails.
    let _ = predicate.apply(|expr| {
        if expr.is::<DynamicFilterPhysicalExpr>() {
            filters.push(Arc::clone(expr));
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    filters
}

/// Returns the filter that restricts `column` to the `values` of a lookup.
///
/// The filter is a disjunction of equalities instead of an IN list, as DataFusion only prunes row
/// groups and pages with IN lists of up to 20 values (see `MAX_IN_LIST_SIZE`). Without pruning,
/// the alternative scan's estimate would cover the whole range of the values. The cheap bound
/// (step 2) keeps the number of values small.
fn lookup_filter(column: &str, values: Vec<ScalarValue>) -> Expr {
    values
        .into_iter()
        .map(|value| col(column).eq(lit(value)))
        .reduce(Expr::or)
        .unwrap_or_else(|| lit(false))
}

/// Returns the estimated number of rows of the `plan`, if known.
fn estimated_rows(plan: &dyn ExecutionPlan) -> DFResult<Option<usize>> {
    let statistics = StatisticsContext::new().compute(plan, &StatisticsArgs::new())?;
    Ok(statistics.num_rows.get_value().copied())
}

/// Returns the files of the `scan`.
fn planned_files(
    scan: &DataSourceExec,
) -> Vec<datafusion::datasource::listing::PartitionedFile> {
    scan.data_source()
        .downcast_ref::<FileScanConfig>()
        .map(|config| {
            config
                .file_groups
                .iter()
                .flat_map(|group| group.iter().cloned())
                .collect()
        })
        .unwrap_or_default()
}

/// A necessary condition `column IN (values)` of a filter.
#[derive(Debug, Clone, PartialEq)]
struct InListCondition {
    column: String,
    values: Vec<ScalarValue>,
}

/// Returns the conditions `column IN (values)` that every row matching `expr` fulfills.
///
/// HashJoinExec creates the following filters:
/// - `CollectLeft`: `col >= min AND col <= max AND col IN (SET) ([..])` (the IN list is missing if
///   the build side has too many distinct values).
/// - `Partitioned`: `CASE hash(col) % n WHEN 0 THEN <filter of partition 0> ... ELSE false END`.
///   A row matches the filter of at most one partition. Hence, a column is restricted to the union
///   of the IN lists of all partitions, if each partition has an IN list for it.
fn in_list_conditions(expr: &Arc<dyn PhysicalExpr>) -> Vec<InListCondition> {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
        && *binary.op() == Operator::And
    {
        let mut conditions = in_list_conditions(binary.left());
        conditions.extend(in_list_conditions(binary.right()));
        return conditions;
    }

    if let Some(in_list) = expr.downcast_ref::<InListExpr>() {
        if in_list.negated() {
            return Vec::new();
        }
        let Some(column) = in_list.expr().downcast_ref::<Column>() else {
            return Vec::new();
        };
        let Some(values) = in_list
            .list()
            .iter()
            .map(|value| Some(value.downcast_ref::<Literal>()?.value().clone()))
            .collect::<Option<Vec<_>>>()
        else {
            return Vec::new();
        };
        // A null value never matches.
        let values = values
            .into_iter()
            .filter(|value| !value.is_null())
            .collect();
        return vec![InListCondition {
            column: column.name().to_owned(),
            values,
        }];
    }

    if let Some(case) = expr.downcast_ref::<CaseExpr>() {
        let branches = case
            .when_then_expr()
            .iter()
            .map(|(_, then)| then)
            .chain(case.else_expr())
            .filter(|branch| !is_false(branch))
            .map(in_list_conditions)
            .collect::<Vec<_>>();
        let Some((first, rest)) = branches.split_first() else {
            // No row matches.
            return Vec::new();
        };
        return first
            .iter()
            .filter_map(|condition| {
                let mut values = condition.values.clone();
                for branch in rest {
                    let other = branch
                        .iter()
                        .find(|other| other.column == condition.column)?;
                    values.extend(other.values.iter().cloned());
                }
                let mut seen = HashSet::new();
                values.retain(|value| seen.insert(value.clone()));
                Some(InListCondition {
                    column: condition.column.clone(),
                    values,
                })
            })
            .collect();
    }

    Vec::new()
}

fn is_false(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<Literal>()
        .is_some_and(|literal| literal.value() == &ScalarValue::Boolean(Some(false)))
}

fn component_of_column(column: &str) -> Option<QuadComponent> {
    QuadComponent::all()
        .into_iter()
        .find(|component| component.column_name() == column)
}

/// Returns the average number of rows per page of the `column` in the files of the `scan`.
///
/// The metadata of the files has been loaded while planning the scan, so it is usually served
/// from the metadata cache. The pages are taken from the offset index. If a file has no (loaded)
/// offset index, its row groups are used instead, which overestimates the rows per page.
async fn rows_per_page(
    state: &SessionState,
    scan: &DataSourceExec,
    column: &str,
) -> DFResult<usize> {
    let config = scan
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .ok_or_else(|| internal_datafusion_err!("Expected a FileScanConfig"))?;
    let store = state.runtime_env().object_store(&config.object_store_url)?;
    let fetcher = ParquetMetadataFetcher::new(state, store);
    let schema = Arc::clone(config.file_schema());

    let (mut rows, mut pages) = (0usize, 0usize);
    for file in planned_files(scan) {
        let metadata = fetcher
            .fetch(&file.object_meta, PageIndexPolicy::Skip)
            .await?;
        let (file_rows, file_pages) = count_rows_and_pages(&metadata, &schema, column);
        rows += file_rows;
        pages += file_pages;
    }
    Ok(rows.div_ceil(pages.max(1)).max(1))
}

/// Returns the number of rows and pages of the `column` in a file. Without an offset index for the
/// column, each row group counts as a single page.
fn count_rows_and_pages(
    metadata: &ParquetMetaData,
    schema: &datafusion::arrow::datatypes::Schema,
    column: &str,
) -> (usize, usize) {
    let leaf = parquet_column(metadata.file_metadata().schema_descr(), schema, column)
        .map(|(idx, _)| idx);
    let mut rows = 0;
    let mut pages = 0;
    for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
        rows += usize::try_from(row_group.num_rows()).unwrap_or(0);
        let num_pages = leaf
            .and_then(|leaf| metadata.offset_index()?.get(rg_idx)?.get(leaf))
            .map(|offset_index| offset_index.page_locations().len())
            .filter(|num_pages| *num_pages > 0);
        pages += num_pages.unwrap_or(1);
    }
    (rows, pages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::datasource::physical_plan::parquet::PagePruningAccessPlanFilter;
    use datafusion::logical_expr::execution_props::ExecutionProps;
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion::physical_expr::expressions::{col, lit};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("subject", DataType::Utf8, false),
            Field::new("object", DataType::Utf8, false),
        ])
    }

    fn in_list(column: &str, values: &[&str]) -> Arc<dyn PhysicalExpr> {
        let schema = schema();
        Arc::new(
            InListExpr::try_new(
                col(column, &schema).unwrap(),
                values.iter().map(|v| lit(*v)).collect(),
                false,
                &schema,
            )
            .unwrap(),
        )
    }

    fn condition(column: &str, values: &[&str]) -> InListCondition {
        InListCondition {
            column: column.to_owned(),
            values: values.iter().map(|v| ScalarValue::from(*v)).collect(),
        }
    }

    /// Returns the number of predicates that DataFusion's page pruning uses from `filter`.
    fn page_pruning_predicates(filter: &Expr) -> usize {
        let schema = Arc::new(schema());
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let expr = create_physical_expr(
            filter,
            &df_schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )
        .unwrap();
        PagePruningAccessPlanFilter::new(&expr, schema).filter_number()
    }

    #[test]
    fn lookup_filter_prunes_pages_for_many_values() {
        let values = (0..40)
            .map(|i| ScalarValue::from(format!("v{i:02}")))
            .collect::<Vec<_>>();
        // An IN list with more than 20 values is not used for pruning.
        let in_list = datafusion::logical_expr::col("subject").in_list(
            values
                .iter()
                .cloned()
                .map(datafusion::logical_expr::lit)
                .collect(),
            false,
        );
        assert_eq!(page_pruning_predicates(&in_list), 0);
        assert_eq!(
            page_pruning_predicates(&lookup_filter("subject", values)),
            1
        );
    }

    #[test]
    fn extracts_in_list_from_conjunction() {
        let schema = schema();
        let bounds = Arc::new(BinaryExpr::new(
            col("subject", &schema).unwrap(),
            Operator::GtEq,
            lit("a"),
        ));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            bounds,
            Operator::And,
            in_list("subject", &["a", "b"]),
        ));
        assert_eq!(
            in_list_conditions(&expr),
            vec![condition("subject", &["a", "b"])]
        );
    }

    #[test]
    fn ignores_disjunctions_and_negations() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            in_list("subject", &["a"]),
            Operator::Or,
            in_list("subject", &["b"]),
        ));
        assert_eq!(in_list_conditions(&expr), vec![]);

        let schema = schema();
        let negated: Arc<dyn PhysicalExpr> = Arc::new(
            InListExpr::try_new(
                col("subject", &schema).unwrap(),
                vec![lit("a")],
                true,
                &schema,
            )
            .unwrap(),
        );
        assert_eq!(in_list_conditions(&negated), vec![]);
    }

    #[test]
    fn unions_in_lists_of_partitioned_case() {
        let schema = schema();
        let case: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(
                Some(col("object", &schema).unwrap()),
                vec![
                    (lit(0u64), in_list("subject", &["a", "b"])),
                    (lit(1u64), lit(false)),
                    (lit(2u64), in_list("subject", &["c"])),
                ],
                Some(lit(false)),
            )
            .unwrap(),
        );
        assert_eq!(
            in_list_conditions(&case),
            vec![condition("subject", &["a", "b", "c"])]
        );
    }

    #[test]
    fn partitioned_case_requires_in_list_in_every_partition() {
        let schema = schema();
        let case: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(
                Some(col("object", &schema).unwrap()),
                vec![
                    (lit(0u64), in_list("subject", &["a"])),
                    (lit(1u64), lit(true)),
                ],
                Some(lit(false)),
            )
            .unwrap(),
        );
        assert_eq!(in_list_conditions(&case), vec![]);
    }
}
