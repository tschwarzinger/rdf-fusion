use crate::object_id::exec::{DecodeObjectIdsExec, ObjectIdDecodingExecProjection};
use crate::quad_pattern::MaterializedQuadPatternExec;
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{
    Column, DFSchema, DataFusionError, JoinSide, JoinType, NullEquality,
    Result as DFResult, ScalarValue, Statistics, plan_err,
};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::utils::{expr_to_columns, split_conjunction};
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column as PhysicalColumn, Literal};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter, JoinOn};
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode,
};
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, StatisticsArgs, StatisticsContext,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::future::try_join_all;
use rdf_fusion_common::Term;
use rdf_fusion_encoding::object_id::is_object_id_data_type;
use rdf_fusion_encoding::plain_term::PlainTermArray;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::string::StringTermArray;
use rdf_fusion_encoding::{QuadStorageEncoding, TermDecoder};
use rdf_fusion_logical::bgp::BgpNode;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct BgpPlanner {
    decoding_udf: Option<Arc<ScalarUDF>>,
    /// The planners that are used for planning quad patterns with bound variables. If empty, no
    /// variables are bound (see [`BgpPlanner::bind_single_row_values`]).
    quad_pattern_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

#[async_trait]
impl ExtensionPlanner for BgpPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(bgp) = node.as_any().downcast_ref::<BgpNode>() else {
            return Ok(None);
        };

        let physical_inputs = &self
            .bind_single_row_values(
                planner,
                logical_inputs,
                physical_inputs,
                session,
                planning_ctx,
            )
            .await?;

        if bgp.patterns.is_empty() {
            return Ok(Some(Arc::new(PlaceholderRowExec::new(Arc::new(
                bgp.schema.as_arrow().clone(),
            )))));
        }

        // 1. Flatten all logical AND filters & compute required columns
        let flat_filters: Vec<Expr> = bgp
            .filters
            .iter()
            .flat_map(split_conjunction)
            .cloned()
            .collect();

        let mut top_level_needs: HashSet<String> = bgp
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        for filter in &flat_filters {
            let mut cols = HashSet::new();
            if expr_to_columns(filter, &mut cols).is_ok() {
                top_level_needs.extend(cols.into_iter().map(|c| c.name));
            }
        }
        top_level_needs.extend(bgp.columns_to_decode.iter().map(|c| c.name.clone()));

        let mut needed_columns = top_level_needs.clone();
        let mut column_occurrences: HashMap<String, usize> = HashMap::new();

        for exec in physical_inputs {
            for field in exec.schema().fields() {
                let base_name = field.name();
                *column_occurrences.entry(base_name.to_string()).or_default() += 1;
            }
        }
        for (col, count) in column_occurrences {
            if count > 1 {
                needed_columns.insert(col);
            }
        }

        // 2. Prepare Leaf Patterns
        let prepared = self.prepare_and_sort_patterns(
            planner,
            physical_inputs,
            &flat_filters,
            &bgp.columns_to_decode,
            &needed_columns,
            session,
            planning_ctx,
        )?;

        let (mut patterns, mut pending_filters) = match prepared {
            Some(result) => result,
            None => {
                return Ok(Some(Arc::new(EmptyExec::new(Arc::new(
                    bgp.schema.as_arrow().clone(),
                )))));
            }
        };

        // 3. Build the Join Tree
        let mut exec = patterns.remove(0);
        while !patterns.is_empty() {
            let next_idx = self.find_next_join_pattern(
                &exec,
                &patterns,
                &pending_filters,
                &bgp.columns_to_decode,
            );
            let next = patterns.remove(next_idx);

            let mut needed_after_join = top_level_needs.clone();
            for pattern in &patterns {
                for field in pattern.schema().fields() {
                    needed_after_join.insert(field.name().to_string());
                }
            }

            exec = self.join_execs(
                planner,
                exec,
                next,
                &needed_after_join,
                &bgp.columns_to_decode,
                &mut pending_filters,
                session,
                planning_ctx,
            )?;

            // Apply cross-column filters post-join
            exec = self.apply_ready_filters(
                planner,
                exec,
                &mut pending_filters,
                &bgp.columns_to_decode,
                session,
                planning_ctx,
            )?;
        }

        // 4. Determine filter needs
        let mut filter_needs = HashSet::new();
        for filter in &pending_filters {
            let mut cols = HashSet::new();
            if expr_to_columns(filter, &mut cols).is_ok() {
                filter_needs.extend(cols.into_iter().map(|c| c.name));
            }
        }

        // 5. Decode only what's needed for filters
        exec =
            self.decode_columns_for_filters(exec, bgp, &top_level_needs, &filter_needs)?;

        // 6. Apply any remaining filters that were waiting on decoded strings
        exec = self.apply_ready_filters(
            planner,
            exec,
            &mut pending_filters,
            &[],
            session,
            planning_ctx,
        )?;

        // 7. Apply any final filters
        for filter in pending_filters {
            let df_schema = DFSchema::from_unqualified_fields(
                exec.schema().fields().clone(),
                HashMap::new(),
            )?;
            let phys_expr = planner.create_physical_expr(
                &filter,
                &df_schema,
                session,
                planning_ctx,
            )?;
            exec = Arc::new(FilterExec::try_new(phys_expr, exec)?);
        }

        // 8. Decode remaining columns
        exec = self.decode_final_columns(exec, bgp)?;

        // 9. Final Projection to align with bgp.schema
        let mut final_projection = Vec::with_capacity(bgp.schema.fields().len());
        for field in bgp.schema.fields() {
            let field_name = field.name();
            let idx = exec.schema().index_of(field_name)?;
            final_projection.push((
                Arc::new(PhysicalColumn::new(exec.schema().field(idx).name(), idx)) as _,
                field_name.to_string(),
            ));
        }
        exec = Arc::new(ProjectionExec::try_new(final_projection, exec)?);

        Ok(Some(exec))
    }
}

/// A list of prepared sort patterns, along with any remaining filters that need to be applied.
type PreparedSortPatterns = Option<(Vec<Arc<dyn ExecutionPlan>>, Vec<Expr>)>;

impl BgpPlanner {
    pub fn new(decoding_udf: Option<Arc<ScalarUDF>>) -> Self {
        Self {
            decoding_udf,
            quad_pattern_planners: Vec::new(),
        }
    }

    /// Sets the planners that are used for planning quad patterns with bound variables. The
    /// planners are tried in order.
    pub fn with_quad_pattern_planners(
        mut self,
        planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        self.quad_pattern_planners = planners;
        self
    }

    /// Binds the values of materialized patterns with a single row in the other quad patterns of
    /// the BGP.
    ///
    /// For example, if `?person dblp:creatorName "Tobias Schwarzinger"` has a single match, the
    /// pattern `?publication dblp:authoredBy ?person` is re-planned as
    /// `?publication dblp:authoredBy <https://dblp.org/pid/...>`. The bound variables are re-added
    /// as constant columns, such that the schema of the inputs does not change. As the single-row
    /// pattern remains in the BGP, the join produces the same result.
    ///
    /// The approach is as follows:
    /// 1. The [`QuadPatternPlanner`] materializes the small scans (concurrently) while planning the
    ///    leaves of the query (see [`MaterializedQuadPatternExec`]).
    /// 2. If a materialized pattern has a single row, its values are bound in the patterns that
    ///    have not been materialized. The bound patterns are re-planned (concurrently). Small
    ///    patterns are thereby materialized as well, which may uncover new single-row patterns.
    ///    Hence, this step is repeated until no new values are bound.
    ///
    /// [`QuadPatternPlanner`]: crate::quad_pattern::QuadPatternPlanner
    async fn bind_single_row_values(
        &self,
        planner: &dyn PhysicalPlanner,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Vec<Arc<dyn ExecutionPlan>>> {
        if self.quad_pattern_planners.is_empty()
            || logical_inputs.len() != physical_inputs.len()
        {
            return Ok(physical_inputs.to_vec());
        }

        let mut nodes = logical_inputs
            .iter()
            .map(|plan| match plan {
                LogicalPlan::Extension(extension) => extension
                    .node
                    .as_any()
                    .downcast_ref::<QuadPatternNode>()
                    .cloned(),
                _ => None,
            })
            .collect::<Vec<_>>();
        // The plans of the patterns (without the re-added bound columns).
        let mut plans = physical_inputs.to_vec();
        let mut bindings = Bindings::new();

        loop {
            // Collect the values of the materialized patterns with a single row.
            let num_bindings = bindings.len();
            for (node, plan) in nodes.iter().zip(&plans) {
                let (Some(node), Some(row)) = (node, single_row(plan)) else {
                    continue;
                };
                for (field, column) in row.schema().fields().iter().zip(row.columns()) {
                    if bindings.contains_key(field.name()) {
                        continue;
                    }
                    if let Some(term) =
                        decode_single_term(node.storage_encoding(), column)
                    {
                        let value = ScalarValue::try_from_array(column, 0)?;
                        bindings.insert(field.name().clone(), (term, value));
                    }
                }
            }
            if bindings.len() == num_bindings {
                break;
            }

            // Re-plan the patterns that have not been materialized with the bound variables.
            let terms = bindings
                .iter()
                .map(|(name, (term, _))| (name.clone(), term.clone()))
                .collect::<HashMap<_, _>>();
            let mut tasks = Vec::new();
            for (idx, (node, plan)) in nodes.iter().zip(&plans).enumerate() {
                let Some(node) = node.as_ref().filter(|_| !is_materialized(plan)) else {
                    continue;
                };
                let Some(bound) = node.bind_variables(&terms)? else {
                    continue;
                };
                tasks.push(async move {
                    let plan = self
                        .plan_quad_pattern(planner, &bound, session, planning_ctx)
                        .await?;
                    Ok::<_, DataFusionError>((idx, bound, plan))
                });
            }
            for (idx, bound, plan) in try_join_all(tasks).await? {
                plans[idx] = plan;
                nodes[idx] = Some(bound);
            }
        }

        // Re-add the bound variables that have been removed from the re-planned patterns.
        plans
            .into_iter()
            .zip(physical_inputs)
            .map(|(plan, input)| {
                add_bound_columns(plan, input.schema().fields(), &bindings)
            })
            .collect()
    }

    /// Plans `node` with the first of the [`Self::quad_pattern_planners`] that supports it.
    async fn plan_quad_pattern(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &QuadPatternNode,
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        for quad_pattern_planner in &self.quad_pattern_planners {
            let exec = quad_pattern_planner
                .plan_extension(planner, node, &[], &[], session, planning_ctx)
                .await?;
            if let Some(exec) = exec {
                return Ok(exec);
            }
        }
        plan_err!(
            "No planner for quad pattern {}",
            node.quad_pattern().triple_pattern
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_and_sort_patterns(
        &self,
        planner: &dyn PhysicalPlanner,
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        filters: &[Expr],
        columns_to_decode: &[Column],
        needed_columns: &HashSet<String>,
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<PreparedSortPatterns> {
        let mut patterns = Vec::new();
        let mut pending_filters = filters.to_vec();

        // Check if any input is guaranteed to have no rows, and return early if so.
        for exec in physical_inputs {
            let stats = StatisticsContext::new()
                .compute(exec.as_ref(), &StatisticsArgs::new())?;
            if let Precision::Exact(0) = stats.num_rows {
                return Ok(None);
            }
        }

        for exec in physical_inputs {
            let mut current_exec = Arc::clone(exec);

            current_exec = self.apply_ready_filters(
                planner,
                current_exec,
                &mut pending_filters,
                columns_to_decode,
                session,
                planning_ctx,
            )?;

            let mut projection = Vec::new();
            for (idx, field) in current_exec.schema().fields().iter().enumerate() {
                let base_name = field.name();

                if needed_columns.contains(base_name) {
                    projection.push((
                        Arc::new(PhysicalColumn::new(field.name(), idx)) as _,
                        base_name.to_string(),
                    ));
                }
            }
            current_exec = Arc::new(ProjectionExec::try_new(projection, current_exec)?);

            current_exec = self.apply_ready_filters(
                planner,
                current_exec,
                &mut pending_filters,
                columns_to_decode,
                session,
                planning_ctx,
            )?;

            let stats = StatisticsContext::new()
                .compute(exec.as_ref(), &StatisticsArgs::new())?;
            let bytes = self.pattern_scan_bytes(&stats)?;
            let is_materialized = is_materialized(exec);
            patterns.push((current_exec, is_materialized, bytes));
        }

        // Materialized patterns are joined first, as they require no further I/O. Furthermore,
        // their size (in memory) is not comparable to the estimated size of the scans that have
        // not been executed (usually compressed bytes in the storage). Within both groups,
        // patterns are ordered by their size.
        patterns.sort_by_key(|(_, is_materialized, bytes)| (!is_materialized, *bytes));
        Ok(Some((
            patterns.into_iter().map(|(exec, _, _)| exec).collect(),
            pending_filters,
        )))
    }

    fn pattern_scan_bytes(&self, stats: &Statistics) -> DFResult<usize> {
        if let Some(bytes) = stats.total_byte_size.get_value() {
            return Ok(*bytes);
        }
        Ok(usize::MAX)
    }

    fn apply_ready_filters(
        &self,
        planner: &dyn PhysicalPlanner,
        mut exec: Arc<dyn ExecutionPlan>,
        pending_filters: &mut Vec<Expr>,
        columns_to_decode: &[Column],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let schema = exec.schema();
        let mut ready_filters = Vec::new();
        let use_oids = self.decoding_udf.is_some();

        pending_filters.retain(|filter| {
            let mut cols = HashSet::new();
            if expr_to_columns(filter, &mut cols).is_err() {
                return true;
            }

            let all_present = cols.iter().all(|c| schema.index_of(&c.name).is_ok());
            let needs_decode = use_oids
                && !columns_to_decode.is_empty()
                && cols
                    .iter()
                    .any(|c| columns_to_decode.iter().any(|dc| dc.name == c.name));

            if all_present && !needs_decode {
                ready_filters.push(filter.clone());
                false
            } else {
                true
            }
        });

        for filter in ready_filters {
            let df_schema = DFSchema::from_unqualified_fields(
                exec.schema().fields().clone(),
                HashMap::new(),
            )?;
            let phys_expr = planner.create_physical_expr(
                &filter,
                &df_schema,
                session,
                planning_ctx,
            )?;
            exec = Arc::new(FilterExec::try_new(phys_expr, exec)?);
        }

        Ok(exec)
    }

    /// Returns the index of the pattern in `patterns` that should be joined next with
    /// `current_exec`.
    ///
    /// 1. A small pattern that is only connected to `current_exec` via a pending filter (e.g.,
    ///    `FILTER(?a < ?b)`) is joined as soon as the filter can be evaluated. Joining it early
    ///    (with a nested loop join) acts as a filter. Otherwise, such patterns are only joined at
    ///    the end, after all other (potentially large) patterns have been joined.
    /// 2. Otherwise, the first pattern that shares a column with `current_exec` and enables
    ///    joining such a small pattern in the next step is chosen.
    /// 3. Otherwise, the first pattern that shares a column with `current_exec` is chosen.
    /// 4. Otherwise, the first pattern is chosen (cross join).
    fn find_next_join_pattern(
        &self,
        current_exec: &Arc<dyn ExecutionPlan>,
        patterns: &[Arc<dyn ExecutionPlan>],
        pending_filters: &[Expr],
        columns_to_decode: &[Column],
    ) -> usize {
        let current_schema = current_exec.schema();
        // Evaluating a filter on columns that must be decoded (object ids) decodes the columns in
        // the join tree. Joining such columns with columns that have not been decoded would fail.
        // Hence, filters that require decoding are not considered for joining patterns early.
        let pending_filters = pending_filters
            .iter()
            .filter(|filter| !references_any(filter, columns_to_decode))
            .cloned()
            .collect::<Vec<_>>();
        let pending_filters = pending_filters.as_slice();
        let shares_column = |pattern: &Arc<dyn ExecutionPlan>| {
            pattern
                .schema()
                .fields()
                .iter()
                .any(|f| current_schema.index_of(f.name()).is_ok())
        };

        let small_patterns = patterns
            .iter()
            .enumerate()
            .filter(|(_, pattern)| !shares_column(pattern) && is_small_pattern(pattern))
            .collect::<Vec<_>>();

        if let Some((idx, _)) = small_patterns.iter().find(|(_, pattern)| {
            enables_filter(&current_schema, &pattern.schema(), pending_filters)
        }) {
            return *idx;
        }

        // Prefer patterns that allow joining a small pattern in the next step.
        if !small_patterns.is_empty()
            && let Some(idx) = patterns.iter().position(|pattern| {
                if !shares_column(pattern) {
                    return false;
                }
                let joined = Schema::new(
                    current_schema
                        .fields()
                        .iter()
                        .chain(pattern.schema().fields().iter())
                        .cloned()
                        .collect::<Vec<_>>(),
                );
                small_patterns.iter().any(|(_, small)| {
                    enables_filter(&joined, &small.schema(), pending_filters)
                })
            })
        {
            return idx;
        }

        patterns.iter().position(shares_column).unwrap_or(0)
    }

    #[allow(clippy::too_many_arguments)]
    fn join_execs(
        &self,
        planner: &dyn PhysicalPlanner,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        needed_after_join: &HashSet<String>,
        columns_to_decode: &[Column],
        pending_filters: &mut Vec<Expr>,
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let mut on: JoinOn = Vec::new();
        for l_field in left_schema.fields() {
            let col_name = l_field.name();

            if right_schema.index_of(col_name).is_ok() {
                let l_idx = left_schema.index_of(col_name).unwrap();
                let r_idx = right_schema.index_of(col_name).unwrap();

                on.push((
                    Arc::new(PhysicalColumn::new(col_name, l_idx)) as _,
                    Arc::new(PhysicalColumn::new(col_name, r_idx)) as _,
                ));
            }
        }

        if !on.is_empty() {
            let left_len = left_schema.fields().len();
            let mut projection = Vec::new();

            for (i, l_field) in left_schema.fields().iter().enumerate() {
                let base_name = l_field.name();
                if needed_after_join.contains(base_name) {
                    projection.push(i);
                }
            }
            for (r_idx, r_field) in right_schema.fields().iter().enumerate() {
                if left_schema.index_of(r_field.name()).is_err() {
                    let base_name = r_field.name();
                    if needed_after_join.contains(base_name) {
                        projection.push(left_len + r_idx);
                    }
                }
            }

            let partition_mode = if left.output_partitioning().partition_count() <= 1
                && right.output_partitioning().partition_count() <= 1
            {
                PartitionMode::CollectLeft
            } else {
                PartitionMode::Partitioned
            };

            Ok(Arc::new(HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                Some(projection),
                partition_mode,
                NullEquality::NullEqualsNothing,
                false,
            )?))
        } else {
            self.optimize_cross_join(
                planner,
                left,
                right,
                needed_after_join,
                columns_to_decode,
                pending_filters,
                session,
                planning_ctx,
            )
        }
    }

    /// Handles CrossJoin fallbacks. Promotes to a NestedLoopJoinExec if filters are present.
    #[allow(clippy::too_many_arguments)]
    fn optimize_cross_join(
        &self,
        planner: &dyn PhysicalPlanner,
        mut left: Arc<dyn ExecutionPlan>,
        mut right: Arc<dyn ExecutionPlan>,
        needed_after_join: &HashSet<String>,
        columns_to_decode: &[Column],
        pending_filters: &mut Vec<Expr>,
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut eligible_filters = Vec::new();
        let mut decode_left = Vec::new();
        let mut decode_right = Vec::new();
        let use_oids = self.decoding_udf.is_some();

        let left_schema = left.schema();
        let right_schema = right.schema();

        pending_filters.retain(|filter| {
            let mut cols = HashSet::new();
            if expr_to_columns(filter, &mut cols).is_err() {
                return true;
            }

            let mut temp_decode_left = Vec::new();
            let mut temp_decode_right = Vec::new();

            for c in &cols {
                let name = c.name.as_str();
                let needs_decode =
                    use_oids && columns_to_decode.iter().any(|dc| dc.name == name);

                if needs_decode {
                    if left_schema.index_of(name).is_ok() {
                        temp_decode_left.push((name.to_string(), name.to_string()));
                    } else if right_schema.index_of(name).is_ok() {
                        temp_decode_right.push((name.to_string(), name.to_string()));
                    } else {
                        return true;
                    }
                } else {
                    if left_schema.index_of(name).is_ok()
                        || right_schema.index_of(name).is_ok()
                    {
                        continue;
                    } else {
                        return true;
                    }
                }
            }

            decode_left.extend(temp_decode_left);
            decode_right.extend(temp_decode_right);
            eligible_filters.push(filter.clone());
            false
        });

        decode_left.sort();
        decode_left.dedup();
        decode_right.sort();
        decode_right.dedup();

        if use_oids {
            let decoding_udf = self.decoding_udf.clone().unwrap();
            if !decode_left.is_empty() {
                let mut projections = Vec::new();
                for field in left.schema().fields() {
                    let field_name = field.name().clone();
                    if decode_left.iter().any(|(_, target)| target == &field_name) {
                        projections.push(ObjectIdDecodingExecProjection::Decode {
                            source_column: field_name.clone(),
                            target_column: field_name,
                        });
                    } else {
                        projections.push(ObjectIdDecodingExecProjection::Column {
                            source_column: field_name.clone(),
                            target_column: field_name,
                        });
                    }
                }
                left = Arc::new(DecodeObjectIdsExec::try_new(
                    left,
                    projections,
                    Arc::clone(&decoding_udf),
                )?);
            }
            if !decode_right.is_empty() {
                let mut projections = Vec::new();
                for field in right.schema().fields() {
                    let field_name = field.name().clone();
                    if decode_right.iter().any(|(_, target)| target == &field_name) {
                        projections.push(ObjectIdDecodingExecProjection::Decode {
                            source_column: field_name.clone(),
                            target_column: field_name,
                        });
                    } else {
                        projections.push(ObjectIdDecodingExecProjection::Column {
                            source_column: field_name.clone(),
                            target_column: field_name,
                        });
                    }
                }
                right = Arc::new(DecodeObjectIdsExec::try_new(
                    right,
                    projections,
                    decoding_udf,
                )?);
            }
        }

        let exec: Arc<dyn ExecutionPlan> = if eligible_filters.is_empty() {
            Arc::new(CrossJoinExec::new(left, right))
        } else {
            // Combine all eligible filters into a single AND expression
            let combined_filter = eligible_filters
                .into_iter()
                .reduce(|a, b| a.and(b))
                .unwrap();

            // Construct the intermediate unified schema of what the cross join would produce
            let mut combined_fields =
                left.schema().fields().iter().cloned().collect::<Vec<_>>();
            combined_fields.extend(right.schema().fields().iter().cloned());
            let intermediate_schema = Arc::new(Schema::new(combined_fields));

            let df_schema = DFSchema::from_unqualified_fields(
                intermediate_schema.fields().clone(),
                HashMap::new(),
            )?;

            let phys_expr = planner.create_physical_expr(
                &combined_filter,
                &df_schema,
                session,
                planning_ctx,
            )?;

            // Assign column quad_tables dynamically to map left/right sources
            let mut column_indices = Vec::new();
            for i in 0..left.schema().fields().len() {
                column_indices.push(ColumnIndex {
                    index: i,
                    side: JoinSide::Left,
                });
            }
            for i in 0..right.schema().fields().len() {
                column_indices.push(ColumnIndex {
                    index: i,
                    side: JoinSide::Right,
                });
            }

            let join_filter =
                JoinFilter::new(phys_expr, column_indices, intermediate_schema);

            Arc::new(NestedLoopJoinExec::try_new(
                left,
                right,
                Some(join_filter),
                &JoinType::Inner,
                None,
            )?)
        };

        // Standard Deduplication Projection Output
        let exec_schema = exec.schema();
        let mut projection = Vec::new();
        for (idx, field) in exec_schema.fields().iter().enumerate() {
            let base_name = field.name();
            if needed_after_join.contains(base_name) {
                projection.push((
                    Arc::new(PhysicalColumn::new(field.name(), idx)) as _,
                    field.name().to_string(),
                ));
            }
        }

        if projection.len() < exec_schema.fields().len() {
            Ok(Arc::new(ProjectionExec::try_new(projection, exec)?))
        } else {
            Ok(exec)
        }
    }

    fn decode_columns_for_filters(
        &self,
        exec: Arc<dyn ExecutionPlan>,
        bgp: &BgpNode,
        top_level_needs: &HashSet<String>,
        filter_needs: &HashSet<String>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let use_oids = self.decoding_udf.is_some();
        let join_schema = exec.schema();

        if !use_oids {
            return Ok(exec);
        }

        let decoding_udf = self.decoding_udf.clone().unwrap();
        let mut projections = Vec::with_capacity(top_level_needs.len());
        let columns_to_decode: HashSet<String> = bgp
            .columns_to_decode
            .iter()
            .map(|c| c.flat_name())
            .collect();

        let mut projected_names = HashSet::new();
        let mut needs_decode = false;

        for field in join_schema.fields() {
            let field_name = field.name();

            if !top_level_needs.contains(field_name) {
                continue;
            }

            if columns_to_decode.contains(field_name)
                && filter_needs.contains(field_name)
                && is_object_id_data_type(field.data_type())
            {
                if projected_names.insert(field_name.to_string()) {
                    projections.push(ObjectIdDecodingExecProjection::Decode {
                        source_column: field_name.to_string(),
                        target_column: field_name.to_string(),
                    });
                    needs_decode = true;
                }
            } else {
                if projected_names.insert(field_name.to_string()) {
                    projections.push(ObjectIdDecodingExecProjection::Column {
                        source_column: field_name.to_string(),
                        target_column: field_name.to_string(),
                    });
                }
            }
        }

        // Also if any column needed is already available but we missed it:
        for needed in top_level_needs {
            if !projected_names.contains(needed) && join_schema.index_of(needed).is_ok() {
                projections.push(ObjectIdDecodingExecProjection::Column {
                    source_column: needed.clone(),
                    target_column: needed.clone(),
                });
            }
        }

        if !needs_decode {
            return Ok(exec);
        }

        Ok(Arc::new(DecodeObjectIdsExec::try_new(
            exec,
            projections,
            decoding_udf,
        )?))
    }

    fn decode_final_columns(
        &self,
        exec: Arc<dyn ExecutionPlan>,
        bgp: &BgpNode,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let use_oids = self.decoding_udf.is_some();
        let schema = exec.schema();

        if !use_oids {
            return Ok(exec);
        }

        let decoding_udf = self.decoding_udf.clone().unwrap();
        let mut projections = Vec::with_capacity(schema.fields().len());
        let columns_to_decode: HashSet<String> = bgp
            .columns_to_decode
            .iter()
            .map(|c| c.flat_name())
            .collect();
        let mut needs_decode = false;

        for field in schema.fields() {
            let field_name = field.name();

            if columns_to_decode.contains(field_name)
                && is_object_id_data_type(field.data_type())
            {
                projections.push(ObjectIdDecodingExecProjection::Decode {
                    source_column: field_name.to_string(),
                    target_column: field_name.to_string(),
                });
                needs_decode = true;
            } else {
                projections.push(ObjectIdDecodingExecProjection::Column {
                    source_column: field_name.to_string(),
                    target_column: field_name.to_string(),
                });
            }
        }

        if !needs_decode {
            return Ok(exec);
        }

        Ok(Arc::new(DecodeObjectIdsExec::try_new(
            exec,
            projections,
            decoding_udf,
        )?))
    }
}

/// The maximum number of (estimated) rows of a pattern that is joined early via a filter (see
/// [`BgpPlanner::find_next_join_pattern`]).
const SMALL_PATTERN_MAX_ROWS: usize = 16;

/// Returns whether `exec` is estimated to produce at most [`SMALL_PATTERN_MAX_ROWS`] rows.
fn is_small_pattern(exec: &Arc<dyn ExecutionPlan>) -> bool {
    StatisticsContext::new()
        .compute(exec.as_ref(), &StatisticsArgs::new())
        .ok()
        .and_then(|stats| stats.num_rows.get_value().copied())
        .is_some_and(|rows| rows <= SMALL_PATTERN_MAX_ROWS)
}

/// Returns whether `expr` references any of the `columns` (by name).
fn references_any(expr: &Expr, columns: &[Column]) -> bool {
    let mut referenced = HashSet::new();
    if expr_to_columns(expr, &mut referenced).is_err() {
        return true;
    }
    referenced
        .iter()
        .any(|c| columns.iter().any(|column| column.name == c.name))
}

/// Returns whether a filter in `filters` references columns of both `left` and `right` and can be
/// evaluated after joining them.
fn enables_filter(left: &Schema, right: &Schema, filters: &[Expr]) -> bool {
    filters.iter().any(|filter| {
        let mut columns = HashSet::new();
        if expr_to_columns(filter, &mut columns).is_err() {
            return false;
        }
        let in_left = |name: &str| left.index_of(name).is_ok();
        let in_right = |name: &str| right.index_of(name).is_ok();
        columns.iter().any(|c| in_left(&c.name))
            && columns.iter().any(|c| in_right(&c.name))
            && columns
                .iter()
                .all(|c| in_left(&c.name) || in_right(&c.name))
    })
}

/// Returns whether `exec` is a materialized pattern. Patterns with bound variables re-add the
/// variables in a projection (see [`add_bound_columns`]).
fn is_materialized(exec: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(projection) = exec.downcast_ref::<ProjectionExec>() {
        return is_materialized(projection.input());
    }
    exec.is::<MaterializedQuadPatternExec>()
}

/// Decodes the single term in `array`. Returns `None` if the encoding is not supported or the
/// value is null.
fn decode_single_term(encoding: &QuadStorageEncoding, array: &ArrayRef) -> Option<Term> {
    let array = match encoding {
        QuadStorageEncoding::PlainTerm => {
            PlainTermArray::try_from(Arc::clone(array)).ok()?
        }
        QuadStorageEncoding::String => StringTermArray::new_unchecked(Arc::clone(array))
            .as_plain_term_array()
            .ok()?,
        // Object ids would require looking up the term.
        QuadStorageEncoding::ObjectId(_) => return None,
    };
    let term = DefaultPlainTermDecoder::decode_terms(&array).next()?.ok()?;
    Some(term.into_owned())
}

/// The bound variables, with their term and their value in the storage encoding.
type Bindings = HashMap<String, (Term, ScalarValue)>;

/// Returns the row of `exec` if it is a materialized pattern with a single row.
fn single_row(exec: &Arc<dyn ExecutionPlan>) -> Option<&RecordBatch> {
    exec.downcast_ref::<MaterializedQuadPatternExec>()?
        .batch()
        .filter(|batch| batch.num_rows() == 1)
}

/// Projects `exec` to `fields`. Fields that are missing in `exec` are bound variables and are added
/// as constant columns.
fn add_bound_columns(
    exec: Arc<dyn ExecutionPlan>,
    fields: &Fields,
    bindings: &Bindings,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let schema = exec.schema();
    if schema.fields() == fields {
        return Ok(exec);
    }
    let mut projection = Vec::with_capacity(fields.len());
    for field in fields {
        let expr: Arc<dyn PhysicalExpr> = match schema.index_of(field.name()) {
            Ok(idx) => Arc::new(PhysicalColumn::new(field.name(), idx)),
            Err(_) => match bindings.get(field.name()) {
                Some((_, value)) => Arc::new(Literal::new(value.clone())),
                None => {
                    return plan_err!("Missing binding for variable {}", field.name());
                }
            },
        };
        projection.push((expr, field.name().clone()));
    }
    Ok(Arc::new(ProjectionExec::try_new(projection, exec)?))
}

#[cfg(test)]
mod tests {
    //! There are also integration tests for the BgpPlanner.

    use super::*;
    use datafusion::arrow::datatypes::Fields;
    use datafusion::common::DFSchema;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_empty_patterns() -> DFResult<()> {
        let planner = BgpPlanner::new(None);
        let ctx = SessionContext::new();
        let bgp = BgpNode::try_new(vec![], vec![], None, vec![]).unwrap();

        let plan = planner
            .plan_extension(
                &MockPlanner {
                    plans: HashMap::new(),
                },
                &bgp,
                &[],
                &[],
                &ctx.state(),
                &PhysicalPlanningContext::default(),
            )
            .await?
            .unwrap();

        assert!(plan.is::<PlaceholderRowExec>());
        assert_eq!(plan.schema().fields().len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_short_circuit_zero_rows() -> DFResult<()> {
        let planner = BgpPlanner::new(None);
        let ctx = SessionContext::new();
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            Fields::empty(),
            HashMap::new(),
        )?);

        let lp = LogicalPlanBuilder::empty(false).build()?;
        let bgp = BgpNode::try_new(vec![lp.clone()], vec![], None, vec![]).unwrap();

        // EmptyExec reports Precision::Exact(0) rows
        let empty_exec = Arc::new(EmptyExec::new(Arc::new(schema.as_arrow().clone())));

        let mut plans = HashMap::new();
        plans.insert(
            lp.clone(),
            Arc::clone(&empty_exec) as Arc<dyn ExecutionPlan>,
        );

        let plan = planner
            .plan_extension(
                &MockPlanner { plans },
                &bgp,
                &[&lp],
                &[empty_exec as Arc<dyn ExecutionPlan>],
                &ctx.state(),
                &PhysicalPlanningContext::default(),
            )
            .await?
            .unwrap();

        assert!(plan.is::<EmptyExec>());

        Ok(())
    }

    /// Creates a quad pattern. Terms starting with `?` are variables, the others are IRIs.
    fn pattern(subject: &str, predicate: &str, object: &str) -> LogicalPlan {
        use datafusion::logical_expr::Extension;
        use rdf_fusion_common::{
            ActiveGraph, NamedNode, TermPattern, TriplePattern, Variable,
        };

        let term = |value: &str| match value.strip_prefix('?') {
            Some(name) => TermPattern::Variable(Variable::new_unchecked(name)),
            None => TermPattern::NamedNode(NamedNode::new_unchecked(value)),
        };
        LogicalPlan::Extension(Extension {
            node: Arc::new(QuadPatternNode::new(
                QuadStorageEncoding::String,
                ActiveGraph::DefaultGraph,
                None,
                TriplePattern {
                    subject: term(subject),
                    predicate: NamedNode::new_unchecked(predicate).into(),
                    object: term(object),
                },
            )),
        })
    }

    /// A materialized result of `pattern` whose rows bind all variables to `iris`.
    async fn materialized(
        pattern: &LogicalPlan,
        iris: &[&str],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::compute::cast;
        use datafusion::datasource::memory::MemorySourceConfig;

        let schema = Arc::new(pattern.schema().as_arrow().clone());
        let values = iris.iter().map(|iri| format!("<{iri}>"));
        let values = StringArray::from_iter_values(values);
        let columns = schema
            .fields()
            .iter()
            .map(|field| cast(&values, field.data_type()))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
        let task_ctx = SessionContext::new().task_ctx();
        Ok(Arc::new(
            MaterializedQuadPatternExec::materialize(source, task_ctx).await?,
        ))
    }

    /// A scan of `pattern` that has not been materialized.
    fn scan(pattern: &LogicalPlan) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(
            pattern.schema().as_arrow().clone(),
        )))
    }

    /// Binds the values of the single-row `inputs` in the other patterns. Returns the triple
    /// patterns that have been re-planned and the field names of the resulting inputs.
    async fn bind_single_row_values(
        inputs: &[(&LogicalPlan, Arc<dyn ExecutionPlan>)],
    ) -> DFResult<(Vec<String>, Vec<Vec<String>>)> {
        let recording = Arc::new(RecordingPlanner::default());
        let planner = BgpPlanner::new(None)
            .with_quad_pattern_planners(vec![Arc::clone(&recording) as _]);
        let logical_inputs = inputs.iter().map(|(plan, _)| *plan).collect::<Vec<_>>();
        let physical_inputs = inputs
            .iter()
            .map(|(_, exec)| Arc::clone(exec))
            .collect::<Vec<_>>();

        let bound = planner
            .bind_single_row_values(
                &MockPlanner {
                    plans: HashMap::new(),
                },
                &logical_inputs,
                &physical_inputs,
                &SessionContext::new().state(),
                &PhysicalPlanningContext::default(),
            )
            .await?;

        let mut planned = recording.planned.lock().unwrap().clone();
        planned.sort();
        let fields = bound
            .iter()
            .map(|exec| {
                let schema = exec.schema();
                schema.fields().iter().map(|f| f.name().clone()).collect()
            })
            .collect();
        Ok((planned, fields))
    }

    #[tokio::test]
    async fn test_bind_single_row_value() -> DFResult<()> {
        let seed = pattern("http://ex/s", "http://ex/p1", "?x");
        let other = pattern("?x", "http://ex/p2", "?y");

        let (planned, fields) = bind_single_row_values(&[
            (&seed, materialized(&seed, &["http://ex/o"]).await?),
            (&other, scan(&other)),
        ])
        .await?;

        assert_eq!(planned, ["<http://ex/o> <http://ex/p2> ?y"]);
        // The bound variable ?x is re-added as a constant column.
        assert_eq!(fields, [vec!["x"], vec!["x", "y"]]);
        Ok(())
    }

    /// The joint publications of two authors: both name lookups match a single person. Hence,
    /// both `authoredBy` patterns are re-planned with their author bound.
    #[tokio::test]
    async fn test_bind_single_row_values_of_joint_publications() -> DFResult<()> {
        let name1 = pattern("http://ex/name1", "http://ex/nameOf", "?person1");
        let name2 = pattern("http://ex/name2", "http://ex/nameOf", "?person2");
        let authored_by1 = pattern("?pub", "http://ex/authoredBy", "?person1");
        let authored_by2 = pattern("?pub", "http://ex/authoredBy", "?person2");

        let (planned, _) = bind_single_row_values(&[
            (&name1, materialized(&name1, &["http://ex/a"]).await?),
            (&name2, materialized(&name2, &["http://ex/b"]).await?),
            (&authored_by1, scan(&authored_by1)),
            (&authored_by2, scan(&authored_by2)),
        ])
        .await?;

        assert_eq!(
            planned,
            [
                "?pub <http://ex/authoredBy> <http://ex/a>",
                "?pub <http://ex/authoredBy> <http://ex/b>",
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_bind_single_row_values_ignores_multiple_rows() -> DFResult<()> {
        let seed = pattern("http://ex/s", "http://ex/p1", "?x");
        let other = pattern("?x", "http://ex/p2", "?y");

        let (planned, _) = bind_single_row_values(&[
            (
                &seed,
                materialized(&seed, &["http://ex/o1", "http://ex/o2"]).await?,
            ),
            (&other, scan(&other)),
        ])
        .await?;

        assert!(planned.is_empty());
        Ok(())
    }

    /// Materialized patterns are not re-planned.
    #[tokio::test]
    async fn test_bind_single_row_values_skips_materialized_patterns() -> DFResult<()> {
        let seed = pattern("http://ex/s", "http://ex/p1", "?x");
        let other = pattern("?x", "http://ex/p2", "?y");

        let (planned, _) = bind_single_row_values(&[
            (&seed, materialized(&seed, &["http://ex/o"]).await?),
            (
                &other,
                materialized(&other, &["http://ex/o1", "http://ex/o2"]).await?,
            ),
        ])
        .await?;

        assert!(planned.is_empty());
        Ok(())
    }

    /// Records the triple patterns it plans and returns an empty scan.
    #[derive(Default)]
    struct RecordingPlanner {
        planned: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ExtensionPlanner for RecordingPlanner {
        async fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            node: &dyn UserDefinedLogicalNode,
            _logical_inputs: &[&LogicalPlan],
            _physical_inputs: &[Arc<dyn ExecutionPlan>],
            _session: &dyn Session,
            _planning_ctx: &PhysicalPlanningContext,
        ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
            let Some(node) = node.as_any().downcast_ref::<QuadPatternNode>() else {
                return Ok(None);
            };
            self.planned
                .lock()
                .unwrap()
                .push(node.quad_pattern().triple_pattern.to_string());
            Ok(Some(Arc::new(EmptyExec::new(Arc::new(
                node.schema().as_arrow().clone(),
            )))))
        }
    }

    struct MockPlanner {
        plans: HashMap<LogicalPlan, Arc<dyn ExecutionPlan>>,
    }

    #[async_trait]
    impl PhysicalPlanner for MockPlanner {
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            _session: &dyn Session,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            Ok(Arc::clone(
                self.plans
                    .get(logical_plan)
                    .expect("Plan not found in MockPlanner"),
            ))
        }

        fn create_physical_expr(
            &self,
            _expr: &Expr,
            _input_dfschema: &DFSchema,
            _session: &dyn Session,
            _planning_ctx: &PhysicalPlanningContext,
        ) -> DFResult<Arc<dyn PhysicalExpr>> {
            unimplemented!()
        }
    }
}
