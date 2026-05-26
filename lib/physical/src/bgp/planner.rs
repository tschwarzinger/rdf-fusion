use async_trait::async_trait;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{
    Column, DFSchema, JoinSide, JoinType, NullEquality, Result as DFResult,
    TableReference,
};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter, JoinOn};
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode,
};
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_logical::bgp::BgpNode;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Physical planner for [`BgpNode`].
///
/// This planner is responsible for translating a logical BGP node into a physical execution plan
/// consisting of joins. It performs a simple join ordering based on the estimated number of rows
/// from each pattern, while prioritizing joins on overlapping variables to avoid cross joins.
pub struct BgpPlanner;

impl Default for BgpPlanner {
    fn default() -> Self {
        Self
    }
}

#[async_trait]
impl ExtensionPlanner for BgpPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(bgp) = node.as_any().downcast_ref::<BgpNode>() else {
            return Ok(None);
        };

        if bgp.patterns.is_empty() {
            return Ok(Some(Arc::new(PlaceholderRowExec::new(Arc::new(
                bgp.schema.as_arrow().clone(),
            )))));
        }

        // 1. Plan each pattern and retrieve its statistics
        let mut physical_patterns = Vec::new();
        let mut pending_filters = bgp.filters.clone();

        for pattern in &bgp.patterns {
            let mut exec = planner.create_physical_plan(pattern, session_state).await?;

            // Try to apply filters early
            let mut i = 0;
            while i < pending_filters.len() {
                let filter = &pending_filters[i];
                let mut columns = HashSet::new();
                expr_to_columns(filter, &mut columns)?;

                let schema = exec.schema();
                let can_apply = columns
                    .iter()
                    .all(|col| schema.index_of(col.name()).is_ok());

                if can_apply {
                    let phys_expr = planner.create_physical_expr(
                        filter,
                        pattern.schema(),
                        session_state,
                    )?;
                    exec = Arc::new(FilterExec::try_new(phys_expr, exec)?);
                    pending_filters.remove(i);
                } else {
                    i += 1;
                }
            }

            let stats = exec.partition_statistics(None)?;
            if let Precision::Exact(0) = stats.num_rows {
                return Ok(Some(Arc::new(EmptyExec::new(Arc::new(
                    bgp.schema.as_arrow().clone(),
                )))));
            }

            let rows = stats.num_rows.get_value().cloned().unwrap_or(usize::MAX);
            physical_patterns.push((exec, rows));
        }

        // 2. Sort patterns by their estimated row count (ascending)
        physical_patterns.sort_by_key(|(_, rows)| *rows);

        // 3. Construct a physical join tree prioritizing overlapping variables
        let (mut current_exec, _) = physical_patterns.remove(0);

        while !physical_patterns.is_empty() {
            let current_schema = current_exec.schema();

            // Find the first pattern (smallest row count) that shares variables with the current plan
            // OR has a join filter with the current plan
            let next_idx = physical_patterns
                .iter()
                .position(|(exec, _)| {
                    let pattern_schema = exec.schema();
                    // Direct overlap
                    if pattern_schema
                        .fields()
                        .iter()
                        .any(|f| current_schema.index_of(f.name()).is_ok())
                    {
                        return true;
                    }

                    // Join filter overlap
                    pending_filters.iter().any(|filter| {
                        let mut columns = HashSet::new();
                        let _ = expr_to_columns(filter, &mut columns);
                        let mut touches_current = false;
                        let mut touches_pattern = false;
                        for col in columns {
                            if current_schema.index_of(col.name()).is_ok() {
                                touches_current = true;
                            } else if pattern_schema.index_of(col.name()).is_ok() {
                                touches_pattern = true;
                            }
                        }
                        touches_current && touches_pattern
                    })
                })
                .unwrap_or(0); // If no overlap exists, default to the smallest remaining pattern (cross join)

            let (next_exec, _) = physical_patterns.remove(next_idx);

            let is_final_join = physical_patterns.is_empty();
            let final_projection = if is_final_join {
                bgp.projection.as_deref()
            } else {
                None
            };

            current_exec = self.join_execs(
                planner,
                session_state,
                current_exec,
                next_exec,
                final_projection,
                &mut pending_filters,
            )?;

            // Try to apply remaining filters after join
            let mut i = 0;
            while i < pending_filters.len() {
                let filter = &pending_filters[i];
                let mut columns = HashSet::new();
                expr_to_columns(filter, &mut columns)?;

                let schema = current_exec.schema();
                let can_apply = columns
                    .iter()
                    .all(|col| schema.index_of(col.name()).is_ok());

                if can_apply {
                    let current_df_schema = DFSchema::from_unqualified_fields(
                        current_exec.schema().fields().clone(),
                        HashMap::new(),
                    )?;

                    let phys_expr = planner.create_physical_expr(
                        filter,
                        &current_df_schema,
                        session_state,
                    )?;
                    current_exec =
                        Arc::new(FilterExec::try_new(phys_expr, current_exec)?);
                    pending_filters.remove(i);
                } else {
                    i += 1;
                }
            }
        }

        // Apply any remaining filters that couldn't be applied early (e.g. they need columns from all patterns)
        for filter in pending_filters {
            let current_df_schema = DFSchema::from_unqualified_fields(
                current_exec.schema().fields().clone(),
                HashMap::new(),
            )?;

            let phys_expr = planner.create_physical_expr(
                &filter,
                &current_df_schema,
                session_state,
            )?;
            current_exec = Arc::new(FilterExec::try_new(phys_expr, current_exec)?);
        }

        // 4. Ensure the output schema matches the logical schema's exact column order
        let join_schema = current_exec.schema();
        let needs_projection = join_schema.fields().len() != bgp.schema.fields().len()
            || !bgp.schema.matches_arrow_schema(join_schema.as_ref());

        let result = if needs_projection {
            let mut projection = Vec::with_capacity(bgp.schema.fields().len());
            for field in bgp.schema.fields() {
                let idx = join_schema.index_of(field.name())?;
                projection.push((
                    Arc::new(PhysicalColumn::new(field.name(), idx)) as _,
                    field.name().to_string(),
                ));
            }
            Arc::new(ProjectionExec::try_new(projection, current_exec)?)
        } else {
            current_exec
        };

        Ok(Some(result))
    }
}

impl BgpPlanner {
    /// Creates a join between two physical execution plans.
    ///
    /// It uses a [HashJoinExec] if there are common columns, or a [NestedLoopJoinExec] if there are
    /// cross-side filters, otherwise a [CrossJoinExec].
    fn join_execs(
        &self,
        planner: &dyn PhysicalPlanner,
        session_state: &SessionState,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        final_projection: Option<&[Column]>,
        pending_filters: &mut Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let mut on: JoinOn = Vec::new();
        // 1. Direct same-named column joins
        for (l_idx, l_field) in left_schema.fields().iter().enumerate() {
            if let Ok(r_idx) = right_schema.index_of(l_field.name()) {
                on.push((
                    Arc::new(PhysicalColumn::new(l_field.name(), l_idx)) as _,
                    Arc::new(PhysicalColumn::new(right_schema.field(r_idx).name(), r_idx))
                        as _,
                ));
            }
        }

        // 2. Identify filters that reference both sides
        let mut join_predicates = Vec::new();
        let mut i = 0;
        while i < pending_filters.len() {
            let filter = &pending_filters[i];
            let mut columns = HashSet::new();
            expr_to_columns(filter, &mut columns)?;

            let mut touches_left = false;
            let mut touches_right = false;
            let mut others = false;

            for col in columns {
                if left_schema.index_of(col.name()).is_ok() {
                    touches_left = true;
                } else if right_schema.index_of(col.name()).is_ok() {
                    touches_right = true;
                } else {
                    others = true;
                }
            }

            if touches_left && touches_right && !others {
                join_predicates.push(pending_filters.remove(i));
            } else {
                i += 1;
            }
        }

        let join_filter = if !join_predicates.is_empty() {
            let combined_schema = {
                let mut fields = Vec::new();
                for f in left_schema.fields() {
                    fields.push((Some(TableReference::bare("left")), Arc::clone(f)));
                }
                for f in right_schema.fields() {
                    fields.push((Some(TableReference::bare("right")), Arc::clone(f)));
                }
                DFSchema::new_with_metadata(fields, HashMap::new())?
            };

            let combined_expr = join_predicates
                .into_iter()
                .map(|expr| {
                    expr.transform(|e| {
                        if let Expr::Column(col) = e {
                            if left_schema.index_of(col.name()).is_ok() {
                                Ok(Transformed::yes(Expr::Column(Column::new(
                                    Some("left"),
                                    col.name(),
                                ))))
                            } else if right_schema.index_of(col.name()).is_ok() {
                                Ok(Transformed::yes(Expr::Column(Column::new(
                                    Some("right"),
                                    col.name(),
                                ))))
                            } else {
                                Ok(Transformed::no(Expr::Column(col)))
                            }
                        } else {
                            Ok(Transformed::no(e))
                        }
                    })
                    .map(|t| t.data)
                })
                .collect::<DFResult<Vec<_>>>()?
                .into_iter()
                .reduce(datafusion::logical_expr::and)
                .expect("Not empty");

            let phys_expr = planner.create_physical_expr(
                &combined_expr,
                &combined_schema,
                session_state,
            )?;

            let mut column_indices = Vec::new();
            for i in 0..left_schema.fields().len() {
                column_indices.push(ColumnIndex {
                    index: i,
                    side: JoinSide::Left,
                });
            }
            for i in 0..right_schema.fields().len() {
                column_indices.push(ColumnIndex {
                    index: i,
                    side: JoinSide::Right,
                });
            }

            Some(JoinFilter::new(
                phys_expr,
                column_indices,
                Arc::new(combined_schema.as_arrow().clone()),
            ))
        } else {
            None
        };

        let left_len = left_schema.fields().len();
        let mut projection = Vec::new();

        if let Some(final_cols) = final_projection.filter(|_| pending_filters.is_empty())
        {
            for col in final_cols {
                if let Ok(idx) = left_schema.index_of(col.name()) {
                    projection.push(idx);
                } else if let Ok(idx) = right_schema.index_of(col.name()) {
                    projection.push(left_len + idx);
                }
            }
        } else {
            // Keep all columns from the left side
            for i in 0..left_len {
                projection.push(i);
            }

            // Keep only the non-overlapping columns from the right side
            for (r_idx, r_field) in right_schema.fields().iter().enumerate() {
                if left_schema.index_of(r_field.name()).is_err() {
                    projection.push(left_len + r_idx);
                }
            }
        }

        if !on.is_empty() {
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
                join_filter,
                &JoinType::Inner,
                Some(projection),
                partition_mode,
                NullEquality::NullEqualsNothing,
                false,
            )?))
        } else if let Some(filter) = join_filter {
            Ok(Arc::new(NestedLoopJoinExec::try_new(
                left,
                right,
                Some(filter),
                &JoinType::Inner,
                Some(projection),
            )?))
        } else {
            Ok(Arc::new(CrossJoinExec::new(left, right)))
        }
    }
}

#[cfg(test)]
mod tests {
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
        let planner = BgpPlanner::default();
        let ctx = SessionContext::new();
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            Fields::empty(),
            HashMap::new(),
        )?);
        let bgp = BgpNode::new(vec![], Arc::clone(&schema), vec![], None);

        let plan = planner
            .plan_extension(
                &MockPlanner {
                    plans: HashMap::new(),
                },
                &bgp,
                &[],
                &[],
                &ctx.state(),
            )
            .await?
            .unwrap();

        assert!(plan.as_any().is::<PlaceholderRowExec>());
        assert_eq!(plan.schema().fields().len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_short_circuit_zero_rows() -> DFResult<()> {
        let planner = BgpPlanner::default();
        let ctx = SessionContext::new();
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            Fields::empty(),
            HashMap::new(),
        )?);

        let lp = LogicalPlanBuilder::empty(false).build()?;
        let bgp = BgpNode::new(vec![lp.clone()], Arc::clone(&schema), vec![], None);

        // EmptyExec reports Precision::Exact(0) rows
        let empty_exec = Arc::new(EmptyExec::new(Arc::new(schema.as_arrow().clone())));

        let mut plans = HashMap::new();
        plans.insert(lp, empty_exec as Arc<dyn ExecutionPlan>);

        let plan = planner
            .plan_extension(&MockPlanner { plans }, &bgp, &[], &[], &ctx.state())
            .await?
            .unwrap();

        assert!(plan.as_any().is::<EmptyExec>());

        Ok(())
    }

    struct MockPlanner {
        plans: HashMap<LogicalPlan, Arc<dyn ExecutionPlan>>,
    }

    #[async_trait]
    impl PhysicalPlanner for MockPlanner {
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            _session_state: &SessionState,
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
            _session_state: &SessionState,
        ) -> DFResult<Arc<dyn PhysicalExpr>> {
            unimplemented!()
        }
    }
}
