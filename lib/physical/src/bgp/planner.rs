use crate::object_id::exec::{DecodeObjectIdsExec, ObjectIdDecodingExecProjection};
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{
    Column, DFSchema, JoinSide, JoinType, NullEquality, Result as DFResult,
};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::utils::{expr_to_columns, split_conjunction};
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF, UserDefinedLogicalNode};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
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
use rdf_fusion_encoding::object_id::is_object_id_data_type;
use rdf_fusion_logical::bgp::BgpNode;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct BgpPlanner {
    decoding_udf: Option<Arc<ScalarUDF>>,
}

#[async_trait]
impl ExtensionPlanner for BgpPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(bgp) = node.as_any().downcast_ref::<BgpNode>() else {
            return Ok(None);
        };

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
            let next_idx = self.find_next_join_pattern(&exec, &patterns);
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
        Self { decoding_udf }
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
            let rows = stats.num_rows.get_value().cloned().unwrap_or(usize::MAX);
            patterns.push((current_exec, rows));
        }

        patterns.sort_by_key(|(_, rows)| *rows);
        Ok(Some((
            patterns.into_iter().map(|(exec, _)| exec).collect(),
            pending_filters,
        )))
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

    fn find_next_join_pattern(
        &self,
        current_exec: &Arc<dyn ExecutionPlan>,
        patterns: &[Arc<dyn ExecutionPlan>],
    ) -> usize {
        let current_schema = current_exec.schema();
        patterns
            .iter()
            .position(|pattern| {
                pattern
                    .schema()
                    .fields()
                    .iter()
                    .any(|f| current_schema.index_of(f.name()).is_ok())
            })
            .unwrap_or(0)
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
