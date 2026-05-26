use crate::bgp::BgpNode;
use crate::quad_pattern::QuadPatternNode;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::utils::{expr_to_columns, split_conjunction};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A rule that absorbs [LogicalPlan::Filter] nodes into a [BgpNode].
#[derive(Debug)]
pub struct BgpFilterAbsorbRule;

impl OptimizerRule for BgpFilterAbsorbRule {
    fn name(&self) -> &str {
        "BgpFilterAbsorbRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            if let LogicalPlan::Filter(filter) = &plan {
                if let LogicalPlan::Extension(Extension { node }) = filter.input.as_ref()
                {
                    if let Some(bgp) = node.as_any().downcast_ref::<BgpNode>() {
                        let predicates = split_conjunction(&filter.predicate);
                        let mut pushable = Vec::new();
                        let mut remaining = Vec::new();

                        for p in predicates {
                            if contains_subquery(p) {
                                remaining.push((*p).clone());
                            } else {
                                pushable.push((*p).clone());
                            }
                        }

                        if pushable.is_empty() {
                            return Ok(Transformed::no(plan));
                        }

                        let mut new_filters = bgp.filters.clone();
                        new_filters.extend(pushable);

                        let new_bgp = BgpNode::new(
                            bgp.patterns.clone(),
                            Arc::clone(&bgp.schema),
                            new_filters,
                            bgp.projection.clone(),
                        );

                        let new_bgp_plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(new_bgp),
                        });

                        return if remaining.is_empty() {
                            Ok(Transformed::yes(new_bgp_plan))
                        } else {
                            let combined_remaining = remaining
                                .into_iter()
                                .reduce(datafusion::logical_expr::and)
                                .expect("At least one remaining");
                            Ok(Transformed::yes(LogicalPlan::Filter(
                                datafusion::logical_expr::Filter::try_new(
                                    combined_remaining,
                                    Arc::new(new_bgp_plan),
                                )?,
                            )))
                        };
                    }
                }
            }
            Ok(Transformed::no(plan))
        })
    }
}

/// A rule that pushes down [LogicalPlan::Projection] nodes into a [BgpNode] and its patterns.
#[derive(Debug)]
pub struct BgpProjectionPushdownRule;

impl OptimizerRule for BgpProjectionPushdownRule {
    fn name(&self) -> &str {
        "BgpProjectionPushdownRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            if let LogicalPlan::Projection(projection) = &plan {
                if let LogicalPlan::Extension(Extension { node }) =
                    projection.input.as_ref()
                {
                    if let Some(bgp) = node.as_any().downcast_ref::<BgpNode>() {
                        // 1. Determine required columns
                        let mut required_columns = HashSet::new();

                        // Columns from projection expressions
                        for expr in &projection.expr {
                            expr_to_columns(expr, &mut required_columns)?;
                        }

                        // Columns from BGP filters
                        for expr in &bgp.filters {
                            expr_to_columns(expr, &mut required_columns)?;
                        }

                        // Implicit join keys (columns shared across patterns)
                        let mut column_counts = HashMap::new();
                        for pattern in &bgp.patterns {
                            for col in pattern.schema().columns() {
                                *column_counts.entry(col).or_insert(0) += 1;
                            }
                        }
                        for (col, count) in column_counts {
                            if count > 1 {
                                required_columns.insert(col);
                            }
                        }

                        // 2. Push down to QuadPatternNodes
                        let mut new_patterns = Vec::new();
                        let mut new_merged_schema = None;
                        let mut changed = false;

                        for pattern in &bgp.patterns {
                            let new_pattern =
                                if let LogicalPlan::Extension(Extension { node }) =
                                    pattern
                                {
                                    if let Some(quad_pattern) =
                                        node.as_any().downcast_ref::<QuadPatternNode>()
                                    {
                                        let mut projection_indices = Vec::new();
                                        let schema = quad_pattern.schema();
                                        for (i, col) in
                                            schema.columns().into_iter().enumerate()
                                        {
                                            if required_columns.contains(&col) {
                                                projection_indices.push(i);
                                            }
                                        }

                                        if projection_indices.len()
                                            < quad_pattern.schema().fields().len()
                                        {
                                            let new_quad_pattern = quad_pattern
                                                .with_projection(projection_indices)?;
                                            changed = true;
                                            LogicalPlan::Extension(Extension {
                                                node: Arc::new(new_quad_pattern),
                                            })
                                        } else {
                                            pattern.clone()
                                        }
                                    } else {
                                        pattern.clone()
                                    }
                                } else {
                                    pattern.clone()
                                };

                            match &mut new_merged_schema {
                                None => {
                                    new_merged_schema =
                                        Some(new_pattern.schema().as_ref().clone())
                                }
                                Some(s) => s.merge(new_pattern.schema()),
                            }
                            new_patterns.push(new_pattern);
                        }

                        let new_merged_schema = Arc::new(new_merged_schema.unwrap());

                        // 3. Determine if it's a simple projection that can be absorbed
                        let mut can_absorb = true;
                        let mut projection_columns = Vec::new();
                        for expr in &projection.expr {
                            if let Expr::Column(col) = expr {
                                projection_columns.push(col.clone());
                            } else {
                                can_absorb = false;
                                break;
                            }
                        }

                        if can_absorb {
                            let new_bgp = BgpNode::new(
                                new_patterns,
                                Arc::clone(&projection.schema),
                                bgp.filters.clone(),
                                Some(projection_columns),
                            );
                            return Ok(Transformed::yes(LogicalPlan::Extension(
                                Extension {
                                    node: Arc::new(new_bgp),
                                },
                            )));
                        } else if changed {
                            let new_bgp = BgpNode::new(
                                new_patterns,
                                new_merged_schema,
                                bgp.filters.clone(),
                                bgp.projection.clone(),
                            );
                            return Ok(Transformed::yes(LogicalPlan::Projection(
                                datafusion::logical_expr::Projection::try_new(
                                    projection.expr.clone(),
                                    Arc::new(LogicalPlan::Extension(Extension {
                                        node: Arc::new(new_bgp),
                                    })),
                                )?,
                            )));
                        }
                    }
                }
            }
            Ok(Transformed::no(plan))
        })
    }
}

/// Helper function to check if an expression contains a subquery.
fn contains_subquery(expr: &Expr) -> bool {
    let mut has_subquery = false;
    expr.apply(|e| {
        if matches!(
            e,
            Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
        ) {
            has_subquery = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    has_subquery
}
