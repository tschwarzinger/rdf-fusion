use crate::bgp::BgpNode;
use crate::quad_pattern::QuadPatternNode;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
                        let mut proj_required_columns = HashSet::new();

                        // Columns from projection expressions
                        for expr in &projection.expr {
                            expr_to_columns(expr, &mut required_columns)?;
                            expr_to_columns(expr, &mut proj_required_columns)?;
                        }

                        // Columns from BGP filters
                        for expr in &bgp.filters {
                            expr_to_columns(expr, &mut required_columns)?;
                        }

                        // Filter columns_to_decode to only include columns requested by the projection
                        let new_columns_to_decode: Vec<_> = bgp
                            .columns_to_decode
                            .iter()
                            .filter(|c| proj_required_columns.contains(*c))
                            .cloned()
                            .collect();

                        let decoding_changed =
                            new_columns_to_decode.len() != bgp.columns_to_decode.len();

                        // Add remaining columns_to_decode to the overall required columns
                        for col in &new_columns_to_decode {
                            required_columns.insert(col.clone());
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
                        let mut patterns_changed = false;

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
                                            patterns_changed = true;
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

                            new_patterns.push(new_pattern);
                        }

                        let changed = patterns_changed || decoding_changed;

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
                            let new_bgp = BgpNode::try_new(
                                new_patterns,
                                bgp.filters.clone(),
                                Some(projection_columns),
                                new_columns_to_decode,
                            )?;
                            return Ok(Transformed::yes(LogicalPlan::Extension(
                                Extension {
                                    node: Arc::new(new_bgp),
                                },
                            )));
                        } else if changed {
                            let new_bgp = BgpNode::try_new(
                                new_patterns,
                                bgp.filters.clone(),
                                bgp.projection.clone(),
                                new_columns_to_decode,
                            )?;
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
