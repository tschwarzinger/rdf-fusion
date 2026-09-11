use crate::bgp::BgpNode;
use crate::encoding::object_id::DecodeObjectIdsNode;
use crate::quad_pattern::QuadPatternNode;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::utils::{expr_to_columns, split_conjunction};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A rule that pushes filters, projections, and decodings into a [BgpNode] in a single pass.
///
/// Whereas the previous implementation used one rule per candidate, this rule recognizes filter,
/// projection, and decode nodes and pushes them into the underlying [BgpNode] within a single
/// bottom-up pass. Because the children are transformed first, chains of candidates such as
///
/// ```text
/// Filter:
///   Decode:
///     Projection:
///       BGP:
/// ```
///
/// are collected and pushed into the BGP in one go.
#[derive(Debug)]
pub struct BgpPushdownRule;

impl OptimizerRule for BgpPushdownRule {
    fn name(&self) -> &str {
        "BgpPushdownRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            let new_plan = pushdown_decode(&plan)?
                .or(pushdown_filter(&plan)?)
                .or(pushdown_projection(&plan)?);
            match new_plan {
                Some(new_plan) => Ok(Transformed::yes(new_plan)),
                None => Ok(Transformed::no(plan)),
            }
        })
    }
}

/// Absorbs a [DecodeObjectIdsNode] into a [BgpNode].
fn pushdown_decode(plan: &LogicalPlan) -> DFResult<Option<LogicalPlan>> {
    let LogicalPlan::Extension(Extension { node }) = plan else {
        return Ok(None);
    };

    let Some(decode_node) = node.as_any().downcast_ref::<DecodeObjectIdsNode>() else {
        return Ok(None);
    };

    let input_plans = decode_node.inputs();
    let input_plan = input_plans
        .first()
        .expect("DecodeObjectIdsNode should have one child");
    let LogicalPlan::Extension(Extension { node: bgp_node_ext }) = input_plan else {
        return Ok(None);
    };
    let Some(bgp) = bgp_node_ext.as_any().downcast_ref::<BgpNode>() else {
        return Ok(None);
    };

    let mut new_columns_to_decode = bgp.columns_to_decode.clone();
    for col in decode_node.columns_to_decode() {
        if !new_columns_to_decode.contains(col) {
            new_columns_to_decode.push(col.clone());
        }
    }

    let new_bgp = BgpNode::try_new(
        bgp.patterns.clone(),
        bgp.filters.clone(),
        bgp.projection.clone(),
        new_columns_to_decode,
    )?;

    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(new_bgp),
    })))
}

/// Absorbs a [LogicalPlan::Filter] into a [BgpNode].
fn pushdown_filter(plan: &LogicalPlan) -> DFResult<Option<LogicalPlan>> {
    let LogicalPlan::Filter(filter) = plan else {
        return Ok(None);
    };
    let LogicalPlan::Extension(Extension { node }) = filter.input.as_ref() else {
        return Ok(None);
    };
    let Some(bgp) = node.as_any().downcast_ref::<BgpNode>() else {
        return Ok(None);
    };

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
        return Ok(None);
    }

    let mut new_filters = bgp.filters.clone();
    new_filters.extend(pushable);

    let new_bgp = BgpNode::try_new(
        bgp.patterns.clone(),
        new_filters,
        bgp.projection.clone(),
        bgp.columns_to_decode.clone(),
    )?;

    let new_bgp_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(new_bgp),
    });

    Ok(Some(if remaining.is_empty() {
        new_bgp_plan
    } else {
        let combined_remaining = remaining
            .into_iter()
            .reduce(datafusion::logical_expr::and)
            .expect("At least one remaining");
        LogicalPlan::Filter(datafusion::logical_expr::Filter::try_new(
            combined_remaining,
            Arc::new(new_bgp_plan),
        )?)
    }))
}

/// Pushes a [LogicalPlan::Projection] into a [BgpNode] and its patterns.
fn pushdown_projection(plan: &LogicalPlan) -> DFResult<Option<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(None);
    };
    let LogicalPlan::Extension(Extension { node }) = projection.input.as_ref() else {
        return Ok(None);
    };
    let Some(bgp) = node.as_any().downcast_ref::<BgpNode>() else {
        return Ok(None);
    };

    // 1. Determine required columns
    let mut required_columns = HashSet::new();
    let mut proj_required_columns = HashSet::new();

    for expr in &projection.expr {
        expr_to_columns(expr, &mut required_columns)?;
        expr_to_columns(expr, &mut proj_required_columns)?;
    }

    for expr in &bgp.filters {
        expr_to_columns(expr, &mut required_columns)?;
    }

    let new_columns_to_decode: Vec<_> = bgp
        .columns_to_decode
        .iter()
        .filter(|c| proj_required_columns.contains(*c))
        .cloned()
        .collect();

    let decoding_changed = new_columns_to_decode.len() != bgp.columns_to_decode.len();

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
        let new_pattern = if let LogicalPlan::Extension(Extension { node }) = pattern {
            if let Some(quad_pattern) = node.as_any().downcast_ref::<QuadPatternNode>() {
                let mut projection_indices = Vec::new();
                let schema = quad_pattern.schema();
                for (i, col) in schema.columns().into_iter().enumerate() {
                    if required_columns.contains(&col) {
                        projection_indices.push(i);
                    }
                }

                if projection_indices.len() < quad_pattern.schema().fields().len() {
                    let new_quad_pattern =
                        quad_pattern.with_projection(projection_indices)?;
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
        return Ok(Some(LogicalPlan::Extension(Extension {
            node: Arc::new(new_bgp),
        })));
    }

    if changed {
        let new_bgp = BgpNode::try_new(
            new_patterns,
            bgp.filters.clone(),
            bgp.projection.clone(),
            new_columns_to_decode,
        )?;
        return Ok(Some(LogicalPlan::Projection(
            datafusion::logical_expr::Projection::try_new(
                projection.expr.clone(),
                Arc::new(LogicalPlan::Extension(Extension {
                    node: Arc::new(new_bgp),
                })),
            )?,
        )));
    }

    Ok(None)
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
