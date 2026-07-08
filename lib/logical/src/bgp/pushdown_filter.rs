use crate::bgp::BgpNode;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::sync::Arc;

/// A rule that absorbs [LogicalPlan::Filter] nodes into a [BgpNode].
#[derive(Debug)]
pub struct BgpFilterPushdownRule;

impl OptimizerRule for BgpFilterPushdownRule {
    fn name(&self) -> &str {
        "BgpFilterPushdownRule"
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

                        let new_bgp = BgpNode::try_new(
                            bgp.patterns.clone(),
                            new_filters,
                            bgp.projection.clone(),
                            bgp.columns_to_decode.clone(),
                        )?;

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
