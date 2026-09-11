use crate::extend::ExtendNode;
use datafusion::common::Column;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, col};
use rdf_fusion_common::DFResult;

/// Rewrites an [ExtendNode] into a projection.
pub(crate) fn rewrite_extend_node(node: &ExtendNode) -> DFResult<LogicalPlan> {
    let mut new_exprs: Vec<_> = node
        .inner()
        .schema()
        .fields()
        .iter()
        .map(|f| col(Column::new_unqualified(f.name())))
        .collect();
    new_exprs.push(node.expression().clone().alias(node.variable().as_str()));

    LogicalPlanBuilder::new(node.inner().clone())
        .project(new_exprs)?
        .build()
}
