use crate::RdfFusionExprBuilderContext;
use crate::minus::MinusNode;
use datafusion::common::{
    Column, DFSchemaRef, JoinType, NullEquality, plan_datafusion_err,
};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, and};
use rdf_fusion_common::DFResult;
use rdf_fusion_extensions::RdfFusionContextView;
use std::collections::HashSet;
use std::sync::Arc;

/// The lowering for a [MinusNode] into a left-anti join.
struct MinusLowering<'a> {
    /// The RDF Fusion configuration.
    context: &'a RdfFusionContextView,
}

impl<'a> MinusLowering<'a> {
    /// Rewrites a [MinusNode] into a left-anti join.
    fn rewrite_minus(&self, node: &MinusNode) -> DFResult<LogicalPlan> {
        let overlapping_keys = compute_join_keys(node);

        // If there are no overlapping keys, then we cannot remove any solution.
        if overlapping_keys.is_empty() {
            return Ok(node.lhs().clone());
        }

        let lhs = LogicalPlanBuilder::new(node.lhs().clone()).alias("lhs")?;
        let rhs = LogicalPlanBuilder::new(node.rhs().clone()).alias("rhs")?;
        let lhs_schema = Arc::clone(lhs.schema());

        // Compute the result via a LeftAnti join.
        let filter_expr = self.compute_filter_expression(
            lhs.schema(),
            rhs.schema(),
            &overlapping_keys,
        )?;
        let join_result = lhs.join_detailed(
            rhs.build()?,
            JoinType::LeftAnti,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            filter_expr,
            NullEquality::NullEqualsNothing,
        )?;

        // Eliminate the "lhs" qualifier.
        let projections = lhs_schema
            .columns()
            .into_iter()
            .map(|k| Expr::from(Column::new(Some("lhs"), &k.name)).alias(k.name))
            .collect::<Vec<_>>();
        join_result.project(projections)?.build()
    }

    /// Computes the filter expression for the left-anti join.
    ///
    /// The filter expression ensures that solutions from the right-hand side are only
    /// considered if they are compatible with the left-hand side, according to SPARQL
    /// semantics.
    fn compute_filter_expression(
        &self,
        lhs_schema: &DFSchemaRef,
        rhs_schema: &DFSchemaRef,
        overlapping_keys: &HashSet<String>,
    ) -> DFResult<Option<Expr>> {
        let mut join_schema = lhs_schema.as_ref().clone();
        join_schema.merge(rhs_schema);
        let expr_builder_root =
            RdfFusionExprBuilderContext::new(self.context, &join_schema);

        let mut join_filters = Vec::new();

        // Filter based on the overlapping keys.
        for k in overlapping_keys {
            let expr = expr_builder_root
                .try_create_builder(Expr::from(Column::new(Some("lhs"), k)))?
                .build_is_compatible(Expr::from(Column::new(Some("rhs"), k)))?;
            join_filters.push(expr);
        }

        // At least one of the overlapping keys must be not null.
        let any_both_not_null = overlapping_keys
            .iter()
            .map(|k| {
                and(
                    Expr::from(Column::new(Some("lhs"), k)).is_not_null(),
                    Expr::from(Column::new(Some("rhs"), k)).is_not_null(),
                )
            })
            .reduce(Expr::or)
            .ok_or(plan_datafusion_err!(
                "There must be at least one overlapping key"
            ))?;
        join_filters.push(any_both_not_null);

        let filter_expr = join_filters.into_iter().reduce(Expr::and);
        Ok(filter_expr)
    }
}

/// Rewrites a [MinusNode] into a left-anti join.
pub(crate) fn rewrite_minus_node(
    node: &MinusNode,
    context: &RdfFusionContextView,
) -> DFResult<LogicalPlan> {
    MinusLowering::new(context).rewrite_minus(node)
}

impl<'a> MinusLowering<'a> {
    /// Creates a new [MinusLowering].
    fn new(context: &'a RdfFusionContextView) -> Self {
        Self { context }
    }
}

/// Computes the overlapping columns between the lhs and rhs of the minus.
fn compute_join_keys(node: &MinusNode) -> HashSet<String> {
    let lhs_keys: HashSet<_> = node
        .lhs()
        .schema()
        .columns()
        .into_iter()
        .map(|c| c.name().to_owned())
        .collect();
    let rhs_keys: HashSet<_> = node
        .rhs()
        .schema()
        .columns()
        .into_iter()
        .map(|c| c.name().to_owned())
        .collect();

    lhs_keys
        .intersection(&rhs_keys)
        .cloned()
        .collect::<HashSet<String>>()
}
