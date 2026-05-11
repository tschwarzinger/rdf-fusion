use crate::RdfFusionExprBuilderContext;
use crate::check_same_schema;
use crate::join::{SparqlJoinNode, SparqlJoinType};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{
    Column, ExprSchema, JoinConstraint, JoinType, NullEquality, plan_err,
};
use datafusion::logical_expr::{Expr, ExprSchemable, Join, UserDefinedLogicalNode};
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use rdf_fusion_common::DFResult;
use rdf_fusion_extensions::RdfFusionContextView;
use std::collections::HashSet;

/// A rewriting rule that transforms SPARQL join operations into DataFusion join operations.
///
/// The rewriting rule incorporates nullability information to reduce the is_compatible check
/// to equality if the joined columns cannot be null.
///
/// # Additional Resources
/// /// - [SPARQL 1.1 - Compatibile Mappings](https://www.w3.org/TR/sparql11-query/#defn_algCompatibleMapping)
#[derive(Debug)]
pub struct SparqlJoinLoweringRule {
    /// The RDF Fusion configuration
    context: RdfFusionContextView,
}

impl OptimizerRule for SparqlJoinLoweringRule {
    fn name(&self) -> &str {
        "sparql-join-lowering"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            let new_plan = match &plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<SparqlJoinNode>() {
                        let new_plan = self.rewrite_sparql_join(node)?;
                        check_same_schema(node.schema(), new_plan.schema())?;
                        Transformed::yes(new_plan)
                    } else {
                        Transformed::no(plan)
                    }
                }
                _ => Transformed::no(plan),
            };
            Ok(new_plan)
        })
    }
}

impl SparqlJoinLoweringRule {
    /// Creates a new instance of the SPARQL join lowering rule.
    pub fn new(context: RdfFusionContextView) -> Self {
        Self { context }
    }

    /// Rewrites a SPARQL join node into a DataFusion join operation.
    ///
    /// This method analyzes the join conditions and chooses the most efficient join implementation:
    /// - For disjoint solutions with no filter, it uses a cross-join
    /// - For non-null columns, it attempts to use a regular join
    /// - Otherwise, it uses a join with compatibility checks
    ///
    /// # Additional Resources
    /// - [SPARQL Join Semantics](https://www.w3.org/TR/sparql11-query/#BasicGraphPatterns)
    fn rewrite_sparql_join(&self, node: &SparqlJoinNode) -> DFResult<LogicalPlan> {
        let (lhs_keys, rhs_keys) = get_join_keys(node);

        // If both solutions are disjoint, there is no filter, and this is an inner join we must
        // use a cross-join.
        if lhs_keys.is_disjoint(&rhs_keys) && node.filter().is_none() {
            let result = match node.join_type() {
                SparqlJoinType::Inner => LogicalPlanBuilder::new(node.lhs().clone())
                    .cross_join(node.rhs().clone())?
                    .build(),
                SparqlJoinType::Left => {
                    let join = Join::try_new(
                        node.lhs().clone().into(),
                        node.rhs().clone().into(),
                        vec![],
                        None,
                        JoinType::Left,
                        JoinConstraint::On,
                        NullEquality::NullEqualsNothing,
                        false,
                    )?;

                    Ok(LogicalPlan::Join(join))
                }
            };
            return result;
        }

        let mut join_on = lhs_keys
            .intersection(&rhs_keys)
            .map(Column::new_unqualified)
            .collect::<Vec<_>>();
        join_on.sort(); // Ensure stable output

        // Try to reduce the SPARQL join to a regular join.
        if let Some(result) = self.try_build_regular_join(node, &join_on)? {
            return Ok(result);
        }

        // Otherwise, use a join with a filter that checks if the values are compatible.
        self.build_join_with_is_compatible(node, &join_on)
    }

    /// Attempts to build a regular DataFusion join from a SPARQL join node.
    ///
    /// This method checks if the join can be implemented as a standard DataFusion join
    /// by verifying that none of the join columns are nullable. If all join columns
    /// are non-nullable, it creates a regular join; otherwise, it returns None.
    ///
    /// # Arguments
    /// * `node` - The SPARQL join node to rewrite
    /// * `join_on` - The columns to join on
    ///
    /// # Returns
    /// * `Some(LogicalPlan)` if a regular join can be built
    /// * `None` if a regular join cannot be used (e.g., due to nullable columns)
    fn try_build_regular_join(
        &self,
        node: &SparqlJoinNode,
        join_on: &[Column],
    ) -> DFResult<Option<LogicalPlan>> {
        let any_column_not_null = join_on
            .iter()
            .map(|col| {
                let lhs_field = node.lhs().schema().field_from_column(col)?;
                let rhs_field = node.rhs().schema().field_from_column(col)?;
                DFResult::Ok(lhs_field.is_nullable() || rhs_field.is_nullable())
            })
            .reduce(|l, r| Ok(l? || r?))
            .transpose()?;

        Ok(match any_column_not_null {
            // If no column is nullable, we can use a regular join.
            Some(false) => {
                let lhs = LogicalPlanBuilder::new(node.lhs().clone()).alias("lhs")?;
                let rhs = LogicalPlanBuilder::new(node.rhs().clone()).alias("rhs")?;
                let projections =
                    self.create_join_projections(node, &lhs, &rhs, false)?;

                let filter = node
                    .filter()
                    .map(|f| self.rewrite_filter_for_join(node, f, false))
                    .transpose()?;
                let plan = lhs
                    .join(
                        rhs.build()?,
                        get_data_fusion_join_type(node),
                        (join_on.to_vec(), join_on.to_vec()),
                        filter,
                    )?
                    .project(projections)?
                    .build()?;
                Some(plan)
            }
            // If at least one column is nullable, we cannot use a regular join. Furthermore, if
            // there are no equi-join conditions, we skip this step.
            Some(true) | None => None,
        })
    }

    /// Builds a join with compatibility checks for SPARQL semantics.
    ///
    /// This method creates a join that uses the `is_compatible` function to check
    /// if values from the left and right sides are compatible according to SPARQL
    /// semantics. This is used when a regular join cannot be applied, typically
    /// due to nullable columns or when SPARQL-specific compatibility is required.
    ///
    /// # Arguments
    /// * `node` - The SPARQL join node to rewrite
    /// * `join_on` - The columns to join on
    ///
    /// # Additional Resources
    /// - [SPARQL 1.1 - Value Compatibility](https://www.w3.org/TR/sparql11-query/#func-RDFterm-equal)
    fn build_join_with_is_compatible(
        &self,
        node: &SparqlJoinNode,
        join_on: &[Column],
    ) -> DFResult<LogicalPlan> {
        let lhs = LogicalPlanBuilder::new(node.lhs().clone()).alias("lhs")?;
        let rhs = LogicalPlanBuilder::new(node.rhs().clone()).alias("rhs")?;
        let projections = self.create_join_projections(node, &lhs, &rhs, true)?;

        let mut join_schema = lhs.schema().as_ref().clone();
        join_schema.merge(rhs.schema());
        let expr_builder_root =
            RdfFusionExprBuilderContext::new(&self.context, &join_schema);

        let mut join_filters = join_on
            .iter()
            .map(|col| {
                expr_builder_root
                    .try_create_builder(Expr::from(col.with_relation("lhs".into())))?
                    .build_is_compatible(Expr::from(col.with_relation("rhs".into())))
            })
            .collect::<DFResult<Vec<_>>>()?;

        if let Some(filter) = node.filter() {
            let filter = self.rewrite_filter_for_join(node, filter, true)?;
            join_filters.push(filter);
        }
        let filter_expr = join_filters.into_iter().reduce(Expr::and);

        let join = lhs.join_detailed(
            rhs.build()?,
            get_data_fusion_join_type(node),
            (Vec::<Column>::new(), Vec::<Column>::new()),
            filter_expr,
            NullEquality::NullEqualsNothing,
        )?;

        join.project(projections)?.build()
    }

    /// Creates projection expressions for the output of a join operation.
    ///
    /// This method generates the expressions needed to project the correct columns
    /// after a join operation. It handles the merging of columns from both sides
    /// of the join and applies coalescing when necessary to maintain SPARQL semantics.
    ///
    /// # Arguments
    /// * `node` - The SPARQL join node being processed
    /// * `lhs` - The left-hand side logical plan builder
    /// * `rhs` - The right-hand side logical plan builder
    /// * `requires_coalesce` - Whether coalescing is required for overlapping columns
    ///
    /// # Returns
    /// A vector of expressions to be used in the projection after the join
    fn create_join_projections(
        &self,
        node: &SparqlJoinNode,
        lhs: &LogicalPlanBuilder,
        rhs: &LogicalPlanBuilder,
        requires_coalesce: bool,
    ) -> DFResult<Vec<Expr>> {
        let mut join_schema = lhs.schema().as_ref().clone();
        join_schema.merge(rhs.schema());
        let expr_builder_root =
            RdfFusionExprBuilderContext::new(&self.context, &join_schema);

        let (lhs_keys, rhs_keys) = get_join_keys(node);
        let projections = node
            .schema()
            .columns()
            .into_iter()
            .map(|c| {
                value_from_joined(
                    expr_builder_root,
                    &lhs_keys,
                    &rhs_keys,
                    c.name(),
                    requires_coalesce,
                )
            })
            .collect::<DFResult<Vec<_>>>()?;

        Ok(projections)
    }

    /// Rewrites a filter expression to work with the joined data.
    ///
    /// This method transforms column references in the filter expression to correctly
    /// reference columns in the joined result. It handles the aliasing of tables and
    /// applies coalescing when necessary to maintain SPARQL filter semantics.
    ///
    /// # Arguments
    /// * `node` - The SPARQL join node being processed
    /// * `filter` - The filter expression to rewrite
    /// * `requires_coalesce` - Whether coalescing is required for overlapping columns
    ///
    /// # Additional Resources
    /// - SPARQL Filter Evaluation: <https://www.w3.org/TR/sparql11-query/#expressions>
    fn rewrite_filter_for_join(
        &self,
        node: &SparqlJoinNode,
        filter: &Expr,
        requires_coalesce: bool,
    ) -> DFResult<Expr> {
        let lhs = LogicalPlanBuilder::new(node.lhs().clone()).alias("lhs")?;
        let rhs = LogicalPlanBuilder::new(node.rhs().clone()).alias("rhs")?;

        let mut join_schema = lhs.schema().as_ref().clone();
        join_schema.merge(rhs.schema());
        let expr_builder_root =
            RdfFusionExprBuilderContext::new(&self.context, &join_schema);

        let (lhs_keys, rhs_keys) = get_join_keys(node);
        let filter = filter
            .clone()
            .transform(|e| {
                Ok(match e {
                    Expr::Column(c) => Transformed::yes(value_from_joined(
                        expr_builder_root,
                        &lhs_keys,
                        &rhs_keys,
                        c.name(),
                        requires_coalesce,
                    )?),
                    _ => Transformed::no(e),
                })
            })?
            .data;
        Ok(filter)
    }
}

/// Returns an expression that obtains value `variable` from either the lhs, the rhs, or both
/// depending on the schema.
fn value_from_joined(
    expr_builder_root: RdfFusionExprBuilderContext<'_>,
    lhs_keys: &HashSet<String>,
    rhs_keys: &HashSet<String>,
    variable: &str,
    requires_coalesce: bool,
) -> DFResult<Expr> {
    let lhs_expr = Expr::from(Column::new(Some("lhs"), variable));
    let rhs_expr = Expr::from(Column::new(Some("rhs"), variable));

    let expr = match (lhs_keys.contains(variable), rhs_keys.contains(variable)) {
        (true, true) => {
            if requires_coalesce {
                let lhs_field = lhs_expr.to_field(expr_builder_root.schema())?.1;
                let rhs_field = rhs_expr.to_field(expr_builder_root.schema())?.1;
                if lhs_field.data_type() != rhs_field.data_type() {
                    return plan_err!(
                        "The two columns for creating a COALESCE are different."
                    );
                }

                expr_builder_root
                    .try_create_builder(lhs_expr)?
                    .coalesce(vec![rhs_expr])?
                    .build()?
            } else {
                lhs_expr
            }
        }
        (true, false) => lhs_expr,
        (false, true) => rhs_expr,
        (false, false) => {
            unreachable!("At least one of lhs or rhs must contain variable")
        }
    };
    Ok(expr.alias(variable))
}

/// Returns the DataFusion [JoinType] type corresponding to the given [SparqlJoinType].
fn get_data_fusion_join_type(node: &SparqlJoinNode) -> JoinType {
    match node.join_type() {
        SparqlJoinType::Inner => JoinType::Inner,
        SparqlJoinType::Left => JoinType::Left,
    }
}

fn get_join_keys(node: &SparqlJoinNode) -> (HashSet<String>, HashSet<String>) {
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
    (lhs_keys, rhs_keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RdfFusionLogicalPlanBuilder, RdfFusionLogicalPlanBuilderContext};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::EmptyRelation;
    use datafusion::optimizer::OptimizerContext;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
    use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
    use rdf_fusion_encoding::string::STRING_ENCODING;
    use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
    use rdf_fusion_encoding::{QuadStorageEncoding, RdfFusionEncodings, TermEncoding};
    use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
    use std::sync::Arc;

    #[test]
    fn join_non_overlapping_variables_produces_cross_join() {
        let ctx = make_test_context();
        let builder_ctx = RdfFusionLogicalPlanBuilderContext::new(ctx.clone());

        let left = logical_plan_with_column("a");
        let right = logical_plan_with_column("b");
        let builder = RdfFusionLogicalPlanBuilder::new(builder_ctx, Arc::new(left));
        let initial_plan = builder
            .join(right, SparqlJoinType::Inner, None)
            .unwrap()
            .build()
            .unwrap();

        let result = rewrite_plan(ctx, initial_plan);

        assert_snapshot!(&result, @"
        Cross Join:
          EmptyRelation: rows=0
          EmptyRelation: rows=0
        ");
    }

    #[test]
    fn optional_non_overlapping_variables_produces_left_join_with_empty_filter() {
        let ctx = make_test_context();
        let builder_ctx = RdfFusionLogicalPlanBuilderContext::new(ctx.clone());

        let left = logical_plan_with_column("a");
        let right = logical_plan_with_column("b");
        let builder = RdfFusionLogicalPlanBuilder::new(builder_ctx, Arc::new(left));
        let initial_plan = builder
            .join(right, SparqlJoinType::Left, None)
            .unwrap()
            .build()
            .unwrap();

        let result = rewrite_plan(ctx, initial_plan);

        assert_snapshot!(&result, @"
        Left Join:
          EmptyRelation: rows=0
          EmptyRelation: rows=0
        ");
    }

    fn make_test_context() -> RdfFusionContextView {
        let encodings = RdfFusionEncodings::new(
            Arc::clone(&PLAIN_TERM_ENCODING),
            Arc::new(TypedFamilyEncoding::default()),
            None,
            Arc::clone(&SORTABLE_TERM_ENCODING),
            Arc::clone(&STRING_ENCODING),
        );
        let registry = Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone()));
        RdfFusionContextView::new(registry, encodings, QuadStorageEncoding::PlainTerm)
    }

    fn logical_plan_with_column(name: &str) -> LogicalPlan {
        let schema = DFSchema::new_with_metadata(
            vec![(
                None,
                Arc::new(Field::new(
                    name,
                    PLAIN_TERM_ENCODING.data_type().clone(),
                    false,
                )),
            )],
            Default::default(),
        )
        .unwrap();

        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(schema),
        })
    }

    fn rewrite_plan(ctx: RdfFusionContextView, plan: LogicalPlan) -> LogicalPlan {
        let config = OptimizerContext::new();
        SparqlJoinLoweringRule::new(ctx)
            .rewrite(plan, &config)
            .unwrap()
            .data
    }
}
