use crate::logical_plan_builder_context::RdfFusionLogicalPlanBuilderContext;
use crate::paths::kleene_plus::KleenePlusClosureNode;
use crate::paths::{COL_PATH_GRAPH, COL_PATH_SOURCE, COL_PATH_TARGET, PropertyPathNode};
use crate::patterns::PatternNode;
use crate::{ActiveGraph, RdfFusionExprBuilderContext, check_same_schema};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, JoinType, NullEquality, plan_datafusion_err};
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode, col,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::{not, or};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{
    NamedNode, NamedNodePattern, PropertyPathExpression, TermPattern, TermRef,
    TriplePattern, Variable,
};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::RdfFusionContextView;
use std::sync::Arc;

#[derive(Debug)]
pub struct PropertyPathLoweringRule {
    /// The RDF Fusion configuration.
    context: RdfFusionContextView,
}

impl OptimizerRule for PropertyPathLoweringRule {
    fn name(&self) -> &str {
        "property-path-lowering"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            let new_plan = match &plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<PropertyPathNode>() {
                        let new_plan = self.rewrite_property_path_node(node)?;
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

impl PropertyPathLoweringRule {
    /// Creates a new [PropertyPathLoweringRule].
    pub fn new(context: RdfFusionContextView) -> Self {
        Self { context }
    }

    /// Rewrites a [PropertyPathNode] into a regular logical plan.
    fn rewrite_property_path_node(
        &self,
        node: &PropertyPathNode,
    ) -> DFResult<LogicalPlan> {
        let inf = PropertyPathLoweringInformation {
            disallow_cross_graph_paths: node.graph_name_var().is_some(),
            active_graph: node.active_graph().clone(),
        };
        let query = self.rewrite_property_path_expression(&inf, node.path())?;

        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(PatternNode::try_new(
                query.build()?,
                vec![
                    node.graph_name_var()
                        .map(|v| TermPattern::Variable(v.clone())),
                    node.subject().clone().into(),
                    node.object().clone().into(),
                ],
            )?),
        });
        Ok(logical_plan)
    }

    /// The resulting query always has a column "start" and "end" that indicates the respective start
    /// and end of the current path. In addition to that, the result contains a graph column.
    fn rewrite_property_path_expression(
        &self,
        inf: &PropertyPathLoweringInformation,
        path: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        match path {
            PropertyPathExpression::NamedNode(node) => self.rewrite_named_node(inf, node),
            PropertyPathExpression::Reverse(inner) => self.rewrite_reverse(inf, inner),
            PropertyPathExpression::Sequence(lhs, rhs) => {
                self.rewrite_sequence(inf, lhs, rhs)
            }
            PropertyPathExpression::Alternative(lhs, rhs) => {
                self.rewrite_alternative(inf, lhs, rhs)
            }
            PropertyPathExpression::ZeroOrMore(inner) => {
                self.rewrite_zero_or_more(inf, inner)
            }
            PropertyPathExpression::OneOrMore(inner) => {
                self.rewrite_one_or_more(inf, inner)
            }
            PropertyPathExpression::ZeroOrOne(inner) => {
                self.rewrite_zero_or_one(inf, inner)
            }
            PropertyPathExpression::NegatedPropertySet(inner) => {
                self.rewrite_negated_property_set(inf, inner)
            }
        }
    }

    /// Rewrites a named node path to scanning the quads relation and checking whether the predicate
    /// matches the given `node`.
    fn rewrite_named_node(
        &self,
        inf: &PropertyPathLoweringInformation,
        node: &NamedNode,
    ) -> DFResult<LogicalPlanBuilder> {
        let filter = RdfFusionExprBuilderContext::new(
            &self.context,
            &QuadStorageEncoding::PlainTerm.quad_schema(),
        )
        .try_create_builder(col(COL_PREDICATE))?
        .build_same_term_scalar(TermRef::from(node.as_ref()))?;
        self.scan_quads(&inf.active_graph, Some(filter))
    }

    /// Rewrites a negated property set to scanning the quads relation and checking whether the
    /// predicate does not match any of the given `nodes`.
    fn rewrite_negated_property_set(
        &self,
        inf: &PropertyPathLoweringInformation,
        nodes: &[NamedNode],
    ) -> DFResult<LogicalPlanBuilder> {
        let schema = QuadStorageEncoding::PlainTerm.quad_schema();
        let predicate_builder = RdfFusionExprBuilderContext::new(&self.context, &schema)
            .try_create_builder(col(COL_PREDICATE))?;

        let test_expressions = nodes
            .iter()
            .map(|nn| {
                predicate_builder
                    .clone()
                    .build_same_term_scalar(TermRef::from(nn.as_ref()))
            })
            .collect::<DFResult<Vec<Expr>>>()?;
        let test_expression =
            test_expressions
                .into_iter()
                .reduce(or)
                .ok_or(plan_datafusion_err!(
                    "The negated property set must not be empty"
                ))?;

        self.scan_quads(&inf.active_graph, Some(not(test_expression)))?
            .distinct()
    }

    /// Reverses the inner path by swapping [COL_PATH_SOURCE] and [COL_PATH_TARGET].
    fn rewrite_reverse(
        &self,
        inf: &PropertyPathLoweringInformation,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_property_path_expression(inf, inner)?;
        inner.project([
            col(COL_PATH_GRAPH),
            col(COL_PATH_TARGET).alias(COL_PATH_SOURCE),
            col(COL_PATH_SOURCE).alias(COL_PATH_TARGET),
        ])
    }

    /// Rewrites an alternative path to union over both (distinct).
    fn rewrite_alternative(
        &self,
        inf: &PropertyPathLoweringInformation,
        lhs: &PropertyPathExpression,
        rhs: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_property_path_expression(inf, lhs)?;
        let rhs = self.rewrite_property_path_expression(inf, rhs)?;
        join_path_alternatives(lhs, rhs)?.distinct()
    }

    /// Rewrites a sequence by joining the [COL_PATH_TARGET] of the lhs to the [COL_PATH_SOURCE] of the `rhs`.
    fn rewrite_sequence(
        &self,
        inf: &PropertyPathLoweringInformation,
        lhs: &PropertyPathExpression,
        rhs: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_property_path_expression(inf, lhs)?;
        let rhs = self.rewrite_property_path_expression(inf, rhs)?;
        self.join_path_sequence(inf, lhs, rhs)?.distinct()
    }

    /// Rewrites a zero or more to a CTE.
    fn rewrite_zero_or_more(
        &self,
        inf: &PropertyPathLoweringInformation,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let zero = self.zero_length_paths(inf)?;
        let repetition = self.rewrite_one_or_more(inf, inner)?;
        join_path_alternatives(zero, repetition)?.distinct()
    }

    /// Rewrites a one or more by building a recursive query.
    fn rewrite_one_or_more(
        &self,
        inf: &PropertyPathLoweringInformation,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_property_path_expression(inf, inner)?;

        // The kleene node currenly only supports the plain term encoding.
        let builder_context =
            RdfFusionLogicalPlanBuilderContext::new(self.context.clone());
        let inner = builder_context
            .create(Arc::new(inner.build()?))
            .with_plain_terms()?
            .build()?;
        let node = KleenePlusClosureNode::try_new(inner, inf.disallow_cross_graph_paths)?;

        let builder = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }));
        Ok(builder)
    }

    fn rewrite_zero_or_one(
        &self,
        inf: &PropertyPathLoweringInformation,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let zero = self.zero_length_paths(inf)?;
        let one = self.rewrite_property_path_expression(inf, inner)?;
        join_path_alternatives(zero, one)
    }

    /// Returns a list of all subjects and objects in the graph where they both are the source and
    /// the target of the path.
    fn zero_length_paths(
        &self,
        inf: &PropertyPathLoweringInformation,
    ) -> DFResult<LogicalPlanBuilder> {
        // TODO: This must be optimized
        let subjects = self.scan_quads(&inf.active_graph, None)?.project([
            col(COL_PATH_GRAPH).alias(COL_PATH_GRAPH),
            col(COL_PATH_SOURCE).alias(COL_PATH_SOURCE),
            col(COL_PATH_SOURCE).alias(COL_PATH_TARGET),
        ])?;
        let objects = self.scan_quads(&inf.active_graph, None)?.project([
            col(COL_PATH_GRAPH).alias(COL_PATH_GRAPH),
            col(COL_PATH_TARGET).alias(COL_PATH_SOURCE),
            col(COL_PATH_TARGET).alias(COL_PATH_TARGET),
        ])?;
        subjects.union(objects.build()?)?.distinct()
    }

    /// Creates a join that represents a sequence of two paths.
    fn join_path_sequence(
        &self,
        inf: &PropertyPathLoweringInformation,
        lhs: LogicalPlanBuilder,
        rhs: LogicalPlanBuilder,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = lhs.alias("lhs")?;
        let rhs = rhs.alias("rhs")?;

        let join_schema = lhs.schema().join(rhs.schema())?;
        let expr_builder_root =
            RdfFusionExprBuilderContext::new(&self.context, &join_schema);
        let filter = create_path_sequence_join_filter(inf, expr_builder_root)?;

        let join_result = lhs.join_detailed(
            rhs.build()?,
            JoinType::Inner,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            Some(filter),
            NullEquality::NullEqualsNothing,
        )?;
        join_result.project([
            col(Column::new(Some("lhs"), COL_PATH_GRAPH)).alias(COL_PATH_GRAPH),
            col(Column::new(Some("lhs"), COL_PATH_SOURCE)).alias(COL_PATH_SOURCE),
            col(Column::new(Some("rhs"), COL_PATH_TARGET)).alias(COL_PATH_TARGET),
        ])
    }

    /// Scans the quads table and optionally filters it.
    fn scan_quads(
        &self,
        active_graph: &ActiveGraph,
        filter: Option<Expr>,
    ) -> DFResult<LogicalPlanBuilder> {
        let pattern = TriplePattern {
            subject: TermPattern::Variable(Variable::new_unchecked(COL_SUBJECT)),
            predicate: NamedNodePattern::Variable(Variable::new_unchecked(COL_PREDICATE)),
            object: TermPattern::Variable(Variable::new_unchecked(COL_OBJECT)),
        };

        let builder = RdfFusionLogicalPlanBuilderContext::new(self.context.clone())
            .create_pattern(
                active_graph.clone(),
                Some(Variable::new_unchecked(COL_GRAPH)),
                pattern,
            )
            .with_plain_terms()?;

        // Apply filter if present
        let builder = if let Some(filter) = filter {
            builder.filter(filter)?
        } else {
            builder
        };

        // Project columns to PATH_TABLE
        let query = builder.into_inner().project([
            col(COL_GRAPH).alias(COL_PATH_GRAPH),
            col(COL_SUBJECT).alias(COL_PATH_SOURCE),
            col(COL_OBJECT).alias(COL_PATH_TARGET),
        ])?;

        Ok(query)
    }
}

struct PropertyPathLoweringInformation {
    active_graph: ActiveGraph,
    disallow_cross_graph_paths: bool,
}

/// Creates a filter [Expr] for joining a sequence of two paths.
#[allow(clippy::unwrap_in_result)]
#[allow(clippy::expect_used)]
fn create_path_sequence_join_filter(
    inf: &PropertyPathLoweringInformation,
    expr_builder_root: RdfFusionExprBuilderContext<'_>,
) -> DFResult<Expr> {
    let path_join_expr = expr_builder_root
        .try_create_builder(Expr::from(Column::new(Some("lhs"), COL_PATH_TARGET)))?
        .build_is_compatible(Expr::from(Column::new(Some("rhs"), COL_PATH_SOURCE)))?;
    let mut on_exprs = vec![path_join_expr];

    if inf.disallow_cross_graph_paths {
        let path_join_expr = expr_builder_root
            .try_create_builder(Expr::from(Column::new(Some("lhs"), COL_PATH_GRAPH)))?
            .build_same_term(Expr::from(Column::new(Some("rhs"), COL_PATH_GRAPH)))?;
        on_exprs.push(path_join_expr)
    }

    Ok(on_exprs
        .into_iter()
        .reduce(or)
        .expect("At least one expression must be present"))
}

/// Creates a union that represents an alternative of two paths.
fn join_path_alternatives(
    lhs: LogicalPlanBuilder,
    rhs: LogicalPlanBuilder,
) -> DFResult<LogicalPlanBuilder> {
    lhs.union(rhs.build()?)
}
