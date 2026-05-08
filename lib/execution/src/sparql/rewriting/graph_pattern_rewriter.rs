use crate::sparql::QueryDataset;
use crate::sparql::rewriting::expression_rewriter::ExpressionRewriter;
use datafusion::common::{Column, DFSchema, not_impl_err, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr};
use rdf_fusion_common::Iri;
use rdf_fusion_common::sparql::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, OrderExpression,
};
use rdf_fusion_common::{DFResult, NamedNodePattern, NamedOrBlankNode};
use rdf_fusion_common::{GraphName, Variable};
use rdf_fusion_encoding::EncodingName;
use rdf_fusion_functions::aggregates::sparql_sum;
use rdf_fusion_logical::join::SparqlJoinType;
use rdf_fusion_logical::{
    ActiveGraph, RdfFusionLogicalPlanBuilder, RdfFusionLogicalPlanBuilderContext,
};
use std::cell::RefCell;
use std::sync::Arc;

/// A rewriter that transforms SPARQL graph patterns into a DataFusion logical plan.
///
/// The resulting logical plans can then be optimized and executed using the query engine.
pub struct GraphPatternRewriter {
    /// Registry of functions that can be used during rewriting.
    builder_context: RdfFusionLogicalPlanBuilderContext,
    /// The dataset against which the query is evaluated.
    dataset: QueryDataset,
    /// The base IRI used for resolving relative IRIs in the query.
    base_iri: Option<Iri<String>>,
    /// The current state of the rewriting process.
    state: RefCell<RewritingState>,
}

impl GraphPatternRewriter {
    /// Creates a new `GraphPatternRewriter` with the specified registry, dataset, and base IRI.
    ///
    /// # Arguments
    /// * `builder_context` - The context necessary for building logical plans.
    /// * `dataset` - The dataset against which the query will be evaluated
    /// * `base_iri` - The base IRI used for resolving relative IRIs in the query
    pub fn new(
        builder_context: RdfFusionLogicalPlanBuilderContext,
        dataset: QueryDataset, // TODO: Moving dataset and base_iri to rewrite allows reusing
        base_iri: Option<Iri<String>>,
    ) -> Self {
        let active_graph = compute_default_active_graph(&dataset);
        let state = RewritingState::default().with_active_graph(active_graph);
        Self {
            builder_context,
            dataset,
            base_iri,
            state: RefCell::new(state),
        }
    }

    /// Rewrites a SPARQL graph pattern into a DataFusion logical plan.
    ///
    /// The method ensures that all results are encoded as plain terms and can be displayed to
    /// users.
    pub fn rewrite(&self, pattern: &GraphPattern) -> DFResult<LogicalPlan> {
        let plan = self.rewrite_graph_pattern(pattern)?;
        plan.with_plain_terms()?.build()
    }

    /// Similar to [Self::rewrite] but does not transform all columns into the plain term encoding.
    pub fn rewrite_with_existing_encoding(
        &self,
        pattern: &GraphPattern,
    ) -> DFResult<LogicalPlan> {
        self.rewrite_graph_pattern(pattern)?.build()
    }

    /// Rewrites a SPARQL graph pattern into a logical plan builder.
    fn rewrite_graph_pattern(
        &self,
        pattern: &GraphPattern,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        match pattern {
            GraphPattern::Bgp { patterns } => {
                let state = self.state.borrow();
                self.builder_context.create_bgp(
                    &state.active_graph,
                    state.graph_name_var.as_ref(),
                    patterns,
                )
            }
            GraphPattern::Project { inner, variables } => {
                if self.graph_variable_goes_out_of_scope(variables) {
                    let old_state = self.state.borrow().clone();
                    let new_state = old_state.without_graph_variable();
                    self.state.replace(new_state);

                    let inner = self.rewrite_graph_pattern(inner.as_ref())?;
                    let result = inner.project(variables);

                    self.state.replace(old_state);
                    result
                } else {
                    let inner = self.rewrite_graph_pattern(inner.as_ref())?;
                    inner.project(variables)
                }
            }
            GraphPattern::Filter { inner, expr } => {
                let inner = self.rewrite_graph_pattern(inner.as_ref())?;
                let expr = self.rewrite_to_boolean_expression(inner.schema(), expr)?;
                inner.filter(expr)
            }
            GraphPattern::Extend {
                inner,
                expression,
                variable,
            } => {
                let inner = self.rewrite_graph_pattern(inner)?;
                let expr = self.rewrite_expression(inner.schema(), expression)?;
                inner.extend(variable.clone(), expr)
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => self.builder_context.create_values(variables, bindings),
            GraphPattern::Join { left, right } => {
                let left = self.rewrite_graph_pattern(left)?;
                let right = self.rewrite_graph_pattern(right)?;
                left.join(right.build()?, SparqlJoinType::Inner, None)
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                let lhs = self.rewrite_graph_pattern(left)?;
                let rhs = self.rewrite_graph_pattern(right)?;

                let mut join_schema = lhs.schema().as_ref().clone();
                join_schema.merge(rhs.schema());

                let filter = expression
                    .as_ref()
                    .map(|f| self.rewrite_to_boolean_expression(&join_schema, f))
                    .transpose()?;

                lhs.join(rhs.build()?, SparqlJoinType::Left, filter)
            }
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => {
                let inner = self.rewrite_graph_pattern(inner)?;
                inner.slice(*start, *length)
            }
            GraphPattern::Distinct { inner } => {
                let sort_exprs = get_sort_expressions(inner);
                let inner = self.rewrite_graph_pattern(inner)?;

                let Some(sort_exprs) = sort_exprs else {
                    return inner.distinct();
                };

                let sort_exprs = sort_exprs
                    .iter()
                    .map(|e| self.rewrite_order_expression(inner.schema(), e))
                    .collect::<Result<Vec<_>, _>>()?;
                inner.distinct_with_sort(sort_exprs)
            }
            GraphPattern::Reduced { inner } => {
                let inner = self.rewrite_graph_pattern(inner)?;
                inner.reduced()
            }
            GraphPattern::OrderBy { inner, expression } => {
                let inner = self.rewrite_graph_pattern(inner)?;
                let sort_exprs = expression
                    .iter()
                    .map(|e| self.rewrite_order_expression(inner.schema(), e))
                    .collect::<Result<Vec<_>, _>>()?;
                inner.order_by(&sort_exprs)
            }
            GraphPattern::Union { left, right } => {
                let lhs = self.rewrite_graph_pattern(left)?;
                let rhs = self.rewrite_graph_pattern(right)?;
                lhs.union(rhs.build()?)
            }
            GraphPattern::Graph { name, inner } => {
                let old_state = self.state.borrow().clone();
                let active_graph = compute_active_graph_for_pattern(&self.dataset, name);
                let variable = match name {
                    NamedNodePattern::Variable(var) => Some(var.clone()),
                    NamedNodePattern::NamedNode(_) => None,
                };
                let new_state = old_state
                    .with_active_graph(active_graph)
                    .with_graph_variable(variable);
                self.state.replace(new_state);
                let result = self.rewrite_graph_pattern(inner.as_ref());
                self.state.replace(old_state);
                result
            }
            GraphPattern::Path {
                path,
                subject,
                object,
            } => {
                let state = self.state.borrow();
                Ok(self.builder_context.create_property_path(
                    state.active_graph.clone(),
                    state.graph_name_var.clone(),
                    path.clone(),
                    subject.clone(),
                    object.clone(),
                ))
            }
            GraphPattern::Minus { left, right } => {
                let left = self.rewrite_graph_pattern(left)?;
                let right = self.rewrite_graph_pattern(right)?;
                left.minus(right.build()?)
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                let inner = self.rewrite_graph_pattern(inner)?;

                let aggregate_exprs = aggregates
                    .iter()
                    .map(|(var, aggregate)| {
                        self.rewrite_aggregate(inner.schema(), aggregate)
                            .map(|a| (var.clone(), a))
                    })
                    .collect::<DFResult<Vec<_>>>()?;

                inner.group(variables, &aggregate_exprs)
            }
            _ => not_impl_err!("rewrite_graph_pattern: {:?}", pattern),
        }
    }

    /// Checks whether a potential variable in the GRAPH pattern goes out of scope. This is the case
    /// if it either already is out of scope or if the variable is not projected to the outer
    /// query.
    fn graph_variable_goes_out_of_scope(&self, variables: &[Variable]) -> bool {
        let state = self.state.borrow();
        match &state.graph_name_var {
            Some(v) => !variables.contains(v),
            _ => false,
        }
    }

    /// Rewrites an [Expression].
    fn rewrite_expression(
        &self,
        schema: &DFSchema,
        expression: &Expression,
    ) -> DFResult<Expr> {
        let expr_builder_root = self
            .builder_context
            .expr_builder_context_with_schema(schema);
        let expression_rewriter =
            ExpressionRewriter::new(self, expr_builder_root, self.base_iri.as_ref());
        expression_rewriter.rewrite(expression)
    }

    /// Rewrites an [Expression].
    fn rewrite_to_boolean_expression(
        &self,
        schema: &DFSchema,
        expression: &Expression,
    ) -> DFResult<Expr> {
        let expr_builder = self
            .builder_context
            .expr_builder_context_with_schema(schema);
        let expression_rewriter =
            ExpressionRewriter::new(self, expr_builder, self.base_iri.as_ref());
        expression_rewriter.rewrite_to_boolean(expression)
    }

    /// Rewrites an [OrderExpression].
    fn rewrite_order_expression(
        &self,
        schema: &DFSchema,
        expression: &OrderExpression,
    ) -> DFResult<SortExpr> {
        let expr_builder = self
            .builder_context
            .expr_builder_context_with_schema(schema);
        let expression_rewriter =
            ExpressionRewriter::new(self, expr_builder, self.base_iri.as_ref());
        let (asc, expression) = match expression {
            OrderExpression::Asc(inner) => (true, expression_rewriter.rewrite(inner)?),
            OrderExpression::Desc(inner) => (false, expression_rewriter.rewrite(inner)?),
        };
        Ok(expr_builder
            .try_create_builder(expression)?
            .with_encoding(EncodingName::Sortable)?
            .build()?
            .sort(asc, true))
    }

    /// Rewrites an [AggregateExpression].
    pub fn rewrite_aggregate(
        &self,
        schema: &DFSchema,
        expression: &AggregateExpression,
    ) -> DFResult<Expr> {
        let expr_builder = self
            .builder_context
            .expr_builder_context_with_schema(schema);
        let expression_rewriter =
            ExpressionRewriter::new(self, expr_builder, self.base_iri.as_ref());
        match expression {
            AggregateExpression::CountSolutions { distinct } => match distinct {
                false => {
                    let lit = expr_builder
                        .literal(&rdf_fusion_common::Literal::from(1))?
                        .build()?;
                    expr_builder.try_create_builder(lit)?.count(false)?.build()
                }
                true => {
                    let exprs = schema
                        .columns()
                        .into_iter()
                        .map(|c| Expr::from(Column::new_unqualified(c.name())))
                        .collect::<Vec<_>>();
                    Ok(Expr::AggregateFunction(
                        datafusion::logical_expr::expr::AggregateFunction::new_udf(
                            Arc::new(sparql_sum(Arc::clone(
                                self.builder_context.encodings().typed_family(),
                            ))),
                            exprs,
                            true,
                            None,
                            Vec::new(),
                            None,
                        ),
                    ))
                }
            },
            AggregateExpression::FunctionCall {
                name,
                expr,
                distinct,
            } => {
                let expr = expression_rewriter.rewrite(expr)?;
                let expr = expr_builder.try_create_builder(expr)?;
                Ok(match name {
                    AggregateFunction::Avg => expr
                        .with_encoding(EncodingName::TypedFamily)?
                        .avg(*distinct),
                    AggregateFunction::Count => expr.count(*distinct),
                    AggregateFunction::Max => {
                        expr.with_encoding(EncodingName::TypedFamily)?.max()
                    }
                    AggregateFunction::Min => {
                        expr.with_encoding(EncodingName::TypedFamily)?.min()
                    }
                    AggregateFunction::Sample => expr.sample(),
                    AggregateFunction::Sum => expr
                        .with_encoding(EncodingName::TypedFamily)?
                        .sum(*distinct),
                    AggregateFunction::GroupConcat { separator } => expr
                        .with_encoding(EncodingName::TypedFamily)?
                        .group_concat(*distinct, separator.as_deref()),
                    AggregateFunction::Custom(name) => {
                        plan_err!("Unsupported custom aggregate function: {name}")
                    }
                }?
                .build_any())
            }
        }
    }
}

#[derive(Clone)]
struct RewritingState {
    /// Currently active graph.
    active_graph: ActiveGraph,
    /// Indicates whether the graph should be bound to a variable.
    graph_name_var: Option<Variable>,
}

impl Default for RewritingState {
    fn default() -> Self {
        RewritingState {
            active_graph: ActiveGraph::DefaultGraph,
            graph_name_var: None,
        }
    }
}

impl RewritingState {
    /// Uses the new `variable` for the graph name variable.
    #[allow(clippy::unused_self)]
    fn with_graph_variable(&self, variable: Option<Variable>) -> RewritingState {
        RewritingState {
            graph_name_var: variable,
            active_graph: self.active_graph.clone(),
        }
    }

    /// Removes the current graph name variable.
    #[allow(clippy::unused_self)]
    fn without_graph_variable(&self) -> RewritingState {
        RewritingState {
            graph_name_var: None,
            active_graph: self.active_graph.clone(),
        }
    }

    /// Uses the new `active_graph` for the active graph of the query.
    #[allow(clippy::unused_self)]
    fn with_active_graph(&self, active_graph: ActiveGraph) -> RewritingState {
        RewritingState {
            graph_name_var: None,
            active_graph,
        }
    }
}

fn compute_default_active_graph(dataset: &QueryDataset) -> ActiveGraph {
    match dataset.default_graph_graphs() {
        None => ActiveGraph::DefaultGraph,
        Some(graphs) => {
            if matches!(graphs, [GraphName::DefaultGraph]) {
                ActiveGraph::DefaultGraph
            } else {
                ActiveGraph::Union(graphs.to_vec())
            }
        }
    }
}

fn compute_active_graph_for_pattern(
    dataset: &QueryDataset,
    name: &NamedNodePattern,
) -> ActiveGraph {
    match name {
        NamedNodePattern::NamedNode(nn) => {
            if let Some(allowed) = dataset.available_named_graphs() {
                if allowed.contains(&NamedOrBlankNode::NamedNode(nn.clone())) {
                    ActiveGraph::Union(vec![GraphName::NamedNode(nn.clone())])
                } else {
                    ActiveGraph::Union(vec![])
                }
            } else {
                ActiveGraph::Union(vec![GraphName::NamedNode(nn.clone())])
            }
        }
        NamedNodePattern::Variable(_) => match dataset.available_named_graphs() {
            None => ActiveGraph::AnyNamedGraph,
            Some(graphs) => {
                ActiveGraph::Union(graphs.iter().cloned().map(Into::into).collect())
            }
        },
    }
}

/// Extracts sort expressions from possible solution modifiers.
fn get_sort_expressions(graph_pattern: &GraphPattern) -> Option<&Vec<OrderExpression>> {
    match graph_pattern {
        GraphPattern::OrderBy { expression, .. } => Some(expression),
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner, .. }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Reduced { inner, .. }
        | GraphPattern::Group { inner, .. } => get_sort_expressions(inner),
        _ => None,
    }
}
