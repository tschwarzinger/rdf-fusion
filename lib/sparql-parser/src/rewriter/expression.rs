use crate::SparqlParseError;
use crate::ast;
use crate::rewriter::GraphPatternRewriter;
use crate::span::Spanned;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, plan_datafusion_err};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::{Expr, Operator, lit, or};
use rdf_fusion_common::TermRef;
use rdf_fusion_common::vocab::xsd;
use rdf_fusion_common::{Variable, VariableRef};
use rdf_fusion_extensions::functions::BuiltinName as CommonBuiltinName;
use rdf_fusion_logical::{RdfFusionExprBuilder, RdfFusionExprBuilderContext};

pub struct ExpressionRewriter<'rewriter> {
    graph_rewriter: &'rewriter GraphPatternRewriter,
    pub expr_builder_root: RdfFusionExprBuilderContext<'rewriter>,
}

impl<'rewriter> ExpressionRewriter<'rewriter> {
    pub fn new(
        graph_rewriter: &'rewriter GraphPatternRewriter,
        expr_builder_root: RdfFusionExprBuilderContext<'rewriter>,
    ) -> Self {
        Self {
            graph_rewriter,
            expr_builder_root,
        }
    }

    pub fn rewrite_expr_with_aggregates(
        &self,
        expression: &ast::Expression,
        preferred_alias: Option<Variable>,
    ) -> Result<(Expr, Vec<(Variable, Expr)>), SparqlParseError> {
        let expr = self.rewrite_internal(expression)?.build()?;

        if matches!(expr, Expr::AggregateFunction(_)) {
            let alias = preferred_alias.unwrap_or_else(|| {
                Variable::new_unchecked(self.graph_rewriter.next_aggregate_alias())
            });
            return Ok((
                Expr::Column(Column::new_unqualified(alias.as_str())),
                vec![(alias, expr)],
            ));
        }

        let mut aggregates = Vec::new();
        let transformed = expr
            .transform_up(|e: Expr| {
                if matches!(e, Expr::AggregateFunction(_)) {
                    let alias = Variable::new_unchecked(
                        self.graph_rewriter.next_aggregate_alias(),
                    );
                    aggregates.push((alias.clone(), e));
                    Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                        alias.as_str(),
                    ))))
                } else {
                    Ok(Transformed::no(e))
                }
            })
            .map_err(|e| plan_datafusion_err!("Error extracting aggregates: {}", e))?;

        Ok((transformed.data, aggregates))
    }

    pub fn rewrite_scalar_expr(
        &self,
        expression: &ast::Expression,
    ) -> Result<Expr, SparqlParseError> {
        let (expr, aggregates) = self.rewrite_expr_with_aggregates(expression, None)?;
        if !aggregates.is_empty() {
            return Err(SparqlParseError::new_without_span(
                "Aggregates are not allowed in scalar expressions.".to_string(),
            ));
        }
        Ok(expr)
    }

    pub fn rewrite_expr(
        &self,
        expression: &ast::Expression,
    ) -> Result<Expr, SparqlParseError> {
        Ok(self.rewrite_internal(expression)?.build()?)
    }

    pub fn rewrite_scalar_expr_to_boolean(
        &self,
        expression: &ast::Expression,
    ) -> Result<Expr, SparqlParseError> {
        let (expr, aggregates) = self.rewrite_expr_with_aggregates(expression, None)?;
        if !aggregates.is_empty() {
            return Err(SparqlParseError::new_without_span("Aggregates are not allowed in scalar expressions or complex aggregate expressions are not yet supported.".to_string()));
        }
        let builder = self.expr_builder_root.try_create_builder(expr)?;
        Ok(builder.build_effective_boolean_value()?)
    }

    fn rewrite_internal(
        &self,
        expression: &ast::Expression,
    ) -> Result<RdfFusionExprBuilder<'rewriter>, SparqlParseError> {
        let expr = match expression {
            ast::Expression::Not(inner) => match inner.value {
                ast::Expression::Exists(ref pattern) => {
                    self.rewrite_not_exists(pattern.as_ref())?
                }
                _ => self.rewrite_internal(&inner.value)?.not()?,
            },
            ast::Expression::Equal(lhs, rhs) => self
                .rewrite_internal(&lhs.value)?
                .rdf_term_equal(self.rewrite_expr(&rhs.value)?)?,
            ast::Expression::NotEqual(lhs, rhs) => self
                .rewrite_internal(&lhs.value)?
                .rdf_term_equal(self.rewrite_expr(&rhs.value)?)?
                .not()?,
            ast::Expression::Greater(lhs, rhs) => self
                .rewrite_internal(&lhs.value)?
                .greater_than(self.rewrite_expr(&rhs.value)?)?,
            ast::Expression::GreaterOrEqual(lhs, rhs) => self
                .rewrite_internal(&lhs.value)?
                .greater_or_equal(self.rewrite_expr(&rhs.value)?)?,
            ast::Expression::Less(lhs, rhs) => self
                .rewrite_internal(&lhs.value)?
                .less_than(self.rewrite_expr(&rhs.value)?)?,
            ast::Expression::LessOrEqual(lhs, rhs) => self
                .rewrite_internal(&lhs.value)?
                .less_or_equal(self.rewrite_expr(&rhs.value)?)?,
            ast::Expression::Literal(literal) => {
                let common_literal =
                    self.graph_rewriter.planner_context().map_literal(literal)?;
                self.expr_builder_root.literal(&common_literal)?
            }
            ast::Expression::Var(var) => self
                .expr_builder_root
                .variable(VariableRef::new_unchecked(var.value))?,
            ast::Expression::Iri(iri) => {
                let named_node =
                    self.graph_rewriter.planner_context().resolve_iri(iri)?;
                self.expr_builder_root.literal(&named_node)?
            }
            ast::Expression::Function(f) => match &f.name {
                ast::FunctionName::BuiltIn(name) => {
                    self.rewrite_builtin(name, &f.args)?
                }
                ast::FunctionName::Iri(iri) => {
                    let named_node =
                        self.graph_rewriter.planner_context().resolve_iri(iri)?;
                    let rewritten_args = self.rewrite_args(&f.args)?;
                    self.apply_custom_function(named_node, rewritten_args)?
                }
            },
            ast::Expression::Or(lhs, rhs) => {
                self.logical_expression(Operator::Or, &lhs.value, &rhs.value)?
            }
            ast::Expression::And(lhs, rhs) => {
                self.logical_expression(Operator::And, &lhs.value, &rhs.value)?
            }
            ast::Expression::In(lhs, rhs) => self.rewrite_in(&lhs.value, rhs)?,
            ast::Expression::NotIn(lhs, rhs) => {
                self.rewrite_in(&lhs.value, rhs)?.not()?
            }
            ast::Expression::Add(lhs, rhs) => self
                .rewrite_internal(&lhs.value)?
                .add(self.rewrite_expr(&rhs.value)?)?,
            ast::Expression::Subtract(lhs, rhs) => self
                .rewrite_internal(&lhs.value)?
                .sub(self.rewrite_expr(&rhs.value)?)?,
            ast::Expression::Multiply(lhs, rhs) => self
                .rewrite_internal(&lhs.value)?
                .mul(self.rewrite_expr(&rhs.value)?)?,
            ast::Expression::Divide(lhs, rhs) => self
                .rewrite_internal(&lhs.value)?
                .div(self.rewrite_expr(&rhs.value)?)?,
            ast::Expression::UnaryPlus(value) => {
                self.rewrite_internal(&value.value)?.unary_plus()?
            }
            ast::Expression::UnaryMinus(value) => {
                self.rewrite_internal(&value.value)?.unary_minus()?
            }
            ast::Expression::Exists(pattern) => self.rewrite_exists(pattern.as_ref())?,
            ast::Expression::NotExists(pattern) => {
                self.rewrite_not_exists(pattern.as_ref())?
            }
            ast::Expression::Aggregate(agg) => self.rewrite_aggregate(agg)?,
        };
        Ok(expr)
    }

    pub fn rewrite_aggregate(
        &self,
        agg: &ast::Aggregate,
    ) -> Result<RdfFusionExprBuilder<'rewriter>, SparqlParseError> {
        match &agg.name {
            ast::FunctionName::BuiltIn(name) => {
                let name_upper = name.value.to_ascii_uppercase();

                // SAMPLE is a custom aggregate without a corresponding built-in name.
                if name_upper == "SAMPLE" {
                    return Ok(self.rewrite_expr_from_agg(agg)?.sample()?);
                }

                let aggregate =
                    CommonBuiltinName::try_from(name.value).map_err(|_| {
                        SparqlParseError::new(
                            name.span,
                            format!("Unsupported aggregate function: {name_upper}"),
                        )
                    })?;
                match aggregate {
                    CommonBuiltinName::Count => {
                        if agg.star {
                            let lit = self
                                .expr_builder_root
                                .literal(&rdf_fusion_common::Literal::from(1))?
                                .build()?;
                            Ok(self
                                .expr_builder_root
                                .try_create_builder(lit)?
                                .count(agg.distinct)?)
                        } else {
                            Ok(self.rewrite_expr_from_agg(agg)?.count(agg.distinct)?)
                        }
                    }
                    CommonBuiltinName::Sum => {
                        Ok(self.rewrite_expr_from_agg(agg)?.sum(agg.distinct)?)
                    }
                    CommonBuiltinName::Min => {
                        Ok(self.rewrite_expr_from_agg(agg)?.min()?)
                    }
                    CommonBuiltinName::Max => {
                        Ok(self.rewrite_expr_from_agg(agg)?.max()?)
                    }
                    CommonBuiltinName::Avg => {
                        Ok(self.rewrite_expr_from_agg(agg)?.avg(agg.distinct)?)
                    }
                    CommonBuiltinName::GroupConcat => {
                        let separator = self.aggregate_separator(agg);
                        Ok(self
                            .rewrite_expr_from_agg(agg)?
                            .group_concat(agg.distinct, separator.as_deref())?)
                    }
                    _ => Err(SparqlParseError::new(
                        name.span,
                        format!("Unsupported aggregate function: {name_upper}"),
                    )),
                }
            }
            ast::FunctionName::Iri(iri) => {
                let named_node =
                    self.graph_rewriter.planner_context().resolve_iri(iri)?;
                let fn_name = rdf_fusion_extensions::functions::FunctionName::Custom(
                    named_node.clone(),
                );
                let udaf = self
                    .graph_rewriter
                    .builder_context()
                    .registry()
                    .udaf(&fn_name)
                    .map_err(|_| {
                        SparqlParseError::new(
                            iri.span(),
                            format!("Unknown aggregate function: {named_node}"),
                        )
                    })?;
                let rewritten_args = self.rewrite_args(&agg.args)?;
                let expr = Expr::AggregateFunction(AggregateFunction::new_udf(
                    udaf,
                    rewritten_args,
                    agg.distinct,
                    None,
                    Vec::new(),
                    None,
                ));
                Ok(self.expr_builder_root.try_create_builder(expr)?)
            }
        }
    }

    /// Rewrites the first positional expression argument of an aggregate.
    fn rewrite_arg(&self, agg: &ast::Aggregate) -> Result<Expr, SparqlParseError> {
        let Some(arg) = agg.args.iter().find_map(|arg| match arg {
            ast::FunctionArg::Positional(e) => Some(e),
            _ => None,
        }) else {
            let name_str: &str = match &agg.name {
                ast::FunctionName::BuiltIn(s) => s.value,
                ast::FunctionName::Iri(iri) => match iri {
                    ast::Iri::IriRef(s) => s.value.as_ref(),
                    ast::Iri::PrefixedName(s) => s.local.as_ref(),
                },
            };
            return Err(SparqlParseError::new(
                agg.name.span(),
                format!("ast::Aggregate '{name_str}' requires an expression argument"),
            ));
        };
        self.rewrite_expr(&arg.value)
    }

    /// Rewrites the first positional expression argument of an aggregate and wraps it
    /// into a builder.
    fn rewrite_expr_from_agg(
        &self,
        agg: &ast::Aggregate,
    ) -> Result<RdfFusionExprBuilder<'rewriter>, SparqlParseError> {
        let expr = self.rewrite_arg(agg)?;
        Ok(self.expr_builder_root.try_create_builder(expr)?)
    }

    /// Returns the separator for a `GROUP_CONCAT` aggregate, defaulting to a space.
    fn aggregate_separator(&self, agg: &ast::Aggregate) -> Option<String> {
        agg.args
            .iter()
            .find_map(|arg| match arg {
                ast::FunctionArg::Named(_, e) => Some(e),
                _ => None,
            })
            .and_then(|e| {
                // The separator is a simple literal string.
                match &e.value {
                    ast::Expression::Literal(ast::Literal::String(s)) => {
                        Some(s.value.to_string())
                    }
                    _ => None,
                }
            })
    }

    fn rewrite_builtin(
        &self,
        function: &Spanned<&str>,
        args: &[ast::FunctionArg],
    ) -> Result<RdfFusionExprBuilder<'rewriter>, SparqlParseError> {
        let rewritten_args = self.rewrite_args(args)?;

        let name = function.value.to_ascii_uppercase();
        match name.as_str() {
            "IRI" | "URI" => {
                let mut args = rewritten_args;
                if args.len() == 1 {
                    if let Some(base_iri) =
                        self.graph_rewriter.planner_context().base_iri()
                    {
                        let base_lit = self.expr_builder_root.literal(
                            &rdf_fusion_common::Literal::new_simple_literal(
                                base_iri.as_str(),
                            ),
                        )?;
                        args.push(base_lit.build()?);
                    }
                }
                return Ok(self.expr_builder_root.try_create_builder_for_udf(
                    &rdf_fusion_extensions::functions::FunctionName::Builtin(
                        CommonBuiltinName::Iri,
                    ),
                    args,
                )?);
            }
            "SAMETERM" => {
                let arg0 = rewritten_args[0].clone();
                let arg1 = rewritten_args[1].clone();
                let boolean_expr = self
                    .expr_builder_root
                    .try_create_builder(arg0)?
                    .build_same_term(arg1)?;
                return Ok(self
                    .expr_builder_root
                    .native_boolean_as_term(boolean_expr)?);
            }
            "NOW" => {
                let literal = rdf_fusion_common::Literal::new_typed_literal(
                    self.graph_rewriter.planner_context().now().to_string(),
                    xsd::DATE_TIME,
                );
                return Ok(self
                    .expr_builder_root
                    .literal(TermRef::from(literal.as_ref()))?);
            }
            _ => {}
        }

        let common_builtin =
            CommonBuiltinName::try_from(function.value).map_err(|_| {
                SparqlParseError::new(
                    function.span,
                    format!("Unsupported built-in function: {name}"),
                )
            })?;

        Ok(self.expr_builder_root.try_create_builder_for_udf(
            &rdf_fusion_extensions::functions::FunctionName::Builtin(common_builtin),
            rewritten_args,
        )?)
    }

    /// Rewrites the arguments of a function call, in order, into DataFusion expressions.
    fn rewrite_args(
        &self,
        args: &[ast::FunctionArg],
    ) -> Result<Vec<Expr>, SparqlParseError> {
        args.iter()
            .map(|arg| match arg {
                ast::FunctionArg::Positional(e) => self.rewrite_expr(&e.value),
                ast::FunctionArg::Named(_, e) => self.rewrite_expr(&e.value),
            })
            .collect()
    }

    /// Applies a user-defined (IRI) function.
    fn apply_custom_function(
        &self,
        named_node: rdf_fusion_common::NamedNode,
        args: Vec<Expr>,
    ) -> Result<RdfFusionExprBuilder<'rewriter>, SparqlParseError> {
        let name = rdf_fusion_extensions::functions::FunctionName::Custom(named_node);
        Ok(self
            .expr_builder_root
            .try_create_builder_for_udf(&name, args)?)
    }

    fn rewrite_in(
        &self,
        lhs: &ast::Expression,
        rhs: &[Spanned<ast::Expression>],
    ) -> Result<RdfFusionExprBuilder<'rewriter>, SparqlParseError> {
        let lhs = self.rewrite_internal(lhs)?;
        let expressions = rhs
            .iter()
            .map(|e| -> Result<_, SparqlParseError> {
                Ok(lhs
                    .clone()
                    .rdf_term_equal(self.rewrite_expr(&e.value)?)?
                    .build_effective_boolean_value()?)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let result = expressions
            .into_iter()
            .reduce(or)
            .unwrap_or_else(|| lit(false));
        Ok(self.expr_builder_root.native_boolean_as_term(result)?)
    }

    fn rewrite_exists(
        &self,
        inner: &ast::GraphPattern,
    ) -> Result<RdfFusionExprBuilder<'rewriter>, SparqlParseError> {
        let exists_plan = self.graph_rewriter.rewrite(inner, None)?;
        Ok(self.expr_builder_root.exists(exists_plan)?)
    }

    fn rewrite_not_exists(
        &self,
        inner: &ast::GraphPattern,
    ) -> Result<RdfFusionExprBuilder<'rewriter>, SparqlParseError> {
        let exists_plan = self.graph_rewriter.rewrite(inner, None)?;
        Ok(self.expr_builder_root.not_exists(exists_plan)?)
    }

    fn logical_expression(
        &self,
        operator: Operator,
        lhs: &ast::Expression,
        rhs: &ast::Expression,
    ) -> Result<RdfFusionExprBuilder<'rewriter>, SparqlParseError> {
        let lhs = self
            .rewrite_internal(lhs)?
            .build_effective_boolean_value()?;
        let rhs = self
            .rewrite_internal(rhs)?
            .build_effective_boolean_value()?;

        match operator {
            Operator::And => Ok(self.expr_builder_root.and(lhs, rhs)?),
            Operator::Or => Ok(self.expr_builder_root.sparql_or(lhs, rhs)?),
            _ => Err(SparqlParseError::new_without_span(format!(
                "Unsupported logical expression: {operator}"
            ))),
        }
    }
}
