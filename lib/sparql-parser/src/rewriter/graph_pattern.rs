use crate::ast;
use crate::rewriter::{ExpressionRewriter, RewriterContext};
use crate::span::Span;
use crate::{DiagnosticsGroup, SparqlParseError, SparqlSyntaxError};
use datafusion_common::{Column, DFSchema};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, LogicalPlan, SortExpr, or};
use rdf_fusion_common::sparql::{
    GraphNamePattern, GroundTerm, NamedNodePattern, PropertyPathExpression, QuadPattern,
    QueryDataset, TermPattern, TriplePattern,
};
use rdf_fusion_common::{
    BlankNode as CommonBlankNode, GraphName, NamedNode, TermRef, Variable,
};
use rdf_fusion_encoding::EncodingName;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_logical::join::SparqlJoinType;
use rdf_fusion_logical::{
    RdfFusionLogicalPlanBuilder, RdfFusionLogicalPlanBuilderContext,
};
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;

/// A rewriter that can transforms [`ast::GraphPattern`] to a [`LogicalPlan`].
pub struct GraphPatternRewriter {
    builder_context: RdfFusionLogicalPlanBuilderContext,
    planner_context: RewriterContext,
    state: RefCell<RewriteState>,
}

/// The result from analyzing a `SELECT`.
struct SelectInformation<'a, 'b> {
    expr_ast: Option<&'b ast::Expression<'a>>,
    pre_rewritten: Option<Expr>,
    has_aggs: bool,
    var_name: Variable,
    var_span: Span,
}

impl GraphPatternRewriter {
    pub fn new(
        builder_context: RdfFusionLogicalPlanBuilderContext,
        planner_context: RewriterContext,
    ) -> Self {
        let active_graph = compute_default_active_graph(planner_context.dataset());
        Self::with_state(
            builder_context,
            planner_context,
            RewriteState::new(active_graph),
        )
    }

    /// Constructs a rewriter whose output is evaluated within the given (immutable) graph
    /// context, so that the active graph propagates downwards without mutating shared state.
    fn with_state(
        builder_context: RdfFusionLogicalPlanBuilderContext,
        planner_context: RewriterContext,
        state: RewriteState,
    ) -> Self {
        Self {
            builder_context,
            planner_context,
            state: RefCell::new(state),
        }
    }

    pub fn next_aggregate_alias(&self) -> String {
        CommonBlankNode::default().into_string()
    }

    pub fn next_bnode_term(&self) -> TermPattern {
        TermPattern::BlankNode(CommonBlankNode::default())
    }

    pub fn next_guid_var(&self) -> Variable {
        Variable::new_unchecked(CommonBlankNode::default().into_string())
    }

    /// Returns a reference to the [`RewriterContext`].
    pub fn builder_context(&self) -> &RdfFusionLogicalPlanBuilderContext {
        &self.builder_context
    }

    pub fn planner_context(&self) -> &RewriterContext {
        &self.planner_context
    }

    /// Rewrites a SPARQL graph pattern into a DataFusion logical plan, ensuring that the output
    /// has the given output encoding. If [`None`] is given, no encoding transformation is applied.
    pub fn rewrite(
        &self,
        pattern: &ast::GraphPattern,
        output_encoding_name: Option<EncodingName>,
    ) -> Result<LogicalPlan, SparqlParseError> {
        let plan = self.rewrite_graph_pattern(pattern)?;
        Ok(match output_encoding_name {
            None => plan.build()?,
            Some(encoding) => plan.with_encoding(encoding)?.build()?,
        })
    }

    /// Rewrites a SPARQL graph pattern into a DataFusion logical plan.
    ///
    /// This method does consider the scoping rules of SPARQL filters. All filters will be evaluated
    /// last.
    pub fn rewrite_graph_pattern(
        &self,
        pattern: &ast::GraphPattern,
    ) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        let (plan, deferred_filters) = self.rewrite_pattern_internal(pattern)?;

        // Decode all columns referenced in the filter, leaving columns that are not referenced
        // untouched.
        let decoded_expr_builder_ctx = self
            .builder_context
            .expr_builder_context_with_schema(plan.decoded_schema());
        let decoded_expr_builder =
            ExpressionRewriter::new(self, decoded_expr_builder_ctx);
        let decoded_filters = deferred_filters
            .iter()
            .map(|exp| decoded_expr_builder.rewrite_scalar_expr_to_boolean(exp))
            .collect::<Result<Vec<_>, _>>()?;
        let filter = conjunction(decoded_filters);
        let plan = if let Some(filter) = filter {
            plan.decode_for_exprs(std::slice::from_ref(&filter))?
        } else {
            plan
        };

        // Build the actual expression using the schema that may still contain object id
        // columns, thus allowing evaluating `EXISTS` queries without forced decoding.
        let real_schema_expr_builder_ctx = self
            .builder_context
            .expr_builder_context_with_schema(plan.schema());
        let real_schema_expr_builder =
            ExpressionRewriter::new(self, real_schema_expr_builder_ctx);
        let actual_filters = deferred_filters
            .iter()
            .map(|exp| real_schema_expr_builder.rewrite_scalar_expr_to_boolean(exp))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(filter) = conjunction(actual_filters) {
            Ok(plan.filter(filter)?)
        } else {
            Ok(plan)
        }
    }

    /// Evaluates a graph pattern but DEFERS the application of FILTER expressions.
    /// This is required because FILTERs inside an OPTIONAL block must be applied as
    /// LEFT JOIN conditions rather than standard Filter operators.
    fn rewrite_pattern_internal<'a>(
        &self,
        pattern: &'a ast::GraphPattern,
    ) -> Result<
        (RdfFusionLogicalPlanBuilder, Vec<&'a ast::Expression<'a>>),
        SparqlParseError,
    > {
        match pattern {
            ast::GraphPattern::Group(elements) => {
                let mut deferred_filters = Vec::new();
                let mut used_vars = std::collections::HashMap::new();

                // 1. Pre-scan: extract filters and validate variable bindings
                for element in elements {
                    match &element.value {
                        ast::GraphPatternElement::Filter(expr) => {
                            deferred_filters.push(&expr.value);
                        }
                        ast::GraphPatternElement::Bind(_, var) => {
                            if let Some(&prev_span) = used_vars.get(var.value) {
                                return Err(SparqlParseError::new_with_info(
                                    var.span,
                                    format!("Variable ?{} used here", var.value),
                                    format!(
                                        "Variable ?{} is already used in the group graph pattern",
                                        var.value
                                    ),
                                    prev_span,
                                    format!(
                                        "Variable ?{} previously used here",
                                        var.value
                                    ),
                                ));
                            }
                            collect_variables(&element.value, &mut used_vars);
                        }
                        _ => {
                            collect_variables(&element.value, &mut used_vars);
                        }
                    }
                }

                // 2. Build the logical plan. A `BgpBuilder` owns the running plan and the
                //    collection of pending triple patterns, automatically flushing accumulated
                //    basic graph patterns whenever a non-BGP element is encountered.
                let mut bgp = BgpBuilder::new(self);
                for element in elements {
                    let bgp_starting = matches!(
                        element.value,
                        ast::GraphPatternElement::Filter(_)
                            | ast::GraphPatternElement::Triples(_)
                            | ast::GraphPatternElement::Bind(_, _)
                    );
                    if bgp_starting {
                        bgp.open_bgp();
                    } else {
                        bgp.close_bgp();
                    }

                    match &element.value {
                        ast::GraphPatternElement::Filter(_) => {
                            // Ignored here; we already collected them in the pre-scan
                        }
                        ast::GraphPatternElement::Triples(triples) => {
                            bgp.accumulate_triples(triples)?;
                        }
                        ast::GraphPatternElement::Optional(optional_pattern) => {
                            // OPTIONAL explicitly consumes its internal filters as join conditions
                            let (right_plan, opt_filters) =
                                self.rewrite_pattern_internal(optional_pattern.as_ref())?;
                            bgp.with_optional(right_plan, opt_filters)?;
                        }
                        ast::GraphPatternElement::Minus(minus_pattern) => {
                            let right_plan =
                                self.rewrite_graph_pattern(minus_pattern.as_ref())?;
                            bgp.with_minus(right_plan)?;
                        }
                        ast::GraphPatternElement::Bind(expr, var) => {
                            bgp.with_bind(&expr.value, var)?;
                        }
                        _ => {
                            let plan = self.rewrite_element(&element.value)?;
                            bgp.with_element(plan)?;
                        }
                    }
                }

                Ok((bgp.into_plan()?, deferred_filters))
            }
            ast::GraphPattern::SubSelect(s) => {
                let (active_graph, parent_graph_name_var) = {
                    let state = self.state.borrow();
                    (state.active_graph.clone(), state.graph_name_var.clone())
                };
                let graph_name_var = match parent_graph_name_var {
                    Some(g_var) => {
                        let projects_g = match &s.select_clause.bindings.value {
                            ast::SelectVariables::Explicit(explicit_vars) => {
                                explicit_vars
                                    .iter()
                                    .any(|v| v.value.variable.value == g_var.as_str())
                            }
                            ast::SelectVariables::Star => {
                                let mut inner_vars = HashSet::new();
                                collect_in_scope_variables(
                                    &s.where_clause,
                                    s.values_clause.as_ref(),
                                    &mut inner_vars,
                                );
                                inner_vars.contains(g_var.as_str())
                            }
                        };
                        if projects_g {
                            Some(g_var.clone())
                        } else {
                            None
                        }
                    }
                    None => None,
                };
                let mut sub_state = RewriteState::new(active_graph);
                sub_state.graph_name_var = graph_name_var;
                let sub_rewriter = GraphPatternRewriter::with_state(
                    self.builder_context.clone(),
                    self.planner_context.clone(),
                    sub_state,
                );

                let plan = sub_rewriter.rewrite_graph_pattern(&s.where_clause)?;
                let plan = sub_rewriter.apply_modifiers_and_select(
                    plan,
                    &s.solution_modifier,
                    Some(&s.select_clause),
                    Some(&s.where_clause),
                    s.values_clause.as_ref(),
                )?;
                Ok((plan, vec![]))
            }
        }
    }

    pub fn apply_modifiers_and_select<'a>(
        &self,
        mut plan: RdfFusionLogicalPlanBuilder,
        modifier: &ast::SolutionModifier<'a>,
        select_clause: Option<&ast::SelectClause<'a>>,
        where_pattern: Option<&ast::GraphPattern<'a>>,
        values_clause: Option<&ast::ValuesClause<'a>>,
    ) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        if let Some(values) = values_clause {
            let values_plan = self.rewrite_values_clause(values)?;
            plan = plan.join(values_plan.build()?, SparqlJoinType::Inner, None)?;
        }

        let (new_plan, group_vars) = self.extend_group_by(plan, modifier)?;
        plan = new_plan;

        let pre_group_schema = Arc::clone(plan.decoded_schema());
        let mut all_aggregates = Vec::new();

        let having_exprs =
            self.extract_having(&pre_group_schema, modifier, &mut all_aggregates)?;
        let order_exprs =
            self.extract_order_by(&pre_group_schema, modifier, &mut all_aggregates)?;

        let select_extensions = if let Some(select) = select_clause {
            Some(self.extract_select_aggregates(
                &pre_group_schema,
                select,
                &mut all_aggregates,
            )?)
        } else {
            None
        };

        let has_grouping = !group_vars.is_empty() || !all_aggregates.is_empty();
        if has_grouping {
            plan = plan.group(&group_vars, &all_aggregates)?;
        }

        plan = self.apply_having(plan, having_exprs)?;

        let has_distinct = select_clause.is_some_and(|s| {
            matches!(
                s.option,
                ast::SelectionOption::Distinct | ast::SelectionOption::Reduced
            )
        });
        let (new_plan, sort_exprs) =
            self.apply_order_by(plan, order_exprs, has_distinct)?;
        plan = new_plan;

        if let Some(select) = select_clause {
            plan = self.apply_select_projection(
                plan,
                &pre_group_schema,
                select,
                where_pattern,
                values_clause,
                modifier,
                select_extensions.unwrap(),
                has_grouping,
            )?;
        }

        if has_distinct {
            plan = plan.distinct_with_sort(sort_exprs)?;
        }

        plan = self.apply_limit_offset(plan, modifier)?;

        Ok(plan)
    }

    fn extend_group_by<'a>(
        &self,
        mut plan: RdfFusionLogicalPlanBuilder,
        modifier: &ast::SolutionModifier<'a>,
    ) -> Result<(RdfFusionLogicalPlanBuilder, Vec<Variable>), SparqlParseError> {
        let mut group_vars = Vec::new();
        for (i, (expr, var)) in modifier.group_clause.iter().enumerate() {
            let schema = Arc::clone(plan.decoded_schema());
            let expr_builder_context = self
                .builder_context
                .expr_builder_context_with_schema(&schema);
            let expr_rewriter = ExpressionRewriter::new(self, expr_builder_context);
            let rewritten = expr_rewriter.rewrite_scalar_expr(&expr.value)?;

            let var_name = if let Some(v) = var {
                Variable::new_unchecked(v.value)
            } else {
                Variable::new_unchecked(format!("__group_{i}"))
            };

            let var_name = match (&expr.value, var) {
                (ast::Expression::Var(v), None) => Variable::new_unchecked(v.value),
                _ => {
                    plan = plan.extend(var_name.clone(), rewritten)?;
                    var_name
                }
            };
            group_vars.push(var_name);
        }
        Ok((plan, group_vars))
    }

    fn extract_having<'a>(
        &self,
        schema: &DFSchema,
        modifier: &ast::SolutionModifier<'a>,
        all_aggregates: &mut Vec<(Variable, Expr)>,
    ) -> Result<Vec<Expr>, SparqlParseError> {
        let expr_builder_context = self
            .builder_context
            .expr_builder_context_with_schema(schema);
        let expr_rewriter = ExpressionRewriter::new(self, expr_builder_context);
        let mut having_exprs = Vec::new();
        for having_expr in &modifier.having_clause {
            let (rewritten, aggs) =
                expr_rewriter.rewrite_expr_with_aggregates(&having_expr.value, None)?;
            having_exprs.push(rewritten);
            all_aggregates.extend(aggs);
        }
        Ok(having_exprs)
    }

    fn extract_order_by<'a>(
        &self,
        schema: &DFSchema,
        modifier: &ast::SolutionModifier<'a>,
        all_aggregates: &mut Vec<(Variable, Expr)>,
    ) -> Result<Vec<(Expr, bool)>, SparqlParseError> {
        let expr_builder_context = self
            .builder_context
            .expr_builder_context_with_schema(schema);
        let expr_rewriter = ExpressionRewriter::new(self, expr_builder_context);
        let mut order_exprs = Vec::new();
        for condition in &modifier.order_clause {
            let (expr_ast, asc) = match condition {
                ast::OrderCondition::Asc(e) => (&e.value, true),
                ast::OrderCondition::Desc(e) => (&e.value, false),
                ast::OrderCondition::Plain(e) => (&e.value, true),
            };

            let (rewritten, aggs) =
                expr_rewriter.rewrite_expr_with_aggregates(expr_ast, None)?;
            order_exprs.push((rewritten, asc));
            all_aggregates.extend(aggs);
        }
        Ok(order_exprs)
    }

    fn extract_select_aggregates<'a, 'b>(
        &self,
        schema: &DFSchema,
        select_clause: &'b ast::SelectClause<'a>,
        all_aggregates: &mut Vec<(Variable, Expr)>,
    ) -> Result<Vec<SelectInformation<'a, 'b>>, SparqlParseError> {
        let expr_builder_context = self
            .builder_context
            .expr_builder_context_with_schema(schema);
        let expr_rewriter = ExpressionRewriter::new(self, expr_builder_context);
        let mut select_explicit_vars = Vec::new();

        if let ast::SelectVariables::Explicit(vars) = &select_clause.bindings.value {
            let mut assigned_vars = std::collections::HashMap::new();
            for spanned_var in vars {
                let select_var = &spanned_var.value;
                let expr_opt = &select_var.expression;
                let var_ast = &select_var.variable;
                let var_name = Variable::new_unchecked(var_ast.value);
                let var_span = var_ast.span;
                let mut pre_rewritten = None;
                let mut has_aggs = false;

                if let Some(expr_ast) = expr_opt {
                    if let Some(&prev_span) = assigned_vars.get(var_ast.value) {
                        return Err(SparqlParseError::new_with_info(
                            var_span,
                            format!("Variable ?{} assigned here", var_ast.value),
                            format!(
                                "Variable ?{} is already assigned in the SELECT clause",
                                var_ast.value
                            ),
                            prev_span,
                            format!(
                                "Variable ?{} previously assigned here",
                                var_ast.value
                            ),
                        ));
                    }
                    if schema.has_column_with_unqualified_name(var_ast.value) {
                        return Err(SparqlParseError::new(
                            var_span,
                            format!(
                                "Variable ?{} is already in-scope and cannot be reassigned",
                                var_ast.value
                            ),
                        ));
                    }
                    assigned_vars.insert(var_ast.value, var_span);

                    let (rewritten, aggs) = expr_rewriter.rewrite_expr_with_aggregates(
                        &expr_ast.value,
                        Some(var_name.clone()),
                    )?;
                    has_aggs = !aggs.is_empty();
                    all_aggregates.extend(aggs);
                    pre_rewritten = Some(rewritten);
                }

                select_explicit_vars.push(SelectInformation {
                    expr_ast: expr_opt.as_ref().map(|e| &e.value),
                    pre_rewritten,
                    has_aggs,
                    var_name,
                    var_span,
                });
            }
        }
        Ok(select_explicit_vars)
    }

    fn apply_having(
        &self,
        mut plan: RdfFusionLogicalPlanBuilder,
        having_exprs: Vec<Expr>,
    ) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        for expr in having_exprs {
            let schema = Arc::clone(plan.decoded_schema());
            let expr_builder_context = self
                .builder_context
                .expr_builder_context_with_schema(&schema);
            let builder = expr_builder_context.try_create_builder(expr)?;
            plan = plan.filter(builder.build_effective_boolean_value()?)?;
        }
        Ok(plan)
    }

    fn apply_order_by(
        &self,
        mut plan: RdfFusionLogicalPlanBuilder,
        order_exprs: Vec<(Expr, bool)>,
        has_distinct: bool,
    ) -> Result<(RdfFusionLogicalPlanBuilder, Vec<SortExpr>), SparqlParseError> {
        let mut sort_exprs = Vec::new();
        for (expr, asc) in order_exprs {
            let schema = Arc::clone(plan.decoded_schema());
            let expr_builder_context = self
                .builder_context
                .expr_builder_context_with_schema(&schema);
            let sort_expr = expr_builder_context
                .try_create_builder(expr)?
                .build_as_sortable_bytes()?;
            sort_exprs.push(SortExpr::new(sort_expr, asc, true));
        }

        if !has_distinct && !sort_exprs.is_empty() {
            plan = plan.sort(sort_exprs.clone())?;
        }
        Ok((plan, sort_exprs))
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_select_projection<'a, 'b>(
        &self,
        mut plan: RdfFusionLogicalPlanBuilder,
        pre_group_schema: &DFSchema,
        select_clause: &ast::SelectClause<'a>,
        where_pattern: Option<&ast::GraphPattern<'a>>,
        values_clause: Option<&ast::ValuesClause<'a>>,
        modifier: &ast::SolutionModifier<'a>,
        extensions: Vec<SelectInformation<'a, 'b>>,
        has_grouping: bool,
    ) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        let mut project_vars = Vec::new();
        let is_star = matches!(select_clause.bindings.value, ast::SelectVariables::Star);

        let mut in_scope_vars = HashSet::new();
        if is_star {
            if let Some(pattern) = where_pattern {
                collect_in_scope_variables(pattern, values_clause, &mut in_scope_vars);
            }
            for (_expr, opt_var) in &modifier.group_clause {
                if let Some(v) = opt_var {
                    in_scope_vars.insert(v.value);
                }
            }
        }

        let grouped_vars: Vec<&str> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        if has_grouping {
            if is_star {
                for f in pre_group_schema.fields() {
                    let name = f.name().as_str();
                    if in_scope_vars.contains(name) && !grouped_vars.contains(&name) {
                        return Err(SparqlParseError::new(
                            select_clause.bindings.span,
                            format!(
                                "Variable ?{name} is projected in SELECT * but not grouped or aggregated"
                            ),
                        ));
                    }
                }
            } else {
                for ext in &extensions {
                    if let Some(expr_ast) = ext.expr_ast {
                        let mut non_agg_vars = std::collections::HashMap::new();
                        collect_non_aggregate_expr_variables(
                            self,
                            expr_ast,
                            &mut non_agg_vars,
                        );
                        let mut sorted_vars: Vec<(&str, Span)> =
                            non_agg_vars.into_iter().collect();
                        sorted_vars.sort_unstable_by_key(|(v, _)| *v);
                        for (v, span) in sorted_vars {
                            if !grouped_vars.contains(&v) {
                                return Err(SparqlParseError::new(
                                    span,
                                    format!(
                                        "Variable ?{v} is projected but not grouped or aggregated"
                                    ),
                                ));
                            }
                        }
                    } else if !grouped_vars.contains(&ext.var_name.as_str()) {
                        return Err(SparqlParseError::new(
                            ext.var_span,
                            format!(
                                "Variable ?{} is projected but not grouped or aggregated",
                                ext.var_name.as_str()
                            ),
                        ));
                    }
                }
            }
        }

        for ext in extensions {
            let var_in_schema = plan
                .schema()
                .has_column_with_unqualified_name(ext.var_name.as_str());
            if let Some(expr_ast) = ext.expr_ast {
                let rewritten = if ext.has_aggs {
                    ext.pre_rewritten.unwrap()
                } else {
                    let schema = Arc::clone(plan.decoded_schema());
                    let expr_builder_context = self
                        .builder_context
                        .expr_builder_context_with_schema(&schema);
                    let expr_rewriter =
                        ExpressionRewriter::new(self, expr_builder_context);
                    let (rewritten, _) = expr_rewriter.rewrite_expr_with_aggregates(
                        expr_ast,
                        Some(ext.var_name.clone()),
                    )?;
                    rewritten
                };

                if rewritten
                    != Expr::Column(Column::new_unqualified(ext.var_name.as_str()))
                {
                    plan = plan.extend(ext.var_name.clone(), rewritten)?;
                }
            } else if !var_in_schema {
                let schema = Arc::clone(plan.decoded_schema());
                let expr_builder_context = self
                    .builder_context
                    .expr_builder_context_with_schema(&schema);
                let unbound_expr = expr_builder_context
                    .variable(rdf_fusion_common::VariableRef::new_unchecked(
                        ext.var_name.as_str(),
                    ))?
                    .build()?;
                plan = plan.extend(ext.var_name.clone(), unbound_expr)?;
            }
            project_vars.push(ext.var_name);
        }

        let current_schema = plan.schema();
        let current_vars: Vec<&str> = current_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        if is_star {
            let mut star_vars = Vec::new();
            for f in current_schema.fields() {
                let name = f.name().as_str();
                if in_scope_vars.contains(name) {
                    star_vars.push(Variable::new_unchecked(name));
                }
            }
            for &var_name in &in_scope_vars {
                if !plan.schema().has_column_with_unqualified_name(var_name) {
                    let schema = Arc::clone(plan.decoded_schema());
                    let expr_builder_context = self
                        .builder_context
                        .expr_builder_context_with_schema(&schema);
                    let unbound_expr = expr_builder_context
                        .variable(rdf_fusion_common::VariableRef::new_unchecked(
                            var_name,
                        ))?
                        .build()?;
                    let v = Variable::new_unchecked(var_name);
                    plan = plan.extend(v.clone(), unbound_expr)?;
                    star_vars.push(v);
                }
            }
            let current_vars: Vec<&str> = plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            let star_vars_str: Vec<&str> = star_vars.iter().map(|v| v.as_str()).collect();
            if current_vars != star_vars_str {
                plan = plan.project(&star_vars)?;
            }
        } else {
            let project_vars_str: Vec<&str> =
                project_vars.iter().map(|v| v.as_str()).collect();
            if current_vars != project_vars_str {
                plan = plan.project(&project_vars)?;
            }
        }

        Ok(plan)
    }

    fn apply_limit_offset<'a>(
        &self,
        mut plan: RdfFusionLogicalPlanBuilder,
        modifier: &ast::SolutionModifier<'a>,
    ) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        if let Some(limit_offset) = &modifier.limit_offset_clauses {
            let offset = limit_offset.offset.map_or(0, |o| o.value as usize);
            let limit = limit_offset.limit.map(|l| l.value as usize);
            plan = plan.slice(offset, limit)?;
        }
        Ok(plan)
    }

    pub fn rewrite_values_clause<'a>(
        &self,
        values: &ast::ValuesClause<'a>,
    ) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        let variables = values
            .variables
            .iter()
            .map(|v| Variable::new_unchecked(v.value))
            .collect::<Vec<_>>();
        let mut bindings = Vec::new();
        for row in &values.values.value {
            if row.len() != variables.len() {
                let diag = if let (Some(first_var), Some(last_var)) =
                    (values.variables.first(), values.variables.last())
                {
                    let vars_span = Span::new(first_var.span.start, last_var.span.end);
                    DiagnosticsGroup::error(format!(
                        "VALUES clause has {} variables but row has {} values",
                        variables.len(),
                        row.len()
                    ))
                    .with_error(values.values.span, "row values defined here")
                    .with_info(
                        vars_span,
                        format!("{} variables defined here", variables.len()),
                    )
                } else {
                    DiagnosticsGroup::error(format!(
                        "VALUES clause has {} variables but row has {} values",
                        variables.len(),
                        row.len()
                    ))
                    .with_error(values.values.span, "row values defined here")
                };
                return Err(SparqlParseError::Syntax(SparqlSyntaxError::single(diag)));
            }
            let mut binding = Vec::new();
            for value in row {
                match value {
                    ast::DataBlockValue::Iri(iri) => {
                        let named_node = self.planner_context.resolve_iri(iri)?;
                        binding.push(Some(GroundTerm::NamedNode(named_node)));
                    }
                    ast::DataBlockValue::Literal(lit) => {
                        let common_literal = self.planner_context.map_literal(lit)?;
                        binding.push(Some(GroundTerm::Literal(common_literal)));
                    }
                    ast::DataBlockValue::Undef => {
                        binding.push(None);
                    }
                }
            }
            bindings.push(binding);
        }
        Ok(self.builder_context.create_values(&variables, &bindings)?)
    }

    fn build_optional_filter<'a>(
        &self,
        left: &RdfFusionLogicalPlanBuilder,
        right: &RdfFusionLogicalPlanBuilder,
        filters: Vec<&'a ast::Expression<'a>>,
    ) -> Result<Option<Expr>, SparqlParseError> {
        if filters.is_empty() {
            return Ok(None);
        }

        let mut schema = left.decoded_schema().as_ref().clone();
        schema.merge(right.decoded_schema());
        let expr_builder_context = self
            .builder_context
            .expr_builder_context_with_schema(&schema);
        let expr_rewriter = ExpressionRewriter::new(self, expr_builder_context);

        let mut combined_expr: Option<Expr> = None;
        for filter_expr in filters {
            let expr = expr_rewriter.rewrite_scalar_expr_to_boolean(filter_expr)?;
            if let Some(c) = combined_expr {
                combined_expr = Some(c.and(expr));
            } else {
                combined_expr = Some(expr);
            }
        }

        Ok(combined_expr)
    }

    fn rewrite_element(
        &self,
        element: &ast::GraphPatternElement,
    ) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        match element {
            ast::GraphPatternElement::Triples(triples) => {
                let mut bgp_patterns = Vec::new();
                let mut path_plans = Vec::new();
                self.collect_bgp_patterns(triples, &mut bgp_patterns, &mut path_plans)?;
                self.build_bgp_plan(&bgp_patterns, path_plans)
            }
            ast::GraphPatternElement::Union(patterns) => {
                if patterns.is_empty() {
                    return Ok(self.builder_context.create_empty_solution());
                }
                let mut plan = self.rewrite_graph_pattern(&patterns[0])?;
                for pattern in patterns.iter().skip(1) {
                    let right = self.rewrite_graph_pattern(pattern)?;
                    plan = plan.union(right.build()?)?;
                }
                Ok(plan)
            }
            ast::GraphPatternElement::Values(values) => self.rewrite_values_clause(values),
            ast::GraphPatternElement::Graph { name, pattern } => {
                let (next_state, graph_var_opt) = {
                    let mut state = self.state.borrow().clone();
                    let (active_graph, graph_var_opt) = match name {
                        ast::VarOrIri::Var(v) => {
                            let var = Variable::new_unchecked(v.value);
                            state.graph_name_var = Some(var.clone());
                            let ag = match self
                                .planner_context
                                .dataset()
                                .available_named_graphs()
                            {
                                None => ActiveGraph::AnyNamedGraph,
                                Some(graphs) => {
                                    let names =
                                        graphs.iter().map(|g| g.clone().into()).collect();
                                    ActiveGraph::Union(names)
                                }
                            };
                            (ag, Some(var))
                        }
                        ast::VarOrIri::Iri(iri) => {
                            state.graph_name_var = None;
                            let named_node = self.planner_context().resolve_iri(iri)?;
                            let ag = match self
                                .planner_context
                                .dataset()
                                .available_named_graphs()
                            {
                                None => ActiveGraph::Union(vec![named_node.into()]),
                                Some(graphs) => {
                                    if graphs.contains(
                                        &rdf_fusion_common::NamedOrBlankNode::NamedNode(
                                            named_node.clone(),
                                        ),
                                    ) {
                                        ActiveGraph::Union(vec![named_node.into()])
                                    } else {
                                        // The graph is not part of the dataset's named graphs.
                                        ActiveGraph::Union(Vec::new())
                                    }
                                }
                            };
                            (ag, None)
                        }
                    };
                    state.active_graph = active_graph;
                    (state, graph_var_opt)
                };

                let old_state = self.state.replace(next_state);
                let mut plan = self.rewrite_graph_pattern(pattern)?;
                self.state.replace(old_state);

                if let Some(graph_var) = graph_var_opt {
                    if plan.schema().has_column(
                        &Column::new_unqualified(graph_var.as_str()),
                    ) {
                        if let Some(graphs) =
                            self.planner_context.dataset().available_named_graphs()
                        {
                            let schema = Arc::clone(plan.decoded_schema());
                            let expr_builder_context = self
                                .builder_context
                                .expr_builder_context_with_schema(&schema);
                            let var_builder =
                                expr_builder_context.variable(graph_var.as_ref())?;
                            let mut graph_filters = Vec::new();
                            for g in graphs {
                                if let rdf_fusion_common::NamedOrBlankNode::NamedNode(
                                    nn,
                                ) = g
                                {
                                    let filter =
                                        var_builder.clone().build_same_term_scalar(
                                            TermRef::from(nn.as_ref()),
                                        )?;
                                    graph_filters.push(filter);
                                }
                            }
                            if let Some(combined) = graph_filters.into_iter().reduce(or) {
                                plan = plan.filter(combined)?;
                            }
                        }
                    }
                }

                Ok(plan)
            }
            ast::GraphPatternElement::Service { .. } => {
                Err(SparqlParseError::new_without_span("SPARQL federated queries are not yet supported in RDF Fusion. See https://codeberg.org/tschwarzinger/rdf-fusion/issues/131".to_string()))
            }
            _ => Err(SparqlParseError::new_without_span("Graph pattern element not yet implemented".to_string())),
        }
    }

    /// Collects the `TriplePattern`s (and any property-path sub-plans) of a
    /// [`ast::GraphPatternElement::Triples`] block into the given buffers, without
    /// building a logical plan.
    fn collect_bgp_patterns<'a>(
        &self,
        triples: &[(ast::GraphNodePath<'a>, ast::PropertyListPath<'a>)],
        patterns: &mut Vec<TriplePattern>,
        path_plans: &mut Vec<RdfFusionLogicalPlanBuilder>,
    ) -> Result<(), SparqlParseError> {
        for (subject, property_list) in triples {
            let s =
                self.graph_node_path_to_term_pattern(subject, patterns, path_plans)?;
            for (verb, objects) in property_list.iter() {
                for object in objects {
                    self.push_property_list_path_element(
                        &s, verb, object, patterns, path_plans,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Builds a logical plan from a set of collected basic graph pattern
    /// `TriplePattern`s joined with any property-path sub-plans.
    fn build_bgp_plan(
        &self,
        patterns: &[TriplePattern],
        path_plans: Vec<RdfFusionLogicalPlanBuilder>,
    ) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        let mut plan: Option<RdfFusionLogicalPlanBuilder> = if !patterns.is_empty() {
            Some(self.builder_context.create_bgp(
                &self.state.borrow().active_graph,
                self.state.borrow().graph_name_var.as_ref(),
                patterns,
            )?)
        } else {
            None
        };

        for path_plan in path_plans {
            plan = Some(match plan {
                Some(p) => p.join(path_plan.build()?, SparqlJoinType::Inner, None)?,
                None => path_plan,
            });
        }

        Ok(plan.unwrap_or_else(|| self.builder_context.create_empty_solution()))
    }

    fn graph_node_path_to_term_pattern<'a>(
        &self,
        node: &ast::GraphNodePath<'a>,
        bgp_patterns: &mut Vec<TriplePattern>,
        path_plans: &mut Vec<RdfFusionLogicalPlanBuilder>,
    ) -> Result<TermPattern, SparqlParseError> {
        match node {
            ast::GraphNodePath::VarOrTerm(vot) => self.var_or_term_to_term_pattern(vot),
            ast::GraphNodePath::Collection(c) => {
                let elements = &c.value;
                if elements.is_empty() {
                    Ok(TermPattern::NamedNode(NamedNode::new_unchecked(
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil",
                    )))
                } else {
                    let mut first_bnode = self.next_bnode_term();
                    let return_bnode = first_bnode.clone();
                    for (i, element) in elements.iter().enumerate() {
                        let element_term = self.graph_node_path_to_term_pattern(
                            element,
                            bgp_patterns,
                            path_plans,
                        )?;
                        bgp_patterns.push(TriplePattern {
                            subject: first_bnode.clone(),
                            predicate: NamedNodePattern::NamedNode(
                                NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
                                ),
                            ),
                            object: element_term,
                        });

                        let next_bnode = if i == elements.len() - 1 {
                            TermPattern::NamedNode(NamedNode::new_unchecked(
                                "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil",
                            ))
                        } else {
                            self.next_bnode_term()
                        };

                        bgp_patterns.push(TriplePattern {
                            subject: first_bnode,
                            predicate: NamedNodePattern::NamedNode(
                                NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
                                ),
                            ),
                            object: next_bnode.clone(),
                        });
                        first_bnode = next_bnode;
                    }
                    Ok(return_bnode)
                }
            }
            ast::GraphNodePath::BlankNodePropertyList(b) => {
                let subject = self.next_bnode_term();
                for (verb, objects) in b.value.iter() {
                    for object in objects {
                        self.push_property_list_path_element(
                            &subject,
                            verb,
                            object,
                            bgp_patterns,
                            path_plans,
                        )?;
                    }
                }
                Ok(subject)
            }
        }
    }

    fn can_unroll_path(path: &ast::Path) -> bool {
        match path {
            ast::Path::Iri(_) | ast::Path::A => true,
            ast::Path::Inverse(inner) => Self::can_unroll_path(inner),
            ast::Path::Sequence(a, b) => {
                Self::can_unroll_path(a) && Self::can_unroll_path(b)
            }
            _ => false,
        }
    }

    fn unroll_path<'a>(
        &self,
        subject: &TermPattern,
        path: &ast::Path<'a>,
        object: &TermPattern,
        bgp_patterns: &mut Vec<TriplePattern>,
    ) -> Result<(), SparqlParseError> {
        match path {
            ast::Path::Iri(iri) => {
                let named_node = self.planner_context.resolve_iri(iri)?;
                bgp_patterns.push(TriplePattern {
                    subject: subject.clone(),
                    predicate: NamedNodePattern::NamedNode(named_node),
                    object: object.clone(),
                });
                Ok(())
            }
            ast::Path::A => {
                let rdf_type = NamedNode::new_unchecked(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                );
                bgp_patterns.push(TriplePattern {
                    subject: subject.clone(),
                    predicate: NamedNodePattern::NamedNode(rdf_type),
                    object: object.clone(),
                });
                Ok(())
            }
            ast::Path::Inverse(inner) => {
                self.unroll_path(object, inner, subject, bgp_patterns)
            }
            ast::Path::Sequence(a, b) => {
                let mid = self.next_bnode_term();
                self.unroll_path(subject, a, &mid, bgp_patterns)?;
                self.unroll_path(&mid, b, object, bgp_patterns)?;
                Ok(())
            }
            _ => unreachable!("path was checked with can_unroll_path"),
        }
    }

    fn push_property_list_path_element<'a>(
        &self,
        subject: &TermPattern,
        verb: &ast::VarOrPath<'a>,
        object: &ast::ObjectPath<'a>,
        bgp_patterns: &mut Vec<TriplePattern>,
        path_plans: &mut Vec<RdfFusionLogicalPlanBuilder>,
    ) -> Result<(), SparqlParseError> {
        let object_term = self.graph_node_path_to_term_pattern(
            &object.graph_node,
            bgp_patterns,
            path_plans,
        )?;

        match verb {
            ast::VarOrPath::Var(v) => {
                bgp_patterns.push(TriplePattern {
                    subject: subject.clone(),
                    predicate: NamedNodePattern::Variable(Variable::new_unchecked(
                        v.value,
                    )),
                    object: object_term,
                });
            }
            ast::VarOrPath::Path(p) => {
                if Self::can_unroll_path(p) {
                    self.unroll_path(subject, p, &object_term, bgp_patterns)?;
                } else {
                    let path_expr = self.path_to_property_path_expression(p)?;
                    let active_graph = self.state.borrow().active_graph.clone();
                    let graph_variable = self.state.borrow().graph_name_var.clone();
                    let plan = self.builder_context.create_property_path(
                        active_graph,
                        graph_variable,
                        path_expr,
                        subject.clone(),
                        object_term,
                    );
                    path_plans.push(plan);
                }
            }
        }
        Ok(())
    }

    fn path_to_property_path_expression<'a>(
        &self,
        path: &ast::Path<'a>,
    ) -> Result<PropertyPathExpression, SparqlParseError> {
        match path {
            ast::Path::Alternative(a, b) => Ok(PropertyPathExpression::Alternative(
                Box::new(self.path_to_property_path_expression(a)?),
                Box::new(self.path_to_property_path_expression(b)?),
            )),
            ast::Path::Sequence(a, b) => Ok(PropertyPathExpression::Sequence(
                Box::new(self.path_to_property_path_expression(a)?),
                Box::new(self.path_to_property_path_expression(b)?),
            )),
            ast::Path::Inverse(a) => Ok(PropertyPathExpression::Reverse(Box::new(
                self.path_to_property_path_expression(a)?,
            ))),
            ast::Path::ZeroOrOne(a) => Ok(PropertyPathExpression::ZeroOrOne(Box::new(
                self.path_to_property_path_expression(a)?,
            ))),
            ast::Path::ZeroOrMore(a) => Ok(PropertyPathExpression::ZeroOrMore(Box::new(
                self.path_to_property_path_expression(a)?,
            ))),
            ast::Path::OneOrMore(a) => Ok(PropertyPathExpression::OneOrMore(Box::new(
                self.path_to_property_path_expression(a)?,
            ))),
            ast::Path::Iri(iri) => Ok(PropertyPathExpression::NamedNode(
                self.planner_context.resolve_iri(iri)?,
            )),
            ast::Path::A => {
                Ok(PropertyPathExpression::NamedNode(NamedNode::new_unchecked(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                )))
            }
            ast::Path::NegatedPropertySet(set) => {
                let mut direct = Vec::new();
                let mut inverse = Vec::new();
                for item in set {
                    match item {
                        ast::PathOneInPropertySet::Iri(iri) => {
                            direct.push(self.planner_context.resolve_iri(iri)?);
                        }
                        ast::PathOneInPropertySet::A => {
                            direct.push(NamedNode::new_unchecked(
                                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                            ));
                        }
                        ast::PathOneInPropertySet::InverseIri(iri) => {
                            inverse.push(self.planner_context.resolve_iri(iri)?);
                        }
                        ast::PathOneInPropertySet::InverseA => {
                            inverse.push(NamedNode::new_unchecked(
                                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                            ));
                        }
                    }
                }
                if inverse.is_empty() {
                    Ok(PropertyPathExpression::NegatedPropertySet(direct))
                } else if direct.is_empty() {
                    Ok(PropertyPathExpression::Reverse(Box::new(
                        PropertyPathExpression::NegatedPropertySet(inverse),
                    )))
                } else {
                    Ok(PropertyPathExpression::Alternative(
                        Box::new(PropertyPathExpression::NegatedPropertySet(direct)),
                        Box::new(PropertyPathExpression::Reverse(Box::new(
                            PropertyPathExpression::NegatedPropertySet(inverse),
                        ))),
                    ))
                }
            }
        }
    }

    fn var_or_term_to_term_pattern(
        &self,
        vot: &ast::VarOrTerm,
    ) -> Result<TermPattern, SparqlParseError> {
        match vot {
            ast::VarOrTerm::Var(v) => {
                Ok(TermPattern::Variable(Variable::new_unchecked(v.value)))
            }
            ast::VarOrTerm::Iri(iri) => {
                let named_node = self.planner_context.resolve_iri(iri)?;
                Ok(TermPattern::NamedNode(named_node))
            }
            ast::VarOrTerm::Literal(lit) => {
                let common_literal = self.planner_context().map_literal(lit)?;
                Ok(TermPattern::Literal(common_literal))
            }
            ast::VarOrTerm::BlankNode(b) => {
                if let Some(label) = b.value.0 {
                    let bgp_id = self.state.borrow().current_bgp_id;
                    let scopes = &mut self.state.borrow_mut().bnode_scopes;
                    if let Some(&(prev_bgp_id, prev_span)) = scopes.get(label) {
                        if prev_bgp_id != bgp_id {
                            return Err(SparqlParseError::new_with_info(
                                b.span,
                                format!("Blank node label _:{label} used here"),
                                format!(
                                    "Blank node label _:{label} is used across different basic graph patterns"
                                ),
                                prev_span,
                                format!(
                                    "Blank node label _:{label} previously used here"
                                ),
                            ));
                        }
                    } else {
                        scopes.insert(label.to_string(), (bgp_id, b.span));
                    }
                    Ok(TermPattern::BlankNode(CommonBlankNode::new_unchecked(
                        label,
                    )))
                } else {
                    Ok(self.next_bnode_term())
                }
            }
            ast::VarOrTerm::Nil => Ok(TermPattern::NamedNode(NamedNode::new_unchecked(
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil",
            ))),
        }
    }

    fn graph_node_to_term_pattern<'a>(
        &self,
        node: &ast::GraphNode<'a>,
        triples: &mut Vec<TriplePattern>,
    ) -> Result<TermPattern, SparqlParseError> {
        match node {
            ast::GraphNode::VarOrTerm(vot) => self.var_or_term_to_term_pattern(vot),
            ast::GraphNode::Collection(c) => {
                let elements = &c.value;
                if elements.is_empty() {
                    Ok(TermPattern::NamedNode(NamedNode::new_unchecked(
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil",
                    )))
                } else {
                    let mut first_bnode = self.next_bnode_term();
                    let return_bnode = first_bnode.clone();
                    for (i, element) in elements.iter().enumerate() {
                        let element_term =
                            self.graph_node_to_term_pattern(element, triples)?;
                        triples.push(TriplePattern {
                            subject: first_bnode.clone(),
                            predicate: NamedNodePattern::NamedNode(
                                NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
                                ),
                            ),
                            object: element_term,
                        });

                        let next_bnode = if i == elements.len() - 1 {
                            TermPattern::NamedNode(NamedNode::new_unchecked(
                                "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil",
                            ))
                        } else {
                            self.next_bnode_term()
                        };

                        triples.push(TriplePattern {
                            subject: first_bnode,
                            predicate: NamedNodePattern::NamedNode(
                                NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
                                ),
                            ),
                            object: next_bnode.clone(),
                        });
                        first_bnode = next_bnode;
                    }
                    Ok(return_bnode)
                }
            }
            ast::GraphNode::BlankNodePropertyList(b) => {
                let subject = self.next_bnode_term();
                for (verb, objects) in b.value.iter() {
                    for object in objects {
                        let object_term =
                            self.graph_node_to_term_pattern(&object.graph_node, triples)?;
                        let predicate = match verb {
                            ast::Verb::Var(v) => NamedNodePattern::Variable(
                                Variable::new_unchecked(v.value),
                            ),
                            ast::Verb::Iri(iri) => NamedNodePattern::NamedNode(
                                self.planner_context.resolve_iri(iri)?,
                            ),
                            ast::Verb::A => {
                                NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                                ))
                            }
                        };
                        triples.push(TriplePattern {
                            subject: subject.clone(),
                            predicate,
                            object: object_term,
                        });
                    }
                }
                Ok(subject)
            }
        }
    }

    fn graph_node_to_term_pattern_quads<'a>(
        &self,
        node: &ast::GraphNode<'a>,
        quads: &mut Vec<QuadPattern>,
        graph_name: &GraphNamePattern,
    ) -> Result<TermPattern, SparqlParseError> {
        match node {
            ast::GraphNode::VarOrTerm(vot) => self.var_or_term_to_term_pattern(vot),
            ast::GraphNode::Collection(c) => {
                let elements = &c.value;
                if elements.is_empty() {
                    Ok(TermPattern::NamedNode(NamedNode::new_unchecked(
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil",
                    )))
                } else {
                    let mut first_bnode = self.next_bnode_term();
                    let return_bnode = first_bnode.clone();
                    for (i, element) in elements.iter().enumerate() {
                        let element_term = self.graph_node_to_term_pattern_quads(
                            element, quads, graph_name,
                        )?;
                        quads.push(QuadPattern {
                            subject: first_bnode.clone(),
                            predicate: NamedNodePattern::NamedNode(
                                NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
                                ),
                            ),
                            object: element_term,
                            graph_name: graph_name.clone(),
                        });

                        let next_bnode = if i == elements.len() - 1 {
                            TermPattern::NamedNode(NamedNode::new_unchecked(
                                "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil",
                            ))
                        } else {
                            self.next_bnode_term()
                        };

                        quads.push(QuadPattern {
                            subject: first_bnode,
                            predicate: NamedNodePattern::NamedNode(
                                NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
                                ),
                            ),
                            object: next_bnode.clone(),
                            graph_name: graph_name.clone(),
                        });
                        first_bnode = next_bnode;
                    }
                    Ok(return_bnode)
                }
            }
            ast::GraphNode::BlankNodePropertyList(b) => {
                let subject = self.next_bnode_term();
                for (verb, objects) in b.value.iter() {
                    for object in objects {
                        let object_term = self.graph_node_to_term_pattern_quads(
                            &object.graph_node,
                            quads,
                            graph_name,
                        )?;
                        let predicate = match verb {
                            ast::Verb::Var(v) => NamedNodePattern::Variable(
                                Variable::new_unchecked(v.value),
                            ),
                            ast::Verb::Iri(iri) => NamedNodePattern::NamedNode(
                                self.planner_context.resolve_iri(iri)?,
                            ),
                            ast::Verb::A => {
                                NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                                ))
                            }
                        };
                        quads.push(QuadPattern {
                            subject: subject.clone(),
                            predicate,
                            object: object_term,
                            graph_name: graph_name.clone(),
                        });
                    }
                }
                Ok(subject)
            }
        }
    }

    /// Rewrites a CONSTRUCT template (which has the same structure as a basic graph pattern)
    /// into a flat list of `TriplePattern`s.
    pub fn rewrite_construct_template(
        &self,
        triples: &[(ast::GraphNode<'_>, ast::PropertyList<'_>)],
    ) -> Result<Vec<TriplePattern>, SparqlParseError> {
        let mut patterns = Vec::new();
        for (subject, paths) in triples {
            let s = self.graph_node_to_term_pattern(subject, &mut patterns)?;
            for (verb, objects) in paths.iter() {
                let p = match verb {
                    ast::Verb::Var(v) => {
                        NamedNodePattern::Variable(Variable::new_unchecked(v.value))
                    }
                    ast::Verb::Iri(iri) => NamedNodePattern::NamedNode(
                        self.planner_context.resolve_iri(iri)?,
                    ),
                    ast::Verb::A => {
                        NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        ))
                    }
                };
                for object in objects {
                    let o = self
                        .graph_node_to_term_pattern(&object.graph_node, &mut patterns)?;
                    patterns.push(TriplePattern {
                        subject: s.clone(),
                        predicate: p.clone(),
                        object: o,
                    });
                }
            }
        }
        Ok(patterns)
    }

    /// Creates a BGP plan builder directly from a list of `TriplePattern`s.
    pub fn create_bgp_from_triples(
        &self,
        triples: &[TriplePattern],
    ) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        if triples.is_empty() {
            Ok(self.builder_context.create_empty_solution())
        } else {
            Ok(self.builder_context.create_bgp(
                &self.state.borrow().active_graph,
                self.state.borrow().graph_name_var.as_ref(),
                triples,
            )?)
        }
    }

    /// Rewrites quad patterns (e.g. from INSERT DATA).
    pub fn rewrite_quad_patterns(
        &self,
        quad_patterns: &ast::QuadPatterns,
    ) -> Result<Vec<QuadPattern>, SparqlParseError> {
        let mut patterns = Vec::new();
        for (graph_name, triples) in quad_patterns.iter() {
            let g = match graph_name {
                Some(ast::VarOrIri::Var(v)) => {
                    GraphNamePattern::Variable(Variable::new_unchecked(v.value))
                }
                Some(ast::VarOrIri::Iri(iri)) => {
                    let nn = self.planner_context.resolve_iri(iri)?;
                    GraphNamePattern::NamedNode(nn)
                }
                None => GraphNamePattern::DefaultGraph,
            };

            for (subject, paths) in triples.iter() {
                let s =
                    self.graph_node_to_term_pattern_quads(subject, &mut patterns, &g)?;
                for (verb, objects) in paths.iter() {
                    let p = match verb {
                        ast::Verb::Var(v) => {
                            NamedNodePattern::Variable(Variable::new_unchecked(v.value))
                        }
                        ast::Verb::Iri(iri) => NamedNodePattern::NamedNode(
                            self.planner_context.resolve_iri(iri)?,
                        ),
                        ast::Verb::A => {
                            NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                            ))
                        }
                    };
                    for object in objects {
                        let o = self.graph_node_to_term_pattern_quads(
                            &object.graph_node,
                            &mut patterns,
                            &g,
                        )?;
                        patterns.push(QuadPattern {
                            subject: s.clone(),
                            predicate: p.clone(),
                            object: o,
                            graph_name: g.clone(),
                        });
                    }
                }
            }
        }
        Ok(patterns)
    }

    pub fn rewrite_quad_patterns_as_logical_plan(
        &self,
        quad_patterns: &ast::QuadPatterns,
    ) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        let mut plan: Option<RdfFusionLogicalPlanBuilder> = None;
        for (graph_name, triples) in &quad_patterns.patterns {
            let mut state = self.state.borrow().clone();

            if let Some(name) = graph_name {
                let active_graph = match name {
                    ast::VarOrIri::Var(v) => {
                        state.graph_name_var = Some(Variable::new_unchecked(v.value));
                        match self.planner_context.dataset().available_named_graphs() {
                            None => ActiveGraph::AnyNamedGraph,
                            Some(graphs) => {
                                let names =
                                    graphs.iter().map(|g| g.clone().into()).collect();
                                ActiveGraph::Union(names)
                            }
                        }
                    }
                    ast::VarOrIri::Iri(iri) => {
                        state.graph_name_var = None;
                        let named_node = self.planner_context.resolve_iri(iri)?;
                        ActiveGraph::Union(vec![named_node.into()])
                    }
                };
                state = state.with_active_graph(active_graph);
            }

            let mut patterns = Vec::new();
            for (subject, paths) in triples.iter() {
                let s = self.graph_node_to_term_pattern(subject, &mut patterns)?;
                for (verb, objects) in paths.iter() {
                    let p = match verb {
                        ast::Verb::Var(v) => {
                            NamedNodePattern::Variable(Variable::new_unchecked(v.value))
                        }
                        ast::Verb::Iri(iri) => NamedNodePattern::NamedNode(
                            self.planner_context.resolve_iri(iri)?,
                        ),
                        ast::Verb::A => {
                            NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                            ))
                        }
                    };
                    for object in objects {
                        let o = self.graph_node_to_term_pattern(
                            &object.graph_node,
                            &mut patterns,
                        )?;
                        patterns.push(TriplePattern {
                            subject: s.clone(),
                            predicate: p.clone(),
                            object: o,
                        });
                    }
                }
            }

            let bgp_plan = self.builder_context.create_bgp(
                &state.active_graph,
                state.graph_name_var.as_ref(),
                &patterns,
            )?;

            if let Some(p) = plan {
                plan = Some(p.join(bgp_plan.build()?, SparqlJoinType::Inner, None)?);
            } else {
                plan = Some(bgp_plan);
            }
        }
        Ok(plan.unwrap_or_else(|| self.builder_context.create_empty_solution()))
    }
}

/// Accumulates basic graph patterns (BGPs) while rewriting the elements of a
/// [`ast::GraphPattern::Group`].
///
/// Consecutive triple blocks that are only separated by deferred FILTERs or BINDs are
/// accumulated into a single BGP, which is flushed into the running plan as soon as a
/// non-BGP element (OPTIONAL, MINUS, UNION, ...) is encountered.
struct BgpBuilder<'r> {
    rewriter: &'r GraphPatternRewriter,
    plan: Option<RdfFusionLogicalPlanBuilder>,
    patterns: Vec<TriplePattern>,
    path_plans: Vec<RdfFusionLogicalPlanBuilder>,
    bgp_open: bool,
}

impl<'r> BgpBuilder<'r> {
    fn new(rewriter: &'r GraphPatternRewriter) -> Self {
        BgpBuilder {
            rewriter,
            plan: None,
            patterns: Vec::new(),
            path_plans: Vec::new(),
            bgp_open: false,
        }
    }

    /// Opens a new basic graph pattern block, bumping the shared BGP id exactly once per
    /// contiguous block of BGP-starting elements.
    fn open_bgp(&mut self) {
        if !self.bgp_open {
            self.rewriter.state.borrow_mut().current_bgp_id += 1;
            self.bgp_open = true;
        }
    }

    fn close_bgp(&mut self) {
        self.bgp_open = false;
    }

    fn accumulate_triples<'a>(
        &mut self,
        triples: &[(ast::GraphNodePath<'a>, ast::PropertyListPath<'a>)],
    ) -> Result<(), SparqlParseError> {
        if triples.is_empty() {
            return Ok(());
        }
        self.rewriter.collect_bgp_patterns(
            triples,
            &mut self.patterns,
            &mut self.path_plans,
        )?;
        Ok(())
    }

    /// Joins any accumulated basic graph pattern into the running plan.
    fn flush(&mut self) -> Result<(), SparqlParseError> {
        if self.patterns.is_empty() && self.path_plans.is_empty() {
            return Ok(());
        }
        let bgp = self.rewriter.build_bgp_plan(
            &std::mem::take(&mut self.patterns),
            std::mem::take(&mut self.path_plans),
        )?;
        self.plan = Some(match self.plan.take() {
            Some(p) => p.join(bgp.build()?, SparqlJoinType::Inner, None)?,
            None => bgp,
        });
        Ok(())
    }

    fn into_plan(mut self) -> Result<RdfFusionLogicalPlanBuilder, SparqlParseError> {
        self.flush()?;
        Ok(self
            .plan
            .unwrap_or_else(|| self.rewriter.builder_context.create_empty_solution()))
    }

    fn with_optional<'a>(
        &mut self,
        right_plan: RdfFusionLogicalPlanBuilder,
        filters: Vec<&'a ast::Expression<'a>>,
    ) -> Result<(), SparqlParseError> {
        self.flush()?;
        let p = self
            .plan
            .take()
            .unwrap_or_else(|| self.rewriter.builder_context.create_empty_solution());
        let join_condition =
            self.rewriter
                .build_optional_filter(&p, &right_plan, filters)?;
        self.plan =
            Some(p.join(right_plan.build()?, SparqlJoinType::Left, join_condition)?);
        Ok(())
    }

    fn with_minus(
        &mut self,
        right_plan: RdfFusionLogicalPlanBuilder,
    ) -> Result<(), SparqlParseError> {
        self.flush()?;
        let p = self
            .plan
            .take()
            .unwrap_or_else(|| self.rewriter.builder_context.create_empty_solution());
        self.plan = Some(p.minus(right_plan.build()?)?);
        Ok(())
    }

    fn with_bind<'a>(
        &mut self,
        expr: &'a ast::Expression<'a>,
        var: &'a ast::Var<'a>,
    ) -> Result<(), SparqlParseError> {
        self.flush()?;
        let p = self
            .plan
            .take()
            .unwrap_or_else(|| self.rewriter.builder_context.create_empty_solution());
        let schema = Arc::clone(p.decoded_schema());
        let expr_ctx = self
            .rewriter
            .builder_context
            .expr_builder_context_with_schema(&schema);
        let expr_rewriter = ExpressionRewriter::new(self.rewriter, expr_ctx);
        let df_expr = expr_rewriter.rewrite_scalar_expr(expr)?;
        self.plan = Some(p.extend(Variable::new_unchecked(var.value), df_expr)?);
        Ok(())
    }

    fn with_element(
        &mut self,
        other_plan: RdfFusionLogicalPlanBuilder,
    ) -> Result<(), SparqlParseError> {
        self.flush()?;
        self.plan = Some(match self.plan.take() {
            Some(p) => p.join(other_plan.build()?, SparqlJoinType::Inner, None)?,
            None => other_plan,
        });
        Ok(())
    }
}

/// The single mutable state record owned by a [`GraphPatternRewriter`].
///
/// It combines the active-graph context, which is threaded downwards into nested rewrites,
/// with the bookkeeping needed to validate labeled blank nodes across basic graph patterns.
#[derive(Clone, Default)]
struct RewriteState {
    /// The active graph against which the current pattern is evaluated.
    active_graph: ActiveGraph,
    /// The variable holding the current graph name, if any.
    graph_name_var: Option<Variable>,
    /// Tracks the basic graph pattern in which each labeled blank node was first seen,
    /// used to reject labels that are reused across different basic graph patterns.
    bnode_scopes: std::collections::HashMap<String, (usize, Span)>,
    /// Monotonically increasing id of the current basic graph pattern.
    current_bgp_id: usize,
}

impl RewriteState {
    /// Creates a fresh state evaluated against `active_graph`, with no pending blank-node
    /// or basic-graph-pattern bookkeeping.
    fn new(active_graph: ActiveGraph) -> Self {
        RewriteState {
            active_graph,
            ..Default::default()
        }
    }

    /// Returns a copy of this state narrowed to `active_graph` (clearing the graph-name
    /// variable, as the graph name changes together with the active graph).
    fn with_active_graph(&self, active_graph: ActiveGraph) -> RewriteState {
        RewriteState {
            active_graph,
            graph_name_var: None,
            ..self.clone()
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

/// A unified traversal over the SPARQL graph-pattern AST.
pub(crate) trait GraphPatternVisitor<'a> {
    /// Visits a graph pattern (a group or sub-select), descending into all its elements.
    fn visit_graph_pattern(&mut self, pattern: &'a ast::GraphPattern<'a>) {
        walk_graph_pattern(self, pattern);
    }

    /// Visits a sub-select, descending into its `WHERE` clause.
    fn visit_subselect(&mut self, sub: &'a ast::SubSelect<'a>) {
        walk_graph_pattern(self, &sub.where_clause);
    }

    /// Visits a graph pattern element, descending into its children.
    fn visit_element(&mut self, element: &'a ast::GraphPatternElement<'a>) {
        walk_element(self, element);
    }

    /// Visits an expression, descending into its operands.
    fn visit_expression(&mut self, expr: &'a ast::Expression<'a>) {
        walk_expression(self, expr);
    }

    /// Visits the arguments of an aggregate.
    fn visit_aggregate(&mut self, agg: &'a ast::Aggregate<'a>) {
        for arg in &agg.args {
            self.visit_function_arg(arg);
        }
    }

    /// Visits the arguments of a function call.
    fn visit_function(&mut self, func: &'a ast::Function<'a>) {
        for arg in &func.args {
            self.visit_function_arg(arg);
        }
    }

    /// Visits a function argument.
    fn visit_function_arg(&mut self, arg: &'a ast::FunctionArg<'a>) {
        match arg {
            ast::FunctionArg::Positional(expr) | ast::FunctionArg::Named(_, expr) => {
                self.visit_expression(&expr.value)
            }
        }
    }

    /// Visits a variable-or-IRI term, descending into it if it is a variable.
    fn visit_var_or_iri(&mut self, term: &'a ast::VarOrIri<'a>) {
        if let ast::VarOrIri::Var(v) = term {
            self.visit_var(v);
        }
    }

    /// Visits a verb-or-path term, descending into it if it is a variable.
    fn visit_var_or_path(&mut self, term: &'a ast::VarOrPath<'a>) {
        if let ast::VarOrPath::Var(v) = term {
            self.visit_var(v);
        }
    }

    /// Visits a variable-or-term, descending into it if it is a variable.
    fn visit_var_or_term(&mut self, term: &'a ast::VarOrTerm<'a>) {
        if let ast::VarOrTerm::Var(v) = term {
            self.visit_var(v);
        }
    }

    /// Visits a graph node path (subject/object position), descending into any variables.
    fn visit_graph_node_path(&mut self, node: &'a ast::GraphNodePath<'a>) {
        walk_graph_node_path(self, node);
    }

    /// Leaf hook invoked for every variable occurrence. Overridden by concrete visitors.
    fn visit_var(&mut self, _var: &'a ast::Var<'a>) {}
}

/// Default structural traversal of a graph pattern.
fn walk_graph_pattern<'a, V: GraphPatternVisitor<'a> + ?Sized>(
    visitor: &mut V,
    pattern: &'a ast::GraphPattern<'a>,
) {
    match pattern {
        ast::GraphPattern::Group(elements) => {
            for element in elements {
                visitor.visit_element(&element.value);
            }
        }
        ast::GraphPattern::SubSelect(sub) => visitor.visit_subselect(sub),
    }
}

/// Default structural traversal of a graph pattern element.
fn walk_element<'a, V: GraphPatternVisitor<'a> + ?Sized>(
    visitor: &mut V,
    element: &'a ast::GraphPatternElement<'a>,
) {
    match element {
        ast::GraphPatternElement::Filter(expr) => visitor.visit_expression(&expr.value),
        ast::GraphPatternElement::Triples(triples) => {
            for (subject, property_list) in triples {
                visitor.visit_graph_node_path(subject);
                for (verb, objects) in property_list.iter() {
                    visitor.visit_var_or_path(verb);
                    for object in objects {
                        visitor.visit_graph_node_path(&object.graph_node);
                    }
                }
            }
        }
        ast::GraphPatternElement::Union(patterns) => {
            for pattern in patterns {
                visitor.visit_graph_pattern(pattern);
            }
        }
        ast::GraphPatternElement::Minus(pattern) => visitor.visit_graph_pattern(pattern),
        ast::GraphPatternElement::Values(values) => {
            for variable in &values.variables {
                visitor.visit_var(variable);
            }
        }
        ast::GraphPatternElement::Bind(expr, variable) => {
            visitor.visit_var(variable);
            visitor.visit_expression(&expr.value);
        }
        ast::GraphPatternElement::Service { name, pattern, .. } => {
            visitor.visit_var_or_iri(name);
            visitor.visit_graph_pattern(pattern);
        }
        ast::GraphPatternElement::Graph { name, pattern } => {
            visitor.visit_var_or_iri(name);
            visitor.visit_graph_pattern(pattern);
        }
        ast::GraphPatternElement::Optional(pattern) => {
            visitor.visit_graph_pattern(pattern)
        }
    }
}

/// Default structural traversal of an expression.
fn walk_expression<'a, V: GraphPatternVisitor<'a> + ?Sized>(
    visitor: &mut V,
    expr: &'a ast::Expression<'a>,
) {
    match expr {
        ast::Expression::Or(a, b)
        | ast::Expression::And(a, b)
        | ast::Expression::Equal(a, b)
        | ast::Expression::NotEqual(a, b)
        | ast::Expression::Less(a, b)
        | ast::Expression::LessOrEqual(a, b)
        | ast::Expression::Greater(a, b)
        | ast::Expression::GreaterOrEqual(a, b)
        | ast::Expression::Add(a, b)
        | ast::Expression::Subtract(a, b)
        | ast::Expression::Multiply(a, b)
        | ast::Expression::Divide(a, b) => {
            visitor.visit_expression(&a.value);
            visitor.visit_expression(&b.value);
        }
        ast::Expression::In(a, rest) | ast::Expression::NotIn(a, rest) => {
            visitor.visit_expression(&a.value);
            for expr in rest {
                visitor.visit_expression(&expr.value);
            }
        }
        ast::Expression::UnaryPlus(a)
        | ast::Expression::UnaryMinus(a)
        | ast::Expression::Not(a) => {
            visitor.visit_expression(&a.value);
        }
        ast::Expression::Aggregate(agg) => visitor.visit_aggregate(agg),
        ast::Expression::Function(func) => visitor.visit_function(func),
        ast::Expression::Var(v) => visitor.visit_var(v),
        ast::Expression::Iri(_) | ast::Expression::Literal(_) => {}
        ast::Expression::Exists(pattern) | ast::Expression::NotExists(pattern) => {
            visitor.visit_graph_pattern(pattern)
        }
    }
}

/// Default structural traversal of a graph node path.
fn walk_graph_node_path<'a, V: GraphPatternVisitor<'a> + ?Sized>(
    visitor: &mut V,
    node: &'a ast::GraphNodePath<'a>,
) {
    match node {
        ast::GraphNodePath::VarOrTerm(vot) => visitor.visit_var_or_term(vot),
        ast::GraphNodePath::Collection(items) => {
            for item in &items.value {
                visitor.visit_graph_node_path(item);
            }
        }
        ast::GraphNodePath::BlankNodePropertyList(pl) => {
            for (verb, objects) in pl.value.iter() {
                visitor.visit_var_or_path(verb);
                for object in objects {
                    visitor.visit_graph_node_path(&object.graph_node);
                }
            }
        }
    }
}

/// Collects the names and spans of every variable occurring in an element or pattern.
struct VariableCollection<'a, 'm> {
    map: &'m mut std::collections::HashMap<&'a str, Span>,
}

impl<'a, 'm> GraphPatternVisitor<'a> for VariableCollection<'a, 'm> {
    fn visit_var(&mut self, var: &'a ast::Var<'a>) {
        self.map.entry(var.value).or_insert(var.span);
    }
}

/// Collects the in-scope variable names of a pattern according to SPARQL 1.1 Section 18.2.1.
struct InScopeCollection<'a, 'm> {
    set: &'m mut HashSet<&'a str>,
}

impl<'a, 'm> GraphPatternVisitor<'a> for InScopeCollection<'a, 'm> {
    fn visit_element(&mut self, element: &'a ast::GraphPatternElement<'a>) {
        // FILTER expressions contribute no in-scope variables (handled via the
        // expression no-op below) and MINUS pattern variables do not enter the outer scope.
        match element {
            ast::GraphPatternElement::Minus(_) => {}
            _ => walk_element(self, element),
        }
    }

    fn visit_subselect(&mut self, sub: &'a ast::SubSelect<'a>) {
        match &sub.select_clause.bindings.value {
            ast::SelectVariables::Explicit(vars) => {
                for spanned_var in vars {
                    self.set.insert(spanned_var.value.variable.value);
                }
            }
            ast::SelectVariables::Star => {
                walk_graph_pattern(self, &sub.where_clause);
                if let Some(values) = sub.values_clause.as_ref() {
                    for variable in &values.variables {
                        self.set.insert(variable.value);
                    }
                }
            }
        }
    }

    fn visit_expression(&mut self, _expr: &'a ast::Expression<'a>) {}

    fn visit_var(&mut self, var: &'a ast::Var<'a>) {
        self.set.insert(var.value);
    }
}

/// Collects the names and spans of all variables in an expression, excluding those inside
/// aggregates (including IRIs that resolve to registered aggregate functions).
struct NonAggregateExprCollection<'a, 'm, 'r> {
    rewriter: &'r GraphPatternRewriter,
    map: &'m mut std::collections::HashMap<&'a str, Span>,
}

impl<'a, 'm, 'r> GraphPatternVisitor<'a> for NonAggregateExprCollection<'a, 'm, 'r> {
    fn visit_aggregate(&mut self, _agg: &'a ast::Aggregate<'a>) {}

    fn visit_function(&mut self, func: &'a ast::Function<'a>) {
        if let ast::FunctionName::Iri(iri) = &func.name {
            if let Ok(named_node) = self.rewriter.planner_context.resolve_iri(iri) {
                let fn_name =
                    rdf_fusion_extensions::functions::FunctionName::Custom(named_node);
                if self
                    .rewriter
                    .builder_context
                    .registry()
                    .udaf(&fn_name)
                    .is_ok()
                {
                    return;
                }
            }
        }
        for arg in &func.args {
            self.visit_function_arg(arg);
        }
    }

    fn visit_var(&mut self, var: &'a ast::Var<'a>) {
        self.map.entry(var.value).or_insert(var.span);
    }
}

/// Collects the names and spans of all variables occurring in a graph pattern element.
fn collect_variables<'a>(
    element: &'a ast::GraphPatternElement<'a>,
    map: &mut std::collections::HashMap<&'a str, Span>,
) {
    let mut visitor = VariableCollection { map };
    visitor.visit_element(element);
}

/// Collects all in-scope variables of a graph pattern according to SPARQL 1.1 Section 18.2.1.
pub fn collect_in_scope_variables<'a>(
    pattern: &'a ast::GraphPattern<'a>,
    values_clause: Option<&'a ast::ValuesClause<'a>>,
    set: &mut HashSet<&'a str>,
) {
    let mut visitor = InScopeCollection { set };
    visitor.visit_graph_pattern(pattern);
    if let Some(values) = values_clause {
        for variable in &values.variables {
            visitor.set.insert(variable.value);
        }
    }
}

/// Collects the names and spans of all variables occurring in an expression, excluding those
/// inside aggregates.
fn collect_non_aggregate_expr_variables<'a>(
    rewriter: &GraphPatternRewriter,
    expr: &'a ast::Expression<'a>,
    map: &mut std::collections::HashMap<&'a str, Span>,
) {
    let mut visitor = NonAggregateExprCollection { rewriter, map };
    visitor.visit_expression(expr);
}
