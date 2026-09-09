use crate::ast;
use crate::error::SparqlParseError;
use crate::options::ParserOptions;
use crate::rewriter::{
    GraphPatternRewriter, collect_in_scope_variables, create_rewriter_context,
};
use datafusion_common::Column;
use rdf_fusion_common::Variable;
use rdf_fusion_common::sparql::{QueryVariant, RdfFusionQuery, TriplePattern};
use rdf_fusion_encoding::EncodingName;
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;
use rdf_fusion_logical::join::SparqlJoinType;
use std::sync::Arc;

/// A dedicated rewriter to transform SPARQL Update ASTs into Update Algebra operations.
pub struct QueryRewriter {
    context_view: RdfFusionContextView,
}

impl QueryRewriter {
    pub fn new(context_view: RdfFusionContextView) -> Self {
        Self { context_view }
    }

    /// Rewrites a full SPARQL Update AST into an `RdfFusionUpdate`.
    pub fn rewrite(
        &self,
        ast: &ast::Query,
        config: &ParserOptions,
    ) -> Result<RdfFusionQuery, SparqlParseError> {
        let builder_context =
            RdfFusionLogicalPlanBuilderContext::new(self.context_view.clone());
        let rewriter_context = create_rewriter_context(ast, config)?;
        let rewriter = GraphPatternRewriter::new(builder_context, rewriter_context);

        let create_logical_plan =
            |plan_result: Result<_, _>,
             modifier: &ast::SolutionModifier,
             select: Option<&ast::SelectClause>,
             where_pattern: Option<&ast::GraphPattern>,
             values: Option<&ast::ValuesClause>| {
                plan_result
                    .and_then(|plan| {
                        rewriter.apply_modifiers_and_select(
                            plan,
                            modifier,
                            select,
                            where_pattern,
                            values,
                        )
                    })
                    .and_then(|plan| {
                        let encoding = config
                            .output_encoding_name()
                            .unwrap_or(EncodingName::PlainTerm);
                        Ok(plan.with_encoding(encoding)?)
                    })
                    .and_then(|plan| Ok(plan.build()?))
                    .map_err(|e| match e {
                        SparqlParseError::Syntax(_) | SparqlParseError::Validation(_) => {
                            e
                        }
                        other => SparqlParseError::new_without_span(format!(
                            "Planning error: {other}"
                        )),
                    })
            };

        match &ast.variant {
            ast::QueryQuery::Select(s) => {
                let plan = create_logical_plan(
                    rewriter.rewrite_graph_pattern(&s.where_clause),
                    &s.solution_modifier,
                    Some(&s.select_clause),
                    Some(&s.where_clause),
                    ast.values_clause.as_ref(),
                )?;
                Ok(RdfFusionQuery::new(plan, QueryVariant::Select))
            }
            ast::QueryQuery::Construct(s) => {
                let template_triples = rewriter
                    .rewrite_construct_template(&s.template.value)
                    .map_err(|e| match e {
                        SparqlParseError::Syntax(_) => e,
                        other => SparqlParseError::new_without_span(format!(
                            "Planning error: {other}"
                        )),
                    })?;

                let plan_result = match &s.where_clause {
                    Some(where_clause) => rewriter.rewrite_graph_pattern(where_clause),
                    None => rewriter.create_bgp_from_triples(&template_triples),
                };

                let plan = create_logical_plan(
                    plan_result,
                    &s.solution_modifier,
                    None,
                    s.where_clause.as_ref(),
                    ast.values_clause.as_ref(),
                )?;

                Ok(RdfFusionQuery::new(
                    plan,
                    QueryVariant::Construct {
                        template: template_triples,
                    },
                ))
            }
            ast::QueryQuery::Ask(s) => {
                let plan = create_logical_plan(
                    rewriter.rewrite_graph_pattern(&s.where_clause),
                    &s.solution_modifier,
                    None,
                    Some(&s.where_clause),
                    ast.values_clause.as_ref(),
                )?;
                Ok(RdfFusionQuery::new(plan, QueryVariant::Ask))
            }
            ast::QueryQuery::Describe(s) => {
                let mut plan = match &s.where_clause {
                    Some(where_clause) => rewriter.rewrite_graph_pattern(where_clause)?,
                    None => rewriter.builder_context().create_empty_solution(),
                };

                let mut describe_vars = Vec::new();
                match &s.targets.value {
                    ast::DescribeTargets::Star => {
                        let mut in_scope_vars = std::collections::HashSet::new();
                        if let Some(where_clause) = &s.where_clause {
                            collect_in_scope_variables(
                                where_clause,
                                ast.values_clause.as_ref(),
                                &mut in_scope_vars,
                            );
                        }
                        for field in plan.decoded_schema().fields() {
                            if in_scope_vars.contains(field.name().as_str()) {
                                describe_vars.push(Variable::new_unchecked(field.name()));
                            }
                        }
                    }
                    ast::DescribeTargets::Explicit(targets) => {
                        let mut target_vars = Vec::new();
                        for target in targets {
                            match target {
                                ast::VarOrIri::Var(v) => {
                                    let var = Variable::new_unchecked(v.value);
                                    if !plan.schema().has_column(
                                        &Column::new_unqualified(var.as_str()),
                                    ) {
                                        let schema = Arc::clone(plan.decoded_schema());
                                        let expr_builder_context = rewriter
                                            .builder_context()
                                            .expr_builder_context_with_schema(&schema);
                                        let unbound_expr = expr_builder_context
                                            .variable(rdf_fusion_common::VariableRef::new_unchecked(
                                                var.as_str(),
                                            ))
                                            .map_err(|e| {
                                                SparqlParseError::new_without_span(e.to_string())
                                            })?
                                            .build()
                                            .map_err(|e| {
                                                SparqlParseError::new_without_span(e.to_string())
                                            })?;
                                        plan = plan
                                            .extend(var.clone(), unbound_expr)
                                            .map_err(|e| {
                                                SparqlParseError::new_without_span(
                                                    e.to_string(),
                                                )
                                            })?;
                                    }
                                    target_vars.push(var.clone());
                                    describe_vars.push(var);
                                }
                                ast::VarOrIri::Iri(iri) => {
                                    let named_node =
                                        rewriter.planner_context().resolve_iri(iri)?;
                                    let fresh_var = rewriter.next_guid_var();
                                    let schema = Arc::clone(plan.decoded_schema());
                                    let expr_builder_context = rewriter
                                        .builder_context()
                                        .expr_builder_context_with_schema(&schema);
                                    let iri_expr = expr_builder_context
                                        .literal(&named_node)
                                        .map_err(|e| {
                                            SparqlParseError::new_without_span(
                                                e.to_string(),
                                            )
                                        })?
                                        .build()
                                        .map_err(|e| {
                                            SparqlParseError::new_without_span(
                                                e.to_string(),
                                            )
                                        })?;
                                    plan = plan
                                        .extend(fresh_var.clone(), iri_expr)
                                        .map_err(|e| {
                                            SparqlParseError::new_without_span(
                                                e.to_string(),
                                            )
                                        })?;
                                    target_vars.push(fresh_var.clone());
                                    describe_vars.push(fresh_var);
                                }
                            }
                        }
                        plan = plan.project(&target_vars).map_err(|e| {
                            SparqlParseError::new_without_span(e.to_string())
                        })?;
                    }
                }

                plan = rewriter.apply_modifiers_and_select(
                    plan,
                    &s.solution_modifier,
                    None,
                    s.where_clause.as_ref(),
                    ast.values_clause.as_ref(),
                )?;

                let mut describe_template = Vec::new();
                for var in &describe_vars {
                    let type_var =
                        Variable::new_unchecked(format!("{}__type", var.as_str()));
                    describe_template.push(TriplePattern {
                        subject: var.clone().into(),
                        predicate: rdf_fusion_common::vocab::rdf::TYPE
                            .into_owned()
                            .into(),
                        object: type_var.into(),
                    });
                }

                if !describe_template.is_empty() {
                    let bgp_plan =
                        rewriter.create_bgp_from_triples(&describe_template)?;
                    plan = plan
                        .join(
                            bgp_plan.build().map_err(|e| {
                                SparqlParseError::new_without_span(e.to_string())
                            })?,
                            SparqlJoinType::Inner,
                            None,
                        )
                        .map_err(|e| SparqlParseError::new_without_span(e.to_string()))?;
                }

                let encoding = config
                    .output_encoding_name()
                    .unwrap_or(EncodingName::PlainTerm);
                let plan = plan
                    .with_encoding(encoding)
                    .map_err(|e| SparqlParseError::new_without_span(e.to_string()))?
                    .build()
                    .map_err(|e| SparqlParseError::new_without_span(e.to_string()))?;

                Ok(RdfFusionQuery::new(
                    plan,
                    QueryVariant::Describe {
                        template: describe_template,
                    },
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SparqlParser;
    use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
    use rdf_fusion_encoding::string::STRING_ENCODING;
    use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
    use rdf_fusion_encoding::{QuadStorageEncoding, RdfFusionEncodings};
    use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
    use std::sync::Arc;

    fn test_context_view() -> RdfFusionContextView {
        let encodings = RdfFusionEncodings::new(
            Arc::clone(&PLAIN_TERM_ENCODING),
            Arc::new(TypedFamilyEncoding::default()),
            None,
            Arc::clone(&STRING_ENCODING),
        );
        let registry = Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone()));
        RdfFusionContextView::new(registry, encodings, QuadStorageEncoding::PlainTerm)
    }

    #[test]
    fn test_select_star_excludes_property_path_intermediate_blank_nodes() {
        let ctx = test_context_view();
        let query_str = "
            PREFIX ex: <http://www.example.org/schema#>
            PREFIX in: <http://www.example.org/instance#>
            SELECT * WHERE { in:c ^(ex:p1/ex:p2) ?x }
        ";
        let ast = SparqlParser::new(query_str, Arc::clone(ctx.functions()))
            .parse_query()
            .unwrap();
        let query = QueryRewriter::new(ctx)
            .rewrite(&ast, &ParserOptions::default())
            .unwrap();
        let fields = query.logical_plan().schema().fields();
        let var_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(var_names, vec!["x"]);
    }

    #[test]
    fn test_select_star_excludes_explicit_and_anonymous_blank_nodes() {
        let ctx = test_context_view();
        let query_str = "
            PREFIX ex: <http://www.example.org/>
            SELECT * WHERE { ?s ex:p _:b1 . [ ex:q ?o ] }
        ";
        let ast = SparqlParser::new(query_str, Arc::clone(ctx.functions()))
            .parse_query()
            .unwrap();
        let query = QueryRewriter::new(ctx)
            .rewrite(&ast, &ParserOptions::default())
            .unwrap();
        let fields = query.logical_plan().schema().fields();
        let var_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(var_names, vec!["s", "o"]);
    }

    #[test]
    fn test_xsd_cast_resolves_via_registry() {
        let ctx = test_context_view();
        let query_str = "
            PREFIX ex: <http://www.example.org/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            SELECT (xsd:string(?x) AS ?s) WHERE { ?x ex:p 1 }
        ";
        let ast = SparqlParser::new(query_str, Arc::clone(ctx.functions()))
            .parse_query()
            .unwrap();
        let query = QueryRewriter::new(ctx)
            .rewrite(&ast, &ParserOptions::default())
            .unwrap();
        let plan_str = format!("{}", query.logical_plan());
        assert!(
            plan_str.contains("xsd:string"),
            "expected a dynamically resolved xsd:string cast, got: {plan_str}"
        );
    }

    #[test]
    fn test_builtin_resolves_via_try_from() {
        let ctx = test_context_view();
        let query_str = "
            PREFIX ex: <http://www.example.org/>
            SELECT (CONCAT(?a, ?b) AS ?c)
            WHERE { ?x ex:p ?a . ?x ex:q ?b }
        ";
        let ast = SparqlParser::new(query_str, Arc::clone(ctx.functions()))
            .parse_query()
            .unwrap();
        let query = QueryRewriter::new(ctx)
            .rewrite(&ast, &ParserOptions::default())
            .unwrap();
        let plan_str = format!("{}", query.logical_plan());
        assert!(
            plan_str.contains("CONCAT"),
            "expected a resolved CONCAT builtin, got: {plan_str}"
        );
    }

    #[test]
    fn test_aggregate_resolves_via_try_from() {
        let ctx = test_context_view();
        let query_str = "
            PREFIX ex: <http://www.example.org/>
            SELECT (SUM(?o) AS ?total) WHERE { ?x ex:p ?o }
        ";
        let ast = SparqlParser::new(query_str, Arc::clone(ctx.functions()))
            .parse_query()
            .unwrap();
        let query = QueryRewriter::new(ctx)
            .rewrite(&ast, &ParserOptions::default())
            .unwrap();
        let plan_str = format!("{}", query.logical_plan());
        assert!(
            plan_str.contains("SUM"),
            "expected a resolved SUM aggregate, got: {plan_str}"
        );
    }

    #[test]
    fn reject_blank_node_reuse_across_bgps_seq() {
        let ctx = test_context_view();
        let query_str = "SELECT * WHERE { { _:a ?p ?v . } _:a ?q 1 }";
        let ast = SparqlParser::new(query_str, Arc::clone(ctx.functions()))
            .parse_query()
            .unwrap();
        let err = QueryRewriter::new(ctx)
            .rewrite(&ast, &ParserOptions::default())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Blank node label _:a used here"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn reject_blank_node_reuse_across_bgps_union() {
        let ctx = test_context_view();
        let query_str = "SELECT * WHERE { { _:a ?p ?v . } UNION { _:a ?q 1 } }";
        let ast = SparqlParser::new(query_str, Arc::clone(ctx.functions()))
            .parse_query()
            .unwrap();
        let err = QueryRewriter::new(ctx)
            .rewrite(&ast, &ParserOptions::default())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Blank node label _:a used here"),
            "unexpected: {err}"
        );
    }
}
