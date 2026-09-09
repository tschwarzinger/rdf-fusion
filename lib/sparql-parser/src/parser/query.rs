use crate::Diagnostic;
use crate::ast::{
    AskQuery, ConstructQuery, DescribeQuery, DescribeTargets, Expression, GraphClause,
    GraphPattern, LimitOffsetClauses, OrderCondition, Query, QueryQuery, SelectClause,
    SelectQuery, SelectVariable, SelectVariables, SelectionOption, SolutionModifier,
    SubSelect,
};
use crate::error::SparqlSyntaxError;
use crate::lexer::Token;
use crate::parser::SparqlParser;
use crate::span::{Span, Spanned};

impl<'a> SparqlParser<'a> {
    /// Parses a `Query`, the top-level entry point for SPARQL queries.
    ///
    /// ```text
    /// Query ::= Prologue ( SelectQuery | ConstructQuery | DescribeQuery | AskQuery )
    ///           ValuesClause
    /// ```
    pub fn parse_query(&mut self) -> Result<Query<'a>, SparqlSyntaxError> {
        let prologue = self.parse_prologue()?;

        let variant = if self.peek_keyword("SELECT") {
            QueryQuery::Select(self.parse_select_query()?)
        } else if self.peek_keyword("CONSTRUCT") {
            QueryQuery::Construct(self.parse_construct_query()?)
        } else if self.peek_keyword("DESCRIBE") {
            QueryQuery::Describe(self.parse_describe_query()?)
        } else if self.peek_keyword("ASK") {
            QueryQuery::Ask(self.parse_ask_query()?)
        } else {
            self.expected("a query (SELECT, CONSTRUCT, DESCRIBE, or ASK)")?
        };

        let values_clause = if self.parse_keyword("VALUES") {
            Some(self.parse_values_clause()?)
        } else {
            None
        };

        if !self.at_end() {
            return self.expected("end of input");
        }

        Ok(Query {
            prologue,
            variant,
            values_clause,
        })
    }

    /// Parses a `SelectQuery`.
    ///
    /// ```text
    /// SelectQuery ::= SelectClause DatasetClause* WhereClause SolutionModifier
    /// ```
    fn parse_select_query(&mut self) -> Result<SelectQuery<'a>, SparqlSyntaxError> {
        let select_clause = self.parse_select_clause()?;
        let dataset_clause = self.parse_dataset_clauses()?;
        let where_clause = self.parse_where_clause()?;
        let solution_modifier = self.parse_solution_modifier()?;
        Ok(SelectQuery {
            select_clause,
            dataset_clause,
            where_clause,
            solution_modifier,
        })
    }

    /// Parses a `SubSelect` (used inside a `GroupGraphPattern`).
    ///
    /// ```text
    /// SubSelect ::= SelectClause WhereClause SolutionModifier ValuesClause
    /// ```
    pub(crate) fn parse_sub_select_clause(
        &mut self,
    ) -> Result<SubSelect<'a>, SparqlSyntaxError> {
        let select_clause = self.parse_select_clause()?;
        let where_clause = self.parse_where_clause()?;
        let solution_modifier = self.parse_solution_modifier()?;
        let values_clause = if self.parse_keyword("VALUES") {
            Some(self.parse_values_clause()?)
        } else {
            None
        };
        Ok(SubSelect {
            select_clause,
            where_clause,
            solution_modifier,
            values_clause,
        })
    }

    /// Parses a `SelectClause`.
    ///
    /// ```text
    /// SelectClause ::= 'SELECT' ( 'DISTINCT' | 'REDUCED' )?
    ///                  ( ( Var | ( '(' Expression 'AS' Var ')' ) )+ | '*' )
    /// ```
    fn parse_select_clause(&mut self) -> Result<SelectClause<'a>, SparqlSyntaxError> {
        self.expect_keyword("SELECT")?;
        let option = if self.parse_keyword("DISTINCT") {
            SelectionOption::Distinct
        } else if self.parse_keyword("REDUCED") {
            SelectionOption::Reduced
        } else {
            SelectionOption::Default
        };

        let start = self.current_span().start;
        let bindings = if self.peek_operator("*") {
            let star_span = self.bump().unwrap().span;
            Spanned {
                value: SelectVariables::Star,
                span: star_span,
            }
        } else {
            let mut vars = Vec::new();
            loop {
                if self.peek().is_some_and(|t| t.value.is_var()) {
                    let var = self.parse_var()?;
                    let span = var.span;
                    vars.push(Spanned {
                        value: SelectVariable {
                            expression: None,
                            variable: var,
                        },
                        span,
                    });
                } else if self.peek_operator("(") {
                    self.bump();
                    let expr = self.parse_expr()?;
                    self.expect_keyword("AS")?;
                    let var = self.parse_var()?;
                    self.expect_operator(")")?;
                    let start = expr.span.start;
                    let end =
                        self.previous().map(|token| token.span.end).unwrap_or(start);
                    vars.push(Spanned {
                        value: SelectVariable {
                            expression: Some(expr),
                            variable: var,
                        },
                        span: Span::new(start, end),
                    });
                } else {
                    break;
                }
            }
            if vars.is_empty() {
                return self.expected("a select variable or expression");
            }
            let end = self.previous().map(|token| token.span.end).unwrap_or(start);
            Spanned {
                value: SelectVariables::Explicit(vars),
                span: Span::new(start, end),
            }
        };

        Ok(SelectClause { option, bindings })
    }

    /// Parses a `ConstructQuery`.
    ///
    /// ```text
    /// ConstructQuery ::= 'CONSTRUCT' ( ConstructTemplate DatasetClause* WhereClause
    ///                                  SolutionModifier
    ///                                | DatasetClause* 'WHERE' '{' TriplesTemplate? '}'
    ///                                  SolutionModifier )
    /// ```
    fn parse_construct_query(&mut self) -> Result<ConstructQuery<'a>, SparqlSyntaxError> {
        self.expect_keyword("CONSTRUCT")?;

        if self.peek_operator("{") {
            let template = self.parse_construct_template()?;
            let dataset_clause = self.parse_dataset_clauses()?;
            let where_clause = self.parse_where_clause()?;
            let solution_modifier = self.parse_solution_modifier()?;
            Ok(ConstructQuery {
                template,
                dataset_clause,
                where_clause: Some(where_clause),
                solution_modifier,
            })
        } else {
            let dataset_clause = self.parse_dataset_clauses()?;
            self.expect_keyword("WHERE")?;
            let template = self.parse_construct_template()?;
            let solution_modifier = self.parse_solution_modifier()?;
            Ok(ConstructQuery {
                template,
                dataset_clause,
                where_clause: None,
                solution_modifier,
            })
        }
    }

    /// Parses a `ConstructTemplate`.
    ///
    /// ```text
    /// ConstructTemplate ::= '{' ConstructTriples? '}'
    /// ```
    fn parse_construct_template(
        &mut self,
    ) -> Result<
        Spanned<Vec<(crate::ast::GraphNode<'a>, crate::ast::PropertyList<'a>)>>,
        SparqlSyntaxError,
    > {
        self.expect_operator("{")?;
        let triples = self.parse_triples_template()?;
        self.expect_operator("}")?;
        Ok(triples)
    }

    /// Parses a `DescribeQuery`.
    ///
    /// ```text
    /// DescribeQuery ::= 'DESCRIBE' ( VarOrIri+ | '*' ) DatasetClause* WhereClause?
    ///                   SolutionModifier
    /// ```
    fn parse_describe_query(&mut self) -> Result<DescribeQuery<'a>, SparqlSyntaxError> {
        self.expect_keyword("DESCRIBE")?;
        let start = self.current_span().start;
        let targets = if self.peek_operator("*") {
            let star_span = self.bump().unwrap().span;
            Spanned {
                value: DescribeTargets::Star,
                span: star_span,
            }
        } else {
            let mut list = Vec::new();
            loop {
                if self
                    .peek()
                    .is_some_and(|t| t.value.is_var() || t.value.is_iri())
                {
                    let value = self.parse_var_or_iri()?;
                    list.push(value);
                } else {
                    break;
                }
            }
            if list.is_empty() {
                return self.expected("a var or IRI to describe");
            }
            let end = self.previous().map(|token| token.span.end).unwrap_or(start);
            Spanned {
                value: DescribeTargets::Explicit(list),
                span: Span::new(start, end),
            }
        };

        let dataset_clause = self.parse_dataset_clauses()?;
        let where_clause = if self.peek_keyword("WHERE") || self.peek_operator("{") {
            Some(self.parse_where_clause()?)
        } else {
            None
        };
        let solution_modifier = self.parse_solution_modifier()?;

        Ok(DescribeQuery {
            targets,
            dataset_clause,
            where_clause,
            solution_modifier,
        })
    }

    /// Parses an `AskQuery`.
    ///
    /// ```text
    /// AskQuery ::= 'ASK' DatasetClause* WhereClause SolutionModifier
    /// ```
    fn parse_ask_query(&mut self) -> Result<AskQuery<'a>, SparqlSyntaxError> {
        self.expect_keyword("ASK")?;
        let dataset_clause = self.parse_dataset_clauses()?;
        let where_clause = self.parse_where_clause()?;
        let solution_modifier = self.parse_solution_modifier()?;
        Ok(AskQuery {
            dataset_clause,
            where_clause,
            solution_modifier,
        })
    }

    /// Parses zero or more `DatasetClause`s (`FROM` / `FROM NAMED`).
    ///
    /// ```text
    /// DatasetClause ::= 'FROM' ( DefaultGraphClause | NamedGraphClause )
    /// ```
    fn parse_dataset_clauses(
        &mut self,
    ) -> Result<Vec<GraphClause<'a>>, SparqlSyntaxError> {
        self.parse_graph_clauses("FROM")
    }

    /// Parses a `WhereClause`.
    ///
    /// ```text
    /// WhereClause ::= 'WHERE'? GroupGraphPattern
    /// ```
    fn parse_where_clause(&mut self) -> Result<GraphPattern<'a>, SparqlSyntaxError> {
        self.parse_keyword("WHERE");
        self.parse_group_graph_pattern()
    }

    /// Parses a `SolutionModifier`.
    ///
    /// ```text
    /// SolutionModifier ::= GroupClause? HavingClause? OrderClause? LimitOffsetClauses?
    /// ```
    fn parse_solution_modifier(
        &mut self,
    ) -> Result<SolutionModifier<'a>, SparqlSyntaxError> {
        let group_clause = if self.parse_keyword("GROUP") {
            self.expect_keyword("BY")?;
            let mut conditions = Vec::new();
            while !self.at_clause_boundary() {
                conditions.push(self.parse_group_condition()?);
            }
            conditions
        } else {
            Vec::new()
        };

        let having_clause = if self.parse_keyword("HAVING") {
            let mut conditions = Vec::new();
            while !self.at_clause_boundary() {
                conditions.push(self.parse_expr()?);
            }
            conditions
        } else {
            Vec::new()
        };

        let order_clause = if self.parse_keyword("ORDER") {
            self.expect_keyword("BY")?;
            let mut conditions = Vec::new();
            while !self.at_clause_boundary() {
                conditions.push(self.parse_order_condition()?);
            }
            conditions
        } else {
            Vec::new()
        };

        let limit_offset_clauses = self.parse_limit_offset_clauses()?;

        Ok(SolutionModifier {
            group_clause,
            having_clause,
            order_clause,
            limit_offset_clauses,
        })
    }

    /// Parses a single `GroupCondition`.
    ///
    /// ```text
    /// GroupCondition ::= BuiltInCall | FunctionCall | '(' Expression ( 'AS' Var )? ')'
    ///                    | Var
    /// ```
    fn parse_group_condition(
        &mut self,
    ) -> Result<(Spanned<Expression<'a>>, Option<crate::ast::Var<'a>>), SparqlSyntaxError>
    {
        if self.peek_operator("(") {
            self.bump();
            let expr = self.parse_expr()?;
            let var = if self.parse_keyword("AS") {
                Some(self.parse_var()?)
            } else {
                None
            };
            self.expect_operator(")")?;
            Ok((expr, var))
        } else {
            let expr = self.parse_expr()?;
            Ok((expr, None))
        }
    }

    /// Parses a single `OrderCondition`.
    ///
    /// ```text
    /// OrderCondition ::= ( ( 'ASC' | 'DESC' ) BrackettedExpression )
    ///                    | ( Constraint | Var )
    /// ```
    fn parse_order_condition(&mut self) -> Result<OrderCondition<'a>, SparqlSyntaxError> {
        if self.parse_keyword("ASC") {
            Ok(OrderCondition::Asc(self.parse_bracketted_expression()?))
        } else if self.parse_keyword("DESC") {
            Ok(OrderCondition::Desc(self.parse_bracketted_expression()?))
        } else if self.peek_operator("(") {
            Ok(OrderCondition::Plain(self.parse_bracketted_expression()?))
        } else {
            Ok(OrderCondition::Plain(self.parse_expr()?))
        }
    }

    /// Parses a `BrackettedExpression` (`'(' Expression ')'`).
    fn parse_bracketted_expression(
        &mut self,
    ) -> Result<Spanned<Expression<'a>>, SparqlSyntaxError> {
        self.expect_operator("(")?;
        let expr = self.parse_expr()?;
        self.expect_operator(")")?;
        Ok(expr)
    }

    /// Parses the optional `LIMIT`/`OFFSET` clauses, in either order.
    ///
    /// ```text
    /// LimitOffsetClauses ::= LimitClause OffsetClause? | OffsetClause LimitClause?
    /// ```
    fn parse_limit_offset_clauses(
        &mut self,
    ) -> Result<Option<LimitOffsetClauses>, SparqlSyntaxError> {
        let mut limit = None;
        let mut offset = None;
        loop {
            if self.parse_keyword("LIMIT") {
                if limit.is_some() {
                    break;
                }
                limit = Some(self.parse_integer_value()?);
            } else if self.parse_keyword("OFFSET") {
                if offset.is_some() {
                    break;
                }
                offset = Some(self.parse_integer_value()?);
            } else {
                break;
            }
        }
        if limit.is_none() && offset.is_none() {
            Ok(None)
        } else {
            Ok(Some(LimitOffsetClauses { offset, limit }))
        }
    }

    /// Parses an `INTEGER` token into a `u64` with its span.
    fn parse_integer_value(&mut self) -> Result<Spanned<u64>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(Token::Integer(n)) => {
                let token = self.bump().unwrap();
                let value = n.parse::<u64>().map_err(|_| {
                    SparqlSyntaxError::single(Diagnostic::error(
                        token.span,
                        format!("`{n}` does not fit in a u64"),
                    ))
                })?;
                Ok(Spanned {
                    value,
                    span: token.span,
                })
            }
            _ => self.expected("an integer"),
        }
    }

    /// Returns `true` when the current token ends one of the `GROUP BY`,
    /// `HAVING` or `ORDER BY` condition lists (i.e. starts the next clause).
    fn at_clause_boundary(&self) -> bool {
        self.at_end()
            || self.peek_operator("}")
            || self.peek_keyword("GROUP")
            || self.peek_keyword("HAVING")
            || self.peek_keyword("ORDER")
            || self.peek_keyword("LIMIT")
            || self.peek_keyword("OFFSET")
            || self.peek_keyword("VALUES")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::test_helpers::*;

    fn render_query(input: &str) -> String {
        render(&parse(input, |parser| parser.parse_query()))
    }

    fn assert_round_trip(input: &str) {
        let once = render_query(input);
        let twice = render_query(&once);
        assert_eq!(
            once, twice,
            "\npretty-printed output is not a stable round-trip for `{input}`:\n  once:  {once}\n  twice: {twice}"
        );
    }

    #[test]
    fn select_simple() {
        assert_round_trip("SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
    }

    #[test]
    fn select_star_distinct() {
        assert_round_trip("SELECT DISTINCT * WHERE { ?s ?p ?o }");
        assert_round_trip("SELECT REDUCED ?s WHERE { ?s ?p ?o }");
    }

    #[test]
    fn select_expression_as() {
        assert_round_trip("SELECT (SUM(?o) AS ?total) WHERE { ?s ?p ?o }");
        assert_round_trip("SELECT ?s (COUNT(*) AS ?c) WHERE { ?s ?p ?o }");
    }

    #[test]
    fn select_without_where_keyword() {
        assert_round_trip("SELECT ?s WHERE { ?s ?p ?o }");
        assert_round_trip("SELECT ?s { ?s ?p ?o }");
    }

    #[test]
    fn select_from() {
        assert_round_trip("SELECT ?s FROM <g1> FROM NAMED <g2> WHERE { ?s ?p ?o }");
    }

    #[test]
    fn select_solution_modifier() {
        assert_round_trip("SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s");
        assert_round_trip("SELECT ?s WHERE { ?s ?p ?o } ORDER BY DESC(?s) LIMIT 10");
        assert_round_trip(
            "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ASC(?s) OFFSET 5 LIMIT 10",
        );
        assert_round_trip(
            "SELECT ?s WHERE { ?s ?p ?o } GROUP BY ?s HAVING (COUNT(?s) > 1)",
        );
        assert_round_trip(
            "SELECT ?s WHERE { ?s ?p ?o } GROUP BY (?s AS ?g) VALUES ?x { 1 }",
        );
    }

    #[test]
    fn select_prologue_and_values() {
        assert_round_trip(
            "PREFIX ex: <http://example.org/> SELECT ?s WHERE { ?s ex:p ?o }",
        );
        assert_round_trip("SELECT ?s WHERE { ?s ?p ?o } VALUES (?s ?o) { (<a> <b>) }");
    }

    #[test]
    fn construct() {
        assert_round_trip("CONSTRUCT { <a> <b> ?o } WHERE { ?s ?p ?o }");
        assert_round_trip("CONSTRUCT { ?s <p> ?o } FROM <g> WHERE { ?s ?p ?o }");
    }

    #[test]
    fn construct_where_shortcut() {
        assert_round_trip("CONSTRUCT WHERE { ?s ?p ?o }");
        assert_round_trip("CONSTRUCT WHERE { ?s ?p ?o } LIMIT 3");
    }

    #[test]
    fn describe() {
        assert_round_trip("DESCRIBE <a>");
        assert_round_trip("DESCRIBE ?s ?o WHERE { ?s ?p ?o }");
        assert_round_trip("DESCRIBE *");
    }

    #[test]
    fn ask() {
        assert_round_trip("ASK WHERE { ?s ?p ?o }");
        assert_round_trip("ASK { ?s ?p ?o }");
        assert_round_trip("ASK { ?s ?p ?o } LIMIT 1");
    }

    #[test]
    fn lowercase_keywords() {
        assert!(
            SparqlParser::new(
                "prefix ex: <http://example.org/> select * where { ?s ?p ?o }",
                default_registry(),
            )
            .parse_query()
            .is_ok()
        );
        assert!(
            SparqlParser::new("ask where { ?s ?p ?o }", default_registry())
                .parse_query()
                .is_ok()
        );
        assert!(
            SparqlParser::new("construct where { ?s ?p ?o }", default_registry())
                .parse_query()
                .is_ok()
        );
    }

    #[test]
    fn error_unknown_query_type() {
        match SparqlParser::new("UPDATE { }", default_registry()).parse_query() {
            Ok(_) => panic!("expected a parse error"),
            Err(e) => assert!(
                e.to_string().contains("expected a query"),
                "unexpected error: {e}"
            ),
        }
    }
}
