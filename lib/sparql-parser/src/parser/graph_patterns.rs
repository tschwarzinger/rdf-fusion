use crate::ast::{
    DataBlockValue, Expression, GraphClause, GraphNodePath, GraphPattern,
    GraphPatternElement, PropertyListPath, SubSelect, ValuesClause, VarOrIri,
};
use crate::error::{DiagnosticsGroup, SparqlSyntaxError};
use crate::lexer::Token;
use crate::parser::SparqlParser;
use crate::span::{Span, Spanned};

impl<'a> SparqlParser<'a> {
    /// Parses a `GroupGraphPattern`.
    ///
    /// ```text
    /// GroupGraphPattern ::= '{' ( SubSelect | GroupGraphPatternSub ) '}'
    /// ```
    pub fn parse_group_graph_pattern(
        &mut self,
    ) -> Result<GraphPattern<'a>, SparqlSyntaxError> {
        self.expect_operator("{")?;
        let pattern = if self.peek_keyword("SELECT") {
            GraphPattern::SubSelect(Box::new(self.parse_sub_select()?))
        } else {
            let elements = self.parse_group_graph_pattern_sub()?;
            GraphPattern::Group(elements)
        };
        self.expect_operator("}")?;
        Ok(pattern)
    }

    /// Parses a `SubSelect`.
    ///
    /// ```text
    /// SubSelect ::= SelectClause WhereClause SolutionModifier ValuesClause
    /// ```
    fn parse_sub_select(&mut self) -> Result<SubSelect<'a>, SparqlSyntaxError> {
        self.parse_sub_select_clause()
    }

    /// Parses a `GroupGraphPatternSub`: a leading `TriplesBlock`, then zero or
    /// more `GraphPatternNotTriples` (each optionally followed by a `.` and
    /// another `TriplesBlock`).
    ///
    /// ```text
    /// GroupGraphPatternSub ::= TriplesBlock? ( GraphPatternNotTriples '.'? TriplesBlock? )*
    /// ```
    fn parse_group_graph_pattern_sub(
        &mut self,
    ) -> Result<Vec<Spanned<GraphPatternElement<'a>>>, SparqlSyntaxError> {
        let mut elements: Vec<Spanned<GraphPatternElement<'a>>> = Vec::new();

        if self.peek_triples_start() {
            elements.push(Self::triples_block_to_element(self.parse_triples_block()?));
        }

        while let Some((value, span)) = self.parse_graph_pattern_not_triples_opt()? {
            elements.push(Spanned { value, span });
            self.consume_operator(".");
            if self.peek_triples_start() {
                elements
                    .push(Self::triples_block_to_element(self.parse_triples_block()?));
            }
        }

        Ok(elements)
    }

    /// Wraps a spanned triples block in a [`GraphPatternElement::Triples`].
    fn triples_block_to_element(
        block: Spanned<Vec<(GraphNodePath<'a>, PropertyListPath<'a>)>>,
    ) -> Spanned<GraphPatternElement<'a>> {
        Spanned {
            value: GraphPatternElement::Triples(block.value),
            span: block.span,
        }
    }

    /// Parses a `TriplesBlock`: one or more `TriplesSameSubjectPath` separated
    /// by dots (a trailing dot before the next non-triple is consumed).
    ///
    /// ```text
    /// TriplesBlock ::= TriplesSameSubjectPath ( '.' TriplesBlock? )?
    /// ```
    fn parse_triples_block(
        &mut self,
    ) -> Result<Spanned<Vec<(GraphNodePath<'a>, PropertyListPath<'a>)>>, SparqlSyntaxError>
    {
        let start = self.current_span().start;
        let mut triples = Vec::new();
        loop {
            triples.push(self.parse_triples_same_subject_path()?);
            if self.consume_operator(".") {
                if !self.peek_triples_start() {
                    break;
                }
                continue;
            }
            if self.peek_operator("}")
                || self.peek_graph_pattern_not_triples()
                || self.at_end()
            {
                break;
            }
            let span = self.current_span();
            let found = self.found();

            let mut group = DiagnosticsGroup::error("invalid triple pattern termination")
                .with_error(span, format!("expected `.` (or `}}`), found `{found}`"));
            if self.peek_triples_start() {
                group = group.with_advice(
                    span,
                    "triple patterns must be separated by `.` or continued with `;`",
                );
            }

            return Err(SparqlSyntaxError::single(group));
        }
        let end = self.previous().map(|token| token.span.end).unwrap_or(start);
        Ok(Spanned {
            value: triples,
            span: Span::new(start, end),
        })
    }

    /// Parses a single `TriplesSameSubjectPath`.
    ///
    /// ```text
    /// TriplesSameSubjectPath ::= VarOrTerm PropertyListPathNotEmpty
    ///                            | TriplesNodePath PropertyListPath
    /// ```
    fn parse_triples_same_subject_path(
        &mut self,
    ) -> Result<(GraphNodePath<'a>, PropertyListPath<'a>), SparqlSyntaxError> {
        if self.peek_operator("[") || self.peek_operator("(") {
            let subject = self.parse_graph_node_path()?;
            let is_triples_node = match &subject {
                GraphNodePath::BlankNodePropertyList(p) => !p.value.is_empty(),
                GraphNodePath::Collection(c) => !c.value.is_empty(),
                _ => false,
            };
            let properties = if is_triples_node {
                self.parse_optional_property_list_path()?
            } else {
                self.parse_property_list_path()?
            };
            Ok((subject, properties))
        } else {
            let subject = GraphNodePath::VarOrTerm(self.parse_var_or_term()?);
            let properties = self.parse_property_list_path()?;
            Ok((subject, properties))
        }
    }

    /// Parses a `PropertyListPath` (which may be empty): delegates to the
    /// non-empty variant only if the current token can start a verb.
    fn parse_optional_property_list_path(
        &mut self,
    ) -> Result<PropertyListPath<'a>, SparqlSyntaxError> {
        if self.peek_verb_start() {
            self.parse_property_list_path()
        } else {
            Ok(PropertyListPath::default())
        }
    }

    /// Returns `true` if the current token can start a `VerbPath` or
    /// `VerbSimple` (a variable, an IRI, `a`, `!`, `^`, or `(`).
    pub(crate) fn peek_verb_start(&self) -> bool {
        match self.peek().map(|token| token.value) {
            Some(v) if v.is_var() || v.is_iri() => true,
            Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("a") => true,
            Some(Token::Operator("!") | Token::Operator("(") | Token::Operator("^")) => {
                true
            }
            _ => false,
        }
    }

    /// Parses a `GraphPatternNotTriples`, returning `None` if the current token
    /// does not start one (so the caller can loop).
    fn parse_graph_pattern_not_triples_opt(
        &mut self,
    ) -> Result<Option<(GraphPatternElement<'a>, Span)>, SparqlSyntaxError> {
        let start = self.current_span().start;
        if let Some(element) = self.parse_graph_pattern_not_triples()? {
            let end = self.previous().map(|token| token.span.end).unwrap_or(start);
            return Ok(Some((element, Span::new(start, end))));
        }
        Ok(None)
    }

    /// Parses a `GraphPatternNotTriples`, returning `None` when the current
    /// token does not start one.
    ///
    /// ```text
    /// GraphPatternNotTriples ::= GroupOrUnionGraphPattern | OptionalGraphPattern
    ///                            | MinusGraphPattern | GraphGraphPattern
    ///                            | ServiceGraphPattern | Filter | Bind | InlineData
    /// ```
    fn parse_graph_pattern_not_triples(
        &mut self,
    ) -> Result<Option<GraphPatternElement<'a>>, SparqlSyntaxError> {
        if self.peek_operator("{") {
            return Ok(Some(self.parse_group_or_union_graph_pattern()?));
        }
        if self.peek_keyword("OPTIONAL") {
            self.bump();
            let pattern = self.parse_group_graph_pattern()?;
            return Ok(Some(GraphPatternElement::Optional(Box::new(pattern))));
        }
        if self.peek_keyword("MINUS") {
            self.bump();
            let pattern = self.parse_group_graph_pattern()?;
            return Ok(Some(GraphPatternElement::Minus(Box::new(pattern))));
        }
        if self.peek_keyword("GRAPH") {
            self.bump();
            let name = self.parse_var_or_iri()?;
            let pattern = self.parse_group_graph_pattern()?;
            return Ok(Some(GraphPatternElement::Graph {
                name,
                pattern: Box::new(pattern),
            }));
        }
        if self.peek_keyword("SERVICE") {
            self.bump();
            let silent = self.parse_keyword("SILENT");
            let name = self.parse_var_or_iri()?;
            let pattern = self.parse_group_graph_pattern()?;
            return Ok(Some(GraphPatternElement::Service {
                silent,
                name,
                pattern: Box::new(pattern),
            }));
        }
        if self.peek_keyword("FILTER") {
            self.bump();
            let expression = self.parse_constraint()?;
            return Ok(Some(GraphPatternElement::Filter(expression)));
        }
        if self.peek_keyword("BIND") {
            self.bump();
            return Ok(Some(self.parse_bind()?));
        }
        if self.peek_keyword("VALUES") {
            self.bump();
            let values = self.parse_values_clause()?;
            return Ok(Some(GraphPatternElement::Values(values)));
        }
        Ok(None)
    }

    /// Parses a `GroupOrUnionGraphPattern`.
    ///
    /// ```text
    /// GroupOrUnionGraphPattern ::= GroupGraphPattern ( 'UNION' GroupGraphPattern )*
    /// ```
    fn parse_group_or_union_graph_pattern(
        &mut self,
    ) -> Result<GraphPatternElement<'a>, SparqlSyntaxError> {
        let mut patterns = vec![self.parse_group_graph_pattern()?];
        while self.parse_keyword("UNION") {
            patterns.push(self.parse_group_graph_pattern()?);
        }
        Ok(GraphPatternElement::Union(patterns))
    }

    /// Parses a `Bind`.
    ///
    /// ```text
    /// Bind ::= 'BIND' '(' Expression 'AS' Var ')'
    /// ```
    fn parse_bind(&mut self) -> Result<GraphPatternElement<'a>, SparqlSyntaxError> {
        self.expect_operator("(")?;
        let expression = self.parse_expr()?;
        self.expect_keyword("AS")?;
        let var = self.parse_var()?;
        self.expect_operator(")")?;
        Ok(GraphPatternElement::Bind(expression, var))
    }

    /// Parses a `Constraint` (a bracketted expression, or a built-in/function
    /// call) as a spanned expression.
    ///
    /// ```text
    /// Constraint ::= BrackettedExpression | BuiltInCall | FunctionCall
    /// ```
    fn parse_constraint(&mut self) -> Result<Spanned<Expression<'a>>, SparqlSyntaxError> {
        let start = self.current_span().start;
        let expression = if self.consume_operator("(") {
            let inner = self.parse_expr()?;
            self.expect_operator(")")?;
            inner
        } else {
            let expr = self.parse_expr()?;
            if !matches!(
                expr.value,
                Expression::Function(..)
                    | Expression::Exists(..)
                    | Expression::NotExists(..)
                    | Expression::Aggregate(..)
            ) {
                return self.expected(
                    "a bracketted expression, built-in call, or function call",
                );
            }
            expr
        };
        let end = self.previous().map(|token| token.span.end).unwrap_or(start);
        let span = Span::new(start, end);
        Ok(Spanned {
            value: expression.value,
            span,
        })
    }

    /// Parses an inline `VALUES` block into a [`ValuesClause`] (called after
    /// the leading `VALUES` keyword has been consumed).
    ///
    /// ```text
    /// DataBlock ::= InlineDataOneVar | InlineDataFull
    /// ```
    pub(crate) fn parse_values_clause(
        &mut self,
    ) -> Result<ValuesClause<'a>, SparqlSyntaxError> {
        if match self.peek().map(|token| token.value) {
            Some(v) => v.is_var(),
            None => false,
        } {
            self.parse_inline_data_one_var()
        } else {
            self.parse_inline_data_full()
        }
    }

    /// Parses an `InlineDataOneVar` (called after `VALUES`).
    ///
    /// ```text
    /// InlineDataOneVar ::= Var '{' DataBlockValue* '}'
    /// ```
    fn parse_inline_data_one_var(
        &mut self,
    ) -> Result<ValuesClause<'a>, SparqlSyntaxError> {
        let var = self.parse_var()?;
        self.expect_operator("{")?;
        let start = self.current_span().start;
        let mut rows = Vec::new();
        while !self.peek_operator("}") {
            let value = self.parse_data_block_value()?;
            rows.push(vec![value]);
        }
        self.expect_operator("}")?;
        let end = self.previous().map(|token| token.span.end).unwrap_or(start);
        Ok(ValuesClause {
            variables: vec![var],
            values: Spanned {
                value: rows,
                span: Span::new(start, end),
            },
        })
    }

    /// Parses an `InlineDataFull` (called after `VALUES`).
    ///
    /// ```text
    /// InlineDataFull ::= ( NIL | '(' Var* ')' ) '{' ( '(' DataBlockValue* ')' | NIL )* '}'
    /// ```
    fn parse_inline_data_full(&mut self) -> Result<ValuesClause<'a>, SparqlSyntaxError> {
        let variables = if self.is_nil() {
            self.parse_nil()?;
            Vec::new()
        } else {
            self.expect_operator("(")?;
            let mut vars = Vec::new();
            while !self.peek_operator(")") {
                vars.push(self.parse_var()?);
            }
            self.expect_operator(")")?;
            vars
        };

        self.expect_operator("{")?;
        let start = self.current_span().start;
        let mut rows = Vec::new();
        while !self.peek_operator("}") {
            if self.is_nil() {
                self.parse_nil()?;
                rows.push(Vec::new());
            } else {
                self.expect_operator("(")?;
                let mut row = Vec::new();
                while !self.peek_operator(")") {
                    row.push(self.parse_data_block_value()?);
                }
                self.expect_operator(")")?;
                rows.push(row);
            }
        }
        self.expect_operator("}")?;
        let end = self.previous().map(|token| token.span.end).unwrap_or(start);

        Ok(ValuesClause {
            variables,
            values: Spanned {
                value: rows,
                span: Span::new(start, end),
            },
        })
    }

    /// Parses a single `DataBlockValue`.
    ///
    /// ```text
    /// DataBlockValue ::= iri | RDFLiteral | NumericLiteral | BooleanLiteral | 'UNDEF'
    /// ```
    fn parse_data_block_value(
        &mut self,
    ) -> Result<DataBlockValue<'a>, SparqlSyntaxError> {
        if self.parse_keyword("UNDEF") {
            return Ok(DataBlockValue::Undef);
        }
        match self.peek().map(|token| token.value) {
            Some(v) if v.is_iri() => Ok(DataBlockValue::Iri(self.parse_iri()?)),
            _ => match self.parse_literal() {
                Ok(literal) => Ok(DataBlockValue::Literal(literal)),
                Err(_) => self.expected("a data block value"),
            },
        }
    }

    /// Parses a `VarOrIri`.
    ///
    /// ```text
    /// VarOrIri ::= Var | iri
    /// ```
    pub(crate) fn parse_var_or_iri(&mut self) -> Result<VarOrIri<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(v) if v.is_var() => Ok(VarOrIri::Var(
                self.parse_var().expect("peeked a variable token"),
            )),
            Some(v) if v.is_iri() => Ok(VarOrIri::Iri(self.parse_iri()?)),
            _ => self.expected("a variable or an IRI"),
        }
    }

    /// Returns `true` if the current token can start a triple subject (`Var`,
    /// `iri`, literal, blank node, collection, or empty list).
    pub(crate) fn peek_triples_start(&self) -> bool {
        match self.peek().map(|token| token.value) {
            None => false,
            Some(Token::Operator("(")) | Some(Token::Operator("[")) => true,
            Some(v)
                if v.is_var() || v.is_iri() || v.is_literal() || v.is_blank_node() =>
            {
                true
            }
            Some(_) => false,
        }
    }

    /// Returns `true` if the current token can start a `GraphPatternNotTriples`.
    fn peek_graph_pattern_not_triples(&self) -> bool {
        if self.peek_operator("{") {
            return true;
        }
        if let Some(Token::Keyword(kw)) = self.peek().map(|t| t.value) {
            matches!(
                kw.to_ascii_uppercase().as_str(),
                "OPTIONAL" | "MINUS" | "GRAPH" | "SERVICE" | "FILTER" | "BIND" | "VALUES"
            )
        } else {
            false
        }
    }

    /// Parses zero or more `'<keyword>' ( iri | 'NAMED' iri )` clauses into a
    /// list of [`GraphClause`]s. Shared by the query dataset `FROM` clause and
    /// the Update `USING` clause, which share the same shape.
    pub(crate) fn parse_graph_clauses(
        &mut self,
        keyword: &str,
    ) -> Result<Vec<GraphClause<'a>>, SparqlSyntaxError> {
        let mut clauses = Vec::new();
        while self.parse_keyword(keyword) {
            if self.parse_keyword("NAMED") {
                let iri = self.parse_iri()?;
                clauses.push(GraphClause::Named(iri));
            } else {
                let iri = self.parse_iri()?;
                clauses.push(GraphClause::Default(iri));
            }
        }
        Ok(clauses)
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::SparqlParser;
    use crate::parser::test_helpers::*;

    fn render_group(input: &str) -> String {
        render(&parse(input, |p| p.parse_group_graph_pattern()))
    }

    fn render_err(input: &str) -> String {
        crate::render_internal(
            &match SparqlParser::new(input, default_registry())
                .parse_group_graph_pattern()
            {
                Ok(_) => panic!("expected parse error for `{input}`"),
                Err(error) => error,
            },
            input,
        )
    }

    /// Asserts that pretty-printing is stable under a parse → print →
    /// re-parse → re-print round-trip.
    fn assert_round_trip(input: &str) {
        let once = render_group(input);
        let twice = render_group(&once);
        assert_eq!(
            once, twice,
            "\npretty-printed output is not a stable round-trip for `{input}`:\n  once:  {once}\n  twice: {twice}"
        );
    }

    #[test]
    fn round_trip_triples() {
        assert_round_trip("{ ?s ?p ?o }");
        assert_round_trip("{ ?s ?p ?o, ?o2; ?q \"v\" }");
        assert_round_trip("{ ?s ?p ?o . ?a ?b ?c }");
        assert_round_trip("{ ?s ?p ?o OPTIONAL { ?a ?b ?c } }");
        assert_round_trip("{ { ?s ?p ?o } UNION { ?a ?b ?c } }");
        assert_round_trip("{ GRAPH <g> { ?s ?p ?o } }");
        assert_round_trip("{ FILTER(?s = <a>) }");
        assert_round_trip("{ VALUES ?x { \"a\" \"b\" } }");
        assert_round_trip("{ VALUES (?x ?y) { (<a> UNDEF) () } }");
    }

    #[test]
    fn round_trip_sub_select() {
        assert_round_trip("{ SELECT ?s WHERE { ?s ?p ?o } }");
        assert_round_trip("{ SELECT (COUNT(?o) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?s }");
        assert_round_trip("{ SELECT * WHERE { ?s ?p ?o } LIMIT 10 }");
    }

    #[test]
    fn simple_triple() {
        assert_eq!(render_group("{ ?s ?p ?o }"), "{ ?s ?p ?o }");
    }

    #[test]
    fn multiple_triples() {
        assert_eq!(
            render_group("{ ?s ?p ?o . ?a ?b ?c }"),
            "{ ?s ?p ?o . ?a ?b ?c }"
        );
    }

    #[test]
    fn property_path() {
        assert_eq!(render_group("{ ?s ex:p/ex:q ?o }"), "{ ?s ex:p/ex:q ?o }");
    }

    #[test]
    fn empty_group() {
        assert_eq!(render_group("{ }"), "{ }");
    }

    #[test]
    fn optional() {
        assert_eq!(
            render_group("{ ?s ?p ?o OPTIONAL { ?a ?b ?c } }"),
            "{ ?s ?p ?o OPTIONAL { ?a ?b ?c } }"
        );
    }

    #[test]
    fn minus() {
        assert_eq!(
            render_group("{ ?s ?p ?o MINUS { ?a ?b ?c } }"),
            "{ ?s ?p ?o MINUS { ?a ?b ?c } }"
        );
    }

    #[test]
    fn union() {
        assert_eq!(
            render_group("{ { ?s ?p ?o } UNION { ?a ?b ?c } }"),
            "{ { ?s ?p ?o } UNION { ?a ?b ?c } }"
        );
    }

    #[test]
    fn graph() {
        assert_eq!(
            render_group("{ GRAPH <g> { ?s ?p ?o } }"),
            "{ GRAPH <g> { ?s ?p ?o } }"
        );
    }

    #[test]
    fn graph_var() {
        assert_eq!(
            render_group("{ GRAPH ?g { ?s ?p ?o } }"),
            "{ GRAPH ?g { ?s ?p ?o } }"
        );
    }

    #[test]
    fn service() {
        assert_eq!(
            render_group("{ SERVICE <g> { ?s ?p ?o } }"),
            "{ SERVICE <g> { ?s ?p ?o } }"
        );
    }

    #[test]
    fn service_silent() {
        assert_eq!(
            render_group("{ SERVICE SILENT <g> { ?s ?p ?o } }"),
            "{ SERVICE SILENT <g> { ?s ?p ?o } }"
        );
    }

    #[test]
    fn nested_group() {
        assert_eq!(render_group("{ { ?s ?p ?o } }"), "{ { ?s ?p ?o } }");
    }

    #[test]
    fn values_one_var() {
        assert_eq!(
            render_group("{ VALUES ?x { \"a\" \"b\" } }"),
            "{ VALUES ?x { \"a\" \"b\" } }"
        );
    }

    #[test]
    fn values_full() {
        assert_eq!(
            render_group("{ VALUES (?x ?y) { (<a> UNDEF) () } }"),
            "{ VALUES (?x ?y) { (<a> UNDEF) () } }"
        );
    }

    #[test]
    fn values_undef_iri_numeric() {
        assert_eq!(
            render_group("{ VALUES ?x { UNDEF <i> 42 } }"),
            "{ VALUES ?x { UNDEF <i> 42 } }"
        );
    }

    #[test]
    fn bind() {
        assert_eq!(
            render_group("{ BIND(?x + 1 AS ?y) }"),
            "{ BIND((?x + 1) AS ?y) }"
        );
    }

    #[test]
    fn filter() {
        assert_eq!(
            render_group("{ ?s ?p ?o FILTER(?s = <a>) }"),
            "{ ?s ?p ?o FILTER (?s = <a>) }"
        );
        assert_eq!(
            render_group("{ ?s ?p ?o ; FILTER(?s = <a>) }"),
            "{ ?s ?p ?o FILTER (?s = <a>) }"
        );
        assert_eq!(
            render_group("{ ?s ?p ?o ; OPTIONAL { ?a ?b ?c } }"),
            "{ ?s ?p ?o OPTIONAL { ?a ?b ?c } }"
        );
    }

    #[test]
    fn error_unclosed_group() {
        insta::assert_snapshot!(render_err("{ ?s ?p ?o"), @"
        error: expected }, found `end of input`
          ┌─ :1:11
          │
        1 │ { ?s ?p ?o
          │           ^ expected }, found `end of input`
        ");
    }
}
