use crate::ast::{GraphNode, Object, PropertyList, Verb};
use crate::error::{Diagnostic, SparqlSyntaxError};
use crate::lexer::Token;
use crate::parser::SparqlParser;
use crate::span::{Span, Spanned};

impl<'a> SparqlParser<'a> {
    /// Parses a `TriplesTemplate`: zero or more `TriplesSameSubject` separated
    /// by `.`.
    ///
    /// ```text
    /// TriplesTemplate ::= TriplesSameSubject ('.' TriplesTemplate?)?
    /// ```
    ///
    /// Errors are recovered from: an offending triple is skipped up to the next
    /// triple boundary (`.`, `}` or `]`), so that subsequent triples can still
    /// be parsed, and their [`Diagnostic`]s are gathered into the returned
    /// [`SparqlSyntaxError`].
    pub fn parse_triples_template(
        &mut self,
    ) -> Result<Spanned<Vec<(GraphNode<'a>, PropertyList<'a>)>>, SparqlSyntaxError> {
        let start = self.current_span().start;
        let mut triples = Vec::new();
        let mut errors = SparqlSyntaxError::default();

        loop {
            if self.at_end()
                || self.peek_operator("}")
                || self.peek_operator("]")
                || self.peek_keyword("GRAPH")
            {
                break;
            }
            match self.parse_triples_same_subject() {
                Ok(triple) => triples.push(triple),
                Err(error) => {
                    errors.extend(error);
                    self.recover_template_triple();
                }
            }

            if self.consume_operator(".") {
                continue;
            }
            if self.at_end()
                || self.peek_operator("}")
                || self.peek_operator("]")
                || self.peek_keyword("GRAPH")
            {
                break;
            }
            errors.push(Diagnostic::error(
                self.current_span(),
                format!("expected `.` between triples, found `{}`", self.found()),
            ));
            self.recover_template_triple();
            break;
        }

        let end = self.previous().map(|token| token.span.end).unwrap_or(start);

        if errors.has_errors() {
            return Err(errors);
        }
        Ok(Spanned {
            value: triples,
            span: Span::new(start, end),
        })
    }

    /// Parses a single `TriplesSameSubject`: a subject followed by a non-empty
    /// property list (or optional property list if subject is a TriplesNode).
    ///
    /// ```text
    /// TriplesSameSubject ::= VarOrTerm PropertyListNotEmpty | TriplesNode PropertyList
    /// ```
    fn parse_triples_same_subject(
        &mut self,
    ) -> Result<(GraphNode<'a>, PropertyList<'a>), SparqlSyntaxError> {
        let subject = self.parse_graph_node()?;
        let is_triples_node = match &subject {
            GraphNode::BlankNodePropertyList(p) => !p.value.is_empty(),
            GraphNode::Collection(c) => !c.value.is_empty(),
            _ => false,
        };
        let properties = if is_triples_node {
            if self.peek_verb_start_simple() {
                self.parse_property_list()?
            } else {
                PropertyList::default()
            }
        } else {
            self.parse_property_list()?
        };
        Ok((subject, properties))
    }

    /// Parses a `PropertyListNotEmpty` (a sequence of `Verb ObjectList`
    /// groups separated by `;`, with an optional trailing `;`).
    ///
    /// ```text
    /// PropertyListNotEmpty ::= Verb ObjectList (';' (Verb ObjectList)?)*
    /// ```
    pub fn parse_property_list(&mut self) -> Result<PropertyList<'a>, SparqlSyntaxError> {
        let mut list: PropertyList<'a> = PropertyList::default();
        loop {
            let verb = self.parse_verb()?;
            let objects = self.parse_object_list()?;
            list.push(verb, objects);
            if !self.consume_operator(";") {
                break;
            }
            if self.is_group_end() || !self.peek_verb_start_simple() {
                break;
            }
        }
        Ok(list)
    }

    /// Returns `true` if the current token can start a simple verb (variable, IRI, or `'a'`).
    fn peek_verb_start_simple(&self) -> bool {
        match self.peek().map(|t| t.value) {
            Some(v) if v.is_var() || v.is_iri() => true,
            Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("a") => true,
            _ => false,
        }
    }

    /// Parses an `ObjectList`: one or more objects separated by `,`.
    ///
    /// ```text
    /// ObjectList ::= Object (',' Object)*
    /// ```
    fn parse_object_list(&mut self) -> Result<Vec<Object<'a>>, SparqlSyntaxError> {
        let mut objects = vec![self.parse_object()?];
        while self.consume_operator(",") {
            if self.is_object_list_end() {
                let span = self.current_span();
                let found = self.found();
                return Err(Diagnostic::error(
                    span,
                    format!("expected an object after `,`, found `{found}`"),
                )
                .into());
            }
            objects.push(self.parse_object()?);
        }
        Ok(objects)
    }

    /// Parses an `Object` (a single [`GraphNode`]).
    ///
    /// ```text
    /// Object ::= GraphNode
    /// ```
    fn parse_object(&mut self) -> Result<Object<'a>, SparqlSyntaxError> {
        Ok(Object {
            graph_node: self.parse_graph_node()?,
        })
    }

    /// Parses a `Verb`: `'a'`, a variable, or an IRI.
    ///
    /// ```text
    /// Verb ::= VarOrIri | 'a'
    /// ```
    fn parse_verb(&mut self) -> Result<Verb<'a>, SparqlSyntaxError> {
        if self.parse_keyword("a") {
            return Ok(Verb::A);
        }
        match self.peek().map(|token| token.value) {
            Some(v) if v.is_var() => Ok(Verb::Var(self.parse_var()?)),
            Some(v) if v.is_iri() => Ok(Verb::Iri(self.parse_iri()?)),
            _ => self.expected("a predicate"),
        }
    }

    /// Parses a `GraphNode`: a term/variable, a collection, or a blank node
    /// property list.
    ///
    /// ```text
    /// GraphNode ::= VarOrTerm | TriplesNode
    /// ```
    pub fn parse_graph_node(&mut self) -> Result<GraphNode<'a>, SparqlSyntaxError> {
        if self.peek_operator("[") {
            return self.parse_blank_node_property_list();
        }
        if self.peek_operator("(") {
            return self.parse_collection_or_nil();
        }
        Ok(GraphNode::VarOrTerm(self.parse_var_or_term()?))
    }

    /// Parses a blank node property list `[ Verb ObjectList (';' ...)* ]`.
    ///
    /// ```text
    /// BlankNodePropertyList ::= '[' PropertyListNotEmpty ']'
    /// ```
    fn parse_blank_node_property_list(
        &mut self,
    ) -> Result<GraphNode<'a>, SparqlSyntaxError> {
        let open = self.current_span();
        self.bump();
        let properties = if self.peek_operator("]") {
            self.bump();
            PropertyList::default()
        } else {
            let list = self.parse_property_list()?;
            self.expect_operator("]")?;
            list
        };
        let end = self
            .previous()
            .map(|token| token.span.end)
            .unwrap_or(open.start);
        Ok(GraphNode::BlankNodePropertyList(Spanned {
            value: properties,
            span: Span::new(open.start, end),
        }))
    }

    /// Parses a collection `( GraphNode+ )` or the empty list token `()` (NIL).
    ///
    /// ```text
    /// Collection ::= '(' GraphNode+ ')'
    /// NIL ::= '(' ')'
    /// ```
    fn parse_collection_or_nil(&mut self) -> Result<GraphNode<'a>, SparqlSyntaxError> {
        let open = self.current_span();
        if self.is_nil() {
            return Ok(GraphNode::VarOrTerm(self.parse_nil()?));
        }

        self.bump();
        let mut nodes = Vec::new();
        loop {
            if self.peek_operator(")") {
                self.bump();
                break;
            }
            if self.at_end() || self.peek_operator("}") {
                return self.expected("`)` to close the collection");
            }
            nodes.push(self.parse_graph_node()?);
        }
        let end = self
            .previous()
            .map(|token| token.span.end)
            .unwrap_or(open.start);
        Ok(GraphNode::Collection(Spanned {
            value: nodes,
            span: Span::new(open.start, end),
        }))
    }

    /// Error recovery at the triples-template level: skip tokens until a triple
    /// boundary (`.`, `]`, `}`) or end of input.
    ///
    /// `;` is deliberately omitted: a triple parses atomically, so a failed
    /// triple is abandoned in full. Stopping at a `;` would leave the template
    /// loop facing a non-`.` token and emit a spurious `expected . between
    /// triples` diagnostic on top of the original error.
    fn recover_template_triple(&mut self) {
        self.sync_until(&[".", "]", "}"]);
    }

    /// Skips tokens until the current token is one of `stoppables` (or end of
    /// input).
    fn sync_until(&mut self, stoppables: &[&str]) {
        loop {
            match self.peek() {
                None => break,
                Some(Spanned {
                    value: Token::Operator(op),
                    ..
                }) if stoppables.contains(op) => break,
                Some(Spanned {
                    value: Token::Keyword(kw),
                    ..
                }) if stoppables.contains(kw) => break,
                Some(_) => {
                    self.bump();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::pretty_printable;
    use crate::error::SparqlSyntaxError;
    use crate::parser::test_helpers::{default_registry, parse};
    use crate::span::Spanned;
    use std::fmt;

    /// Parses `input` as a triples template, returning the full spanned AST.
    fn parse_triples(input: &str) -> Spanned<Vec<(GraphNode<'_>, PropertyList<'_>)>> {
        parse(input, |parser| parser.parse_triples_template())
    }

    fn parse_err(input: &str) -> SparqlSyntaxError {
        match SparqlParser::new(input, default_registry()).parse_triples_template() {
            Ok(_) => panic!("expected parse error for `{input}`"),
            Err(error) => error,
        }
    }

    /// A wrapper rendering a triples block compactly via the AST
    /// [`SparqlPrettyPrintable`] implementations (no span noise).
    ///
    /// TODO: Remove and find better solution.
    struct Block<'a>(Vec<(GraphNode<'a>, PropertyList<'a>)>);

    impl fmt::Display for Block<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            for (subject, props) in &self.0 {
                write!(f, "{}", pretty_printable(&subject))?;
                for (verb, objects) in props.iter() {
                    write!(f, " {}", pretty_printable(&verb))?;
                    for object in objects {
                        write!(f, " {}", pretty_printable(&object))?;
                    }
                }
                write!(f, " . ")?;
            }
            Ok(())
        }
    }

    #[test]
    fn simple_iri_triple() {
        insta::assert_snapshot!(Block(parse_triples("<s> <p> <o>").value), @"<s> <p> <o> .");
    }

    #[test]
    fn variable_subject_and_predicate() {
        insta::assert_snapshot!(Block(parse_triples("?s ?p <o>").value), @"?s ?p <o> .");
    }

    #[test]
    fn a_verb() {
        insta::assert_snapshot!(Block(parse_triples("<s> a <o>").value), @"<s> a <o> .");
    }

    #[test]
    fn literal_objects() {
        insta::assert_snapshot!(Block(parse_triples("<s> <p> 42, \"hi\", true").value), @r#"<s> <p> 42 "hi" true ."#);
    }

    #[test]
    fn language_and_typed_literals() {
        insta::assert_snapshot!(Block(parse_triples("<s> <p> \"hi\"@en, \"4\"^^<xsd:int>").value), @r#"<s> <p> "hi"@en "4"^^<xsd:int> ."#);
    }

    #[test]
    fn prefixed_names() {
        insta::assert_snapshot!(Block(parse_triples("ex:s ex:p ex:o").value), @"ex:s ex:p ex:o .");
    }

    #[test]
    fn blank_nodes() {
        insta::assert_snapshot!(Block(parse_triples("_:b <p> _:b2").value), @"_:b <p> _:b2 .");
    }

    #[test]
    fn nil_object() {
        insta::assert_snapshot!(Block(parse_triples("<s> <p> ()").value), @"<s> <p> () .");
    }

    #[test]
    fn collection() {
        insta::assert_snapshot!(Block(parse_triples("<s> <p> (<a> <b>)").value), @"<s> <p> (<a> <b>) .");
    }

    #[test]
    fn blank_node_property_list() {
        insta::assert_snapshot!(Block(parse_triples("<s> <p> [<q> <x>]").value), @"<s> <p> [<q> <x>] .");
    }

    #[test]
    fn blank_node_subject_only() {
        insta::assert_snapshot!(Block(parse_triples("[<q> <x>] .").value), @"[<q> <x>] .");
    }

    #[test]
    fn multiple_predicates_and_objects() {
        insta::assert_snapshot!(Block(parse_triples("<s> <p> <o>; <q> <o2>, <o3>").value), @"<s> <p> <o> <q> <o2> <o3> .");
    }

    #[test]
    fn trailing_semicolon_allowed() {
        insta::assert_snapshot!(Block(parse_triples("<s> <p> <o>;").value), @"<s> <p> <o> .");
    }

    #[test]
    fn multiple_triples() {
        insta::assert_snapshot!(Block(parse_triples("<s> <p> <o> . <s2> <p2> <o2>").value), @"<s> <p> <o> . <s2> <p2> <o2> .");
    }

    #[test]
    fn empty_template() {
        insta::assert_snapshot!(Block(parse_triples("").value), @"");
    }

    // ----- Error tests (full messages + spans, rendered with codespan-reporting) --------

    /// Parses `input`, returning the rendered report string.
    fn render_err(input: &str) -> String {
        crate::render_internal(&parse_err(input), input)
    }

    #[test]
    fn error_missing_object() {
        insta::assert_snapshot!(render_err("<s> <p> ."), @"
        error: expected a term or variable, found `.`
          ┌─ :1:9
          │
        1 │ <s> <p> .
          │         ^ expected a term or variable, found `.`
        ");
    }

    #[test]
    fn error_missing_predicate() {
        insta::assert_snapshot!(render_err("<s> ."), @"
        error: expected a predicate, found `.`
          ┌─ :1:5
          │
        1 │ <s> .
          │     ^ expected a predicate, found `.`
        ");
    }

    #[test]
    fn error_missing_dot_between_triples() {
        insta::assert_snapshot!(render_err("<s> <p> <o> <o2>"), @"
        error: expected `.` between triples, found `<o2>`
          ┌─ :1:13
          │
        1 │ <s> <p> <o> <o2>
          │             ^^^^ expected `.` between triples, found `<o2>`
        ");
    }

    #[test]
    fn error_unbalanced_collection() {
        insta::assert_snapshot!(render_err("<s> <p> (<o>"), @"
        error: expected `)` to close the collection, found `end of input`
          ┌─ :1:13
          │
        1 │ <s> <p> (<o>
          │             ^ expected `)` to close the collection, found `end of input`
        ");
    }

    #[test]
    fn error_unbalanced_blank_node_property_list() {
        insta::assert_snapshot!(render_err("<s> <p> [<q> <x>"), @"
        error: expected ], found `end of input`
          ┌─ :1:17
          │
        1 │ <s> <p> [<q> <x>
          │                 ^ expected ], found `end of input`
        ");
    }

    #[test]
    fn error_multiple_errors_reported() {
        insta::assert_snapshot!(render_err("<s> <p> . <a> <b> ."), @"
        error: expected a term or variable, found `.`
          ┌─ :1:9
          │
        1 │ <s> <p> . <a> <b> .
          │         ^ expected a term or variable, found `.`

        error: expected a term or variable, found `.`
          ┌─ :1:19
          │
        1 │ <s> <p> . <a> <b> .
          │                   ^ expected a term or variable, found `.`
        ");
    }

    #[test]
    fn error_no_spurious_dot_after_semicolon_recovery() {
        insta::assert_snapshot!(render_err("?s ?p , ; <a> <b> <c>"), @"
        error: expected a term or variable, found `,`
          ┌─ :1:7
          │
        1 │ ?s ?p , ; <a> <b> <c>
          │       ^ expected a term or variable, found `,`
        ");
    }
}
