use crate::ast::{
    GraphNodePath, ObjectPath, Path, PathOneInPropertySet, PropertyListPath, VarOrPath,
};
use crate::error::{Diagnostic, DiagnosticsGroup, SparqlSyntaxError};
use crate::lexer::Token;
use crate::parser::SparqlParser;
use crate::span::{Span, Spanned};

impl<'a> SparqlParser<'a> {
    /// Parses a `PropertyListPathNotEmpty`.
    ///
    /// ```text
    /// PropertyListPathNotEmpty ::= ( VerbPath | VerbSimple ) ObjectListPath
    ///                              ( ';' ( ( VerbPath | VerbSimple ) ObjectList )? )*
    /// ```
    pub fn parse_property_list_path(
        &mut self,
    ) -> Result<PropertyListPath<'a>, SparqlSyntaxError> {
        let mut list: PropertyListPath<'a> = PropertyListPath::default();
        loop {
            let verb = self.parse_var_or_path()?;
            let objects = self.parse_object_list_path()?;
            list.push(verb, objects);
            if !self.consume_operator(";") {
                break;
            }
            if self.is_group_end() || !self.peek_verb_start() {
                break;
            }
        }
        Ok(list)
    }

    /// Parses a `VerbPath` or `VerbSimple`.
    ///
    /// ```text
    /// VerbPath   ::= Path
    /// VerbSimple ::= Var
    /// ```
    fn parse_var_or_path(&mut self) -> Result<VarOrPath<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(v) if v.is_var() => Ok(VarOrPath::Var(
                self.parse_var().expect("peeked a variable token"),
            )),
            _ => Ok(VarOrPath::Path(self.parse_path()?)),
        }
    }

    /// Parses an `ObjectListPath`.
    ///
    /// ```text
    /// ObjectListPath ::= ObjectPath ( ',' ObjectPath )*
    /// ```
    fn parse_object_list_path(
        &mut self,
    ) -> Result<Vec<ObjectPath<'a>>, SparqlSyntaxError> {
        let mut objects = vec![self.parse_object_path()?];
        while self.consume_operator(",") {
            if self.is_object_list_end() {
                let span = self.current_span();
                let found = self.found();
                return Err(SparqlSyntaxError::single(Diagnostic::error(
                    span,
                    format!("expected an object after `,`, found `{found}`"),
                )));
            }
            objects.push(self.parse_object_path()?);
        }
        Ok(objects)
    }

    /// Parses a single `ObjectPath`.
    ///
    /// ```text
    /// ObjectPath ::= GraphNodePath
    /// ```
    fn parse_object_path(&mut self) -> Result<ObjectPath<'a>, SparqlSyntaxError> {
        Ok(ObjectPath {
            graph_node: self.parse_graph_node_path()?,
        })
    }

    /// Parses a `GraphNodePath`.
    ///
    /// ```text
    /// GraphNodePath ::= VarOrTerm | TriplesNodePath
    /// ```
    pub fn parse_graph_node_path(
        &mut self,
    ) -> Result<GraphNodePath<'a>, SparqlSyntaxError> {
        if self.peek_operator("[") {
            return self.parse_blank_node_property_list_path();
        }
        if self.peek_operator("(") {
            return self.parse_collection_or_nil_path();
        }
        Ok(GraphNodePath::VarOrTerm(self.parse_var_or_term()?))
    }

    /// Parses a `BlankNodePropertyListPath`.
    ///
    /// ```text
    /// BlankNodePropertyListPath ::= '[' PropertyListPathNotEmpty ']'
    /// ```
    fn parse_blank_node_property_list_path(
        &mut self,
    ) -> Result<GraphNodePath<'a>, SparqlSyntaxError> {
        let open = self.current_span();
        self.bump();
        let properties = if self.peek_operator("]") {
            self.bump();
            PropertyListPath::default()
        } else {
            let list = self.parse_property_list_path()?;
            self.expect_operator("]")?;
            list
        };
        let end = self
            .previous()
            .map(|token| token.span.end)
            .unwrap_or(open.start);
        Ok(GraphNodePath::BlankNodePropertyList(Spanned {
            value: properties,
            span: Span::new(open.start, end),
        }))
    }

    /// Parses a path collection `( GraphNodePath+ )` or the empty list `()`.
    ///
    /// ```text
    /// CollectionPath ::= '(' GraphNodePath+ ')'
    /// NIL            ::= '(' ')'
    /// ```
    fn parse_collection_or_nil_path(
        &mut self,
    ) -> Result<GraphNodePath<'a>, SparqlSyntaxError> {
        let open = self.current_span();
        if self.is_nil() {
            return Ok(GraphNodePath::VarOrTerm(self.parse_nil()?));
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
            nodes.push(self.parse_graph_node_path()?);
        }

        let end = self
            .previous()
            .map(|token| token.span.end)
            .unwrap_or(open.start);
        Ok(GraphNodePath::Collection(Spanned {
            value: nodes,
            span: Span::new(open.start, end),
        }))
    }

    /// Parses a `Path`, the top-level entry point for property paths.
    ///
    /// ```text
    /// Path ::= PathAlternative
    /// ```
    pub fn parse_path(&mut self) -> Result<Path<'a>, SparqlSyntaxError> {
        self.parse_path_alternative()
    }

    /// Parses a `PathAlternative`.
    ///
    /// ```text
    /// PathAlternative ::= PathSequence ( '|' PathSequence )*
    /// ```
    fn parse_path_alternative(&mut self) -> Result<Path<'a>, SparqlSyntaxError> {
        let mut path = self.parse_path_sequence()?;
        while self.consume_operator("|") {
            let right = self.parse_path_sequence()?;
            path = Path::Alternative(Box::new(path), Box::new(right));
        }
        Ok(path)
    }

    /// Parses a `PathSequence`.
    ///
    /// ```text
    /// PathSequence ::= PathEltOrInverse ( '/' PathEltOrInverse )*
    /// ```
    fn parse_path_sequence(&mut self) -> Result<Path<'a>, SparqlSyntaxError> {
        let mut path = self.parse_path_elt_or_inverse()?;
        while self.consume_operator("/") {
            let right = self.parse_path_elt_or_inverse()?;
            path = Path::Sequence(Box::new(path), Box::new(right));
        }
        Ok(path)
    }

    /// Parses a `PathEltOrInverse`.
    ///
    /// ```text
    /// PathEltOrInverse ::= PathElt | '^' PathElt
    /// ```
    fn parse_path_elt_or_inverse(&mut self) -> Result<Path<'a>, SparqlSyntaxError> {
        if self.consume_operator("^") {
            let elt = self.parse_path_elt()?;
            Ok(Path::Inverse(Box::new(elt)))
        } else {
            self.parse_path_elt()
        }
    }

    /// Parses a `PathElt`.
    ///
    /// ```text
    /// PathElt ::= PathPrimary PathMod?
    /// ```
    fn parse_path_elt(&mut self) -> Result<Path<'a>, SparqlSyntaxError> {
        let primary = self.parse_path_primary()?;
        match self.peek().map(|t| t.value) {
            Some(Token::Operator("?")) => {
                self.bump();
                Ok(Path::ZeroOrOne(Box::new(primary)))
            }
            Some(Token::Operator("*")) => {
                self.bump();
                Ok(Path::ZeroOrMore(Box::new(primary)))
            }
            Some(Token::Operator("+")) => {
                self.bump();
                Ok(Path::OneOrMore(Box::new(primary)))
            }
            _ => Ok(primary),
        }
    }

    /// Parses a `PathPrimary`.
    ///
    /// ```text
    /// PathPrimary ::= iri | 'a' | '!' PathNegatedPropertySet | '(' Path ')'
    /// ```
    fn parse_path_primary(&mut self) -> Result<Path<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(v) if v.is_iri() => Ok(Path::Iri(self.parse_iri()?)),
            Some(Token::Keyword("a")) => {
                self.bump();
                Ok(Path::A)
            }
            Some(Token::Operator("!")) => {
                self.bump();
                let negated = self.parse_path_negated_property_set()?;
                Ok(Path::NegatedPropertySet(negated))
            }
            Some(Token::Operator("(")) => {
                self.bump();
                let path = self.parse_path()?;
                self.expect_path_close_paren()?;
                Ok(path)
            }
            _ => self.expected("a predicate or property path"),
        }
    }

    /// Consumes the `)` that closes a parenthesized path, reporting a
    /// suggestion to add it when it is missing.
    fn expect_path_close_paren(&mut self) -> Result<(), SparqlSyntaxError> {
        if self.consume_operator(")") {
            Ok(())
        } else {
            let span = self.current_span();
            let found = self.found();
            let group = DiagnosticsGroup::error(format!("expected `)`, found `{found}`"))
                .with_error(span, format!("expected `)`, found `{found}`"))
                .with_advice(span, "consider adding a `)` to close the path");
            Err(group.into())
        }
    }

    /// Parses a `PathNegatedPropertySet` (called after the leading `!` has been
    /// consumed).
    ///
    /// ```text
    /// PathNegatedPropertySet ::= PathOneInPropertySet
    ///                            | '(' ( PathOneInPropertySet ( '|' PathOneInPropertySet )* )? ')'
    /// ```
    fn parse_path_negated_property_set(
        &mut self,
    ) -> Result<Vec<PathOneInPropertySet<'a>>, SparqlSyntaxError> {
        if self.peek_operator("(") {
            self.bump();
            let mut entries = Vec::new();
            if self.peek_operator(")") {
                self.bump();
                return Ok(entries);
            }
            entries.push(self.parse_path_one_in_property_set()?);
            while self.consume_operator("|") {
                entries.push(self.parse_path_one_in_property_set()?);
            }
            self.expect_path_close_paren()?;
            Ok(entries)
        } else {
            Ok(vec![self.parse_path_one_in_property_set()?])
        }
    }

    /// Parses a single `PathOneInPropertySet`.
    ///
    /// ```text
    /// PathOneInPropertySet ::= iri | 'a' | '^' ( iri | 'a' )
    /// ```
    fn parse_path_one_in_property_set(
        &mut self,
    ) -> Result<PathOneInPropertySet<'a>, SparqlSyntaxError> {
        let is_inverse = self.consume_operator("^");

        match self.peek().map(|token| token.value) {
            Some(v) if v.is_iri() => {
                let iri = self.parse_iri()?;
                Ok(if is_inverse {
                    PathOneInPropertySet::InverseIri(iri)
                } else {
                    PathOneInPropertySet::Iri(iri)
                })
            }
            Some(Token::Keyword("a")) => {
                self.bump();
                Ok(if is_inverse {
                    PathOneInPropertySet::InverseA
                } else {
                    PathOneInPropertySet::A
                })
            }
            _ => self.expected("a negated property set entry"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::pretty_print::pretty_printable;
    use crate::parser::test_helpers::*;

    /// Renders a `Path` compactly.
    fn render_path(input: &str) -> String {
        render(&parse(input, |p| p.parse_path()))
    }

    /// Renders a `PropertyListPathNotEmpty` compactly.
    fn render_props(input: &str) -> String {
        let list = parse(input, |p| p.parse_property_list_path());
        format!("{}", pretty_printable(&list))
    }

    #[test]
    fn path_iri() {
        assert_eq!(
            render_path("<http://example.org/p>"),
            "<http://example.org/p>"
        );
        assert_eq!(render_path("ex:p"), "ex:p");
    }

    #[test]
    fn path_a() {
        assert_eq!(render_path("a"), "a");
    }

    #[test]
    fn path_inverse() {
        assert_eq!(render_path("^ex:p"), "^ex:p");
    }

    #[test]
    fn path_sequence() {
        assert_eq!(render_path("ex:p/ex:q"), "ex:p/ex:q");
    }

    #[test]
    fn path_alternative() {
        assert_eq!(render_path("ex:p|ex:q"), "ex:p|ex:q");
    }

    #[test]
    fn path_mixed_precedence() {
        assert_eq!(render_path("ex:p/ex:q|ex:r/ex:s"), "ex:p/ex:q|ex:r/ex:s");
    }

    #[test]
    fn path_mods() {
        assert_eq!(render_path("ex:p?"), "ex:p?");
        assert_eq!(render_path("ex:p*"), "ex:p*");
        assert_eq!(render_path("ex:p+"), "ex:p+");
    }

    #[test]
    fn path_inverse_sequence() {
        assert_eq!(render_path("^ex:p/ex:q"), "^ex:p/ex:q");
    }

    #[test]
    fn path_nested_parens() {
        assert_eq!(render_path("(ex:p/ex:q)"), "ex:p/ex:q");
        assert_eq!(render_path("(ex:p)?"), "ex:p?");
        assert_eq!(render_path("(ex:p|ex:q)*"), "(ex:p|ex:q)*");
    }

    #[test]
    fn path_negation_single() {
        assert_eq!(render_path("!ex:p"), "!ex:p");
    }

    #[test]
    fn path_negation_multiple() {
        assert_eq!(render_path("!(ex:p|ex:q)"), "!(ex:p|ex:q)");
    }

    #[test]
    fn path_negation_with_a() {
        assert_eq!(render_path("!(a|^a|ex:p)"), "!(a|^a|ex:p)");
    }

    #[test]
    fn path_negation_empty_set() {
        assert_eq!(render_path("!()"), "!()");
    }

    #[test]
    fn path_negation_mod() {
        assert_eq!(render_path("!(ex:p|ex:q)+"), "!(ex:p|ex:q)+");
    }

    #[test]
    fn props_path_verb() {
        assert_eq!(render_props("ex:p/ex:q ?o"), "ex:p/ex:q ?o");
    }

    #[test]
    fn props_var_verb() {
        assert_eq!(render_props("?p ?o"), "?p ?o");
    }

    #[test]
    fn props_multiple_objects() {
        assert_eq!(render_props("ex:p ?o1, ?o2"), "ex:p ?o1 ?o2");
    }

    #[test]
    fn props_multiple_predicates() {
        assert_eq!(
            render_props("ex:p ?o; ex:q/ex:r ?o2"),
            "ex:p ?o ; ex:q/ex:r ?o2"
        );
    }

    #[test]
    fn props_trailing_semicolon() {
        assert_eq!(render_props("ex:p ?o;"), "ex:p ?o");
    }

    #[test]
    fn graph_node_path_collection() {
        assert!(matches!(
            parse("(<a> <b>)", |p| p.parse_graph_node_path()),
            GraphNodePath::Collection(_)
        ));
    }

    #[test]
    fn graph_node_path_blank_node_property_list() {
        assert!(matches!(
            parse("[ex:p ?x]", |p| p.parse_graph_node_path()),
            GraphNodePath::BlankNodePropertyList(_)
        ));
    }

    #[test]
    fn graph_node_path_var_or_term() {
        assert!(matches!(
            parse("?x", |p| p.parse_graph_node_path()),
            GraphNodePath::VarOrTerm(_)
        ));
    }

    #[test]
    fn error_empty_path() {
        insta::assert_snapshot!(render_path_err(""), @"
        error: expected a predicate or property path, found `end of input`
          ┌─ :1:1
          │
        1 │ 
          │ ^ expected a predicate or property path, found `end of input`
        ");
    }

    #[test]
    fn error_unbalanced_path_parens() {
        insta::assert_snapshot!(render_path_err("(ex:p"), @"
        error: expected `)`, found `end of input`
          ┌─ :1:6
          │
        1 │ (ex:p
          │      ^
          │      
          │      expected `)`, found `end of input`
          │      consider adding a `)` to close the path
        ");
    }

    #[test]
    fn error_unclosed_negated_set() {
        insta::assert_snapshot!(render_path_err("!(ex:p"), @"
        error: expected `)`, found `end of input`
          ┌─ :1:7
          │
        1 │ !(ex:p
          │       ^
          │       
          │       expected `)`, found `end of input`
          │       consider adding a `)` to close the path
        ");
    }

    fn render_path_err(input: &str) -> String {
        crate::render_internal(
            &match SparqlParser::new(input, default_registry()).parse_path() {
                Ok(_) => panic!("expected parse error for `{input}`"),
                Err(error) => error,
            },
            input,
        )
    }
}
