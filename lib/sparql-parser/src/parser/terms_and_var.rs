use crate::Diagnostic;
use crate::ast::{
    BlankNode, Iri, IriRef, Literal, PrefixedName, String as LString, Var, VarOrTerm,
};
use crate::error::SparqlSyntaxError;
use crate::lexer::Token;
use crate::parser::SparqlParser;
use crate::parser::encoding::{unescape_iriref, unescape_local_name, unescape_string};
use crate::span::{Span, Spanned};
use logos::Source;
use std::borrow::Cow;

impl<'a> SparqlParser<'a> {
    /// Parses a `VarOrTerm`.
    ///
    /// ```text
    /// VarOrTerm ::= Var | GraphTerm
    /// ```
    pub fn parse_var_or_term(&mut self) -> Result<VarOrTerm<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(v) if v.is_var() => Ok(VarOrTerm::Var(
                self.parse_var().expect("peeked a variable token"),
            )),
            _ => {
                // If the current token starts a term, parse it and propagate any
                // term-specific error (e.g. an invalid unescaped unicode codepoint).
                // Only a token that is not a term/variable at all yields the generic
                // "a term or variable" diagnostic.
                let is_term_start = self
                    .peek()
                    .map(|token| {
                        let value = token.value;
                        value.is_string()
                            || value.is_iri()
                            || value.is_numeric_literal()
                            || value.is_boolean()
                            || value.is_blank_node()
                            || (matches!(value, Token::Operator("(")) && self.is_nil())
                    })
                    .unwrap_or(false);
                if is_term_start {
                    self.parse_graph_term()
                } else {
                    self.expected("a term or variable")
                }
            }
        }
    }

    /// Parses a `Var`.
    ///
    /// ```text
    /// Var ::= VAR1 | VAR2
    /// ```
    pub fn parse_var(&mut self) -> Result<Var<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(Token::Var1(name)) | Some(Token::Var2(name)) => {
                let token = self.bump().unwrap();
                Ok(Var {
                    value: &name[1..],
                    span: token.span,
                })
            }
            _ => self.expected("a variable"),
        }
    }

    /// Parses a `GraphTerm`.
    ///
    /// ```text
    /// GraphTerm ::= iri | RDFLiteral | NumericLiteral | BooleanLiteral | BlankNode | NIL
    /// ```
    pub fn parse_graph_term(&mut self) -> Result<VarOrTerm<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(v) if v.is_iri() => Ok(VarOrTerm::Iri(
                self.parse_iri().expect("peeked an iri token"),
            )),
            Some(v) if v.is_string() => Ok(VarOrTerm::Literal(self.parse_rdf_literal()?)),
            Some(v) if v.is_numeric_literal() => Ok(VarOrTerm::Literal(
                self.parse_numeric_literal()
                    .expect("peeked a numeric literal token"),
            )),
            Some(v) if v.is_boolean() => Ok(VarOrTerm::Literal(
                self.parse_boolean_literal()
                    .expect("peeked a boolean literal"),
            )),
            Some(v) if v.is_blank_node() => Ok(VarOrTerm::BlankNode(
                self.parse_blank_node().expect("peeked a blank node token"),
            )),
            Some(Token::Operator("(")) if self.is_nil() => self.parse_nil(),
            _ => self.expected("a term"),
        }
    }

    /// Returns `true` if the next two tokens form a `NIL` (`'(' WS* ')'`).
    pub fn is_nil(&self) -> bool {
        self.peek_nth(1)
            .map(|token| matches!(token.value, Token::Operator(")")))
            .unwrap_or(false)
    }

    /// Parses a `NIL`, returning [`VarOrTerm::Nil`].
    ///
    /// ```text
    /// NIL ::= '(' WS* ')'
    /// ```
    pub fn parse_nil(&mut self) -> Result<VarOrTerm<'a>, SparqlSyntaxError> {
        let current = self.peek().map(|token| token.value);
        let next = self.peek_nth(1).map(|token| token.value);
        if matches!(current, Some(Token::Operator("(")))
            && matches!(next, Some(Token::Operator(")")))
        {
            self.bump();
            self.bump();
            Ok(VarOrTerm::Nil)
        } else {
            self.expected("nil '(' followed by ')'")
        }
    }

    /// Parses an `iri` (either an `IRIREF` or a prefixed name).
    ///
    /// ```text
    /// iri ::= IRIREF | PrefixedName
    /// ```
    pub fn parse_iri(&mut self) -> Result<Iri<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(v) if v.is_iriref() => Ok(Iri::IriRef(
                self.parse_iriref().expect("peeked an IRI reference"),
            )),
            Some(v) if v.is_prefixed_name() => Ok(Iri::PrefixedName(
                self.parse_prefixed_name().expect("peeked a prefixed name"),
            )),
            _ => self.expected("an IRI"),
        }
    }

    /// Parses an `IRIREF` (a `'<' ... '>'` IRI reference).
    ///
    /// This method only accepts a [`Token::IriRef`].
    pub fn parse_iriref(&mut self) -> Result<IriRef<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(Token::IriRef(iri)) => {
                let token = self.bump().unwrap();
                let inner = iri
                    .slice(1..iri.len() - 1)
                    .expect("IRI references always contain <>");
                let value = unescape_iriref(inner).map_err(|e| {
                    SparqlSyntaxError::single(Diagnostic::error(
                        token.span,
                        e.to_string(),
                    ))
                })?;
                Ok(IriRef {
                    value,
                    span: token.span,
                })
            }
            _ => self.expected("an IRI reference"),
        }
    }

    /// Parses a `PrefixedName` (either `PNAME_LN` or `PNAME_NS`).
    ///
    /// Prefixed names only occur within an `iri` in the grammar, so this is an
    /// internal helper rather than a standalone entry point.
    ///
    /// ```text
    /// PrefixedName ::= PNAME_LN | PNAME_NS
    /// ```
    fn parse_prefixed_name(&mut self) -> Result<PrefixedName<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(Token::PnameLn(full)) => {
                let token = self.bump().unwrap();
                let (namespace, local) = full.split_once(':').unwrap_or((full, ""));
                let (local, _) = unescape_local_name(local);
                Ok(PrefixedName {
                    namespace,
                    local,
                    span: token.span,
                })
            }
            Some(Token::PnameNs(full)) => {
                let token = self.bump().unwrap();
                Ok(PrefixedName {
                    namespace: &full[..full.len() - 1],
                    local: "".into(),
                    span: token.span,
                })
            }
            _ => self.expected("a prefixed name"),
        }
    }

    /// Parses a `RDFLiteral`: a string optionally followed by a language tag or
    /// a `'^^' iri` datatype.
    ///
    /// ```text
    /// RDFLiteral ::= String ( LANGTAG | ( '^^' iri ) )?
    /// ```
    pub fn parse_rdf_literal(&mut self) -> Result<Literal<'a>, SparqlSyntaxError> {
        let string = self.parse_string()?;
        let lang = self.peek().and_then(|next| match next.value {
            Token::LangDir(lang) => Some((&lang[1..], next.span)),
            _ => None,
        });
        if let Some((lang, lang_span)) = lang {
            self.bump();
            Ok(Literal::LangString(
                string,
                Spanned {
                    value: lang,
                    span: lang_span,
                },
            ))
        } else if self.consume_operator("^^") {
            let iri = self.parse_iri()?;
            Ok(Literal::Typed(string, iri))
        } else {
            Ok(Literal::String(string))
        }
    }

    /// Parses a `String` literal (any of the four string-literal lexemes).
    ///
    /// ```text
    /// String ::= STRING_LITERAL1 | STRING_LITERAL2 | STRING_LITERAL_LONG1 | STRING_LITERAL_LONG2
    /// ```
    pub fn parse_string(&mut self) -> Result<LString<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(Token::StringLiteral1(s)) | Some(Token::StringLiteral2(s)) => {
                let token = self.bump().unwrap();
                let inner = s
                    .slice(1..s.len() - 1)
                    .expect("String literals always contain wrapping quotes");
                let value = unescape_string(inner).map_err(|e| {
                    SparqlSyntaxError::single(Diagnostic::error(
                        token.span,
                        e.to_string(),
                    ))
                })?;
                Ok(LString {
                    value,
                    span: token.span,
                })
            }
            Some(Token::StringLiteralLong1(s)) | Some(Token::StringLiteralLong2(s)) => {
                let token = self.bump().unwrap();
                let inner = s
                    .slice(3..s.len() - 3)
                    .expect("Long string literals always contain wrapping quotes");
                let value = unescape_string(inner).map_err(|e| {
                    SparqlSyntaxError::single(Diagnostic::error(
                        token.span,
                        e.to_string(),
                    ))
                })?;
                Ok(LString {
                    value,
                    span: token.span,
                })
            }
            _ => self.expected("a string literal"),
        }
    }

    /// Parses a `NumericLiteral`.
    ///
    /// ```text
    /// NumericLiteral ::= NumericLiteralUnsigned | NumericLiteralPositive | NumericLiteralNegative
    /// ```
    pub fn parse_numeric_literal(&mut self) -> Result<Literal<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(Token::Integer(n))
            | Some(Token::IntegerPositive(n))
            | Some(Token::IntegerNegative(n)) => {
                let token = self.bump().unwrap();
                Ok(Literal::Integer(
                    Spanned {
                        value: n.parse().map_err(|e| {
                            SparqlSyntaxError::single(Diagnostic::error(
                                token.span,
                                format!("Invalid integer literal: {e}"),
                            ))
                        })?,
                        span: token.span,
                    },
                    Cow::Borrowed(n),
                ))
            }
            Some(Token::Decimal(n))
            | Some(Token::DecimalPositive(n))
            | Some(Token::DecimalNegative(n)) => {
                let token = self.bump().unwrap();
                Ok(Literal::Decimal(
                    Spanned {
                        value: n.parse().map_err(|e| {
                            SparqlSyntaxError::single(Diagnostic::error(
                                token.span,
                                format!("Invalid decimal literal: {e}"),
                            ))
                        })?,
                        span: token.span,
                    },
                    Cow::Borrowed(n),
                ))
            }
            Some(Token::Double(n))
            | Some(Token::DoublePositive(n))
            | Some(Token::DoubleNegative(n)) => {
                let token = self.bump().unwrap();
                Ok(Literal::Double(
                    Spanned {
                        value: n.parse().map_err(|e| {
                            SparqlSyntaxError::single(Diagnostic::error(
                                token.span,
                                format!("Invalid double literal: {e}"),
                            ))
                        })?,
                        span: token.span,
                    },
                    Cow::Borrowed(n),
                ))
            }
            _ => self.expected("a numeric literal"),
        }
    }

    /// Parses a `BooleanLiteral`.
    ///
    /// ```text
    /// BooleanLiteral ::= 'true' | 'false'
    /// ```
    pub fn parse_boolean_literal(&mut self) -> Result<Literal<'a>, SparqlSyntaxError> {
        let span = self.current_span();
        if self.parse_keyword("true") {
            Ok(Literal::Boolean(Spanned { value: true, span }))
        } else if self.parse_keyword("false") {
            Ok(Literal::Boolean(Spanned { value: false, span }))
        } else {
            self.expected("a boolean literal")
        }
    }

    /// Parses a `BlankNode`, either a labeled blank node or an anonymous one
    /// (`ANON ::= '[' WS* ']'`, represented as [`BlankNode(None)`](BlankNode)).
    ///
    /// ```text
    /// BlankNode ::= BLANK_NODE_LABEL | ANON
    /// ```
    pub fn parse_blank_node(
        &mut self,
    ) -> Result<Spanned<BlankNode<'a>>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(Token::BlankNodeLabel(label)) => {
                let token = self.bump().unwrap();
                Ok(Spanned {
                    value: BlankNode(Some(&label[2..])),
                    span: token.span,
                })
            }
            Some(Token::Operator("[")) => {
                let open = self.bump().unwrap();
                self.expect_operator("]")?;
                let end = self
                    .previous()
                    .map(|token| token.span.end)
                    .unwrap_or(open.span.end);
                Ok(Spanned {
                    value: BlankNode(None),
                    span: Span::new(open.span.start, end),
                })
            }
            _ => self.expected("a blank node"),
        }
    }

    /// Parses any literal (`RDFLiteral`, `NumericLiteral`, or
    /// `BooleanLiteral`) into the AST [`Literal`] type.
    ///
    /// ```text
    /// Literal := RDFLiteral | NumericLiteral | BooleanLiteral
    /// ```
    pub fn parse_literal(&mut self) -> Result<Literal<'a>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(v) if v.is_string() => self.parse_rdf_literal(),
            Some(v) if v.is_numeric_literal() => Ok(self
                .parse_numeric_literal()
                .expect("peeked a numeric literal token")),
            Some(v) if v.is_boolean() => Ok(self
                .parse_boolean_literal()
                .expect("peeked a boolean literal")),
            _ => self.expected("a literal"),
        }
    }

    /// Resolves an AST [`Iri`] into a full IRI string using prologue prefixes and base IRI.
    pub fn resolve_iri_string(&self, iri: &Iri<'a>) -> Result<String, SparqlSyntaxError> {
        match iri {
            Iri::IriRef(iriref) => {
                let value = iriref.value.as_ref();
                if let Some(base) = &self.base_iri {
                    let resolved = base.resolve(value).map_err(|e| {
                        SparqlSyntaxError::single(Diagnostic::error(
                            iriref.span,
                            format!("Invalid IRI '{value}': {e}"),
                        ))
                    })?;
                    Ok(resolved.as_str().to_string())
                } else {
                    Ok(iriref.value.clone().into_owned())
                }
            }
            Iri::PrefixedName(prefixed) => {
                let prefix = prefixed.namespace;
                let local_name = prefixed.local.as_ref();
                if let Some(prefix_iri) = self.prefixes.get(prefix) {
                    let mut resolved = prefix_iri.clone();
                    resolved.push_str(local_name);
                    Ok(resolved)
                } else {
                    Err(SparqlSyntaxError::single(Diagnostic::error(
                        prefixed.span,
                        format!("Prefix '{prefix}' not found"),
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{BlankNode, Literal, VarOrTerm};
    use crate::parser::test_helpers::*;

    #[test]
    fn var_question_and_dollar() {
        assert_eq!(render(&parse("?x", |p| p.parse_var())), "?x");
        assert_eq!(render(&parse("$y", |p| p.parse_var())), "?y");
    }

    #[test]
    fn iri_ref() {
        assert_eq!(
            render(&parse("<http://example.org/s>", |p| p.parse_iri())),
            "<http://example.org/s>"
        );
    }

    #[test]
    fn iri_prefixed_name() {
        assert_eq!(render(&parse("ex:p", |p| p.parse_iri())), "ex:p");
    }

    #[test]
    fn iri_prefixed_default_ns() {
        assert_eq!(render(&parse(":local", |p| p.parse_iri())), ":local");
    }

    #[test]
    fn iri_prefixed_name_ns_only() {
        assert_eq!(render(&parse("ex:", |p| p.parse_iri())), "ex:");
    }

    #[test]
    fn blank_node_label() {
        assert_eq!(render(&parse("_:b", |p| p.parse_blank_node()).value), "_:b");
    }

    #[test]
    fn blank_node_anon() {
        assert!(matches!(
            parse("[]", |p| p.parse_blank_node()).value,
            BlankNode(None)
        ));
        assert!(matches!(
            parse("[]", |p| p.parse_graph_term()),
            VarOrTerm::BlankNode(_)
        ));
        assert!(matches!(
            parse("[]", |p| p.parse_var_or_term()),
            VarOrTerm::BlankNode(_)
        ));
    }

    #[test]
    fn graph_term_nil() {
        assert!(matches!(
            parse("()", |p| p.parse_graph_term()),
            VarOrTerm::Nil
        ));
        assert!(matches!(
            parse("( )", |p| p.parse_graph_term()),
            VarOrTerm::Nil
        ));
        assert!(matches!(
            parse("()", |p| p.parse_var_or_term()),
            VarOrTerm::Nil
        ));
    }

    #[test]
    fn literal_boolean() {
        assert_eq!(render(&parse("true", |p| p.parse_literal())), "true");
        assert_eq!(render(&parse("false", |p| p.parse_literal())), "false");
        assert_eq!(render(&parse("TRUE", |p| p.parse_literal())), "true");
        assert_eq!(render(&parse("FALSE", |p| p.parse_literal())), "false");
    }

    #[test]
    fn literal_integer() {
        assert_eq!(render(&parse("42", |p| p.parse_literal())), "42");
        assert_eq!(render(&parse("+42", |p| p.parse_literal())), "+42");
        assert_eq!(render(&parse("-42", |p| p.parse_literal())), "-42");
    }

    #[test]
    fn literal_decimal() {
        assert_eq!(render(&parse("3.14", |p| p.parse_literal())), "3.14");
        assert_eq!(render(&parse("+3.14", |p| p.parse_literal())), "+3.14");
        assert_eq!(render(&parse("-3.14", |p| p.parse_literal())), "-3.14");
    }

    #[test]
    fn literal_double() {
        assert_eq!(render(&parse("1e3", |p| p.parse_literal())), "1e3");
        assert_eq!(render(&parse("+1e3", |p| p.parse_literal())), "+1e3");
        assert_eq!(render(&parse("-1e3", |p| p.parse_literal())), "-1e3");
    }

    #[test]
    fn literal_string_single_and_double_quotes() {
        assert_eq!(render(&parse("'hi'", |p| p.parse_literal())), r#""hi""#);
        assert_eq!(render(&parse("\"hi\"", |p| p.parse_literal())), r#""hi""#);
        assert_eq!(render(&parse("'''hi'''", |p| p.parse_literal())), r#""hi""#);
        assert_eq!(
            render(&parse("\"\"\"hi\"\"\"", |p| p.parse_literal())),
            r#""hi""#
        );
    }

    #[test]
    fn literal_language_tagged() {
        let lit = parse("\"hi\"@en", |p| p.parse_literal());
        assert_eq!(render(&lit), r#""hi"@en"#);
        if let Literal::LangString(_, lang) = lit {
            assert_eq!(lang.value, "en");
        } else {
            panic!("Expected LangString");
        }
    }

    #[test]
    fn literal_typed() {
        assert_eq!(
            render(&parse("\"4\"^^<xsd:int>", |p| p.parse_literal())),
            r#""4"^^<xsd:int>"#
        );
        assert_eq!(
            render(&parse("\"4\"^^xsd:int", |p| p.parse_literal())),
            r#""4"^^xsd:int"#
        );
    }

    #[test]
    fn var_or_term_kinds() {
        assert_eq!(render(&parse("?x", |p| p.parse_var_or_term())), "?x");
        assert_eq!(render(&parse("<s>", |p| p.parse_var_or_term())), "<s>");
        assert_eq!(render(&parse("42", |p| p.parse_var_or_term())), "42");
        assert_eq!(render(&parse("-42", |p| p.parse_var_or_term())), "-42");
        assert_eq!(render(&parse("_:b", |p| p.parse_var_or_term())), "_:b");
    }

    #[test]
    fn error_iri_at_end() {
        insta::assert_snapshot!(render_err("", |p| p.parse_iri().unwrap_err()), @"
        error: expected an IRI, found `end of input`
          ┌─ :1:1
          │
        1 │ 
          │ ^ expected an IRI, found `end of input`
        ");
    }

    #[test]
    fn error_var_or_term_reserved_word() {
        insta::assert_snapshot!(render_err("SELECT", |p| p.parse_var_or_term().unwrap_err()), @"
        error: expected a term or variable, found `SELECT`
          ┌─ :1:1
          │
        1 │ SELECT
          │ ^^^^^^ expected a term or variable, found `SELECT`
        ");
    }

    #[test]
    fn error_graph_term_non_boolean_keyword() {
        insta::assert_snapshot!(
            render_err("SELECT", |p| p.parse_graph_term().unwrap_err()),
            @"
        error: expected a term, found `SELECT`
          ┌─ :1:1
          │
        1 │ SELECT
          │ ^^^^^^ expected a term, found `SELECT`
        "
        );
    }

    #[test]
    fn error_graph_term_operator() {
        insta::assert_snapshot!(render_err("+", |p| p.parse_graph_term().unwrap_err()), @"
        error: expected a term, found `+`
          ┌─ :1:1
          │
        1 │ +
          │ ^ expected a term, found `+`
        ");
    }
}
