mod encoding;
mod expr;
mod graph_patterns;
mod prologue;
mod property_paths;
mod query;
mod terms_and_var;
mod triples;
mod update;

#[cfg(test)]
mod test_helpers;

use crate::lexer::{Token, TokenCursor, lex_sparql};
use crate::span::{Span, Spanned};
use crate::{Diagnostic, SparqlSyntaxError};
use rdf_fusion_common::Iri;
use rdf_fusion_extensions::functions::RdfFusionFunctionRegistryRef;
use std::collections::HashMap;

/// A SPARQL parser for a single query.
pub struct SparqlParser<'a> {
    /// The raw source text. The tokens only point into this string, thus requiring the lifetime.
    text: &'a str,
    /// The token cursor.
    cursor: TokenCursor<'a>,
    /// Function registry used to recognize scalar and aggregate functions.
    registry: RdfFusionFunctionRegistryRef,
    /// Prefixes declared in the prologue.
    prefixes: HashMap<String, String>,
    /// Base IRI declared in the prologue.
    base_iri: Option<Iri<String>>,
}

impl<'a> SparqlParser<'a> {
    /// Creates a new [`SparqlParser`] for `text`, lexing it up front. The
    /// `registry` is used to recognize scalar and aggregate function calls.
    pub fn new(text: &'a str, registry: RdfFusionFunctionRegistryRef) -> Self {
        let tokens = lex_sparql(text);
        Self {
            text,
            cursor: TokenCursor::new(tokens),
            registry,
            prefixes: HashMap::new(),
            base_iri: None,
        }
    }
}

impl<'a> SparqlParser<'a> {
    /// Returns a reference to the current (first unprocessed) token, if any.
    pub fn peek(&self) -> Option<&Spanned<Token<'a>>> {
        self.cursor.peek()
    }

    /// Returns the `n`th unprocessed token (0 = current), if any.
    pub fn peek_nth(&self, n: usize) -> Option<&Spanned<Token<'a>>> {
        self.cursor.peek_nth(n)
    }

    /// Returns `true` if all tokens have been consumed.
    pub fn at_end(&self) -> bool {
        self.cursor.at_end()
    }

    /// Returns the index of the first unprocessed token.
    pub fn position(&self) -> usize {
        self.cursor.position()
    }

    /// Consumes and returns the current token.
    pub fn bump(&mut self) -> Option<Spanned<Token<'a>>> {
        self.cursor.bump()
    }

    /// Returns a reference to the previously consumed token, if any.
    pub fn previous(&self) -> Option<&Spanned<Token<'a>>> {
        self.cursor.previous()
    }

    /// The length of the source text.
    pub fn source_len(&self) -> usize {
        self.text.len()
    }

    /// The span of the current token, or an empty span at end of input.
    pub fn current_span(&self) -> Span {
        self.peek()
            .map(|token| token.span)
            .unwrap_or_else(|| Span::at(self.text.len()))
    }

    /// A human-readable description of the current token.
    fn found(&self) -> String {
        self.peek()
            .map(|token| token.value.to_string())
            .unwrap_or_else(|| "end of input".to_string())
    }

    /// Returns `true` if the current token is the keyword `kw`.
    pub fn peek_keyword(&self, kw: &str) -> bool {
        matches!(
            self.peek(),
            Some(Spanned {
                value: Token::Keyword(found),
                ..
            }) if found.eq_ignore_ascii_case(kw)
        )
    }

    /// Returns `true` if the keyword `kw` appears at `offset` (0 = current).
    pub fn peek_keyword_at(&self, offset: usize, kw: &str) -> bool {
        matches!(
            self.peek_nth(offset),
            Some(Spanned {
                value: Token::Keyword(found),
                ..
            }) if found.eq_ignore_ascii_case(kw)
        )
    }

    /// Consumes the keyword `kw` if it is the current token.
    pub fn parse_keyword(&mut self, kw: &str) -> bool {
        if self.peek_keyword(kw) {
            self.bump();
            true
        } else {
            false
        }
    }

    /// Consumes the keyword `kw`, erroring if it is not the current token.
    pub fn expect_keyword(&mut self, kw: &str) -> Result<(), SparqlSyntaxError> {
        if self.parse_keyword(kw) {
            Ok(())
        } else {
            self.expected(kw)
        }
    }

    /// Returns `true` if the current token is the operator `op`.
    pub fn peek_operator(&self, op: &str) -> bool {
        matches!(
            self.peek(),
            Some(Spanned {
                value: Token::Operator(found),
                ..
            }) if *found == op
        )
    }

    /// Consumes the operator `op` if it is the current token.
    pub fn consume_operator(&mut self, op: &str) -> bool {
        if self.peek_operator(op) {
            self.bump();
            true
        } else {
            false
        }
    }

    /// Consumes the operator `op`, erroring if it is not the current token.
    pub fn expect_operator(&mut self, op: &str) -> Result<(), SparqlSyntaxError> {
        if self.consume_operator(op) {
            Ok(())
        } else {
            self.expected(op)
        }
    }

    /// Returns `true` if the current token terminates a property list.
    pub(crate) fn is_group_end(&self) -> bool {
        self.at_end()
            || self.peek_operator(".")
            || self.peek_operator("}")
            || self.peek_operator("]")
            || self.peek_operator(")")
    }

    /// Returns `true` if the current token terminates an object list.
    pub(crate) fn is_object_list_end(&self) -> bool {
        self.at_end()
            || self.peek_operator(".")
            || self.peek_operator(";")
            || self.peek_operator("}")
            || self.peek_operator("]")
            || self.peek_operator(")")
    }

    /// Builds a `Result::Err` reporting that `what` was expected but something
    /// else was found at the current token.
    pub fn expected<T>(&self, what: &str) -> Result<T, SparqlSyntaxError> {
        Err(SparqlSyntaxError::single(Diagnostic::error(
            self.current_span(),
            format!("expected {what}, found `{}`", self.found()),
        )))
    }
}
