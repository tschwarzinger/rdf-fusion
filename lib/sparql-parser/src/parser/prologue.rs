//! Parsing for the SPARQL query/update `Prologue` (BASE and PREFIX
//! declarations), shared by both `Query` and `Update` entry points.
//!
//! ```text
//! Prologue   ::= ( BaseDecl | PrefixDecl )*
//! BaseDecl   ::= 'BASE' IRIREF
//! PrefixDecl ::= 'PREFIX' PNAME_NS IRIREF
//! ```

use crate::ast::PrologueDecl;
use crate::error::SparqlSyntaxError;
use crate::lexer::Token;
use crate::parser::SparqlParser;
use rdf_fusion_common::Iri;

impl<'a> SparqlParser<'a> {
    /// Parses a `Prologue`: any number of `BASE` and `PREFIX` declarations.
    ///
    /// ```text
    /// Prologue ::= ( BaseDecl | PrefixDecl )*
    /// ```
    pub fn parse_prologue(&mut self) -> Result<Vec<PrologueDecl<'a>>, SparqlSyntaxError> {
        let mut decls = Vec::new();
        loop {
            if self.parse_keyword("BASE") {
                let iri = self.parse_iriref()?;
                let unescaped = iri.value.as_ref();
                if let Some(resolved) = self.resolve_against_base(unescaped) {
                    self.base_iri = Some(resolved);
                }
                decls.push(PrologueDecl::Base(iri));
            } else if self.parse_keyword("PREFIX") {
                let prefix = self.parse_pname_ns()?;
                let iri = self.parse_iriref()?;
                let unescaped = iri.value.as_ref();
                if let Some(resolved) = self.resolve_against_base(unescaped) {
                    self.prefixes
                        .insert(prefix.to_string(), resolved.into_inner());
                }
                decls.push(PrologueDecl::Prefix(prefix, iri));
            } else {
                break;
            }
        }
        Ok(decls)
    }

    /// Resolves an unescaped IRI against the current base IRI, if any, into an
    /// absolute [`Iri`]. Returns `None` when there is no base IRI and the IRI is
    /// not absolute (or otherwise cannot be parsed).
    fn resolve_against_base(&self, unescaped: &str) -> Option<Iri<String>> {
        match &self.base_iri {
            Some(base) => base.resolve(unescaped).ok(),
            None => Iri::parse(unescaped.to_owned()).ok(),
        }
    }

    /// Parses a `PNAME_NS` (a bare namespace prefix such as `ex:`), returning
    /// the prefix without its trailing colon.
    ///
    /// ```text
    /// PNAME_NS ::= PN_PREFIX? ':'
    /// ```
    fn parse_pname_ns(&mut self) -> Result<&'a str, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(Token::PnameNs(full)) => {
                self.bump();
                Ok(full.strip_suffix(':').unwrap_or(full))
            }
            _ => self.expected("a PNAME_NS prefix"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::PrologueDecl;
    use crate::parser::SparqlParser;
    use crate::parser::test_helpers::*;

    fn parse_decls(input: &str) -> Vec<PrologueDecl<'_>> {
        parse(input, |p| p.parse_prologue())
    }

    #[test]
    fn base_and_prefix_decls() {
        assert_eq!(
            parse_decls("BASE <http://example.org/>").len(),
            1,
            "a single BASE declaration"
        );
        assert_eq!(
            parse_decls("PREFIX ex: <http://example.org/ns/>").len(),
            1,
            "a single PREFIX declaration"
        );
        assert_eq!(
            parse_decls("BASE <http://example.org/> PREFIX ex: <ns/>").len(),
            2,
            "BASE followed by PREFIX"
        );
        assert_eq!(parse_decls("").len(), 0, "empty prologue");
    }

    #[test]
    fn malformed_escape_is_reported() {
        // A malformed `\u`/`\U` escape must fail at the declaration, not be
        // silently swallowed into an "unknown prefix" error later.
        for input in [
            "PREFIX ex: <http://example.org/\\uZZZZ>",
            "BASE <http://example.org/\\UZZZZZZZZ>",
        ] {
            let mut p = SparqlParser::new(input, default_registry());
            assert!(
                p.parse_prologue().is_err(),
                "expected error for malformed escape in: {input}"
            );
        }
    }
}
