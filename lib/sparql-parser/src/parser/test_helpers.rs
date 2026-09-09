//! Shared test helpers for the parser submodules.
//!
//! This module is only compiled for tests (`#[cfg(test)]`). Submodule test
//! suites import it via `crate::parser::test_helpers::*`.

use crate::ast::{SparqlPrettyPrintable, pretty_printable};
use crate::error::SparqlSyntaxError;
use crate::parser::SparqlParser;
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
use rdf_fusion_extensions::functions::RdfFusionFunctionRegistryRef;
use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
use std::sync::Arc;

/// Returns a default function registry backed by a [`DefaultRdfFusionFunctionRegistry`].
pub fn default_registry() -> RdfFusionFunctionRegistryRef {
    let encodings = RdfFusionEncodings::new(
        Arc::clone(&PLAIN_TERM_ENCODING),
        Arc::new(TypedFamilyEncoding::default()),
        None,
        Arc::clone(&STRING_ENCODING),
    );
    Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings))
}

/// Runs `f` over `input`, expecting success and that every token is consumed.
pub fn parse<'a, T>(
    input: &'a str,
    f: impl FnOnce(&mut SparqlParser<'a>) -> Result<T, SparqlSyntaxError>,
) -> T {
    let mut parser = SparqlParser::new(input, default_registry());
    let value = f(&mut parser).unwrap();
    assert!(
        parser.at_end(),
        "parser did not consume all input: {}",
        parser.found()
    );
    value
}

/// Renders a value with the compact pretty-printer.
pub fn render<T: SparqlPrettyPrintable>(value: &T) -> String {
    format!("{}", pretty_printable(value))
}

/// Renders the error report for the error produced by `f` on `input`.
pub fn render_err(
    input: &str,
    f: impl FnOnce(&mut SparqlParser) -> SparqlSyntaxError,
) -> String {
    let error = f(&mut SparqlParser::new(input, default_registry()));
    crate::render_internal(&error, input)
}
