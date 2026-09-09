//! Contains the SPARQL parsing framework of RDF Fusion.

mod error;
mod lexer;
mod options;
mod span;

pub mod ast;
pub mod parser;
pub mod rewriter;

pub use crate::rewriter::{QueryRewriter, UpdateRewriter};
pub use error::*;
pub use lexer::*;
pub use options::*;
pub use parser::SparqlParser;
pub use span::*;

use rdf_fusion_common::sparql::{RdfFusionQuery, RdfFusionUpdate};
use rdf_fusion_extensions::RdfFusionContextView;
use std::sync::Arc;

/// Parses `text` as a SPARQL query and rewrites it into an [`RdfFusionQuery`].
///
/// # Errors
///
/// Returns a [`SparqlParseError`] if `text` is not a valid query or if the query
/// cannot be rewritten into a logical plan.
pub fn parse_query(
    ctx: &RdfFusionContextView,
    text: &str,
    config: &ParserOptions,
) -> Result<RdfFusionQuery, SparqlParseError> {
    let mut parser = SparqlParser::new(text, Arc::clone(ctx.functions()));
    let ast = parser.parse_query()?;
    QueryRewriter::new(ctx.clone()).rewrite(&ast, config)
}

/// Parses `text` as a SPARQL update and rewrites it into an [`RdfFusionUpdate`].
///
/// # Errors
///
/// Returns a [`SparqlParseError`] if `text` is not a valid update or if the update
/// cannot be rewritten into a list of update operations.
pub fn parse_update(
    ctx: &RdfFusionContextView,
    text: &str,
    config: &ParserOptions,
) -> Result<RdfFusionUpdate, SparqlParseError> {
    let mut parser = SparqlParser::new(text, Arc::clone(ctx.functions()));
    let ast = parser.parse_update()?;
    UpdateRewriter::new(ctx.clone()).rewrite(&ast, config)
}
