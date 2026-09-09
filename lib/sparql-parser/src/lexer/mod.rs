//! Lexer for the SPARQL grammar.
//!
//! This lexer is based on the lexer of [spargebra](https://crates.io/crates/spargebra).

mod cursor;
mod token;

pub use cursor::*;
pub use token::*;

use crate::span::{Span, Spanned};
use logos::Logos;

/// Obtain a list of tokens from the input string. Errors are represented as [`Token::Error`].
pub fn lex_sparql(slice: &str) -> Vec<Spanned<Token<'_>>> {
    Token::lexer(slice)
        .spanned()
        .map(|(token, span)| {
            let crate_span = Span {
                start: span.start,
                end: span.end,
            };
            let token = token.unwrap_or_else(|()| Token::Error(&slice[span.clone()]));
            Spanned {
                value: token,
                span: crate_span,
            }
        })
        .collect()
}
