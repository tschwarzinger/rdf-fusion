use crate::lexer::Token;
use crate::span::Spanned;

/// A cursor over a stream of [`Token`]s produced by [`lex_sparql`](crate::lex_sparql).
///
/// The lexer already discards whitespace and comments, so the cursor operates
/// directly on significant tokens.
#[derive(Debug, Clone)]
pub struct TokenCursor<'a> {
    tokens: Vec<Spanned<Token<'a>>>,
    index: usize,
}

impl<'a> TokenCursor<'a> {
    /// Creates a cursor over the given tokens.
    pub fn new(tokens: Vec<Spanned<Token<'a>>>) -> Self {
        Self { tokens, index: 0 }
    }

    /// Returns the index of the first unprocessed token.
    pub fn position(&self) -> usize {
        self.index
    }

    /// Returns `true` if all tokens have been consumed.
    pub fn at_end(&self) -> bool {
        self.index >= self.tokens.len()
    }

    /// Returns a reference to the current (first unprocessed) token, if any.
    pub fn peek(&self) -> Option<&Spanned<Token<'a>>> {
        self.tokens.get(self.index)
    }

    /// Returns a reference to the `n`th unprocessed token (0 = current), if any.
    pub fn peek_nth(&self, n: usize) -> Option<&Spanned<Token<'a>>> {
        self.tokens.get(self.index + n)
    }

    /// Consumes and returns the current token.
    pub fn bump(&mut self) -> Option<Spanned<Token<'a>>> {
        let token = self.tokens.get(self.index).copied();
        if token.is_some() {
            self.index += 1;
        }
        token
    }

    /// Returns a reference to the previously consumed token, if any.
    pub fn previous(&self) -> Option<&Spanned<Token<'a>>> {
        self.index
            .checked_sub(1)
            .and_then(|index| self.tokens.get(index))
    }
}
