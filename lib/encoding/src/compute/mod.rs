//! Contains computations that mostly map between different encodings.
//!
//! This is not part of `rdf-fusion-compute` because otherwise this crate cannot depend on the
//! encoding crate.

mod with_plain_term_encoding;
mod with_string_encoding;

pub use with_plain_term_encoding::*;
pub use with_string_encoding::*;
