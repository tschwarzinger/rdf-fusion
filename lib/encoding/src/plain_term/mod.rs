mod array;
mod builder;
pub mod decoders;
mod element_builder;
pub mod encoders;
mod encoding;
mod quads;
mod scalar;

use crate::EncodingArray;
pub use array::*;
pub use builder::*;
pub use element_builder::*;
pub use encoding::*;
pub use quads::*;
pub use scalar::*;

/// Represents a non-empty list of [`PlainTermArray`]s of the same length. The arrays share the same
/// encoding.
pub struct PlainTermArrays {
    arrays: Vec<PlainTermArray>,
}

impl PlainTermArrays {
    /// Creates a new [PlainTermArrays] without validating invariants.
    pub fn new_unchecked(arrays: Vec<PlainTermArray>) -> Self {
        Self { arrays }
    }

    /// Returns the encoding.
    pub fn encoding(&self) -> &PlainTermEncodingRef {
        &PLAIN_TERM_ENCODING
    }

    /// Returns the number of arrays.
    pub fn len(&self) -> usize {
        self.arrays.len()
    }

    /// Returns true if the array is empty.
    pub fn is_empty(&self) -> bool {
        self.arrays.is_empty()
    }

    /// Returns the array at the given index.
    pub fn get(&self, index: usize) -> &PlainTermArray {
        &self.arrays[index]
    }

    /// Returns the number of rows in the arrays.
    pub fn number_rows(&self) -> usize {
        if self.arrays.is_empty() {
            0
        } else {
            self.arrays[0].inner().len()
        }
    }
}
