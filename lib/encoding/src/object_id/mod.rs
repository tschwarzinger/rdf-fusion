mod array;
mod encoding;
mod mapping;
mod scalar;

use crate::EncodingArray;
pub use array::*;
use datafusion::arrow::datatypes::DataType;
pub use encoding::*;
pub use mapping::*;
pub use scalar::*;
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// The data type of an object id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ObjectIdDataType {
    /// A 32-bit signed integer.
    Int32,
    /// A 64-bit signed integer.
    Int64,
    /// A fixed-size binary array of the given size in bytes.
    FixedSizeBinary(i32),
}

impl ObjectIdDataType {
    /// Returns the data type as an arrow [`DataType`].
    pub fn term_type(self) -> DataType {
        self.into()
    }
}

impl From<ObjectIdDataType> for DataType {
    fn from(value: ObjectIdDataType) -> Self {
        match value {
            ObjectIdDataType::Int32 => DataType::Int32,
            ObjectIdDataType::Int64 => DataType::Int64,
            ObjectIdDataType::FixedSizeBinary(size) => DataType::FixedSizeBinary(size),
        }
    }
}

impl From<ObjectIdDataType> for i32 {
    fn from(value: ObjectIdDataType) -> Self {
        match value {
            ObjectIdDataType::Int32 => 4,
            ObjectIdDataType::Int64 => 8,
            ObjectIdDataType::FixedSizeBinary(size) => size,
        }
    }
}

impl Display for ObjectIdDataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectIdDataType::Int32 => write!(f, "Int32"),
            ObjectIdDataType::Int64 => write!(f, "Int64"),
            ObjectIdDataType::FixedSizeBinary(size) => {
                write!(f, "FixedSizeBinary({size})")
            }
        }
    }
}

#[derive(Error, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[error("Invalid object id size.")]
pub struct ObjectIdCreationError;

/// Represents a non-empty list of [`PlainTermArray`]s of the same length. The arrays share the same
/// encoding.
pub struct ObjectIdArgs {
    arrays: Vec<ObjectIdArray>,
}

impl ObjectIdArgs {
    /// Creates a new [`ObjectIdArgs`] without validating invariants.
    pub fn new_unchecked(arrays: Vec<ObjectIdArray>) -> Self {
        Self { arrays }
    }

    /// Returns the encoding.
    pub fn encoding(&self) -> &ObjectIdEncodingRef {
        self.arrays[0].encoding()
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
    pub fn get(&self, index: usize) -> &ObjectIdArray {
        &self.arrays[index]
    }

    /// Returns the number of rows in the arrays.
    pub fn number_rows(&self) -> usize {
        self.arrays[0].inner().len()
    }
}
