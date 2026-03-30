mod array;
mod encoding;
mod mapping;
mod scalar;

use crate::EncodingArray;
pub use array::*;
use datafusion::arrow::array::{Array, FixedSizeBinaryArray};
pub use encoding::*;
pub use mapping::*;
pub use scalar::*;
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// The size of an object id in bytes.
///
/// An `i32` is used for the size as this is used by Arrow. The length will always be greater than
/// zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectIdSize(i32);

#[derive(Error, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[error("Invalid object id size.")]
pub struct ObjectIdCreationError;

impl TryFrom<i32> for ObjectIdSize {
    type Error = ObjectIdCreationError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value > 0 {
            Ok(Self(value))
        } else {
            Err(ObjectIdCreationError)
        }
    }
}

impl From<ObjectIdSize> for i32 {
    fn from(value: ObjectIdSize) -> Self {
        value.0
    }
}

impl TryFrom<usize> for ObjectIdSize {
    type Error = ObjectIdCreationError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        i32::try_from(value)
            .map(Self)
            .map_err(|_| ObjectIdCreationError)
    }
}

impl From<ObjectIdSize> for usize {
    fn from(value: ObjectIdSize) -> Self {
        value.0 as usize // This works because non-negativity is checked in the constructor
    }
}

impl Display for ObjectIdSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} Bytes", self.0)
    }
}

/// An object id that is not yet related to any [`ObjectIdEncoding`]. For an object id that is
/// related to a specific encoding see [`ObjectIdScalar`].
///
/// This struct guarantees that the slice length fits into a non-negative `i32`.
///
/// # Default Graph
///
/// The default graph is represented as `None` in the underlying [`Option<Box<[u8]>>`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId(Option<Box<[u8]>>);

impl ObjectId {
    /// Creates a new [`ObjectId`].
    ///
    /// # Errors
    ///
    /// Returns an error if the slice length does not fit in an `i32`.
    pub fn try_new(bytes: impl Into<Box<[u8]>>) -> Result<Self, ObjectIdCreationError> {
        let bytes = bytes.into();
        i32::try_from(bytes.len()).map_err(|_| ObjectIdCreationError)?;
        Ok(Self(Some(bytes)))
    }

    /// Creates a new [`ObjectId`] for the default graph.
    pub fn new_default_graph() -> Self {
        Self(None)
    }

    /// Returns true if the object id is the default graph.
    pub fn is_default_graph(&self) -> bool {
        self.0.is_none()
    }

    /// Creates a new [`ObjectId`] from the given `array` at `index`.
    ///
    /// # Panics
    ///
    /// Panics if the given index is out-of-range.
    pub fn from_array_at_index(array: &FixedSizeBinaryArray, index: usize) -> Self {
        match array.is_valid(index) {
            true => Self(Some(array.value(index).into())),
            false => Self(None),
        }
    }

    /// Returns a reference to the underlying bytes.
    ///
    /// Returns `None` if the object id represents the default graph.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.0.as_deref()
    }
}

/// Represents a non-empty list of [`ObjectIdArray`]s of the same length. The arrays share the same
/// encoding.
pub struct ObjectIdArrays {
    arrays: Vec<ObjectIdArray>,
}

impl ObjectIdArrays {
    /// Creates a new [ObjectIdArrays] without validating invariants. See [`ObjectIdArrays`] for
    /// details.
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
}
