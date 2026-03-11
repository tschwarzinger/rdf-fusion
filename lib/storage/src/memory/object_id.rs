#![allow(clippy::unreadable_literal)]

use crate::index::EncodedTerm;
use datafusion::parquet::data_type::AsBytes;
use rdf_fusion_encoding::object_id::ObjectId;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use thiserror::Error;

const SIZE: u8 = 4;

/// The encoded object id represents an [ObjectId] in the storage layer.
///
/// # Default Graph
///
/// The default graph is represented by the [DEFAULT_GRAPH_ID]. Use [EncodedGraphObjectId] to
/// indicate that an id may represent the default graph. If the id is not a [EncodedGraphObjectId],
/// the system assumes that the id cannot represent the default graph and errors may be thrown
/// during decoding.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct EncodedObjectId([u8; SIZE as usize]);

impl EncodedObjectId {
    pub const SIZE: u8 = 4;
    pub const SIZE_I32: i32 = SIZE as i32;
    pub const MIN: EncodedObjectId = EncodedObjectId([0; 4]);
    pub const MAX: EncodedObjectId = EncodedObjectId([255; 4]);

    /// Creates a new [`EncodedObjectId`] from a byte slice that is guaranteed to be 4 bytes long.
    ///
    /// # Panics
    ///
    /// Will panic if the slice is not 4 bytes long.
    pub fn from_4_byte_slice(slice: &[u8]) -> Self {
        EncodedObjectId::try_from(slice).expect("Object id size checked in try_new.")
    }

    /// Returns whether this object id represents the default graph.
    pub fn is_default_graph(&self) -> bool {
        *self == DEFAULT_GRAPH_ID
    }

    /// Returns a [`ObjectId`] from this encoded id.
    pub fn as_object_id(&self) -> ObjectId {
        if self.is_default_graph() {
            ObjectId::new_default_graph()
        } else {
            ObjectId::try_new(self.0).expect("Object ID valid")
        }
    }

    /// Returns the bytes within the encoded object id.
    pub fn as_bytes(&self) -> [u8; 4] {
        self.0
    }

    /// Returns the next object id after this one.
    ///
    /// Returns [`None`] if `self` is the last object id.
    pub fn next(&self) -> Option<EncodedObjectId> {
        self.as_u32()
            .checked_add(1)
            .map(u32::to_be_bytes)
            .map(EncodedObjectId)
    }

    /// Returns the previous object id before this one.
    ///
    /// Returns [`None`] if `self` is the first object id.
    pub fn previous(&self) -> Option<EncodedObjectId> {
        self.as_u32()
            .checked_sub(1)
            .map(u32::to_be_bytes)
            .map(EncodedObjectId)
    }

    /// Returns the a u32 value.
    fn as_u32(&self) -> u32 {
        u32::from_be_bytes(self.0)
    }
}

impl EncodedTerm for EncodedObjectId {
    fn is_default_graph(&self) -> bool {
        *self == DEFAULT_GRAPH_ID
    }
}

impl From<ObjectId> for EncodedObjectId {
    fn from(value: ObjectId) -> Self {
        match value.as_bytes() {
            Some(bytes) => Self::from_4_byte_slice(bytes),
            None => DEFAULT_GRAPH_ID,
        }
    }
}

impl From<u32> for EncodedObjectId {
    fn from(value: u32) -> Self {
        Self(value.to_be_bytes())
    }
}

impl Display for EncodedObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X?}", self.0.as_slice())
    }
}

#[derive(Debug, Error)]
#[error("Invalid object ID.")]
pub struct InvalidObjectIdError;

impl TryFrom<&[u8]> for EncodedObjectId {
    type Error = InvalidObjectIdError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        TryInto::<[u8; 4]>::try_into(value.as_bytes())
            .map(Self)
            .map_err(|_| InvalidObjectIdError)
    }
}

/// The id of the default graph.
pub const DEFAULT_GRAPH_ID: EncodedObjectId = EncodedObjectId([0; 4]);

/// The first regular object id.
pub const FIRST_OBJECT_ID: EncodedObjectId = EncodedObjectId([0, 0, 0, 1]);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_byte_slice_success() {
        let array: [u8; 4] = [0x56, 0x78, 0x9A, 0xBC];
        let id = EncodedObjectId::try_from(array.as_slice()).unwrap();
        assert_eq!(id.0, [0x56, 0x78, 0x9A, 0xBC]);
    }

    #[test]
    fn test_from_byte_slice_success_too_large() {
        let array: [u8; 3] = [0x56, 0x78, 0x9A];
        let result = EncodedObjectId::try_from(array.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn test_from_byte_slice_success_too_small() {
        let array: [u8; 5] = [0x56, 0x78, 0x9A, 0xBC, 0xDE];
        let result = EncodedObjectId::try_from(array.as_slice());
        assert!(result.is_err());
    }
}
