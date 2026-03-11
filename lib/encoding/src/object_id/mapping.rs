use crate::object_id::{ObjectId, ObjectIdSize};
use crate::plain_term::{PlainTermArray, PlainTermScalar, PLAIN_TERM_ENCODING};
use crate::typed_value::{TypedValueArray, TypedValueEncodingRef, TypedValueScalar};
use crate::{EncodingArray, EncodingScalar};
use datafusion::arrow::array::{AsArray, FixedSizeBinaryArray};
use datafusion::arrow::error::ArrowError;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use rdf_fusion_model::{CorruptionError, GraphNameRef, StorageError, ThinError};
use std::error::Error;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;

/// Indicates an error that occurred while working with the [ObjectIdMapping].
#[derive(Error, Debug)]
pub enum ObjectIdMappingError {
    #[error("An error occurred while encoding the result. {0}")]
    ArrowError(ArrowError),
    #[error("Corruption. {0}")]
    IllegalArgument(String),
    #[error("A literal was encountered at a position where a graph name is expected.")]
    LiteralAsGraphName,
    #[error("An error occurred while accessing the object id storage.")]
    Storage(Box<dyn Error + Sync + Send>),
    #[error("Unexpected object id format: {0}")]
    UnexpectedObjectIdFormat(String),
}

#[derive(Error, Debug)]
#[error("An unknown object ID was encountered in an unexpected place.")]
pub struct UnknownObjectIdError;

impl From<ArrowError> for ObjectIdMappingError {
    fn from(value: ArrowError) -> Self {
        ObjectIdMappingError::ArrowError(value)
    }
}

impl From<ObjectIdMappingError> for DataFusionError {
    fn from(value: ObjectIdMappingError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}

impl From<ObjectIdMappingError> for StorageError {
    fn from(value: ObjectIdMappingError) -> Self {
        StorageError::Corruption(CorruptionError::new(value))
    }
}

/// A cheaply cloneable reference to a [`ObjectIdMapping`].
pub type ObjectIdMappingRef = Arc<dyn ObjectIdMapping>;

/// The object id mapping is responsible for mapping between object ids and RDF terms in the
/// [`ObjectIdEncoding`](crate::object_id::ObjectIdEncoding).
///
/// The mapping between the object id and the RDF term is bijective. In other words, each distinct
/// RDF term maps to exactly one object id, while each object id maps to exactly one RDF term. As
/// a result, operations that rely on the equality of RDF terms (`SAME_TERM`) can directly work
/// with the object ids. Joining solution sets is the most important example.
///
/// # Typed Values
///
/// To speed up decoding object ids directly into the [TypedValueEncoding](crate::typed_value::TypedValueEncoding),
/// the trait also contains methods for directly mapping object ids to their typed values. This can
/// be implemented in two ways:
/// 1. Decode the object id to a plain term and then translate the term to a typed value
/// 2. Maintain a second mapping from the object ids to the typed value of their associated RDF term
///
/// Contrary to the mapping between RDF terms and object ids, the mapping between typed values and
/// object ids is not bijective. A single typed value can map to multiple object ids. For example,
/// this is the case for the two RDF terms `"01"^^xsd:integer` and `"1"^^xsd:integer`.
///
/// # Default Graph
///
/// The default graph is represented as the `None` value of the [`ObjectId`] struct.
/// Furthermore, the implementation is responsible for ensuring that all Arrow arrays will have the
/// valid bit set to false for entries that are the default graph (i.e., set them to null).
///
/// Note that some storage implementations might still use a special byte sequence (e.g., all
/// bytes zero) to represent the default graph internally. However, this byte sequence needs then
/// needs to be mapped for implementing this trait.
pub trait ObjectIdMapping: Debug + Send + Sync {
    /// Returns the [`ObjectIdSize`] of the mapped ids.
    fn object_id_size(&self) -> ObjectIdSize;

    /// Try to retrieve the object id of the given `term`.
    ///
    /// This method *does not* automatically create a mapping. See [Self::encode_scalar] for this
    /// functionality.
    fn try_get_object_id(
        &self,
        term: &PlainTermScalar,
    ) -> Result<Option<ObjectId>, ObjectIdMappingError>;

    /// Encodes the entire `array` as an [`FixedSizeBinaryArray`]. Automatically creates a mapping for a
    /// fresh object id if a term is not yet mapped.
    fn encode_array(
        &self,
        array: &PlainTermArray,
    ) -> Result<FixedSizeBinaryArray, ObjectIdMappingError>;

    /// Encodes a single `term` as an [`ObjectId`]. Automatically creates a mapping for a
    /// fresh object id if the term is not yet mapped.
    fn encode_scalar(
        &self,
        term: &PlainTermScalar,
    ) -> Result<ObjectId, ObjectIdMappingError> {
        let array = term
            .to_array(1)
            .expect("Data type is supported for to_array");
        let encoded = self.encode_array(&array)?;
        let object_id = ObjectId::from_array_at_index(&encoded, 0);
        Ok(object_id)
    }

    /// Decodes the entire `array` as a [PlainTermArray].
    fn decode_array(
        &self,
        array: &FixedSizeBinaryArray,
    ) -> Result<PlainTermArray, ObjectIdMappingError>;

    /// Decodes the entire `array` as a [TypedValueArray].
    fn decode_array_to_typed_value(
        &self,
        encoding: &TypedValueEncodingRef,
        array: &FixedSizeBinaryArray,
    ) -> Result<TypedValueArray, ObjectIdMappingError>;

    /// Decodes a single `scalar` as a [PlainTermScalar].
    fn decode_scalar(
        &self,
        scalar: &ObjectId,
    ) -> Result<PlainTermScalar, ObjectIdMappingError> {
        if scalar.is_default_graph() {
            return Ok(PLAIN_TERM_ENCODING
                .encode_term(ThinError::expected())
                .expect("TODO"));
        }

        let array = ScalarValue::FixedSizeBinary(
            self.object_id_size().into(),
            Some(scalar.as_bytes().expect("Not default graph").to_vec()),
        )
        .to_array()
        .expect("Data type is supported for to_array");

        let encoded = self.decode_array(array.as_fixed_size_binary())?;
        Ok(encoded.try_as_scalar(0).expect("Row 0 always exists"))
    }

    /// Decodes a single `scalar` as a [TypedValueScalar].
    fn decode_scalar_to_typed_value(
        &self,
        encoding: &TypedValueEncodingRef,
        scalar: &ObjectId,
    ) -> Result<TypedValueScalar, ObjectIdMappingError> {
        if scalar.is_default_graph() {
            return Ok(encoding.encode_term(ThinError::expected()).expect("TODO"));
        }

        let array = ScalarValue::FixedSizeBinary(
            self.object_id_size().into(),
            Some(scalar.as_bytes().expect("Not default graph").to_vec()),
        )
        .to_array()
        .expect("Data type is supported for to_array");

        let decoded =
            self.decode_array_to_typed_value(encoding, array.as_fixed_size_binary())?;
        Ok(decoded.try_as_scalar(0).expect("Row 0 always exists"))
    }
}

/// A collection of blanked implementation for [`ObjectIdMapping`].
pub trait ObjectIdMappingExtensions {
    /// Tries to get an object id for a term. Returns the default graph id if the graph is the
    /// default graph.
    fn try_get_object_id_for_graph(
        &self,
        graph_name: GraphNameRef<'_>,
    ) -> Result<Option<ObjectId>, ObjectIdMappingError>;

    /// Encodes the given `graph_name`, simply returning the default graph id if the graph is the
    /// default graph.
    fn encode_graph_name(
        &self,
        graph_name: GraphNameRef<'_>,
    ) -> Result<ObjectId, ObjectIdMappingError>;
}

impl<T, U> ObjectIdMappingExtensions for T
where
    T: Deref<Target = U>,
    U: ObjectIdMapping + ?Sized,
{
    fn try_get_object_id_for_graph(
        &self,
        graph_name: GraphNameRef<'_>,
    ) -> Result<Option<ObjectId>, ObjectIdMappingError> {
        match graph_name {
            GraphNameRef::NamedNode(nn) => {
                self.try_get_object_id(&PlainTermScalar::from(nn))
            }
            GraphNameRef::BlankNode(bnode) => {
                self.try_get_object_id(&PlainTermScalar::from(bnode))
            }
            GraphNameRef::DefaultGraph => Ok(Some(ObjectId::new_default_graph())),
        }
    }

    fn encode_graph_name(
        &self,
        graph_name: GraphNameRef<'_>,
    ) -> Result<ObjectId, ObjectIdMappingError> {
        match graph_name {
            GraphNameRef::NamedNode(nn) => self.encode_scalar(&PlainTermScalar::from(nn)),
            GraphNameRef::BlankNode(bnode) => {
                self.encode_scalar(&PlainTermScalar::from(bnode))
            }
            GraphNameRef::DefaultGraph => Ok(ObjectId::new_default_graph()),
        }
    }
}
