use crate::object_id::ObjectIdDataType;
use crate::plain_term::{PLAIN_TERM_ENCODING, PlainTermArray, PlainTermScalar};
use crate::typed_family::{TypedFamilyArray, TypedFamilyEncodingRef, TypedFamilyScalar};
use crate::{EncodingArray, EncodingScalar};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::error::ArrowError;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use rdf_fusion_common::{CorruptionError, GraphNameRef, StorageError, ThinError};
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
    #[error("An error occurred while accessing the object id storage. {0}")]
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

impl From<DataFusionError> for ObjectIdMappingError {
    fn from(value: DataFusionError) -> Self {
        match value {
            DataFusionError::ArrowError(e, _) => ObjectIdMappingError::ArrowError(*e),
            _ => ObjectIdMappingError::Storage(Box::new(value)),
        }
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
/// # Default Graph
///
/// The default graph is represented as a null value in the [`ScalarValue`] enum.
/// Furthermore, the implementation is responsible for ensuring that all Arrow arrays will have the
/// valid bit set to false for entries that are the default graph (i.e., set them to null).
///
/// Note that some storage implementations might still use a special byte sequence (e.g., all
/// bytes zero) to represent the default graph internally. However, this byte sequence needs then
/// needs to be mapped for implementing this trait.
pub trait ObjectIdMapping: Debug + Send + Sync {
    /// Returns the [`ObjectIdDataType`] of the mapped ids.
    fn object_id_data_type(&self) -> ObjectIdDataType;

    /// Try to retrieve the object id of the given `term`.
    ///
    /// This method *does not* automatically create a mapping. See [Self::encode_scalar] for this
    /// functionality.
    fn try_get_object_id(
        &self,
        term: &PlainTermScalar,
    ) -> Result<Option<ScalarValue>, ObjectIdMappingError>;

    /// Encodes the entire `array` as an array of object ids. The [`Self::object_id_data_type`]
    /// determined which array type is used.
    ///
    /// Automatically creates a mapping for a fresh object id if a term is not yet mapped.
    fn encode_array(
        &self,
        array: &PlainTermArray,
    ) -> Result<ArrayRef, ObjectIdMappingError>;

    /// Encodes a single `term` as an [`ScalarValue`]. Automatically creates a mapping for a
    /// fresh object id if the term is not yet mapped.
    fn encode_scalar(
        &self,
        term: &PlainTermScalar,
    ) -> Result<ScalarValue, ObjectIdMappingError> {
        let array = term
            .to_array(1)
            .expect("Data type is supported for to_array");
        let encoded = self.encode_array(&array)?;
        let scalar_value = ScalarValue::try_from_array(encoded.as_ref(), 0)?;
        Ok(scalar_value)
    }

    /// Decodes the entire `array` as a [`PlainTermArray`].
    fn decode_array(
        &self,
        array: &ArrayRef,
    ) -> Result<PlainTermArray, ObjectIdMappingError>;

    /// Decodes the entire `array` as a [`TypedFamilyArray`].
    fn decode_array_to_typed_family(
        &self,
        encoding: &TypedFamilyEncodingRef,
        array: &ArrayRef,
    ) -> Result<TypedFamilyArray, ObjectIdMappingError>;

    /// Decodes a single `scalar` as a [`PlainTermScalar`].
    fn decode_scalar(
        &self,
        scalar: &ScalarValue,
    ) -> Result<PlainTermScalar, ObjectIdMappingError> {
        if scalar.is_null() {
            return Ok(PLAIN_TERM_ENCODING
                .encode_term(ThinError::expected())
                .expect("TODO"));
        }

        let array = scalar
            .to_array()
            .expect("Data type is supported for to_array");

        let encoded = self.decode_array(&array)?;
        Ok(encoded.try_as_scalar(0).expect("Row 0 always exists"))
    }

    /// Decodes a single `scalar` as a [`TypedFamilyScalar`].
    fn decode_scalar_to_typed_family(
        &self,
        encoding: &TypedFamilyEncodingRef,
        scalar: &ScalarValue,
    ) -> Result<TypedFamilyScalar, ObjectIdMappingError> {
        if scalar.is_null() {
            return Ok(encoding.create_scalar_null());
        }

        let array = scalar
            .to_array()
            .expect("Data type is supported for to_array");

        let decoded = self.decode_array_to_typed_family(encoding, &array)?;
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
    ) -> Result<Option<ScalarValue>, ObjectIdMappingError>;

    /// Encodes the given `graph_name`, simply returning the default graph id if the graph is the
    /// default graph.
    fn encode_graph_name(
        &self,
        graph_name: GraphNameRef<'_>,
    ) -> Result<ScalarValue, ObjectIdMappingError>;
}

impl<T, U> ObjectIdMappingExtensions for T
where
    T: Deref<Target = U>,
    U: ObjectIdMapping + ?Sized,
{
    fn try_get_object_id_for_graph(
        &self,
        graph_name: GraphNameRef<'_>,
    ) -> Result<Option<ScalarValue>, ObjectIdMappingError> {
        match graph_name {
            GraphNameRef::NamedNode(nn) => {
                self.try_get_object_id(&PlainTermScalar::from(nn))
            }
            GraphNameRef::BlankNode(bnode) => {
                self.try_get_object_id(&PlainTermScalar::from(bnode))
            }
            GraphNameRef::DefaultGraph => {
                let data_type = datafusion::arrow::datatypes::DataType::from(
                    self.object_id_data_type(),
                );
                Ok(Some(ScalarValue::try_new_null(&data_type).unwrap()))
            }
        }
    }

    fn encode_graph_name(
        &self,
        graph_name: GraphNameRef<'_>,
    ) -> Result<ScalarValue, ObjectIdMappingError> {
        match graph_name {
            GraphNameRef::NamedNode(nn) => self.encode_scalar(&PlainTermScalar::from(nn)),
            GraphNameRef::BlankNode(bnode) => {
                self.encode_scalar(&PlainTermScalar::from(bnode))
            }
            GraphNameRef::DefaultGraph => {
                let data_type = datafusion::arrow::datatypes::DataType::from(
                    self.object_id_data_type(),
                );
                Ok(ScalarValue::try_new_null(&data_type).unwrap())
            }
        }
    }
}
