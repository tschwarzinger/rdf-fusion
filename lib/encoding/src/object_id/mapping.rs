use crate::EncodingArray;
use crate::object_id::ObjectIdDataType;
use crate::plain_term::{PlainTermArray, PlainTermScalar};
use crate::typed_family::{TypedFamilyArray, TypedFamilyEncodingRef, TypedFamilyScalar};
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::error::ArrowError;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use rdf_fusion_common::{CorruptionError, GraphNameRef, StorageError};
use std::error::Error;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;

/// Indicates an error that occurred while working with the [ObjectIdDictionary].
#[derive(Error, Debug)]
pub enum ObjectIdDictionaryError {
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

impl From<ArrowError> for ObjectIdDictionaryError {
    fn from(value: ArrowError) -> Self {
        ObjectIdDictionaryError::ArrowError(value)
    }
}

impl From<DataFusionError> for ObjectIdDictionaryError {
    fn from(value: DataFusionError) -> Self {
        match value {
            DataFusionError::ArrowError(e, _) => ObjectIdDictionaryError::ArrowError(*e),
            _ => ObjectIdDictionaryError::Storage(Box::new(value)),
        }
    }
}

impl From<ObjectIdDictionaryError> for DataFusionError {
    fn from(value: ObjectIdDictionaryError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}

impl From<ObjectIdDictionaryError> for StorageError {
    fn from(value: ObjectIdDictionaryError) -> Self {
        StorageError::Corruption(CorruptionError::new(value))
    }
}

/// A cheaply cloneable reference to a [`ObjectIdDictionary`].
pub type ObjectIdDictionaryRef = Arc<dyn ObjectIdDictionary>;

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
#[async_trait]
pub trait ObjectIdDictionary: Debug + Send + Sync {
    /// Returns the [`ObjectIdDataType`] of the mapped ids.
    fn object_id_data_type(&self) -> ObjectIdDataType;

    /// Try to retrieve the object id of the given `term`.
    ///
    /// This method *does not* automatically create a mapping.
    async fn try_get_object_id(
        &self,
        term: &PlainTermScalar,
    ) -> Result<Option<ScalarValue>, ObjectIdDictionaryError>;

    /// Decodes the entire `array` as a [`PlainTermArray`].
    async fn decode_array(
        &self,
        array: &ArrayRef,
    ) -> Result<PlainTermArray, ObjectIdDictionaryError>;

    /// Decodes the entire `array` as a [`TypedFamilyArray`].
    async fn decode_array_to_typed_family(
        &self,
        encoding: &TypedFamilyEncodingRef,
        array: &ArrayRef,
    ) -> Result<TypedFamilyArray, ObjectIdDictionaryError>;

    /// Decodes a single `scalar` as a [`PlainTermScalar`].
    async fn decode_scalar(
        &self,
        scalar: &ScalarValue,
    ) -> Result<PlainTermScalar, ObjectIdDictionaryError> {
        if scalar.is_null() {
            return Err(ObjectIdDictionaryError::UnexpectedObjectIdFormat(
                "Cannot decode default graph (null) to a term".to_string(),
            ));
        }

        let array = scalar
            .to_array()
            .expect("Data type is supported for to_array");

        let encoded = self.decode_array(&array).await?;
        Ok(encoded.try_as_scalar(0).expect("Row 0 always exists"))
    }

    /// Decodes a single `scalar` as a [`TypedFamilyScalar`].
    async fn decode_scalar_to_typed_family(
        &self,
        encoding: &TypedFamilyEncodingRef,
        scalar: &ScalarValue,
    ) -> Result<TypedFamilyScalar, ObjectIdDictionaryError> {
        if scalar.is_null() {
            return Ok(encoding.create_scalar_null());
        }

        let array = scalar
            .to_array()
            .expect("Data type is supported for to_array");

        let decoded = self.decode_array_to_typed_family(encoding, &array).await?;
        Ok(decoded.try_as_scalar(0).expect("Row 0 always exists"))
    }
}

/// A collection of blanked implementation for [`ObjectIdDictionary`].
#[async_trait]
pub trait ObjectIdDictionaryExtensions {
    /// Tries to get an object id for a term. Returns the default graph id if the graph is the
    /// default graph.
    async fn try_get_object_id_for_graph(
        &self,
        graph_name: GraphNameRef<'_>,
    ) -> Result<Option<ScalarValue>, ObjectIdDictionaryError>;
}

#[async_trait]
impl<T, U> ObjectIdDictionaryExtensions for T
where
    T: Deref<Target = U> + Send + Sync,
    U: ObjectIdDictionary + ?Sized,
{
    async fn try_get_object_id_for_graph(
        &self,
        graph_name: GraphNameRef<'_>,
    ) -> Result<Option<ScalarValue>, ObjectIdDictionaryError> {
        match graph_name {
            GraphNameRef::NamedNode(nn) => {
                self.try_get_object_id(&PlainTermScalar::from(nn)).await
            }
            GraphNameRef::BlankNode(bnode) => {
                self.try_get_object_id(&PlainTermScalar::from(bnode)).await
            }
            GraphNameRef::DefaultGraph => {
                let data_type = datafusion::arrow::datatypes::DataType::from(
                    self.object_id_data_type(),
                );
                Ok(Some(ScalarValue::try_new_null(&data_type).unwrap()))
            }
        }
    }
}
