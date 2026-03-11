use crate::EncodingName;
use crate::encoding::TermEncoding;
use crate::object_id::{
    ObjectIdArray, ObjectIdMapping, ObjectIdMappingError, ObjectIdMappingRef,
    ObjectIdScalar, ObjectIdSize,
};
use crate::plain_term::{PlainTermArray, PlainTermScalar};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use rdf_fusion_model::DFResult;
use std::clone::Clone;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A cheaply cloneable reference to a [`ObjectIdEncoding`].
pub type ObjectIdEncodingRef = Arc<ObjectIdEncoding>;

/// The [`ObjectIdEncoding`] represents each distinct term in the database with a single fixed-size
/// id. We call such an id *object id*. Here is an example of the encoding:
///
/// ```text
/// ?variable
///
///  ┌─────┐
///  │   1 │ ────►  <#MyEntity>
///  ├─────┤
///  │   2 │ ────►  120^^xsd:integer
///  ├─────┤
///  │ ... │
///  └─────┘
/// ```
///
/// # Object ID Mapping
///
/// The mapping implementation depends on the storage layer that is being used. For example, an
/// in-memory RDF store will use a different implementation as an on-disk RDF store. The
/// [`ObjectIdMapping`](crate::object_id::ObjectIdMapping) trait defines the contract.
///
/// # Default Graph
///
/// The default graph is represented as the `None` value of the [`ObjectId`] struct.
/// In addition, functions that return Arrow arrays with object ids need to highlight the default
/// graph by setting the valid bit to `false` (i.e., making them null).
///
/// Note that some storage implementations might still use a special byte sequence (e.g., all
/// bytes zero) to represent the default graph internally. However, this is abstracted away by
/// the [`ObjectId`] struct.
///
/// # Strengths and Weaknesses
///
/// The object id encoding is very well suited for evaluating joins, as instead of joining
/// variable-length RDF terms, we can directly join the object ids. While we do not have recent
/// numbers for the performance gains, the [original pull request](https://github.com/tobixdev/rdf-fusion/pull/27)
/// quadrupled the performance of some queries (with relatively small datasets!).
///
/// However, this also introduces the necessity of decoding the object ids back to RDF terms. For
/// example, by converting it to the [`PlainTermEncoding`](crate::plain_term::PlainTermEncoding).
/// For queries that spend little time on join operations, the cost of decoding the object ids can
/// outweigh the benefits of using the object id encoding.
///
/// Furthermore, the encoding introduces the necessity of maintaining the
/// [`ObjectIdMapping`](crate::object_id::ObjectIdMapping), which can be non-trivial.
///
/// # Equality
///
/// The equality and hashing functions check for pointer equality of the underlying mapping.
///
/// # Current Limitation
///
/// Currently, this id is fixed to being a 32-bit integer. However, we have an
/// [issue](https://github.com/tobixdev/rdf-fusion/issues/50) that tracks the progress on limiting
/// this limitation.
#[derive(Debug, Clone)]
pub struct ObjectIdEncoding {
    /// The number of bytes in a single object id.
    object_id_size: ObjectIdSize,
    /// The data type of the object ids.
    data_type: DataType,
    /// The mapping that is used to encode and decode object ids.
    mapping: Arc<dyn ObjectIdMapping>,
}

impl ObjectIdEncoding {
    /// Creates a new [ObjectIdEncoding].
    pub fn new(mapping: Arc<dyn ObjectIdMapping>) -> Self {
        Self {
            object_id_size: mapping.object_id_size(),
            data_type: DataType::FixedSizeBinary(mapping.object_id_size().into()),
            mapping,
        }
    }

    /// Returns the size of the object id.
    pub fn object_id_size(&self) -> ObjectIdSize {
        self.object_id_size
    }

    /// Returns the mapping that is used to encode and decode object ids.
    pub fn mapping(&self) -> &ObjectIdMappingRef {
        &self.mapping
    }

    /// Encodes a [`PlainTermScalar`] into an [`ObjectIdScalar`].
    ///
    /// See also [`ObjectIdMapping::encode_scalar`].
    pub fn encode_scalar(
        self: &Arc<Self>,
        term: &PlainTermScalar,
    ) -> Result<ObjectIdScalar, ObjectIdMappingError> {
        let object_id = self.mapping.encode_scalar(term)?;
        ObjectIdScalar::from_object_id(Arc::clone(self), object_id).map_err(|_| {
            ObjectIdMappingError::IllegalArgument(
                "Wrong object id length returned by mapping".to_owned(),
            )
        })
    }

    /// Encodes a [`PlainTermArray`] into an [`ObjectIdArray`].
    ///
    /// See also [`ObjectIdMapping::encode_array`].
    pub fn encode_array(
        self: &Arc<Self>,
        array: &PlainTermArray,
    ) -> Result<ObjectIdArray, ObjectIdMappingError> {
        self.mapping.encode_array(array).map(|oids| {
            ObjectIdArray::try_new(Arc::clone(self), Arc::new(oids) as ArrayRef).unwrap()
        })
    }
}

impl TermEncoding for ObjectIdEncoding {
    type Array = ObjectIdArray;
    type Scalar = ObjectIdScalar;

    fn name(&self) -> EncodingName {
        EncodingName::PlainTerm
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn try_new_array(self: &Arc<Self>, array: ArrayRef) -> DFResult<Self::Array> {
        ObjectIdArray::try_new(Arc::clone(self), array)
    }

    fn try_new_scalar(self: &Arc<Self>, scalar: ScalarValue) -> DFResult<Self::Scalar> {
        ObjectIdScalar::try_new(Arc::clone(self), scalar)
    }
}

impl PartialEq for ObjectIdEncoding {
    fn eq(&self, other: &Self) -> bool {
        self.object_id_size == other.object_id_size
            && Arc::ptr_eq(&self.mapping, &other.mapping)
    }
}

impl Eq for ObjectIdEncoding {}

impl Hash for ObjectIdEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.object_id_size.hash(state);
    }
}
