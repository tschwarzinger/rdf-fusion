use crate::TermEncoding;
use crate::encoding::EncodingScalar;
use crate::object_id::{ObjectId, ObjectIdCreationError, ObjectIdEncoding};
use datafusion::common::{ScalarValue, exec_err};
use rdf_fusion_model::DFResult;
use std::sync::Arc;

/// Represents an Arrow scalar with a [ObjectIdEncoding].
pub struct ObjectIdScalar {
    encoding: Arc<ObjectIdEncoding>,
    inner: ScalarValue,
}

impl ObjectIdScalar {
    /// Tries to create a new [ObjectIdScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(
        encoding: Arc<ObjectIdEncoding>,
        value: ScalarValue,
    ) -> DFResult<Self> {
        if &value.data_type() != encoding.data_type() {
            return exec_err!(
                "Expected scalar value with ObjectID encoding. Expected: {:?}, got {:?}",
                encoding.data_type(),
                value.data_type()
            );
        }
        Ok(Self::new_unchecked(encoding, value))
    }

    /// Creates a new [ObjectIdScalar] without checking invariants.
    pub fn new_unchecked(encoding: Arc<ObjectIdEncoding>, inner: ScalarValue) -> Self {
        Self { encoding, inner }
    }

    /// Creates a new [ObjectIdScalar] from the given `object_id`.
    pub fn null(encoding: Arc<ObjectIdEncoding>) -> Self {
        let scalar = ScalarValue::FixedSizeBinary(encoding.object_id_size().0, None);
        Self::new_unchecked(encoding, scalar)
    }

    /// Creates a new [ObjectIdScalar] from the given `object_id`.
    pub fn from_object_id(
        encoding: Arc<ObjectIdEncoding>,
        object_id: ObjectId,
    ) -> Result<Self, ObjectIdCreationError> {
        let bytes = object_id.as_bytes().map(|b| b.to_vec());
        let scalar = ScalarValue::FixedSizeBinary(encoding.object_id_size().0, bytes);
        Ok(Self::new_unchecked(encoding, scalar))
    }
}

impl EncodingScalar for ObjectIdScalar {
    type Encoding = ObjectIdEncoding;

    fn encoding(&self) -> &Arc<Self::Encoding> {
        &self.encoding
    }

    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}

impl From<ObjectIdScalar> for ObjectId {
    fn from(value: ObjectIdScalar) -> Self {
        match value.inner {
            ScalarValue::FixedSizeBinary(_, value) => match value {
                Some(oid) => ObjectId::try_new(oid).unwrap(),
                None => ObjectId::new_default_graph(),
            },
            _ => unreachable!("ObjectID scalar is FixedSizeBinary."),
        }
    }
}
