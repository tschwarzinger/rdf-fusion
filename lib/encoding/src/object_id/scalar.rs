use crate::TermEncoding;
use crate::encoding::EncodingScalar;
use crate::object_id::{ObjectIdEncoding, ObjectIdEncodingRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{ScalarValue, exec_err};
use rdf_fusion_model::DFResult;
use std::sync::Arc;

/// Represents an Arrow scalar with a [ObjectIdEncoding].
#[derive(Clone)]
pub struct ObjectIdScalar {
    encoding: ObjectIdEncodingRef,
    inner: ScalarValue,
}

impl ObjectIdScalar {
    /// Tries to create a new [ObjectIdScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(encoding: ObjectIdEncodingRef, value: ScalarValue) -> DFResult<Self> {
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
    pub fn new_unchecked(encoding: ObjectIdEncodingRef, inner: ScalarValue) -> Self {
        Self { encoding, inner }
    }

    /// Creates a new [ObjectIdScalar] from the given `object_id`.
    pub fn null(encoding: ObjectIdEncodingRef) -> Self {
        let scalar = match encoding.data_type() {
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::FixedSizeBinary(size) => ScalarValue::FixedSizeBinary(*size, None),
            _ => unreachable!("ObjectID encoding is not a supported type."),
        };
        Self::new_unchecked(encoding, scalar)
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
