use crate::TermEncoding;
use crate::encoding::EncodingArray;
use crate::object_id::{ObjectIdEncoding, ObjectIdEncodingRef};
use datafusion::arrow::array::{Array, ArrayRef, FixedSizeBinaryArray};
use datafusion::common::exec_err;
use rdf_fusion_model::DFResult;
use std::sync::Arc;

/// Represents an Arrow array with an [ObjectIdEncoding].
#[derive(Debug, Clone)]
pub struct ObjectIdArray {
    encoding: ObjectIdEncodingRef,
    inner: ArrayRef,
}

impl ObjectIdArray {
    /// Tries to create a new [`ObjectIdArray`] from a regular [`ArrayRef`].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(encoding: ObjectIdEncodingRef, array: ArrayRef) -> DFResult<Self> {
        if array.data_type() != encoding.data_type() {
            return exec_err!("Expected array with ObjectIdEncoding, got {:?}", array);
        }
        Ok(Self::new_unchecked(encoding, array))
    }

    /// Creates a new [ObjectIdArray] without checking invariants.
    pub fn new_unchecked(encoding: ObjectIdEncodingRef, inner: ArrayRef) -> Self {
        Self { encoding, inner }
    }

    /// Returns a reference to the inner [FixedSizeBinaryArray].
    pub fn object_ids(&self) -> &FixedSizeBinaryArray {
        self.inner
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Checked in constructor")
    }
}

impl EncodingArray for ObjectIdArray {
    type Encoding = ObjectIdEncoding;

    fn encoding(&self) -> &Arc<Self::Encoding> {
        &self.encoding
    }

    fn inner(&self) -> &ArrayRef {
        &self.inner
    }

    fn into_array_ref(self) -> ArrayRef {
        self.inner
    }
}
