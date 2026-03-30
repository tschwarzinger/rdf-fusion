use crate::TermEncoding;
use crate::encoding::EncodingArray;
use crate::sortable_term::{SORTABLE_TERM_ENCODING, SortableTermEncoding};
use datafusion::arrow::array::{Array, ArrayRef, new_null_array};
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;
use std::sync::Arc;

/// Represents an Arrow array with a [SortableTermArray].
#[derive(Clone)]
pub struct SortableTermArray {
    inner: ArrayRef,
}

impl SortableTermArray {
    /// Returns a null [`SortableTermArray`].
    pub fn new_null(len: usize) -> Self {
        Self {
            inner: new_null_array(SORTABLE_TERM_ENCODING.data_type(), len),
        }
    }
}

impl EncodingArray for SortableTermArray {
    type Encoding = SortableTermEncoding;

    fn encoding(&self) -> &Arc<Self::Encoding> {
        &SORTABLE_TERM_ENCODING
    }

    fn inner(&self) -> &ArrayRef {
        &self.inner
    }

    fn into_array_ref(self) -> ArrayRef {
        self.inner
    }
}

impl TryFrom<ArrayRef> for SortableTermArray {
    type Error = DataFusionError;

    fn try_from(value: ArrayRef) -> Result<Self, Self::Error> {
        if value.data_type() != SORTABLE_TERM_ENCODING.data_type() {
            return exec_err!(
                "Expected array with SortableEncoded terms, got: {}",
                value.data_type()
            );
        }
        Ok(Self { inner: value })
    }
}
