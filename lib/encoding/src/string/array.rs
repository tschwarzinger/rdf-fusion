use crate::compute::with_plain_term_encoding_from_string;
use crate::plain_term::PlainTermArray;
use crate::string::{STRING_ENCODING, StringEncoding, StringEncodingRef};
use crate::{EncodingArray, EncodingDatum};
use datafusion::arrow::array::{ArrayRef, StringArray};
use rdf_fusion_model::AResult;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct StringEncodingArray {
    /// The inner array.
    inner: ArrayRef,
    /// The encoding of the array.
    encoding: StringEncodingRef,
}

impl StringEncodingArray {
    /// Creates a new [`StringEncodingArray`] without validating the invariants.
    pub fn new_unchecked(inner: ArrayRef) -> Self {
        Self {
            inner,
            encoding: Arc::clone(&STRING_ENCODING),
        }
    }

    /// Creates a new [StringEncodingArray] with `number_rows` null values.
    pub fn new_null(number_rows: usize) -> Self {
        Self::new_unchecked(Arc::new(StringArray::new_null(number_rows)))
    }

    /// Returns the inner array.
    pub fn inner(&self) -> &ArrayRef {
        &self.inner
    }

    /// Returns the plain term representation of this array.
    pub fn as_plain_term_array(&self) -> AResult<PlainTermArray> {
        let result =
            with_plain_term_encoding_from_string(&EncodingDatum::Array(self.clone()))?;
        Ok(result)
    }
}

impl EncodingArray for StringEncodingArray {
    type Encoding = StringEncoding;

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
