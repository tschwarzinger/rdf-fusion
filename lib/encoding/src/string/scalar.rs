use crate::EncodingScalar;
use crate::string::{STRING_ENCODING, StringEncoding, StringEncodingRef};
use datafusion::common::ScalarValue;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct StringEncodingScalar {
    inner: ScalarValue,
    encoding: StringEncodingRef,
}

impl StringEncodingScalar {
    pub fn new_unchecked(inner: ScalarValue) -> Self {
        Self {
            inner,
            encoding: Arc::clone(&STRING_ENCODING),
        }
    }
}

impl EncodingScalar for StringEncodingScalar {
    type Encoding = StringEncoding;

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
