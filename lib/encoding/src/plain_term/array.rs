use crate::TermEncoding;
use crate::encoding::EncodingArray;
use crate::plain_term::{PLAIN_TERM_ENCODING, PlainTermEncoding, PlainTermEncodingField};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, StringArray, StructArray, UInt8Array,
};
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;
use std::sync::Arc;

/// Represents an Arrow array with a [PlainTermEncoding].
#[derive(Debug, Clone)]
pub struct PlainTermArray {
    inner: ArrayRef,
}

/// Holds the parts of a [PlainTermArray].
pub struct PlainTermArrayParts<'array> {
    pub struct_array: &'array StructArray,
    pub term_type: &'array UInt8Array,
    pub value: &'array StringArray,
    pub data_type: &'array StringArray,
    pub language_tag: &'array StringArray,
}

impl PlainTermArray {
    /// Creates a new [PlainTermArray] without validating the schema.
    pub(super) fn new_unchecked(inner: ArrayRef) -> Self {
        Self { inner }
    }

    /// Returns a [PlainTermArrayParts] that holds references to the inner arrays.
    pub fn as_parts(&self) -> PlainTermArrayParts<'_> {
        let struct_array = self.inner.as_struct();
        PlainTermArrayParts {
            struct_array,
            term_type: struct_array
                .column(PlainTermEncodingField::TermType.index())
                .as_primitive(),
            value: struct_array
                .column(PlainTermEncodingField::Value.index())
                .as_string(),
            data_type: struct_array
                .column(PlainTermEncodingField::DataType.index())
                .as_string(),
            language_tag: struct_array
                .column(PlainTermEncodingField::LanguageTag.index())
                .as_string(),
        }
    }
}

impl EncodingArray for PlainTermArray {
    type Encoding = PlainTermEncoding;

    fn encoding(&self) -> &Arc<Self::Encoding> {
        &PLAIN_TERM_ENCODING
    }

    fn array(&self) -> &ArrayRef {
        &self.inner
    }

    fn into_array_ref(self) -> ArrayRef {
        self.inner
    }
}

impl TryFrom<ArrayRef> for PlainTermArray {
    type Error = DataFusionError;

    fn try_from(value: ArrayRef) -> Result<Self, Self::Error> {
        if value.data_type() != PLAIN_TERM_ENCODING.data_type() {
            return exec_err!(
                "Expected array with PlainTermEncoding, got: {}",
                value.data_type()
            );
        }
        Ok(Self { inner: value })
    }
}
