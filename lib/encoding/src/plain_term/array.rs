use crate::TermEncoding;
use crate::encoding::EncodingArray;
use crate::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermEncoding, PlainTermEncodingField, PlainTermScalar,
    PlainTermType,
};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, StringArray, StructArray, UInt8Array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::common::{ScalarValue, exec_err};
use datafusion::error::DataFusionError;
use rdf_fusion_model::AResult;
use std::sync::Arc;

/// Represents an Arrow array with a [`PlainTermEncoding`].
#[derive(Debug, Clone)]
pub struct PlainTermArray {
    inner: ArrayRef,
}

/// Holds the parts of a [`PlainTermArray`].
pub struct PlainTermArrayParts<'array> {
    pub struct_array: &'array StructArray,
    pub term_type: &'array UInt8Array,
    pub value: &'array StringArray,
    pub data_type: &'array StringArray,
    pub language_tag: &'array StringArray,
}

impl PlainTermArray {
    /// Creates a new [`PlainTermArray`] without validating the schema.
    pub fn new_unchecked(inner: ArrayRef) -> Self {
        Self { inner }
    }

    /// Creates a new [`PlainTermArray`] from its parts.
    pub fn try_new(
        term_type: UInt8Array,
        value: StringArray,
        data_type: StringArray,
        language_tag: StringArray,
        nulls: Option<NullBuffer>,
    ) -> AResult<Self> {
        let fields = PlainTermEncoding::fields();
        let array = StructArray::try_new(
            fields,
            vec![
                Arc::new(term_type) as ArrayRef,
                Arc::new(value) as ArrayRef,
                Arc::new(data_type) as ArrayRef,
                Arc::new(language_tag) as ArrayRef,
            ],
            nulls,
        )?;
        Ok(Self {
            inner: Arc::new(array),
        })
    }

    /// Creates a new [PlainTermArray] with only named nodes.
    ///
    /// The null buffer from the given array is used for the null buffer of the result.
    pub fn new_named_nodes(iris: StringArray) -> Self {
        let len = iris.len();
        let nulls = iris.nulls().cloned();
        Self::try_new(
            UInt8Array::from_value(u8::from(PlainTermType::NamedNode), len),
            iris,
            StringArray::new_null(len),
            StringArray::new_null(len),
            nulls,
        )
        .expect("Always a valid StructArray")
    }

    /// Creates a new [PlainTermArray] with only blank nodes.
    ///
    /// The null buffer from the given array is used for the null buffer of the result.
    pub fn new_blank_nodes(blank_nodes: StringArray) -> Self {
        let len = blank_nodes.len();
        let nulls = blank_nodes.nulls().cloned();
        Self::try_new(
            UInt8Array::from_value(u8::from(PlainTermType::BlankNode), len),
            blank_nodes,
            StringArray::new_null(len),
            StringArray::new_null(len),
            nulls,
        )
        .expect("Always a valid StructArray")
    }

    /// Creates a new [PlainTermArray] with only literals.
    pub fn try_new_literals(
        values: StringArray,
        data_types: StringArray,
        languages: StringArray,
        nulls: Option<NullBuffer>,
    ) -> AResult<Self> {
        let len = values.len();
        Self::try_new(
            UInt8Array::from_value(u8::from(PlainTermType::Literal), len),
            values,
            data_types,
            languages,
            nulls,
        )
    }

    /// Creates a new [PlainTermArray] with only null values.
    pub fn new_null(len: usize) -> Self {
        Self::try_new(
            UInt8Array::from_value(0, len),
            StringArray::new_null(len),
            StringArray::new_null(len),
            StringArray::new_null(len),
            Some(NullBuffer::new_null(len)),
        )
        .unwrap()
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

    /// Returns the number of terms in the array.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether the array is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the term at the given index.
    pub fn get(&self, index: usize) -> Option<PlainTermScalar> {
        if self.inner.is_null(index) {
            None
        } else {
            let scalar = ScalarValue::try_from_array(&self.inner, index).unwrap();
            Some(PlainTermScalar::new_unchecked(scalar))
        }
    }
}

impl EncodingArray for PlainTermArray {
    type Encoding = PlainTermEncoding;

    fn encoding(&self) -> &Arc<Self::Encoding> {
        &PLAIN_TERM_ENCODING
    }

    fn inner(&self) -> &ArrayRef {
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
