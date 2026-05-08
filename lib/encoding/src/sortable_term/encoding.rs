use crate::EncodingName;
use crate::encoding::TermEncoding;
use crate::sortable_term::{SortableTermArray, SortableTermScalar};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{ScalarValue, not_impl_err};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::{TermRef, ThinResult};
use std::clone::Clone;
use std::sync::{Arc, LazyLock};

/// Represents a sortable term encoding field.
///
/// This encoding is currently a work-around as user-defined orderings are not yet supported in
/// DataFusion. The idea is to project a column of this type and then use the built-in ordering for
/// structs to establish the SPARQL order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SortableTermEncodingField {
    /// Indicates the type of the encoded term. This is the first column and allows to separate the
    /// ordering into the data types (e.g., blank nodes coming before named nodes).
    Type,
    /// Holds a Float64 representation of a possible numeric value. This can cause problems as some
    /// values (e.g., Decimals) cannot be accurately represented using this approach. However, as we
    /// hope that this is only a temporary solution, it is "good-enough" for now.
    Numeric,
    /// Holds bytes that are compared based on their byte values.
    Bytes,
}

impl SortableTermEncodingField {
    /// Get the name of the field.
    pub fn name(self) -> &'static str {
        match self {
            SortableTermEncodingField::Type => "type",
            SortableTermEncodingField::Numeric => "numeric",
            SortableTermEncodingField::Bytes => "bytes",
        }
    }

    /// Get the index in the struct from that field.
    pub fn index(self) -> usize {
        match self {
            SortableTermEncodingField::Type => 0,
            SortableTermEncodingField::Numeric => 1,
            SortableTermEncodingField::Bytes => 2,
        }
    }

    /// Get the [DataType] of this field.
    pub fn data_type(self) -> DataType {
        match self {
            SortableTermEncodingField::Type => DataType::Int8,
            SortableTermEncodingField::Numeric => DataType::Float64,
            SortableTermEncodingField::Bytes => DataType::Binary,
        }
    }
}

impl From<SortableTermEncodingField> for i8 {
    fn from(val: SortableTermEncodingField) -> i8 {
        val.index() as i8
    }
}

static FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new(
            SortableTermEncodingField::Type.name(),
            SortableTermEncodingField::Type.data_type(),
            false,
        ),
        Field::new(
            SortableTermEncodingField::Numeric.name(),
            SortableTermEncodingField::Numeric.data_type(),
            true,
        ),
        Field::new(
            SortableTermEncodingField::Bytes.name(),
            SortableTermEncodingField::Bytes.data_type(),
            false,
        ),
    ])
});

/// The instance of the [`SortableTermEncoding`].
///
/// As there is currently no way to parameterize the encoding, accessing it via this constant is
/// the preferred way.
pub static SORTABLE_TERM_ENCODING: LazyLock<SortableTermEncodingRef> =
    LazyLock::new(|| Arc::new(SortableTermEncoding));

/// A cheaply cloneable reference to the [`SortableTermEncoding`].
pub type SortableTermEncodingRef = Arc<SortableTermEncoding>;

/// The sortable term encoding allows us to represent the expected SPARQL ordering using
/// DataFusion's built-in ordering for structs.
///
/// This is meant as a work-around until we can define a custom ordering in DataFusion.
/// Alternatively, we could also write a custom operator for sorting SPARQL solutions.
#[derive(Debug)]
pub struct SortableTermEncoding;

impl SortableTermEncoding {
    /// Returns the fields of this encoding.
    pub fn fields() -> Fields {
        FIELDS.clone()
    }

    /// Encodes the `term` as a [SortableTermScalar].
    pub fn encode_term(
        &self,
        _term: ThinResult<TermRef<'_>>,
    ) -> DFResult<SortableTermScalar> {
        not_impl_err!("SortableTermEncoding::encode_term")
    }
}

impl TermEncoding for SortableTermEncoding {
    type Array = SortableTermArray;
    type Scalar = SortableTermScalar;

    fn name(&self) -> EncodingName {
        EncodingName::Sortable
    }

    fn data_type(&self) -> &DataType {
        static DATA_TYPE: LazyLock<DataType> =
            LazyLock::new(|| DataType::Struct(SortableTermEncoding::fields().clone()));
        &DATA_TYPE
    }

    fn try_new_array(self: &Arc<Self>, array: ArrayRef) -> DFResult<Self::Array> {
        array.try_into()
    }

    fn try_new_scalar(self: &Arc<Self>, scalar: ScalarValue) -> DFResult<Self::Scalar> {
        scalar.try_into()
    }
}
