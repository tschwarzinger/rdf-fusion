use crate::encoding::TermEncoding;
use crate::plain_term::encoders::DefaultPlainTermEncoder;
use crate::plain_term::{PlainTermArray, PlainTermScalar};
use crate::{EncodingName, TermEncoder};
use datafusion::arrow::array::{Array, ArrayRef, StringArray, StructArray, UInt8Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::ScalarValue;
use rdf_fusion_model::DFResult;
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{TermRef, ThinResult};
use std::clone::Clone;
use std::fmt::Display;
use std::sync::{Arc, LazyLock};
use thiserror::Error;

/// Represents the fields of the [PlainTermEncoding].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlainTermEncodingField {
    /// Indicates the type of RDF term.
    TermType,
    /// Contains the lexical value of an RDF term.
    Value,
    /// Holds the data type of RDF literal, including simple literals and language-tagged literals.
    /// If an RDF term has a language tag, the datatype must contain rdf:langString.
    ///
    /// This filed should be `null` for named nodes and blank nodes.
    DataType,
    /// Contains an optional language tag for language-tagged literals.
    ///
    /// This field should be `null` for named nodes, blank nodes, and literals without a language
    /// tag.
    LanguageTag,
}

impl PlainTermEncodingField {
    pub fn name(self) -> &'static str {
        match self {
            PlainTermEncodingField::TermType => "term_type",
            PlainTermEncodingField::Value => "value",
            PlainTermEncodingField::DataType => "data_type",
            PlainTermEncodingField::LanguageTag => "language_tag",
        }
    }

    pub fn index(self) -> usize {
        match self {
            PlainTermEncodingField::TermType => 0,
            PlainTermEncodingField::Value => 1,
            PlainTermEncodingField::DataType => 2,
            PlainTermEncodingField::LanguageTag => 3,
        }
    }

    #[allow(clippy::match_same_arms)]
    pub fn data_type(self) -> DataType {
        match self {
            PlainTermEncodingField::TermType => DataType::UInt8,
            PlainTermEncodingField::Value => DataType::Utf8,
            PlainTermEncodingField::DataType => DataType::Utf8,
            PlainTermEncodingField::LanguageTag => DataType::Utf8,
        }
    }

    #[allow(clippy::match_same_arms)]
    pub fn is_nullable(self) -> bool {
        match self {
            PlainTermEncodingField::TermType => false,
            PlainTermEncodingField::Value => false,
            PlainTermEncodingField::DataType => true,
            PlainTermEncodingField::LanguageTag => true,
        }
    }

    pub fn field(self) -> Field {
        Field::new(self.name(), self.data_type(), self.is_nullable())
    }
}

static FIELDS_TYPE: LazyLock<Fields> = LazyLock::new(|| {
    let fields = vec![
        PlainTermEncodingField::TermType.field(),
        PlainTermEncodingField::Value.field(),
        PlainTermEncodingField::DataType.field(),
        PlainTermEncodingField::LanguageTag.field(),
    ];
    Fields::from(fields)
});

/// Indicates the type of an RDF term that is encoded in the [PlainTermEncoding].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlainTermType {
    /// Represents a named node.
    NamedNode,
    /// Represents a blank node.
    BlankNode,
    /// Represents a literal.
    Literal,
}

#[derive(Debug, Clone, Copy, Default, Error, PartialEq, Eq, Hash)]
pub struct UnknownPlainTermTypeError;

impl Display for UnknownPlainTermTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unexpected type_id for encoded RDF Term")
    }
}

impl TryFrom<u8> for PlainTermType {
    type Error = UnknownPlainTermTypeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PlainTermType::NamedNode),
            1 => Ok(PlainTermType::BlankNode),
            2 => Ok(PlainTermType::Literal),
            _ => Err(UnknownPlainTermTypeError),
        }
    }
}

impl From<PlainTermType> for u8 {
    fn from(val: PlainTermType) -> u8 {
        match val {
            PlainTermType::NamedNode => 0,
            PlainTermType::BlankNode => 1,
            PlainTermType::Literal => 2,
        }
    }
}

/// The instance of the [PlainTermEncoding].
///
/// As there is currently no way to parameterize the encoding, accessing it via this constant is
/// the preferred way.
pub static PLAIN_TERM_ENCODING: LazyLock<PlainTermEncodingRef> =
    LazyLock::new(|| Arc::new(PlainTermEncoding));

/// A cheaply cloneable reference to a [PlainTermEncoding].
pub type PlainTermEncodingRef = Arc<PlainTermEncoding>;

#[derive(Debug)]
pub struct PlainTermEncoding;

impl PlainTermEncoding {
    /// Returns the Arrow [Fields] of the [PlainTermEncoding].
    pub(crate) fn fields() -> Fields {
        FIELDS_TYPE.clone()
    }

    /// Returns the type of the [PlainTermEncoding].
    ///
    /// The type of the [PlainTermEncoding] is statically known and cannot be configured.
    pub fn data_type() -> DataType {
        DataType::Struct(Self::fields().clone())
    }

    /// Creates a [`PlainTermArray`] for the given number of null rows.
    pub fn create_null_array(&self, num_rows: usize) -> DFResult<ArrayRef> {
        let array = StructArray::new_null(Self::fields(), num_rows);
        Ok(Arc::new(array))
    }

    /// Creates a [`PlainTermArray`] for the given named nodes.
    ///
    /// Uses the null buffer of the given array.
    pub fn create_named_nodes_array(&self, named_nodes: StringArray) -> ArrayRef {
        let nulls = named_nodes.nulls().cloned();
        let len = named_nodes.len();
        let ids = UInt8Array::from_value(PlainTermType::NamedNode.into(), len);
        let array = StructArray::new(
            Self::fields(),
            vec![
                Arc::new(ids),
                Arc::new(named_nodes),
                Arc::new(StringArray::new_null(len)),
                Arc::new(StringArray::new_null(len)),
            ],
            nulls,
        );
        Arc::new(array)
    }

    /// Creates a [`PlainTermArray`] for the given strings.
    ///
    /// Uses the null buffer of the given array.
    pub fn create_string_array(&self, string_values: StringArray) -> ArrayRef {
        let nulls = string_values.nulls().cloned();
        let len = string_values.len();
        let ids = UInt8Array::from_value(PlainTermType::Literal.into(), len);
        let data_types = StringArray::new_repeated(xsd::STRING.as_str(), len);
        let array = StructArray::new(
            Self::fields(),
            vec![
                Arc::new(ids),
                Arc::new(string_values),
                Arc::new(data_types),
                Arc::new(StringArray::new_null(len)),
            ],
            nulls,
        );
        Arc::new(array)
    }

    /// Encodes the `term` as a [PlainTermScalar].
    pub fn encode_term(
        &self,
        term: ThinResult<TermRef<'_>>,
    ) -> DFResult<PlainTermScalar> {
        DefaultPlainTermEncoder.encode_term(term)
    }
}

impl TermEncoding for PlainTermEncoding {
    type Array = PlainTermArray;
    type Scalar = PlainTermScalar;

    fn name(&self) -> EncodingName {
        EncodingName::PlainTerm
    }

    fn data_type(&self) -> &DataType {
        static DATA_TYPE: LazyLock<DataType> =
            LazyLock::new(PlainTermEncoding::data_type);
        &DATA_TYPE
    }

    fn try_new_array(self: &Arc<Self>, array: ArrayRef) -> DFResult<Self::Array> {
        array.try_into()
    }

    fn try_new_scalar(self: &Arc<Self>, scalar: ScalarValue) -> DFResult<Self::Scalar> {
        scalar.try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_term_type_roundtrip() {
        test_roundtrip(PlainTermType::NamedNode);
        test_roundtrip(PlainTermType::BlankNode);
        test_roundtrip(PlainTermType::Literal);
    }

    fn test_roundtrip(term_field: PlainTermType) {
        let value: u8 = term_field.into();
        assert_eq!(term_field, value.try_into().unwrap());
    }
}
