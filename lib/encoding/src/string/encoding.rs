use crate::encoding::TermEncoding;
use crate::string::{StringEncodingArray, StringEncodingScalar};
use crate::{EncodingName, TermEncoder};
use datafusion::arrow::array::{Array, ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{ScalarValue, exec_err};
use rdf_fusion_model::{DFResult, TermRef, ThinResult};
use std::sync::{Arc, LazyLock};

/// The instance of the [StringEncoding].
pub static STRING_ENCODING: LazyLock<StringEncodingRef> =
    LazyLock::new(|| Arc::new(StringEncoding));

/// A cheaply cloneable reference to a [StringEncoding].
pub type StringEncodingRef = Arc<StringEncoding>;

#[derive(Debug)]
pub struct StringEncoding;

impl StringEncoding {
    /// Returns the type of the [StringEncoding].
    pub fn data_type() -> DataType {
        DataType::Utf8
    }

    /// Encodes the `term` as a [`StringEncodingScalar`].
    pub fn encode_term(
        &self,
        term: ThinResult<TermRef<'_>>,
    ) -> DFResult<StringEncodingScalar> {
        let value = match term {
            Ok(term) => Some(term.to_string()),
            Err(_) => None,
        };
        Ok(StringEncodingScalar::new_unchecked(ScalarValue::Utf8(
            value,
        )))
    }
}

impl TermEncoding for StringEncoding {
    type Array = StringEncodingArray;
    type Scalar = StringEncodingScalar;

    fn name(&self) -> EncodingName {
        EncodingName::String
    }

    fn data_type(&self) -> &DataType {
        static DATA_TYPE: LazyLock<DataType> = LazyLock::new(StringEncoding::data_type);
        &DATA_TYPE
    }

    fn try_new_array(self: &Arc<Self>, array: ArrayRef) -> DFResult<Self::Array> {
        if array.data_type() != self.data_type() {
            return exec_err!(
                "Expected array with StringEncoding (Utf8), got: {}",
                array.data_type()
            );
        }
        Ok(StringEncodingArray::new_unchecked(array))
    }

    fn try_new_scalar(self: &Arc<Self>, scalar: ScalarValue) -> DFResult<Self::Scalar> {
        if scalar.data_type() != *self.data_type() {
            return exec_err!(
                "Expected scalar with StringEncoding (Utf8), got: {}",
                scalar.data_type()
            );
        }
        Ok(StringEncodingScalar::new_unchecked(scalar))
    }
}

impl TermEncoder<StringEncoding> for StringEncoding {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        &self,
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<StringEncodingArray> {
        let mut builder = StringBuilder::new();
        for term in terms {
            match term {
                Ok(term) => builder.append_value(term.to_string()),
                Err(_) => builder.append_null(),
            }
        }
        Ok(StringEncodingArray::new_unchecked(Arc::new(
            builder.finish(),
        )))
    }

    fn encode_term(
        &self,
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<StringEncodingScalar> {
        self.encode_term(term)
    }
}

/// A list of arrays with the [StringEncoding].
#[derive(Debug, Clone)]
pub struct StringArgs {
    arrays: Vec<StringEncodingArray>,
}

impl StringArgs {
    /// Creates a new [StringArgs] from a list of arrays.
    pub fn new_unchecked(arrays: Vec<StringEncodingArray>) -> Self {
        Self { arrays }
    }

    /// Returns the array at `index`.
    pub fn get(&self, index: usize) -> &StringEncodingArray {
        &self.arrays[index]
    }
}
