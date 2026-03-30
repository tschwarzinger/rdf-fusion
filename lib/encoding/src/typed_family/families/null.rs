use crate::plain_term::PlainTermArray;
use crate::sortable_term::SortableTermArray;
use crate::typed_family::TypedFamilyId;
use crate::typed_family::families::{FamilyArray, TypeClaim, TypedFamily};
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, NullArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

/// A family that only stores Null values.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullFamily {}

static DATA_TYPE: LazyLock<DataType> = LazyLock::new(|| DataType::Null);
static CLAIM: LazyLock<TypeClaim> = LazyLock::new(|| TypeClaim::Null);

impl TypedFamily for NullFamily {
    type Array = NullFamilyArray;

    const FAMILY_ID: TypedFamilyId = TypedFamilyId::Null;

    fn data_type() -> &'static DataType {
        &DATA_TYPE
    }

    fn claim() -> &'static TypeClaim {
        &CLAIM
    }

    fn create_array_from_plain_term(
        array: &PlainTermArray,
    ) -> Result<NullFamilyArray, ArrowError> {
        let len = array.as_parts().struct_array.len();
        Ok(NullFamilyArray::new(len))
    }
}

impl Debug for NullFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(Self::FAMILY_ID.as_str())
    }
}

/// A family-specific array for the [`NullFamily`].
#[derive(Debug, Clone)]
pub struct NullFamilyArray {
    array: ArrayRef,
}

impl NullFamilyArray {
    /// Creates a new [`NullFamilyArray`] with the given length.
    pub fn new(length: usize) -> Self {
        Self {
            array: Arc::new(NullArray::new(length)) as ArrayRef,
        }
    }
}

impl FamilyArray for NullFamilyArray {
    type Family = NullFamily;

    fn from_array_unchecked(array: ArrayRef) -> Self {
        Self { array }
    }

    fn inner_ref(&self) -> &ArrayRef {
        &self.array
    }

    fn into_array_ref(self) -> ArrayRef {
        self.array
    }

    fn pretty_print(&self) -> Result<StringArray, ArrowError> {
        let len = self.inner_ref().len();
        Ok(StringArray::new_null(len))
    }

    fn effective_boolean_value(&self) -> Result<BooleanArray, ArrowError> {
        let len = self.inner_ref().len();
        Ok(BooleanArray::new_null(len))
    }

    fn literal_data_types(&self) -> Result<StringArray, ArrowError> {
        let len = self.inner_ref().len();
        Ok(StringArray::new_null(len))
    }

    fn cast_to_plain_term_array(&self) -> Result<PlainTermArray, ArrowError> {
        let len = self.inner_ref().len();
        Ok(PlainTermArray::new_null(len))
    }

    fn cast_to_sortable_array(&self) -> Result<SortableTermArray, ArrowError> {
        let len = self.inner_ref().len();
        Ok(SortableTermArray::new_null(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;

    #[test]
    fn test_null_family_pretty_print() {
        let array = Arc::new(NullArray::new(2)) as ArrayRef;
        let family_array = NullFamilyArray::from_array_unchecked(array);
        let pretty = family_array.pretty_print().unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("pretty", Arc::new(pretty) as ArrayRef)])
                .unwrap();
        let formatted = pretty_format_batches(&[batch]).unwrap().to_string();

        insta::assert_snapshot!(formatted, @r"
        +--------+
        | pretty |
        +--------+
        |        |
        |        |
        +--------+");
    }
}
