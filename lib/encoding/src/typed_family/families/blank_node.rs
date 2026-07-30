use crate::plain_term::{PlainTermArray, PlainTermType};
use crate::typed_family::families::{
    FamilyArray, FamilyComparator, TypeClaim, TypedFamily,
};
use crate::typed_family::{TypedFamilyId, make_null_aware_comparator};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, GenericBinaryBuilder,
    StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use rdf_fusion_common::AResult;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

/// A family that stores Blank Nodes.
///
/// # Layout
///
/// ```text
///  String Array
/// ┌────────┐
/// │ bnode1 │
/// │────────│
/// │ bnode2 │
/// │────────│
/// │ bnode3 │
/// └────────┘
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlankNodeFamily {}

static DATA_TYPE: LazyLock<DataType> = LazyLock::new(|| DataType::Utf8);
static CLAIM: LazyLock<TypeClaim> = LazyLock::new(|| TypeClaim::BlankNode);

impl TypedFamily for BlankNodeFamily {
    type Array = BlankNodeFamilyArray;

    const FAMILY_ID: TypedFamilyId = TypedFamilyId::BlankNode;

    fn data_type() -> &'static DataType {
        &DATA_TYPE
    }

    fn claim() -> &'static TypeClaim {
        &CLAIM
    }

    fn create_array_from_plain_term(
        array: &PlainTermArray,
    ) -> AResult<BlankNodeFamilyArray> {
        validate_input(array)?;

        let parts = array.as_parts();
        let values = parts.value;
        let mut builder =
            StringBuilder::with_capacity(values.len(), values.value_data().len());

        for i in 0..values.len() {
            if parts.struct_array.is_null(i) {
                builder.append_null();
                continue;
            }
            builder.append_value(values.value(i));
        }

        return Ok(BlankNodeFamilyArray::from_array_unchecked(Arc::new(
            builder.finish(),
        )));

        fn validate_input(array: &PlainTermArray) -> Result<(), ArrowError> {
            let parts = array.as_parts();
            for i in 0..parts.struct_array.len() {
                if parts.struct_array.is_null(i) {
                    continue;
                }
                let term_type =
                    PlainTermType::try_from(parts.term_type.value(i)).unwrap();
                if term_type != PlainTermType::BlankNode {
                    return Err(ArrowError::InvalidArgumentError(
                        "Not a blank node".to_string(),
                    ));
                }
            }
            Ok(())
        }
    }
}

impl Debug for BlankNodeFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(Self::FAMILY_ID.as_str())
    }
}

impl BlankNodeFamily {
    pub fn create_array(array: StringArray) -> AResult<BlankNodeFamilyArray> {
        Ok(BlankNodeFamilyArray::new(array))
    }
}

/// A family-specific array for the [`BlankNodeFamily`].
#[derive(Debug, Clone)]
pub struct BlankNodeFamilyArray {
    array: ArrayRef,
}

impl BlankNodeFamilyArray {
    /// Creates a new [`BlankNodeFamilyArray`].
    pub fn new(array: StringArray) -> Self {
        Self {
            array: Arc::new(array),
        }
    }

    /// Returns a reference to the inner [`StringArray`].
    pub fn inner_ref(array: &ArrayRef) -> &StringArray {
        array.as_string()
    }

    /// Returns a reference to the inner [`StringArray`].
    pub fn inner(&self) -> &StringArray {
        Self::inner_ref(&self.array)
    }
}

impl FamilyArray for BlankNodeFamilyArray {
    type Family = BlankNodeFamily;

    fn from_array_unchecked(array: ArrayRef) -> Self {
        Self { array }
    }

    fn inner_ref(&self) -> &ArrayRef {
        &self.array
    }

    fn into_array_ref(self) -> ArrayRef {
        self.array
    }

    fn comparator(&self, other: &Self) -> Option<FamilyComparator> {
        let lhs = self.inner().clone();
        let lhs_nulls = self.null_buffer();

        let rhs = other.inner().clone();
        let rhs_nulls = other.null_buffer();

        let inner: FamilyComparator = Box::new(move |lhs_idx, rhs_idx| {
            let lhs_val = lhs.value(lhs_idx);
            let rhs_val = rhs.value(rhs_idx);
            Some(lhs_val.cmp(rhs_val))
        });

        if lhs_nulls.null_count() > 0 || rhs_nulls.null_count() > 0 {
            Some(make_null_aware_comparator(lhs_nulls, rhs_nulls, inner))
        } else {
            Some(inner)
        }
    }

    fn pretty_print(&self) -> Result<StringArray, ArrowError> {
        Ok(self.inner().clone())
    }

    fn effective_boolean_value(&self) -> Result<BooleanArray, ArrowError> {
        Ok(BooleanArray::new_null(self.inner_ref().len()))
    }

    fn literal_data_types(&self) -> Result<StringArray, ArrowError> {
        Ok(StringArray::new_null(self.inner_ref().len()))
    }

    fn cast_to_plain_term_array(&self) -> Result<PlainTermArray, ArrowError> {
        let len = self.inner_ref().len();
        let term_type = datafusion::arrow::array::Int8Array::from(vec![
            PlainTermType::BlankNode
                as i8;
            len
        ]);
        let values = self.inner().clone();

        Ok(PlainTermArray::try_new(
            term_type,
            values,
            StringArray::new_null(len),
            StringArray::new_null(len),
            self.inner_ref().nulls().cloned(),
        )
        .unwrap())
    }

    fn cast_to_sortable_bytes(&self) -> Result<BinaryArray, ArrowError> {
        let mut builder = GenericBinaryBuilder::<i32>::with_capacity(
            self.inner_ref().len(),
            self.inner_ref().len() * 20,
        );
        for i in 0..self.inner_ref().len() {
            if self.array.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(self.inner().value(i).as_bytes());
            }
        }
        Ok(builder.finish())
    }
}
