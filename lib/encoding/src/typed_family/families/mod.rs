use crate::typed_family::TypedFamilyId;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::is_not_null;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use rdf_fusion_model::{AResult, NamedNode};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

mod boolean;
mod date_time;
mod duration;
mod null;
mod numeric;
mod resource;
mod string;
mod unknown;

use crate::plain_term::PlainTermArray;
use crate::sortable_term::SortableTermArray;
pub use boolean::*;
pub use date_time::*;
pub use duration::*;
pub use null::*;
pub use numeric::*;
pub use resource::*;
pub use string::*;
pub use unknown::*;

/// A trait for obtaining a family-specific array.
///
/// # Null Values
///
/// The family arrays *within* a [`TypedFamilyArray`] may never be null. However, outside of this
/// context, family arrays are allowed to contain null values. As a result, many operations on the
/// [`FamilyArray`]s may return null values. The encoding is then responsible for representing these
/// null values in the global null column.
pub trait FamilyArray: Sized + Send + Sync + Clone + Debug {
    /// A reference to the type family.
    type Family: TypedFamily<Array = Self>;

    /// Creates a [`FamilyArray`] from the given array.
    fn from_array_unchecked(array: ArrayRef) -> Self;

    /// Returns a reference to the inner [`ArrayRef`].
    fn inner_ref(&self) -> &ArrayRef;

    /// Consumes this [`FamilyArray`] and converts it into the [`ArrayRef`].
    fn into_array_ref(self) -> ArrayRef;

    /// Returns whether the value at the given index is null.
    ///
    /// The default implementation uses the [`is_not_null`] kernel for computing the [`NullBuffer`].
    /// This is necessary for arrays that do not contain a [`NullBuffer`] themselves (e.g., Union).
    /// Families may override this value to provide a more performant implementation.
    fn null_buffer(&self) -> NullBuffer {
        let is_null =
            is_not_null(self.inner_ref().as_ref()).expect("is_null does not error");
        assert_eq!(
            is_null.null_count(),
            0,
            "is_null should never return null values"
        );
        NullBuffer::from(is_null.values().clone())
    }

    /// Returns a comparator for the given arrays.
    ///
    /// For now, the comparator is only used within the context of a [`TypedFamilyArray`], thus
    /// ensuring that there are no null values in the arrays. Nevertheless, gracefully handling
    /// null values may become necessary in the future. Two null values should be considered equal.
    ///
    /// If the family does not support comparison, it returns `None`. Most operations (e.g., sort)
    /// will then try to select an arbitrary element from within the family as a representative.
    fn comparator(&self, _other: &Self) -> Option<FamilyComparator> {
        None
    }

    /// Returns a string representation of the given array.
    fn pretty_print(&self) -> Result<StringArray, ArrowError>;

    /// Returns the effective boolean value of the given array.
    fn effective_boolean_value(&self) -> Result<BooleanArray, ArrowError>;

    /// Returns the literal datatypes for the given array. Should return null for resources.
    fn literal_data_types(&self) -> Result<StringArray, ArrowError>;

    /// Returns the [`PlainTermArray`] for the given array.
    fn cast_to_plain_term_array(&self) -> Result<PlainTermArray, ArrowError>;

    /// Returns a [`SortableTermArray`] for the given array.
    fn cast_to_sortable_array(&self) -> Result<SortableTermArray, ArrowError>;
}

/// A helper function to make a comparator null-aware.
///
/// The returned comparator will treat `NULL` as the smallest element and return `Ordering::Equal`
/// if both elements are `NULL`.
pub fn make_null_aware_comparator(
    lhs_nulls: NullBuffer,
    rhs_nulls: NullBuffer,
    inner: FamilyComparator,
) -> FamilyComparator {
    Box::new(move |lhs_idx, rhs_idx| {
        let lhs_is_null = lhs_nulls.is_null(lhs_idx);
        let rhs_is_null = rhs_nulls.is_null(rhs_idx);
        match (lhs_is_null, rhs_is_null) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (false, false) => inner(lhs_idx, rhs_idx),
        }
    })
}

/// A closure that compares two values of the same family.
pub type FamilyComparator = Box<dyn Fn(usize, usize) -> Option<Ordering> + Send + Sync>;

/// A [`TypedFamily`] claims types for which to be responsible for.
///
/// For example, the [`BooleanFamily`] claims to be responsible for `xsd:boolean` values, while the
/// [`NumericFamily`] claims `xsd:integer`, `xsd:float`, etc.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TypeClaim {
    /// Claims the responsibility for IRIs and blank node identifiers.
    Resources,
    /// A claim for a set of literal types.
    Literal(BTreeSet<NamedNode>),
    /// A claim for all literal types that are not covered by other claims.
    UnknownLiterals,
    /// A claim for the null family.
    Null,
}

impl TypeClaim {
    /// Checks whether this type is responsible for the given datatype.
    pub fn is_responsible_for_datatype(&self, datatype: &str) -> bool {
        match self {
            TypeClaim::Resources => false,
            TypeClaim::Literal(types) => types.iter().any(|t| t.as_str() == datatype),
            TypeClaim::UnknownLiterals => true,
            TypeClaim::Null => false,
        }
    }

    /// Returns whether this claim is for a literal.
    pub fn is_literal(&self) -> bool {
        matches!(self, TypeClaim::Literal(_) | TypeClaim::UnknownLiterals)
    }
}

/// A type family groups together values of related types. Each family defines the encoding
/// of its types within the [`TypedFamilyEncoding`](crate::encoding::TypedFamilyEncoding).
///
/// Each type family "claims" the types that it is responsible for. See [`TypeClaim`].
pub trait TypedFamily: Debug + Send + Sync + 'static {
    /// The associated array for this family.
    type Array: FamilyArray<Family = Self>;

    /// The id of the typed value family.
    const FAMILY_ID: TypedFamilyId;

    /// Returns the data type that is used to encode the values of this type family.
    fn data_type() -> &'static DataType;

    /// Returns the set of claims of this type family.
    fn claim() -> &'static TypeClaim;

    /// Returns a [`NullBuffer`] and a dense family array from the given [`PlainTermArray`](PlainTermArray).
    ///
    /// If an element cannot be encoded in this family (e.g., wrong data type), the function should error.
    /// If the string representation is invalid, the resulting element should be null.
    fn create_array_from_plain_term(array: &PlainTermArray) -> AResult<Self::Array>;
}

/// A cheaply clonable reference to a [`TypedFamily`] that is dyn-compatible.
pub struct TypedFamilyRef(Box<dyn TypedFamilyErased>);

impl TypedFamilyRef {
    pub fn new<T: TypedFamily>() -> Self {
        TypedFamilyRef(Box::new(TypedFamilyRefInternal::<T>(PhantomData)))
    }

    pub fn family_id(&self) -> TypedFamilyId {
        self.0.family_id()
    }

    pub fn data_type(&self) -> &'static DataType {
        self.0.data_type()
    }

    pub fn claim(&self) -> &'static TypeClaim {
        self.0.claim()
    }

    pub fn cast_from_plain_term_array(
        &self,
        array: &PlainTermArray,
    ) -> AResult<ArrayRef> {
        self.0.cast_from_plain_term_array(array)
    }

    pub fn comparator(&self, lhs: ArrayRef, rhs: ArrayRef) -> Option<FamilyComparator> {
        self.0.comparator(lhs, rhs)
    }

    pub fn pretty_print(&self, array: ArrayRef) -> Result<StringArray, ArrowError> {
        self.0.pretty_print(array)
    }

    pub fn effective_boolean_value(
        &self,
        array: ArrayRef,
    ) -> Result<BooleanArray, ArrowError> {
        self.0.effective_boolean_value(array)
    }

    pub fn literal_data_types(&self, array: ArrayRef) -> Result<StringArray, ArrowError> {
        self.0.literal_data_types(array)
    }

    pub fn is_null(&self, array: ArrayRef) -> NullBuffer {
        self.0.is_null(array)
    }

    pub fn cast_to_plain_term_array(
        &self,
        array: ArrayRef,
    ) -> Result<PlainTermArray, ArrowError> {
        self.0.cast_to_plain_term_array(array)
    }

    pub fn cast_to_sortable_array(
        &self,
        array: ArrayRef,
    ) -> Result<SortableTermArray, ArrowError> {
        self.0.cast_to_sortable_array(array)
    }
}

impl Clone for TypedFamilyRef {
    fn clone(&self) -> Self {
        TypedFamilyRef(self.0.clone_box())
    }
}

impl Debug for TypedFamilyRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeFamilyRef")
            .field("family_id", &self.family_id())
            .finish()
    }
}

impl PartialEq for TypedFamilyRef {
    fn eq(&self, other: &Self) -> bool {
        self.family_id() == other.family_id()
    }
}

impl Eq for TypedFamilyRef {}

/// A type-erased version of [`TypedFamily`]
trait TypedFamilyErased: Send + Sync + 'static {
    fn family_id(&self) -> TypedFamilyId;
    fn data_type(&self) -> &'static DataType;
    fn claim(&self) -> &'static TypeClaim;
    fn cast_from_plain_term_array(&self, array: &PlainTermArray) -> AResult<ArrayRef>;

    fn comparator(&self, lhs: ArrayRef, rhs: ArrayRef) -> Option<FamilyComparator>;
    fn pretty_print(&self, array: ArrayRef) -> AResult<StringArray>;
    fn effective_boolean_value(&self, array: ArrayRef) -> AResult<BooleanArray>;
    fn literal_data_types(&self, array: ArrayRef) -> AResult<StringArray>;
    fn is_null(&self, array: ArrayRef) -> NullBuffer;
    fn cast_to_plain_term_array(&self, array: ArrayRef) -> AResult<PlainTermArray>;
    fn cast_to_sortable_array(&self, array: ArrayRef) -> AResult<SortableTermArray>;

    fn clone_box(&self) -> Box<dyn TypedFamilyErased>;
}

/// A helper struct that implements the [`TypedFamilyErased`] trait for a given [`TFamily`].
struct TypedFamilyRefInternal<TFamily: TypedFamily>(PhantomData<TFamily>);

impl<TFamily: TypedFamily> TypedFamilyErased for TypedFamilyRefInternal<TFamily> {
    fn family_id(&self) -> TypedFamilyId {
        TFamily::FAMILY_ID
    }

    fn data_type(&self) -> &'static DataType {
        TFamily::data_type()
    }

    fn claim(&self) -> &'static TypeClaim {
        TFamily::claim()
    }

    fn cast_from_plain_term_array(
        &self,
        array: &PlainTermArray,
    ) -> Result<ArrayRef, ArrowError> {
        TFamily::create_array_from_plain_term(array).map(|array| array.into_array_ref())
    }

    fn comparator(&self, lhs: ArrayRef, rhs: ArrayRef) -> Option<FamilyComparator> {
        let lhs = TFamily::Array::from_array_unchecked(lhs);
        let rhs = TFamily::Array::from_array_unchecked(rhs);
        lhs.comparator(&rhs)
    }

    fn pretty_print(&self, array: ArrayRef) -> Result<StringArray, ArrowError> {
        let array = TFamily::Array::from_array_unchecked(array);
        array.pretty_print()
    }

    fn effective_boolean_value(
        &self,
        array: ArrayRef,
    ) -> Result<BooleanArray, ArrowError> {
        let array = TFamily::Array::from_array_unchecked(array);
        array.effective_boolean_value()
    }

    fn literal_data_types(&self, array: ArrayRef) -> Result<StringArray, ArrowError> {
        let array = TFamily::Array::from_array_unchecked(array);
        array.literal_data_types()
    }

    fn is_null(&self, array: ArrayRef) -> NullBuffer {
        let array = TFamily::Array::from_array_unchecked(array);
        array.null_buffer()
    }

    fn cast_to_plain_term_array(
        &self,
        array: ArrayRef,
    ) -> Result<PlainTermArray, ArrowError> {
        let array = TFamily::Array::from_array_unchecked(array);
        array.cast_to_plain_term_array()
    }

    fn cast_to_sortable_array(
        &self,
        array: ArrayRef,
    ) -> Result<SortableTermArray, ArrowError> {
        let array = TFamily::Array::from_array_unchecked(array);
        array.cast_to_sortable_array()
    }

    fn clone_box(&self) -> Box<dyn TypedFamilyErased> {
        Box::new(TypedFamilyRefInternal::<TFamily>(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::buffer::BooleanBuffer;

    #[test]
    fn test_make_null_aware_comparator() {
        let lhs_nulls = NullBuffer::from(BooleanBuffer::from(vec![false, true]));
        let rhs_nulls = NullBuffer::from(BooleanBuffer::from(vec![false, true]));

        let inner: FamilyComparator = Box::new(|_, _| None);
        let cmp = make_null_aware_comparator(lhs_nulls.clone(), rhs_nulls.clone(), inner);

        assert_eq!(cmp(0, 0), Some(Ordering::Equal));
        assert_eq!(cmp(0, 1), Some(Ordering::Less));
        assert_eq!(cmp(1, 0), Some(Ordering::Greater));
        assert_eq!(cmp(1, 1), None);
    }
}
