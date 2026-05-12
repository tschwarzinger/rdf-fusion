use crate::encoding::EncodingArray;
use crate::plain_term::{PLAIN_TERM_ENCODING, PlainTermArray};
use crate::typed_family::{
    BooleanFamilyArray, DateTimeFamilyArray, DurationFamilyArray, FamilyArray,
    NullFamilyArray, NumericFamilyArray, ResourceFamilyArray, StringFamilyArray,
    TypedFamilyArgs, TypedFamilyChild, TypedFamilyEncoding, TypedFamilyEncodingRef,
    TypedFamilyId, UnknownFamilyArray,
};
use crate::{EncodingDatum, TermEncoding};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, GenericBinaryBuilder,
    Int32Array, StringArray, UInt32Array, new_empty_array,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_common::AResult;
use std::iter::repeat_n;
use std::sync::Arc;

/// Represents an Arrow array with a [`TypedFamilyEncoding`].
#[derive(Debug, Clone)]
pub struct TypedFamilyArray {
    /// The typed family encoding of this array.
    encoding: TypedFamilyEncodingRef,
    /// The Arrow array.
    inner: ArrayRef,
}

impl TypedFamilyArray {
    /// Tries to create a new [`TypedFamilyArray`] from the given `array` and `encoding`.
    ///
    /// Returns an error if the array does not match the given encoding.
    pub fn try_new(encoding: TypedFamilyEncodingRef, array: ArrayRef) -> AResult<Self> {
        if array.data_type() != encoding.data_type() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Expected array with TypedFamilyEncoding, got {:?}",
                array.data_type()
            )));
        }

        Ok(Self::new_unchecked(encoding, array))
    }

    /// Creates a new [`TypedFamilyArray`] without verifying the schema.
    pub fn new_unchecked(encoding: TypedFamilyEncodingRef, array: ArrayRef) -> Self {
        Self {
            encoding,
            inner: array,
        }
    }

    /// Creates a new empty [`TypedFamilyArray`].
    pub fn new_empty(encoding: TypedFamilyEncodingRef) -> Self {
        let array = new_empty_array(encoding.data_type());
        Self::new_unchecked(encoding, array)
    }

    /// Returns the type ids of the array.
    pub fn type_ids(&self) -> &ScalarBuffer<i8> {
        self.inner.as_union().type_ids()
    }

    /// Returns the length of the array.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if the array is homogeneous (i.e., all entries have the same type).
    pub fn is_homogeneous(&self) -> bool {
        let type_ids = self.type_ids();
        let first_type_id = type_ids[0];
        type_ids.iter().all(|&id| id == first_type_id)
    }

    /// Returns a homogenous inner child if the array is homogenous. Otherwise, returns [`None`].
    pub fn try_get_homogeneous_child(&self) -> Option<TypedFamilyChild> {
        if !self.is_homogeneous() {
            return None;
        }

        let type_id = self.type_ids()[0];
        let offsets = self
            .inner
            .as_union()
            .offsets()
            .expect("Expected Dense Union");

        let candidate_child = self.inner.as_union().child(type_id);
        if offsets.len() != candidate_child.len() {
            return None;
        }

        // The offsets buffer is guaranteed to be increasing for each type. Therefore, given that
        // the child array has the same length as the offset array, we do not have to use take here.
        //
        // From the Arrow documentation:
        // > Offsets buffer: A buffer of signed Int32 values indicating the relative offset into the
        // > respective child array for the type in a given slot. The respective offsets for each
        // > child value array must be in order / increasing.
        // https://arrow.apache.org/docs/format/Columnar.html#dense-union
        Some(TypedFamilyChild {
            family: self.encoding.type_families()[type_id as usize].clone(),
            number_rows: self.len(),
            value: ColumnarValue::Array(Arc::clone(candidate_child)),
        })
    }

    /// Returns the child array for the given [`TypedFamilyId`].
    ///
    /// # Panics
    ///
    /// Panics if the child array does not exist.
    pub fn child_for_family_id(&self, id: TypedFamilyId) -> &ArrayRef {
        let type_id = self
            .encoding
            .find_typed_family_type_id(id)
            .expect("Child array should exist for given family id");
        self.inner.as_union().child(type_id)
    }

    /// Returns the effective boolean value of this array.
    pub fn effective_boolean_value(&self) -> Result<BooleanArray, ArrowError> {
        let array = self.map_unary(
            |child| {
                child
                    .family()
                    .effective_boolean_value(child.to_array())
                    .map(|array| Arc::new(array) as ArrayRef)
            },
            &DataType::Boolean,
        );
        array.map(|arr| arr.as_boolean().clone())
    }

    /// Returns the pretty printed values of this array.
    pub fn pretty_print(&self) -> Result<StringArray, ArrowError> {
        let array = self.map_unary(
            |child| {
                child
                    .family()
                    .pretty_print(child.to_array())
                    .map(|array| Arc::new(array) as ArrayRef)
            },
            &DataType::Utf8,
        );
        array.map(|arr| arr.as_string().clone())
    }

    /// Returns the datatype values of this array.
    pub fn literal_data_types(&self) -> Result<StringArray, ArrowError> {
        let array = self.map_unary(
            |child| {
                child
                    .family()
                    .literal_data_types(child.to_array())
                    .map(|array| Arc::new(array) as ArrayRef)
            },
            &DataType::Utf8,
        );
        array.map(|arr| arr.as_string().clone())
    }

    /// Returns the language tags of this array.
    ///
    /// Only the string family provides language tags. For all other families, the result is null.
    pub fn language_tags(&self) -> Result<StringArray, ArrowError> {
        let array = self.map_unary(
            |child| match child.as_downcast_array() {
                DowncastTypedFamilyArray::String(string_family) => {
                    Ok(Arc::clone(string_family.language_array_ref()))
                }
                _ => Ok(Arc::new(StringArray::new_null(child.number_rows))),
            },
            &DataType::Utf8,
        );
        array.map(|arr| arr.as_string().clone())
    }

    /// Returns a boolean array indicating whether each entry is a literal.
    pub fn is_literal(&self) -> Result<BooleanArray, ArrowError> {
        let array = self.map_unary(
            |child| match child.as_downcast_array() {
                DowncastTypedFamilyArray::Null(null_array) => {
                    Ok(Arc::clone(null_array.inner_ref()))
                }
                DowncastTypedFamilyArray::Resource(_) => Ok(Arc::new(
                    BooleanArray::from_iter(repeat_n(false, child.number_rows)),
                )),
                _ => Ok(Arc::new(BooleanArray::from_iter(repeat_n(
                    true,
                    child.number_rows,
                )))),
            },
            &DataType::Boolean,
        );
        array.map(|arr| arr.as_boolean().clone())
    }

    /// Returns the plain term representation of this array.
    pub fn as_plain_term_array(&self) -> Result<PlainTermArray, ArrowError> {
        let array = self.map_unary(
            |child| {
                child
                    .family
                    .cast_to_plain_term_array(child.to_array())
                    .map(|array| array.into_array_ref())
            },
            PLAIN_TERM_ENCODING.data_type(),
        );
        array.map(PlainTermArray::new_unchecked)
    }

    /// Returns a [`BinaryArray`] containing the sortable bytes for each entry.
    pub fn as_sortable_bytes(&self) -> Result<BinaryArray, ArrowError> {
        let array = self.map_unary(
            |child| {
                let family_id = child.family().family_id();
                let type_id = self
                    .encoding
                    .find_typed_family_type_id(family_id)
                    .expect("Family should exist");
                let sortable_bytes =
                    child.family().cast_to_sortable_array(child.to_array())?;

                let mut builder = GenericBinaryBuilder::<i32>::new();
                for i in 0..sortable_bytes.len() {
                    let mut row = Vec::with_capacity(1 + sortable_bytes.value(i).len());
                    row.push(type_id as u8);
                    row.extend_from_slice(sortable_bytes.value(i));
                    builder.append_value(&row);
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            },
            &DataType::Binary,
        );
        Ok(array?.as_binary::<i32>().clone())
    }

    /// Splits the array by its type families.
    ///
    /// It is not guaranteed that the child arrays are consecutive in the array.
    pub fn non_empty_children(&self) -> Vec<TypedFamilyChild> {
        let union_array = self.inner.as_union();
        let len = union_array.len();
        if len == 0 {
            return Vec::new();
        }

        // The union array may have unused elements in the children arrays. This is indicated by the
        // type_id and offset buffers. This can happen when, for example, slicing a UnionArray.
        let type_ids = union_array.type_ids();
        let offsets = union_array.offsets().expect("Expected Dense Union");
        let mut family_to_offsets = vec![Vec::new(); self.encoding.type_families().len()];
        for i in 0..len {
            family_to_offsets[type_ids[i] as usize].push(offsets[i] as u32);
        }

        self.encoding
            .type_families()
            .iter()
            .enumerate()
            .filter_map(|(i, family)| {
                let tid = i as i8;
                let child_offsets = std::mem::take(&mut family_to_offsets[i]);
                if child_offsets.is_empty() {
                    return None;
                }

                let child_raw = union_array.child(tid);
                let child_offsets_array = UInt32Array::from(child_offsets);
                let child_inner = take(child_raw.as_ref(), &child_offsets_array, None)
                    .expect("Failed to narrow child array in non_empty_children");

                Some(TypedFamilyChild {
                    family: family.clone(),
                    number_rows: child_inner.len(),
                    value: ColumnarValue::Array(child_inner),
                })
            })
            .collect()
    }

    /// Convenience function for calling [`TypedFamilyArgs::map_children`] with a single argument.
    pub fn map_unary<F>(&self, f: F, target_type: &DataType) -> AResult<ArrayRef>
    where
        F: Fn(TypedFamilyChild) -> AResult<ArrayRef>,
    {
        self.as_unary_args().map_children(
            |children| {
                if children.len() != 1 {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Expected 1 child for map_unary, got {}",
                        children.len()
                    )));
                }

                let child = children.first().unwrap();
                f(child.clone())
            },
            target_type,
        )
    }

    /// Convenience function for calling [`TypedFamilyArgs::map_children_tf_unary`].
    pub fn map_unary_tf<F>(&self, f: F) -> AResult<TypedFamilyArray>
    where
        F: Fn(TypedFamilyChild) -> AResult<TypedFamilyArray>,
    {
        self.as_unary_args().map_children_tf_unary(f)
    }

    /// Internal access to the inner array.
    pub fn inner(&self) -> &ArrayRef {
        &self.inner
    }

    /// Creates unary arguments from this array.
    fn as_unary_args(&self) -> TypedFamilyArgs {
        let cloned = TypedFamilyArray::new_unchecked(
            Arc::clone(&self.encoding),
            Arc::clone(&self.inner),
        );
        TypedFamilyArgs::new_unchecked(
            self.inner.len(),
            vec![EncodingDatum::Array(cloned)],
        )
    }
}

/// A downcast family array.
pub enum DowncastTypedFamilyArray {
    /// The null family.
    Null(NullFamilyArray),
    /// The resource family.
    Resource(ResourceFamilyArray),
    /// The string family.
    String(StringFamilyArray),
    /// The boolean family.
    Boolean(BooleanFamilyArray),
    /// The numeric family.
    Numeric(NumericFamilyArray),
    /// The date-time family.
    DateTime(DateTimeFamilyArray),
    /// The duration family.
    Duration(DurationFamilyArray),
    /// The unknown family.
    Unknown(UnknownFamilyArray),
    /// An extension family which can be downcast by the respective extension.
    Extension(TypedFamilyId, ArrayRef),
}

/// A downcast family array.
pub enum DowncastTypedFamilyDatum {
    /// The null family.
    Null(Box<dyn FamilyDatum<NullFamilyArray>>),
    /// The resource family.
    Resource(Box<dyn FamilyDatum<ResourceFamilyArray>>),
    /// The string family.
    String(Box<dyn FamilyDatum<StringFamilyArray>>),
    /// The boolean family.
    Boolean(Box<dyn FamilyDatum<BooleanFamilyArray>>),
    /// The numeric family.
    Numeric(Box<dyn FamilyDatum<NumericFamilyArray>>),
    /// The date-time family.
    DateTime(Box<dyn FamilyDatum<DateTimeFamilyArray>>),
    /// The duration family.
    Duration(Box<dyn FamilyDatum<DurationFamilyArray>>),
    /// The unknown family.
    Unknown(Box<dyn FamilyDatum<UnknownFamilyArray>>),
    /// An extension family which can be downcast by the respective extension.
    Extension(TypedFamilyId, ColumnarValue),
}

/// A scalar whose value is represented as an array of length one.
pub struct FamilyScalar<TArray: FamilyArray> {
    /// A family array of length one.
    inner: TArray,
}

impl<TArray: FamilyArray> FamilyScalar<TArray> {
    /// Creates a new [`FamilyScalar`].
    ///
    /// # Panics
    ///
    /// Panics if the given array does not have a length of one.
    pub fn new(inner: TArray) -> Self {
        assert_eq!(
            inner.inner_ref().len(),
            1,
            "Scalar must have a length of one"
        );
        Self { inner }
    }

    /// Creates a scalar value from this family scalar.
    pub fn to_scalar_value(&self) -> ScalarValue {
        ScalarValue::try_from_array(self.inner.inner_ref(), 0)
            .expect("Scalar must have a length of one")
    }
}

/// A family array that is either a regular array or a scalar.
///
/// This is inspired by the arrow-rs's `Scalar` trait but is used for typed family arrays.
pub trait FamilyDatum<TArray: FamilyArray> {
    /// Returns the inner array and an indicator whether the array is a scalar or not.
    fn get(&self) -> (bool, &TArray);
}

impl<TArray: FamilyArray> FamilyDatum<TArray> for TArray {
    fn get(&self) -> (bool, &TArray) {
        (false, self)
    }
}

impl<TArray: FamilyArray> FamilyDatum<TArray> for FamilyScalar<TArray> {
    fn get(&self) -> (bool, &TArray) {
        (true, &self.inner)
    }
}

/// A list of helper functions for working with typed family data. See [`FamilyDatum`] for more
/// information.
pub trait FamilyDatumExt<TArray: FamilyArray> {
    /// Creates an array from this datum.
    fn to_array(&self, number_rows: usize) -> AResult<TArray>;

    /// Returns a list of indices that can be used for indexing `number_rows` elements of this
    /// datum. Errors if the number of rows does not match the array length, and this is not a
    /// scalar.
    fn indices_of_length(&self, number_rows: usize) -> AResult<Vec<usize>>;
}

impl<TArray: FamilyArray> FamilyDatumExt<TArray> for dyn FamilyDatum<TArray> + '_ {
    fn to_array(&self, number_rows: usize) -> AResult<TArray> {
        let (is_scalar, array) = self.get();
        match is_scalar {
            false => Ok(array.clone()),
            true => {
                let indices = Int32Array::from(vec![0; number_rows]);
                let result = take(&array.inner_ref(), &indices, None)?;
                Ok(TArray::from_array_unchecked(result))
            }
        }
    }

    fn indices_of_length(&self, number_rows: usize) -> AResult<Vec<usize>> {
        let (is_scalar, array) = self.get();
        match is_scalar {
            false => {
                if array.inner_ref().len() != number_rows {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "The number of rows ({}) does not match the array length ({})",
                        number_rows,
                        array.inner_ref().len()
                    )));
                }
                Ok((0..number_rows).collect())
            }
            true => Ok(vec![0; number_rows]),
        }
    }
}

impl EncodingArray for TypedFamilyArray {
    type Encoding = TypedFamilyEncoding;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plain_term::PlainTermArray;
    use crate::typed_family::{TypedFamilyArrayBuilder, TypedFamilyEncoding};
    use datafusion::arrow::array::{BooleanArray, Int64Array};

    #[test]
    fn test_typed_family_array_child() {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let array = BooleanFamilyArray::new(BooleanArray::from(vec![true, false]));
        let tf_array = encoding.create_array_from_family(array).unwrap();

        let children = tf_array.non_empty_children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].to_array().len(), 2);
    }

    #[test]
    fn test_typed_family_array_mixed() {
        use crate::typed_family::families::{BooleanFamilyArray, NumericFamilyArray};
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let bool_type_id = encoding
            .find_typed_family_type_id(TypedFamilyId::Boolean)
            .unwrap();
        let numeric_type_id = encoding
            .find_typed_family_type_id(TypedFamilyId::Numeric)
            .unwrap();

        let type_ids = vec![bool_type_id, numeric_type_id, bool_type_id];
        let offsets = vec![0, 0, 1];

        let bool_array = Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef;
        let numeric_array = NumericFamilyArray::new_integers(Int64Array::from(vec![42]));

        let tf_array = TypedFamilyArrayBuilder::new(encoding, type_ids, offsets)
            .unwrap()
            .with_family_array(Some(BooleanFamilyArray::from_array_unchecked(bool_array)))
            .unwrap()
            .with_family_array(Some(numeric_array))
            .unwrap()
            .finish()
            .unwrap();

        assert_eq!(tf_array.inner().len(), 3);

        let ebv = tf_array.effective_boolean_value().unwrap();
        assert_eq!(ebv.len(), 3);
        assert_eq!(ebv.value(0), true); // true
        assert_eq!(ebv.value(1), true); // 42 is not zero
        assert_eq!(ebv.value(2), false); // false

        let pretty = tf_array.pretty_print().unwrap();
        assert_eq!(pretty.value(0), "true");
        assert_eq!(pretty.value(1), "42");
        assert_eq!(pretty.value(2), "false");
    }

    #[test]
    fn test_typed_family_from_plain_term_with_null() {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let plain_term_array = PlainTermArray::new_null(2);
        assert!(
            encoding
                .cast_from_plain_term_array(&plain_term_array)
                .is_ok()
        );
    }

    /// This caused a bug as the old implementation did not consider the union information and
    /// simply returned the results.
    #[test]
    fn test_non_empty_children_on_sliced_array() {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let numeric_type_id = encoding
            .find_typed_family_type_id(TypedFamilyId::Numeric)
            .unwrap();

        // Create an array with two integers: [1, 2]
        let type_ids = vec![numeric_type_id, numeric_type_id];
        let offsets = vec![0, 1];
        let numeric_array =
            NumericFamilyArray::new_integers(Int64Array::from(vec![1, 2]));

        let tf_array = TypedFamilyArrayBuilder::new(encoding, type_ids, offsets)
            .unwrap()
            .with_family_array(Some(numeric_array))
            .unwrap()
            .finish()
            .unwrap();

        // Slice the array to only include the second element: [2]
        let sliced_array = TypedFamilyArray::new_unchecked(
            tf_array.encoding().clone(),
            tf_array.inner().slice(1, 1),
        );

        let children = sliced_array.non_empty_children();
        assert_eq!(children.len(), 1);

        let child = &children[0];
        assert_eq!(child.family().family_id(), TypedFamilyId::Numeric);
        assert_eq!(child.to_array().len(), 1);
    }
}
