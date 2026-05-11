use crate::plain_term::{PLAIN_TERM_ENCODING, PlainTermArray, PlainTermType};
use crate::typed_family::families::{
    BooleanFamily, DateTimeFamily, DurationFamily, NullFamily, NumericFamily,
    ResourceFamily, StringFamily, TypedFamilyRef, UnknownFamily,
};
use crate::typed_family::{
    FamilyArray, NullFamilyArray, TypeClaim, TypedFamily, TypedFamilyArray,
};
use crate::typed_family::{TypedFamilyArrayBuilder, TypedFamilyId, TypedFamilyScalar};
use crate::{EncodingArray, EncodingName, TermEncoding};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, NullArray, UnionArray, new_empty_array,
};
use datafusion::arrow::compute::{filter, is_not_null, is_null, take};
use datafusion::arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{ScalarValue, exec_datafusion_err};
use rdf_fusion_common::{AResult, DFResult};
use std::clone::Clone;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// A cheaply clonable reference to a [`TypedFamilyEncoding`].
pub type TypedFamilyEncodingRef = Arc<TypedFamilyEncoding>;

/// The [`TypedFamilyEncoding`] stores the *value* of RDF terms within a so-called [`TypedFamily`].
/// Each family is responsible for a set or RDF terms. For example, the [`NumericFamily`] stores
/// the RDF literals `xsd:integer`, `xsd:float`, and so on. For more details, see the
/// [`TypedFamily`] documentation.
///
/// # Value Spaces
///
/// Each RDF literal type has an associated value space (e.g., `xsd:int` has the value space of
/// 32-bit integers). Transforming the RDF literals from the lexical space to the value space
/// might be a lossy transformation. For example, the two distinct RDF terms `"1"^^xsd::int` and
/// `"01"^^xsd::int` map to the same value. The [`TypedFamilyEncoding`] cannot distinguish between
/// these two terms and therefore should only be used for query parts that do not rely on this
/// distinction.
///
/// # Equality
///
/// Two typed value encodings are considered to be equal if the registered families are equal.
#[derive(Debug, Clone)]
pub struct TypedFamilyEncoding {
    /// The data type of this encoding instance.
    data_type: DataType,
    /// The registered families
    families: Vec<TypedFamilyRef>,
    /// Cache mapping datatype string to the family's index in `self.families`.
    cache: HashMap<String, i8, ahash::RandomState>,
}

impl TypedFamilyEncoding {
    /// The type id of the [`NullFamily`]
    pub const NULL_TYPE_ID: i8 = 0;

    /// Creates a new [`TypedFamilyEncoding`] with the default families installed.
    pub fn new() -> Self {
        let families = vec![
            TypedFamilyRef::new::<NullFamily>(),
            TypedFamilyRef::new::<ResourceFamily>(),
            TypedFamilyRef::new::<StringFamily>(),
            TypedFamilyRef::new::<BooleanFamily>(),
            TypedFamilyRef::new::<NumericFamily>(),
            TypedFamilyRef::new::<DateTimeFamily>(),
            TypedFamilyRef::new::<DurationFamily>(),
            TypedFamilyRef::new::<UnknownFamily>(),
        ];
        Self::new_with_families(families)
    }

    /// Creates a new [`TypedFamilyEncoding`] with the given families.
    fn new_with_families(families: Vec<TypedFamilyRef>) -> Self {
        let mut cache = HashMap::with_hasher(ahash::RandomState::new());

        for (i, family) in families.iter().enumerate() {
            if let TypeClaim::Literal(data_types) = family.claim() {
                for data_type in data_types {
                    cache.insert(data_type.as_str().to_owned(), i as i8);
                }
            }
        }

        Self {
            data_type: build_data_type(&families),
            families,
            cache,
        }
    }

    /// Returns the type id of the [`ResourceFamily`].
    pub fn resource_family_type_id(&self) -> i8 {
        1
    }

    /// Returns the type id of the [`ResourceFamily`].
    pub fn unknown_family_type_id(&self) -> i8 {
        7
    }

    /// Encodes terms from a [`PlainTermArray`] into a [`TypedFamilyArray`].
    ///
    /// If an element cannot be encoded from the given plain terms, the entire functions
    /// should error (the caller is responsible for filtering).
    pub fn cast_from_plain_term_array(
        self: &Arc<Self>,
        array: &PlainTermArray,
    ) -> AResult<TypedFamilyArray> {
        if array.is_empty() {
            return self.create_null_array(0);
        }

        let row_to_family = compute_row_to_family(self, array);

        if let Some(result) =
            try_cast_from_plain_term_array_fast_path(self, array, &row_to_family)?
        {
            return Ok(result);
        }

        let family_to_rows = compute_family_to_rows(self, &row_to_family);
        let family_arrays = create_array_for_families(self, array, &family_to_rows)?;
        let (final_type_ids, final_offsets, family_arrays) =
            extract_null_values(&row_to_family, family_arrays);

        return TypedFamilyArrayBuilder::new(
            Arc::clone(self),
            final_type_ids,
            final_offsets,
        )?
        .with_family_arrays(family_arrays)?
        .finish();

        /// Computes the mapping from row index to its corresponding type family ID.
        fn compute_row_to_family(
            encoding: &TypedFamilyEncoding,
            array: &PlainTermArray,
        ) -> Vec<i8> {
            let parts = array.as_parts();
            let len = parts.struct_array.len();
            let mut row_to_family = Vec::with_capacity(len);

            for i in 0..len {
                if parts.struct_array.is_null(i) {
                    row_to_family.push(TypedFamilyEncoding::NULL_TYPE_ID);
                    continue;
                }

                let term_type =
                    PlainTermType::try_from(parts.term_type.value(i)).unwrap();
                let datatype = parts.data_type.value(i);

                let family_info = match term_type {
                    PlainTermType::NamedNode | PlainTermType::BlankNode => {
                        encoding.resource_family_type_id()
                    }
                    PlainTermType::Literal => {
                        encoding.find_type_family_for_datatype(datatype).0
                    }
                };
                row_to_family.push(family_info);
            }

            row_to_family
        }

        /// Maps the precomputed family IDs back to their respective row indices.
        fn compute_family_to_rows(
            encoding: &TypedFamilyEncoding,
            row_to_family: &[i8],
        ) -> Vec<Vec<usize>> {
            let mut family_to_rows = vec![Vec::new(); encoding.families.len()];

            for (i, &family_id) in row_to_family.iter().enumerate() {
                family_to_rows[family_id as usize].push(i);
            }

            family_to_rows
        }

        /// Implements a specialized fast path that checks if all rows map to the same family.
        fn try_cast_from_plain_term_array_fast_path(
            encoding: &TypedFamilyEncodingRef,
            array: &PlainTermArray,
            row_to_family: &[i8],
        ) -> AResult<Option<TypedFamilyArray>> {
            let first_family = row_to_family[0];
            let all_same_family = row_to_family.iter().all(|&f| f == first_family);

            if !all_same_family {
                return Ok(None);
            }

            // Since all rows map to the exact same family, we can process the whole array at once
            if first_family == encoding.resource_family_type_id() {
                let family = ResourceFamily::create_array_from_plain_term(array)?;
                Ok(Some(encoding.create_array_from_family(family)?))
            } else {
                let family = &encoding.families[first_family as usize];
                let family_array = family.cast_from_plain_term_array(array)?;
                Ok(Some(encoding.create_array_with_single_family(
                    family.family_id(),
                    family_array,
                )?))
            }
        }

        /// Creates an array for each type family based on the given family to rows mapping.
        fn create_array_for_families(
            encoding: &TypedFamilyEncoding,
            input: &PlainTermArray,
            family_to_rows: &[Vec<usize>],
        ) -> DFResult<Vec<ArrayRef>> {
            let mut family_results = Vec::with_capacity(family_to_rows.len());

            for (tid, row_indices) in family_to_rows.iter().enumerate() {
                let family = &encoding.families[tid];

                if row_indices.is_empty() {
                    family_results.push(new_empty_array(family.data_type()));
                    continue;
                }

                let indices = datafusion::arrow::array::UInt32Array::from_iter_values(
                    row_indices.iter().map(|&i| i as u32),
                );

                let family_plain_term_array = PLAIN_TERM_ENCODING
                    .try_new_array(take(input.inner(), &indices, None)?)
                    .expect("Inner array is a PlainTermArray");

                let family_array =
                    family.cast_from_plain_term_array(&family_plain_term_array)?;
                family_results.push(family_array);
            }

            Ok(family_results)
        }

        /// Extract the null values from the family arrays and replace them with the global null array.
        fn extract_null_values(
            row_to_family: &[i8],
            family_arrays: Vec<ArrayRef>,
        ) -> (Vec<i8>, Vec<i32>, Vec<ArrayRef>) {
            let family_null_masks: Vec<BooleanArray> = family_arrays
                .iter()
                .map(|arr| is_null(arr.as_ref()).expect("Never fails"))
                .collect();
            let no_null_handling_necessary =
                family_null_masks.iter().all(|arr| arr.true_count() == 0);

            if no_null_handling_necessary {
                let mut family_offsets = vec![0; family_arrays.len()];
                let offsets = row_to_family
                    .iter()
                    .map(|tid| {
                        let offset = family_offsets[*tid as usize];
                        family_offsets[*tid as usize] += 1;
                        offset
                    })
                    .collect();
                return (row_to_family.to_vec(), offsets, family_arrays);
            }

            let len = row_to_family.len();
            let mut type_ids = Vec::with_capacity(len);
            let mut offsets = Vec::with_capacity(len);
            let mut family_counters = vec![0; family_arrays.len()];
            let mut family_offsets = vec![0; family_arrays.len()];

            for tid in row_to_family {
                let tid_usize = *tid as usize;
                let family_counter = family_counters[tid_usize];

                if *tid != TypedFamilyEncoding::NULL_TYPE_ID
                    && family_null_masks[tid_usize].value(family_counter)
                {
                    type_ids.push(TypedFamilyEncoding::NULL_TYPE_ID);
                    offsets.push(family_offsets[0]);
                    family_offsets[0] += 1;
                    family_counters[tid_usize] += 1;
                } else {
                    type_ids.push(*tid);
                    offsets.push(family_offsets[tid_usize]);
                    family_offsets[tid_usize] += 1;
                    family_counters[tid_usize] += 1;
                }
            }

            let family_arrays = family_arrays
                .into_iter()
                .enumerate()
                .map(|(tid, arr)| {
                    if tid == TypedFamilyEncoding::NULL_TYPE_ID as usize {
                        return Arc::new(NullArray::new(family_offsets[0] as usize))
                            as ArrayRef;
                    }
                    let mask = is_not_null(arr.as_ref()).expect("Never fails");
                    filter(arr.as_ref(), &mask).expect("Same size")
                })
                .collect();

            (type_ids, offsets, family_arrays)
        }
    }

    /// Returns the union fields of this encoding.
    pub fn union_fields(&self) -> &UnionFields {
        match &self.data_type {
            DataType::Union(fields, _) => fields,
            _ => unreachable!(),
        }
    }

    /// Returns the type families of this encoding.
    pub fn type_families(&self) -> &[TypedFamilyRef] {
        &self.families
    }

    /// Returns the number of registered type families.
    pub fn num_type_families(&self) -> usize {
        self.families.len()
    }

    /// Returns the type ID for the null family.
    pub fn null_type_id(&self) -> i8 {
        TypedFamilyEncoding::NULL_TYPE_ID
    }

    /// Returns the type ID for the resource family.
    pub fn resource_type_id(&self) -> i8 {
        self.find_typed_family_type_id(TypedFamilyId::Resource)
            .unwrap()
    }

    /// Returns the type ID for the unknown family.
    pub fn unknown_type_id(&self) -> i8 {
        self.find_typed_family_type_id(TypedFamilyId::Unknown)
            .unwrap()
    }

    /// Tries to find a registered [`TypedFamilyRef`] with the given [`TypedFamilyId`].
    pub fn find_typed_family(&self, id: TypedFamilyId) -> Option<(i8, &TypedFamilyRef)> {
        self.families
            .iter()
            .enumerate()
            .find(|(_, f)| f.family_id() == id)
            .map(|(i, f)| (i as i8, f))
    }

    /// Returns the type ID for the given [`TypedFamilyId`].
    pub fn find_typed_family_type_id(&self, id: TypedFamilyId) -> Option<i8> {
        self.find_typed_family(id).map(|(tid, _)| tid)
    }

    /// Tries to find a registered [`TypedFamilyRef`] that is responsible for the given `datatype`.
    pub fn find_type_family_for_datatype(&self, datatype: &str) -> (i8, &TypedFamilyRef) {
        let index = self
            .cache
            .get(datatype)
            .copied()
            .unwrap_or((self.families.len() - 1) as i8);
        let index_usize = index as usize;
        (index, &self.families[index_usize])
    }

    /// Creates a new [`TypedFamilyArray`] with the given number of nulls.
    pub fn create_null_array(self: &Arc<Self>, len: usize) -> AResult<TypedFamilyArray> {
        self.create_array_from_family(NullFamilyArray::new(len))
    }

    /// Creates a [`TypedFamilyScalar`] with a single type family.
    pub fn create_array_from_family<TArray: FamilyArray>(
        self: &Arc<Self>,
        array: TArray,
    ) -> AResult<TypedFamilyArray> {
        self.create_array_with_single_family(
            TArray::Family::FAMILY_ID,
            array.into_array_ref(),
        )
    }

    /// Creates a [`TypedFamilyArray`] with a single type family for all rows using a
    /// [`TypedFamilyId`].
    pub fn create_array_with_single_family(
        self: &Arc<Self>,
        family: TypedFamilyId,
        array: ArrayRef,
    ) -> AResult<TypedFamilyArray> {
        let (family_tid, _) = self
            .find_typed_family(family)
            .ok_or_else(|| exec_datafusion_err!("Family not found"))?;

        // Compute is_null to support UnionArray's
        let is_null = is_null(array.as_ref())?;
        if is_null.true_count() == 0 {
            let num_rows = array.len();
            let num_rows_i32 = i32::try_from(num_rows).map_err(|_| {
                ArrowError::ArithmeticOverflow("Array too long".to_owned())
            })?;
            let type_ids = vec![family_tid; num_rows];
            let offsets = (0..num_rows_i32).collect();

            return TypedFamilyArrayBuilder::new(Arc::clone(self), type_ids, offsets)?
                .with_array(family, Some(array))?
                .finish();
        }

        let mut type_ids = Vec::with_capacity(array.len());
        let mut offsets = Vec::with_capacity(array.len());
        let mut valid_indices = Vec::with_capacity(array.len());
        let mut null_count = 0;
        let mut family_count = 0;

        for i in 0..array.len() {
            let is_valid = if let DataType::Union(_, _) = array.data_type() {
                let union = array
                    .as_any()
                    .downcast_ref::<UnionArray>()
                    .expect("UnionArray");
                let type_id = union.type_id(i);
                let offset = union.value_offset(i);
                union.child(type_id).is_valid(offset)
            } else {
                array.is_valid(i)
            };

            if is_valid {
                type_ids.push(family_tid);
                offsets.push(family_count);
                family_count += 1;
                valid_indices.push(i as u32);
            } else {
                type_ids.push(TypedFamilyEncoding::NULL_TYPE_ID);
                offsets.push(null_count as i32);
                null_count += 1;
            }
        }

        let filtered_array = take(
            &array,
            &datafusion::arrow::array::UInt32Array::from(valid_indices),
            None,
        )?;

        TypedFamilyArrayBuilder::new(Arc::clone(self), type_ids, offsets)?
            .with_nulls(NullFamilyArray::new(null_count))?
            .with_array(family, Some(filtered_array))?
            .finish()
    }

    /// Creates a [`TypedFamilyScalar`] representing a null/unbound value.
    pub fn create_scalar_null(self: &Arc<Self>) -> TypedFamilyScalar {
        let scalar = ScalarValue::Union(
            Some((Self::NULL_TYPE_ID, Box::new(ScalarValue::Null))),
            self.union_fields().clone(),
            UnionMode::Dense,
        );

        TypedFamilyScalar::new_unchecked(Arc::clone(self), scalar)
    }

    /// Creates a [`TypedFamilyScalar`] with a single type family.
    pub fn create_scalar_from_family<TFamily: TypedFamily>(
        self: &Arc<Self>,
        scalar: ScalarValue,
    ) -> DFResult<TypedFamilyScalar> {
        let (family_tid, _) = self
            .find_typed_family(TFamily::FAMILY_ID)
            .ok_or_else(|| exec_datafusion_err!("Family not found"))?;

        let scalar = ScalarValue::Union(
            Some((family_tid, Box::new(scalar))),
            self.union_fields().clone(),
            UnionMode::Dense,
        );

        Ok(TypedFamilyScalar::new_unchecked(Arc::clone(self), scalar))
    }
}

impl PartialEq for TypedFamilyEncoding {
    fn eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type
    }
}

impl Eq for TypedFamilyEncoding {}

impl Hash for TypedFamilyEncoding {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.data_type.hash(state);
    }
}

/// Creates a [`DataType::Union`] for the given families.
///
/// The type ids in the underlying [`UnionArray`] are assigned in the same order as the given
/// families.
fn build_data_type(families: &[TypedFamilyRef]) -> DataType {
    let type_ids: Vec<i8> = (0..families.len() as i8).collect();
    let fields: Vec<Field> = families
        .iter()
        .map(|f| Field::new(f.family_id().as_str(), f.data_type().clone(), true))
        .collect();
    let union_fields =
        UnionFields::try_new(type_ids, fields).expect("Valid union fields");
    DataType::Union(union_fields, UnionMode::Dense)
}

impl Default for TypedFamilyEncoding {
    fn default() -> Self {
        TypedFamilyEncoding::new()
    }
}

impl TermEncoding for TypedFamilyEncoding {
    type Array = TypedFamilyArray;
    type Scalar = TypedFamilyScalar;

    fn name(&self) -> EncodingName {
        EncodingName::TypedFamily
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn try_new_array(self: &Arc<Self>, array: ArrayRef) -> DFResult<Self::Array> {
        Ok(TypedFamilyArray::try_new(Arc::clone(self), array)?)
    }

    fn try_new_scalar(self: &Arc<Self>, scalar: ScalarValue) -> DFResult<Self::Scalar> {
        TypedFamilyScalar::try_new(Arc::clone(self), scalar)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plain_term::PlainTermArrayElementBuilder;
    use crate::typed_family::NumericFamilyArray;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::util::pretty::pretty_format_columns;
    use insta::assert_snapshot;
    use rdf_fusion_common::LiteralRef;
    use rdf_fusion_common::vocab::xsd;

    #[test]
    fn test_create_array_with_single_family_with_nulls() {
        let encoding = Arc::new(TypedFamilyEncoding::new());

        let input_array = NumericFamilyArray::new_ints(Int32Array::from(vec![
            Some(10),
            None,
            Some(20),
            None,
        ]));
        let result = encoding
            .create_array_from_family(input_array)
            .expect("Failed to create array with nulls");

        let printed = pretty_format_columns("result", &[result.inner().clone()]).unwrap();
        assert_snapshot!(printed, @"
        +-------------------------------+
        | result                        |
        +-------------------------------+
        | {rdf-fusion.numeric={int=10}} |
        | {rdf-fusion.null=}            |
        | {rdf-fusion.numeric={int=20}} |
        | {rdf-fusion.null=}            |
        +-------------------------------+
        ");
    }

    #[test]
    fn test_cast_from_plain_term_array_with_invalid_lexical_values() {
        let encoding = Arc::new(TypedFamilyEncoding::new());
        let mut input = PlainTermArrayElementBuilder::new();
        input.append_literal(LiteralRef::new_typed_literal("123", xsd::INTEGER));
        input.append_literal(LiteralRef::new_typed_literal("abc", xsd::INTEGER));
        let input = input.finish();

        let result = encoding
            .cast_from_plain_term_array(&input)
            .expect("Failed to create array with nulls");

        let printed = pretty_format_columns("result", &[result.inner().clone()]).unwrap();
        assert_snapshot!(printed, @r"
        +------------------------------------+
        | result                             |
        +------------------------------------+
        | {rdf-fusion.numeric={integer=123}} |
        | {rdf-fusion.null=}                 |
        +------------------------------------+
        ");
    }
}
