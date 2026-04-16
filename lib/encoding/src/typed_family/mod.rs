use crate::encoding::TermEncoding;
mod array;
mod builder;
mod datum;
mod encoding;
mod families;
mod id;
mod scalar;

use crate::{EncodingArray, EncodingDatum};
pub use array::*;
pub use builder::*;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, UInt32Array, new_empty_array,
};
use datafusion::arrow::compute::{interleave, take};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::logical_expr::ColumnarValue;
pub use encoding::*;
pub use families::*;
pub use id::*;
use rdf_fusion_model::AResult;
pub use scalar::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

/// Represents a non-empty list of [`TypedFamilyArray`]s of the same length. The arrays share the
/// same encoding.
#[derive(Clone)]
pub struct TypedFamilyArgs {
    /// The number of rows.
    number_rows: usize,
    /// The arrays.
    args: Vec<EncodingDatum<TypedFamilyEncoding>>,
}

impl TypedFamilyArgs {
    /// Creates a new [`TypedFamilyArgs`].
    pub fn new_unchecked(
        number_rows: usize,
        args: Vec<EncodingDatum<TypedFamilyEncoding>>,
    ) -> Self {
        Self { number_rows, args }
    }

    /// Returns the encoding of the arrays.
    pub fn encoding(&self) -> &TypedFamilyEncodingRef {
        self.args[0].encoding()
    }

    /// Returns the number of arrays.
    pub fn number_of_arrays(&self) -> usize {
        self.args.len()
    }

    /// Returns an iterator over the arrays.
    pub fn iter(&self) -> impl Iterator<Item = &EncodingDatum<TypedFamilyEncoding>> {
        self.args.iter()
    }

    /// Returns the array at the given index.
    pub fn get(&self, index: usize) -> TypedFamilyArray {
        self.args[index].to_array(self.number_rows)
    }

    /// Calls [`Self::map_children`] while providing mapping between [`ArrayRef`] and
    /// [`TypedFamilyArray`]. This helper makes it easier to define UDFs that map a typed family
    /// array to another typed family array.
    pub fn map_children_tf<F>(&self, mapping_function: F) -> AResult<TypedFamilyArray>
    where
        F: Fn(&[TypedFamilyChild]) -> AResult<TypedFamilyArray>,
    {
        let raw_result = self.map_children(
            |children| mapping_function(children).map(|res| res.into_array_ref()),
            self.encoding().data_type(),
        )?;

        Ok(TypedFamilyArray::new_unchecked(
            Arc::clone(self.encoding()),
            raw_result,
        ))
    }

    /// Helper functions for calling [`Self::map_children_tf`] for mappings that accept a single
    /// family child.
    pub fn map_children_tf_unary<F>(
        &self,
        mapping_function: F,
    ) -> AResult<TypedFamilyArray>
    where
        F: Fn(TypedFamilyChild) -> AResult<TypedFamilyArray>,
    {
        assert_eq!(self.args.len(), 1, "map_unary requires exactly one array");
        self.map_children_tf(|children| mapping_function(children[0].clone()))
    }

    /// Helper functions for calling [`Self::map_children_tf`] for mappings that accept two family
    /// children.
    pub fn map_children_tf_binary<F>(
        &self,
        mapping_function: F,
    ) -> AResult<TypedFamilyArray>
    where
        F: Fn(TypedFamilyChild, TypedFamilyChild) -> AResult<TypedFamilyArray>,
    {
        assert_eq!(self.args.len(), 2, "map_binary requires exactly two arrays");
        self.map_children_tf(|children| {
            mapping_function(children[0].clone(), children[1].clone())
        })
    }

    /// Applies a binary comparison operation to each matching family child of the first two arrays
    /// and interleaves the results.
    pub fn map_binary_comparison<F>(
        &self,
        ordering_to_boolean: F,
    ) -> AResult<TypedFamilyArray>
    where
        F: Fn(Ordering) -> bool + Send + Sync + 'static,
    {
        let encoding = Arc::clone(self.encoding());
        self.map_children_tf_binary(move |lhs, rhs| {
            if lhs.family().family_id() != rhs.family().family_id() {
                return Ok(TypedFamilyArray::new_unchecked(
                    Arc::clone(&encoding),
                    encoding
                        .create_null_array(lhs.number_rows)?
                        .into_array_ref(),
                ));
            }

            let family = lhs.family();
            let comparator = family.comparator(
                lhs.to_array_with_single_row_scalar(),
                rhs.to_array_with_single_row_scalar(),
            );

            if let Some(comparator) = comparator {
                let lhs_idx = match lhs.value_is_scalar() {
                    true => vec![0; lhs.number_rows],
                    false => (0..lhs.number_rows).collect(),
                };
                let rhs_idx = match rhs.value_is_scalar() {
                    true => vec![0; rhs.number_rows],
                    false => (0..rhs.number_rows).collect(),
                };

                let result = lhs_idx
                    .into_iter()
                    .zip(rhs_idx)
                    .map(|(lhs, rhs)| comparator(lhs, rhs).map(&ordering_to_boolean))
                    .collect::<BooleanArray>();
                Ok(TypedFamilyArray::new_unchecked(
                    Arc::clone(&encoding),
                    encoding
                        .create_array_from_family(BooleanFamilyArray::new(result))?
                        .into_array_ref(),
                ))
            } else {
                Ok(TypedFamilyArray::new_unchecked(
                    Arc::clone(&encoding),
                    encoding
                        .create_null_array(lhs.to_array().len())?
                        .into_array_ref(),
                ))
            }
        })
    }

    /// Helper functions for calling [`Self::map_children_tf`] for mappings that accept three family
    /// children.
    pub fn map_children_tf_ternary<F>(
        &self,
        mapping_function: F,
    ) -> AResult<TypedFamilyArray>
    where
        F: Fn(
            TypedFamilyChild,
            TypedFamilyChild,
            TypedFamilyChild,
        ) -> AResult<TypedFamilyArray>,
    {
        assert_eq!(
            self.args.len(),
            3,
            "map_ternary requires exactly three arrays"
        );
        self.map_children_tf(|children| {
            mapping_function(
                children[0].clone(),
                children[1].clone(),
                children[2].clone(),
            )
        })
    }

    /// Helper functions for calling [`Self::map_children_tf`] for mappings that accept four family
    /// children.
    pub fn map_children_tf_quaternary<F>(
        &self,
        mapping_function: F,
    ) -> AResult<TypedFamilyArray>
    where
        F: Fn(
            TypedFamilyChild,
            TypedFamilyChild,
            TypedFamilyChild,
            TypedFamilyChild,
        ) -> AResult<TypedFamilyArray>,
    {
        assert_eq!(
            self.args.len(),
            4,
            "map_quaternary requires exactly four arrays"
        );
        self.map_children_tf(|children| {
            mapping_function(
                children[0].clone(),
                children[1].clone(),
                children[2].clone(),
                children[3].clone(),
            )
        })
    }

    /// Applies an operation to each matching family child across all arrays and interleaves the
    /// results. Returns an empty array if the input is empty.
    ///
    /// The closure `f` is called for each family combination. The returned arrays from the closure
    /// are expected to have the same length as the input arrays. Furthermore, the returned arrays
    /// must all share the same data type, as otherwise the arrays cannot be interleaved.
    pub fn map_children<F>(
        &self,
        mapping_function: F,
        result_type: &DataType,
    ) -> AResult<ArrayRef>
    where
        F: Fn(&[TypedFamilyChild]) -> AResult<ArrayRef>,
    {
        if self.number_rows == 0 {
            return Ok(new_empty_array(result_type));
        }

        let num_arrays = self.args.len();
        assert!(num_arrays > 0, "map_children requires at least one array");

        for a in &self.args {
            if let EncodingDatum::Array(array) = a {
                assert_eq!(
                    array.len(),
                    self.number_rows,
                    "Arrays must have the same length"
                );
            }
        }

        if let Some(result) =
            self.try_map_children_fast_path(&mapping_function, result_type)?
        {
            return Ok(result);
        }

        self.map_children_default_path(&mapping_function, result_type)
    }

    /// Implements map_children for the case where all elements of the family array are from a
    /// single family.
    fn try_map_children_fast_path<F>(
        &self,
        mapping_function: &F,
        result_type: &DataType,
    ) -> Result<Option<ArrayRef>, ArrowError>
    where
        F: Fn(&[TypedFamilyChild]) -> AResult<ArrayRef>,
    {
        let mut homogenous_children = Vec::new();
        for array in &self.args {
            if let Some(array) = array.try_get_homogeneous_child(self.number_rows) {
                homogenous_children.push(array);
            } else {
                return Ok(None);
            }
        }

        let result = mapping_function(homogenous_children.as_ref())?;
        Self::validate_mapping_result(result.as_ref(), result_type, self.number_rows)?;

        Ok(Some(result))
    }

    /// Implements map_children by creating new arrays for the possible combinations. This should
    /// work for all valid [`TypedFamilyArgs`] but is relatively compute intensive.
    fn map_children_default_path<F>(
        &self,
        mapping_function: &F,
        result_type: &DataType,
    ) -> Result<ArrayRef, ArrowError>
    where
        F: Fn(&[TypedFamilyChild]) -> AResult<ArrayRef>,
    {
        let type_ids = self
            .args
            .iter()
            .map(|a| compute_type_ids(a, self.number_rows))
            .collect::<Vec<_>>();

        // Group rows by family combination
        let mut family_combinations: HashMap<Vec<i8>, Vec<u32>> = HashMap::new();

        for i in 0..self.number_rows {
            let combination: Vec<i8> = type_ids.iter().map(|u| u[i]).collect();
            family_combinations
                .entry(combination)
                .or_default()
                .push(i as u32);
        }

        let mut results = Vec::new();
        let mut combination_to_result_idx = HashMap::new();

        for (combination, indices) in family_combinations {
            let mut children = Vec::with_capacity(self.args.len());

            for (array_idx, &type_id) in combination.iter().enumerate() {
                let family = self.encoding().type_families()[type_id as usize].clone();
                match &self.args[array_idx] {
                    EncodingDatum::Array(array) => {
                        let union = array.inner().as_union();

                        // Extract offsets for this family child
                        let offsets: UInt32Array = indices
                            .iter()
                            .map(|&i| union.value_offset(i as usize) as u32)
                            .collect();

                        let child_raw = union.child(type_id);
                        let child_inner = take(child_raw.as_ref(), &offsets, None)?;

                        children.push(TypedFamilyChild {
                            family,
                            number_rows: indices.len(),
                            value: ColumnarValue::Array(child_inner),
                        });
                    }
                    EncodingDatum::Scalar(scalar) => children.push(TypedFamilyChild {
                        family,
                        number_rows: indices.len(),
                        value: ColumnarValue::Scalar(scalar.inner_value().clone()),
                    }),
                }
            }

            combination_to_result_idx.insert(combination, results.len());
            let mapping_result = mapping_function(&children)?;
            Self::validate_mapping_result(&mapping_result, result_type, indices.len())?;

            results.push(mapping_result);
        }

        // Interleave
        let mut interleave_indices = Vec::with_capacity(self.number_rows);
        let mut combination_counters = HashMap::new();

        for i in 0..self.number_rows {
            let combination: Vec<i8> =
                type_ids.iter().map(|type_ids| type_ids[i]).collect();
            let res_idx = *combination_to_result_idx.get(&combination).unwrap();
            let counter = combination_counters.entry(combination).or_insert(0);
            interleave_indices.push((res_idx, *counter));
            *counter += 1;
        }

        let arrays: Vec<&dyn Array> = results.iter().map(|r| r.as_ref()).collect();
        let interleaved = interleave(&arrays, &interleave_indices)?;

        return Ok(interleaved);

        /// Computes the type ids for each argument. For scalars, the type id is repeated.
        fn compute_type_ids(
            arg: &EncodingDatum<TypedFamilyEncoding>,
            number_rows: usize,
        ) -> Vec<i8> {
            match arg {
                EncodingDatum::Array(array) => array
                    .inner()
                    .as_union()
                    .type_ids()
                    .iter()
                    .copied()
                    .collect(),
                EncodingDatum::Scalar(scalar) => {
                    let type_id = scalar.type_id();
                    vec![type_id; number_rows]
                }
            }
        }
    }

    /// Validate the result of the mapping function.
    fn validate_mapping_result(
        result: &dyn Array,
        result_type: &DataType,
        expected_length: usize,
    ) -> Result<(), ArrowError> {
        if result.data_type() != result_type {
            return Err(ArrowError::ComputeError(format!(
                "The mapping function returned an array of data type {}, but expected {}",
                result.data_type(),
                result_type
            )));
        }

        if result.len() != expected_length {
            return Err(ArrowError::ComputeError(format!(
                "The mapping function returned an array of length {}, but expected {}",
                result.len(),
                expected_length
            )));
        }

        Ok(())
    }
}

/// Represents a child array of a [`TypedFamilyArray`], associated with the respective typed family.
///
/// This type does not guarantee that this child contains all elements of the parent array. See
/// [`TypedFamilyArray::non_empty_children`] and [`TypedFamilyArray::non_empty_consecutive_children`]
/// for more information.
#[derive(Clone)]
pub struct TypedFamilyChild {
    /// The typed family of this child.
    family: TypedFamilyRef,
    /// The number of rows in this child array.
    number_rows: usize,
    /// The value of this child.
    value: ColumnarValue,
}

impl TypedFamilyChild {
    /// Returns the typed family of this child.
    pub fn family(&self) -> &TypedFamilyRef {
        &self.family
    }

    /// Returns the value of this child.
    pub fn value(&self) -> &ColumnarValue {
        &self.value
    }

    /// Returns the number of rows in this child array.
    pub fn number_rows(&self) -> usize {
        self.number_rows
    }

    /// Returns whether the value of this child is a scalar.
    pub fn value_is_scalar(&self) -> bool {
        matches!(self.value, ColumnarValue::Scalar(_))
    }

    /// Returns the child array.
    pub fn to_array(&self) -> ArrayRef {
        self.value
            .to_array(self.number_rows)
            .expect("Scalar is convertible")
    }

    /// Returns the child array.
    pub fn to_array_with_single_row_scalar(&self) -> ArrayRef {
        match &self.value {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(scalar) => {
                scalar.to_array_of_size(1).expect("Scalar is convertible")
            }
        }
    }

    /// Converts all children to arrays and downcasts the result. This will expand literals to
    /// arrays.
    pub fn as_downcast_array(&self) -> DowncastTypedFamilyArray {
        let array = self.to_array();
        match self.family.family_id() {
            TypedFamilyId::Null => DowncastTypedFamilyArray::Null(
                NullFamilyArray::from_array_unchecked(array),
            ),
            TypedFamilyId::Resource => DowncastTypedFamilyArray::Resource(
                ResourceFamilyArray::from_array_unchecked(array),
            ),
            TypedFamilyId::String => DowncastTypedFamilyArray::String(
                StringFamilyArray::from_array_unchecked(array),
            ),
            TypedFamilyId::Boolean => DowncastTypedFamilyArray::Boolean(
                BooleanFamilyArray::from_array_unchecked(array),
            ),
            TypedFamilyId::Numeric => DowncastTypedFamilyArray::Numeric(
                NumericFamilyArray::from_array_unchecked(array),
            ),
            TypedFamilyId::DateTime => DowncastTypedFamilyArray::DateTime(
                DateTimeFamilyArray::from_array_unchecked(array),
            ),
            TypedFamilyId::Duration => DowncastTypedFamilyArray::Duration(
                DurationFamilyArray::from_array_unchecked(array),
            ),
            TypedFamilyId::Unknown => DowncastTypedFamilyArray::Unknown(
                UnknownFamilyArray::from_array_unchecked(array),
            ),
            TypedFamilyId::Extension(_) => {
                DowncastTypedFamilyArray::Extension(self.family.family_id(), array)
            }
        }
    }

    /// Downcasts the values of the child.
    pub fn downcast(&self) -> DowncastTypedFamilyDatum {
        let array = self.to_array_with_single_row_scalar();
        match self.family.family_id() {
            TypedFamilyId::Null => DowncastTypedFamilyDatum::Null(
                self.wrap_in_datum(NullFamilyArray::from_array_unchecked(array)),
            ),
            TypedFamilyId::Resource => DowncastTypedFamilyDatum::Resource(
                self.wrap_in_datum(ResourceFamilyArray::from_array_unchecked(array)),
            ),
            TypedFamilyId::String => DowncastTypedFamilyDatum::String(
                self.wrap_in_datum(StringFamilyArray::from_array_unchecked(array)),
            ),
            TypedFamilyId::Boolean => DowncastTypedFamilyDatum::Boolean(
                self.wrap_in_datum(BooleanFamilyArray::from_array_unchecked(array)),
            ),
            TypedFamilyId::Numeric => DowncastTypedFamilyDatum::Numeric(
                self.wrap_in_datum(NumericFamilyArray::from_array_unchecked(array)),
            ),
            TypedFamilyId::DateTime => DowncastTypedFamilyDatum::DateTime(
                self.wrap_in_datum(DateTimeFamilyArray::from_array_unchecked(array)),
            ),
            TypedFamilyId::Duration => DowncastTypedFamilyDatum::Duration(
                self.wrap_in_datum(DurationFamilyArray::from_array_unchecked(array)),
            ),
            TypedFamilyId::Unknown => DowncastTypedFamilyDatum::Unknown(
                self.wrap_in_datum(UnknownFamilyArray::from_array_unchecked(array)),
            ),
            TypedFamilyId::Extension(_) => DowncastTypedFamilyDatum::Extension(
                self.family.family_id(),
                self.value.clone(),
            ),
        }
    }

    fn wrap_in_datum<TArray: FamilyArray + 'static>(
        &self,
        array: TArray,
    ) -> Box<dyn FamilyDatum<TArray>> {
        match self.value_is_scalar() {
            true => Box::new(FamilyScalar::new(array)),
            false => Box::new(array),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BooleanArray, Int32Array, StringArray};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::util::pretty::pretty_format_columns;
    use insta::assert_snapshot;
    use std::sync::Arc;

    #[test]
    fn test_map_children_empty_array() {
        let encoding = create_dummy_encoding();
        let array1 = TypedFamilyArray::new_empty(Arc::clone(&encoding));
        let arrays =
            TypedFamilyArgs::new_unchecked(0, vec![EncodingDatum::Array(array1)]);

        let result = arrays
            .map_children(
                |_children| {
                    panic!("Mapping function should not be called for empty arrays");
                },
                &DataType::Int32,
            )
            .expect("Failed to map empty children");

        assert_eq!(result.len(), 0);
        assert_eq!(result.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_map_children_length_mismatch_error() {
        let encoding = create_dummy_encoding();
        let array1 = create_null_array_of_size(&encoding, 2);
        let arrays =
            TypedFamilyArgs::new_unchecked(2, vec![EncodingDatum::Array(array1)]);

        let result = arrays.map_children(
            |_children| Ok(Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            &DataType::Int32,
        );

        assert_eq!(
            result.unwrap_err().to_string(),
            "Compute error: The mapping function returned an array of length 1, but expected 2"
        );
    }

    #[test]
    fn test_map_children_type_mismatch_error() {
        let encoding = create_dummy_encoding();
        let array1 = create_null_array_of_size(&encoding, 2);
        let arrays =
            TypedFamilyArgs::new_unchecked(2, vec![EncodingDatum::Array(array1)]);

        let result = arrays.map_children(
            |_children| Ok(Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef),
            &DataType::Int32,
        );

        assert_eq!(
            result.unwrap_err().to_string(),
            "Compute error: The mapping function returned an array of data type Boolean, but expected Int32"
        );
    }

    #[test]
    fn test_map_children_successful_interleave() {
        let encoding = create_dummy_encoding();

        let iris_array =
            StringArray::from(vec!["https://example.com/1", "https://example.com/2"]);
        let resource_array =
            ResourceFamily::create_named_nodes_array(iris_array).unwrap();
        let array1 = TypedFamilyArrayBuilder::new(
            Arc::clone(&encoding),
            vec![0, 1, 0, 1],
            vec![0, 0, 1, 1],
        )
        .unwrap()
        .with_nulls(NullFamilyArray::new(2))
        .unwrap()
        .with_family_array(Some(resource_array))
        .unwrap()
        .finish()
        .unwrap();

        let result =
            TypedFamilyArgs::new_unchecked(4, vec![EncodingDatum::Array(array1)])
                .map_children(
                    |children| {
                        let child = &children[0];
                        let len = child.to_array().len();

                        let type_id = child.family().family_id();
                        let values = vec![type_id.as_str(); len];

                        Ok(Arc::new(StringArray::from(values)) as ArrayRef)
                    },
                    &DataType::Utf8,
                )
                .unwrap();

        let result = pretty_format_columns("result", &[result]).unwrap();
        assert_snapshot!(
            result,
            @"
        +----------------------+
        | result               |
        +----------------------+
        | rdf-fusion.null      |
        | rdf-fusion.resources |
        | rdf-fusion.null      |
        | rdf-fusion.resources |
        +----------------------+
        "
        )
    }

    /// Helper function to create a dummy encoding for testing purposes.
    fn create_dummy_encoding() -> TypedFamilyEncodingRef {
        Arc::new(TypedFamilyEncoding::default())
    }

    /// Helper function to create an array of all nulls.
    fn create_null_array_of_size(
        encoding: &TypedFamilyEncodingRef,
        size: usize,
    ) -> TypedFamilyArray {
        let size_i32 = i32::try_from(size).expect("Too long to represent as i32");
        TypedFamilyArrayBuilder::new(
            Arc::clone(encoding),
            vec![TypedFamilyEncoding::NULL_TYPE_ID; size],
            (0..size_i32).collect(),
        )
        .unwrap()
        .with_nulls(NullFamilyArray::new(size))
        .unwrap()
        .finish()
        .unwrap()
    }
}
