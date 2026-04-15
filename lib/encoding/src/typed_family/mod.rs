use crate::encoding::TermEncoding;
mod array;
mod builder;
mod encoding;
mod families;
mod id;
mod scalar;

use crate::EncodingArray;
pub use array::*;
pub use builder::*;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, UInt32Array, new_empty_array,
};
use datafusion::arrow::compute::{interleave, take};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
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
pub struct TypedFamilyArrays {
    arrays: Vec<TypedFamilyArray>,
}

impl TypedFamilyArrays {
    /// Creates a new [`TypedFamilyArrays`].
    pub fn new_unchecked(arrays: Vec<TypedFamilyArray>) -> Self {
        Self { arrays }
    }

    /// Returns the encoding of the arrays.
    pub fn encoding(&self) -> &TypedFamilyEncodingRef {
        self.arrays[0].encoding()
    }

    /// Returns the number of arrays.
    pub fn number_of_arrays(&self) -> usize {
        self.arrays.len()
    }

    /// Returns an iterator over the arrays.
    pub fn iter(&self) -> impl Iterator<Item = &TypedFamilyArray> {
        self.arrays.iter()
    }

    /// Returns the array at the given index.
    pub fn get(&self, index: usize) -> &TypedFamilyArray {
        &self.arrays[index]
    }

    /// Calls [`Self::map_children`] while providing mapping between [`ArrayRef`] and
    /// [`TypedFamilyArray`]. This helper makes it easier to define UDFs that map a typed family
    /// array to another typed family array.
    pub fn map_children_tf<F>(&self, mapping_function: F) -> AResult<TypedFamilyArray>
    where
        F: Fn(&[TypedFamilyArrayChild]) -> AResult<TypedFamilyArray>,
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
        F: Fn(TypedFamilyArrayChild) -> AResult<TypedFamilyArray>,
    {
        assert_eq!(self.arrays.len(), 1, "map_unary requires exactly one array");
        self.map_children_tf(|children| {
            mapping_function(TypedFamilyArrayChild {
                family: children[0].family.clone(),
                child: Arc::clone(&children[0].child),
            })
        })
    }

    /// Helper functions for calling [`Self::map_children_tf`] for mappings that accept two family
    /// children.
    pub fn map_children_tf_binary<F>(
        &self,
        mapping_function: F,
    ) -> AResult<TypedFamilyArray>
    where
        F: Fn(TypedFamilyArrayChild, TypedFamilyArrayChild) -> AResult<TypedFamilyArray>,
    {
        assert_eq!(
            self.arrays.len(),
            2,
            "map_binary requires exactly two arrays"
        );
        self.map_children_tf(|children| {
            mapping_function(
                TypedFamilyArrayChild {
                    family: children[0].family.clone(),
                    child: Arc::clone(&children[0].child),
                },
                TypedFamilyArrayChild {
                    family: children[1].family.clone(),
                    child: Arc::clone(&children[1].child),
                },
            )
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
                        .create_null_array(lhs.array().len())?
                        .into_array_ref(),
                ));
            }

            let comparator = lhs
                .family()
                .comparator(Arc::clone(lhs.array()), Arc::clone(rhs.array()));

            if let Some(comparator) = comparator {
                let len = lhs.array().len();
                let result = (0..len)
                    .map(|i| comparator(i, i).map(&ordering_to_boolean))
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
                        .create_null_array(lhs.array().len())?
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
            TypedFamilyArrayChild,
            TypedFamilyArrayChild,
            TypedFamilyArrayChild,
        ) -> AResult<TypedFamilyArray>,
    {
        assert_eq!(
            self.arrays.len(),
            3,
            "map_ternary requires exactly three arrays"
        );
        self.map_children_tf(|children| {
            mapping_function(
                TypedFamilyArrayChild {
                    family: children[0].family.clone(),
                    child: Arc::clone(&children[0].child),
                },
                TypedFamilyArrayChild {
                    family: children[1].family.clone(),
                    child: Arc::clone(&children[1].child),
                },
                TypedFamilyArrayChild {
                    family: children[2].family.clone(),
                    child: Arc::clone(&children[2].child),
                },
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
            TypedFamilyArrayChild,
            TypedFamilyArrayChild,
            TypedFamilyArrayChild,
            TypedFamilyArrayChild,
        ) -> AResult<TypedFamilyArray>,
    {
        assert_eq!(
            self.arrays.len(),
            4,
            "map_quaternary requires exactly four arrays"
        );
        self.map_children_tf(|children| {
            mapping_function(
                TypedFamilyArrayChild {
                    family: children[0].family.clone(),
                    child: Arc::clone(&children[0].child),
                },
                TypedFamilyArrayChild {
                    family: children[1].family.clone(),
                    child: Arc::clone(&children[1].child),
                },
                TypedFamilyArrayChild {
                    family: children[2].family.clone(),
                    child: Arc::clone(&children[2].child),
                },
                TypedFamilyArrayChild {
                    family: children[3].family.clone(),
                    child: Arc::clone(&children[3].child),
                },
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
        F: Fn(&[TypedFamilyArrayChild]) -> AResult<ArrayRef>,
    {
        let num_arrays = self.arrays.len();
        assert!(num_arrays > 0, "map_children requires at least one array");

        let len = self.arrays[0].inner().len();
        for a in &self.arrays[1..] {
            assert_eq!(a.inner().len(), len, "Arrays must have the same length");
        }

        if len == 0 {
            return Ok(new_empty_array(result_type));
        }

        if let Some(result) =
            self.try_map_children_fast_path(&mapping_function, &result_type, len)?
        {
            return Ok(result);
        }

        self.map_children_default_path(&mapping_function, result_type, len)
    }

    /// Implements map_children for the case where all elements of the family array are from a
    /// single family.
    fn try_map_children_fast_path<F>(
        &self,
        mapping_function: &F,
        result_type: &DataType,
        expected_length: usize,
    ) -> Result<Option<ArrayRef>, ArrowError>
    where
        F: Fn(&[TypedFamilyArrayChild]) -> AResult<ArrayRef>,
    {
        let mut homogenous_children = Vec::new();
        for array in &self.arrays {
            if let Some(array) = array.try_get_homogeneous_child() {
                homogenous_children.push(array);
            } else {
                return Ok(None);
            }
        }

        let result = mapping_function(homogenous_children.as_ref())?;
        Self::validate_mapping_result(result.as_ref(), result_type, expected_length)?;

        Ok(Some(result))
    }

    /// Implements map_children by creating new arrays for the possible combinations. This should
    /// work for all valid [`TypedFamilyArrays`] but is relatively compute intensive.
    fn map_children_default_path<F>(
        &self,
        mapping_function: &F,
        result_type: &DataType,
        len: usize,
    ) -> Result<ArrayRef, ArrowError>
    where
        F: Fn(&[TypedFamilyArrayChild]) -> AResult<ArrayRef>,
    {
        let unions: Vec<_> = self.arrays.iter().map(|a| a.inner().as_union()).collect();

        // Group rows by family combination
        let mut family_combinations: HashMap<Vec<i8>, Vec<u32>> = HashMap::new();

        for i in 0..len {
            let combination: Vec<i8> = unions.iter().map(|u| u.type_id(i)).collect();
            family_combinations
                .entry(combination)
                .or_default()
                .push(i as u32);
        }

        let mut results = Vec::new();
        let mut combination_to_result_idx = HashMap::new();

        for (combination, indices) in family_combinations {
            let mut children_arrays = Vec::with_capacity(self.arrays.len());

            for (array_idx, &type_id) in combination.iter().enumerate() {
                let u = unions[array_idx];

                // Extract offsets for this family child
                let offsets: UInt32Array = indices
                    .iter()
                    .map(|&i| u.value_offset(i as usize) as u32)
                    .collect();

                let child_raw = u.child(type_id);
                let child_inner = take(child_raw.as_ref(), &offsets, None)?;

                let family = self.encoding().type_families()[type_id as usize].clone();
                children_arrays.push(TypedFamilyArrayChild {
                    family,
                    child: child_inner,
                });
            }

            combination_to_result_idx.insert(combination, results.len());
            let mapping_result = mapping_function(&children_arrays)?;
            Self::validate_mapping_result(&mapping_result, result_type, indices.len())?;

            results.push(mapping_result);
        }

        // Interleave
        let mut interleave_indices = Vec::with_capacity(len);
        let mut combination_counters = HashMap::new();

        for i in 0..len {
            let combination: Vec<i8> = unions.iter().map(|u| u.type_id(i)).collect();
            let res_idx = *combination_to_result_idx.get(&combination).unwrap();
            let counter = combination_counters.entry(combination).or_insert(0);
            interleave_indices.push((res_idx, *counter));
            *counter += 1;
        }

        let arrays: Vec<&dyn Array> = results.iter().map(|r| r.as_ref()).collect();
        let interleaved = interleave(&arrays, &interleave_indices)?;

        Ok(interleaved)
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
pub struct TypedFamilyArrayChild {
    /// The typed family of this child.
    family: TypedFamilyRef,
    /// The child array.
    child: ArrayRef,
}

impl TypedFamilyArrayChild {
    /// Returns the typed family of this child.
    pub fn family(&self) -> &TypedFamilyRef {
        &self.family
    }

    /// Returns the child array.
    pub fn array(&self) -> &ArrayRef {
        &self.child
    }

    /// Downcasts the array for easier handling of the respective families.
    pub fn downcast(&self) -> DowncastTypedFamilyArray {
        match self.family.family_id() {
            TypedFamilyId::Null => DowncastTypedFamilyArray::Null(
                NullFamilyArray::from_array_unchecked(Arc::clone(&self.child)),
            ),
            TypedFamilyId::Resource => DowncastTypedFamilyArray::Resource(
                ResourceFamilyArray::from_array_unchecked(Arc::clone(&self.child)),
            ),
            TypedFamilyId::String => DowncastTypedFamilyArray::String(
                StringFamilyArray::from_array_unchecked(Arc::clone(&self.child)),
            ),
            TypedFamilyId::Boolean => DowncastTypedFamilyArray::Boolean(
                BooleanFamilyArray::from_array_unchecked(Arc::clone(&self.child)),
            ),
            TypedFamilyId::Numeric => DowncastTypedFamilyArray::Numeric(
                NumericFamilyArray::from_array_unchecked(Arc::clone(&self.child)),
            ),
            TypedFamilyId::DateTime => DowncastTypedFamilyArray::DateTime(
                DateTimeFamilyArray::from_array_unchecked(Arc::clone(&self.child)),
            ),
            TypedFamilyId::Duration => DowncastTypedFamilyArray::Duration(
                DurationFamilyArray::from_array_unchecked(Arc::clone(&self.child)),
            ),
            TypedFamilyId::Unknown => DowncastTypedFamilyArray::Unknown(
                UnknownFamilyArray::from_array_unchecked(Arc::clone(&self.child)),
            ),
            TypedFamilyId::Extension(_) => DowncastTypedFamilyArray::Extension(
                self.family.family_id(),
                Arc::clone(&self.child),
            ),
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
        let arrays = TypedFamilyArrays::new_unchecked(vec![array1]);

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
        let arrays = TypedFamilyArrays::new_unchecked(vec![array1]);

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
        let arrays = TypedFamilyArrays::new_unchecked(vec![array1]);

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

        let result = TypedFamilyArrays::new_unchecked(vec![array1])
            .map_children(
                |children| {
                    let child = &children[0];
                    let len = child.array().len();

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
