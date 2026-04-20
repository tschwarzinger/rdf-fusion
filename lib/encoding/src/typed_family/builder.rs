use crate::TermEncoding;
use crate::typed_family::{
    FamilyArray, NullFamilyArray, TypedFamily, TypedFamilyArray, TypedFamilyEncodingRef,
    TypedFamilyId,
};
use datafusion::arrow::array::{ArrayRef, UnionArray, new_null_array};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::error::ArrowError;
use rdf_fusion_model::AResult;
use std::sync::Arc;

/// A builder for creating a [`TypedFamilyArray`].
#[derive(Debug, Clone)]
pub struct TypedFamilyArrayBuilder {
    encoding: TypedFamilyEncodingRef,
    type_ids: ScalarBuffer<i8>,
    offsets: ScalarBuffer<i32>,
    arrays: Vec<Option<ArrayRef>>,
}

impl TypedFamilyArrayBuilder {
    /// Creates a new [`TypedFamilyArrayBuilder`].
    pub fn new(
        encoding: TypedFamilyEncodingRef,
        type_ids: Vec<i8>,
        offsets: Vec<i32>,
    ) -> AResult<Self> {
        let num_families = encoding.num_type_families();
        Ok(Self {
            encoding,
            type_ids: ScalarBuffer::from(type_ids),
            offsets: ScalarBuffer::from(offsets),
            arrays: vec![None; num_families],
        })
    }

    /// Sets the entire list of family arrays.
    pub fn with_family_arrays(self, family_arrays: Vec<ArrayRef>) -> AResult<Self> {
        if self.encoding.num_type_families() != family_arrays.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Expected {} arrays, got {}",
                self.encoding.num_type_families(),
                family_arrays.len()
            )));
        }

        let type_families = self.encoding.type_families().to_vec();
        let mut result = self;
        for (family, array) in type_families.iter().zip(family_arrays) {
            result = result.with_array(family.family_id(), Some(array))?;
        }
        Ok(result)
    }

    /// Sets the null array for this builder.
    pub fn with_nulls(self, array: NullFamilyArray) -> AResult<Self> {
        self.with_family_array(Some(array))
    }

    /// Sets the array for the given type family.
    ///
    /// The given `array` must match the data type of the `type_family`.
    pub fn with_family_array<TArray: FamilyArray>(
        self,
        array: Option<TArray>,
    ) -> AResult<Self> {
        self.with_array(
            TArray::Family::FAMILY_ID,
            array.map(|arr| arr.into_array_ref()),
        )
    }

    /// Sets the array for the given [`TypedFamilyId`].
    pub fn with_array(
        mut self,
        id: TypedFamilyId,
        array: Option<ArrayRef>,
    ) -> AResult<Self> {
        let (type_id, family) = self.encoding.find_typed_family(id).ok_or(
            ArrowError::InvalidArgumentError(format!(
                "Type family {id} not found in encoding {}",
                self.encoding.name()
            )),
        )?;

        if let Some(array) = &array {
            if array.data_type() != family.data_type() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Type family {} has data type {:?} but array has data type {:?}",
                    family.family_id(),
                    family.data_type(),
                    array.data_type()
                )));
            }
        }

        self.arrays[type_id as usize] = array;
        Ok(self)
    }

    /// Builds the [`TypedFamilyArray`].
    ///
    /// This will fill all missing arrays with null arrays.
    pub fn finish(self) -> AResult<TypedFamilyArray> {
        let mut child_arrays = Vec::with_capacity(self.arrays.len());
        for (i, array) in self.arrays.into_iter().enumerate() {
            if let Some(array) = array {
                child_arrays.push(array);
            } else {
                let family = &self.encoding.type_families()[i];
                child_arrays.push(new_null_array(family.data_type(), 0));
            }
        }

        let union_array = UnionArray::try_new(
            self.encoding.union_fields().clone(),
            self.type_ids,
            Some(self.offsets),
            child_arrays,
        )
        .expect("Typed fmaily buiasd");

        Ok(TypedFamilyArray::new_unchecked(
            self.encoding,
            Arc::new(union_array) as ArrayRef,
        ))
    }
}
