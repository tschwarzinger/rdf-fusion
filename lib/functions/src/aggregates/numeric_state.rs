use datafusion::arrow::array::ArrayRef;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, FamilyDatum, TypedFamilyEncodingRef,
};
use rdf_fusion_model::{DFResult, Numeric, ThinError, ThinResult};
use std::sync::Arc;

/// Represents the state of a numeric aggregation for a signle group.
#[derive(Debug, Clone, Copy)]
pub struct NumericState {
    pub is_error: bool,
    pub value: Numeric,
}

impl NumericState {
    /// Creates a new [`NumericState`] with the given value.
    pub fn new_untouched_integer(value: i64) -> Self {
        NumericState {
            is_error: false,
            value: Numeric::Integer(value.into()),
        }
    }

    /// Creates a new [`NumericState`] that represents an error.
    pub fn error() -> Self {
        NumericState {
            is_error: true,
            value: Numeric::Int(0.into()),
        }
    }

    /// Updates the state for the sum aggregation with the given array.
    ///
    /// The parameter `ignore_nulls` determined whether null values should be ignored or cause an
    /// error.
    pub fn acc_sum(
        &mut self,
        encoding: &TypedFamilyEncodingRef,
        array: &ArrayRef,
        ignore_nulls: bool,
    ) -> DFResult<()> {
        if self.is_error {
            return Ok(());
        }

        let arr = encoding.try_new_array(Arc::clone(array))?;
        let typed_arrays = arr.non_empty_children();

        for child in typed_arrays {
            match child.as_downcast_array() {
                DowncastTypedFamilyArray::Null(_) if ignore_nulls => continue,
                DowncastTypedFamilyArray::Null(_) => {
                    self.is_error = true;
                }
                DowncastTypedFamilyArray::Numeric(numeric_child) => {
                    let sum_result = numeric_child.sum();
                    let (_, new_sum) = sum_result.get();

                    if let Some(sum) = new_sum.get_numeric_opt(0) {
                        match self.value.checked_add(sum) {
                            Ok(new_value) => self.value = new_value,
                            Err(_) => self.is_error = true, // Math overflow
                        }
                    }
                }
                _ => {
                    self.is_error = true;
                }
            }
        }

        Ok(())
    }

    /// Updates the state for the sum aggregation with the given array.
    ///
    /// The parameter `ignore_nulls` determined whether null values should be ignored or cause an
    /// error.
    pub fn acc_sum_single(&mut self, numeric: Numeric) {
        if self.is_error {
            return;
        }

        let result = self.value.checked_add(numeric);
        match result {
            Ok(new_result) => {
                self.value = new_result;
            }
            Err(_) => {
                self.is_error = true;
            }
        }
    }

    /// Recreates the [`Numeric`] while considering the error flag.
    pub fn to_numeric(&self) -> ThinResult<Numeric> {
        match self.is_error {
            false => Ok(self.value.clone()),
            true => ThinError::expected(),
        }
    }
}
