use datafusion::arrow::array::{Array, ArrayRef, Int32Array};
use datafusion::arrow::compute::{cast, interleave, take};
use datafusion::arrow::datatypes::DataType;
use rdf_fusion_encoding::typed_family::{FamilyDatum, NumericFamilyArray};
use rdf_fusion_model::AResult;

/// Tries to cast a given numeric family array to the given target data type.
pub fn cast_numeric(
    value: &dyn FamilyDatum<NumericFamilyArray>,
    target_data_type: &DataType,
) -> AResult<ArrayRef> {
    let (_, value_array) = value.get();
    let union_array = value_array.union_array();

    // --- FAST PATH: Homogenous Array ---
    if let Some(type_id) = value_array.try_get_homogenous_type_id_for_fast_path() {
        let child = union_array.child(type_id);
        let offsets = union_array
            .offsets()
            .expect("Dense union must have offsets");

        // Offsets must be increasing for each array, so if these arrays have the same length, we
        // can just scan the inner.
        return if child.len() == offsets.len() {
            cast(&child, target_data_type)
        } else {
            let take_indices = Int32Array::new(offsets.clone(), None);
            let gathered_child = take(child, &take_indices, None)?;
            cast(&gathered_child, target_data_type)
        };
    }

    let arrays = [
        cast(value_array.floats(), target_data_type)?,
        cast(value_array.doubles(), target_data_type)?,
        cast(value_array.decimals(), target_data_type)?,
        cast(value_array.ints(), target_data_type)?,
        cast(value_array.integers(), target_data_type)?,
    ];

    let mut indices = Vec::with_capacity(union_array.len());
    for i in 0..union_array.len() {
        indices.push((union_array.type_id(i) as usize, union_array.value_offset(i)));
    }

    let arrays_ref: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
    let result = interleave(&arrays_ref, &indices)?;
    Ok(result)
}
