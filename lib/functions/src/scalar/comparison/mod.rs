mod compare;
mod is_compatible;

pub use compare::*;
pub use is_compatible::*;

#[cfg(test)]
mod test_utils {
    use crate::test_utils::create_standard_test_vector;
    use datafusion::arrow::array::{ArrayRef, UInt32Array};
    use rdf_fusion_encoding::typed_family::TypedFamilyEncodingRef;

    /// Creates a test vector specialized for binary comparison functions.
    ///
    /// Selects a subset of type combinations from the standard test vector.
    pub(crate) fn create_comparison_test_vector(
        encoding: &TypedFamilyEncodingRef,
    ) -> (ArrayRef, ArrayRef) {
        let standard_vector = create_standard_test_vector(encoding);

        // Indices mapped from `create_standard_test_vector`:
        // 0: Null
        // 1: Named Node,
        // 2: Blank Node
        // 4: Int(10),
        // 5: Float(10.0),
        // 6: Float(0.0)
        // 10: String("b1"),
        // 11: String("just a string"),
        // 12: String("hello"@en)
        // 14: DateTime
        let index_pairs = vec![
            (0, 0),
            (0, 4),
            (1, 1),
            (1, 2),
            (4, 4),
            (4, 5),
            (5, 6),
            (4, 10),
            (10, 10),
            (10, 11),
            (12, 12),
            (12, 10),
            (14, 14),
        ];

        let (left_indices, right_indices): (Vec<u32>, Vec<u32>) = index_pairs
            .into_iter()
            .map(|(l, r)| (l as u32, r as u32))
            .unzip();

        let left = datafusion::arrow::compute::take(
            &standard_vector,
            &UInt32Array::from(left_indices),
            None,
        )
        .unwrap();

        let right = datafusion::arrow::compute::take(
            &standard_vector,
            &UInt32Array::from(right_indices),
            None,
        )
        .unwrap();

        (left, right)
    }
}
