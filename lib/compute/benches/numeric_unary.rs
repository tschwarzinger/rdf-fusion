use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::DataType;
use rdf_fusion_common::Numeric;
use rdf_fusion_compute::numeric::cast_numeric;
use rdf_fusion_encoding::typed_family::{
    NumericFamilyArray, NumericFamilyArrayElementBuilder,
};
use std::hint::black_box;

/// Helper to generate a 10,000 row homogenous array (all Int32)
fn create_homogenous_arrays(len: usize) -> (NumericFamilyArray, NumericFamilyArray) {
    let lhs_inner = Int32Array::from_iter_values(0..len as i32);
    let rhs_inner = Int32Array::from_iter_values(0..len as i32);

    let lhs_array = NumericFamilyArray::new_ints(lhs_inner);
    let rhs_array = NumericFamilyArray::new_ints(rhs_inner);

    (lhs_array, rhs_array)
}

/// Helper to generate a 10,000 row heterogeneous array (mixed Ints and Floats)
fn create_heterogeneous_arrays(len: usize) -> (NumericFamilyArray, NumericFamilyArray) {
    let mut lhs_builder = NumericFamilyArrayElementBuilder::with_capacity(len);
    let mut rhs_builder = NumericFamilyArrayElementBuilder::with_capacity(len);

    for i in 0..len {
        if i % 2 == 0 {
            // Even rows: Int + Float
            lhs_builder.append_numeric(Numeric::Int((i as i32).into()));
            rhs_builder.append_numeric(Numeric::Float((i as f32).into()));
        } else {
            // Odd rows: Float + Int
            lhs_builder.append_numeric(Numeric::Float((i as f32).into()));
            rhs_builder.append_numeric(Numeric::Int((i as i32).into()));
        }
    }

    (lhs_builder.finish(), rhs_builder.finish())
}

fn bench_cast(c: &mut Criterion) {
    let num_rows = 10_000;

    let (homo_array, _) = create_homogenous_arrays(num_rows);
    let (hetero_array, _) = create_heterogeneous_arrays(num_rows);

    let target_type = DataType::Float64;

    let mut group = c.benchmark_group("NumericFamilyArray Cast (10k rows)");

    // --- Cast Benchmarks ---

    group.bench_function("Cast: Fast Path (Homogenous)", |b| {
        b.iter(|| {
            // The compiler requires we unwrap/handle the DFResult to avoid unused variable warnings,
            // but for criterion, we just return the result.
            cast_numeric(black_box(&homo_array), black_box(&target_type)).unwrap()
        })
    });

    group.bench_function("Cast: Slow Path (Heterogeneous)", |b| {
        b.iter(|| {
            cast_numeric(black_box(&hetero_array), black_box(&target_type)).unwrap()
        })
    });

    group.finish();
}

// Add both benchmark functions to the group
criterion_group!(benches, bench_cast);
criterion_main!(benches);
