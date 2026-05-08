use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::Int32Array;
use rdf_fusion_common::Numeric;
use rdf_fusion_compute::numeric::{NumericBinaryOp, apply_numeric_binary};
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

fn bench_numeric_binary(c: &mut Criterion) {
    let num_rows = 10_000;

    // Array setups
    let (homo_lhs, homo_rhs) = create_homogenous_arrays(num_rows);
    let (hetero_lhs, hetero_rhs) = create_heterogeneous_arrays(num_rows);

    // Scalar setup
    let scalar_rhs = NumericFamilyArray::new_int_scalar(5);

    let mut group = c.benchmark_group("NumericFamilyArray Binary Ops (10k rows)");

    // --- Array + Array Benchmarks ---

    group.bench_function("Array + Array: Fast Path (Homogenous)", |b| {
        b.iter(|| {
            apply_numeric_binary(
                black_box(&homo_lhs),
                black_box(&homo_rhs),
                NumericBinaryOp::Add,
            )
        })
    });

    group.bench_function("Array + Array: Slow Path (Heterogeneous)", |b| {
        b.iter(|| {
            apply_numeric_binary(
                black_box(&hetero_lhs),
                black_box(&hetero_rhs),
                NumericBinaryOp::Add,
            )
        })
    });

    // --- Array + Scalar Benchmarks ---

    group.bench_function("Array + Scalar: Fast Path (Homogenous)", |b| {
        b.iter(|| {
            apply_numeric_binary(
                black_box(&homo_lhs),
                black_box(&scalar_rhs),
                NumericBinaryOp::Add,
            )
        })
    });

    group.bench_function("Array + Scalar: Slow Path (Heterogeneous)", |b| {
        b.iter(|| {
            apply_numeric_binary(
                black_box(&hetero_lhs),
                black_box(&scalar_rhs),
                NumericBinaryOp::Add,
            )
        })
    });

    group.finish();
}

criterion_group!(benches, bench_numeric_binary);
criterion_main!(benches);
