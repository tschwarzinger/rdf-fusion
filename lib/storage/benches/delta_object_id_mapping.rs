use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use rand::prelude::SliceRandom;
use rdf_fusion_encoding::object_id::ObjectIdMapping;
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermArrayElementBuilder};
use rdf_fusion_encoding::typed_family::{TypedFamilyEncoding, TypedFamilyEncodingRef};
use rdf_fusion_model::{Literal, NamedNode, Term};
use rdf_fusion_storage::delta::objectids::DeltaObjectIdMapping;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_decode_array(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (mapping, shuffled_id_array) = rt.block_on(async {
        let mapping = DeltaObjectIdMapping::try_new_at_location(
            "memory:///",
            Arc::new(TypedFamilyEncoding::default()),
        )
        .await
        .unwrap();

        let plain_term_array = generate_mixed_term_array(10_000);

        let sequential_id_array = mapping
            .encode_array(&plain_term_array)
            .expect("Failed to encode plain term array");

        let int_array = sequential_id_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        let mut ids: Vec<Option<i64>> = int_array.iter().collect();

        let mut rng = rand::rng();
        ids.shuffle(&mut rng);

        let shuffled_array_ref: ArrayRef = Arc::new(Int64Array::from(ids));

        (mapping, shuffled_array_ref)
    });

    let mut group = c.benchmark_group("ObjectIdMapping");
    group.bench_function("decode_array_shuffled_10k_terms", |b| {
        b.iter(|| {
            let decoded = mapping.decode_array(black_box(&shuffled_id_array)).unwrap();
            black_box(decoded);
        })
    });
    group.finish();
}

fn bench_decode_array_to_typed_value(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (mapping, shuffled_id_array, encoding) = rt.block_on(async {
        let mapping = DeltaObjectIdMapping::try_new_at_location(
            "memory:///",
            Arc::new(TypedFamilyEncoding::default()),
        )
        .await
        .unwrap();

        let plain_term_array = generate_mixed_term_array(10_000);

        let sequential_id_array = mapping
            .encode_array(&plain_term_array)
            .expect("Failed to encode plain term array");

        let int_array = sequential_id_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        let mut ids: Vec<Option<i64>> = int_array.iter().collect();

        let mut rng = rand::rng();
        ids.shuffle(&mut rng);

        let shuffled_array_ref: ArrayRef = Arc::new(Int64Array::from(ids));

        let encoding: TypedFamilyEncodingRef = Arc::new(TypedFamilyEncoding::default());
        (mapping, shuffled_array_ref, encoding)
    });

    let mut group = c.benchmark_group("ObjectIdMapping");
    group.bench_function("decode_array_to_typed_value_shuffled_10k_terms", |b| {
        b.iter(|| {
            let decoded = mapping
                .decode_array_to_typed_family(&encoding, black_box(&shuffled_id_array))
                .unwrap();
            black_box(decoded);
        })
    });
    group.finish();
}

/// Benchmarks the encoding operation but on a store that already contain the terms.
fn bench_encode_array_existing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (mapping, plain_term_array) = rt.block_on(async {
        let mapping = DeltaObjectIdMapping::try_new_at_location(
            "memory:///",
            Arc::new(TypedFamilyEncoding::default()),
        )
        .await
        .unwrap();

        let plain_term_array = generate_mixed_term_array(10_000);

        mapping
            .encode_array(&plain_term_array)
            .expect("Failed to encode plain term array");

        (mapping, plain_term_array)
    });

    let mut group = c.benchmark_group("ObjectIdMapping");
    group.bench_function("encode_array_existing_10k_terms", |b| {
        b.iter(|| {
            let decoded = mapping.encode_array(black_box(&plain_term_array)).unwrap();
            black_box(decoded);
        })
    });
    group.finish();
}

/// Benchmarks the encoding operation but on a store that does not yet contain the terms. Therefore,
/// the encoding operation must also update the internal mappings.
fn bench_encode_array_non_existing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let plain_term_array = generate_mixed_term_array(10_000);

    let mut group = c.benchmark_group("ObjectIdMapping");
    group.bench_function("encode_array_non_existing_10k_terms", |b| {
        b.to_async(&rt).iter(async || {
            let mapping = DeltaObjectIdMapping::try_new_at_location(
                "memory:///",
                Arc::new(TypedFamilyEncoding::default()),
            )
            .await
            .unwrap();
            let decoded = mapping.encode_array(black_box(&plain_term_array)).unwrap();
            black_box(decoded);
        })
    });
    group.finish();
}

/// Helper function to generate a mixed array of URIs, simple strings, and integers
fn generate_mixed_term_array(num_terms: usize) -> PlainTermArray {
    let mut builder = PlainTermArrayElementBuilder::with_capacity(num_terms);

    for i in 0..num_terms {
        let term = match i % 3 {
            0 => Term::NamedNode(NamedNode::new_unchecked(format!("https://my.org/{i}"))),
            1 => Term::Literal(Literal::new_simple_literal(format!("string_{i}"))),
            _ => Term::Literal(Literal::new_typed_literal(
                format!("{i}"),
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        };
        builder.append_term(term.as_ref());
    }

    builder.finish()
}

criterion_group!(
    benches,
    bench_encode_array_existing,
    bench_encode_array_non_existing,
    bench_decode_array,
    bench_decode_array_to_typed_value,
);
criterion_main!(benches);
