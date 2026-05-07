use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use rand::prelude::SliceRandom;
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_encoding::object_id::ObjectIdMapping;
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermArrayElementBuilder};
use rdf_fusion_encoding::typed_family::{TypedFamilyEncoding, TypedFamilyEncodingRef};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_model::{Literal, NamedNode, Term};
use rdf_fusion_storage::delta::DeltaQuadStorageBuilder;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_decode_array(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let term_types = vec![Some("string"), Some("integer")];

    let mut group = c.benchmark_group("ObjectIdMapping_Decode");

    for term_type in term_types {
        let (mapping, shuffled_id_array) =
            rt.block_on(setup_encoded_shuffled_array(10_000, term_type));
        let name = term_type.unwrap_or("mixed");

        group.bench_function(format!("decode_{}_10k", name), |b| {
            b.iter(|| {
                let decoded =
                    mapping.decode_array(black_box(&shuffled_id_array)).unwrap();
                black_box(decoded);
            })
        });
    }
    group.finish();
}

fn bench_decode_array_to_typed_family(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let term_types = vec![None, Some("uri"), Some("string"), Some("integer")];

    let mut group = c.benchmark_group("ObjectIdMapping_DecodeTypedFamily");

    for term_type in term_types {
        let (mapping, shuffled_id_array) =
            rt.block_on(setup_encoded_shuffled_array(10_000, term_type));
        let encoding: TypedFamilyEncodingRef = Arc::new(TypedFamilyEncoding::default());
        let name = term_type.unwrap_or("mixed");

        group.bench_function(format!("decode_to_typed_family_{}_10k", name), |b| {
            b.iter(|| {
                let decoded = mapping
                    .decode_array_to_typed_family(
                        &encoding,
                        black_box(&shuffled_id_array),
                    )
                    .unwrap();
                black_box(decoded);
            })
        });
    }
    group.finish();
}

/// Benchmarks the encoding operation but on a store that already contain the terms.
fn bench_encode_array_existing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (mapping, plain_term_array) = rt.block_on(async {
        let mapping = create_mapping().await;
        let plain_term_array = generate_term_array(10_000, None);

        mapping
            .encode_array(&plain_term_array)
            .expect("Failed to encode plain term array");

        (mapping, plain_term_array)
    });

    let mut group = c.benchmark_group("ObjectIdMapping_Encode");
    group.bench_function("encode_array_existing_10k_terms", |b| {
        b.iter(|| {
            let decoded = mapping.encode_array(black_box(&plain_term_array)).unwrap();
            black_box(decoded);
        })
    });
    group.finish();
}

/// Benchmarks the encoding operation but on a store that does not yet contain the terms.
fn bench_encode_array_non_existing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let plain_term_array = generate_term_array(10_000, None);

    let mut group = c.benchmark_group("ObjectIdMapping_Encode");
    group.bench_function("encode_array_non_existing_10k_terms", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mapping = create_mapping().await;
                let decoded = mapping.encode_array(black_box(&plain_term_array)).unwrap();
                black_box(decoded);
            })
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_encode_array_existing,
    bench_encode_array_non_existing,
    bench_decode_array,
    bench_decode_array_to_typed_family,
);
criterion_main!(benches);

async fn create_mapping() -> Arc<dyn ObjectIdMapping> {
    let storage = DeltaQuadStorageBuilder::new()
        .with_encoding(QuadStorageEncodingName::ObjectId)
        .build()
        .await
        .unwrap();
    let encoding = storage.encoding().object_id_encoding().unwrap().clone();
    encoding.mapping().clone()
}

/// Generates a PlainTermArray. If `term_type` is None, generates a mixed array.
fn generate_term_array(num_terms: usize, term_type: Option<&str>) -> PlainTermArray {
    let mut builder = PlainTermArrayElementBuilder::with_capacity(num_terms);

    for i in 0..num_terms {
        let term = match term_type {
            None => match i % 3 {
                0 => Term::NamedNode(NamedNode::new_unchecked(format!(
                    "https://my.org/{i}"
                ))),
                1 => Term::Literal(Literal::new_simple_literal(format!("string_{i}"))),
                _ => Term::Literal(Literal::new_typed_literal(
                    format!("{i}"),
                    NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
                )),
            },
            Some("uri") | Some("iri") => {
                Term::NamedNode(NamedNode::new_unchecked(format!("https://my.org/{i}")))
            }
            Some("string") => {
                Term::Literal(Literal::new_simple_literal(format!("string_{i}")))
            }
            Some("integer") => Term::Literal(Literal::new_typed_literal(
                format!("{i}"),
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
            _ => panic!("Unknown term type: {:?}", term_type),
        };
        builder.append_term(term.as_ref());
    }

    builder.finish()
}

/// Helper to setup the mapping, encode the generated terms, and shuffle the resulting IDs.
async fn setup_encoded_shuffled_array(
    num_terms: usize,
    term_type: Option<&str>,
) -> (Arc<dyn ObjectIdMapping>, ArrayRef) {
    let mapping = create_mapping().await;
    let plain_term_array = generate_term_array(num_terms, term_type);

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

    (mapping, Arc::new(Int64Array::from(ids)) as ArrayRef)
}
