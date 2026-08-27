use criterion::{Criterion, criterion_group, criterion_main};
use rand::prelude::SliceRandom;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;

use datafusion::arrow::array::Int64Array;
use rdf_fusion_common::{Literal, NamedNode, Term};
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermArrayElementBuilder};
use rdf_fusion_storage::local_object_ids::{
    LocalObjectIdDictionary, RedbObjectIdDictionaryBuilder, StaticObjectIdClaimer,
};

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
            _ => panic!("Unknown term type: {term_type:?}"),
        };
        builder.append_term(term.as_ref());
    }
    builder.finish()
}

struct BenchEnv {
    name: &'static str,
    dict: Arc<dyn LocalObjectIdDictionary>,
    rocksdb_path: Option<PathBuf>,
}

impl Drop for BenchEnv {
    fn drop(&mut self) {
        if let Some(path) = &self.rocksdb_path {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

fn get_envs() -> Vec<BenchEnv> {
    let path1 = std::env::temp_dir().join(format!(
        "rdf-fusion-bench-redb-{}.redb",
        rand::random::<u64>()
    ));
    let path2 = std::env::temp_dir().join(format!(
        "rdf-fusion-bench-redb-{}.redb",
        rand::random::<u64>()
    ));
    let _ = std::fs::remove_file(&path1);
    let _ = std::fs::remove_file(&path2);

    let redb = RedbObjectIdDictionaryBuilder::new_on_disk(path1.clone())
        .with_cache_size(Some(1024 * 1024 * 10))
        .with_claimer(Some(Arc::new(StaticObjectIdClaimer)))
        .finish()
        .unwrap();

    let redb_no_cache = RedbObjectIdDictionaryBuilder::new_on_disk(path2.clone())
        .with_claimer(Some(Arc::new(StaticObjectIdClaimer)))
        .finish()
        .unwrap();

    let redb_in_memory = RedbObjectIdDictionaryBuilder::new_in_memory()
        .with_cache_size(Some(1024 * 1024 * 10))
        .with_claimer(Some(Arc::new(StaticObjectIdClaimer)))
        .finish()
        .unwrap();

    vec![
        BenchEnv {
            name: "redb",
            dict: Arc::new(redb),
            rocksdb_path: Some(path1),
        },
        BenchEnv {
            name: "redb_no_cache",
            dict: Arc::new(redb_no_cache),
            rocksdb_path: Some(path2),
        },
        BenchEnv {
            name: "redb_in_memory",
            dict: Arc::new(redb_in_memory),
            rocksdb_path: None,
        },
    ]
}

async fn setup_encoded_shuffled_array(
    dict: &Arc<dyn LocalObjectIdDictionary>,
    num_terms: usize,
    term_type: Option<&str>,
) -> Int64Array {
    let plain_term_array = generate_term_array(num_terms, term_type);

    let mut txn = dict.transaction().await.unwrap();
    let sequential_id_array = txn.encode_array(&plain_term_array).await.unwrap();
    txn.commit(0).await.unwrap();

    let mut ids: Vec<Option<i64>> = sequential_id_array.iter().collect();
    let mut rng = rand::rng();
    ids.shuffle(&mut rng);

    Int64Array::from(ids)
}

fn bench_decode_array(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let term_types = vec![Some("string"), Some("integer")];

    for env in get_envs() {
        let mut group =
            c.benchmark_group(format!("LocalObjectIdDictionary_Decode_{}", env.name));

        for term_type in &term_types {
            let shuffled_id_array =
                rt.block_on(setup_encoded_shuffled_array(&env.dict, 10_000, *term_type));
            let name = term_type.unwrap_or("mixed");

            group.bench_function(format!("decode_{name}_10k"), |b| {
                b.to_async(&rt).iter(async || {
                    let snapshot = env.dict.snapshot().await.unwrap();
                    let decoded = snapshot
                        .resolve_plain_terms(std::hint::black_box(&shuffled_id_array))
                        .await
                        .unwrap();
                    std::hint::black_box(decoded);
                })
            });
        }
        group.finish();
    }
}

fn bench_encode_array_existing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    for env in get_envs() {
        let plain_term_array = rt.block_on(async {
            let plain_term_array = generate_term_array(10_000, None);
            let mut txn = env.dict.transaction().await.unwrap();
            txn.encode_array(&plain_term_array).await.unwrap();
            txn.commit(0).await.unwrap();
            plain_term_array
        });

        let mut group =
            c.benchmark_group(format!("LocalObjectIdDictionary_Encode_{}", env.name));
        group.bench_function("encode_array_existing_10k_terms", |b| {
            b.to_async(&rt).iter(async || {
                let mut txn = env.dict.transaction().await.unwrap();
                let decoded = txn
                    .encode_array(std::hint::black_box(&plain_term_array))
                    .await
                    .unwrap();
                txn.abort().await.unwrap();
                std::hint::black_box(decoded);
            })
        });
        group.finish();
    }
}

fn bench_encode_array_non_existing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let plain_term_array = generate_term_array(10_000, None);

    let mut group = c.benchmark_group("LocalObjectIdDictionary_Encode_in_memory");
    group.bench_function("encode_array_non_existing_10k_terms", |b| {
        b.to_async(&rt).iter(async || {
            let dict = RedbObjectIdDictionaryBuilder::new_in_memory()
                .with_cache_size(Some(1024 * 1024 * 10))
                .with_claimer(Some(Arc::new(StaticObjectIdClaimer)))
                .finish()
                .unwrap();
            let mut txn = dict.transaction().await.unwrap();
            let decoded = txn
                .encode_array(std::hint::black_box(&plain_term_array))
                .await
                .unwrap();
            txn.commit(0).await.unwrap();
            std::hint::black_box(decoded);
        })
    });
    group.finish();

    let mut group = c.benchmark_group("LocalObjectIdDictionary_Encode_redb");
    group.bench_function("encode_array_non_existing_10k_terms", |b| {
        b.to_async(&rt).iter(async || {
            let path = std::env::temp_dir().join(format!(
                "rdf-fusion-bench-redb-non-ex-{}.redb",
                rand::random::<u64>()
            ));
            let dict = RedbObjectIdDictionaryBuilder::new_on_disk(path.clone())
                .with_cache_size(Some(1024 * 1024 * 10))
                .with_claimer(Some(Arc::new(StaticObjectIdClaimer)))
                .finish()
                .unwrap();

            let mut txn = dict.transaction().await.unwrap();
            let decoded = txn
                .encode_array(std::hint::black_box(&plain_term_array))
                .await
                .unwrap();
            txn.commit(0).await.unwrap();
            std::hint::black_box(decoded);

            let _ = std::fs::remove_file(&path);
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_encode_array_existing,
    bench_encode_array_non_existing,
    bench_decode_array
);
criterion_main!(benches);
