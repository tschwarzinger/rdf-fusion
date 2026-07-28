//! Measures the performance of concurrent encoding when loading BSBM 10000.

mod utils;

use crate::utils::create_runtime;
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion::common::RdfFormat;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::storage::rdf_files::RdfFileScanOptions;
use rdf_fusion_bench::benchmarks::BenchmarkName;
use rdf_fusion_bench::benchmarks::bsbm::NumProducts;
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use rdf_fusion_bench::{BenchQuadStorageTypeArg, QuadStorageLocationArg};
use std::path::PathBuf;

fn bench_concurrent_encode(c: &mut Criterion) {
    let encodings = vec![
        QuadStorageEncodingName::ObjectId,
        QuadStorageEncodingName::String,
    ];
    let partitions = vec![1, 4];

    for encoding in encodings {
        for target_partitions in &partitions {
            let target_partitions = *target_partitions;
            let benchmarking_context = RdfFusionBenchContext::new_for_criterion(
                PathBuf::from("./data"),
                encoding,
                target_partitions,
            )
            .with_storage_type(BenchQuadStorageTypeArg::DeltaQuads)
            .with_storage_location(QuadStorageLocationArg::OnDisk)
            .build();

            let name = BenchmarkName::BsbmExplore {
                num_products: NumProducts::N10_000,
                max_query_count: None,
            };
            let benchmark_context =
                benchmarking_context.create_benchmark_context(name).unwrap();
            let runtime = create_runtime(target_partitions);

            c.bench_function(
                &format!("Concurrent Encode (BSBM 10000, partitions={target_partitions}, encoding={encoding})"),
                |b| {
                    b.to_async(&runtime).iter(|| async {
                        let dataset_path = benchmark_context.data_dir().join("dataset.nt");
                        let data = tokio::fs::File::open(&dataset_path).await.unwrap();
                        let store = benchmark_context.create_store().await;
                        store
                            .load_from_reader(data, RdfFileScanOptions::with_format(RdfFormat::NTriples))
                            .await
                            .unwrap()
                    });
                },
            );
        }
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_concurrent_encode
);
criterion_main!(benches);
