//! Measures the performance of preparing the stores for the respective benchmarks. Usually this
//! is inserting a bunch of statements.

mod utils;

use crate::utils::{benchmark_storage_configs, create_runtime};
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::benchmarks::windfarm::{NumTurbines, WindFarmBenchmark};

fn bench_bsbm_store_prepare(c: &mut Criterion) {
    for storage_configuration in benchmark_storage_configs() {
        let benchmarking_context = storage_configuration.bench_context();
        let name = rdf_fusion_bench::benchmarks::BenchmarkName::BsbmExplore {
            num_products: NumProducts::N10_000,
            max_query_count: None,
        };
        let benchmark_context =
            benchmarking_context.create_benchmark_context(name).unwrap();
        let benchmark = BsbmBenchmark::<ExploreUseCase>::try_new(
            &benchmark_context,
            NumProducts::N10_000,
            None,
        )
        .unwrap();

        let target_partitions = benchmarking_context
            .options()
            .data_fusion_config
            .target_partitions();
        let runtime = create_runtime(target_partitions);

        c.bench_function(
            &format!("Prepare Store (BSBM 10000, {storage_configuration})"),
            |b| {
                b.to_async(&runtime).iter(|| async {
                    benchmark
                        .prepare_store(&benchmark_context, false)
                        .await
                        .unwrap()
                });
            },
        );
    }
}

fn bench_windfarm_store_prepare(c: &mut Criterion) {
    for storage_configuration in benchmark_storage_configs() {
        let benchmarking_context = storage_configuration.bench_context();
        let name = rdf_fusion_bench::benchmarks::BenchmarkName::WindFarm {
            num_turbines: NumTurbines::N16,
        };
        let benchmark_context =
            benchmarking_context.create_benchmark_context(name).unwrap();
        let benchmark =
            WindFarmBenchmark::try_new(&benchmark_context, NumTurbines::N16).unwrap();

        let target_partitions = benchmarking_context
            .options()
            .data_fusion_config
            .target_partitions();
        let runtime = create_runtime(target_partitions);

        c.bench_function(
            &format!("Prepare Store (WindFarm 16, {storage_configuration})"),
            |b| {
                b.to_async(&runtime).iter(|| async {
                    benchmark
                        .prepare_store(&benchmark_context, false)
                        .await
                        .unwrap()
                });
            },
        );
    }
}

criterion_group!(
    name = bench_store_preparation;
    config = Criterion::default().sample_size(10);
    targets = bench_bsbm_store_prepare, bench_windfarm_store_prepare
);
criterion_main!(bench_store_preparation);
