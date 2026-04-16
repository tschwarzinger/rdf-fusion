//! Measures the performance of preparing the stores for the respective benchmarks. Usually this
//! is inserting a bunch of statements.

mod utils;

use crate::utils::create_runtime;
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::benchmarks::windfarm::{NumTurbines, WindFarmBenchmark};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;

fn bench_bsbm_store_prepare(c: &mut Criterion) {
    let benchmarking_context = RdfFusionBenchContext::new_for_criterion(
        PathBuf::from("./data"),
        QuadStorageEncodingName::ObjectId,
        1,
    );
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(NumProducts::N10_000, None).unwrap();

    let target_partitions = benchmarking_context.options().target_partitions.unwrap();
    let runtime = create_runtime(target_partitions);

    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    c.bench_function("Prepare Store (BSBM 10000)", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark
                .prepare_store(&benchmark_context, false)
                .await
                .unwrap()
        });
    });
}

fn bench_windfarm_store_prepare(c: &mut Criterion) {
    let benchmarking_context = RdfFusionBenchContext::new_for_criterion(
        PathBuf::from("./data"),
        QuadStorageEncodingName::ObjectId,
        1,
    );
    let benchmark = WindFarmBenchmark::new(NumTurbines::N16);

    let target_partitions = benchmarking_context.options().target_partitions.unwrap();
    let runtime = create_runtime(target_partitions);

    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    c.bench_function("Prepare Store (WindFarm 16)", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark
                .prepare_store(&benchmark_context, false)
                .await
                .unwrap()
        });
    });
}

criterion_group!(
    name = bench_store_preparation;
    config = Criterion::default().sample_size(10);
    targets = bench_bsbm_store_prepare, bench_windfarm_store_prepare
);
criterion_main!(bench_store_preparation);
