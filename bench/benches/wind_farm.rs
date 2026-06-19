//! Runs the queries from the Wind Farm Benchmark.

mod utils;

use crate::utils::verbose::{is_verbose, print_query_details};
use crate::utils::{consume_results, setup_benchmark_env};
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion_bench::benchmarks::BenchmarkName;
use rdf_fusion_bench::benchmarks::windfarm::{
    NumTurbines, WindFarmBenchmark, WindFarmQueryName,
};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use std::path::PathBuf;

fn bench_planning(c: &mut Criterion) {
    let encoding = QuadStorageEncodingName::String;
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), encoding, 1)
            .build();
    let target_partitions = benchmarking_context
        .options()
        .data_fusion_config
        .target_partitions();
    let name = BenchmarkName::WindFarm {
        num_turbines: NumTurbines::N16,
    };
    let context = benchmarking_context.create_benchmark_context(name).unwrap();
    let benchmark = WindFarmBenchmark::try_new(&context, NumTurbines::N16).unwrap();

    let (runtime, benchmark_context, store) =
        setup_benchmark_env(&benchmarking_context, &benchmark);
    let queries = get_queries(&benchmark, &benchmark_context);
    let verbose = is_verbose();

    for (query_name, query_text) in queries {
        let benchmark_name = format!(
            "Planning (partitions={target_partitions}): Wind Farm 16 - {query_name}"
        );
        if verbose {
            runtime
                .block_on(print_query_details(
                    &store,
                    QueryOptions::default(),
                    &benchmark_name,
                    &query_text,
                ))
                .unwrap();
        }
        c.bench_function(&benchmark_name, |b| {
            b.to_async(&runtime).iter(|| async {
                let result = store.query_opt(&query_text, QueryOptions::default()).await;
                assert!(result.is_ok());
            });
        });
    }
}

fn bench_full_execution(c: &mut Criterion) {
    for storage_config in utils::benchmark_storage_configs() {
        let benchmarking_context = storage_config.bench_context();
        let target_partitions = benchmarking_context
            .options()
            .data_fusion_config
            .target_partitions();
        let name = BenchmarkName::WindFarm {
            num_turbines: NumTurbines::N16,
        };
        let context = benchmarking_context.create_benchmark_context(name).unwrap();
        let benchmark = WindFarmBenchmark::try_new(&context, NumTurbines::N16).unwrap();

        let (runtime, benchmark_context, store) =
            setup_benchmark_env(&benchmarking_context, &benchmark);
        let queries = get_queries(&benchmark, &benchmark_context);

        for (query_name, query_text) in queries {
            let benchmark_name = format!(
                "Execution ({storage_config}, partitions={target_partitions}): Wind Farm 16 - {query_name}"
            );
            c.bench_function(&benchmark_name, |b| {
                b.to_async(&runtime).iter(|| async {
                    let result = store
                        .query_opt(&query_text, QueryOptions::default())
                        .await
                        .unwrap();
                    consume_results(result).await.unwrap();
                });
            });
        }
    }
}

criterion_group!(
    name = wind_farm;
    config = Criterion::default().sample_size(10);
    targets = bench_planning, bench_full_execution
);
criterion_main!(wind_farm);

fn get_queries(
    benchmark: &WindFarmBenchmark,
    benchmark_context: &BenchmarkContext,
) -> Vec<(String, String)> {
    let disabled_queries = [
        WindFarmQueryName::MultiGrouped1,
        WindFarmQueryName::MultiGrouped2,
        WindFarmQueryName::MultiGrouped3,
        WindFarmQueryName::MultiGrouped4,
    ];

    WindFarmQueryName::list_queries()
        .into_iter()
        .filter_map(|query_name| {
            if disabled_queries.contains(&query_name) {
                println!("Skipping query: {query_name}");
                None
            } else {
                let op = benchmark
                    .get_wind_farm_raw_sparql_operation(benchmark_context, query_name)
                    .unwrap();
                Some((query_name.to_string(), op.text().to_string()))
            }
        })
        .collect()
}
