//! Runs the queries from the Wind Farm Benchmark.

mod utils;

use crate::utils::verbose::{is_verbose, print_query_details};
use crate::utils::{consume_results, setup_benchmark_env};
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion_bench::benchmarks::windfarm::{
    NumTurbines, WindFarmBenchmark, WindFarmQueryName, get_wind_farm_raw_sparql_operation,
};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use std::path::PathBuf;

fn bench_planning(c: &mut Criterion) {
    let benchmarking_context = RdfFusionBenchContext::new_for_criterion(
        PathBuf::from("./data"),
        QuadStorageEncodingName::ObjectId,
        1,
    );
    let target_partitions = benchmarking_context.options().target_partitions.unwrap();
    let benchmark = WindFarmBenchmark::new(NumTurbines::N16);

    let (runtime, benchmark_context, store) =
        setup_benchmark_env(&benchmarking_context, &benchmark);
    let queries = get_queries(&benchmark_context, target_partitions);
    let verbose = is_verbose();

    for (benchmark_name, query_text) in queries {
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
        c.bench_function(&format!("Planning: {benchmark_name}"), |b| {
            b.to_async(&runtime).iter(|| async {
                let result = store.query_opt(&query_text, QueryOptions::default()).await;
                assert!(result.is_ok());
            });
        });
    }
}

fn bench_full_execution(c: &mut Criterion) {
    let benchmarking_context = RdfFusionBenchContext::new_for_criterion(
        PathBuf::from("./data"),
        QuadStorageEncodingName::ObjectId,
        1,
    );
    let target_partitions = benchmarking_context.options().target_partitions.unwrap();
    let benchmark = WindFarmBenchmark::new(NumTurbines::N16);

    let (runtime, benchmark_context, store) =
        setup_benchmark_env(&benchmarking_context, &benchmark);
    let queries = get_queries(&benchmark_context, target_partitions);

    for (benchmark_name, query_text) in queries {
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

criterion_group!(
    name = wind_farm;
    config = Criterion::default().sample_size(10);
    targets = bench_planning, bench_full_execution
);
criterion_main!(wind_farm);

fn get_queries(
    benchmark_context: &BenchmarkContext,
    target_partitions: usize,
) -> Vec<(String, String)> {
    let disabled_queries = vec![
        WindFarmQueryName::MultiGrouped1,
        WindFarmQueryName::MultiGrouped2,
        WindFarmQueryName::MultiGrouped3,
        WindFarmQueryName::MultiGrouped4,
    ];

    WindFarmQueryName::list_queries()
        .into_iter()
        .filter_map(|query_name| {
            if disabled_queries.contains(&query_name) {
                println!("Skipping query: {}", query_name);
                None
            } else {
                let op = get_wind_farm_raw_sparql_operation(benchmark_context, query_name).unwrap();
                Some((
                    format!("Wind Farm 16 (target_partitions={target_partitions}) - {query_name}"),
                    op.text().to_string(),
                ))
            }
        })
        .collect()
}
