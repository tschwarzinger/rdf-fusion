//! Runs certain queries from the BSBM benchmark suite as part of the regular benchmark suite.
//!
//! The particular instance of a query (they are generated randomly) is picked arbitrarily. If we
//! ever decide that queries in this file are not representative, we can easily change the query.
//!
//! The tests assume the presence of the benchmark data.

mod utils;

use crate::utils::verbose::{is_verbose, print_query_details};
use crate::utils::{consume_results, setup_benchmark_env};
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmExploreQueryName, ExploreUseCase, NumProducts,
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
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(NumProducts::N10_000, None).unwrap();

    let (runtime, benchmark_context, store) =
        setup_benchmark_env(&benchmarking_context, &benchmark);
    let queries = get_queries(&benchmark, &benchmark_context, target_partitions);
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
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(NumProducts::N10_000, None).unwrap();

    let (runtime, benchmark_context, store) =
        setup_benchmark_env(&benchmarking_context, &benchmark);
    let queries = get_queries(&benchmark, &benchmark_context, target_partitions);

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
    name = bsbm_explore;
    config = Criterion::default().sample_size(10);
    targets = bench_planning, bench_full_execution
);
criterion_main!(bsbm_explore);

fn get_queries(
    benchmark: &BsbmBenchmark<ExploreUseCase>,
    benchmark_context: &BenchmarkContext,
    target_partitions: usize,
) -> Vec<(String, String)> {
    BsbmExploreQueryName::list_queries()
        .into_iter()
        .map(|query_name| {
            let op = benchmark
                .list_raw_operations(benchmark_context)
                .unwrap()
                .into_iter()
                .find(|q| q.query_name() == query_name)
                .unwrap();
            (
                format!("BSBM Explore 10000 (target_partitions={target_partitions}) - {query_name}"),
                op.text().to_string(),
            )
        })
        .collect()
}
