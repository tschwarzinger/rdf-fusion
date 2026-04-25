//! Runs certain queries from the BSBM benchmark suite as part of the regular benchmark suite.
//!
//! The particular instance of a query (they are generated randomly) is picked arbitrarily. If we
//! ever decide that queries in this file are not representative, we can easily change the query.
//!
//! The tests assume the presence of the benchmark data.

mod utils;

use crate::utils::verbose::{is_verbose, print_query_details};
use crate::utils::{ENCODINGS_TO_BENCHMARK, consume_results, setup_benchmark_env};
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmBusinessIntelligenceQueryName, BusinessIntelligenceUseCase,
    NumProducts,
};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use std::path::PathBuf;

fn bench_planning(c: &mut Criterion) {
    let encoding = QuadStorageEncodingName::String;
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), encoding, 1);
    let target_partitions = benchmarking_context.options().target_partitions.unwrap();
    let benchmark =
        BsbmBenchmark::<BusinessIntelligenceUseCase>::try_new(NumProducts::N10_000, None)
            .unwrap();

    let (runtime, benchmark_context, store) =
        setup_benchmark_env(&benchmarking_context, &benchmark);
    let queries = get_queries(&benchmark, &benchmark_context);
    let verbose = is_verbose();

    for (query_name, query_text) in queries {
        let benchmark_name = format!(
            "Planning (partitions={target_partitions}): BSBM Business Intelligence 10000 - {query_name}"
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
    for encoding in ENCODINGS_TO_BENCHMARK {
        let benchmarking_context = RdfFusionBenchContext::new_for_criterion(
            PathBuf::from("./data"),
            encoding,
            1,
        );
        let target_partitions = benchmarking_context.options().target_partitions.unwrap();
        let benchmark = BsbmBenchmark::<BusinessIntelligenceUseCase>::try_new(
            NumProducts::N10_000,
            None,
        )
        .unwrap();

        let (runtime, benchmark_context, store) =
            setup_benchmark_env(&benchmarking_context, &benchmark);
        let queries = get_queries(&benchmark, &benchmark_context);

        for (query_name, query_text) in queries {
            let benchmark_name = format!(
                "Execution ({encoding}, partitions={target_partitions}): BSBM Business Intelligence 10000 - {query_name}"
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
    name = bsbm_business_intelligence;
    config = Criterion::default().sample_size(10);
    targets = bench_planning, bench_full_execution
);
criterion_main!(bsbm_business_intelligence);

fn get_queries(
    benchmark: &BsbmBenchmark<BusinessIntelligenceUseCase>,
    benchmark_context: &BenchmarkContext,
) -> Vec<(String, String)> {
    let mut queries = BsbmBusinessIntelligenceQueryName::list_queries()
        .into_iter()
        .map(|query_name| {
            let op = benchmark
                .list_raw_operations(benchmark_context)
                .unwrap()
                .into_iter()
                .find(|q| q.query_name() == query_name)
                .unwrap();
            (query_name.to_string(), op.text().to_string())
        })
        .collect::<Vec<_>>();

    // Query 64 is work-intensive
    let op64 = benchmark
        .list_raw_operations(benchmark_context)
        .unwrap()
        .into_iter()
        .nth(64)
        .unwrap();

    queries.push(("Query 64".to_string(), op64.text().to_string()));

    queries
}
