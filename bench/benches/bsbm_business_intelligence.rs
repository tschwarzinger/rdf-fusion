//! Runs certain queries from the BSBM benchmark suite as part of the regular benchmark suite.
//!
//! The particular instance of a query (they are generated randomly) is picked arbitrarily. If we
//! ever decide that queries in this file are not representative, we can easily change the query.
//!
//! The tests assume the presence of the benchmark data.

mod utils;

use crate::utils::verbose::{is_verbose, print_query_details};
use crate::utils::{consume_results, create_runtime};
use anyhow::Context;
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmBusinessIntelligenceQueryName, BusinessIntelligenceUseCase,
    NumProducts,
};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::operation::SparqlRawOperation;
use std::path::PathBuf;

fn bsbm_business_intelligence_10000_1_partition(c: &mut Criterion) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);
    bsbm_business_intelligence_10000(c, benchmarking_context);
}

fn bsbm_business_intelligence_10000(
    c: &mut Criterion,
    benchmarking_context: RdfFusionBenchContext,
) {
    let target_partitions = benchmarking_context.options().target_partitions.unwrap();
    execute_benchmark(
        c,
        benchmarking_context,
        &|benchmark: BsbmBenchmark<BusinessIntelligenceUseCase>,
          benchmark_context: BenchmarkContext| {
            let mut queries = BsbmBusinessIntelligenceQueryName::list_queries()
                .into_iter()
                .map(|query_name| {
                    (
                        format!("BSBM Business Intelligence 10000 (target_partitions={target_partitions}) - {query_name}"),
                        get_query_to_execute(
                            benchmark.clone(),
                            &benchmark_context,
                            query_name,
                        ),
                    )
                })
                .collect::<Vec<_>>();

            // Query 64 is work-intensive
            queries.push((
                format!("BSBM Business Intelligence 10000 (target_partitions={target_partitions}) - Query 64"),
                get_nth_query_to_execute(benchmark.clone(), &benchmark_context, 64),
            ));

            queries
        },
    )
}

criterion_group!(
    name = bsbm_business_intelligence;
    config = Criterion::default().sample_size(10);
    targets =  bsbm_business_intelligence_10000_1_partition
);
criterion_main!(bsbm_business_intelligence);

fn execute_benchmark(
    c: &mut Criterion,
    benchmarking_context: RdfFusionBenchContext,
    queries: &dyn Fn(
        BsbmBenchmark<BusinessIntelligenceUseCase>,
        BenchmarkContext,
    ) -> Vec<(
        String,
        SparqlRawOperation<BsbmBusinessIntelligenceQueryName>,
    )>,
) {
    let verbose = is_verbose();
    let runtime =
        create_runtime(benchmarking_context.options().target_partitions.unwrap());

    // Load the benchmark data and set max query count to one.
    let benchmark =
        BsbmBenchmark::<BusinessIntelligenceUseCase>::try_new(NumProducts::N10_000, None)
            .unwrap();
    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = runtime
        .block_on(benchmark.prepare_store(&benchmark_context))
        .context("
    Failed to prepare store. Have you downloaded the data?

    Execute `just prepare-benches` for downloading the data. Then, run the benchmark from the `bench` directory.
    ")
        .unwrap();

    for (benchmark_name, query) in queries(benchmark, benchmark_context) {
        if verbose {
            runtime
                .block_on(print_query_details(
                    &store,
                    QueryOptions::default(),
                    &query.query_name().to_string(),
                    query.text(),
                ))
                .unwrap();
        }

        c.bench_function(&format!("Planning: {benchmark_name}"), |b| {
            b.to_async(&runtime).iter(|| async {
                let result = store.query_opt(query.text(), QueryOptions::default()).await;
                assert!(result.is_ok())
            });
        });

        c.bench_function(&benchmark_name, |b| {
            b.to_async(&runtime).iter(|| async {
                let result = store
                    .query_opt(query.text(), QueryOptions::default())
                    .await
                    .unwrap();
                consume_results(result).await.unwrap();
            });
        });
    }
}

fn get_nth_query_to_execute(
    benchmark: BsbmBenchmark<BusinessIntelligenceUseCase>,
    benchmark_context: &BenchmarkContext,
    n: usize,
) -> SparqlRawOperation<BsbmBusinessIntelligenceQueryName> {
    benchmark
        .list_raw_operations(&benchmark_context)
        .unwrap()
        .into_iter()
        .nth(n)
        .unwrap()
}

fn get_query_to_execute(
    benchmark: BsbmBenchmark<BusinessIntelligenceUseCase>,
    benchmark_context: &BenchmarkContext,
    query_name: BsbmBusinessIntelligenceQueryName,
) -> SparqlRawOperation<BsbmBusinessIntelligenceQueryName> {
    benchmark
        .list_raw_operations(&benchmark_context)
        .unwrap()
        .into_iter()
        .find(|q| q.query_name() == query_name)
        .unwrap()
}
