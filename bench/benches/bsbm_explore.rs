//! Runs certain queries from the BSBM benchmark suite as part of the regular benchmark suite.
//!
//! The query instances (they are generated randomly) are selected by [`BsbmUseCase::queries`].
//!
//! [`BsbmUseCase::queries`]: rdf_fusion_bench::benchmarks::bsbm::BsbmUseCase::queries
//!
//! The tests assume the presence of the benchmark data.

mod utils;

use crate::utils::verbose::{is_verbose, print_query_details};
use crate::utils::{
    bench_num_products, consume_results, load_bsbm_queries, setup_benchmark_env,
};
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion_bench::benchmarks::BenchmarkName;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;

fn bench_planning(c: &mut Criterion) {
    let encoding = QuadStorageEncodingName::String;
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), encoding, 1)
            .build();
    let num_products = bench_num_products();
    let name = BenchmarkName::BsbmExplore {
        num_products,
        max_query_count: None,
    };
    let context = benchmarking_context.create_benchmark_context(name).unwrap();
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(&context, num_products, None).unwrap();

    let (runtime, benchmark_context, store) =
        setup_benchmark_env(&benchmarking_context, &benchmark);
    let queries = load_bsbm_queries(&benchmark, &benchmark_context, true).unwrap();
    let verbose = is_verbose();

    for (query_name, query_text) in queries {
        let benchmark_name = format!("Plan BSBM Explore {num_products} - {query_name}");
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
    let num_products = bench_num_products();
    for storage_config in utils::benchmark_configs() {
        let benchmarking_context = storage_config.bench_context();
        let name = BenchmarkName::BsbmExplore {
            num_products,
            max_query_count: None,
        };
        let context = benchmarking_context.create_benchmark_context(name).unwrap();
        let benchmark =
            BsbmBenchmark::<ExploreUseCase>::try_new(&context, num_products, None)
                .unwrap();

        let (runtime, benchmark_context, store) =
            setup_benchmark_env(&benchmarking_context, &benchmark);
        let queries = load_bsbm_queries(&benchmark, &benchmark_context, true).unwrap();

        for (query_name, query_text) in queries {
            let benchmark_name =
                format!("BSBM Explore {num_products} - {storage_config} - {query_name}",);
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
    name = bsbm_explore;
    config = Criterion::default().sample_size(10);
    targets = bench_planning, bench_full_execution
);
criterion_main!(bsbm_explore);
