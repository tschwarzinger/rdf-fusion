//! This file contains tests for an adapted version of the BSBM explore use case. The adaption have
//! been made such that the queries produce stable results.
//!
//! The results have been compared against GraphDB 11.0.1. The following curl command can be used to
//! get the results of GraphDB. Ideally, one can pipe the result of the request into a file or
//! directly to the clipboard using `| xclip -selection clipboard` (or similar). You can also
//! download the JSON result from the GraphDB UI.
//!
//! ```bash
//! curl -X POST \
//!     "http://<graphdb_url>/repositories/bsbm" \
//!     -H "Content-Type: application/sparql-query" \
//!     -H "Accept: application/sparql-results+json" \
//!     --data-binary '<query>'
//! ```
//!
//! Then, even though we pretty-print our results, there will be some differences (e.g., spacing,
//! order of keys). You can use a tool that semantically compares JSON files to "quickly" check
//! the results of a new test (e.g., [JSON Compare](https://jsoncompare.org/)). `CONSTRUCT` queries
//! have been compared manually.

use crate::query_results::{run_graph_result_query, run_select_query};
use insta::assert_snapshot;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;

#[tokio::test]
pub async fn bsbm_1000_test_results_plain_term() {
    run_bsbm_1000_test_results(QuadStorageEncodingName::PlainTerm).await;
}

#[tokio::test]
pub async fn bsbm_1000_test_results_object_id() {
    run_bsbm_1000_test_results(QuadStorageEncodingName::ObjectId).await;
}

#[tokio::test]
pub async fn bsbm_1000_test_results_string() {
    run_bsbm_1000_test_results(QuadStorageEncodingName::String).await;
}

async fn run_bsbm_1000_test_results(encoding: QuadStorageEncodingName) {
    let encoding_name = encoding.to_string();
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), encoding, 1)
            .build();
    let name = rdf_fusion_bench::benchmarks::BenchmarkName::BsbmExplore {
        num_products: NumProducts::N1_000,
        max_query_count: None,
    };
    let ctx = benchmarking_context.create_benchmark_context(name).unwrap();
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(&ctx, NumProducts::N1_000, None)
            .unwrap();

    let store = benchmark.prepare_store(&ctx, false).await.unwrap();

    //
    // Explore
    //
    let explore_queries =
        crate::load::load_queries("tests/test_queries/bsbm/explore").unwrap();
    for (name, query_str) in explore_queries {
        let formatted_name = name
            .replace("explore-q", "Q")
            .replace("-non-empty-optional", " (non-empty optional)")
            .replace("-empty-optional", " (empty optional)");
        assert_snapshot!(
            format!("Explore {formatted_name} ({encoding_name})"),
            if name == "explore-q9" || name == "explore-q12" {
                run_graph_result_query(&store, &query_str).await
            } else {
                run_select_query(&store, &query_str).await
            }
        );
    }

    //
    // Business Intelligence
    //
    let bi_queries = crate::load::load_queries("tests/test_queries/bsbm/bi").unwrap();
    for (name, query_str) in bi_queries {
        let formatted_name = name.replace("bi-q", "Q");
        assert_snapshot!(
            format!("Business Intelligence {formatted_name} ({encoding_name})"),
            run_select_query(&store, &query_str).await
        );
    }
}
