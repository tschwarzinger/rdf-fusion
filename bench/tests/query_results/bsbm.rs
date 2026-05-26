//! This file contains tests for an adapte version of the BSBM explore use case. The adaption have
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

    assert_snapshot!(
        format!("Explore Q1 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/explore-q1.sparql")).await
    );
    assert_snapshot!(
        format!("Explore Q2 (empty optional) ({encoding_name})"),
        run_select_query(
            &store,
            include_str!("./queries/explore-q2-empty-optional.sparql")
        )
        .await
    );
    assert_snapshot!(
        format!("Explore Q2 (non-empty optional) ({encoding_name})"),
        run_select_query(
            &store,
            include_str!("./queries/explore-q2-non-empty-optional.sparql")
        )
        .await
    );
    assert_snapshot!(
        format!("Explore Q3 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/explore-q3.sparql")).await
    );
    assert_snapshot!(
        format!("Explore Q4 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/explore-q4.sparql")).await
    );
    assert_snapshot!(
        format!("Explore Q5 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/explore-q5.sparql")).await
    );
    assert_snapshot!(
        format!("Explore Q7 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/explore-q7.sparql")).await
    );
    assert_snapshot!(
        format!("Explore Q8 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/explore-q8.sparql")).await
    );
    assert_snapshot!(
        format!("Explore Q9 ({encoding_name})"),
        run_graph_result_query(&store, include_str!("./queries/explore-q9.sparql")).await
    );
    assert_snapshot!(
        format!("Explore Q10 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/explore-q10.sparql")).await
    );
    assert_snapshot!(
        format!("Explore Q11 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/explore-q11.sparql")).await
    );
    assert_snapshot!(
        format!("Explore Q12 ({encoding_name})"),
        run_graph_result_query(&store, include_str!("./queries/explore-q12.sparql"))
            .await
    );

    //
    // Business Intelligence
    //

    assert_snapshot!(
        format!("Business Intelligence Q1 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/bi-q1.sparql")).await
    );
    assert_snapshot!(
        format!("Business Intelligence Q2 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/bi-q2.sparql")).await
    );
    assert_snapshot!(
        format!("Business Intelligence Q3 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/bi-q3.sparql")).await
    );
    assert_snapshot!(
        format!("Business Intelligence Q4 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/bi-q4.sparql")).await
    );
    assert_snapshot!(
        format!("Business Intelligence Q5 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/bi-q5.sparql")).await
    );
    assert_snapshot!(
        format!("Business Intelligence Q6 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/bi-q6.sparql")).await
    );
    assert_snapshot!(
        format!("Business Intelligence Q7 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/bi-q7.sparql")).await
    );
    assert_snapshot!(
        format!("Business Intelligence Q8 ({encoding_name})"),
        run_select_query(&store, include_str!("./queries/bi-q8.sparql")).await
    );
}
