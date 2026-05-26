//! This file contains tests for an adapted version of the Wind Farm benchmark. The adaption have
//! been made such that the queries produce stable results. The results of the queries has been
//! compared to GraphDB 11 to validate their correctness. See [crate::bsbm] for a detailed
//! description.

use crate::query_results::run_select_query;
use insta::assert_snapshot;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::windfarm::{NumTurbines, WindFarmBenchmark};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;

#[tokio::test]
pub async fn wind_farm_4_test_results_plain_term() {
    run_wind_farm_4_test_results(QuadStorageEncodingName::PlainTerm).await;
}

#[tokio::test]
pub async fn wind_farm_4_test_results_object_id() {
    run_wind_farm_4_test_results(QuadStorageEncodingName::ObjectId).await;
}

#[tokio::test]
pub async fn wind_farm_4_test_results_string() {
    run_wind_farm_4_test_results(QuadStorageEncodingName::String).await;
}

async fn run_wind_farm_4_test_results(encoding: QuadStorageEncodingName) {
    let encoding_name = encoding.to_string();
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), encoding, 1)
            .build();
    let name = rdf_fusion_bench::benchmarks::BenchmarkName::WindFarm {
        num_turbines: NumTurbines::N4,
    };
    let ctx = benchmarking_context.create_benchmark_context(name).unwrap();
    let benchmark = WindFarmBenchmark::try_new(&ctx, NumTurbines::N4).unwrap();

    let store = benchmark.prepare_store(&ctx, false).await.unwrap();

    assert_snapshot!(
        format!("Production Q1 ({encoding_name})"),
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-production-query1.sparql")
        )
        .await
    );
    assert_snapshot!(
        format!("Production Q2 ({encoding_name})"),
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-production-query2.sparql")
        )
        .await
    );
    assert_snapshot!(
        format!("Production Q3 ({encoding_name})"),
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-production-query3.sparql")
        )
        .await
    );
    assert_snapshot!(
        format!("Production Q4 ({encoding_name})"),
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-production-query4.sparql")
        )
        .await
    );

    assert_snapshot!(
        format!("Grouped Production Q1 ({encoding_name})"),
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-grouped-production-query1.sparql")
        )
        .await
    );
    assert_snapshot!(
        format!("Grouped Production Q2 ({encoding_name})"),
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-grouped-production-query2.sparql")
        )
        .await
    );
    assert_snapshot!(
        format!("Grouped Production Q3 ({encoding_name})"),
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-grouped-production-query3.sparql")
        )
        .await
    );
    assert_snapshot!(
        format!("Grouped Production Q4 ({encoding_name})"),
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-grouped-production-query4.sparql")
        )
        .await
    );
}
