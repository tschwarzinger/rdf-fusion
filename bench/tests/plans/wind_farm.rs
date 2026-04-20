use crate::plans::run_plan_assertions;
use anyhow::Context;
use datafusion::physical_plan::displayable;
use insta::assert_snapshot;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::sparql::{QueryExplanation, QueryOptions};
use rdf_fusion_bench::benchmarks::windfarm::{
    get_wind_farm_raw_sparql_operation, NumTurbines, WindFarmBenchmark, WindFarmQueryName,
};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::operation::SparqlRawOperation;
use std::path::PathBuf;

#[tokio::test]
pub async fn wind_farm_plain_term_optimized_logical_plan() {
    for_all_explanations(QuadStorageEncodingName::PlainTerm, |name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized, Plain Term)"),
            &explanation.optimized_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn wind_farm_plain_term_execution_plan() {
    for_all_explanations(QuadStorageEncodingName::PlainTerm, |name, explanation| {
        let string = displayable(explanation.execution_plan.as_ref())
            .indent(false)
            .to_string();
        assert_snapshot!(format!("{name} (Execution Plan, Plain Term)"), &string)
    })
    .await;
}

#[tokio::test]
pub async fn wind_farm_object_id_optimized_logical_plan() {
    for_all_explanations(QuadStorageEncodingName::ObjectId, |name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized, Object ID)"),
            &explanation.optimized_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn wind_farm_object_id_execution_plan() {
    for_all_explanations(QuadStorageEncodingName::ObjectId, |name, explanation| {
        let string = displayable(explanation.execution_plan.as_ref())
            .indent(false)
            .to_string();
        assert_snapshot!(format!("{name} (Execution Plan, Object ID)"), &string)
    })
    .await;
}

async fn for_all_explanations(
    encoding: QuadStorageEncodingName,
    assertion: impl Fn(String, QueryExplanation) -> (),
) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), encoding, 1);

    // Load the benchmark data and set max query count to one.
    let benchmark = WindFarmBenchmark::new(NumTurbines::N4);
    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = benchmark
        .prepare_store(&benchmark_context, true)
        .await
        .unwrap();

    // Ignore queries that are not fast enough to be executed in reasonable time.
    let ignored = [
        WindFarmQueryName::MultiGrouped1,
        WindFarmQueryName::MultiGrouped2,
        WindFarmQueryName::MultiGrouped3,
        WindFarmQueryName::MultiGrouped4,
    ];
    for query_name in WindFarmQueryName::list_queries() {
        if ignored.contains(&query_name) {
            continue;
        }

        let benchmark_name = format!("Wind Farm - {query_name}");
        let query = get_query_to_execute(&benchmark_context, query_name);

        let (_, explanation) = store
            .explain_query_opt(query.text(), QueryOptions::default())
            .await
            .unwrap();

        println!(
            "{}:\n{}",
            benchmark_name,
            displayable(explanation.execution_plan.as_ref())
                .indent(false)
                .to_string()
        );

        run_plan_assertions(|| assertion(benchmark_name, explanation));
    }
}

fn get_query_to_execute(
    benchmark_context: &BenchmarkContext,
    query_name: WindFarmQueryName,
) -> SparqlRawOperation<WindFarmQueryName> {
    get_wind_farm_raw_sparql_operation(benchmark_context, query_name)
        .context("Could not list raw operations for Wind Farm benchmark. Have you prepared a wind-farm-4 dataset?")
        .unwrap()
}
