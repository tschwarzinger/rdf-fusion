use crate::plans::run_plan_assertions;
use anyhow::Context;
use datafusion::physical_plan::displayable;
use insta::assert_snapshot;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::sparql::{QueryExplanation, QueryOptions};
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmExploreQueryName, ExploreUseCase, NumProducts,
};
use rdf_fusion_bench::benchmarks::{Benchmark, BenchmarkName};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::operation::SparqlRawOperation;
use std::path::PathBuf;

#[tokio::test]
pub async fn bsbm_explore_plain_term_optimized_logical_plan() {
    for_all_explanations(QuadStorageEncodingName::PlainTerm, |name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized, {})", QuadStorageEncodingName::PlainTerm),
            &explanation.optimized_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn bsbm_explore_plain_term_execution_plan() {
    for_all_explanations(QuadStorageEncodingName::PlainTerm, |name, explanation| {
        let string = displayable(explanation.execution_plan.as_ref())
            .indent(false)
            .to_string();
        assert_snapshot!(
            format!(
                "{name} (Execution Plan, {})",
                QuadStorageEncodingName::PlainTerm
            ),
            &string
        )
    })
    .await;
}

#[tokio::test]
pub async fn bsbm_explore_object_id_optimized_logical_plan() {
    for_all_explanations(QuadStorageEncodingName::ObjectId, |name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized, {})", QuadStorageEncodingName::ObjectId),
            &explanation.optimized_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn bsbm_explore_object_id_execution_plan() {
    for_all_explanations(QuadStorageEncodingName::ObjectId, |name, explanation| {
        let string = displayable(explanation.execution_plan.as_ref())
            .indent(false)
            .to_string();
        assert_snapshot!(
            format!(
                "{name} (Execution Plan, {})",
                QuadStorageEncodingName::ObjectId
            ),
            &string
        )
    })
    .await;
}

#[tokio::test]
pub async fn bsbm_explore_string_optimized_logical_plan() {
    for_all_explanations(QuadStorageEncodingName::String, |name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized, {})", QuadStorageEncodingName::String),
            &explanation.optimized_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn bsbm_explore_string_execution_plan() {
    for_all_explanations(QuadStorageEncodingName::String, |name, explanation| {
        let string = displayable(explanation.execution_plan.as_ref())
            .indent(false)
            .to_string();
        assert_snapshot!(
            format!(
                "{name} (Execution Plan, {})",
                QuadStorageEncodingName::String
            ),
            &string
        )
    })
    .await;
}

async fn for_all_explanations(
    encoding: QuadStorageEncodingName,
    assertion: impl Fn(String, QueryExplanation) -> (),
) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), encoding, 1)
            .build();

    let name = BenchmarkName::BsbmExplore {
        num_products: NumProducts::N1_000,
        max_query_count: None,
    };
    let benchmark_context = benchmarking_context.create_benchmark_context(name).unwrap();
    let benchmark = BsbmBenchmark::<ExploreUseCase>::try_new(
        &benchmark_context,
        NumProducts::N1_000,
        None,
    )
    .unwrap();

    let store = benchmark
        .prepare_store(&benchmark_context, true)
        .await
        .unwrap();
    for query_name in BsbmExploreQueryName::list_queries() {
        let benchmark_name = format!("BSBM Explore - {query_name}");
        let query =
            get_query_to_execute(benchmark.clone(), &benchmark_context, query_name);

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
    benchmark: BsbmBenchmark<ExploreUseCase>,
    benchmark_context: &BenchmarkContext,
    query_name: BsbmExploreQueryName,
) -> SparqlRawOperation<BsbmExploreQueryName> {
    benchmark
        .list_raw_operations(&benchmark_context)
        .context("Could not list raw operations for BSBM Explore benchmark. Have you prepared a bsbm-1000 dataset?")
        .unwrap()
        .into_iter()
        .find(|q| q.query_name() == query_name)
        .unwrap()
}
