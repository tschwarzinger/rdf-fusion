use crate::plans::run_plan_assertions;
use datafusion::physical_plan::displayable;
use insta::assert_snapshot;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::sparql::{QueryExplanation, QueryOptions};
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmExploreQueryName, ExploreUseCase, NumProducts,
};
use rdf_fusion_bench::benchmarks::{Benchmark, BenchmarkName};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::operation::SparqlOperation;
use std::path::PathBuf;

#[tokio::test]
pub async fn bsbm_explore_initial_logical_plan() {
    for_all_explanations(QuadStorageEncodingName::ObjectId, |name, explanation| {
        assert_snapshot!(
            format!("{name} (Initial)"),
            &explanation.initial_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn bsbm_explore_optimized_logical_plan() {
    for_all_explanations(QuadStorageEncodingName::ObjectId, |name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized)"),
            &explanation.optimized_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn bsbm_explore_execution_plan() {
    for_all_explanations(QuadStorageEncodingName::ObjectId, |name, explanation| {
        let string = displayable(explanation.execution_plan.as_ref())
            .indent(false)
            .to_string();
        assert_snapshot!(format!("{name} (Execution Plan)"), &string)
    })
    .await;
}

async fn for_all_explanations(
    encoding: QuadStorageEncodingName,
    assertion: impl Fn(String, QueryExplanation),
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

        run_plan_assertions("bsbm-explore", || assertion(benchmark_name, explanation));
    }
}

fn get_query_to_execute(
    benchmark: BsbmBenchmark<ExploreUseCase>,
    benchmark_context: &BenchmarkContext,
    query_name: BsbmExploreQueryName,
) -> SparqlOperation {
    benchmark
        .list_operations(benchmark_context)
        .unwrap()
        .into_iter()
        .find(|q| q.name() == query_name.to_string())
        .unwrap()
}
