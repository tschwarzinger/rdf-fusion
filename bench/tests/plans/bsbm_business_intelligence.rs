use crate::plans::run_plan_assertions;
use anyhow::Context;
use datafusion::physical_plan::displayable;
use insta::assert_snapshot;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::sparql::{QueryExplanation, QueryOptions};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmBusinessIntelligenceQueryName, BusinessIntelligenceUseCase,
    NumProducts,
};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::operation::SparqlRawOperation;
use std::path::PathBuf;

#[tokio::test]
pub async fn bsbm_business_intelligence_plain_term_optimized_logical_plan() {
    for_all_explanations(QuadStorageEncodingName::PlainTerm, |name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized, {})", QuadStorageEncodingName::PlainTerm),
            &explanation.optimized_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn bsbm_business_intelligence_plain_term_execution_plan() {
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
pub async fn bsbm_business_intelligence_object_id_optimized_logical_plan() {
    for_all_explanations(QuadStorageEncodingName::ObjectId, |name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized, {})", QuadStorageEncodingName::ObjectId),
            &explanation.optimized_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn bsbm_business_intelligence_object_id_execution_plan() {
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
pub async fn bsbm_business_intelligence_string_optimized_logical_plan() {
    for_all_explanations(QuadStorageEncodingName::String, |name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized, {})", QuadStorageEncodingName::String),
            &explanation.optimized_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn bsbm_business_intelligence_string_execution_plan() {
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

    // Load the benchmark data and set max query count to one.
    let benchmark =
        BsbmBenchmark::<BusinessIntelligenceUseCase>::try_new(NumProducts::N1_000, None)
            .unwrap();
    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = benchmark
        .prepare_store(&benchmark_context, true)
        .await
        .unwrap();
    for query_name in BsbmBusinessIntelligenceQueryName::list_queries() {
        let benchmark_name = format!("BSBM Business Intelligence - {query_name}");
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
    benchmark: BsbmBenchmark<BusinessIntelligenceUseCase>,
    benchmark_context: &BenchmarkContext,
    query_name: BsbmBusinessIntelligenceQueryName,
) -> SparqlRawOperation<BsbmBusinessIntelligenceQueryName> {
    benchmark
        .list_raw_operations(&benchmark_context)
        .context("Could not list raw operations for BSBM Business Intelligence benchmark. Have you prepared a bsbm-1000 dataset?")
        .unwrap()
        .into_iter()
        .find(|q| q.query_name() == query_name)
        .unwrap()
}
