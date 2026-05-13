use datafusion::prelude::SessionContext;
use rdf_fusion::common::RdfFormat;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::storage::rdf_files::RdfParserOptions;
use rdf_fusion::store::{DumpOptions, Store};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::{Path, PathBuf};
use tokio::fs::File;

const EXPECTED_COUNT: usize = 374911;

#[tokio::test]
async fn test_dump_correctness_turtle() {
    let store = setup_test_store().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("output.ttl");
    let output_url = format!("file://{}", path.to_str().unwrap());

    store
        .dump(output_url, RdfFormat::Turtle, DumpOptions::default())
        .await
        .unwrap();

    assert_count(&path, RdfFormat::Turtle).await;
}

#[tokio::test]
async fn test_dump_correctness_nquads() {
    let store = setup_test_store().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("output.nq");
    let output_url = format!("file://{}", path.to_str().unwrap());

    store
        .dump(output_url, RdfFormat::NQuads, DumpOptions::default())
        .await
        .unwrap();

    assert_count(&path, RdfFormat::NQuads).await;
}

#[tokio::test]
async fn test_dump_correctness_parquet() {
    let store = setup_test_store().await;
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("output.parquet");
    let output_url = format!("file://{}", path.to_str().unwrap());

    store
        .dump(output_url, RdfFormat::Parquet, DumpOptions::default())
        .await
        .unwrap();

    let (session, _) = store
        .context()
        .quads_for_pattern(None, None, None, None)
        .await
        .unwrap()
        .into_parts();
    let session_ctx = SessionContext::new_with_state(session);
    let df_read = session_ctx
        .read_parquet(path.to_str().unwrap(), Default::default())
        .await
        .unwrap();
    let count = df_read.count().await.unwrap();
    assert_eq!(count, EXPECTED_COUNT, "Parquet count mismatch");
}

async fn setup_test_store() -> Store {
    let encoding = QuadStorageEncodingName::PlainTerm;
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), encoding, 1);
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(NumProducts::N1_000, None).unwrap();
    let benchmark_name = benchmark.name();
    let ctx = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    benchmark.prepare_store(&ctx, false).await.unwrap()
}

async fn assert_count(path: &Path, format: RdfFormat) {
    let file = File::open(&path).await.unwrap();
    let result_store = Store::new_in_memory().await;
    result_store
        .load_from_reader(file, RdfParserOptions::with_format(format))
        .await
        .unwrap();

    assert_eq!(
        result_store.len().await.unwrap(),
        EXPECTED_COUNT,
        "Turtle count mismatch"
    );
}
