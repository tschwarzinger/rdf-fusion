mod file_size;

use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::store::Store;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;

async fn setup_test_store() -> Store {
    let benchmarking_context = RdfFusionBenchContext::new_for_criterion(
        PathBuf::from("./data"),
        QuadStorageEncodingName::String,
        1,
    )
    .build();
    let name = rdf_fusion_bench::benchmarks::BenchmarkName::BsbmExplore {
        num_products: NumProducts::N1_000,
        max_query_count: None,
    };
    let ctx = benchmarking_context.create_benchmark_context(name).unwrap();
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(&ctx, NumProducts::N1_000, None)
            .unwrap();

    benchmark.prepare_store(&ctx, false).await.unwrap()
}
