mod file_size;
mod scanned_bytes;

use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::store::{RdfDumpOptions, Store};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;

pub struct ParquetTestConfig {
    pub name: String,
    pub config: RdfDumpOptions,
}

impl ParquetTestConfig {
    pub fn new(name: impl Into<String>, config: RdfDumpOptions) -> Self {
        Self {
            name: name.into(),
            config,
        }
    }
}

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

fn format_bytes(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let len = s.len();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            result.push(' ');
        }
        result.push(c);
    }
    result
}

async fn get_dumped_bytes(store: &Store, url_str: &str) -> bytes::Bytes {
    use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
    use object_store::ObjectStoreExt;
    use url::Url;

    let url = Url::parse(url_str).unwrap();
    let object_store = store
        .context()
        .session_context()
        .runtime_env()
        .object_store(url.as_object_store_url())
        .unwrap();

    let path = object_store::path::Path::from(url.path());

    object_store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
}
