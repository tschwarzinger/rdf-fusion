//! Measures loading RDF files into Parquet files with the [`RdfParquetLoader`].
//!
//! The benchmark uses prefixes of the dblp RDF dump (see `just prepare-dblp`). Sizes that have not
//! been prepared are skipped. The following environment variables configure the benchmark:
//! - `RDF_FUSION_BENCH_DBLP_SIZES`: the dataset sizes (default: `100k,1m`).
//! - `RDF_FUSION_BENCH_LOAD_PARTITIONS`: the numbers of target partitions (default: `1,4,8`).
//! - `RDF_FUSION_BENCH_LOAD_SORT_ORDERS`: the sort orders, `none` for unsorted
//!   (default: `none,NATIVE(GPOS)`).

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rdf_fusion::common::{GraphName, RdfInput, RdfSortOrder};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::{
    RdfFusionContextBuilder, session_config_from_env_for_rdf_fusion,
};
use rdf_fusion::storage::delta::DeltaQuadsStorageBuilder;
use rdf_fusion::storage::parquet::RdfParquetLoader;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use url::Url;

fn bench_load_parquet(c: &mut Criterion) {
    let sizes = env_list("RDF_FUSION_BENCH_DBLP_SIZES", "100k,1m");
    let partitions = env_list("RDF_FUSION_BENCH_LOAD_PARTITIONS", "1,4,8")
        .into_iter()
        .map(|p| p.parse::<usize>().expect("Invalid number of partitions"))
        .collect::<Vec<_>>();
    let sort_orders = env_list("RDF_FUSION_BENCH_LOAD_SORT_ORDERS", "none,NATIVE(GPOS)")
        .into_iter()
        .map(|order| match order.as_str() {
            "none" => None,
            order => Some(RdfSortOrder::from_str(order).expect("Invalid sort order")),
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("Load Parquet");
    for size in &sizes {
        let input = PathBuf::from(format!("./data/dblp-{size}/dblp.nt"));
        if !input.exists() {
            eprintln!(
                "Skipping dblp-{size}: {} does not exist. Run `just prepare-dblp {size}` first.",
                input.display()
            );
            continue;
        }
        let input_url = Url::from_file_path(input.canonicalize().unwrap()).unwrap();

        for &target_partitions in &partitions {
            let runtime = create_runtime(target_partitions);
            for sort_order in &sort_orders {
                let sort_order_name = sort_order
                    .as_ref()
                    .map_or_else(|| "none".to_owned(), ToString::to_string);
                let loader = runtime
                    .block_on(create_loader(target_partitions, sort_order.clone()));
                group.bench_function(
                    format!(
                        "dblp-{size}, partitions={target_partitions}, sort_order={sort_order_name}"
                    ),
                    |b| {
                        b.to_async(&runtime).iter_batched(
                            || tempfile::tempdir().unwrap(),
                            |output_dir| async {
                                let input = RdfInput::try_new(
                                    input_url.clone(),
                                    GraphName::DefaultGraph,
                                )
                                .unwrap();
                                let output_url = Url::from_directory_path(
                                    output_dir.path().join("dataset"),
                                )
                                .unwrap();
                                loader.load(input, output_url).await.unwrap();
                                output_dir
                            },
                            BatchSize::PerIteration,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

/// Creates a loader that uses `target_partitions` partitions and writes the quads in `sort_order`.
///
/// The remaining configuration is loaded from the environment.
async fn create_loader(
    target_partitions: usize,
    sort_order: Option<RdfSortOrder>,
) -> RdfParquetLoader {
    let session_config = session_config_from_env_for_rdf_fusion()
        .unwrap()
        .with_target_partitions(target_partitions);

    // The loader only uses the context for planning the load. The storage is a dummy.
    let dummy_storage = Arc::new(DeltaQuadsStorageBuilder::new().build().await.unwrap());
    let context = RdfFusionContextBuilder::new(dummy_storage)
        .with_session_config(Some(session_config))
        .build()
        .unwrap();
    RdfParquetLoader::try_new(
        context.session_context().clone(),
        context.create_view(),
        QuadStorageEncodingName::String,
        sort_order,
    )
    .unwrap()
}

fn create_runtime(target_partitions: usize) -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(target_partitions)
        .enable_all()
        .build()
        .unwrap()
}

/// Reads a comma-separated list from the environment variable `name`.
fn env_list(name: &str, default: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}

criterion_group!(
    name = bench_load;
    config = Criterion::default().sample_size(10);
    targets = bench_load_parquet
);
criterion_main!(bench_load);
