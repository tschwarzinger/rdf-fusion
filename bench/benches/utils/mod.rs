#![allow(dead_code)]

use anyhow::Context;
use futures::StreamExt;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::store::Store;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::{BenchQuadStorageTypeArg, QuadStorageLocationArg};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use tokio::runtime::{Builder, Runtime};

pub mod verbose;

pub struct BenchmarkConfig {
    pub name: String,
    pub storage_type: BenchQuadStorageTypeArg,
    pub storage_location: QuadStorageLocationArg,
    pub encoding: QuadStorageEncodingName,
}

impl BenchmarkConfig {
    pub fn bench_context(&self) -> RdfFusionBenchContext {
        RdfFusionBenchContext::new_for_criterion(
            PathBuf::from("./data"),
            self.encoding,
            1,
        )
        .with_storage_type(self.storage_type)
        .with_storage_location(self.storage_location)
        .build()
    }
}

impl Display for BenchmarkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "storage_type={:?}, storage_location={:?}, encoding={}",
            self.storage_type, self.storage_location, self.encoding
        )
    }
}

pub fn benchmark_configs() -> Vec<BenchmarkConfig> {
    vec![
        BenchmarkConfig {
            name: "DQ-ObjectId".to_string(),
            storage_type: BenchQuadStorageTypeArg::DeltaQuads,
            storage_location: QuadStorageLocationArg::OnDisk,
            encoding: QuadStorageEncodingName::ObjectId,
        },
        BenchmarkConfig {
            name: "DQ-String".to_string(),
            storage_type: BenchQuadStorageTypeArg::DeltaQuads,
            storage_location: QuadStorageLocationArg::OnDisk,
            encoding: QuadStorageEncodingName::String,
        },
        BenchmarkConfig {
            name: "DQ-PlainTerm".to_string(),
            storage_type: BenchQuadStorageTypeArg::DeltaQuads,
            storage_location: QuadStorageLocationArg::OnDisk,
            encoding: QuadStorageEncodingName::PlainTerm,
        },
        BenchmarkConfig {
            name: "Parquet-String".to_string(),
            storage_type: BenchQuadStorageTypeArg::Parquet,
            storage_location: QuadStorageLocationArg::OnDisk,
            encoding: QuadStorageEncodingName::String,
        },
        BenchmarkConfig {
            name: "Parquet-PlainTerm".to_string(),
            storage_type: BenchQuadStorageTypeArg::Parquet,
            storage_location: QuadStorageLocationArg::OnDisk,
            encoding: QuadStorageEncodingName::PlainTerm,
        },
    ]
}

pub async fn consume_results(result: QueryResults) -> anyhow::Result<usize> {
    match result {
        QueryResults::Solutions(solutions) => {
            let mut inner = solutions
                .into_record_batch_stream()
                .context("Failed to convert solutions to record batch stream")?;

            let mut count = 0;
            while let Some(sol) = inner.next().await {
                count += sol.context("Error while getting record batch.")?.num_rows();
            }
            Ok(count)
        }
        QueryResults::Graph(mut triples) => {
            let mut count = 0;
            while let Some(sol) = triples.next().await {
                sol.context("Error while getting triple.")?;
                count += 1;
            }
            Ok(count)
        }
        _ => panic!("Unexpected QueryResults"),
    }
}

/// Sets up the runtime, context, and prepares the store for a benchmark.
pub fn setup_benchmark_env<'ctx, B: Benchmark>(
    benchmarking_context: &'ctx RdfFusionBenchContext,
    benchmark: &B,
) -> (Runtime, BenchmarkContext<'ctx>, Store) {
    let target_partitions = benchmarking_context
        .options()
        .data_fusion_config
        .target_partitions();
    let runtime = create_runtime(target_partitions);

    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = runtime
        .block_on(benchmark.prepare_store(&benchmark_context, false))
        .context("
    Failed to prepare store. Have you downloaded the data?

    Execute `just prepare-benches` for downloading the data. Then, run the benchmark from the `bench` directory.
    ")
        .unwrap();

    (runtime, benchmark_context, store)
}

pub fn create_runtime(target_partitions: usize) -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(target_partitions)
        .enable_all()
        .build()
        .unwrap()
}
