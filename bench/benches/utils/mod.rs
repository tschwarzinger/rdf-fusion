#![allow(dead_code)]

use anyhow::Context;
use futures::StreamExt;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::store::Store;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, BsbmUseCase, NumProducts};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::{BenchQuadStorageTypeArg, QuadStorageLocationArg};
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use tokio::runtime::{Builder, Runtime};

pub mod verbose;

#[derive(Clone, Debug, PartialEq, Eq)]
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

/// Returns all available benchmark storage configurations.
fn all_benchmark_configs() -> Vec<BenchmarkConfig> {
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

/// Returns the default benchmark storage configurations when none are explicitly requested.
fn default_benchmark_configs() -> Vec<BenchmarkConfig> {
    vec![
        BenchmarkConfig {
            name: "DQ-ObjectId".to_string(),
            storage_type: BenchQuadStorageTypeArg::DeltaQuads,
            storage_location: QuadStorageLocationArg::OnDisk,
            encoding: QuadStorageEncodingName::ObjectId,
        },
        BenchmarkConfig {
            name: "Parquet-String".to_string(),
            storage_type: BenchQuadStorageTypeArg::Parquet,
            storage_location: QuadStorageLocationArg::OnDisk,
            encoding: QuadStorageEncodingName::String,
        },
    ]
}

/// Parses storage backend and encoding combinations from a configuration string.
///
/// Supports tuples: `(parquet, string)` and `(deltaquads, object_id)`.
/// Multiple tuples can be comma-separated: `(parquet, string), (deltaquads, object_id)`.
///
/// Panics if the input contains an invalid format or unsupported pair.
pub fn parse_storage_backends(input: &str) -> Vec<BenchmarkConfig> {
    let all = all_benchmark_configs();
    let mut selected = Vec::new();

    let trimmed = input.trim();
    if trimmed.is_empty() {
        panic!(
            "RDF_FUSION_BENCH_STORAGE_BACKENDS is set but empty. \
             Expected format e.g. '(parquet, string), (deltaquads, object_id)'"
        );
    }

    let mut rest = trimmed;
    while let Some(start) = rest.find('(') {
        let before = &rest[..start];
        if before.chars().any(|c| !c.is_whitespace() && c != ',') {
            panic!(
                "Invalid syntax in RDF_FUSION_BENCH_STORAGE_BACKENDS='{input}': unexpected token '{before}'. \
                 Expected format e.g. '(parquet, string), (deltaquads, object_id)'"
            );
        }

        if let Some(end) = rest[start..].find(')') {
            let inner = &rest[start + 1..start + end];
            rest = &rest[start + end + 1..];

            let Some((backend_part, encoding_part)) = inner.split_once(',') else {
                panic!(
                    "Malformed storage backend pair '({inner})' in RDF_FUSION_BENCH_STORAGE_BACKENDS='{input}'. \
                     Expected '(backend, encoding)' e.g. '(parquet, string)'"
                );
            };

            let backend_str = backend_part
                .trim()
                .to_ascii_lowercase()
                .replace(['_', '-'], "");
            let encoding_str = encoding_part
                .trim()
                .to_ascii_lowercase()
                .replace(['_', '-'], "");

            let storage_type = match backend_str.as_str() {
                "parquet" => Some(BenchQuadStorageTypeArg::Parquet),
                "delta" | "deltaquads" | "dq" => {
                    Some(BenchQuadStorageTypeArg::DeltaQuads)
                }
                _ => None,
            };

            let encoding = match encoding_str.as_str() {
                "string" | "str" => Some(QuadStorageEncodingName::String),
                "objectid" | "oid" => Some(QuadStorageEncodingName::ObjectId),
                "plainterm" | "pt" => Some(QuadStorageEncodingName::PlainTerm),
                _ => None,
            };

            if let (Some(st), Some(enc)) = (storage_type, encoding)
                && let Some(config) = all
                    .iter()
                    .find(|c| c.storage_type == st && c.encoding == enc)
            {
                if !selected
                    .iter()
                    .any(|c: &BenchmarkConfig| c.name == config.name)
                {
                    selected.push(config.clone());
                }
            } else {
                panic!(
                    "Unsupported storage backend pair '({inner})' in RDF_FUSION_BENCH_STORAGE_BACKENDS='{input}'. \
                     Supported pairs: (parquet, string), (deltaquads, object_id)"
                );
            }
        } else {
            panic!("Unclosed parenthesis in RDF_FUSION_BENCH_STORAGE_BACKENDS='{input}'");
        }
    }

    if rest.chars().any(|c| !c.is_whitespace() && c != ',') {
        panic!(
            "Invalid syntax in RDF_FUSION_BENCH_STORAGE_BACKENDS='{input}': unexpected trailing token '{rest}'. \
             Expected format e.g. '(parquet, string), (deltaquads, object_id)'"
        );
    }

    if selected.is_empty() {
        panic!(
            "No storage backend pairs found in RDF_FUSION_BENCH_STORAGE_BACKENDS='{input}'. \
             Expected format e.g. '(parquet, string), (deltaquads, object_id)'"
        );
    }

    selected
}

/// Returns the benchmark configurations to run.
///
/// Can be overridden via the `RDF_FUSION_BENCH_STORAGE_BACKENDS` environment variable
/// (e.g. `RDF_FUSION_BENCH_STORAGE_BACKENDS="(parquet, string), (deltaquads, object_id)"`).
pub fn benchmark_configs() -> Vec<BenchmarkConfig> {
    if let Ok(env_val) = std::env::var("RDF_FUSION_BENCH_STORAGE_BACKENDS") {
        parse_storage_backends(&env_val)
    } else {
        default_benchmark_configs()
    }
}

/// Returns the number of products to benchmark, configurable via `RDF_FUSION_BENCH_NUM_PRODUCTS`.
/// Defaults to `NumProducts::N1_000` (which has pre-selected queries and empty queries).
pub fn bench_num_products() -> NumProducts {
    match std::env::var("RDF_FUSION_BENCH_NUM_PRODUCTS").as_deref() {
        Ok("1000" | "1_000") => NumProducts::N1_000,
        Ok("10000" | "10_000") => NumProducts::N10_000,
        _ => {
            if Path::new("./data/bsbm-10000").exists()
                && !Path::new("./data/bsbm-1000").exists()
            {
                NumProducts::N10_000
            } else {
                NumProducts::N1_000
            }
        }
    }
}

/// Loads queries for a BSBM benchmark, optionally including pre-selected empty queries.
pub fn load_bsbm_queries<U: BsbmUseCase>(
    benchmark: &BsbmBenchmark<U>,
    context: &BenchmarkContext,
    include_empty: bool,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut queries = benchmark.queries(context)?;
    if include_empty {
        queries.extend(benchmark.empty_queries(context).unwrap_or_default());
    }
    Ok(queries)
}

/// Drains `result` and returns the number of results. Solutions are only drained as record
/// batches (i.e., not materialized as individual solutions). A boolean result counts as one
/// result.
pub async fn consume_results(result: QueryResults) -> anyhow::Result<usize> {
    match result {
        QueryResults::Boolean(_) => Ok(1),
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
