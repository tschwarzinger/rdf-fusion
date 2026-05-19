#![allow(clippy::print_stdout)]

use crate::benchmarks::bsbm::{
    BsbmBenchmark, BusinessIntelligenceUseCase, ExploreUseCase,
};
use crate::benchmarks::windfarm::WindFarmBenchmark;
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::RdfFusionBenchContext;
use clap::ValueEnum;
use datafusion::prelude::SessionConfig;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::store::DumpSortOrder;
use std::fs;

pub mod benchmarks;
pub mod environment;
pub mod operation;
pub mod prepare;
pub mod report;
pub mod runs;
mod utils;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum Operation {
    /// Prepares the data for a given benchmark.
    Prepare,
    /// Executes a given benchmark, assuming that the preparation has already been done.
    Execute,
}

/// Provides options for the benchmarking process.
pub struct BenchmarkingConfig {
    /// Indicates whether the benchmarking results should be verbose.
    ///
    /// For example, while non-verbose results could show an aggregated version of multiple runs,
    /// verbose results could write the results for each run.
    pub verbose_results: bool,
    /// The number of MiBs that DataFusion is allowed to Suse.
    pub memory_size: Option<usize>,
    /// The storage location to use for the benchmark.
    pub storage_location: QuadStorageLocationArg,
    /// The storage type to use for the benchmark.
    pub storage_type: QuadStorageType,
    /// The storage encoding to use for the benchmark.
    pub storage_encoding: QuadStorageEncodingName,
    /// The DataFusion config.
    pub data_fusion_config: SessionConfig,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum QuadStorageLocationArg {
    /// The storage location is in-memory.
    InMemory,
    /// The storage location is on disk.
    OnDisk,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum QuadStorageTypeArg {
    /// Uses a storage based on Delta Lake.
    Delta,
    /// The storage type is a single parquet file.
    Parquet,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum QuadStorageType {
    /// Uses a storage based on Delta Lake.
    Delta,
    /// The storage type is a single parquet file.
    Parquet {
        /// The sort order for the parquet file.
        sort_order: Option<DumpSortOrder>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum QuadStorageEncodingNameArg {
    /// The plain term encoding
    PlainTerm,
    /// The string encoding
    String,
    /// Use the object id
    ObjectId,
}

impl From<QuadStorageEncodingNameArg> for QuadStorageEncodingName {
    fn from(value: QuadStorageEncodingNameArg) -> Self {
        match value {
            QuadStorageEncodingNameArg::PlainTerm => QuadStorageEncodingName::PlainTerm,
            QuadStorageEncodingNameArg::String => QuadStorageEncodingName::String,
            QuadStorageEncodingNameArg::ObjectId => QuadStorageEncodingName::ObjectId,
        }
    }
}

/// Executes an `operation` of a given `benchmark`.
///
/// - [Operation::Prepare] prepares the data for the given benchmark.
/// - [Operation::Execute] executes the given benchmark. The runner verifies the requirements before
///   executing the benchmark (e.g., whether a file exists).
pub async fn execute_benchmark_operation(
    options: BenchmarkingConfig,
    operation: Operation,
    benchmark: BenchmarkName,
) -> anyhow::Result<()> {
    let current_dir = std::env::current_dir()?;
    let bench_files = current_dir.join("bench_files");
    let data = current_dir.join("data");
    let databases = current_dir.join("databases");
    let results = current_dir.join("results");

    fs::create_dir_all(&data)?;
    fs::create_dir_all(&results)?;

    let context =
        RdfFusionBenchContext::builder(options, bench_files, data, databases, results)
            .build();

    let benchmark = create_benchmark_instance(benchmark)?;
    match operation {
        Operation::Prepare => {
            println!("Preparing benchmark '{}' ...", benchmark.name());

            let bench_ctx = context.create_benchmark_context(benchmark.name())?;
            if bench_ctx.data_dir().exists() {
                println!(
                    "Cleaning data directory '{}' ...",
                    bench_ctx.data_dir().display()
                );
                fs::remove_dir_all(bench_ctx.data_dir())?;
            }
            fs::create_dir_all(bench_ctx.data_dir())?;

            for requirement in
                benchmark.requirements(bench_ctx.bench_files_dir().as_path())
            {
                bench_ctx.prepare_requirement(requirement).await?;
            }

            println!("Benchmark '{}' prepared.\n", benchmark.name());
        }
        Operation::Execute => {
            println!("Executing benchmark '{}' ...\n", benchmark.name());

            let bench_ctx = context.create_benchmark_context(benchmark.name())?;
            if bench_ctx.results_dir().exists() {
                println!(
                    "Cleaning results directory '{}' ...",
                    bench_ctx.results_dir().display()
                );
                fs::remove_dir_all(bench_ctx.results_dir())?;
            }
            fs::create_dir_all(bench_ctx.results_dir())?;

            println!("Verifying requirements ...");
            for requirement in
                benchmark.requirements(bench_ctx.bench_files_dir().as_path())
            {
                bench_ctx.ensure_requirement(requirement)?;
            }
            println!("Requirements verified\n");

            println!("Executing benchmark ...");
            {
                let report = benchmark.execute(&bench_ctx).await?;
                report.write_results(bench_ctx.results_dir().as_path())?;
            }
            println!("Benchmark '{}' done\n", benchmark.name());
        }
    }
    Ok(())
}

fn create_benchmark_instance(
    benchmark: BenchmarkName,
) -> anyhow::Result<Box<dyn Benchmark>> {
    let benchmark: Box<dyn Benchmark> = match benchmark {
        BenchmarkName::BsbmExplore {
            num_products: dataset_size,
            max_query_count: query_size,
        } => Box::new(BsbmBenchmark::<ExploreUseCase>::try_new(
            dataset_size,
            query_size,
        )?),
        BenchmarkName::BsbmBusinessIntelligence {
            num_products: dataset_size,
            max_query_count: query_size,
        } => Box::new(BsbmBenchmark::<BusinessIntelligenceUseCase>::try_new(
            dataset_size,
            query_size,
        )?),
        BenchmarkName::WindFarm { num_turbines } => {
            Box::new(WindFarmBenchmark::new(num_turbines))
        }
    };
    Ok(benchmark)
}
