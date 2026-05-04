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
use std::fs;
use std::path::PathBuf;

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
pub struct BenchmarkingOptions {
    /// Indicates whether the benchmarking results should be verbose.
    ///
    /// For example, while non-verbose results could show an aggregated version of multiple runs,
    /// verbose results could write the results for each run.
    pub verbose_results: bool,
    /// The number of MiBs that DataFusion is allowed to Suse.
    pub memory_size: Option<usize>,
    /// The storage backend to use for the benchmark.
    pub storage_backend: BenchmarkStorageBackend,
    /// The storage encoding to use for the benchmark.
    pub storage_encoding: QuadStorageEncodingName,
    /// The DataFusion config.
    pub config: SessionConfig,
}

/// Represents the possible storage backends a benchmark.
pub enum BenchmarkStorageBackend {
    /// A Delta Lake storage backend that is stored in-memory.
    DeltaLakeInMemory,
    /// A Delta Lake storage backend that is stored on disk.
    DeltaLakeOnDisk,
}

/// Executes an `operation` of a given `benchmark`.
///
/// - [Operation::Prepare] prepares the data for the given benchmark.
/// - [Operation::Execute] executes the given benchmark. The runner verifies the requirements before
///   executing the benchmark (e.g., whether a file exists).
pub async fn execute_benchmark_operation(
    options: BenchmarkingOptions,
    operation: Operation,
    benchmark: BenchmarkName,
) -> anyhow::Result<()> {
    let bench_files = PathBuf::from("./bench_files");
    let data = PathBuf::from("./data");
    let databases = PathBuf::from("./databases");
    let results = PathBuf::from("./results");

    fs::create_dir_all(&data)?;
    fs::create_dir_all(&results)?;

    let context =
        RdfFusionBenchContext::new(options, bench_files, data, databases, results);

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
