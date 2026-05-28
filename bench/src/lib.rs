#![allow(clippy::print_stdout)]

use crate::benchmarks::bsbm::{
    BsbmBenchmark, BusinessIntelligenceUseCase, ExploreUseCase,
};
use crate::benchmarks::windfarm::WindFarmBenchmark;
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::{BenchmarkContext, RdfFusionBenchContext};
use clap::ValueEnum;
use std::fs;

pub mod benchmarks;
pub mod config;
pub mod environment;
pub mod operation;
pub mod prepare;
pub mod report;
pub mod runs;
mod utils;

pub use config::*;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum Operation {
    /// Prepares the data for a given benchmark.
    Prepare,
    /// Executes a given benchmark, assuming that the preparation has already been done.
    Execute,
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
    let bench_ctx = context.create_benchmark_context(benchmark)?;
    let benchmark = create_benchmark_instance(&bench_ctx, benchmark)?;

    match operation {
        Operation::Prepare => {
            println!("Preparing benchmark '{}' ...", benchmark.name());

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
    context: &BenchmarkContext,
    benchmark: BenchmarkName,
) -> anyhow::Result<Box<dyn Benchmark>> {
    let benchmark: Box<dyn Benchmark> = match benchmark {
        BenchmarkName::BsbmExplore {
            num_products: dataset_size,
            max_query_count: query_size,
        } => Box::new(BsbmBenchmark::<ExploreUseCase>::try_new(
            context,
            dataset_size,
            query_size,
        )?),
        BenchmarkName::BsbmBusinessIntelligence {
            num_products: dataset_size,
            max_query_count: query_size,
        } => Box::new(BsbmBenchmark::<BusinessIntelligenceUseCase>::try_new(
            context,
            dataset_size,
            query_size,
        )?),
        BenchmarkName::WindFarm { num_turbines } => {
            Box::new(WindFarmBenchmark::try_new(context, num_turbines)?)
        }
    };
    Ok(benchmark)
}
