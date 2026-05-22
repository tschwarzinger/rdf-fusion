use anyhow::Context;
use clap::Parser;
use datafusion::common::runtime::SpawnedTask;
use datafusion::prelude::SessionConfig;
use rdf_fusion::common::RdfSortOrder;
use rdf_fusion::common::config::RdfFusionOptions;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion_bench::benchmarks::BenchmarkName;
use rdf_fusion_bench::{
    BenchQuadStorageType, BenchQuadStorageTypeArg, BenchmarkingConfig, Operation,
    QuadStorageEncodingNameArg, QuadStorageLocationArg, execute_benchmark_operation,
};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = RdfFusionBenchArgs::parse();

    let storage_encoding = QuadStorageEncodingName::from(args.storage_encoding);

    // Validate invalid combinations
    if args.storage_type == BenchQuadStorageTypeArg::Parquet
        && storage_encoding == QuadStorageEncodingName::ObjectId
    {
        anyhow::bail!("Parquet storage does not support object IDs.");
    }

    if args.storage_type == BenchQuadStorageTypeArg::Delta && args.sort_order.is_some() {
        anyhow::bail!("Sort order is only supported for Parquet storage.");
    }

    let storage_type = if args.storage_type == BenchQuadStorageTypeArg::Parquet {
        let sort_order_str = args.sort_order.as_deref().unwrap_or("ZORDER(PS)");
        BenchQuadStorageType::Parquet {
            sort_order: Some(sort_order_str.parse::<RdfSortOrder>()?),
        }
    } else {
        BenchQuadStorageType::Delta
    };

    let mut config =
        SessionConfig::from_env().context("Failed to obtain session config")?;
    config
        .options_mut()
        .extensions
        .insert(RdfFusionOptions::from_env()?);

    let options = BenchmarkingConfig {
        verbose_results: args.verbose_results,
        memory_size: args.memory_limit.map(|val| 1024 * 1024 * val),
        storage_location: args.storage_location,
        storage_type,
        storage_encoding,
        data_fusion_config: config,
    };

    let task = SpawnedTask::spawn(async move {
        execute_benchmark_operation(options, args.operation, args.benchmark).await
    });
    task.await
        .context("Failed to join on benchmarking task")?
        .context("Error while execuitng benchmarking task")?;

    Ok(())
}

#[derive(Parser)]
#[command(about, version, name = "rdf-fusion-bench")]
/// RDF Fusion command line toolkit and SPARQL HTTP server
pub struct RdfFusionBenchArgs {
    /// Indicates whether the benchmark should be prepared or executed.
    pub operation: Operation,
    /// Indicates whether the benchmark results should be verbose.
    #[arg(short, long, default_value = "false")]
    pub verbose_results: bool,
    /// Defines how much memory DataFusion is allowed to use. In MiB.
    #[arg(long)]
    pub memory_limit: Option<usize>,
    /// Defines where to store the database.
    #[arg(long, default_value = "on-disk")]
    pub storage_location: QuadStorageLocationArg,
    /// Defines how to store the database.
    #[arg(long, default_value = "delta")]
    pub storage_type: BenchQuadStorageTypeArg,
    /// Defines which encoding to use for the database.
    #[arg(long, default_value = "object-id")]
    pub storage_encoding: QuadStorageEncodingNameArg,
    /// The sort order to use for Parquet storage.
    #[arg(long)]
    pub sort_order: Option<String>,
    /// Indicates which benchmark should be executed.
    #[clap(subcommand)]
    pub benchmark: BenchmarkName,
}
