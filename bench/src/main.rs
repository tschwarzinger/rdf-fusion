use anyhow::Context;
use clap::Parser;
use datafusion::common::runtime::SpawnedTask;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion_bench::benchmarks::BenchmarkName;
use rdf_fusion_bench::{
    BenchQuadStorageTypeArg, BenchmarkingConfig, Operation, QuadStorageEncodingNameArg,
    QuadStorageLocationArg, execute_benchmark_operation,
};
use tracing::warn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    if std::env::var("RDF_FUSION_LOCAL_WORK_DIR").is_ok() {
        warn!(
            "Environment variable RDF_FUSION_LOCAL_WORK_DIR is set. This could lead to accidentally associating an non-matching object id dictionary with a new database.",
        );
    }

    let args = RdfFusionBenchArgs::parse();
    let storage_encoding = QuadStorageEncodingName::from(args.storage_encoding);

    // Validate invalid combinations
    if args.storage_type == BenchQuadStorageTypeArg::Parquet
        && storage_encoding == QuadStorageEncodingName::ObjectId
    {
        anyhow::bail!("Parquet storage does not support object IDs.");
    }

    let options = BenchmarkingConfig::from_env()
        .context("Failed to load configuration")?
        .with_storage_location(args.storage_location)
        .with_storage_type(args.storage_type)
        .with_storage_encoding(storage_encoding);

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
    /// Defines where to store the database.
    #[arg(long, default_value = "on-disk")]
    pub storage_location: QuadStorageLocationArg,
    /// Defines how to store the database.
    #[arg(long, default_value = "delta-quads")]
    pub storage_type: BenchQuadStorageTypeArg,
    /// Defines which encoding to use for the database.
    #[arg(long, default_value = "object-id")]
    pub storage_encoding: QuadStorageEncodingNameArg,
    /// Indicates which benchmark should be executed.
    #[clap(subcommand)]
    pub benchmark: BenchmarkName,
}
