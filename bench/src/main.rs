use anyhow::Context;
use clap::{Parser, ValueEnum};
use datafusion::common::runtime::SpawnedTask;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion_bench::benchmarks::BenchmarkName;
use rdf_fusion_bench::{
    BenchmarkStorageBackend, BenchmarkingOptions, Operation, execute_benchmark_operation,
};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = RdfFusionBenchArgs::parse();

    let storage_backend = match args.storage_location {
        None | Some(StorageLocationArg::OnDisk) => {
            BenchmarkStorageBackend::DeltaLakeOnDisk
        }
        Some(StorageLocationArg::InMemory) => BenchmarkStorageBackend::DeltaLakeInMemory,
    };
    let storage_encoding = args
        .storage_encoding
        .map(Into::into)
        .unwrap_or(QuadStorageEncodingName::ObjectId);
    let options = BenchmarkingOptions {
        verbose_results: args.verbose_results,
        target_partitions: args.target_partitions,
        memory_size: args.memory_limit.map(|val| 1024 * 1024 * val),
        storage_backend,
        storage_encoding,
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
    /// Defines how many target partitions DataFusion should use.
    #[arg(short, long)]
    pub target_partitions: Option<usize>,
    /// Defines how much memory DataFusion is allowed to use. In MiB.
    #[arg(long)]
    pub memory_limit: Option<usize>,
    /// Defines where to store the database.
    #[arg(long)]
    pub storage_location: Option<StorageLocationArg>,
    /// Defines which encoding to use for the database.
    #[arg(long)]
    pub storage_encoding: Option<QuadStorageEncodingNameArg>,
    /// Indicates which benchmark should be executed.
    #[clap(subcommand)]
    pub benchmark: BenchmarkName,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum StorageLocationArg {
    /// The storage location is in-memory.
    InMemory,
    /// The storage location is on disk.
    OnDisk,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum QuadStorageEncodingNameArg {
    /// The storage location is in-memory.
    PlainTerm,
    /// Use the object id
    ObjectId,
}

impl From<QuadStorageEncodingNameArg> for QuadStorageEncodingName {
    fn from(value: QuadStorageEncodingNameArg) -> Self {
        match value {
            QuadStorageEncodingNameArg::PlainTerm => QuadStorageEncodingName::PlainTerm,
            QuadStorageEncodingNameArg::ObjectId => QuadStorageEncodingName::ObjectId,
        }
    }
}
