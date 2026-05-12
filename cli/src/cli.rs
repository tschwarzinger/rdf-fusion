use clap::{Parser, Subcommand, ValueHint};
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(about, version, name = "rdf-fusion")]
/// RDF Fusion command line toolkit and SPARQL HTTP server
pub struct Args {
    #[command(flatten)]
    pub runtime: RuntimeConfig,
    #[command(flatten)]
    pub storage: StorageConfigArgs,
    #[command(subcommand)]
    pub command: Command,
}

/// Runtime configuration options
///
/// TODO: Environment variable
#[derive(Parser, Debug, Clone)]
pub struct RuntimeConfig {
    /// Memory limit for the process in MiB. Note that this limit only applies to the query engine.
    /// For example, an in-memory storage will not be included in this limit.
    #[arg(long)]
    pub memory_limit: Option<usize>,
}

/// Configuration regarding RDF Fusion's storage.
#[derive(Parser, Debug, Clone)]
pub struct StorageConfigArgs {
    /// Whether the storage should be read-only or read-write.
    #[arg(long)]
    pub storage_type: QuadStorageType,
    /// The location of the storage.
    ///
    /// The semantics of this setting differ depending on the chosen storage type. For example,
    /// a `delta-lake` storage requires a single location to directory, while a `rdf-file` storage
    /// requires a list of locations that point to individual RDF files.
    ///
    /// Supported locations:
    /// - in-memory database [`memory://`]
    /// - file store [`file://`]
    /// - S3-compatible object store [`s3a://[bucket].[endpoint]/path`]. S3 credentials are set via
    ///   the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
    #[arg(long, action = clap::ArgAction::Append)]
    pub location: Option<Vec<String>>,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum QuadStorageType {
    /// Stores RDF quads in various [Delta Lake](https://delta.io) tables.
    ///
    /// Only supports a single location.
    DeltaLake,
    /// Directly queries a set of RDF files.
    ///
    ///
    RdfFiles,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Start RDF Fusion HTTP server in read-write mode
    Serve {
        /// Host and port to listen to
        #[arg(short, long, default_value = "0.0.0.0:7878", value_hint = ValueHint::Hostname)]
        bind: String,
        /// Allows cross-origin requests
        #[arg(long)]
        cors: bool,
        /// If the SPARQL queries should look for triples in all the dataset graphs by default
        /// (i.e., without `GRAPH` operations).
        ///
        /// This is equivalent as setting the union-default-graph option in all SPARQL queries
        #[arg(long)]
        union_default_graph: bool,
    },
    /// Execute a SPARQL query against the database
    Query {
        /// The SPARQL query string to execute
        query: String,
    },
    /// Build a database at the configured location.
    BuildDatabase {
        #[arg(long)]
        inputs: Vec<PathBuf>,
    },
    /// Export the database to an RDF data dump.
    Dump {
        /// The location where the dump should be written.
        #[arg(long)]
        output: String,
        /// The format of the output RDF data dump. If not provided, RDF Fusion tries to guess it
        /// from the extension.
        #[arg(long)]
        format: Option<String>,
        /// Dump a specific graph. If not provided, dumps all graphs.
        #[arg(long)]
        graph: Option<String>,
        /// Sort the output by the given columns.
        ///
        /// Supports the following sort specifications:
        /// - *Regular*: regular sorting as defined in SPARQL's ORDER BY (e.g., `GSPO`, `SP`)
        /// - [*ZOrder*]: Interleave bits of the components (e.g., `ZOrder(PS)`)
        ///
        /// [*ZOrder*]: https://en.wikipedia.org/wiki/Z-order_curve
        #[arg(long)]
        sort_by: Option<String>,
    },
}
