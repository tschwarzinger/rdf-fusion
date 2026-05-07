use clap::{Parser, Subcommand, ValueHint};
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(about, version, name = "rdf-fusion")]
/// RDF Fusion command line toolkit and SPARQL HTTP server
pub struct Args {
    /// Runtime configuration options
    #[command(flatten)]
    pub runtime: RuntimeConfig,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Parser, Debug, Clone)]
/// Runtime configuration options
pub struct RuntimeConfig {
    /// Memory limit for the process in MiB. Note that this limit only applies to the query engine.
    /// For example, an in-memory storage will not be included in this limit.
    #[arg(long)]
    pub memory_limit: Option<usize>,
    /// The location of the database. If [`None`], an in-memory database is used.
    ///
    /// Supported locations: in-memory database [`memory://`], file store [`file://`], S3-compatible object store [`s3a://[bucket].[endpoint]/path`].
    /// S3 credentials are set via the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
    #[arg(long)]
    pub location: Option<String>,
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
    /// Build a database at the configured location.
    BuildDatabase {
        #[arg(long)]
        inputs: Vec<PathBuf>,
    },
}
