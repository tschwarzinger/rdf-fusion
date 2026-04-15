use clap::{Parser, Subcommand, ValueHint};

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
}
