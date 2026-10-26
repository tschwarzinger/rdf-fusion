use clap::{Parser, Subcommand, ValueHint};

#[derive(Parser, Debug, Clone)]
#[command(about, version, name = "rdf-fusion")]
/// RDF Fusion command line toolkit and SPARQL HTTP server
pub struct Args {
    #[command(flatten)]
    pub storage: StorageConfigArgs,
    #[command(subcommand)]
    pub command: Command,
}

/// Configuration regarding RDF Fusion's storage.
#[derive(Parser, Debug, Clone)]
pub struct StorageConfigArgs {
    /// Whether the storage should be read-only or read-write.
    #[arg(long)]
    pub storage_type: QuadStorageType,
    /// The location of the storage.
    ///
    /// The accepted values for this setting differ depending on the chosen storage type. For
    /// example, a `delta-quads` storage requires a single location to a storage prefix, while a
    /// `parquet` storage accepts a single Parquet file or the directory of a dataset (ending with
    /// `/`) that contains a `dataset.ttl`.
    ///
    /// When loading into a `parquet` storage, a location that ends with `/` is treated as the
    /// directory of a dataset: each sort order (`RDF_FUSION_STORAGE_PARQUET_SORT_ORDERS`, e.g.,
    /// `NATIVE(GPOS);NATIVE(GSPO)`) is written as a separate quad table, split into multiple files
    /// (see `DATAFUSION_EXECUTION_SOFT_MAX_ROWS_PER_OUTPUT_FILE`), and described in a
    /// `dataset.ttl`. Otherwise, a single file is written.
    ///
    /// Supported locations:
    /// - in-memory database [`memory://`]
    /// - file store [`file://`]
    /// - S3-compatible object store [`s3a://[bucket].[endpoint]/path`]. S3 credentials are set via
    ///   the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
    #[arg(long)]
    pub location: String,
    /// The encoding to use for the storage.
    ///
    /// Supported encodings:
    /// - *plain-term*: Use the plain term encoding.
    /// - *string*: Use the string encoding.
    #[arg(long, default_value = "string")]
    pub encoding: String,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum QuadStorageType {
    /// Stores RDF quads in various [Delta Lake](https://delta.io) tables.
    ///
    /// Only supports a single location.
    DeltaQuads,
    /// Directly queries a set of Parquet files.
    Parquet,
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
        /// Print the query execution plan.
        #[arg(long)]
        explain: bool,
        /// If provided, the query is executed and the plan includes execution metrics.
        /// Only works in combination with --explain.
        #[arg(long, requires = "explain")]
        analyze: bool,
    },
    /// Load RDF files into a Parquet or Delta Lake storage.
    Load {
        /// The input RDF file to load.
        #[arg(required = true)]
        inputs: Vec<String>,
    },
    /// Optimizes the database.
    Optimize,
    /// Export the database to an RDF data dump.
    Dump {
        /// The location where the dump should be written.
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
        /// - *Native*: Use DataFusion's native order for the respective encoding (e.g., `NATIVE(GSPO)`)
        #[arg(long)]
        sort_by: Option<String>,
        /// The strategy to use when exporting quads to a triple-only format.
        ///
        /// Supported strategies:
        /// - *error*: returns an error if a non-default graph is encountered.
        /// - *ignore*: ignores the graph column and deduplicates the triples.
        #[arg(long, default_value = "error")]
        triple_fallback: String,
        /// The encoding to use for dumping.
        ///
        /// Supported encodings:
        /// - *plain-term*: Use the plain term encoding.
        /// - *string*: Use the string encoding.
        #[arg(long, default_value = "string")]
        encoding: String,
    },
}
