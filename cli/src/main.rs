#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::use_debug)]
use crate::cli::{Args, Command};
use anyhow::{Context, bail};
use clap::Parser;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::{FairSpillPool, TrackConsumersPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::object_store::memory::InMemory;
use datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use deltalake::logstore::{IORuntime, StorageConfig, logstore_with};
use object_store::aws::AmazonS3Builder;
use object_store::http::HttpBuilder;
use rdf_fusion::common::config::{RdfFusionOptions, RdfFusionSessionConfigExt};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::{
    RdfFusionContext, RdfFusionContextBuilder, session_config_from_env_for_rdf_fusion,
};
use rdf_fusion::storage::delta::{DeltaQuadsStorageBuilder, LoadMode};
use rdf_fusion::storage::parquet::ParquetQuadStorage;
use rdf_fusion::store::Store;
use rdf_fusion_extensions::storage::QuadStorage;
use std::env;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::Handle;
use tracing::warn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use url::Url;

mod cli;
mod commands;

#[cfg(not(feature = "perf-debugging"))]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "perf-debugging")]
    console_subscriber::init();

    #[cfg(not(feature = "perf-debugging"))]
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    let args = Args::parse();

    match &args.command {
        Command::Serve {
            bind,
            cors,
            union_default_graph,
        } => {
            let store = create_store(&args).await?;
            commands::serve::serve(store, bind, false, *cors, *union_default_graph).await
        }
        Command::Load { inputs } => {
            let location_str = resolve_location(&args.storage.location)?;
            let location_url = Url::parse(&location_str)?;
            let inputs = inputs
                .iter()
                .map(|u| Ok(Url::parse(&resolve_location(u)?)?))
                .collect::<Result<Vec<Url>, anyhow::Error>>()?;

            match args.storage.storage_type {
                cli::QuadStorageType::Parquet => {
                    let context = create_context_for_parquet_load(&args).await?;
                    let session_context = context.session_context().clone();
                    let context_view = context.create_view();
                    let sort_orders =
                        context.options().storage.parquet.sort_orders.clone();
                    let encoding =
                        QuadStorageEncodingName::from_str(&args.storage.encoding)?;

                    commands::load::load(
                        commands::load::LoadCommandType::Parquet {
                            session_context,
                            context_view,
                            encoding,
                            sort_orders,
                        },
                        &inputs,
                        location_url,
                    )
                    .await
                }
                cli::QuadStorageType::DeltaQuads => {
                    let store = create_store(&args).await?;
                    commands::load::load(
                        commands::load::LoadCommandType::Other(store),
                        &inputs,
                        location_url,
                    )
                    .await
                }
            }
        }
        Command::Optimize => {
            let store = create_store(&args).await?;
            commands::optimize::optimize(store).await
        }
        Command::Query {
            query,
            explain,
            analyze,
            format,
        } => {
            let format = commands::query::ResultsOutputFormat::from_str(format)?;
            let store = create_store(&args).await?;
            commands::query::query(store, query.clone(), *explain, *analyze, format).await
        }
        Command::Dump {
            output,
            format,
            graph,
            sort_by,
            triple_fallback,
            encoding,
        } => {
            let store = create_store(&args).await?;
            let output_url = resolve_location(output)?;
            commands::dump::dump(
                store,
                output_url,
                format.clone(),
                graph.clone(),
                sort_by.clone(),
                triple_fallback.clone(),
                encoding.clone(),
            )
            .await
        }
    }
}

/// Creates a [`Store`] instance from the given arguments.
async fn create_store(args: &Args) -> anyhow::Result<Store> {
    let location = Url::parse(&resolve_location(&args.storage.location)?)
        .context("Invalid object store URL")?;
    let session_config = create_cli_session_config(&location)?;
    let rdf_fusion_options = session_config
        .rdf_fusion_options()
        .expect("RDF fusion options must be set")
        .clone();

    let runtime_env = build_runtime_env(args)?;

    let encoding = QuadStorageEncodingName::from_str(&args.storage.encoding)?;

    let storage: Arc<dyn QuadStorage> = match args.storage.storage_type {
        cli::QuadStorageType::DeltaQuads => {
            create_delta_storage(
                &location,
                encoding,
                &runtime_env,
                &session_config,
                &rdf_fusion_options,
            )
            .await?
        }
        cli::QuadStorageType::Parquet => {
            if matches!(encoding, QuadStorageEncodingName::ObjectId) {
                bail!("ObjectId encoding is not supported for Parquet storage");
            }

            // Use the same runtime environment as the query engine, such that the eagerly loaded
            // Parquet footers are cached for querying.
            let loading_state = SessionStateBuilder::new()
                .with_runtime_env(Arc::clone(&runtime_env))
                .with_config(session_config.clone())
                .build();
            Arc::new(
                ParquetQuadStorage::builder(location)
                    .with_encoding(encoding)
                    .with_options(rdf_fusion_options.storage.parquet.clone())
                    .build(&loading_state)
                    .await
                    .context("Failed to create Parquet storage")?,
            )
        }
    };

    let context = RdfFusionContextBuilder::new(storage)
        .with_runtime_env(Some(runtime_env))
        .with_session_config(Some(session_config))
        .build()
        .context("Failed to create RDF Fusion Context")?;
    Ok(Store::new(context))
}

/// Creates the session config of the CLI for a store at `location`.
fn create_cli_session_config(location: &Url) -> anyhow::Result<SessionConfig> {
    let mut config = session_config_from_env_for_rdf_fusion()?;

    // CLI-specific overrides
    //
    // A single partition avoids the repartitioning overhead, which dominates for the selective
    // queries that are typical for SPARQL workloads.
    if env::var("DATAFUSION_EXECUTION_TARGET_PARTITIONS").is_err() {
        config = config.with_target_partitions(1);
    }

    //
    // Executing small scans during planning overlaps their latency, which only pays off for
    // remote stores. Larger thresholds also execute broad patterns (e.g., all labels) that dynamic
    // filters would prune during execution.
    if env::var("RDF_FUSION_EXECUTION_SMALL_SCAN_THRESHOLD").is_err()
        && is_remote_location(location)
    {
        config = config.set_str("rdf_fusion.execution.small_scan_threshold", "256K")
    }

    // Switching quad tables plans an alternative scan at execution time. For local stores, this
    // costs more than reading the planned quad table.
    if env::var("RDF_FUSION_EXECUTION_QUAD_TABLE_SWITCHING").is_err()
        && !is_remote_location(location)
    {
        config = config.set_bool("rdf_fusion.execution.quad_table_switching", false)
    }

    // Bloom filters of Parquet storages are only cached in the data cache.
    if env::var("RDF_FUSION_STORAGE_PARQUET_DATA_CACHE_ENABLED").is_err() {
        config = config.set_str("rdf_fusion.storage.parquet.data_cache_enabled", "true")
    }

    Ok(config)
}

/// Helper to create a `DeltaQuadsStorage` instance.
async fn create_delta_storage(
    location: &Url,
    encoding: QuadStorageEncodingName,
    runtime_env: &Arc<RuntimeEnv>,
    session_config: &SessionConfig,
    rdf_fusion_options: &RdfFusionOptions,
) -> anyhow::Result<Arc<dyn QuadStorage>> {
    let object_store_url = location.as_object_store_url();

    let object_store = runtime_env
        .object_store(&object_store_url)
        .context("Failed to get object store")?;
    let log_store = logstore_with(
        Arc::clone(&object_store),
        location,
        StorageConfig::default().with_io_runtime(IORuntime::RT(Handle::current())),
    )
    .context("Failed to create log store")?;

    let loading_state = SessionStateBuilder::new()
        .with_runtime_env(Arc::clone(runtime_env))
        .with_config(session_config.clone())
        .build();

    Ok(Arc::new(
        DeltaQuadsStorageBuilder::new()
            .with_log_store(log_store)
            .with_load_mode(LoadMode::Load(Box::new(loading_state)))
            .with_encoding(encoding)
            .with_log_max_age(rdf_fusion_options.storage.delta.log_max_age)
            .build()
            .await?,
    ))
}

/// HACK: Creates a dummy RdfFusionContext for Parquet loading because we cannot create an empty
/// Parquet store, so we set up a dummy DeltaQuads store inside an in-memory object store.
async fn create_context_for_parquet_load(
    args: &Args,
) -> anyhow::Result<RdfFusionContext> {
    let runtime_env = build_runtime_env(args)?;

    let mut session_config = session_config_from_env_for_rdf_fusion()?;
    let rdf_fusion_options = RdfFusionOptions::from_env()?;
    session_config
        .options_mut()
        .extensions
        .insert(rdf_fusion_options.clone());
    let encoding = QuadStorageEncodingName::from_str(&args.storage.encoding)?;

    if matches!(encoding, QuadStorageEncodingName::ObjectId) {
        bail!("ObjectId encoding is not supported for Parquet storage");
    }

    // Register a dummy in-memory location for Delta lake storage setup
    let dummy_location = Url::parse("memory:///temp/").unwrap();
    runtime_env.register_object_store(&dummy_location, Arc::new(InMemory::new()));

    let storage = create_delta_storage(
        &dummy_location,
        encoding,
        &runtime_env,
        &session_config,
        &rdf_fusion_options,
    )
    .await?;

    let context = RdfFusionContextBuilder::new(storage)
        .with_runtime_env(Some(runtime_env))
        .build()
        .context("Failed to create RDF Fusion Context")?;

    Ok(context)
}

/// Returns whether the store at `location` is accessed over the network (e.g., `s3a://`).
fn is_remote_location(location: &Url) -> bool {
    !matches!(location.scheme(), "file" | "memory")
}

/// Resolves a location string to a uniform absolute URL string.
///
/// If the location is a URL (other than `file://`), it is returned as is.
/// If the location is a local file path or a `file://` URL, it is resolved to an absolute `file://` URL.
fn resolve_location(location: &str) -> anyhow::Result<String> {
    let stripped = location.strip_prefix("file://").unwrap_or(location);
    if location.contains("://") && !location.starts_with("file://") {
        Ok(location.to_owned())
    } else {
        let path = PathBuf::from(stripped);
        let absolute_path = if path.is_absolute() {
            path
        } else {
            env::current_dir()?.join(path)
        };
        let mut url = Url::from_file_path(absolute_path)
            .map_err(|_| anyhow::anyhow!("Failed to convert path to URL: {location}"))?
            .to_string();
        // A trailing `/` denotes a directory, which `Url::from_file_path` drops.
        if location.ends_with('/') && !url.ends_with('/') {
            url.push('/');
        }
        Ok(url)
    }
}

/// The fraction of the system memory that is used as the default memory limit.
///
/// The memory pool only tracks large allocations of operators (e.g., hash tables, sort buffers).
/// The remaining memory is left for untracked allocations (e.g., caches, record batches in flight)
/// and other processes.
const DEFAULT_MEMORY_LIMIT_FRACTION: f64 = 0.7;

/// Returns the default memory limit, a fraction of the total memory of the system.
///
/// If the process runs in a cgroup with a memory limit (e.g., a container), the limit of the
/// cgroup is used instead of the system's memory. Returns [`None`] if the memory cannot be
/// determined.
fn default_memory_limit() -> Option<usize> {
    let mut system = sysinfo::System::new();
    system.refresh_memory();

    let system_memory = system.total_memory();
    let total_memory = system
        .cgroup_limits()
        .map(|limits| limits.total_memory)
        .filter(|&memory| memory > 0)
        .map_or(system_memory, |memory| memory.min(system_memory));
    if total_memory == 0 {
        warn!("Could not determine the system memory. Running without a memory limit.");
        return None;
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let limit = (total_memory as f64 * DEFAULT_MEMORY_LIMIT_FRACTION) as usize;
    Some(limit)
}

/// Builds the runtime environment from the given arguments.
fn build_runtime_env(args: &Args) -> anyhow::Result<Arc<RuntimeEnv>> {
    let cache_config = CacheManagerConfig::default();
    let mut builder = RuntimeEnvBuilder::new()
        .with_disk_manager_builder(DiskManagerBuilder::default())
        .with_cache_manager(cache_config);

    let memory_limit = match env::var("DATAFUSION_RUNTIME_MEMORY_LIMIT") {
        Ok(limit_str) => Some(SessionContext::parse_capacity_limit(
            "DATAFUSION_RUNTIME_MEMORY_LIMIT",
            &limit_str,
        )?),
        Err(_) => default_memory_limit(),
    };
    if let Some(memory_limit) = memory_limit {
        builder = builder.with_memory_pool(Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(memory_limit),
            NonZeroUsize::new(5).unwrap(),
        )));
    }
    let registry = Arc::clone(&builder.object_store_registry);

    // Register s3-compatible object store if its in the arguments
    let mut locations = Vec::new();
    locations.push(resolve_location(&args.storage.location)?);

    if let Command::Dump { output, .. } = &args.command {
        locations.push(resolve_location(output)?);
    }
    if let Command::Load { inputs } = &args.command {
        for input in inputs {
            locations.push(resolve_location(input)?);
        }
    }

    for location in &locations {
        let url = Url::parse(location)
            .context("Failed to parse the S3 URL from the location argument")?;
        let schema = url.scheme();

        match schema {
            "file" => {}
            "http" | "https" => {
                let base_url = Url::parse(&url[..url::Position::AfterPort])?;
                let store = HttpBuilder::new().with_url(base_url.as_str()).build()?;
                registry.register_store(&base_url, Arc::new(store));
            }
            "s3a" => {
                register_s3_store(&registry, &url)?;
            }
            "memory" => {
                registry.register_store(&url, Arc::new(InMemory::new()));
            }
            _ => {
                warn!(
                    "Unknown location type: {}. Check usage information for supported storage locations",
                    location
                )
            }
        }
    }

    builder = builder.with_object_store_registry(registry);

    builder.build_arc().context("Failed to build RuntimeEnv")
}

fn register_s3_store(
    registry: &Arc<dyn ObjectStoreRegistry>,
    url: &Url,
) -> anyhow::Result<()> {
    let s3_domain = url
        .domain()
        .context("The S3 URL does not contain a domain")?;

    let (s3_bucket, s3_endpoint) = if let Some(index) = s3_domain.find('.') {
        (&s3_domain[..index], Some(&s3_domain[index + 1..]))
    } else {
        (s3_domain, None)
    };

    if env::var("AWS_ACCESS_KEY_ID").ok().is_none() {
        warn!("AWS_ACCESS_KEY_ID not set, using default credentials")
    }
    if env::var("AWS_SECRET_ACCESS_KEY").ok().is_none() {
        warn!("AWS_SECRET_ACCESS_KEY not set, using default credentials")
    }

    let mut s3_builder = AmazonS3Builder::from_env().with_bucket_name(s3_bucket);

    if s3_bucket.ends_with("--x-s3") || s3_bucket.ends_with("--xa-s3") {
        s3_builder = s3_builder.with_s3_express(true);
    }

    if let Some(endpoint) = s3_endpoint {
        s3_builder = s3_builder.with_endpoint(format!("https://{endpoint}"));
    }

    if let Ok(s3) = s3_builder.build() {
        registry.register_store(url, Arc::new(s3));
    } else {
        warn!(
            "Building the S3-compatible object store failed.
            Check if the endpoint and bucket name in location argument are valid.
            Check if the S3 credential environment variables are set correctly."
        )
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_location_keeps_trailing_slash_of_directories() {
        assert!(
            resolve_location("/tmp/dataset/")
                .unwrap()
                .ends_with("/tmp/dataset/")
        );
        assert!(
            resolve_location("/tmp/dataset.parquet")
                .unwrap()
                .ends_with("/tmp/dataset.parquet")
        );
        assert_eq!(
            resolve_location("s3a://bucket/dataset/").unwrap(),
            "s3a://bucket/dataset/"
        );
    }

    #[test]
    fn only_object_stores_accessed_over_the_network_are_remote() {
        let is_remote = |location: &str| {
            is_remote_location(&Url::parse(&resolve_location(location).unwrap()).unwrap())
        };
        assert!(!is_remote("/tmp/dataset/"));
        assert!(!is_remote("file:///tmp/dataset/"));
        assert!(!is_remote("memory:///"));
        assert!(is_remote("s3a://bucket.endpoint/dataset/"));
    }
}
