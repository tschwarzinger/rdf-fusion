#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::use_debug)]
use crate::cli::{Args, Command};
use anyhow::{Context, bail};
use clap::Parser;
use datafusion::common::runtime::SpawnedTask;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::object_store::memory::InMemory;
use datafusion::prelude::SessionConfig;
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use deltalake::logstore::{IORuntime, StorageConfig, logstore_with};
use object_store::ClientOptions;
use object_store::aws::AmazonS3Builder;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::execution::cache::CachingObjectStoreRegistry;
use rdf_fusion::execution::ingest::RdfParserOptions;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::model::config::RdfFusionOptions;
use rdf_fusion::storage::delta::{DeltaQuadStorageBuilder, LoadMode};
use rdf_fusion::store::Store;
use rdf_fusion_web::ServerConfig;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tracing::info;
use tracing::warn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use url::Url;

mod cli;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();
    let store = create_store(&args).await?;

    match args.command {
        Command::Serve {
            bind,
            cors,
            union_default_graph,
        } => {
            SpawnedTask::spawn(async move {
                serve(store, &bind, false, cors, union_default_graph).await
            })
            .await
            .context("Failed to join Web Server")?
            .context("Error from Web Server")?;

            Ok(())
        }
        Command::BuildDatabase { inputs } => {
            if inputs.is_empty() {
                bail!("No input file given");
            }

            for input in &inputs {
                info!("Loading file {} ...", input.display());

                let file = tokio::fs::File::open(input)
                    .await
                    .context("Cannot open file.")?;
                store
                    .load_from_reader(
                        file,
                        RdfParserOptions::with_format(RdfFormat::NTriples),
                    )
                    .await
                    .context("Error while loading data file")?;
            }

            info!("All files loaded.");

            info!("Optimizing store ...");
            store.optimize().await?;

            info!("Database built.");
            Ok(())
        }
    }
}

/// Creates a [`Store`] instance from the given arguments.
async fn create_store(args: &Args) -> anyhow::Result<Store> {
    let runtime_env = build_runtime_env(args)?;
    let location = args.runtime.location.as_deref().unwrap_or("memory://");
    let url = Url::parse(location).context("Invalid object store URL")?;
    let object_store_url = url.as_object_store_url();

    let object_store = runtime_env
        .object_store(&object_store_url)
        .expect("Failed to get object store");
    let log_store = logstore_with(
        Arc::clone(&object_store),
        &url,
        StorageConfig::default().with_io_runtime(IORuntime::RT(Handle::current())),
    )
    .expect("Failed to create log store");

    let mut session_config = SessionConfig::from_env()?;
    let rdf_fusion_options = RdfFusionOptions::from_env()?;
    session_config
        .options_mut()
        .extensions
        .insert(rdf_fusion_options.clone());

    let loading_state = SessionStateBuilder::new()
        .with_runtime_env(Arc::clone(&runtime_env))
        .with_config(session_config.clone())
        .build();

    let storage = DeltaQuadStorageBuilder::new()
        .with_log_store(log_store)
        .with_load_mode(LoadMode::Load(Box::new(loading_state)))
        .with_log_max_age(rdf_fusion_options.storage.delta.log_max_age)
        .build()
        .await?;

    let context = RdfFusionContextBuilder::new(Arc::new(storage))
        .with_session_config(Some(session_config))
        .with_runtime_env(Some(runtime_env))
        .build()
        .context("Failed to create RDF Fusion Context")?;
    let store = Store::new(context);
    Ok(store)
}

/// Builds the runtime environment from the given arguments.
fn build_runtime_env(args: &Args) -> anyhow::Result<Arc<RuntimeEnv>> {
    let cache_config = CacheManagerConfig::default();
    let mut builder = RuntimeEnvBuilder::new().with_cache_manager(cache_config);
    if let Some(memory_limit) = args.runtime.memory_limit {
        builder = builder.with_memory_limit(memory_limit * 1024 * 1024, 1.0);
    }

    let registry = Arc::new(CachingObjectStoreRegistry::new(
        Arc::clone(&builder.object_store_registry),
        1024 * 1024 * 1024,
    ));

    // Register s3-compatible object store if its in the arguments
    match args.runtime.location {
        Some(ref location) if location.starts_with("s3a://") => {
            register_s3_store(&registry, location)?;
        }
        Some(ref location) if location.starts_with("file://") => {
            // Store is already registered by `create_store`
        }
        Some(_) => {
            warn!(
                "Unknown location type. Check usage information for supported storage locations"
            )
        }
        // If location is none use in-memory database
        None => {
            registry.register_store(
                &Url::parse("memory:///").unwrap(),
                Arc::new(InMemory::new()),
            );
        }
    }

    builder = builder.with_object_store_registry(registry);

    builder.build_arc().context("Failed to build RuntimeEnv")
}

fn register_s3_store(
    registry: &Arc<CachingObjectStoreRegistry>,
    location: &str,
) -> anyhow::Result<()> {
    let s3_url = Url::parse(location)
        .context("Failed to parse the S3 URL from the location argument")?;
    let s3_domain = s3_url
        .domain()
        .context("The S3 URL does not contain a domain")?;

    // Extract the bucket name from the s3_domain
    // [bucket].[endpoint]
    let s3_bucket_index = s3_domain
        .find(".")
        .context("The S3 doamin does not contain a bucket name")?;
    let s3_bucket = &s3_domain[..s3_bucket_index];
    let s3_endpoint = &s3_domain[s3_bucket_index + 1..];

    let client_options = ClientOptions::new()
        .with_timeout(Duration::from_secs(15 * 60))
        .with_connect_timeout(Duration::from_secs(60))
        .with_pool_idle_timeout(Duration::from_secs(90));

    if env::var("AWS_ACCESS_KEY_ID").ok().is_none() {
        warn!("AWS_ACCESS_KEY_ID not set, using default credentials")
    }
    if env::var("AWS_SECRET_ACCESS_KEY").ok().is_none() {
        warn!("AWS_SECRET_ACCESS_KEY not set, using default credentials")
    }

    let s3_builder = AmazonS3Builder::from_env()
        .with_bucket_name(s3_bucket)
        .with_endpoint(format!("https://{s3_endpoint}"))
        .with_client_options(client_options)
        .build();
    if let Ok(s3) = s3_builder {
        registry.register_store(&s3_url, Arc::new(s3));
    } else {
        warn!(
            "Building the S3-compatible object store failed.
            Check if the endpoint and bucket name in location argument are valid.
            Check if the S3 credential environment variables are set correctly."
        )
    }

    Ok(())
}

/// Starts a Web Server that serves RDF Fusion's Web API.
async fn serve(
    store: Store,
    bind: &str,
    read_only: bool,
    cors: bool,
    union_default_graph: bool,
) -> anyhow::Result<()> {
    let server_config = ServerConfig {
        store,
        bind: bind.to_owned(),
        read_only,
        cors,
        union_default_graph,
    };
    rdf_fusion_web::serve(server_config).await
}
