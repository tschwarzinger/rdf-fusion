#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::use_debug)]
use crate::cli::{Args, Command};
use anyhow::{Context, bail};
use clap::Parser;
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
use rdf_fusion::common::config::RdfFusionOptions;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::storage::delta::{DeltaQuadStorageBuilder, LoadMode};
use rdf_fusion::storage::parquet::ParquetQuadStorage;
use rdf_fusion::store::Store;
use rdf_fusion_extensions::storage::QuadStorage;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tracing::warn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use url::Url;

mod cli;
mod commands;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
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

            let store = create_store(&args).await?;

            commands::load::load(
                store,
                &inputs,
                location_url,
                args.storage.storage_type.clone(),
            )
            .await
        }
        Command::Query {
            query,
            explain,
            analyze,
        } => {
            let store = create_store(&args).await?;
            commands::query::query(store, query.clone(), *explain, *analyze).await
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
    let runtime_env = build_runtime_env(args)?;

    let mut session_config = SessionConfig::from_env()?;
    let rdf_fusion_options = RdfFusionOptions::from_env()?;
    session_config
        .options_mut()
        .extensions
        .insert(rdf_fusion_options.clone());
    let encoding = QuadStorageEncodingName::from_str(&args.storage.encoding)?;
    let location = Url::parse(&resolve_location(&args.storage.location)?)
        .context("Invalid object store URL")?;

    let storage: Arc<dyn QuadStorage> = match args.storage.storage_type {
        cli::QuadStorageType::DeltaLake => {
            let object_store_url = location.as_object_store_url();

            let object_store = runtime_env
                .object_store(&object_store_url)
                .expect("Failed to get object store");
            let log_store = logstore_with(
                Arc::clone(&object_store),
                &location,
                StorageConfig::default()
                    .with_io_runtime(IORuntime::RT(Handle::current())),
            )
            .expect("Failed to create log store");

            let loading_state = SessionStateBuilder::new()
                .with_runtime_env(Arc::clone(&runtime_env))
                .with_config(session_config.clone())
                .build();

            Arc::new(
                DeltaQuadStorageBuilder::new()
                    .with_log_store(log_store)
                    .with_load_mode(LoadMode::Load(Box::new(loading_state)))
                    .with_encoding(encoding)
                    .with_log_max_age(rdf_fusion_options.storage.delta.log_max_age)
                    .build()
                    .await?,
            )
        }
        cli::QuadStorageType::Parquet => {
            if matches!(encoding, QuadStorageEncodingName::ObjectId) {
                bail!("ObjectId encoding is not supported for Parquet storage");
            }

            Arc::new(
                ParquetQuadStorage::try_load(
                    location,
                    encoding,
                    runtime_env.object_store_registry.as_ref(),
                )
                .await
                .context("Failed to create Parquet storage")?,
            )
        }
    };

    let context = RdfFusionContextBuilder::new(storage)
        .with_runtime_env(Some(runtime_env))
        .build()
        .context("Failed to create RDF Fusion Context")?;
    Ok(Store::new(context))
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
        Ok(Url::from_file_path(absolute_path)
            .map_err(|_| anyhow::anyhow!("Failed to convert path to URL: {location}"))?
            .to_string())
    }
}

/// Builds the runtime environment from the given arguments.
fn build_runtime_env(args: &Args) -> anyhow::Result<Arc<RuntimeEnv>> {
    let cache_config = CacheManagerConfig::default();
    let mut builder = RuntimeEnvBuilder::new().with_cache_manager(cache_config);
    if let Some(memory_limit) = args.runtime.memory_limit {
        builder = builder.with_memory_limit(memory_limit * 1024 * 1024, 1.0);
    }

    // let registry = Arc::new(CachingObjectStoreRegistry::new(
    //     Arc::clone(&builder.object_store_registry),
    //     1024 * 1024 * 1024,
    // ));
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
        if location.starts_with("s3a://") {
            register_s3_store(&registry, location)?;
        } else if location.starts_with("file://") {
            // Store is already registered by `create_store` or it's a local file
        } else {
            warn!(
                "Unknown location type: {}. Check usage information for supported storage locations",
                location
            )
        }
    }

    // If no locations are provided, register memory store.
    // Note: This matches the old behavior but might be redundant if datafusion already does it.
    if locations.is_empty() {
        registry.register_store(
            &Url::parse("memory:///").unwrap(),
            Arc::new(InMemory::new()),
        );
    }

    builder = builder.with_object_store_registry(registry);

    builder.build_arc().context("Failed to build RuntimeEnv")
}

fn register_s3_store(
    registry: &Arc<dyn ObjectStoreRegistry>,
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
