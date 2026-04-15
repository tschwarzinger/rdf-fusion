#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::use_debug)]
use crate::cli::{Args, Command};
use anyhow::Context;
use clap::Parser;
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use rdf_fusion::encoding::typed_family::TypedFamilyEncoding;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::storage::delta::DeltaQuadStorageBuilder;
use rdf_fusion::store::Store;
use rdf_fusion_web::ServerConfig;
use std::str;
use std::sync::Arc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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
    match args.command {
        Command::Serve {
            bind,
            cors,
            union_default_graph,
        } => {
            let cache_config = CacheManagerConfig::default();
            let mut builder = RuntimeEnvBuilder::new().with_cache_manager(cache_config);
            if let Some(memory_limit) = args.runtime.memory_limit {
                builder = builder.with_memory_limit(memory_limit * 1024 * 1024, 1.0);
            }
            let runtime_env = builder.build_arc().expect("Failed to build RuntimeEnv");

            let location = args.runtime.location.as_deref().unwrap_or("memory://");
            let storage =
                DeltaQuadStorageBuilder::new(Arc::new(TypedFamilyEncoding::default()))
                    .with_location(location)
                    .build()
                    .await?;

            let context = RdfFusionContextBuilder::new(storage)
                .with_session_config(Some(SessionConfig::from_env()?))
                .with_runtime_env(Some(runtime_env))
                .build()
                .context("Failed to create RDF Fusion Context")?;
            let store = Store::new(context);

            SpawnedTask::spawn(async move {
                serve(store, &bind, false, cors, union_default_graph).await
            })
            .await
            .context("Failed to join Web Server")?
            .context("Error from Web Server")?;

            Ok(())
        }
    }
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
