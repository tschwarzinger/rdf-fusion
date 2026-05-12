use anyhow::Context;
use datafusion::common::runtime::SpawnedTask;
use rdf_fusion::store::Store;
use rdf_fusion_web::ServerConfig;

/// Starts a Web Server that serves RDF Fusion's Web API.
pub async fn serve(
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
    SpawnedTask::spawn(async move { rdf_fusion_web::serve(server_config).await })
        .await
        .context("Failed to join Web Server")?
        .context("Error from Web Server")?;

    Ok(())
}
