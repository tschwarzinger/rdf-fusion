use rdf_fusion::store::Store;
use tracing::info;

/// Optimizes a store.
pub async fn optimize(store: Store) -> anyhow::Result<()> {
    info!("Optimizing database ....");
    store.optimize().await?;
    info!("Database optimized.");
    Ok(())
}
