use crate::cli::QuadStorageType;
use anyhow::{Context, bail};
use object_store::ObjectStoreExt;
use object_store::path::Path;
use rdf_fusion::common::{GraphName, RdfInput};
use rdf_fusion::execution::load::RdfParquetLoader;
use rdf_fusion::storage::rdf_files::RdfFileScanOptions;
use rdf_fusion::store::Store;
use tokio_util::io::StreamReader;
use tracing::info;
use url::Url;

/// Loads an RDF file into the database.
pub async fn load(
    store: Store,
    inputs: &[Url],
    output: Url,
    storage_type: QuadStorageType,
) -> anyhow::Result<()> {
    if inputs.is_empty() {
        bail!("No input files provided");
    }

    info!("Loading {} input(s) into {} ...", inputs.len(), output);

    match storage_type {
        QuadStorageType::Parquet => {
            let inputs = inputs
                .iter()
                .map(|u| RdfInput::try_new(u.clone(), GraphName::DefaultGraph))
                .collect::<Result<Vec<_>, _>>()
                .context("Error while processing input URLs")?;

            let encoding = store.context().storage().encoding().name();
            let loader = RdfParquetLoader::try_new(store.context().clone(), encoding)?;
            loader
                .load_many(inputs, output)
                .await
                .context("Failed to load RDF file into Parquet")?;
        }
        _ => {
            let context = store.context();
            let runtime_env = context.session_context().runtime_env();

            for input in inputs {
                let object_store = runtime_env
                    .object_store_registry
                    .get_store(input)
                    .context("Failed to get object store implementation for input URL")?;
                let path = Path::from_url_path(input.path())?;
                let result = object_store.get(&path).await?;

                let input = RdfInput::try_new(input.clone(), GraphName::DefaultGraph)?;
                store
                    .load_from_reader(
                        StreamReader::new(result.into_stream()),
                        RdfFileScanOptions::with_format(input.format),
                    )
                    .await
                    .context("Failed to load RDF file into Delta Lake")?;
            }
        }
    }

    info!("Data loaded successfully.");
    Ok(())
}
