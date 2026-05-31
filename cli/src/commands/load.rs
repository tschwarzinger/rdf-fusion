use anyhow::{Context, bail};
use datafusion::prelude::SessionContext;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use rdf_fusion::common::{GraphName, RdfInput, RdfSortOrder};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::storage::parquet::RdfParquetLoader;
use rdf_fusion::storage::rdf_files::RdfFileScanOptions;
use rdf_fusion::store::Store;
use rdf_fusion_extensions::RdfFusionContextView;
use tokio_util::io::StreamReader;
use tracing::info;
use url::Url;

pub enum LoadCommandType {
    Parquet {
        session_context: SessionContext,
        context_view: RdfFusionContextView,
        encoding: QuadStorageEncodingName,
        sort_order: Option<RdfSortOrder>,
    },
    Other(Store),
}

/// Loads an RDF file into the database.
pub async fn load(
    command: LoadCommandType,
    inputs: &[Url],
    output: Url,
) -> anyhow::Result<()> {
    if inputs.is_empty() {
        bail!("No input files provided");
    }

    info!("Loading {} input(s) into {} ...", inputs.len(), output);

    match command {
        LoadCommandType::Parquet {
            session_context,
            context_view,
            encoding,
            sort_order,
        } => {
            let inputs = inputs
                .iter()
                .map(|u| RdfInput::try_new(u.clone(), GraphName::DefaultGraph))
                .collect::<Result<Vec<_>, _>>()
                .context("Error while processing input URLs")?;

            let loader = RdfParquetLoader::try_new(
                session_context,
                context_view,
                encoding,
                sort_order,
            )?;
            loader
                .load_many(inputs, output)
                .await
                .context("Failed to load RDF file into Parquet")?;
        }
        LoadCommandType::Other(store) => {
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
