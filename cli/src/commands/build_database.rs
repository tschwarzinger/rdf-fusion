use anyhow::{Context, bail};
use rdf_fusion::io::RdfFormat;
use rdf_fusion::storage::rdf_files::RdfParserOptions;
use rdf_fusion::store::Store;
use std::path::PathBuf;
use tracing::info;

/// Builds a database by loading all inputs into the store.
pub async fn build_database(store: Store, inputs: Vec<PathBuf>) -> anyhow::Result<()> {
    if inputs.is_empty() {
        bail!("No input file given");
    }

    for input in &inputs {
        info!("Loading file {} ...", input.display());

        let format = input
            .extension()
            .and_then(|e| e.to_str())
            .and_then(RdfFormat::from_extension)
            .context("Cannot determine RDF format from file extension. Please specify the format explicitly using the --format option.")?;

        let file = tokio::fs::File::open(input)
            .await
            .context("Cannot open file.")?;
        store
            .load_from_reader(file, RdfParserOptions::with_format(format))
            .await
            .context("Error while loading data file")?;
    }

    info!("All files loaded.");

    info!("Optimizing store ...");
    store.optimize().await?;

    info!("Database built.");
    Ok(())
}
