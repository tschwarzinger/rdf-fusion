use anyhow::Context;
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::execution::sparql::RdfFusionQuery;
use rdf_fusion::store::Store;

/// Executes a SPARQL query against the given store and prints the result to stdout.
pub async fn query(store: Store, query_str: String) -> anyhow::Result<()> {
    let parsed_query = RdfFusionQuery::parse(&query_str, None)
        .context("Failed to parse SPARQL query")?;

    let results = store.query(parsed_query).await?;
    results
        .write(std::io::stdout(), QueryResultsFormat::Tsv)
        .await?;

    Ok(())
}
