#![cfg(test)]

use anyhow::Result;
use rdf_fusion_testsuite::w3c::W3CSparqlTestSuiteBuilder;

#[tokio::test]
async fn rdf_fusion_sparql_testsuite() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/testsuite/rdf-fusion-tests/sparql/manifest.ttl",
    )?
        .build()
        .await?
        .run()
        .await
        .assert_success();

    Ok(())
}
