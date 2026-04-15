#![cfg(test)]

use anyhow::Result;
use rdf_fusion_testsuite::w3c::W3CSparqlTestSuiteBuilder;

#[tokio::test]
#[ignore = "Not yet supported"]
async fn w3c_sparql11_update_testsuite() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
    )
    .await?
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}
