#![cfg(test)]

use anyhow::Result;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion_testsuite::store_factories::{
    delta_quads_store_factory, memory_store_factory,
};
use rdf_fusion_testsuite::w3c::W3CSparqlTestSuiteBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_update_testsuite_plain_term() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
    )
    .await?
    .with_store_factory(delta_quads_store_factory(
        QuadStorageEncodingName::PlainTerm,
    ))
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_update_testsuite_object_id() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
    )
    .await?
    .with_store_factory(delta_quads_store_factory(QuadStorageEncodingName::ObjectId))
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_update_testsuite_memory() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
    )
    .await?
    .with_store_factory(memory_store_factory())
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_update_testsuite_string() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
    )
    .await?
    .with_store_factory(delta_quads_store_factory(QuadStorageEncodingName::String))
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}
