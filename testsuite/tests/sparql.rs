#![cfg(test)]

use anyhow::Result;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::storage::delta::DeltaQuadStorageBuilder;
use rdf_fusion::store::Store;
use rdf_fusion_testsuite::w3c::{StoreFactory, W3CSparqlTestSuiteBuilder};
use std::sync::Arc;

#[tokio::test]
async fn sparql10_w3c_query_syntax_testsuite() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-syntax.ttl",
    )
    .await?
    .ignore_test(
        // Tokenizer
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql3/manifest#syn-bad-26",
    )
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

/// Contains tests that are not yet supported by RDF Fusion
const UNSUPPORTED_SPARQL10_TESTS: [&str; 9] = [
    // Testing equality of illegal literals ("xyz"^^<http://www.w3.org/2001/XMLSchema#integer>)
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-07",
    //Simple literal vs xsd:string. We apply RDF 1.1
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-08",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-10",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-11",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12",
    // We use XSD 1.1 equality on dates
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#date-2",
    // We choose to simplify first the nested group patterns in OPTIONAL
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-005-not-simplified",
    // This test relies on naive iteration on the input file
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/reduced/manifest#reduced-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/reduced/manifest#reduced-2",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_plain_term() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_store_factory(plain_term_store_factory())
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_object_id() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_plain_term_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_store_factory(plain_term_store_factory())
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_object_id_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_string() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_store_factory(string_store_factory())
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_string_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_store_factory(string_store_factory())
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_plain_term() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .with_store_factory(plain_term_store_factory())
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_plain_term_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .with_store_factory(plain_term_store_factory())
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_object_id() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_object_id_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_string() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .with_store_factory(string_store_factory())
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_string_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .with_store_factory(string_store_factory())
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_json_evaluation() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/json-res/manifest.ttl",
    )
    .await?
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_tsv_evaluation() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/csv-tsv-res/manifest.ttl",
    )
    .await?
    .ignore_tests([
        // We do not run CSVResultFormatTest tests yet
        "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#csv01",
        "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#csv02",
        "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#csv03",
    ])
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

/// Creates the [`Store`] using the plain term encoding that is used for the plain term encoding
/// tests.
fn plain_term_store_factory() -> StoreFactory {
    Arc::new(|runtime_env| {
        Box::pin(async move {
            let delta_storage = DeltaQuadStorageBuilder::new()
                .with_encoding(QuadStorageEncodingName::PlainTerm)
                .build()
                .await
                .unwrap();

            let context = RdfFusionContextBuilder::new(Arc::new(delta_storage))
                .with_runtime_env(Some(runtime_env))
                .with_single_partition_session_config()
                .build()
                .unwrap();
            Store::new(context)
        })
    })
}

/// Creates the [`Store`] using the plain term encoding that is used for the plain term encoding
/// tests.
fn string_store_factory() -> StoreFactory {
    Arc::new(|runtime_env| {
        Box::pin(async move {
            let delta_storage = DeltaQuadStorageBuilder::new()
                .with_encoding(QuadStorageEncodingName::String)
                .build()
                .await
                .unwrap();

            let context = RdfFusionContextBuilder::new(Arc::new(delta_storage))
                .with_runtime_env(Some(runtime_env))
                .with_single_partition_session_config()
                .build()
                .unwrap();
            Store::new(context)
        })
    })
}
