#![cfg(test)]

use anyhow::Result;
use rdf_fusion_testsuite::w3c::W3CSparqlTestSuiteBuilder;

#[tokio::test]
async fn sparql10_w3c_query_syntax_testsuite() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-syntax.ttl",
    )?
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

#[tokio::test]
async fn sparql10_w3c_query_evaluation_testsuite() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )?
    .ignore_tests([
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
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/reduced/manifest#reduced-2"
    ])
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test]
async fn sparql11_query_w3c_evaluation_testsuite() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )?
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test]
async fn sparql11_json_w3c_evaluation_testsuite() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/json-res/manifest.ttl",
    )?
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test]
async fn sparql11_tsv_w3c_evaluation_testsuite() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/csv-tsv-res/manifest.ttl",
    )?
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
