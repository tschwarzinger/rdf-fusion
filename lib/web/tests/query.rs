//! Tests the SPARQL query endpoint in-process.
//!
//! These tests snapshot full responses (status, content type, and body). A regression that, for
//! example, ends the result stream early would still return `200 OK` and therefore slip through
//! status-only checks.

use axum_test::{TestResponse, TestServer};
use rdf_fusion::common::RdfFormat;
use rdf_fusion::storage::rdf_files::RdfFileScanOptions;
use rdf_fusion::store::Store;
use rdf_fusion_web::{AppState, create_router};
use std::sync::Arc;

const QUERY_PATH: &str = "/repositories/default/query";

const SELECT_NAMES: &str = "SELECT ?name WHERE { \
    <http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> ?name \
} ORDER BY ?name";

async fn spiderman_server() -> TestServer {
    let store = Store::new_in_memory().await;
    let file = tokio::fs::File::open("../../examples/data/spiderman.ttl")
        .await
        .expect("spiderman.ttl should exist");
    store
        .load_from_reader(file, RdfFileScanOptions::with_format(RdfFormat::Turtle))
        .await
        .expect("spiderman.ttl should load");

    TestServer::new(create_router(AppState {
        store: Arc::new(store),
        read_only: false,
        union_default_graph: false,
    }))
}

/// Renders the status, the content type, and the body of `response` for snapshotting.
fn render(response: &TestResponse) -> String {
    let content_type = response
        .maybe_header("content-type")
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    format!(
        "{}\n{content_type}\n\n{}",
        response.status_code(),
        response.text().replace("\r\n", "\n")
    )
}

#[tokio::test]
async fn test_select_csv() {
    let server = spiderman_server().await;

    let response = server
        .get(QUERY_PATH)
        .add_query_param("query", SELECT_NAMES)
        .add_header("accept", "text/csv")
        .await;

    insta::assert_snapshot!(render(&response), @"
    200 OK
    text/csv; charset=utf-8

    name
    Spiderman
    Человек-паук
    ");
}

#[tokio::test]
async fn test_select_tsv() {
    let server = spiderman_server().await;

    let response = server
        .get(QUERY_PATH)
        .add_query_param("query", SELECT_NAMES)
        .add_header("accept", "text/tab-separated-values")
        .await;

    insta::assert_snapshot!(render(&response), @r#"
    200 OK
    text/tab-separated-values; charset=utf-8

    ?name
    "Spiderman"
    "Человек-паук"@ru
    "#);
}

#[tokio::test]
async fn test_select_json_via_post() {
    let server = spiderman_server().await;

    let response = server
        .post(QUERY_PATH)
        .bytes(SELECT_NAMES.into())
        .content_type("application/sparql-query")
        .add_header("accept", "application/sparql-results+json")
        .await;

    insta::assert_snapshot!(render(&response), @r#"
    200 OK
    application/sparql-results+json

    {"head":{"vars":["name"]},"results":{"bindings":[{"name":{"type":"literal","value":"Spiderman"}},{"name":{"type":"literal","value":"Человек-паук","xml:lang":"ru"}}]}}
    "#);
}

#[tokio::test]
async fn test_ask() {
    let server = spiderman_server().await;

    let response = server
        .get(QUERY_PATH)
        .add_query_param(
            "query",
            "ASK { <http://example.org/#spiderman> a <http://xmlns.com/foaf/0.1/Person> }",
        )
        .add_header("accept", "application/sparql-results+json")
        .await;

    insta::assert_snapshot!(render(&response), @r#"
    200 OK
    application/sparql-results+json

    {"head":{},"boolean":true}
    "#);
}

#[tokio::test]
async fn test_construct() {
    let server = spiderman_server().await;

    let response = server
        .get(QUERY_PATH)
        .add_query_param("query", "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
        .add_header("accept", "application/n-triples")
        .await;

    // The order of the triples is not defined.
    let mut triples = response
        .text()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    triples.sort();
    insta::assert_snapshot!(
        format!("{}\n\n{}", response.status_code(), triples.join("\n")),
        @r#"
    200 OK

    <http://example.org/#green-goblin> <http://www.perceive.net/schemas/relationship/enemyOf> <http://example.org/#spiderman> .
    <http://example.org/#green-goblin> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
    <http://example.org/#green-goblin> <http://xmlns.com/foaf/0.1/name> "Green Goblin" .
    <http://example.org/#spiderman> <http://www.perceive.net/schemas/relationship/enemyOf> <http://example.org/#green-goblin> .
    <http://example.org/#spiderman> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
    <http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> "Spiderman" .
    <http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> "Человек-паук"@ru .
    "#
    );
}

#[tokio::test]
async fn test_syntax_error_is_bad_request() {
    let server = spiderman_server().await;

    let response = server
        .get(QUERY_PATH)
        .add_query_param("query", "SELECT WHERE {")
        .expect_failure()
        .await;

    insta::assert_snapshot!(render(&response), @"
    400 Bad Request
    text/plain; charset=utf-8

    error at 7..12: expected a select variable or expression, found `WHERE`
    ");
}

#[tokio::test]
async fn test_unsupported_accept_is_not_acceptable() {
    let server = spiderman_server().await;

    let response = server
        .get(QUERY_PATH)
        .add_query_param("query", SELECT_NAMES)
        .add_header("accept", "image/png")
        .expect_failure()
        .await;

    insta::assert_snapshot!(render(&response), @"
    406 Not Acceptable
    text/plain; charset=utf-8

    The accept header does not provide any accepted format like application/sparql-results+json or text/tsv.
    ");
}
