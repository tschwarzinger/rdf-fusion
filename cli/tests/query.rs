use assert_cmd::Command;
use std::str;

#[test]
fn test_cli_query_spiderman() {
    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "rdf-files",
        "--location",
        "file://../examples/data/spiderman.ttl",
        "query",
        "SELECT ?name WHERE { <http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> ?name } ORDER BY ?name",
    ]);

    let assert = cmd.assert().success();
    let output = assert.get_output();
    let stdout = str::from_utf8(&output.stdout).unwrap();

    insta::assert_snapshot!(
        stdout,
        @r#"
    ?name
    "Spiderman"
    "Человек-паук"@ru
    "#
    );
}

#[test]
fn test_cli_query_explain() {
    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "rdf-files",
        "--location",
        "file://../examples/data/spiderman.ttl",
        "query",
        "--explain",
        "SELECT ?name WHERE { <http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> ?name }",
    ]);

    let assert = cmd.assert().success();
    let stdout = str::from_utf8(&assert.get_output().stdout).unwrap();

    assert!(stdout.contains("Execution Plan:"));
}

#[test]
fn test_cli_query_explain_analyze() {
    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "rdf-files",
        "--location",
        "file://../examples/data/spiderman.ttl",
        "query",
        "--explain",
        "--analyze",
        "SELECT ?name WHERE { <http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> ?name }",
    ]);

    let assert = cmd.assert().success();
    let stdout = str::from_utf8(&assert.get_output().stdout).unwrap();

    assert!(stdout.contains("elapsed_compute")); // elapsed_compute is a common metric
}

#[test]
fn test_cli_query_analyze_without_explain_fails() {
    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "rdf-files",
        "--location",
        "file://../examples/data/spiderman.ttl",
        "query",
        "--analyze",
        "SELECT ?name WHERE { <http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> ?name }",
    ]);

    let assert = cmd.assert().failure();
    let stderr = str::from_utf8(&assert.get_output().stderr).unwrap();

    assert!(stderr.contains("the following required arguments were not provided:"));
}
