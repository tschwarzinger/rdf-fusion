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
