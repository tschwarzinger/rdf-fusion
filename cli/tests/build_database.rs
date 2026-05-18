use assert_cmd::Command;
use std::fs;

fn temp_db_path(name: &str) -> String {
    let mut db_dir = std::env::temp_dir();
    db_dir.push(name);
    let _ = fs::remove_dir_all(&db_dir);
    db_dir.to_str().unwrap().to_string()
}

#[test]
fn test_cli_build_database_delta_lake() {
    let db_path = temp_db_path("test_cli_build_database_delta_lake");

    // 1. Build the database
    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "delta-lake",
        "--location",
        "../examples/data/spiderman.ttl",
        "build-database",
        &db_path,
    ]);
    cmd.assert().success();

    // 2. Query the built database to verify
    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "delta-lake",
        "--location",
        &db_path,
        "query",
        "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }",
    ]);
    let assert = cmd.assert().success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();

    // spiderman.ttl has 7 triples
    assert!(stdout.contains("7"));

    // Cleanup
    let _ = fs::remove_dir_all(&db_path);
}
