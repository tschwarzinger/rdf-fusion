// This file contains tests for the examples of the readme.
use assert_cmd::Command;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_readme_example_load_delta() {
    let temp_dir = TempDir::new().unwrap();
    let db_dir = temp_dir.path().join("my-db");
    let db_path = format!("file://{}", db_dir.to_str().unwrap());

    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "delta-lake",
        "--location",
        &db_path,
        "load",
        "../examples/data/spiderman.ttl",
    ]);
    cmd.assert().success();
}

#[test]
fn test_readme_example_serve_delta() {
    let temp_dir = TempDir::new().unwrap();
    let db_dir = temp_dir.path().join("my-db");
    let db_path = format!("file://{}", db_dir.to_str().unwrap());

    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "delta-lake",
        "--location",
        &db_path,
        "load",
        "../examples/data/spiderman.ttl",
    ]);
    cmd.assert().success();

    let mut child =
        std::process::Command::new(assert_cmd::cargo::cargo_bin("rdf-fusion"))
            .args([
                "--storage-type",
                "delta-lake",
                "--location",
                &db_path,
                "serve",
            ])
            .spawn()
            .expect("Failed to start serve command");

    std::thread::sleep(std::time::Duration::from_millis(500));
    let status = child
        .try_wait()
        .expect("Failed to check child process status");
    assert!(status.is_none(), "Serve process exited early");
    let _ = child.kill();
}

#[test]
fn test_readme_example_query_parquet() {
    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "parquet",
        "--location",
        "../examples/data/spiderman.parquet",
        "query",
        "SELECT (COUNT(?name) AS ?count) WHERE { <http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> ?name }",
    ]);
    let assert = cmd.assert().success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    assert!(stdout.contains("2"));
}

#[test]
fn test_readme_example_dump_delta() {
    let temp_dir = TempDir::new().unwrap();
    let db_dir = temp_dir.path().join("my-db");
    let db_path = format!("file://{}", db_dir.to_str().unwrap());

    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "delta-lake",
        "--location",
        &db_path,
        "load",
        "../examples/data/spiderman.ttl",
    ]);
    cmd.assert().success();

    let dump_file = temp_dir.path().join("dump.nq");
    let dump_path = format!("file://{}", dump_file.to_str().unwrap());
    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "delta-lake",
        "--location",
        &db_path,
        "dump",
        &dump_path,
        "--format",
        "nq",
        "--sort-by",
        "GSPO",
    ]);
    cmd.assert().success();

    let content = fs::read_to_string(&dump_file).expect("Dump file should exist");
    assert!(!content.is_empty());
    assert!(content.contains("<http://example.org/#spiderman>"));
}
