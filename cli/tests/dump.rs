use assert_cmd::Command;
use std::fs;
use std::str;

#[test]
fn test_cli_dump_spiderman() {
    let mut dump_file = std::env::temp_dir();
    dump_file.push("dump_spiderman.nq");
    let dump_path = dump_file.to_str().unwrap().to_string();
    let _ = fs::remove_file(&dump_path);

    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "rdf-files",
        "--location",
        "file://../examples/data/spiderman.ttl",
        "dump",
        "--output",
        &format!("file://{}", dump_path),
        "--format",
        "nq",
    ]);

    cmd.assert().success();

    let content = fs::read_to_string(&dump_path).expect("Dump file should exist");
    let mut lines: Vec<&str> = content.lines().collect();
    lines.sort();
    let sorted_content = lines.join("\n");

    insta::assert_snapshot!(sorted_content);
}

#[test]
fn test_cli_dump_spiderman_graph() {
    let mut dump_file = std::env::temp_dir();
    dump_file.push("dump_spiderman_graph.nq");
    let dump_path = dump_file.to_str().unwrap().to_string();
    let _ = fs::remove_file(&dump_path);

    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "rdf-files",
        "--location",
        "file://../examples/data/spiderman.ttl",
        "dump",
        "--output",
        &format!("file://{}", dump_path),
        "--format",
        "nq",
        "--graph",
        "http://example.org/default", // spiderman.ttl doesn't have named graphs, but let's see what happens.
    ]);

    // Actually, spiderman.ttl is loaded into the default graph.
    // If I ask for a non-existent graph, it should be empty.

    cmd.assert().success();
    let content = fs::read_to_string(&dump_path).expect("Dump file should exist");
    assert!(content.is_empty());
}

#[test]
fn test_cli_dump_spiderman_sorted() {
    let mut dump_file = std::env::temp_dir();
    dump_file.push("dump_spiderman_sorted.nq");
    let dump_path = dump_file.to_str().unwrap().to_string();
    let _ = fs::remove_file(&dump_path);

    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "rdf-files",
        "--location",
        "file://../examples/data/spiderman.ttl",
        "dump",
        "--output",
        &format!("file://{}", dump_path),
        "--format",
        "nq",
        "--sort-by",
        "SPO",
    ]);

    cmd.assert().success();

    let content = fs::read_to_string(&dump_path).expect("Dump file should exist");
    insta::assert_snapshot!(content);
}

#[test]
fn test_cli_dump_spiderman_sorted_osp() {
    let mut dump_file = std::env::temp_dir();
    dump_file.push("dump_spiderman_sorted_osp.nq");
    let dump_path = dump_file.to_str().unwrap().to_string();
    let _ = fs::remove_file(&dump_path);

    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "rdf-files",
        "--location",
        "file://../examples/data/spiderman.ttl",
        "dump",
        "--output",
        &format!("file://{}", dump_path),
        "--format",
        "nq",
        "--sort-by",
        "OSP",
    ]);

    cmd.assert().success();

    let content = fs::read_to_string(&dump_path).expect("Dump file should exist");
    insta::assert_snapshot!(content);
}

#[test]
fn test_cli_dump_spiderman_sorted_zorder() {
    let mut dump_file = std::env::temp_dir();
    dump_file.push("dump_spiderman_sorted_zorder.nq");
    let dump_path = dump_file.to_str().unwrap().to_string();
    let _ = fs::remove_file(&dump_path);

    let mut cmd = Command::cargo_bin("rdf-fusion").unwrap();
    cmd.args([
        "--storage-type",
        "rdf-files",
        "--location",
        "file://../examples/data/spiderman.ttl",
        "dump",
        "--output",
        &format!("file://{}", dump_path),
        "--format",
        "nq",
        "--sort-by",
        "ZOrder(SPO)",
    ]);

    cmd.assert().success();

    let content = fs::read_to_string(&dump_path).expect("Dump file should exist");
    insta::assert_snapshot!(content);
}
