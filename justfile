mod bench

# List available commands
default:
    @just --list

# Runs all relevant formatters
fmt:
    cargo fmt
    taplo fmt **.toml

# Run all lints (e.g., formatting, clippy)
lint:
    cargo fmt --all -- --check
    taplo fmt **.toml --check
    cargo clippy --workspace --all-targets -- -D warnings -D clippy::all

# Run all tests
test:
    cargo test --workspace --exclude rdf-fusion-examples

# Runs all examples to see whether they fail
test-examples:
    cargo test --package rdf-fusion-examples --example custom_function
    cargo run --package rdf-fusion-examples --example custom_function
    cargo test --package rdf-fusion-examples --example custom_storage
    cargo run --package rdf-fusion-examples --example custom_storage
    cargo test --package rdf-fusion-examples --example plan_builder
    cargo run --package rdf-fusion-examples --example plan_builder
    cargo test --package rdf-fusion-examples --example query_store
    cargo run --package rdf-fusion-examples --example query_store
    cargo test --package rdf-fusion-examples --example use_store
    cargo run --package rdf-fusion-examples --example use_store

# Build and check documentation
rustdoc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --document-private-items

# Starts a webserver that can answer SPARQL queries
serve location="memory:///" profile="profiling-nonlto":
    RUSTFLAGS="-C target-cpu=native" cargo run --profile {{ profile }} --bin rdf-fusion -- --location {{ location }} serve --bind 0.0.0.0:7878 --cors

#
# Releases
#

# Check that the crate version matches the release tag
ci-check-version ref_name:
    #!/usr/bin/env bash
    VERSION=$(grep -m 1 "^version = " cargo.toml | cut -d '"' -f 2)
    EXPECTED_VERSION=$(echo "{{ ref_name }}" | sed 's|^release/||' | sed 's/^v//')
    if [ "$VERSION" != "$EXPECTED_VERSION" ]; then \
      echo "Error: Version mismatch. cargo.toml has $VERSION, but tag is {{ ref_name }} (expected $EXPECTED_VERSION)"; \
      exit 1; \
    fi
    echo "Version $VERSION matches tag {{ ref_name }}"

# Creates a tarball from the current version of the repository
prepare-release:
    #!/usr/bin/env bash
    if [[ `git status --porcelain` ]]; then \
        echo "The working directory is not clean. Commit ongoing work before creating a release archive."; \
        exit 1; \
    fi
    git archive --format=tar.gz -o target/rdf-fusion-source.tar.gz HEAD;
    echo "Source archive created. Move the archive to a new folder and extract it. Then run just release.";

# Runs all checks and releases all crates to crates.io
release: lint bench::prepare-benches-tests test test-examples rustdoc
    (cd lib/common && cargo publish)
    (cd lib/encoding && cargo publish)
    (cd lib/extensions && cargo publish)
    (cd lib/compute && cargo publish)
    (cd lib/functions && cargo publish)
    (cd lib/logical && cargo publish)
    (cd lib/physical && cargo publish)
    (cd lib/execution && cargo publish)
    (cd lib/storage && cargo publish)
    (cd lib/rdf-fusion && cargo publish)
    (cd lib/web && cargo publish)
    (cd cli && cargo publish)
    (cd bench && cargo publish)

# CI: Build the release binary and package it
ci-build-binary target:
    #!/usr/bin/env bash
    VERSION=$(grep -m 1 "^version = " Cargo.toml | cut -d '"' -f 2)
    cargo build --profile release --bin rdf-fusion --target {{ target }}
    BINARY_DIR="target/{{ target }}/release"
    TARBALL="target/rdf-fusion-$VERSION-{{ target }}.tar.gz"
    tar -czf "$TARBALL" -C "$BINARY_DIR" rdf-fusion
    echo "Created $TARBALL"

# CI: Package the source code
ci-package-source:
    git archive --format=tar.gz -o target/rdf-fusion-source.tar.gz HEAD
    echo "Source archive created at target/rdf-fusion-source.tar.gz"
