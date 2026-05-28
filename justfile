# List available commands
default:
    @just --list

# Runs all relevant formatters
fmt:
    cargo fmt
    taplo fmt **.toml

# Run all lints (e.g., formatting, clippy)
lint:
    cargo fmt -- --check
    taplo fmt **.toml --check
    cargo clippy -- -D warnings -D clippy::all

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
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items

import 'bench/justfile'

# Starts a webserver that can answer SPARQL queries
serve location="memory:///" profile="profiling-nonlto":
    RUSTFLAGS="-C target-cpu=native" cargo run --profile {{profile}} --bin rdf-fusion -- --location {{location}} serve --bind 0.0.0.0:7878 --cors

#
# Releases
#

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
release: lint prepare-benches-tests test test-examples rustdoc
    (cd lib/common && cargo publish)
    (cd lib/encoding && cargo publish)
    (cd lib/compute && cargo publish)
    (cd lib/extensions && cargo publish)
    (cd lib/functions && cargo publish)
    (cd lib/logical && cargo publish)
    (cd lib/physical && cargo publish)
    (cd lib/storage && cargo publish)
    (cd lib/execution && cargo publish)
    (cd lib/rdf-fusion && cargo publish)
    (cd lib/web && cargo publish)
    (cd cli && cargo publish)
    (cd bench && cargo publish)
    echo "All crates release. Please rename the archive, upload the tarball to GitHub, and create a Git tag."
