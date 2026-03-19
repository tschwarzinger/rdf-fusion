# List available commands
default:
    @just --list

# Run all lints (e.g., formatting, clippy)
lint:
    taplo fmt **.toml --check
    cargo fmt -- --check
    cargo clippy -- -D warnings -D clippy::all

# Run all tests
test:
    cargo test --workspace --exclude rdf-fusion-examples

# Runs all examples to see whether they fail
test-examples:
    cargo run --package rdf-fusion-examples --example custom_function
    cargo run --package rdf-fusion-examples --example custom_storage
    cargo run --package rdf-fusion-examples --example plan_builder
    cargo run --package rdf-fusion-examples --example query_store
    cargo run --package rdf-fusion-examples --example use_store

# Build and check documentation
rustdoc:
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items

[working-directory: 'bench']
prepare-benches-tests:
    cargo run --profile test prepare bsbm-explore --num-products 1000 # BSBM use cases share the data
    cargo run --profile test prepare wind-farm --num-turbines 4

[working-directory: 'bench']
prepare-benches:
    cargo run --profile profiling-nonlto prepare bsbm-explore --num-products 10000 # BSBM use cases share the data
    cargo run --profile profiling-nonlto prepare wind-farm --num-turbines 16

# Starts a webserver that can answer SPARQL queries (debug)
serve-dbg:
    cargo run --bin rdf-fusion -- serve --bind 0.0.0.0:7878

# Starts a webserver that can answer SPARQL queries (profiling)
serve:
    RUSTFLAGS="-C target-cpu=native" cargo run --profile profiling --bin rdf-fusion -- serve --bind 0.0.0.0:7878

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
    (cd lib/model && cargo publish)
    (cd lib/encoding && cargo publish)
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
