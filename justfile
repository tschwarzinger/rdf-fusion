mod bench

# List available commands
default:
    @just --list

# Runs all relevant formatters
fmt:
    cargo fmt
    taplo fmt **.toml
    npx --yes prettier@3 --write "**/*.{css,html}"

# Run all lints (e.g., formatting, clippy)
lint:
    cargo fmt --all -- --check
    taplo fmt **.toml --check
    npx --yes prettier@3 --check "**/*.{css,html}"
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

