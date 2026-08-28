mod bench

# List available commands
default:
    @just --list

# Runs all relevant formatters
fmt:
    cargo fmt
    taplo fmt **.toml
    npx --yes prettier@3 --write "**/*.{css,html}"

# Run all Rust lints (e.g., formatting, clippy)
lint profile="dev": (lint-rust profile)

# Run all Rust lints (e.g., formatting, clippy)
lint-rust profile="dev":
    cargo fmt --all -- --check
    taplo fmt **.toml --check
    cargo clippy --workspace --all-targets --profile {{ profile }} -- -D warnings -D clippy::all

# Lint the Wasm bindings (requires a Rust toolchain + wasm32 target).
# Only used by the wasm CI workflow.
lint-wasm:
    cargo clippy --package rdf-fusion-wasm --target wasm32-unknown-unknown -- -D warnings -D clippy::all

# Lint the web app / playground frontend (no Rust toolchain required; needs `npm ci` in misc/pages first).
# Only used by the web CI workflow.
lint-web:
    npx --yes prettier@3 --check "**/*.{css,html}"
    npm run lint --prefix misc/pages

# Run all regular tests
test profile="test":
    cargo test --workspace --exclude rdf-fusion-examples --exclude rdf-fusion-wasm --profile {{ profile }}

# Run the tests related to RDF Fusion's Wasm bindings and the playground
#
# The tests run in release mode as we have had out-of-memory issues before.
test-web:
    RUST_TEST_THREADS=1 wasm-pack test --firefox --headless --release ./lib/wasm

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

