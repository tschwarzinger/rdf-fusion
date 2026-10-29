# RDF Fusion

Embeddable SPARQL engine written in Rust on top of Apache DataFusion (forked from Oxigraph). Experimental: APIs, encodings, and storage formats are unstable.

## Setup

- Clone with `--recursive` (or run `git submodule update --init`). `testsuite/suites/rdf-tests` is the w3c/rdf-tests submodule; it is fetched into the workspace checkout, not via the network at test time.
- Git LFS is used (`*.bz2` BSBM query files under `bench/bench_files`). CI installs `git-lfs`; a plain clone that skips LFS will have missing/stub benchmark inputs.
- The workspace `[patch]` in `Cargo.toml` redirects `deltalake` / `delta_kernel` to **sibling checkouts** at `../delta-rs` and `../delta-kernel-rs` (relative to the repo root). These must exist or the build fails. The pinned git revs are the fallback if the patch block is disabled.
- Toolchain: `cargo` + `just` + `taplo-cli` (TOML formatter). MSRV is DataFusion's (currently `rust-version = "1.94"`); edition 2024.

## Common commands

Everything goes through the [`justfile`](./justfile) (plus `bench/justfile` for the bench module).

| Command | Purpose |
| --- | --- |
| `just test` | Run the suite: `cargo test --workspace --exclude rdf-fusion-examples --exclude rdf-fusion-wasm --profile test`. Excludes examples and wasm by design. |
| `just bench prepare-benches-tests` | **Must run before `just test`** (network download to `bench/data/bsbm-1000`). CI does this first. |
| `just lint` | `cargo fmt --all -- --check` + `taplo fmt **.toml --check` + `cargo clippy --workspace --all-targets --profile dev -- -D warnings -D clippy::all`. Pass a profile arg (`ci`) to mirror CI. |
| `just test-examples` | Run the `examples/` examples (excluded from `just test`). |
| `just rustdoc` | `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --document-private-items`. |
| `just fmt` | Formats Rust, TOML, and web css/html (prettier). |
| `just serve` | Run the CLI query server on `0.0.0.0:7878` (`--profile profiling-nonlto`). |
| `just lint-wasm` / `just lint-web` / `just test-web` | WAsm/web targets; see "Web / Wasm" below. |

The canonical check order is `lint` then `test` (as in CI).

## Workspace layout

- `lib/rdf-fusion` — the public facade crate (`rdf_fusion`). This is the API consumers use; keep user-facing types here and re-export layer crates (`rdf_fusion::common`, `::encoding`, `::execution`, `::storage`, `::sparql_parser`, `::logical`, `::physical`, `::functions`, `::compute`, `::extensions`).
- `lib/*` — one crate per architecture layer, mirroring DataFusion: `logical` (planning), `physical` (execution plans), `execution`, `compute`, `functions`, `common`, `encoding` (quad storage encoding), `storage` (shared storage building blocks: RDF file scanning, quad table names, block cache), `storage-parquet` (Parquet quad storage), `storage-deltalake` (Delta Lake quad storage; depends on `storage-parquet`, native-only), `sparql-parser`, `extensions` (DataFusion extension points), `wasm` (wasm-bindgen bindings), `web` (HTTP service, used only by the CLI).
- `cli/` — package `rdf-fusion-cli`, binary **`rdf-fusion`** (build.rs generates shell completions + manpages). Think "main" entrypoint for runnable pie -- it is the `bench` crate's sibling, not the lib.
- `bench/` — `rdf-fusion-bench` binary + criterion benches (`harness = false`), BSBM datasets. `bench/justfile` (wired in via `mod bench`) has dataset prep and bench commands.
- `testsuite/` — end-to-end and W3C SPARQL conformance tests (insta snapshots), plus a CLI wrapper crate.
- `examples/` — runnable usage examples, excluded from `just test`.

## Lint gotchas

`clippy.toml` (inherited from DataFusion) is enforced with `-D warnings`:

- `large_futures` threshold is lowered to `future-size-threshold = 10000` to prevent stack-overflow crashes. Keep stack-allocated futures small.
- Disallowed: `tokio::task::spawn`/`spawn_blocking` (use `SpawnedTask::…` instead), `std::thread::sleep` and `std::fs::read`/`read_to_string` in async contexts (use `tokio::time`/`tokio::fs`), and `std::time::Instant` (use `datafusion_common::instant::Instant` for wasm compatibility).

## Build profiles

- `dev` and `test` set `overflow-checks = false` (workaround for DataFusion join-estimation overflow). Don't re-enable casually.
- `release` enables `lto = true` and is slow. Use `release-nonlto` for local "release-ish" builds during development.
- CI uses `ci` (make deps smaller for caching); bench/`serve` uses `profiling-nonlto`.

## Testing quirks

- W3C conformance/parser tests (`testsuite/tests/*.rs`, `testsuite/src/w3c/*`) fetch manifests/data **over the network** (`https://w3c.github.io/rdf-tests/...`, `codeberg.org`, `www.w3.org`) via `TEST_RUNTIME_ENV`, not from the local submodule. They need network access and can be slow.
- Parser/conformance assertions use **insta snapshots** (`testsuite/tests/parser/**/*.snap`). Update with `INSTA_UPDATE=always cargo test -p rdf-fusion-testsuite ...` (or `cargo insta`); do not hand-edit snapshots.
- Repo-local SPARQL tests (new `.rq`/`.ttl`/`.srx` + `manifest.ttl`) live under `testsuite/suites/rdf-fusion-tests/sparql/`, referenced by a manifest URL.
- Benchmarks (criterion, `harness = false`) require prepared BSBM data in `bench/data/`.

## Web / Wasm

- `lib/wasm` — wasm-bindgen bindings; requires the `wasm32-unknown-unknown` target. `wasm-bindgen`/`wasm-bindgen-futures` are pinned (`=0.2.129` / `=0.4.79`). `.cargo/config.toml` sets wasm rustflags.
- `misc/pages` — website + playground (Hugo + Svelte/Rollup). `npm ci` there, then `just test-web` runs the Playwright (Firefox) E2E suite (build wasm first, needs `playground-browsers` once). `just test-web-update` regenerates golden screenshots.
- `lib/web` is not re-exported from the `rdf-fusion` crate; it is wired directly into the CLI.
