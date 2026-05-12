# RDF Fusion

> **Warning:** RDF Fusion is currently **experimental**. Everything, including the APIs, encodings, and storage formats
> are subject to breaking changes. It is not yet recommended for production use. 

<p align="center">
  <img src="misc/logo/logo.png" width="128" alt="RDF Fusion Logo" align="right">
</p>

RDF Fusion is an embeddable [SPARQL](https://www.w3.org/TR/sparql11-overview/) engine based on [Apache DataFusion](https://datafusion.apache.org/).

A primary goal of RDF Fusion is to preserve the strengths of DataFusion and make them available to the Semantic Web
community.
These strengths include:

- Extensibility: DataFusion features many extension points that we use to implement SPARQL.
  We expose these extension points to RDF Fusion users for experimenting with SPARQL and domain-specific dialects.
  In the future, we would like to provide further extension points that are tailored towards SPARQL.
- Performance: DataFusion features a vectorized execution engine that can leverage the capabilities of modern CPUs.
  We will provide comparisons to other query engines soon in a
  [separate repository](https://codeberg.org/tschwarzinger/sparql-bencher/).
- Boring Architecture: DataFusion implements an "industry-proven" architecture for query planning and query execution.
  If logical plans and execution plans are familiar to you, you will feel right at home.
  There is no need to learn a fundamentally different architecture for working SPARQL.
  We refer to [DataFusion's documentation](https://datafusion.apache.org/contributor-guide/architecture.html) for this
  purpose.
- Ecosystem: One can integrate RDF Fusion directly with other projects revolving around DataFusion.
  This includes, for example, projects related to spatial data, streaming, and storage.
  For example, we employ the [`delta-rs`](https://github.com/delta-io/delta-rs) crate to store RDF datasets directly in
  cloud object stores.

## Getting Started

You can use `cargo` to interact with the codebase or use [Just](https://github.com/casey/just) to run the pre-defined
commands, also used for continuous integration builds.

```bash
git clone --recursive https://codeberg.org/tschwarzinger/rdf-fusion.git # Clone Repository
git submodule update --init # Initialize submodules
just test # Run tests 
```

### Using RDF Fusion's CLI

Use `cargo` to install the CLI.

```bash
cargo install rdf-fusion-cli
```

Once installed, you can use the CLI to run a SPARQL engine. See `rdf-fusion --help` for more information.

#### Examples

**Serve a SPARQL HTTP server from RDF files:**

```bash
rdf-fusion --storage-type rdf-files --location file://examples/data/spiderman.ttl serve
```

**Build a Delta Lake database from RDF files:**

```bash
rdf-fusion --storage-type delta-lake --location file:///tmp/my-db build-database --inputs file://examples/data/spiderman.ttl
```

**Dump a store into a sorted N-Quads file:**

```bash
rdf-fusion --storage-type rdf-files --location file://examples/data/spiderman.ttl dump --output ./dump.nq --format nq --sort-by GSPO
```

### Using RDF Fusion in your Project

Documentation for using RDF Fusion from another Rust project can be found in the main crate's [documentation](https://docs.rs/rdf-fusion).
Examples of using RDF Fusion can be found in the [examples](./examples) directory.

## Missing Feature?

As mentioned above, RDF Fusion is still in an early stage.
We are missing essential features for a standalone SPARQL engine, such as persistent storage, RDF 1.2 support, a
graphical user interface, and many other features that have been developed in other engines over many years.
Even though Arrow and DataFusion helps us in building these features (A LOT!), this is still a non-trivial task that
requires sustained effort.
If you are looking to implement some of these features, please create or comment on
an [issue](https://codeberg.org/tschwarzinger/rdf-fusion/issues) to get in touch with us.
We are more than happy to help you with your first steps and welcome all kinds of contributions!

## Project Structure

You can find the core implementation of RDF Fusion in the [rdf-fusion](./lib/rdf-fusion) crate.
The sub crates of this crate are explained in its documentation.

In addition to that, this repository also contains the following crates:

- [rdf-fusion-cli](./cli): A command line interface for using RDF Fusion.
- [rdf-fusion-bench](./bench): A program for executing benchmarks with RDF Fusion. Note that Criterion benchmarks are
  not also part of the other creations.
- [rdf-fusion-examples](./examples): A collection of examples for using RDF Fusion.
- [rdf-fusion-testsuite](./testsuite): An end-to-end test suite, including SPARQL conformance tests.

In addition to the Rust code, this repository also contains the following folders:

- [misc](./misc): Miscellaneous files, such as the changelog and license files of Oxigraph (see below).
- [justfile](./justfile) A justfile for running common commands (e.g., preparing benchmark data) with the goal of making
  it easier to run the project locally.

## Help

Feel free to open an issue to ask questions or talk about RDF Fusion!

## License

This project is licensed under the Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE.txt) or
http://www.apache.org/licenses/LICENSE-2.0).

As this project started as a fork of [Oxigraph](https://github.com/oxigraph/oxigraph), it still contains some code
from Oxigraph. Oxigraph was originally licensed under Apache 2.0 OR MIT. This means that the Oxigraph portions of
the code can also be used under the MIT license, while all new contributions in this repository are provided only
under Apache 2.0.

The license files of Oxigraph at the moment of the fork can be found in [oxigraph_license](./misc/oxigraph_license).

## Minimum Supported Rust Version Policy

Our policy is to adopt the Minimum Supported Rust Version (MSRV) of DataFusion.

## Acknowledgements

The project started as a fork from [Oxigraph](https://github.com/oxigraph/oxigraph), a graph database written in Rust
with a custom SPARQL query engine.
RDF Fusion would likely not exist today if it were not for Oxigraph.
While large portions of the codebase have been written from scratch, there is still code from Oxigraph in this
repository.