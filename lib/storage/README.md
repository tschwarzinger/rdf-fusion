RDF Fusion Storage
======

[RDF Fusion][rdf-fusion] is an extensible query execution framework, written in Rust, that is based
on [Apache DataFusion][df].

This crate is a submodule of RDF Fusion that contains shared building blocks for the storage layers (e.g., RDF file scanning, quad table names, and a block cache).

Most projects should use the [`rdf-fusion`] crate directly, which re-exports this module. If you are already using the
[`rdf-fusion`] crate, there is no reason to use this crate directly in your project as well.

[df]: https://crates.io/crates/datafusion

[rdf-fusion]: https://crates.io/crates/rdf-fusion

[`rdf-fusion`]: https://crates.io/crates/rdf-fusion