RDF Fusion Compute
======

[RDF Fusion][rdf-fusion] is an extensible query execution framework, written in Rust, that is based
on [Apache DataFusion][df].

This crate is a submodule of RDF Fusion that defines computations on RDF terms.

Most projects should use the [`rdf-fusion`] crate directly, which re-exports this module. If you are already using the
[`rdf-fusion`] crate, there is no reason to use this crate directly in your project as well.

[df]: https://crates.io/crates/datafusion

[rdf-fusion]: https://crates.io/crates/rdf-fusion

[`rdf-fusion`]: https://crates.io/crates/rdf-fusion