RDF Fusion SPARQL Parser
======

[RDF Fusion][rdf-fusion] is an extensible query execution framework, written in Rust, that is based
on [Apache DataFusion][df].

This crate is a submodule of RDF Fusion that contains the SPARQL parser framework of RDF Fusion.

Most projects should use the [`rdf-fusion`] crate directly, which re-exports this module. If you are already using the
[`rdf-fusion`] crate, there is no reason to use this crate directly in your project as well.

# Acknowledgements

The lexer, the Abstract Syntax Tree (AST), and some data models types (e.g., `GraphTarget`) are sourced or based on
[spargebra](https://crates.io/), the SPARQL algebra crate from [oxigraph](https://github.com/oxigraph/oxigraph).
Furthermore, the architecture is inspired by DataFusion's SQL parser, [datafusion-sqlparser-rs](https://github.com/apache/datafusion-sqlparser-rs). 

[df]: https://crates.io/crates/datafusion

[rdf-fusion]: https://crates.io/crates/rdf-fusion

[`rdf-fusion`]: https://crates.io/crates/rdf-fusion