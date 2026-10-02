---
title: "RDF Fusion 0.3.0"
date: "2026-10-02"
author: "Tobias Schwarzinger"
tags: ["Release"]
abstract: "RDF Fusion 0.3.0 brings a new tailored SPARQL parser with rich error diagnostics, redb dictionary storage, query planning optimizations, and in-browser RDF-to-Parquet conversion."
---

Today we've released [RDF Fusion 0.3.0](https://codeberg.org/tschwarzinger/rdf-fusion/releases)!
While the immediate trigger for this release was fixing an [issue](https://codeberg.org/tschwarzinger/rdf-fusion/issues/376)
regarding compiling RDF Fusion from [crates.io](https://crates.io), the release bundles a few new features and
improvements developed over the past two months.

This release introduces a handwritten, extensible SPARQL parser with rustc-style diagnostics, migrates our persistent
term dictionary to pure-Rust `redb`, adds smarter query planning and Parquet pruning, and brings in-browser
RDF-to-Parquet conversion to the playground.

## A Tailored SPARQL Parser with Rich Diagnostics

Up until now, RDF Fusion used the lexer and parser from [spargebra](https://crates.io/crates/spargebra).
While `spargebra` served us well, we will run into limitations when we want to write parsers for SPARQL extensions
(e.g., [SigSPARQL](https://ebooks.iospress.nl/volumearticle/74633)).
To solve this, RDF Fusion 0.3.0 comes with its own tailored parser crate, implemented as a handwritten
recursive-descent parser.
By exposing the AST and parsing routines in our public API, users and extensions can construct custom SPARQL
dialects while reusing RDF Fusion's parser for standard elements (such as graph patterns, expressions, and `WHERE`
clauses).
The trade-off is an increased API surface area, but the flexibility is necessary for research and domain-specific
extensions.

Furthermore, the new parser integrates [codespan-reporting](https://crates.io/crates/codespan-reporting) to provide
clear, rustc-style compiler diagnostics with precise source spans and secondary labels. For example, if you forget to
group by a projected variable:

```text
error: Variable ?p is projected but not grouped or aggregated
  ┌─ query.rq:1:8
  │
1 │ SELECT ?p (COUNT(?o) AS ?c)
  │        ^^ Variable ?p is projected but not grouped or aggregated
```

Or when a `.` delimiter is missing:

```text
error: expected `.` between triples, found `FILTER`
  ┌─ query.rq:2:20
  │
2 │   ?s ?p ?o FILTER(?o > 10)
  │            ^^^^^^ expected `.` between triples, found `FILTER`
```

The parser has been validated against the official W3C SPARQL 1.0 and SPARQL 1.1 query and update syntax test suites. If
you encounter an unhelpful error message or believe a query was rejected incorrectly, please open an [issue](https://codeberg.org/tschwarzinger/rdf-fusion/issues)!

## Object ID Storage: Switching from LMDB to redb

In our 0.2.0 release, we introduced persistent on-disk term dictionaries based on LMDB (via the [heed](https://docs.rs/heed) crate)
to support dictionaries larger than available RAM.
While LMDB is extremely fast on native systems, it relies on C bindings and platform-specific memory mapping that cannot
be compiled to WebAssembly.

In this release we've switched the B-Tree backend to [redb](https://github.com/cberner/redb).
This switch enables use to use B-Trees for a triple store inside the playground in the future. 

## Other Changes

For the complete list of changes, please check out the [0.3.0 changelog](https://codeberg.org/tschwarzinger/rdf-fusion/src/branch/main/misc/changelog/0.3.0.md).

If you want to discuss or collaborate on anything we're doing here, don't hesitate to drop me an [e-mail](mailto:tobias.schwarzinger@tuwien.ac.at) or open an [issue](https://codeberg.org/tschwarzinger/rdf-fusion/issues).