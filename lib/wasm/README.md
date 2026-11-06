RDF Fusion Wasm
======

[RDF Fusion][rdf-fusion] is an extensible query execution framework, written in Rust, that is based
on [Apache DataFusion][df].

This crate provides JavaScript and WebAssembly bindings for RDF Fusion. Currently, it is mostly used for powering the
[RDF Fusion Playground][playground]. APIs for using RDF Fusion in another application will likely be missing.

Build the bindings with [wasm-pack]:

```bash
wasm-pack build --target web --release lib/wasm
```

[df]: https://crates.io/crates/datafusion

[rdf-fusion]: https://crates.io/crates/rdf-fusion

[playground]: https://rdf-fusion.org/playground/

[wasm-pack]: https://github.com/drager/wasm-pack
