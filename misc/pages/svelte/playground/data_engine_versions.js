export const OFFICIAL_VERSIONS = [
    {
        id: "latest",
        name: "Latest Nightly",
        jsUrl: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/wasm/latest/rdf_fusion_wasm.js",
        wasmUrl: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/wasm/latest/rdf_fusion_wasm_bg.wasm",
        supportedStorage: [{ type: "parquet", version: "0.1" }],
        capabilities: ["rdf-conversion"]
    },
    {
        id: "0.3.0",
        name: "RDF Fusion 0.3.0",
        jsUrl: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/wasm/0.3.0/rdf_fusion_wasm.js",
        wasmUrl: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/wasm/0.3.0/rdf_fusion_wasm_bg.wasm",
        supportedStorage: [{ type: "parquet", version: "0.1" }],
        capabilities: ["rdf-conversion"]
    },
    {
        id: "initial",
        name: "Initial Wasm Build (~v0.2.1)",
        jsUrl: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/wasm/initial/rdf_fusion_wasm.js",
        wasmUrl: "https://rdf-fusion-public.b-cdn.net/rdf-fusion-public/wasm/initial/rdf_fusion_wasm_bg.wasm",
        supportedStorage: [{ type: "parquet", version: "0.1" }],
        capabilities: []
    }
];