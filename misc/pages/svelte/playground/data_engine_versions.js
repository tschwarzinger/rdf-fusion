export const OFFICIAL_VERSIONS = [
    {
        id: "latest",
        name: "Latest Nightly",
        jsUrl: "https://rdf-fusion-public.hel1.your-objectstorage.com/wasm/latest/rdf_fusion_wasm.js",
        wasmUrl: "https://rdf-fusion-public.hel1.your-objectstorage.com/wasm/latest/rdf_fusion_wasm_bg.wasm",
        supportedStorage: [{ type: "parquet", version: "0.1" }]
    },
    {
        id: "initial",
        name: "Initial Wasm Build (~v0.2.1)",
        jsUrl: "https://rdf-fusion-public.hel1.your-objectstorage.com/wasm/initial/rdf_fusion_wasm.js",
        wasmUrl: "https://rdf-fusion-public.hel1.your-objectstorage.com/wasm/initial/rdf_fusion_wasm_bg.wasm",
        supportedStorage: [{ type: "parquet", version: "0.1" }]
    }
];
