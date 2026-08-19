use js_sys::Object;
use rdf_fusion_wasm::context::{
    JsEngineConfig, JsExplainConfig, JsQuadStorageEncoding, JsRdfFusionContext,
    create_parquet_context_from_buffer,
};
use rdf_fusion_wasm::store::JsStore;
use wasm_bindgen_test::*;

// Configure the tests to run in a headless browser environment
wasm_bindgen_test_configure!(run_in_browser);

static SPIDERMAN_PARQUET: &[u8] =
    include_bytes!("../../../examples/data/spiderman.parquet");

async fn setup_test_context() -> JsRdfFusionContext {
    let explain_config = JsExplainConfig::new(true, true);
    let engine_config = JsEngineConfig::new(1024, explain_config, Object::new());
    create_parquet_context_from_buffer(
        SPIDERMAN_PARQUET,
        JsQuadStorageEncoding::String,
        engine_config,
    )
    .await
    .unwrap()
}

#[wasm_bindgen_test]
async fn test_js_store_query() {
    let context = setup_test_context().await;
    let store = JsStore::new(context);

    let query = "SELECT * WHERE { ?s ?p ?o } LIMIT 10";
    let result = store.query(query, 10).await;

    let js_value = result.unwrap();
    assert!(
        !js_value.is_undefined(),
        "Query returned undefined instead of a result set"
    );
}

#[wasm_bindgen_test]
async fn test_js_store_query_explain() {
    let context = setup_test_context().await;
    let store = JsStore::new(context);

    let query = "SELECT * WHERE { ?s ?p ?o } LIMIT 10";
    let result = store.query_explain(query, 10).await;

    let detailed_result = result.unwrap();

    let explanation = &detailed_result.explanation;
    assert!(
        explanation.planning_latency_ms >= 0.0,
        "Planning latency should be non-negative"
    );
    assert!(
        explanation.planning_compute_ms >= 0.0,
        "Planning compute should be non-negative"
    );

    assert!(
        !explanation.initial_logical_plan.is_empty(),
        "Initial logical plan string is empty"
    );
    assert!(
        !explanation.optimized_logical_plan.is_empty(),
        "Optimized logical plan string is empty"
    );
    assert!(
        !explanation.execution_plan.is_empty(),
        "Execution plan string is empty"
    );
}
