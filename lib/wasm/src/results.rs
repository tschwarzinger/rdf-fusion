use futures::StreamExt;
use rdf_fusion::execution::results::QueryResults;
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
#[derive(Clone)] // Required so the DetailedQueryResult can clone it
pub struct JsQueryExplanation {
    pub planning_latency_ms: f64,
    pub planning_compute_ms: f64,

    #[wasm_bindgen(getter_with_clone)]
    pub initial_logical_plan: String,

    #[wasm_bindgen(getter_with_clone)]
    pub optimized_logical_plan: String,

    #[wasm_bindgen(getter_with_clone)]
    pub execution_plan: String,
}

#[wasm_bindgen]
pub struct JsDetailedQueryResult {
    #[wasm_bindgen(getter_with_clone)]
    pub results: JsValue,

    #[wasm_bindgen(getter_with_clone)]
    pub explanation: JsQueryExplanation,
}

/// Transforms the results of a query into a JavaScript object.
///
/// Only the first `limit` rows are materialized into JavaScript objects; the
/// full result stream is still drained so the query runs to completion. The
/// returned object carries the total row count in `total_count` alongside the
/// (possibly truncated) `solutions` array.
pub(crate) async fn transform_results(
    results: QueryResults,
    limit: usize,
) -> Result<JsValue, JsValue> {
    let results_obj = js_sys::Object::new();

    match results {
        QueryResults::Solutions(mut solutions) => {
            let vars = solutions.variables().to_vec();
            let cols_arr = js_sys::Array::new();
            for var in &vars {
                cols_arr.push(&JsValue::from_str(var.as_str()));
            }

            let rows_arr = js_sys::Array::new();
            let mut rendered = 0usize;
            let mut total_count = 0usize;
            while let Some(solution) = solutions.next().await {
                let solution = solution.map_err(|e| e.to_string())?;
                total_count += 1;
                if rendered < limit {
                    let row_arr = js_sys::Array::new();
                    for var in &vars {
                        match solution.get(var) {
                            Some(term) => {
                                row_arr.push(&JsValue::from_str(&term.to_string()));
                            }
                            None => {
                                row_arr.push(&JsValue::NULL);
                            }
                        }
                    }
                    rows_arr.push(&row_arr);
                    rendered += 1;
                }
            }

            js_sys::Reflect::set(
                &results_obj,
                &JsValue::from_str("variables"),
                &cols_arr,
            )?;
            js_sys::Reflect::set(
                &results_obj,
                &JsValue::from_str("solutions"),
                &rows_arr,
            )?;
            js_sys::Reflect::set(
                &results_obj,
                &JsValue::from_str("total_count"),
                &JsValue::from_f64(total_count as f64),
            )?;
        }
        QueryResults::Boolean(b) => {
            js_sys::Reflect::set(
                &results_obj,
                &JsValue::from_str("boolean"),
                &JsValue::from_bool(b),
            )?;
        }
        QueryResults::Graph(mut stream) => {
            let cols_arr = js_sys::Array::new();
            cols_arr.push(&JsValue::from_str("subject"));
            cols_arr.push(&JsValue::from_str("predicate"));
            cols_arr.push(&JsValue::from_str("object"));

            let rows_arr = js_sys::Array::new();
            let mut rendered = 0usize;
            let mut total_count = 0usize;
            while let Some(triple) = stream.next().await {
                let triple = triple.map_err(|e| e.to_string())?;
                total_count += 1;
                if rendered < limit {
                    let row_arr = js_sys::Array::new();
                    row_arr.push(&JsValue::from_str(&triple.subject.to_string()));
                    row_arr.push(&JsValue::from_str(&triple.predicate.to_string()));
                    row_arr.push(&JsValue::from_str(&triple.object.to_string()));
                    rows_arr.push(&row_arr);
                    rendered += 1;
                }
            }

            js_sys::Reflect::set(
                &results_obj,
                &JsValue::from_str("variables"),
                &cols_arr,
            )?;
            js_sys::Reflect::set(
                &results_obj,
                &JsValue::from_str("solutions"),
                &rows_arr,
            )?;
            js_sys::Reflect::set(
                &results_obj,
                &JsValue::from_str("total_count"),
                &JsValue::from_f64(total_count as f64),
            )?;
        }
    }

    Ok(results_obj.into())
}
