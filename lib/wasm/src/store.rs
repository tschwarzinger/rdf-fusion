use crate::context::JsRdfFusionContext;
use crate::results::{JsDetailedQueryResult, JsQueryExplanation, transform_results};
use datafusion::physical_plan::displayable;
use js_sys::Date;
use rdf_fusion::common::DateTime;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion::store::Store;
use rdf_fusion_encoding::EncodingName;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct JsStore {
    inner: Store,
    metrics: crate::context::JsExplainConfig,
}

#[wasm_bindgen]
impl JsStore {
    #[wasm_bindgen(constructor)]
    pub fn new(context: JsRdfFusionContext) -> Self {
        Self {
            inner: Store::new(context.inner),
            metrics: context.metrics,
        }
    }

    /// Parses and executes the given query against this store.
    ///
    /// A convenience function for calling [`Self::query_explain`] and ignoring the explanation.
    ///
    /// Only the first `limit` solution rows are converted into JavaScript; the
    /// returned result object still reports the total row count.
    pub async fn query(&self, query: &str, limit: usize) -> Result<JsValue, JsValue> {
        self.query_explain(query, limit)
            .await
            .map(|result| result.results)
    }

    /// Parses and executes the given query against this store, returning the result and an
    /// explanation.
    ///
    /// Only the first `limit` solution rows are converted into JavaScript; the
    /// full query is still evaluated and the returned result object reports the
    /// total row count in `total_count`.
    pub async fn query_explain(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<JsDetailedQueryResult, JsValue> {
        let store = self.inner.clone();
        let query = query.to_string();
        let metrics = self.metrics;

        crate::runtime::run(async move {
            let options = QueryOptions {
                output_encoding_name: Some(EncodingName::String),
                now: Some(current_date_time()),
                ..Default::default()
            };

            let (results, explanation) = store
                .explain_query_opt(&query, options)
                .await
                .map_err(|e| e.to_string())?;
            let results_val = transform_results(results, limit).await?;

            let mut disp = if metrics.show_metrics {
                datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(
                    explanation.execution_plan.as_ref(),
                )
            } else {
                displayable(explanation.execution_plan.as_ref())
            };

            if metrics.show_statistics {
                disp = disp.set_show_statistics(true);
            }
            let exec_plan_str = disp.indent(true).to_string();

            Ok(JsDetailedQueryResult {
                results: results_val,
                explanation: JsQueryExplanation {
                    planning_latency_ms: explanation.planning_latency.as_secs_f64() * 1000.0,
                    planning_compute_ms: explanation.planning_compute.as_secs_f64() * 1000.0,
                    initial_logical_plan: format!(
                        "{}",
                        explanation.initial_logical_plan.display_indent()
                    ),
                    optimized_logical_plan: format!(
                        "{}",
                        explanation.optimized_logical_plan.display_indent()
                    ),
                    execution_plan: exec_plan_str,
                },
            })
        })
        .await?
    }
}

/// Returns the current time as a [`DateTime`] derived from the browser's clock.
///
/// `wasm32-unknown-unknown` has no `std::time`, so `DateTime::now()` would panic. Instead we read
/// the JavaScript clock (`Date.now()`, milliseconds since the Unix epoch) and convert it.
fn current_date_time() -> DateTime {
    let millis = Date::now() as i64;
    DateTime::from_unix_millis(millis)
        .expect("The current JS time represents a valid dateTime")
}
