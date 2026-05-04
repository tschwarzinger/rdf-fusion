mod evaluate;
mod results;

use crate::AppState;
use crate::error::RdfFusionServerError;
use crate::repositories::query::evaluate::evaluate_sparql_query;
use crate::repositories::query::results::HandleQueryResponse;
use crate::repositories::service_description::{
    EndpointKind, generate_service_description,
};
use crate::repositories::sparql_query_params::{SparqlQueryParams, SparqlQueryParamsRaw};
use axum::extract::{Query, State};
use axum::http::header::CONTENT_TYPE;
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::io::RdfFormat;

/// Implements the query GET API.
pub async fn handle_query_get(
    State(state): State<AppState>,
    query_params: SparqlQueryParams,
    rdf_format: Result<RdfFormat, RdfFusionServerError>,
    query_format: Result<QueryResultsFormat, RdfFusionServerError>,
) -> Result<HandleQueryResponse, RdfFusionServerError> {
    handle_query(&state, &query_params, rdf_format, query_format).await
}

/// Implements the query POST API.
pub async fn handle_query_post(
    State(state): State<AppState>,
    query_params: Query<SparqlQueryParamsRaw>,
    rdf_format: Result<RdfFormat, RdfFusionServerError>,
    query_format: Result<QueryResultsFormat, RdfFusionServerError>,
    headers: axum::http::HeaderMap,
    body: String,
) -> Result<HandleQueryResponse, RdfFusionServerError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");

    // See https://www.w3.org/TR/sparql11-protocol/#query-operation
    if content_type.starts_with("application/x-www-form-urlencoded") {
        // Spec: Query string parameters should be "None". All data is in the body.
        // We parse the raw body string directly into our struct.
        let form_params: SparqlQueryParamsRaw = serde_urlencoded::from_str(&body)
            .map_err(|e| {
                RdfFusionServerError::BadRequest(format!("Invalid URL-encoded body: {e}"))
            })?;

        if form_params.query.is_none()
            || form_params.query.as_ref().unwrap().trim().is_empty()
        {
            return Err(RdfFusionServerError::BadRequest(
                "Missing or empty 'query' parameter in form body.".to_string(),
            ));
        }

        let params = form_params.into_params_unchecked(state.union_default_graph);
        handle_query(&state, &params, rdf_format, query_format).await
    } else if content_type.starts_with("application/sparql-query") {
        // Spec: default-graph-uri and named-graph-uri come from the URL.
        // The body contains the raw SPARQL query string.
        let params = query_params.0;

        if body.trim().is_empty() {
            return Err(RdfFusionServerError::BadRequest(
                "Empty SPARQL query body.".to_string(),
            ));
        }

        let params = SparqlQueryParams {
            query: Some(body),
            ..params.into_params_unchecked(state.union_default_graph)
        };
        handle_query(&state, &params, rdf_format, query_format).await
    } else {
        Err(RdfFusionServerError::BadRequest(format!(
            "Unsupported content-type: {content_type}",
        )))
    }
}

/// Implementation of the query API (for GET and POST).
async fn handle_query(
    state: &AppState,
    query_params: &SparqlQueryParams,
    rdf_format: Result<RdfFormat, RdfFusionServerError>,
    query_format: Result<QueryResultsFormat, RdfFusionServerError>,
) -> Result<HandleQueryResponse, RdfFusionServerError> {
    let Some(query) = &query_params.query else {
        return Ok(generate_service_description(
            rdf_format?,
            EndpointKind::Query,
            query_params.default_graph_as_union,
        )
        .into());
    };

    if query.is_empty() {
        return Ok(generate_service_description(
            rdf_format?,
            EndpointKind::Query,
            query_params.default_graph_as_union,
        )
        .into());
    }

    evaluate_sparql_query(&state.store, query_params, query, rdf_format, query_format)
        .await
}
