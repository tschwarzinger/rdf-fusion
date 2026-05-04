use crate::AppState;
use crate::error::RdfFusionServerError;
use axum::RequestPartsExt;
use axum::extract::{FromRequestParts, Query};
use axum::http::request::Parts;
use rdf_fusion::execution::sparql::QueryOptions;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct SparqlQueryParamsRaw {
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    #[serde(rename = "using-union-graph")]
    pub using_union_graph: Option<bool>,
    #[serde(default)]
    #[serde(rename = "using-graph-uri")]
    pub using_graph_uri: Vec<String>,
    #[serde(default)]
    #[serde(rename = "using-named-graph-uri")]
    pub using_named_graph_uri: Vec<String>,
}

impl SparqlQueryParamsRaw {
    pub fn into_params_unchecked(
        self,
        graph_as_union_default: bool,
    ) -> SparqlQueryParams {
        SparqlQueryParams {
            query: self.query,
            base_uri: "http://localhost:7878".to_owned(), // TODO
            default_graph_uris: self.using_graph_uri,
            named_graph_uris: self.using_named_graph_uri,
            default_graph_as_union: self
                .using_union_graph
                .unwrap_or(graph_as_union_default),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SparqlQueryParams {
    pub query: Option<String>,
    pub base_uri: String,
    pub default_graph_uris: Vec<String>,
    pub named_graph_uris: Vec<String>,
    pub default_graph_as_union: bool,
}

impl SparqlQueryParams {
    #[allow(clippy::unused_self, reason = "Self does not yet contain options.")]
    pub fn to_query_options(&self) -> QueryOptions {
        QueryOptions::default()
    }
}

impl FromRequestParts<AppState> for SparqlQueryParams {
    type Rejection = RdfFusionServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let raw_params = parts
            .extract::<Query<SparqlQueryParamsRaw>>()
            .await
            .map_err(|e| RdfFusionServerError::BadRequest(e.to_string()))?
            .0;

        let use_default_graph_as_union = raw_params
            .using_union_graph
            .unwrap_or(state.union_default_graph);
        if use_default_graph_as_union
            && (!raw_params.using_graph_uri.is_empty()
                || !raw_params.using_named_graph_uri.is_empty())
        {
            return Err(RdfFusionServerError::BadRequest(
                "default-graph-uri or named-graph-uri and union-default-graph should not be set at the same time".to_owned()
            ));
        }

        let result = SparqlQueryParams {
            query: raw_params.query,
            base_uri: "http://localhost:7878".to_owned(), // TODO
            named_graph_uris: raw_params.using_named_graph_uri,
            default_graph_uris: raw_params.using_graph_uri,
            default_graph_as_union: use_default_graph_as_union,
        };
        Ok(result)
    }
}
