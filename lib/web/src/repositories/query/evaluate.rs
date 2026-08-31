use crate::error::RdfFusionServerError;
use crate::repositories::query::HandleQueryResponse;
use crate::repositories::query::results::serialize_query_result;
use crate::repositories::sparql_query_params::SparqlQueryParams;
use anyhow::anyhow;
use oxrdfio::RdfFormat;
use rdf_fusion::common::{IriParseError, NamedNode};
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion::store::Store;

/// Evaluates a SPARQL query and turns it into a result.
pub async fn evaluate_sparql_query(
    store: &Store,
    params: &SparqlQueryParams,
    query: &str,
    rdf_format: Result<RdfFormat, RdfFusionServerError>,
    query_format: Result<QueryResultsFormat, RdfFusionServerError>,
) -> Result<HandleQueryResponse, RdfFusionServerError> {
    let mut options = QueryOptions {
        base_iri: Some(
            params
                .base_uri
                .parse::<rdf_fusion::common::Iri<String>>()
                .map_err(|e| RdfFusionServerError::BadRequest(e.to_string()))?,
        ),
        ..Default::default()
    };

    if params.default_graph_as_union {
        options.dataset.default_graph_as_union = true;
    } else {
        if !params.default_graph_uris.is_empty() {
            options.dataset.default_graphs = Some(
                params
                    .default_graph_uris
                    .iter()
                    .map(|e| {
                        NamedNode::new(e).map_err(|e: IriParseError| {
                            RdfFusionServerError::BadRequest(e.to_string())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        if !params.named_graph_uris.is_empty() {
            options.dataset.named_graphs = Some(
                params
                    .named_graph_uris
                    .iter()
                    .map(|e| {
                        NamedNode::new(e).map_err(|e: IriParseError| {
                            RdfFusionServerError::BadRequest(e.to_string())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }

    let query_result = store
        .query_opt(query, options)
        .await
        .map_err(|e| RdfFusionServerError::Internal(anyhow!(e)))?;
    serialize_query_result(query_result, rdf_format, query_format)
        .await
        .map_err(|e| RdfFusionServerError::Internal(anyhow!(e)))
}
