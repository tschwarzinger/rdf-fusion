use crate::error::RdfFusionServerError;
use crate::repositories::query::HandleQueryResponse;
use crate::repositories::query::results::serialize_query_result;
use crate::repositories::sparql_query_params::SparqlQueryParams;
use anyhow::anyhow;
use oxrdfio::RdfFormat;
use rdf_fusion::common::{GraphName, IriParseError, NamedNode, NamedOrBlankNode};
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::execution::sparql::RdfFusionQuery;
use rdf_fusion::store::Store;

/// Evaluates a SPARQL query and turns it into a result.
pub async fn evaluate_sparql_query(
    store: &Store,
    params: &SparqlQueryParams,
    query: &str,
    rdf_format: Result<RdfFormat, RdfFusionServerError>,
    query_format: Result<QueryResultsFormat, RdfFusionServerError>,
) -> Result<HandleQueryResponse, RdfFusionServerError> {
    let mut query = RdfFusionQuery::parse(query, Some(params.base_uri.as_str()))
        .map_err(|e| RdfFusionServerError::BadRequest(e.to_string()))?;

    if params.default_graph_as_union {
        query.dataset_mut().set_default_graph_as_union()
    } else if !params.default_graph_uris.is_empty() || !params.named_graph_uris.is_empty()
    {
        query.dataset_mut().set_default_graph(
            params
                .default_graph_uris
                .iter()
                .map(|e| Ok(NamedNode::new(e)?.into()))
                .collect::<Result<Vec<GraphName>, IriParseError>>()
                .map_err(|e| RdfFusionServerError::BadRequest(e.to_string()))?,
        );
        query.dataset_mut().set_available_named_graphs(
            params
                .named_graph_uris
                .iter()
                .map(|e| Ok(NamedNode::new(e)?.into()))
                .collect::<Result<Vec<NamedOrBlankNode>, IriParseError>>()
                .map_err(|e| RdfFusionServerError::BadRequest(e.to_string()))?,
        );
    }

    let query_result = store
        .query_opt(query, params.to_query_options())
        .await
        .map_err(|e| RdfFusionServerError::Internal(anyhow!(e)))?;
    serialize_query_result(query_result, rdf_format, query_format)
        .await
        .map_err(|e| RdfFusionServerError::Internal(anyhow!(e)))
}
