mod solutions;

use crate::error::RdfFusionServerError;
use crate::repositories::query::results::solutions::serialize_solutions;
use crate::repositories::service_description::ServiceDescription;
use anyhow::Context;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use oxrdfio::{RdfFormat, RdfSerializer};
use rdf_fusion::execution::results::{
    QueryResults, QueryResultsFormat, QueryResultsSerializer,
};

pub async fn serialize_query_result(
    query_result: QueryResults,
    rdf_format: Result<RdfFormat, RdfFusionServerError>,
    query_format: Result<QueryResultsFormat, RdfFusionServerError>,
) -> anyhow::Result<HandleQueryResponse> {
    let response = match query_result {
        QueryResults::Solutions(solutions) => {
            let format = query_format?;
            let result = serialize_solutions(solutions, format).await?;
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", format.media_type())
                .body(result.into())
                .context("Could not build response")
        }
        QueryResults::Boolean(result) => {
            let format = query_format?;

            let mut buffer = Vec::new();
            QueryResultsSerializer::from_format(format)
                .serialize_boolean_to_writer(&mut buffer, result)?;

            Response::builder()
                .header("Content-Type", format.media_type())
                .status(StatusCode::OK)
                .body(buffer.into())
                .context("Could not build response")
        }
        QueryResults::Graph(mut triples) => {
            let format = rdf_format?;

            let mut buffer = Vec::new();
            let serializer = RdfSerializer::from_format(format);
            let mut serializer = serializer.for_writer(&mut buffer);

            while let Some(triple) = triples.next().await {
                serializer.serialize_triple(triple?.as_ref())?;
            }

            serializer
                .finish()
                .context("Could not finalize serializer")?;

            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", format.media_type())
                .body(buffer.into())
                .context("Could not build response")
        }
    }?;
    Ok(HandleQueryResponse::from(response))
}

/// Holds any of the possible responses from a query request.
pub enum HandleQueryResponse {
    ServiceDescription(ServiceDescription),
    QueryResults(Response),
}

impl IntoResponse for HandleQueryResponse {
    fn into_response(self) -> Response {
        match self {
            HandleQueryResponse::ServiceDescription(sd) => sd.into_response(),
            HandleQueryResponse::QueryResults(resp) => resp,
        }
    }
}

impl From<ServiceDescription> for HandleQueryResponse {
    fn from(value: ServiceDescription) -> Self {
        Self::ServiceDescription(value)
    }
}

impl From<Response> for HandleQueryResponse {
    fn from(value: Response) -> Self {
        Self::QueryResults(value)
    }
}
