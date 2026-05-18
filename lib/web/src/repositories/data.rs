use crate::AppState;
use crate::error::RdfFusionServerError;
use anyhow::anyhow;
use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum_extra::TypedHeader;
use futures::TryStreamExt;
use headers::ContentType;
use rdf_fusion::common::RdfFormat;
use rdf_fusion::error::LoaderError;
use rdf_fusion::storage::rdf_files::RdfFileScanOptions;
use tokio_util::io::StreamReader;

/// Inserts the RDF data into the store and optimizes it.
pub async fn handle_data_post(
    content_type: TypedHeader<ContentType>,
    State(state): State<AppState>,
    body: Body,
) -> Result<Response, RdfFusionServerError> {
    if state.read_only {
        return Err(RdfFusionServerError::ReadOnly);
    }

    let format =
        RdfFormat::from_media_type(&content_type.0.to_string()).ok_or_else(|| {
            RdfFusionServerError::BadRequest("Invalid content type.".to_owned())
        })?;

    let body_with_io_error = body
        .into_data_stream()
        .map_err(|e| std::io::Error::other(e.to_string()));

    // TODO logging
    state
        .store
        .load_from_reader(
            StreamReader::new(body_with_io_error),
            RdfFileScanOptions::with_format(format),
        )
        .await
        .map_err(|error| match error {
            LoaderError::Parsing(err) => RdfFusionServerError::BadRequest(format!(
                "Error parsing {format} RDF file: {err}",
            )),
            LoaderError::Storage(_) => {
                RdfFusionServerError::Internal(anyhow!("Error with storage layer."))
            }
            LoaderError::InvalidBaseIri { .. } => {
                RdfFusionServerError::BadRequest("Invalid base IRI.".to_owned())
            }
            LoaderError::UnsupportedRdfFormat(_) => RdfFusionServerError::Internal(
                anyhow!("Unsupported RDF format should be caught before this point."),
            ),
        })?;

    state.store.optimize().await.map_err(|error| {
        RdfFusionServerError::Internal(
            anyhow!(error).context("Could not optimize the store."),
        )
    })?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}
