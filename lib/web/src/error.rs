use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(thiserror::Error, Debug)]
pub enum RdfFusionServerError {
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Content Negotiation Failed: {0}")]
    ContentNegotiation(String),
    #[error("Server is read-only")]
    ReadOnly,
    #[error("Internal server error: {0}")]
    Internal(anyhow::Error),
}

impl IntoResponse for RdfFusionServerError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            RdfFusionServerError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            RdfFusionServerError::ContentNegotiation(msg) => {
                (StatusCode::NOT_ACCEPTABLE, msg)
            }
            RdfFusionServerError::ReadOnly => (
                StatusCode::FORBIDDEN,
                "Server is in read-only mode".to_owned(),
            ),
            RdfFusionServerError::Internal(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
            }
        };

        (status, message).into_response()
    }
}
