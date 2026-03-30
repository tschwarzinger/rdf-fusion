#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! Contains the Web API for [RDF Fusion](https://docs.rs/rdf-fusion/).

use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use axum::{Router, middleware, routing::get};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tower_http::trace::{HttpMakeClassifier, TraceLayer};
use tracing::{Level, error};

mod app;
mod config;
mod error;
mod repositories;
mod state;

use crate::app::create_app_routes;
use crate::repositories::create_repositories_routes;
pub use config::ServerConfig;
pub use state::AppState;

// TODO: proper logging
#[allow(clippy::print_stdout)]
pub async fn serve(config: ServerConfig) -> anyhow::Result<()> {
    let addr = SocketAddr::from_str(&config.bind)?;

    let app_state = AppState {
        store: Arc::new(config.store),
        read_only: config.read_only,
        union_default_graph: config.union_default_graph,
    };
    let app = create_router(app_state);

    let app = if config.cors {
        // TODO: check how permissive this should be
        app.layer(tower_http::cors::CorsLayer::permissive())
    } else {
        app
    };

    println!("Listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    Ok(axum::serve(listener, app).await?)
}

pub fn create_router(app_state: AppState) -> Router {
    Router::new()
        .route("/", get(|| async { Redirect::permanent("/app") }))
        .nest("/app", create_app_routes())
        .nest("/repositories", create_repositories_routes())
        .with_state(app_state)
        .layer(DefaultBodyLimit::disable())
        .layer(create_tracing_layer())
        .layer(middleware::from_fn(log_error_responses))
}

/// Creates the tracing (logging) layer for the web application.
fn create_tracing_layer() -> TraceLayer<HttpMakeClassifier> {
    TraceLayer::new_for_http()
        .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(Level::INFO))
        .on_request(tower_http::trace::DefaultOnRequest::new().level(Level::DEBUG))
        .on_response(tower_http::trace::DefaultOnResponse::new().level(Level::DEBUG))
        .on_failure(tower_http::trace::DefaultOnFailure::new().level(Level::ERROR))
}

/// Logs the body of an error response.
pub async fn log_error_responses(req: Request<Body>, next: Next) -> impl IntoResponse {
    let response = next.run(req).await;
    let status = response.status();

    if status.is_client_error() || status.is_server_error() {
        let (parts, body) = response.into_parts();
        let body_result = axum::body::to_bytes(body, usize::MAX).await;

        match body_result {
            Ok(bytes) => {
                let body_text = String::from_utf8_lossy(bytes.as_ref());
                error!("Error response {}: {}", status, body_text);
                Response::from_parts(parts, Body::from(bytes))
            }
            Err(error) => {
                error!(
                    "Error response {}: <Could not read body>, {}",
                    status, error
                );
                Response::from_parts(parts, Body::empty())
            }
        }
    } else {
        response
    }
}
