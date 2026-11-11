use anyhow::Context;
use datafusion::common::runtime::SpawnedTask;
use rdf_fusion::store::Store;
use rdf_fusion_web::ServerConfig;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tracing::{info, warn};

/// How long in-flight requests may take to finish after a shutdown signal was received.
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(5);

/// Starts a Web Server that serves RDF Fusion's Web API.
///
/// The server shuts down gracefully on SIGINT or SIGTERM. In-flight requests get
/// [`SHUTDOWN_GRACE_PERIOD`] to finish; a second signal exits immediately.
pub async fn serve(
    store: Store,
    bind: &str,
    read_only: bool,
    cors: bool,
    union_default_graph: bool,
) -> anyhow::Result<()> {
    let server_config = ServerConfig {
        store,
        bind: bind.to_owned(),
        read_only,
        cors,
        union_default_graph,
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let shutdown = async move {
        // An error means the sender was dropped, which also means we should stop.
        let _ = shutdown_rx.await;
    };
    let mut server = SpawnedTask::spawn(async move {
        rdf_fusion_web::serve(server_config, shutdown).await
    });

    tokio::select! {
        result = &mut server => return server_result(result),
        () = shutdown_signal() => {}
    }

    info!("Shutting down");
    let _ = shutdown_tx.send(());

    tokio::select! {
        result = &mut server => server_result(result),
        () = tokio::time::sleep(SHUTDOWN_GRACE_PERIOD) => {
            warn!(
                "In-flight requests did not finish within {}s, aborting them",
                SHUTDOWN_GRACE_PERIOD.as_secs()
            );
            Ok(())
        }
        () = shutdown_signal() => {
            warn!("Received a second shutdown signal, aborting in-flight requests");
            Ok(())
        }
    }
}

fn server_result(result: Result<anyhow::Result<()>, JoinError>) -> anyhow::Result<()> {
    result
        .context("Failed to join Web Server")?
        .context("Error from Web Server")
}

/// Resolves once the process receives SIGINT (Ctrl-C) or, on Unix, SIGTERM.
///
/// Installing these handlers is required for stopping the server in a container, where it runs as
/// PID 1 and the kernel does not apply the default action of signals without a handler.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!("Failed to listen for Ctrl-C: {error}");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                warn!("Failed to listen for SIGTERM: {error}");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
