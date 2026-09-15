//! OS-signal handling for graceful shutdown.
//!
//! Resolves on Ctrl+C or SIGTERM (Unix); on non-Unix platforms SIGTERM is
//! unavailable and the future only resolves on Ctrl+C.

use tokio::sync::watch;

/// Resolves when the watched flag becomes `true`, or the sender is dropped.
pub(crate) async fn wait_for_halt(halt: &mut watch::Receiver<bool>) {
    loop {
        if halt.changed().await.is_err() || *halt.borrow_and_update() {
            return;
        }
    }
}

pub(crate) async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            // A failed handler installation must not look like a received
            // signal: log it and keep serving instead of initiating shutdown.
            tracing::error!(%error, "failed to install Ctrl+C handler; Ctrl+C shutdown disabled");
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
                tracing::error!(%error, "failed to install SIGTERM handler; SIGTERM shutdown disabled");
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
