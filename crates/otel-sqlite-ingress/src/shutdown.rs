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
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
