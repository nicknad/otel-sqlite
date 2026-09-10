//! gRPC health checking (`grpc.health.v1`).
//!
//! The health service answers `Check`/`Watch` for the overall server (empty
//! service name, what standard probes use) and for both OTLP services
//! individually. Statuses are driven by a [`ServingCheck`] sampled once per
//! second: while the storage pipeline is healthy every service reports
//! `SERVING`; the moment the writer or batcher dies they flip to
//! `NOT_SERVING`, so load balancers stop routing traffic instead of queueing
//! into a dead pipeline.
//!
//! Shutdown is explicit too: on the halt/signal path the monitor marks
//! everything NOT_SERVING *before* the drain deadline starts, so in-flight
//! connections finish but no new ones are routed.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::server::HealthReporter;

use crate::ServingCheck;
use crate::shutdown::wait_for_halt;

const OVERALL_SERVICE_NAME: &str = "";
const LOGS_SERVICE_NAME: &str = "opentelemetry.proto.collector.logs.v1.LogsService";
const METRICS_SERVICE_NAME: &str = "opentelemetry.proto.collector.metrics.v1.MetricsService";

/// How often the readiness source is polled; bounds how long a dead pipeline
/// keeps reporting SERVING.
const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Creates the health service plus its status handle. All tracked services
/// start NOT_SERVING; [`spawn_health_monitor`] drives them afterwards.
pub(crate) async fn new_health() -> (HealthReporter, HealthServer<impl Health>) {
    let (reporter, server) = tonic_health::server::health_reporter();
    apply_serving_state(&reporter, false).await;
    (reporter, server)
}

async fn apply_serving_state(reporter: &HealthReporter, serving: bool) {
    let status = if serving {
        tonic_health::ServingStatus::Serving
    } else {
        tonic_health::ServingStatus::NotServing
    };
    reporter
        .set_service_status(OVERALL_SERVICE_NAME, status)
        .await;
    reporter.set_service_status(LOGS_SERVICE_NAME, status).await;
    reporter
        .set_service_status(METRICS_SERVICE_NAME, status)
        .await;
}

/// Spawns the one-second readiness monitor. The task ends when shutdown
/// fires, after marking everything NOT_SERVING so drained traffic stops
/// being routed.
pub(crate) fn spawn_health_monitor(
    reporter: HealthReporter,
    check: Arc<dyn ServingCheck>,
    mut halted: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(HEALTH_POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    apply_serving_state(&reporter, check.is_serving()).await;
                }
                () = wait_for_halt(&mut halted) => {
                    apply_serving_state(&reporter, false).await;
                    return;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tonic::transport::{Channel, Server};
    use tonic_health::pb::health_check_response::ServingStatus;

    struct Dynamic(Arc<AtomicBool>);

    impl ServingCheck for Dynamic {
        fn is_serving(&self) -> bool {
            self.0.load(Ordering::Relaxed)
        }
    }

    async fn check(
        client: &mut tonic_health::pb::health_client::HealthClient<Channel>,
        service: &str,
    ) -> ServingStatus {
        let response = client
            .check(tonic::Request::new(tonic_health::pb::HealthCheckRequest {
                service: service.to_owned(),
            }))
            .await
            .expect("check responds")
            .into_inner();
        ServingStatus::try_from(response.status).unwrap_or(ServingStatus::Unknown)
    }

    #[tokio::test]
    async fn monitor_flips_statuses_with_the_readiness_source() {
        // Start unhealthy: the monitor's first (immediate) tick must keep
        // everything NOT_SERVING.
        let flag = Arc::new(AtomicBool::new(false));
        let (reporter, health_service) = new_health().await;

        let (halt_tx, halt_rx) = watch::channel(false);
        spawn_health_monitor(
            reporter.clone(),
            Arc::new(Dynamic(Arc::clone(&flag))),
            halt_rx,
        );

        // Serve the real health protocol on an ephemeral port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let serve_task = tokio::spawn(async move {
            Server::builder()
                .add_service(health_service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
        });

        let mut client = tonic_health::pb::health_client::HealthClient::new(
            Channel::from_shared(format!("http://{addr}"))
                .expect("valid endpoint")
                .connect()
                .await
                .expect("connect"),
        );

        // Starts NOT_SERVING and stays there while the pipeline is down.
        assert_eq!(check(&mut client, "").await, ServingStatus::NotServing);

        // Healthy pipeline -> SERVING within one poll interval.
        flag.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(check(&mut client, "").await, ServingStatus::Serving);

        // Per-service names track too.
        assert_eq!(
            check(&mut client, LOGS_SERVICE_NAME).await,
            ServingStatus::Serving
        );
        assert_eq!(
            check(&mut client, METRICS_SERVICE_NAME).await,
            ServingStatus::Serving
        );

        // Pipeline dies -> NOT_SERVING within one poll interval.
        flag.store(false, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(check(&mut client, "").await, ServingStatus::NotServing);
        assert_eq!(
            check(&mut client, LOGS_SERVICE_NAME).await,
            ServingStatus::NotServing
        );

        // Shutdown flips everything off and ends the monitor.
        let _ = halt_tx.send(true);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(check(&mut client, "").await, ServingStatus::NotServing);

        serve_task.abort();
    }
}
