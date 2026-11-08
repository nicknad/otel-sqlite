//! Bounded execution of CPU-heavy mapping jobs on the blocking pool.
//!
//! `spawn_blocking` cannot be cancelled: when a caller's timeout fires the
//! job keeps running and keeps owning the request it was given. Without a
//! budget a burst of large requests can queue an unbounded number of
//! abandoned jobs behind the stream cap. [`MappingBudget`] acquires one
//! permit *before* submitting the job and moves it into the blocking closure,
//! so the permit is held until the work actually finishes — bounding both
//! running and queued-but-not-yet-started mapping work.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinError;
use tokio::time::{Instant, timeout_at};

/// Process-wide budget for mapping jobs submitted to tokio's blocking pool.
#[derive(Debug, Clone)]
pub(crate) struct MappingBudget {
    permits: Arc<Semaphore>,
    timeout: Duration,
}

/// Why a mapping job did not produce a value.
#[derive(Debug)]
pub(crate) enum MappingError {
    /// The deadline expired while waiting for a permit or while the job ran.
    Timeout,
    /// The mapping job panicked or was cancelled by the runtime.
    Join(JoinError),
    /// The budget is closed. Production never closes it.
    Closed,
}

impl MappingBudget {
    /// Creates a budget admitting at most `concurrency` jobs at once. Zero is
    /// clamped to one so the pipeline always makes progress.
    pub(crate) fn new(concurrency: usize, timeout: Duration) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(concurrency.max(1))),
            timeout,
        }
    }

    /// A budget that admits every job, for callers that wire the ingestor
    /// without a process-wide limit (tests, fuzz harness).
    pub(crate) fn unlimited(timeout: Duration) -> Self {
        Self::new(Semaphore::MAX_PERMITS, timeout)
    }

    /// Runs `job` on the blocking pool under one permit and one deadline.
    ///
    /// The permit is owned by the blocking closure, so a caller timeout drops
    /// the future but leaves the abandoned job counted against the budget
    /// until it really finishes. `job` is never submitted if the permit
    /// cannot be acquired before the deadline.
    pub(crate) async fn run<F, T>(&self, job: F) -> Result<T, MappingError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let deadline = Instant::now() + self.timeout;
        let permit = timeout_at(deadline, Arc::clone(&self.permits).acquire_owned())
            .await
            .map_err(|_| MappingError::Timeout)?
            .map_err(|_| MappingError::Closed)?;
        let handle = tokio::task::spawn_blocking(move || {
            // The permit travels with the job, not with the caller's future:
            // a timed-out caller must not release capacity for work that is
            // still running or still queued.
            let _permit = permit;
            job()
        });
        match timeout_at(deadline, handle).await {
            Ok(joined) => joined.map_err(MappingError::Join),
            Err(_) => Err(MappingError::Timeout),
        }
    }

    #[cfg(test)]
    pub(crate) fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Barrier, mpsc};

    use super::*;

    fn budget(limit: usize, timeout_ms: u64) -> MappingBudget {
        MappingBudget::new(limit, Duration::from_millis(timeout_ms))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_job_releases_its_permit() {
        let budget = budget(1, 1_000);
        assert_eq!(budget.run(|| 7).await.expect("job runs"), 7);
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn error_value_releases_its_permit() {
        let budget = budget(1, 1_000);
        let outcome: Result<Result<(), &str>, _> = budget.run(|| Err("bad input")).await;
        assert_eq!(outcome.expect("job runs"), Err("bad input"));
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn panicking_job_releases_its_permit() {
        let budget = budget(1, 1_000);
        let outcome = budget.run(|| -> () { panic!("mapping panic") }).await;
        assert!(matches!(outcome, Err(MappingError::Join(_))));
        assert_eq!(budget.available_permits(), 1, "panic must not leak");
        budget.run(|| ()).await.expect("permit reused");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn permits_are_exhausted_at_the_limit_and_all_return() {
        let budget = Arc::new(budget(2, 5_000));
        let started = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));

        let mut tasks = Vec::new();
        for _ in 0..2 {
            let budget = Arc::clone(&budget);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            tasks.push(tokio::spawn(async move {
                budget
                    .run(move || {
                        started.wait();
                        release.wait();
                    })
                    .await
            }));
        }

        // Both blocking closures are inside the first gate: both permits are
        // held. The second gate keeps them alive while we inspect the budget.
        tokio::task::block_in_place(|| started.wait());
        assert_eq!(budget.available_permits(), 0);
        tokio::task::block_in_place(|| release.wait());

        for task in tasks {
            task.await.expect("task").expect("job");
        }
        assert_eq!(budget.available_permits(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timeout_during_acquire_never_submits_the_job() {
        let budget = budget(1, 100);
        // Hold the only permit directly so the budgeted run must wait for it
        // and hit its deadline before any job is submitted.
        let held = Arc::clone(&budget.permits)
            .acquire_owned()
            .await
            .expect("permit available");

        let ran = Arc::new(AtomicBool::new(false));
        let ran_in_job = Arc::clone(&ran);
        let outcome = budget
            .run(move || {
                ran_in_job.store(true, Ordering::SeqCst);
            })
            .await;
        assert!(matches!(outcome, Err(MappingError::Timeout)));
        assert!(
            !ran.load(Ordering::SeqCst),
            "a job must not be submitted without a permit"
        );

        drop(held);
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoned_job_keeps_its_permit_until_it_finishes() {
        let budget = budget(1, 200);
        let (done_tx, done_rx) = mpsc::channel();

        let outcome = budget
            .run(move || {
                std::thread::sleep(Duration::from_millis(400));
                done_tx.send(()).expect("signal done");
            })
            .await;
        assert!(matches!(outcome, Err(MappingError::Timeout)));
        assert_eq!(
            budget.available_permits(),
            0,
            "the abandoned job must keep its permit until it really finishes"
        );

        tokio::task::block_in_place(|| {
            done_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("abandoned job finished");
        });
        // The next run waits for the abandoned closure to drop its permit.
        budget.run(|| ()).await.expect("permit released");
        assert_eq!(budget.available_permits(), 1);
    }

    #[test]
    fn zero_concurrency_is_clamped_to_one() {
        let budget = budget(0, 1_000);
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_jobs_never_leak_permits() {
        let budget = budget(4, 5_000);
        for value in 0..1_000_u32 {
            let outcome = budget.run(move || value).await.expect("job runs");
            assert_eq!(outcome, value);
        }
        assert_eq!(budget.available_permits(), 4);
    }
}
