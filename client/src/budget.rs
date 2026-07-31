//! Global concurrent-stream budget for transfer commands: `-P` permits,
//! cooperatively shared between objects and segments (allocator/token
//! pattern). Exactly the leaf operations that move one stream hold one
//! [`StreamPermit`] for the stream's duration; orchestration (HEAD,
//! create/complete-multipart, planning, deletes) never holds one — nested
//! acquisition could deadlock once P objects each held a permit and
//! waited for segment permits. tokio's semaphore is FIFO, so a large
//! file's many parts queue fairly against other files' work.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub(crate) struct StreamBudget {
    sem: Arc<Semaphore>,
}

impl StreamBudget {
    pub(crate) fn new(permits: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(permits.clamp(1, Semaphore::MAX_PERMITS))),
        }
    }

    pub(crate) async fn acquire(&self) -> StreamPermit {
        StreamPermit {
            _permit: self
                .sem
                .clone()
                .acquire_owned()
                .await
                .expect("stream-budget semaphore never closes"),
        }
    }
}

/// RAII token: the stream slot is returned when this drops.
pub(crate) struct StreamPermit {
    _permit: OwnedSemaphorePermit,
}

/// Runs a byte-less S3 operation (HEAD, create/complete/abort multipart,
/// list, delete...) under the stream budget, with a spinner task line for
/// the duration. The token is acquired *before* the task line is created,
/// so a visible line always means a held token; the line is finished on
/// both the `Ok` and `Err` paths, and the permit drops (returning the
/// token) when this function returns.
pub(crate) async fn dispatch<T, F: std::future::Future<Output = T>>(
    budget: &StreamBudget,
    ui: Option<&crate::progress::ProgressUi>,
    label: crate::progress::TransferLabel,
    api: &'static str,
    fut: F,
) -> T {
    let _permit = budget.acquire().await;
    let task = match ui {
        Some(ui) => ui.task(label, api),
        None => crate::progress::UnitHandle::noop(),
    };
    let result = fut.await;
    task.finish();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn budget_caps_max_observed_concurrency() {
        let budget = StreamBudget::new(4);
        let current = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let budget = budget.clone();
            let current = current.clone();
            let peak = peak.clone();
            handles.push(tokio::spawn(async move {
                let _permit = budget.acquire().await;
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                current.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert!(
            peak.load(Ordering::SeqCst) <= 4,
            "peak {} > 4",
            peak.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn p1_serializes_and_completes() {
        // liveness regression for the leaf-only rule: P=1 must never deadlock
        let budget = StreamBudget::new(1);
        let done = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..20 {
            let budget = budget.clone();
            let done = done.clone();
            handles.push(tokio::spawn(async move {
                let _permit = budget.acquire().await;
                done.fetch_add(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(done.load(Ordering::SeqCst), 20);
    }

    #[tokio::test]
    async fn zero_permits_clamps_to_one() {
        let budget = StreamBudget::new(0);
        let _permit = budget.acquire().await; // must not hang or panic
    }

    #[tokio::test]
    async fn huge_permit_count_does_not_panic() {
        // `-P 18446744073709551615` (usize::MAX) must not panic building the
        // semaphore -- `Semaphore::new` panics above `Semaphore::MAX_PERMITS`,
        // so `new` must clamp rather than pass the raw CLI value through.
        let budget = StreamBudget::new(usize::MAX);
        let _permit = budget.acquire().await;
    }

    use crate::progress::{ProgressUi, TransferLabel, Verb};

    fn lbl(verb: Verb, path: &str, part: Option<(u64, u64)>) -> TransferLabel {
        TransferLabel {
            verb,
            path: path.into(),
            part,
        }
    }

    #[tokio::test]
    async fn dispatch_releases_token_and_slot_on_ok_and_err() {
        let ui = ProgressUi::hidden(5);
        let budget = StreamBudget::new(1);
        let ok: Result<u32, anyhow::Error> = dispatch(
            &budget,
            Some(&ui),
            lbl(Verb::Inspecting, "b/k", None),
            "HeadObject",
            async { Ok(7) },
        )
        .await;
        assert_eq!(ok.unwrap(), 7);
        let err: Result<u32, anyhow::Error> = dispatch(
            &budget,
            Some(&ui),
            lbl(Verb::Completing, "b/k", None),
            "CompleteMultipartUpload",
            async { Err(anyhow::anyhow!("boom")) },
        )
        .await;
        assert!(err.is_err());
        // budget of 1: a third dispatch only completes if both tokens were returned
        let again: Result<u32, anyhow::Error> = dispatch(
            &budget,
            Some(&ui),
            lbl(Verb::Listing, "b", None),
            "ListObjectsV2",
            async { Ok(1) },
        )
        .await;
        assert_eq!(again.unwrap(), 1);
        assert_eq!(ui.active_detail_bars(), 0, "all task lines cleared");
    }
}
