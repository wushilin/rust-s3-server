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
            sem: Arc::new(Semaphore::new(permits.max(1))),
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
}
