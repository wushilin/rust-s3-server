//! Background job: reclaim orphaned empty fanout directories.
//!
//! Deleting/overwriting an object removes its leaf dir but can leave an empty
//! fanout dir behind, so a bucket's directory count grows with churn (a real
//! bucket was seen with 22K objects but 13.8M directories under the old 4-level
//! layout). This job walks each bucket's `objects/` tree and `remove_dir`s the
//! empties.
//!
//! Cadence (starts immediately, then every `reclaim_interval_secs`): **one pass
//! per run**. Deliberately not a drain-to-quiescence loop — under continuous
//! deletes a drain never converges (each pass finds freshly-emptied dirs) and
//! would spin forever, and the single-level layout has no multi-level chains to
//! converge anyway. A legacy 4-level chain whose parent is exposed empty this
//! run is simply reclaimed on the next run.
//!
//! Safety lives in [`LocalObjectStore::reclaim_empty_dirs_pass`]: it uses
//! `remove_dir` only, so it can never touch a non-empty directory (an object),
//! and the publish path retries on `ENOENT`, so a lost race is a no-op — no PUT
//! ever fails. Every `remove_dir` error is ignored.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::server::registry::{TaskKind, TaskRegistry};
use crate::storage::store::LocalObjectStore;

pub(crate) const JOB: &str = "reclaim_dirs";

/// Runs one reclaim pass over every bucket, then returns the total removed.
/// Registers a cancellable task whose note reports the running count (the total
/// is unknowable, so progress is indeterminate — the panel shows uptime +
/// "reclaimed N …"). One pass per run (not a drain): the scheduler re-runs on an
/// interval, which idles cleanly instead of spinning under delete churn.
pub(crate) async fn run_once(
    store: &LocalObjectStore,
    cancel: &CancellationToken,
    tasks: &Arc<TaskRegistry>,
    run_id: &str,
) -> usize {
    let buckets = match store.list_buckets().await {
        Ok(buckets) => buckets,
        Err(err) => {
            log::warn!("[{run_id}] {JOB} failed to list buckets error={err}");
            return 0;
        }
    };

    let reclaimed = Arc::new(AtomicUsize::new(0));
    let guard = tasks.register(run_id, TaskKind::Job, JOB, "all-buckets");
    let progress = guard.progress();
    // Live-count poller: refresh the status note ~1/s from the shared counter,
    // so the panel shows the count climbing during a long pass.
    let poller = {
        let progress = Arc::clone(&progress);
        let reclaimed = Arc::clone(&reclaimed);
        tokio::spawn(async move {
            loop {
                progress.set_note(format!(
                    "reclaimed {} empty dirs",
                    reclaimed.load(Ordering::Relaxed)
                ));
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    };

    // One pass per bucket per run — NOT a drain-to-quiescence loop. Under delete
    // churn a drain would never converge (every pass finds freshly-emptied dirs
    // and loops forever), and the single-level layout needs no multi-level
    // convergence anyway. Any empty parents exposed this run (only possible for
    // legacy 4-level chains) are reclaimed on the next scheduled run. The pass
    // is handed the operator-cancel token so a UI cancel stops it mid-pass; it
    // also checks the store shutdown token internally.
    let guard_cancel = guard.cancel_token();
    for (bucket, _) in &buckets {
        if cancel.is_cancelled() || guard.is_cancelled() {
            break;
        }
        if let Err(err) = store.reclaim_empty_dirs_pass(bucket, &guard_cancel, &reclaimed).await {
            log::warn!("[{run_id}] {JOB} bucket={bucket} error={err}");
        }
    }

    poller.abort();
    let total = reclaimed.load(Ordering::Relaxed);
    if total > 0 {
        log::info!("[{run_id}] {JOB} complete reclaimed={total}");
    }
    total
}
