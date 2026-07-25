//! Scheduled job: delete expired trash directories (retired/overwritten/deleted
//! blobs past their grace window) for every bucket. Registers in the task
//! registry.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::server::config::SweeperConfig;
use crate::server::registry::{TaskKind, TaskRegistry};
use crate::storage::store::LocalObjectStore;
use crate::storage::sweeper::delete_trash_bucket;
use crate::storage::time::now_ms;

pub(crate) const JOB: &str = "delete_trash";

pub(crate) async fn run_once(
    store: &LocalObjectStore,
    cfg: &SweeperConfig,
    cancel: &CancellationToken,
    tasks: &Arc<TaskRegistry>,
    run_id: &str,
) -> usize {
    let guard = tasks.register(run_id, TaskKind::Job, JOB, "all-buckets");
    let progress = guard.progress();
    let buckets = match store.list_buckets().await {
        Ok(buckets) => buckets,
        Err(err) => {
            log::warn!("[{run_id}] {JOB} failed to list buckets error={err}");
            return 0;
        }
    };
    let pass = cfg.sweep_pass();
    let mut removed = 0;
    for (bucket, _) in &buckets {
        // Stop on scheduler shutdown or an operator cancel of this run.
        if cancel.is_cancelled() || guard.is_cancelled() {
            break;
        }
        match delete_trash_bucket(store, bucket, &pass, now_ms()).await {
            Ok(n) => removed += n,
            Err(err) => log::warn!("[{run_id}] {JOB} bucket={bucket} error={err}"),
        }
        progress.set_note(format!("deleted {removed} trash dirs"));
    }
    if removed > 0 {
        log::info!("[{run_id}] {JOB} complete removed={removed}");
    }
    removed
}
