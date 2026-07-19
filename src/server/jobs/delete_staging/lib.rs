//! Scheduled job: delete expired staging directories (abandoned single-PUT and
//! multipart uploads) for every bucket. Registers in the task registry.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::server::config::SweeperConfig;
use crate::server::registry::{TaskKind, TaskRegistry};
use crate::storage::store::LocalObjectStore;
use crate::storage::sweeper::{delete_staging_bucket, SweepConfig};
use crate::storage::time::now_ms;

pub(crate) const JOB: &str = "delete_staging";

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
    let mut removed = 0;
    for (bucket, _) in &buckets {
        if cancel.is_cancelled() || guard.is_cancelled() {
            break;
        }
        let pass = SweepConfig {
            intent_batch_size: cfg.intent_batch_size,
            intent_grace_period_ms: cfg.intent_grace_period_secs as i64 * 1000,
            staging_expiry_ms: cfg.staging_expiry_secs as i64 * 1000,
            trash_expiry_ms: cfg.trash_expiry_secs as i64 * 1000,
            now_ms: now_ms(),
        };
        match delete_staging_bucket(store, bucket, &pass).await {
            Ok(n) => removed += n,
            Err(err) => log::warn!("[{run_id}] {JOB} bucket={bucket} error={err}"),
        }
        progress.set_note(format!("deleted {removed} staging dirs"));
    }
    if removed > 0 {
        log::info!("[{run_id}] {JOB} complete removed={removed}");
    }
    removed
}
