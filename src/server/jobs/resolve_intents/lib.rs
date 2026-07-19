//! Scheduled job: drain resolvable stale intents (abandoned publishes /
//! unfinished retirements) for every bucket. Hygiene only — never affects
//! correctness. Registers in the task registry so a stuck run is visible.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::server::config::SweeperConfig;
use crate::server::registry::{TaskKind, TaskRegistry};
use crate::storage::store::LocalObjectStore;
use crate::storage::sweeper::{resolve_intents_bucket, SweepConfig};
use crate::storage::time::now_ms;

pub(crate) const JOB: &str = "resolve_intents";

pub(crate) async fn run_once(
    store: &LocalObjectStore,
    cfg: &SweeperConfig,
    cancel: &CancellationToken,
    tasks: &Arc<TaskRegistry>,
    run_id: &str,
) -> usize {
    // Not operator-cancellable: resolving stale intents is a crash-recovery
    // prerequisite for correctness, not routine cleanup you'd interrupt.
    let guard = tasks.register_uncancellable(run_id, TaskKind::Job, JOB, "all-buckets");
    let progress = guard.progress();
    let buckets = match store.list_buckets().await {
        Ok(buckets) => buckets,
        Err(err) => {
            log::warn!("[{run_id}] {JOB} failed to list buckets error={err}");
            return 0;
        }
    };
    let mut resolved = 0;
    for (bucket, _) in &buckets {
        // Only process shutdown stops it early — never an operator cancel.
        if cancel.is_cancelled() {
            break;
        }
        let pass = SweepConfig {
            intent_batch_size: cfg.intent_batch_size,
            intent_grace_period_ms: cfg.intent_grace_period_secs as i64 * 1000,
            staging_expiry_ms: cfg.staging_expiry_secs as i64 * 1000,
            trash_expiry_ms: cfg.trash_expiry_secs as i64 * 1000,
            now_ms: now_ms(),
        };
        match resolve_intents_bucket(store, bucket, &pass).await {
            Ok(n) => resolved += n,
            Err(crate::storage::errors::StorageError::BucketRebuilding(_)) => {}
            Err(err) => log::warn!("[{run_id}] {JOB} bucket={bucket} error={err}"),
        }
        progress.set_note(format!("resolved {resolved} intents"));
    }
    if resolved > 0 {
        log::info!("[{run_id}] {JOB} complete resolved={resolved}");
    }
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolves_abandoned_intents() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        let index = store.index("bucket").await.unwrap();
        for i in 1..=3u32 {
            index
                .insert_publish_intent(&format!("k{i}"), &format!("objects/none-{i}"), 0)
                .await
                .unwrap();
        }
        let cfg = SweeperConfig {
            interval_secs: 300,
            intent_batch_size: 100,
            intent_grace_period_secs: 0,
            staging_expiry_secs: 0,
            trash_expiry_secs: 0,
            reclaim_interval_secs: 300,
        };
        let n = run_once(
            &store,
            &cfg,
            &CancellationToken::new(),
            &TaskRegistry::new(),
            "t",
        )
        .await;
        assert_eq!(n, 3);
    }
}
