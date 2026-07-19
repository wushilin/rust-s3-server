//! Background job: migrate objects from the legacy 4-level fanout layout
//! (`objects/aa/bb/cc/dd/…`) to the current single-level layout
//! (`objects/<4hex>/…`).
//!
//! New writes already use the single-level layout, so this only relocates
//! pre-existing objects and then finds nothing to do. Per object it does
//! **hardlink → atomic index flip → unlink** (see
//! [`LocalObjectStore::migrate_object_layout`]) — metadata-only, fast, and
//! idempotent. A read concurrent with an object's migration may transiently
//! fail (the old path can vanish mid-read) but never returns wrong content.
//!
//! Detection is a cheap `read_dir` of each bucket's `objects/` root (a legacy
//! fanout dir has a 2-char name; the current layout uses 4-char names), so a
//! fully-migrated bucket is skipped without ever scanning the index.

use tokio_util::sync::CancellationToken;

use std::sync::Arc;

use crate::server::registry::{TaskKind, TaskRegistry};
use crate::storage::store::LocalObjectStore;

pub(crate) const JOB: &str = "migrate_layout";

/// Keys relocated per index query.
const BATCH: usize = 500;

/// Migrates every legacy-layout object across all buckets, then returns the
/// count migrated. Registers a cancellable task (indeterminate progress — total
/// is unknown; the panel shows "migrated N objects" + uptime) only when there is
/// actually work, so it's invisible once migration is complete.
pub(crate) async fn run_once(
    store: &LocalObjectStore,
    cancel: &CancellationToken,
    tasks: &Arc<TaskRegistry>,
    run_id: &str,
) -> u64 {
    let buckets = match store.list_buckets().await {
        Ok(buckets) => buckets,
        Err(err) => {
            log::warn!("[{run_id}] {JOB} failed to list buckets error={err}");
            return 0;
        }
    };

    // Cheap gate: which buckets still carry legacy structure? No index scan.
    let mut legacy_buckets = Vec::new();
    for (bucket, _) in &buckets {
        if store.has_legacy_layout_dirs(bucket).await.unwrap_or(false) {
            legacy_buckets.push(bucket.clone());
        }
    }
    if legacy_buckets.is_empty() {
        return 0; // nothing to migrate — no task, no work
    }

    let guard = tasks.register(run_id, TaskKind::Job, JOB, "all-buckets");
    let mut migrated = 0u64;
    'buckets: for bucket in &legacy_buckets {
        loop {
            if cancel.is_cancelled() || guard.is_cancelled() {
                break 'buckets;
            }
            let keys = match store.legacy_layout_keys(bucket, BATCH).await {
                Ok(keys) => keys,
                Err(err) => {
                    log::warn!("[{run_id}] {JOB} bucket={bucket} error={err}");
                    break; // next bucket
                }
            };
            if keys.is_empty() {
                break; // bucket fully migrated
            }
            for key in keys {
                if cancel.is_cancelled() || guard.is_cancelled() {
                    break 'buckets;
                }
                match store.migrate_object_layout(bucket, &key).await {
                    Ok(true) => migrated += 1,
                    Ok(false) => {}
                    Err(err) => log::warn!("[{run_id}] {JOB} bucket={bucket} key={key} error={err}"),
                }
            }
            guard.progress().set_note(format!("migrated {migrated} objects"));
        }
    }

    if migrated > 0 {
        log::info!("[{run_id}] {JOB} complete migrated={migrated}");
    }
    migrated
}
