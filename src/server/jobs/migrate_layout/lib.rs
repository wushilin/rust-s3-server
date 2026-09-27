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
        // Key cursor: each batch resumes after the previous one, so a key whose
        // migration fails every time is skipped for the rest of this run (and
        // retried next run) instead of being refetched forever.
        let mut after: Option<String> = None;
        loop {
            if cancel.is_cancelled() || guard.is_cancelled() {
                break 'buckets;
            }
            let keys = match store.legacy_layout_keys(bucket, after.as_deref(), BATCH).await {
                Ok(keys) => keys,
                Err(err) => {
                    log::warn!("[{run_id}] {JOB} bucket={bucket} error={err}");
                    break; // next bucket
                }
            };
            if keys.is_empty() {
                break; // bucket fully migrated (or only failures remain behind the cursor)
            }
            after = keys.last().cloned();
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes an object, then moves it to a legacy 4-level path and repoints
    /// the index — a pre-migration object.
    async fn make_legacy_object(store: &LocalObjectStore, key: &str) -> std::path::PathBuf {
        store.put_object("bucket", key, b"data", None, None, false).await.unwrap();
        let index = store.index("bucket").await.unwrap();
        let row = index.get(key).await.unwrap().unwrap();
        let bucket_dir = store.layout().bucket_dir("bucket").unwrap();
        let leaf = row.blob_dir.rsplit('/').next().unwrap();
        let legacy_rel = format!("objects/aa/bb/cc/dd/{leaf}");
        let legacy_dir = bucket_dir.join(&legacy_rel);
        tokio::fs::create_dir_all(legacy_dir.parent().unwrap()).await.unwrap();
        tokio::fs::rename(bucket_dir.join(&row.blob_dir), &legacy_dir).await.unwrap();
        index.update_blob_dir(key, &row.blob_dir, &legacy_rel).await.unwrap();
        legacy_dir
    }

    /// Leaf dirs under the single-level (4-char) fanout dirs.
    fn single_level_leaves(objects: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut leaves = Vec::new();
        for fanout in std::fs::read_dir(objects).unwrap() {
            let fanout = fanout.unwrap().path();
            if fanout.file_name().unwrap().len() != 4 {
                continue;
            }
            for leaf in std::fs::read_dir(&fanout).unwrap() {
                leaves.push(leaf.unwrap().path());
            }
        }
        leaves
    }

    #[tokio::test]
    async fn a_key_that_always_fails_neither_spins_nor_leaks_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        // "a-broken" sorts first: its legacy dir is gone, so every attempt fails.
        let broken = make_legacy_object(&store, "a-broken").await;
        std::fs::remove_dir_all(&broken).unwrap();
        make_legacy_object(&store, "b-good").await;
        let objects = store.layout().bucket_dir("bucket").unwrap().join("objects");

        assert!(store.migrate_object_layout("bucket", "a-broken").await.is_err());
        assert!(single_level_leaves(&objects).is_empty(), "failed attempt removed its new dir");

        let tasks = TaskRegistry::new();
        let cancel = CancellationToken::new();
        let migrated = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_once(&store, &cancel, &tasks, "test"),
        )
        .await
        .expect("the run must end despite a permanently failing key");
        assert_eq!(migrated, 1, "the good key behind the failing one migrates");
        let leaves = single_level_leaves(&objects);
        assert_eq!(leaves.len(), 1, "only the migrated object's dir: {leaves:?}");
        assert!(std::fs::read_dir(&leaves[0]).unwrap().next().is_some());
    }
}
