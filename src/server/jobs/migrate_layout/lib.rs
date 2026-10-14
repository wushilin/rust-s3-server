//! Job: migrate a bucket to the current on-disk layout, at startup, behind
//! the 503 gate.
//!
//! A bucket serves only while its `bucket.json` says it is stable on the
//! layout this build writes. The startup check
//! ([`LocalObjectStore::plan_layout_migrations`]) marks every bucket on an
//! older layout `migrating` and raises its gate; this job runs the step
//! ([`LocalObjectStore::migrate_bucket_layout`]) and lowers the gate once the
//! bucket is stable. A run that stops half way leaves the bucket `migrating`
//! and the next start resumes it; a step that fails (an object that cannot
//! be moved) is retried every `retry` while the bucket keeps answering 503,
//! so a bucket is never served on a mixed layout.
//!
//! The only step today is layout 1 → 2 (the 4-level fanout to the
//! single-level one): per object **hardlink → atomic index flip → unlink**
//! (see [`LocalObjectStore::migrate_object_layout`]), metadata-only and
//! idempotent.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::server::registry::{TaskKind, TaskRegistry};
use crate::storage::errors::StorageError;
use crate::storage::metadata::BucketLayout;
use crate::storage::store::LocalObjectStore;

pub(crate) const JOB: &str = "migrate_layout";

/// Migrates `bucket` (whose gate the startup check raised) to the current
/// layout, retrying a failed step every `retry`, then lowers the gate.
pub(crate) fn spawn(
    store: LocalObjectStore,
    bucket: String,
    layout: BucketLayout,
    tasks: Arc<TaskRegistry>,
    cancel: CancellationToken,
    retry: Duration,
) {
    let run_id = crate::server::new_request_id();
    tokio::spawn(async move {
        let guard = tasks.register(&run_id, TaskKind::Job, JOB, format!("/{bucket}"));
        log::warn!("[{run_id}] {JOB} started bucket={bucket} layout={layout:?}: the bucket answers 503 until it is done");
        // The gate stays up until the bucket is stable, whatever happens to
        // this task — except at shutdown, when the process ends anyway.
        struct GateGuard {
            store: LocalObjectStore,
            bucket: String,
        }
        impl Drop for GateGuard {
            fn drop(&mut self) {
                self.store.end_rebuild(&self.bucket);
            }
        }
        let gate = GateGuard { store: store.clone(), bucket: bucket.clone() };
        loop {
            match store.migrate_bucket_layout(&bucket, &cancel).await {
                Ok(moved) => {
                    log::info!("[{run_id}] {JOB} complete bucket={bucket} moved={moved}");
                    break;
                }
                Err(err) if cancel.is_cancelled() || guard.is_cancelled() => {
                    log::info!("[{run_id}] {JOB} stopped bucket={bucket}: {err}; resumes at the next start");
                    // Keep the gate up: nothing may serve a half-migrated bucket.
                    std::mem::forget(gate);
                    return;
                }
                Err(err @ StorageError::UnsupportedLayout { .. }) => {
                    log::error!("[{run_id}] {JOB} cannot run bucket={bucket}: {err}; the bucket stays unavailable");
                    guard.progress().set_note(err.to_string());
                    std::mem::forget(gate);
                    return;
                }
                Err(err) => {
                    log::error!("[{run_id}] {JOB} failed bucket={bucket}: {err}; retrying in {}s", retry.as_secs());
                    guard.progress().set_note(format!("{err}; retrying"));
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            std::mem::forget(gate);
                            return;
                        }
                        _ = tokio::time::sleep(retry) => {}
                    }
                }
            }
        }
        drop(gate);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::metadata::CURRENT_LAYOUT;

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

    /// Strips the layout marker, as a bucket.json from before layouts were
    /// recorded has none.
    async fn forget_layout(store: &LocalObjectStore) {
        let path = store.layout().bucket_meta_path("bucket").unwrap();
        let mut meta: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        meta.as_object_mut().unwrap().remove("layout");
        std::fs::write(&path, meta.to_string()).unwrap();
        store.forget_bucket_meta("bucket");
    }

    async fn wait_until(what: &str, mut check: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while !check() {
            assert!(std::time::Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn a_legacy_bucket_is_gated_migrated_and_then_stable() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store.create_bucket("fresh").await.unwrap();
        assert_eq!(store.bucket_layout("fresh").await.unwrap(), BucketLayout::current());
        make_legacy_object(&store, "a").await;
        make_legacy_object(&store, "b").await;
        forget_layout(&store).await;
        assert_eq!(store.bucket_layout("bucket").await.unwrap(), BucketLayout::Stable { version: 1 }, "detected from the tree");

        // Startup: the layout is recorded; a bucket on an older layout keeps
        // serving until an operator upgrades it.
        assert!(store.check_layouts().await.unwrap().is_empty(), "nothing to resume");
        assert_eq!(store.bucket_layout("bucket").await.unwrap(), BucketLayout::Stable { version: 1 });
        assert!(store.index("bucket").await.is_ok(), "an old layout still serves");
        assert_eq!(store.read_object("bucket", "a").await.unwrap().meta.size, 4);
        assert!(LocalObjectStore::layout_upgrade_available(store.bucket_layout("bucket").await.unwrap()));
        assert!(!LocalObjectStore::layout_upgrade_available(store.bucket_layout("fresh").await.unwrap()));

        // The operator starts the upgrade: the bucket is marked migrating
        // and gated; the fresh one is left alone.
        let migrating = BucketLayout::Migrating { from: 1, to: CURRENT_LAYOUT };
        assert_eq!(store.start_layout_upgrade("bucket").await.unwrap(), migrating);
        assert_eq!(store.bucket_layout("bucket").await.unwrap(), migrating);
        assert!(matches!(store.index("bucket").await, Err(StorageError::BucketRebuilding(_))), "nothing serves it");
        assert!(store.index("fresh").await.is_ok());
        assert!(matches!(store.start_layout_upgrade("bucket").await, Err(StorageError::BucketInUse(_))), "already in progress");
        assert!(matches!(store.start_layout_upgrade("fresh").await, Err(StorageError::BucketInUse(_))), "already current");

        let tasks = TaskRegistry::new();
        spawn(store.clone(), "bucket".into(), migrating, tasks, CancellationToken::new(), Duration::from_millis(100));
        wait_until("the bucket is stable", || {
            futures::executor::block_on(async { store.index("bucket").await.is_ok() })
        })
        .await;
        assert_eq!(store.bucket_layout("bucket").await.unwrap(), BucketLayout::current());
        let objects = store.layout().bucket_dir("bucket").unwrap().join("objects");
        assert_eq!(single_level_leaves(&objects).len(), 2);
        assert!(!store.has_legacy_layout_dirs("bucket").await.unwrap());
        assert_eq!(store.read_object("bucket", "a").await.unwrap().meta.size, 4);
        // A later start finds nothing to resume.
        assert!(store.check_layouts().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_interrupted_migration_resumes_and_a_stuck_one_keeps_the_gate_up() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        // "a-broken" sorts first: its legacy dir is gone, so every attempt fails.
        let broken = make_legacy_object(&store, "a-broken").await;
        std::fs::remove_dir_all(&broken).unwrap();
        make_legacy_object(&store, "b-good").await;
        make_legacy_object(&store, "c-done").await;
        // The previous run moved one object and stopped.
        assert!(store.migrate_object_layout("bucket", "c-done").await.unwrap());
        let migrating = BucketLayout::Migrating { from: 1, to: CURRENT_LAYOUT };
        store.set_bucket_layout("bucket", migrating).await.unwrap();

        let resume = store.check_layouts().await.unwrap();
        assert_eq!(resume, vec![("bucket".to_string(), migrating)], "resumes the recorded step, gated");
        assert!(matches!(store.index("bucket").await, Err(StorageError::BucketRebuilding(_))));
        let cancel = CancellationToken::new();
        let tasks = TaskRegistry::new();
        spawn(store.clone(), "bucket".into(), migrating, tasks.clone(), cancel.clone(), Duration::from_millis(200));
        // The good key moves; the broken one keeps the bucket migrating and gated.
        wait_until("b-good moved", || {
            futures::executor::block_on(async {
                !store.index_unchecked("bucket").await.unwrap().get("b-good").await.unwrap().unwrap().blob_dir.starts_with("objects/aa/")
            })
        })
        .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(store.bucket_layout("bucket").await.unwrap(), migrating, "not stable while a key cannot move");
        assert!(matches!(store.index("bucket").await, Err(StorageError::BucketRebuilding(_))), "still gated");
        let objects = store.layout().bucket_dir("bucket").unwrap().join("objects");
        assert_eq!(single_level_leaves(&objects).len(), 2, "no dir leaked per failed attempt");
        // The operator removes the dead row (an index rebuild does the same);
        // the next retry completes.
        store.index_unchecked("bucket").await.unwrap().commit_delete("a-broken", "objects/aa/bb/cc/dd/x", 0).await.unwrap();
        wait_until("the bucket is stable", || futures::executor::block_on(async { store.index("bucket").await.is_ok() })).await;
        assert_eq!(store.bucket_layout("bucket").await.unwrap(), BucketLayout::current());
        cancel.cancel();
    }

    #[tokio::test]
    async fn a_layout_from_a_newer_build_refuses_to_start() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store.set_bucket_layout("bucket", BucketLayout::Stable { version: CURRENT_LAYOUT + 1 }).await.unwrap();
        let err = store.check_layouts().await.unwrap_err();
        assert!(matches!(err, StorageError::UnsupportedLayout { .. }), "{err}");
        assert!(err.to_string().contains("newer build"), "{err}");
        store.set_bucket_layout("bucket", BucketLayout::Migrating { from: CURRENT_LAYOUT, to: CURRENT_LAYOUT + 1 }).await.unwrap();
        assert!(matches!(store.check_layouts().await, Err(StorageError::UnsupportedLayout { .. })));
    }
}
