//! Job: rebuild a bucket's SQLite index from the on-disk blob tree.
//!
//! Rebuild is genuinely a verb — the server doing work — so it registers in the
//! task registry and is visible/cancellable like any other. It runs in the
//! background because it can be long; while it runs the storage layer keeps the
//! data-plane 503 gate up (control plane and UI are unaffected). Triggered by
//! the admin HTTP verb and by the startup missing-index scan; the storage layer
//! also auto-triggers a (gated) rebuild on data access, which is its own
//! safety net.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::server::registry::{TaskKind, TaskRegistry};
use crate::storage::store::LocalObjectStore;

pub(crate) const JOB: &str = "rebuild_index";

/// Launches a registry-tracked rebuild for `bucket` unless one is already
/// running. Returns `false` if a rebuild was already in progress.
pub(crate) fn spawn(store: LocalObjectStore, bucket: String, tasks: Arc<TaskRegistry>) -> bool {
    if !store.try_begin_rebuild(&bucket) {
        log::info!("{JOB} ignored bucket={bucket} reason=already_running");
        return false;
    }
    let run_id = crate::server::new_request_id();
    tokio::spawn(async move {
        // Rebuild is not operator-cancellable: aborting mid-flight just leaves
        // the index unbuilt and re-triggers on next access.
        let guard = tasks.register_uncancellable(&run_id, TaskKind::Job, JOB, format!("/{bucket}"));
        log::info!("[{run_id}] {JOB} started bucket={bucket}");

        // Live progress: the rebuild engine records objects-indexed / trashed in
        // the store's status map; mirror it onto the task so the panel shows a
        // climbing object count (total is unknown until done → indeterminate
        // bar).
        let progress = guard.progress();
        let done = Arc::new(AtomicBool::new(false));
        let poller = {
            let store = store.clone();
            let bucket = bucket.clone();
            let done = done.clone();
            tokio::spawn(async move {
                while !done.load(Ordering::Relaxed) {
                    if let Some((_, p)) = store
                        .rebuild_statuses()
                        .into_iter()
                        .find(|(b, _)| *b == bucket)
                    {
                        // Count goes into the Display string; the total is
                        // unknown until the scan ends, so the bar stays
                        // indeterminate rather than forcing a fake percentage.
                        progress.set_note(format!(
                            "indexed {} objects, trashed {} dirs",
                            p.objects_indexed, p.dirs_trashed
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
            })
        };

        match store.rebuild_sqlite(&bucket).await {
            Ok(count) => log::info!("[{run_id}] {JOB} complete bucket={bucket} objects={count}"),
            Err(err) => log::error!("[{run_id}] {JOB} failed bucket={bucket} error={err}"),
        }
        done.store(true, Ordering::Relaxed);
        let _ = poller.await;
        // Lower the gate last, so no request slips in against a half-swapped
        // index. The guard drops here too, deregistering the task.
        store.end_rebuild(&bucket);
    });
    true
}
