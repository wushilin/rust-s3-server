//! Background maintenance — hygiene only, never correctness.
//!
//! Under the index-as-truth design the request path is self-sufficient:
//! consistency is established synchronously at the row commit. Everything
//! here only reclaims space:
//!
//! * **Stale intents** — abandoned publishes / unfinished retirements left
//!   by failed operations while the process kept running (crash leftovers
//!   are drained at startup). Reads a near-empty table; never walks the
//!   tree.
//! * **Staging expiry** — abandoned uploads and multiparts.
//! * **Trash expiry** — retired blob dirs past their grace window.
//!
//! A staging directory is safe to delete only when BOTH its folder-name
//! epoch is older than the expiry window AND every file inside it has an
//! mtime older than the window — the second condition prevents racing an
//! in-progress multipart upload whose upload-id happens to look old.

use std::path::Path;
use std::time::SystemTime;

use tokio::task::yield_now;

use super::errors::Result;
use super::metadata::UploadMeta;
use super::staging::epoch_ms_from_staging_id;
use super::store::LocalObjectStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepConfig {
    pub intent_batch_size: usize,
    /// Only intents older than this are resolved — an in-flight operation's
    /// intent must never be mistaken for an abandoned one.
    pub intent_grace_period_ms: i64,
    /// Idle-age threshold for single-PUT staging dirs.
    pub staging_expiry_ms: i64,
    /// Idle-age threshold for incomplete multipart uploads. `0` (or negative)
    /// disables multipart cleanup entirely, matching S3's keep-forever default.
    pub multipart_expiry_ms: i64,
    pub trash_expiry_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepStats {
    pub intents_resolved: usize,
    pub staging_dirs_removed: usize,
    pub trash_dirs_removed: usize,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            intent_batch_size: 100,
            intent_grace_period_ms: 60 * 60 * 1000,
            staging_expiry_ms: 24 * 60 * 60 * 1000,
            multipart_expiry_ms: 30 * 24 * 60 * 60 * 1000,
            trash_expiry_ms: 24 * 60 * 60 * 1000,
        }
    }
}

/// Purpose 1 — drain resolvable stale intents for one bucket. Returns the
/// number resolved.
pub async fn resolve_intents_bucket(
    store: &LocalObjectStore,
    bucket: &str,
    config: &SweepConfig,
) -> Result<usize> {
    let batch_size = config.intent_batch_size.max(1);
    let mut resolved = 0;
    let mut after_id = None;
    loop {
        let outcome = store
            .resolve_stale_intents(bucket, config.intent_grace_period_ms, batch_size, after_id)
            .await?;
        resolved += outcome.resolved;
        // Walk past failed intents with the id cursor: they are the oldest, so
        // restarting from the front would re-select them and starve the rest.
        // Each is retried once per pass.
        if outcome.selected < batch_size || outcome.last_id.is_none() {
            break;
        }
        after_id = outcome.last_id;
        yield_now().await;
    }
    Ok(resolved)
}

/// Purpose 2 — delete expired staging directories for one bucket, with ages
/// evaluated against the caller-supplied `now_ms` ("a sweep pass at time T").
/// Returns the number removed.
pub async fn delete_staging_bucket(
    store: &LocalObjectStore,
    bucket: &str,
    config: &SweepConfig,
    now_ms: i64,
) -> Result<usize> {
    let mut stats = SweepStats::default();
    let bucket_dir = store.layout().bucket_dir(bucket)?;
    sweep_staging(store, bucket, &bucket_dir.join("staging"), config, now_ms, &mut stats).await?;
    Ok(stats.staging_dirs_removed)
}

/// Purpose 3 — delete expired trash directories for one bucket, with ages
/// evaluated against the caller-supplied `now_ms`. Returns the number removed.
pub async fn delete_trash_bucket(
    store: &LocalObjectStore,
    bucket: &str,
    config: &SweepConfig,
    now_ms: i64,
) -> Result<usize> {
    let mut stats = SweepStats::default();
    let bucket_dir = store.layout().bucket_dir(bucket)?;
    sweep_trash(&bucket_dir.join("trash"), config, now_ms, &mut stats).await?;
    Ok(stats.trash_dirs_removed)
}

async fn sweep_staging(
    store: &LocalObjectStore,
    bucket: &str,
    staging_dir: &Path,
    config: &SweepConfig,
    now_ms: i64,
    stats: &mut SweepStats,
) -> Result<()> {
    for kind in ["put", "multipart"] {
        // Single-PUT staging is client-invisible orphaned temp data; an
        // incomplete multipart upload is client-visible and resumable, so it
        // gets its own (longer, S3-friendlier) window — and `0` disables its
        // cleanup entirely, matching S3's keep-forever behavior. Because
        // `all_files_old_enough` inspects every part's mtime, uploading any part
        // refreshes the window, so an actively-progressing upload is never
        // reaped — only one idle for the full window.
        let expiry_ms = if kind == "multipart" {
            config.multipart_expiry_ms
        } else {
            config.staging_expiry_ms
        };
        if expiry_ms <= 0 {
            continue; // cleanup disabled for this kind
        }
        let dir = staging_dir.join(kind);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
                continue;
            };
            let created_ms = epoch_ms_from_staging_id(name).unwrap_or_else(|_| {
                now_ms.saturating_sub(path_age_ms(&path, now_ms).unwrap_or(0))
            });
            let staging_age = now_ms.saturating_sub(created_ms);
            if staging_age >= expiry_ms && all_files_old_enough(&path, now_ms, expiry_ms) {
                // A multipart upload can be Completed at any moment, and
                // Complete's renames don't bump mtimes: take the same locks
                // Complete/Abort hold (upload lock, then the key lock), then
                // re-check idleness under them before reaping.
                let _locks = if kind == "multipart" {
                    let upload_guard = store.lock_multipart_upload(bucket, name).await;
                    let key_guard = match read_upload_key(&path).await {
                        Some(key) => Some(store.lock_object_key(bucket, &key).await),
                        None => None,
                    };
                    if !path.is_dir() || !all_files_old_enough(&path, now_ms, expiry_ms) {
                        continue;
                    }
                    Some((upload_guard, key_guard))
                } else {
                    None
                };
                match tokio::fs::remove_dir_all(&path).await {
                    Ok(()) => {
                        stats.staging_dirs_removed += 1;
                        log::info!(
                            "sweeper removed staging dir kind={} path={} age_ms={}",
                            kind,
                            path.display(),
                            staging_age,
                        );
                    }
                    Err(err) => {
                        log::warn!(
                            "sweeper failed to remove staging dir kind={} path={} error={}",
                            kind,
                            path.display(),
                            err,
                        );
                    }
                }
                yield_now().await;
            }
        }
    }
    Ok(())
}

async fn sweep_trash(
    trash_dir: &Path,
    config: &SweepConfig,
    now_ms: i64,
    stats: &mut SweepStats,
) -> Result<()> {
    let entries = match std::fs::read_dir(trash_dir) {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
            continue;
        };
        // The trash dir name is a staging id stamped at *deletion* time, so the
        // grace window is measured from when the object was deleted — not from
        // the blob's mtime, which `rename` preserves from publish time and
        // would make any not-recently-written object eligible for purge on the
        // very next sweep. Fall back to mtime only if the name is unparseable.
        let deleted_ms = epoch_ms_from_staging_id(name).unwrap_or_else(|_| {
            now_ms.saturating_sub(path_age_ms(&path, now_ms).unwrap_or(0))
        });
        let trash_age = now_ms.saturating_sub(deleted_ms);
        if trash_age < config.trash_expiry_ms {
            continue;
        }
        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => {
                stats.trash_dirs_removed += 1;
                log::info!("sweeper removed trash dir path={}", path.display());
            }
            Err(err) => {
                log::warn!(
                    "sweeper failed to remove trash dir path={} error={}",
                    path.display(),
                    err,
                );
            }
        }
        yield_now().await;
    }
    Ok(())
}

/// The object key a multipart upload targets, from its `upload.json`.
async fn read_upload_key(upload_dir: &Path) -> Option<String> {
    let bytes = tokio::fs::read(upload_dir.join("upload.json")).await.ok()?;
    serde_json::from_slice::<UploadMeta>(&bytes)
        .ok()
        .map(|upload| upload.object_key)
}

fn all_files_old_enough(dir: &Path, now_ms: i64, expiry_ms: i64) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return true;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() {
            let age = path_age_ms(&path, now_ms).unwrap_or(0);
            if age < expiry_ms {
                return false;
            }
        } else if path.is_dir() && !all_files_old_enough(&path, now_ms, expiry_ms) {
            return false;
        }
    }
    true
}

fn path_age_ms(path: &Path, now: i64) -> Result<i64> {
    let modified = std::fs::metadata(path)
        .and_then(|v| v.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let modified_ms = modified
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|v| v.as_millis() as i64)
        .unwrap_or(0);
    Ok(now.saturating_sub(modified_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::time::now_ms;

    /// Test convenience: all three maintenance purposes for one bucket at one
    /// evaluation time. Production drives the three entry points independently.
    async fn sweep_bucket(
        store: &LocalObjectStore,
        bucket: &str,
        config: &SweepConfig,
        now_ms: i64,
    ) -> Result<SweepStats> {
        Ok(SweepStats {
            intents_resolved: resolve_intents_bucket(store, bucket, config).await?,
            staging_dirs_removed: delete_staging_bucket(store, bucket, config, now_ms).await?,
            trash_dirs_removed: delete_trash_bucket(store, bucket, config, now_ms).await?,
        })
    }

    /// Intents that fail every time sit at the front of the id order; with at
    /// least a batch of them, the pass must still walk past to the rest.
    #[tokio::test]
    async fn failing_intents_do_not_starve_resolvable_ones() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        let index = store.index("bucket").await.unwrap();
        let bucket_dir = store.layout().bucket_dir("bucket").unwrap();
        let old = now_ms() - 60_000;

        // Unresolvable: a blob sits at the path, but trash is a plain file so
        // the move to trash fails every time.
        let trash = store.layout().trash_dir("bucket").unwrap();
        let _ = std::fs::remove_dir_all(&trash);
        std::fs::write(&trash, b"not a dir").unwrap();
        let mut stuck = Vec::new();
        for n in 0..5 {
            let rel = format!("objects/ZZ0{n}/stuck");
            std::fs::create_dir_all(bucket_dir.join(&rel)).unwrap();
            std::fs::write(bucket_dir.join(&rel).join("meta.json"), b"{}").unwrap();
            stuck.push(index.insert_publish_intent(&format!("stuck{n}"), &rel, old).await.unwrap());
        }
        // Resolvable: nothing at the recorded path.
        for n in 0..3 {
            index
                .insert_publish_intent(&format!("gone{n}"), &format!("objects/ZZ9{n}/gone"), old)
                .await
                .unwrap();
        }

        let config = SweepConfig {
            intent_batch_size: 2,
            intent_grace_period_ms: 1,
            ..SweepConfig::default()
        };
        assert_eq!(resolve_intents_bucket(&store, "bucket", &config).await.unwrap(), 3);
        let left = index.stale_intents(now_ms(), 0, 100).await.unwrap();
        assert_eq!(left.iter().map(|r| r.id).collect::<Vec<_>>(), stuck);
        assert!(left.iter().all(|r| r.attempts == 1), "each failure retried once per pass");
        // The startup drain walks past them too, and terminates.
        assert_eq!(store.drain_intents("bucket").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn staging_with_recent_files_is_not_swept() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();

        // Create a staging dir whose name has epoch=0 (ancient), but files are freshly written.
        let staging_id = "0_aaaaaaaaaaaaaaaa";
        let staging_dir = store
            .layout()
            .put_staging_dir("bucket", staging_id)
            .unwrap();
        tokio::fs::create_dir_all(&staging_dir).await.unwrap();
        tokio::fs::write(staging_dir.join("part.1"), b"in-progress data")
            .await
            .unwrap();

        // Sweep with now_ms=10_000 (near epoch) so the staging name looks ancient, but the
        // actual file mtime is the real wall clock (≫ 10_000 ms), making path_age_ms return 0.
        let stats = sweep_bucket(
            &store,
            "bucket",
            &SweepConfig {
                intent_batch_size: 100,
                intent_grace_period_ms: 1,
                staging_expiry_ms: 1_000,
                multipart_expiry_ms: 1_000,
                trash_expiry_ms: 1_000,
            },
            10_000,
        )
        .await
        .unwrap();

        assert_eq!(
            stats.staging_dirs_removed, 0,
            "must not sweep a staging dir with recent files"
        );
        assert!(staging_dir.exists());
    }

    #[tokio::test]
    async fn empty_staging_dir_is_swept_when_old_enough() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();

        let staging_id = "0_aaaaaaaaaaaaaaaa";
        let staging_dir = store
            .layout()
            .put_staging_dir("bucket", staging_id)
            .unwrap();
        tokio::fs::create_dir_all(&staging_dir).await.unwrap();
        // No files inside — empty dir is safe to sweep when name is old enough.

        let stats = sweep_bucket(
            &store,
            "bucket",
            &SweepConfig {
                intent_batch_size: 100,
                intent_grace_period_ms: 1,
                staging_expiry_ms: 1_000,
                multipart_expiry_ms: 1_000,
                trash_expiry_ms: 1_000,
            },
            10_000,
        )
        .await
        .unwrap();

        assert_eq!(stats.staging_dirs_removed, 1);
        assert!(!staging_dir.exists());
    }

    #[tokio::test]
    async fn removes_trash_dirs_after_object_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "folder/test.txt", b"hello", None, None, false)
            .await
            .unwrap();

        store
            .delete_object("bucket", "folder/test.txt")
            .await
            .unwrap();
        let trash_dir = store.layout().trash_dir("bucket").unwrap();
        assert!(trash_dir.exists());

        let stats = sweep_bucket(
            &store,
            "bucket",
            &SweepConfig {
                intent_batch_size: 100,
                intent_grace_period_ms: 0,
                staging_expiry_ms: 1000,
                multipart_expiry_ms: 1000,
                trash_expiry_ms: 0,
            },
            now_ms(),
        )
        .await
        .unwrap();

        assert_eq!(stats.trash_dirs_removed, 1);
    }

    #[tokio::test]
    async fn sweep_resolves_stale_intents_in_batches() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        let index = store.index("bucket").await.unwrap();
        // Five abandoned publish intents pointing at nonexistent dirs.
        for i in 1..=5u32 {
            index
                .insert_publish_intent(&format!("key-{i:02}"), &format!("objects/none-{i}"), 0)
                .await
                .unwrap();
        }

        let cfg = SweepConfig {
            intent_batch_size: 2,
            intent_grace_period_ms: 1,
            staging_expiry_ms: 1000,
            multipart_expiry_ms: 1000,
            trash_expiry_ms: 1000,
        };

        let stats = sweep_bucket(&store, "bucket", &cfg, now_ms()).await.unwrap();
        assert_eq!(stats.intents_resolved, 5);
        assert!(index
            .stale_intents(now_ms(), 0, 10)
            .await
            .unwrap()
            .is_empty());
    }
}
