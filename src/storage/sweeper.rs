//! Background maintenance for bounded, non-live cleanup.
//!
//! A staging directory is safe to delete only when BOTH its folder-name epoch
//! is older than the expiry window AND every file inside it has an mtime
//! older than the expiry window.  The second condition prevents racing against
//! an in-progress multipart upload whose upload-id happens to look old.
//!
//! The maintenance pass does not scan the live object namespace or every SQLite
//! object row. SQLite drift is repaired only through the targeted
//! visibility_repair_queue.

use std::path::Path;
use std::time::SystemTime;

use tokio::task::yield_now;

use super::errors::Result;
use super::staging::epoch_ms_from_staging_id;
use super::store::LocalObjectStore;
use super::time::now_ms;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepConfig {
    pub visibility_repair_batch_size: usize,
    pub visibility_repair_grace_period_ms: i64,
    pub staging_expiry_ms: i64,
    pub trash_expiry_ms: i64,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepStats {
    pub sqlite_orphans_removed: usize,
    pub physical_orphans_removed: usize,
    pub staging_dirs_removed: usize,
    pub trash_dirs_removed: usize,
    pub fanout_dirs_removed: usize,
    pub sqlite_entries_checked: usize,
    pub visibility_repairs_processed: usize,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            visibility_repair_batch_size: 100,
            visibility_repair_grace_period_ms: 24 * 60 * 60 * 1000,
            staging_expiry_ms: 24 * 60 * 60 * 1000,
            trash_expiry_ms: 10 * 60 * 1000,
            now_ms: now_ms(),
        }
    }
}

/// Runs bounded maintenance for one bucket.
pub async fn sweep_bucket(
    store: &LocalObjectStore,
    bucket: &str,
    config: &SweepConfig,
) -> Result<SweepStats> {
    let mut stats = SweepStats::default();
    let batch_size = config.visibility_repair_batch_size.max(1);
    loop {
        let batch = store
            .process_visibility_repairs(
                bucket,
                config.visibility_repair_grace_period_ms,
                batch_size,
            )
            .await?;
        stats.visibility_repairs_processed += batch.selected;
        if batch.selected < batch_size || batch.failed > 0 {
            break;
        }
        yield_now().await;
    }
    let bucket_dir = store.layout().bucket_dir(bucket)?;
    sweep_staging(&bucket_dir.join("staging"), config, &mut stats).await?;
    sweep_trash(&bucket_dir.join("trash"), config, &mut stats).await?;
    Ok(stats)
}

async fn sweep_staging(
    staging_dir: &Path,
    config: &SweepConfig,
    stats: &mut SweepStats,
) -> Result<()> {
    for kind in ["put", "multipart"] {
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
                config
                    .now_ms
                    .saturating_sub(path_age_ms(&path, config.now_ms).unwrap_or(0))
            });
            let staging_age = config.now_ms.saturating_sub(created_ms);
            if staging_age >= config.staging_expiry_ms
                && all_files_old_enough(&path, config.now_ms, config.staging_expiry_ms)
            {
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

async fn sweep_trash(trash_dir: &Path, config: &SweepConfig, stats: &mut SweepStats) -> Result<()> {
    let entries = match std::fs::read_dir(trash_dir) {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let old_enough = path_age_ms(&path, config.now_ms)? >= config.trash_expiry_ms;
        if !old_enough || !all_files_old_enough(&path, config.now_ms, config.trash_expiry_ms) {
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
                visibility_repair_batch_size: 100,
                visibility_repair_grace_period_ms: 1,
                staging_expiry_ms: 1_000,
                trash_expiry_ms: 1_000,
                now_ms: 10_000,
            },
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
                visibility_repair_batch_size: 100,
                visibility_repair_grace_period_ms: 1,
                staging_expiry_ms: 1_000,
                trash_expiry_ms: 1_000,
                now_ms: 10_000,
            },
        )
        .await
        .unwrap();

        assert_eq!(stats.staging_dirs_removed, 1);
        assert!(!staging_dir.exists());
    }

    #[tokio::test]
    async fn repairs_queued_sqlite_orphan_after_grace_period() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "key", b"hello", None, None, false)
            .await
            .unwrap();
        let read = store.read_object("bucket", "key").await.unwrap();
        let object_dir = read.object_dir.clone();
        tokio::fs::remove_file(object_dir.join("meta.json"))
            .await
            .unwrap();
        let index = store.index("bucket").await.unwrap();
        index
            .put(&crate::storage::index::ObjectIndexEntry {
                object_key: "key".to_string(),
                size: 5,
                etag: "etag".to_string(),
                last_modified_ms: 0,
            })
            .await
            .unwrap();
        index.enqueue_visibility_repair("key", 0).await.unwrap();

        let stats = sweep_bucket(
            &store,
            "bucket",
            &SweepConfig {
                visibility_repair_batch_size: 10,
                visibility_repair_grace_period_ms: 1,
                staging_expiry_ms: 1000,
                trash_expiry_ms: 1000,
                now_ms: 10_000,
            },
        )
        .await
        .unwrap();
        assert_eq!(stats.visibility_repairs_processed, 1);
        assert!(index.get("key").await.unwrap().is_none());
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
                visibility_repair_batch_size: 100,
                visibility_repair_grace_period_ms: 0,
                staging_expiry_ms: 1000,
                trash_expiry_ms: 0,
                now_ms: now_ms(),
            },
        )
        .await
        .unwrap();

        assert_eq!(stats.trash_dirs_removed, 1);
    }

    #[tokio::test]
    async fn visibility_repair_queue_drains_all_eligible_rows_in_batches() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        let index = store.index("bucket").await.unwrap();
        for i in 1..=5u32 {
            index
                .enqueue_visibility_repair(&format!("key-{i:02}"), 0)
                .await
                .unwrap();
        }

        let cfg = SweepConfig {
            visibility_repair_batch_size: 2,
            visibility_repair_grace_period_ms: 1,
            staging_expiry_ms: 1000,
            trash_expiry_ms: 1000,
            now_ms: now_ms(),
        };

        let stats = sweep_bucket(&store, "bucket", &cfg).await.unwrap();
        assert_eq!(stats.visibility_repairs_processed, 5);
    }
}
