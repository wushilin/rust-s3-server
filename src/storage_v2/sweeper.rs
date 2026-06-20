//! Background sweeper: cleans up abandoned staging directories and orphaned objects.
//!
//! A staging directory is safe to delete only when BOTH its folder-name epoch
//! is older than the expiry window AND every file inside it has an mtime
//! older than the expiry window.  The second condition prevents racing against
//! an in-progress multipart upload whose upload-id happens to look old.
//!
//! The SQLite orphan walk is incremental: each call to [`sweep_bucket`] processes
//! at most `max_objects` entries starting after the provided `after` cursor.
//! When the returned [`SweepStats::pass_complete`] is false the caller should
//! resume from [`SweepStats::last_sqlite_key`] on the next tick.

use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use tokio::task::yield_now;

use super::errors::Result;
use super::staging::epoch_ms_from_staging_id;
use super::store::LocalObjectStore;
use super::time::now_ms;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepConfig {
    pub max_objects: usize,
    pub orphan_grace_period_ms: i64,
    pub staging_expiry_ms: i64,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepStats {
    pub sqlite_orphans_removed: usize,
    pub physical_orphans_removed: usize,
    pub staging_dirs_removed: usize,
    pub fanout_dirs_removed: usize,
    /// `true` when the entire bucket was processed in this call (SQLite walk
    /// completed + physical orphan walk + staging sweep).  When `false`, the
    /// SQLite walk hit the batch limit and the caller should resume from
    /// [`last_sqlite_key`] on the next tick.
    pub pass_complete: bool,
    /// The last SQLite key visited in this call.  Used as the resume cursor
    /// when `pass_complete` is false.  `None` if no entries were visited.
    pub last_sqlite_key: Option<String>,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            max_objects: 100,
            orphan_grace_period_ms: 5 * 60 * 1000,
            staging_expiry_ms: 24 * 60 * 60 * 1000,
            now_ms: now_ms(),
        }
    }
}

/// Sweeps one bucket.
///
/// `after` is the resume cursor from the previous call (`None` = start from
/// the beginning).  Returns [`SweepStats`] containing a new cursor when the
/// batch limit was hit, or `pass_complete = true` when the full sweep finished.
pub async fn sweep_bucket(
    store: &LocalObjectStore,
    bucket: &str,
    config: &SweepConfig,
    after: Option<&str>,
) -> Result<SweepStats> {
    let mut stats = SweepStats::default();
    let index = store.index(bucket).await?;

    // Fetch one extra entry so we can detect whether there are more after the batch.
    let entries = index
        .all_entries_after(after, (config.max_objects + 1) as i64)
        .await?;
    let has_more_sqlite = entries.len() > config.max_objects;

    for entry in entries.iter().take(config.max_objects) {
        stats.last_sqlite_key = Some(entry.object_key.clone());
        let object_path = store.layout().object_path(bucket, &entry.object_key)?;
        if !object_path.meta_path.exists()
            && config.now_ms.saturating_sub(entry.last_modified_ms) >= config.orphan_grace_period_ms
        {
            index.delete(&entry.object_key).await?;
            stats.sqlite_orphans_removed += 1;
            log::info!(
                "sweeper removed sqlite orphan bucket={} key={} physical_id={}",
                bucket,
                entry.object_key,
                entry.physical_id,
            );
            yield_now().await;
        }
    }

    if has_more_sqlite {
        // Batch limit hit — caller should resume from last_sqlite_key next tick.
        log::info!(
            "sweeper sqlite scan yielded bucket={} visited={} next_after={}",
            bucket,
            config.max_objects,
            stats.last_sqlite_key.as_deref().unwrap_or("-"),
        );
        return Ok(stats);
    }

    // SQLite walk complete; run the physical orphan walk and staging sweep to
    // completion.  These secondary sweeps don't have their own cursor: physical
    // orphans are rare (only appear after a crash), and the staging walk is fast.
    // We do NOT apply the max_objects batch limit here — doing so would cause the
    // sweeper to stall indefinitely on a large healthy bucket (same SQLite cursor
    // re-enters, SQLite portion completes instantly, physical portion hits limit
    // every tick without making progress).
    let bucket_dir = store.layout().bucket_dir(bucket)?;
    sweep_physical_orphans(
        store,
        bucket,
        &bucket_dir.join("objects"),
        config,
        &mut stats,
    )
    .await?;
    sweep_empty_fanout_dirs(
        store,
        bucket,
        &bucket_dir.join("objects"),
        config,
        &mut stats,
    )
    .await?;
    sweep_staging(&bucket_dir.join("staging"), config, &mut stats).await?;
    stats.pass_complete = true;
    Ok(stats)
}

async fn sweep_physical_orphans(
    store: &LocalObjectStore,
    bucket: &str,
    objects_dir: &Path,
    config: &SweepConfig,
    stats: &mut SweepStats,
) -> Result<()> {
    for dir in leaf_dirs(objects_dir) {
        if relative_depth(objects_dir, &dir) < 5 {
            continue;
        }
        if dir.join("meta.json").exists() {
            continue;
        }
        if path_age_ms(&dir, config.now_ms)? < config.orphan_grace_period_ms {
            continue;
        }
        let Some(top) = top_fanout_segment(objects_dir, &dir) else {
            continue;
        };
        let _fanout_guard = store.fanout_shard_write_lock(bucket, &top).await;
        if dir.join("meta.json").exists() || !dir.is_dir() {
            continue;
        }
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => {
                stats.physical_orphans_removed += 1;
                log::info!(
                    "sweeper removed physical orphan bucket={} fanout={} dir={}",
                    bucket,
                    top,
                    dir.display()
                );
            }
            Err(err) => {
                log::warn!(
                    "sweeper failed to remove physical orphan dir={} error={}",
                    dir.display(),
                    err,
                );
            }
        }
        yield_now().await;
    }
    Ok(())
}

async fn sweep_empty_fanout_dirs(
    store: &LocalObjectStore,
    bucket: &str,
    objects_dir: &Path,
    config: &SweepConfig,
    stats: &mut SweepStats,
) -> Result<()> {
    let Ok(top_entries) = std::fs::read_dir(objects_dir) else {
        return Ok(());
    };
    for top_entry in top_entries.flatten() {
        let top_dir = top_entry.path();
        if !top_dir.is_dir() {
            continue;
        }
        let Some(top) = top_dir
            .file_name()
            .and_then(|v| v.to_str())
            .map(str::to_string)
        else {
            continue;
        };
        let _fanout_guard = store.fanout_shard_write_lock(bucket, &top).await;
        let mut parents_made_empty = HashSet::new();
        let mut removed_in_shard = 0usize;
        loop {
            let mut removed_in_pass = false;
            let mut dirs = Vec::new();
            collect_dirs_postorder(&top_dir, &mut dirs);
            for dir in dirs {
                let old_enough = path_age_ms(&dir, config.now_ms)? >= config.orphan_grace_period_ms;
                if !old_enough && !parents_made_empty.contains(&dir) {
                    continue;
                }
                if !is_empty_dir(&dir) {
                    continue;
                }
                match tokio::fs::remove_dir(&dir).await {
                    Ok(()) => {
                        removed_in_pass = true;
                        stats.fanout_dirs_removed += 1;
                        if let Some(parent) = dir.parent() {
                            if parent != objects_dir {
                                parents_made_empty.insert(parent.to_path_buf());
                            }
                        }
                        log::info!(
                            "sweeper removed empty fanout dir bucket={} fanout={} dir={}",
                            bucket,
                            top,
                            dir.display()
                        );
                        removed_in_shard += 1;
                        if removed_in_shard % 100 == 0 {
                            log::info!(
                                "sweeper empty fanout cleanup yielded bucket={} fanout={} removed_in_shard={}",
                                bucket,
                                top,
                                removed_in_shard,
                            );
                            yield_now().await;
                        }
                    }
                    Err(err)
                        if matches!(
                            err.kind(),
                            ErrorKind::NotFound | ErrorKind::DirectoryNotEmpty
                        ) => {}
                    Err(err) => {
                        log::warn!(
                            "sweeper failed to remove empty fanout dir bucket={} fanout={} dir={} error={}",
                            bucket,
                            top,
                            dir.display(),
                            err,
                        );
                    }
                }
            }
            if !removed_in_pass {
                break;
            }
        }
        if removed_in_shard > 0 {
            log::info!(
                "sweeper empty fanout cleanup shard complete bucket={} fanout={} removed_in_shard={}",
                bucket,
                top,
                removed_in_shard,
            );
            yield_now().await;
        }
    }
    Ok(())
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

fn leaf_dirs(root: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    collect_leaf_dirs(root, &mut result);
    result
}

fn collect_leaf_dirs(path: &Path, result: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    let mut has_child_dir = false;
    for entry in entries.flatten() {
        let child = entry.path();
        if child.is_dir() {
            has_child_dir = true;
            collect_leaf_dirs(&child, result);
        }
    }
    if !has_child_dir && path.is_dir() {
        result.push(path.to_path_buf());
    }
}

fn collect_dirs_postorder(path: &Path, result: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let child = entry.path();
        if child.is_dir() {
            collect_dirs_postorder(&child, result);
        }
    }
    if path.is_dir() {
        result.push(path.to_path_buf());
    }
}

fn top_fanout_segment(objects_dir: &Path, path: &Path) -> Option<String> {
    path.strip_prefix(objects_dir)
        .ok()?
        .components()
        .next()
        .map(|v| v.as_os_str().to_string_lossy().to_string())
}

fn relative_depth(root: &Path, path: &Path) -> usize {
    path.strip_prefix(root)
        .map(|relative| relative.components().count())
        .unwrap_or(0)
}

fn is_empty_dir(path: &Path) -> bool {
    match std::fs::read_dir(path) {
        Ok(mut entries) => entries.next().is_none(),
        Err(_) => false,
    }
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
    use crate::storage_v2::index::ObjectIndexEntry;

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
                max_objects: 100,
                orphan_grace_period_ms: 1,
                staging_expiry_ms: 1_000,
                now_ms: 10_000,
            },
            None,
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
                max_objects: 100,
                orphan_grace_period_ms: 1,
                staging_expiry_ms: 1_000,
                now_ms: 10_000,
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(stats.staging_dirs_removed, 1);
        assert!(!staging_dir.exists());
    }

    #[tokio::test]
    async fn removes_sqlite_orphan_after_grace_period() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "key", b"hello", None, false)
            .await
            .unwrap();
        let object_path = store.layout().object_path("bucket", "key").unwrap();
        tokio::fs::remove_file(&object_path.meta_path)
            .await
            .unwrap();
        let index = store.index("bucket").await.unwrap();
        index
            .put(&ObjectIndexEntry {
                object_key: "key".to_string(),
                physical_id: object_path.physical_id,
                size: 5,
                etag: "etag".to_string(),
                last_modified_ms: 0,
            })
            .await
            .unwrap();

        let stats = sweep_bucket(
            &store,
            "bucket",
            &SweepConfig {
                max_objects: 10,
                orphan_grace_period_ms: 1,
                staging_expiry_ms: 1000,
                now_ms: 10_000,
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(stats.sqlite_orphans_removed, 1);
        assert!(index.get("key").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn removes_empty_fanout_dirs_after_object_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "folder/test.txt", b"hello", None, false)
            .await
            .unwrap();
        let object_path = store
            .layout()
            .object_path("bucket", "folder/test.txt")
            .unwrap();
        let fanout_dirs = object_path
            .object_dir
            .ancestors()
            .skip(1)
            .take(4)
            .map(Path::to_path_buf)
            .collect::<Vec<_>>();

        store
            .delete_object("bucket", "folder/test.txt")
            .await
            .unwrap();
        assert_eq!(fanout_dirs.len(), 4);
        for dir in &fanout_dirs {
            assert!(
                dir.exists(),
                "expected fanout dir to exist: {}",
                dir.display()
            );
        }

        let stats = sweep_bucket(
            &store,
            "bucket",
            &SweepConfig {
                max_objects: 100,
                orphan_grace_period_ms: 0,
                staging_expiry_ms: 1000,
                now_ms: now_ms(),
            },
            None,
        )
        .await
        .unwrap();

        assert!(
            stats.fanout_dirs_removed >= 4,
            "expected to remove the four empty fanout dirs, got {stats:?}"
        );
        for dir in &fanout_dirs {
            assert!(
                !dir.exists(),
                "expected fanout dir to be removed: {}",
                dir.display()
            );
        }
    }

    #[tokio::test]
    async fn cursor_advances_across_ticks() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        // Put 5 objects; sweep with max_objects=2 so it takes multiple ticks.
        for i in 1..=5u32 {
            store
                .put_object("bucket", &format!("key-{i:02}"), b"x", None, false)
                .await
                .unwrap();
        }

        let cfg = SweepConfig {
            max_objects: 2,
            orphan_grace_period_ms: 1,
            staging_expiry_ms: 1000,
            now_ms: now_ms(),
        };

        // Tick 1: after=None → processes key-01, key-02; has_more=true
        let s1 = sweep_bucket(&store, "bucket", &cfg, None).await.unwrap();
        assert!(!s1.pass_complete);
        assert_eq!(s1.last_sqlite_key.as_deref(), Some("key-02"));

        // Tick 2: after=key-02 → processes key-03, key-04; has_more=true
        let s2 = sweep_bucket(&store, "bucket", &cfg, s1.last_sqlite_key.as_deref())
            .await
            .unwrap();
        assert!(!s2.pass_complete);
        assert_eq!(s2.last_sqlite_key.as_deref(), Some("key-04"));

        // Tick 3: after=key-04 → processes key-05; has_more=false → pass_complete
        let s3 = sweep_bucket(&store, "bucket", &cfg, s2.last_sqlite_key.as_deref())
            .await
            .unwrap();
        assert!(s3.pass_complete);
        assert_eq!(s3.last_sqlite_key.as_deref(), Some("key-05"));
    }
}
