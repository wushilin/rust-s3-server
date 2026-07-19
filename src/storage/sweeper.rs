//! Background maintenance — hygiene only, never correctness.
//!
//! Under the SQLite-as-truth design the request path is self-sufficient:
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
use super::staging::epoch_ms_from_staging_id;
use super::store::LocalObjectStore;
use super::time::now_ms;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepConfig {
    pub intent_batch_size: usize,
    /// Only intents older than this are resolved — an in-flight operation's
    /// intent must never be mistaken for an abandoned one.
    pub intent_grace_period_ms: i64,
    pub staging_expiry_ms: i64,
    pub trash_expiry_ms: i64,
    pub now_ms: i64,
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
            trash_expiry_ms: 24 * 60 * 60 * 1000,
            now_ms: now_ms(),
        }
    }
}

/// Runs all three maintenance purposes for one bucket. Retained as a
/// convenience (and for tests); the scheduler drives the three single-purpose
/// entry points below independently.
pub async fn sweep_bucket(
    store: &LocalObjectStore,
    bucket: &str,
    config: &SweepConfig,
) -> Result<SweepStats> {
    Ok(SweepStats {
        intents_resolved: resolve_intents_bucket(store, bucket, config).await?,
        staging_dirs_removed: delete_staging_bucket(store, bucket, config).await?,
        trash_dirs_removed: delete_trash_bucket(store, bucket, config).await?,
    })
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
    loop {
        let outcome = store
            .resolve_stale_intents(bucket, config.intent_grace_period_ms, batch_size)
            .await?;
        resolved += outcome.resolved;
        if outcome.selected < batch_size || outcome.failed > 0 {
            break;
        }
        yield_now().await;
    }
    Ok(resolved)
}

/// Purpose 2 — delete expired staging directories for one bucket. Returns the
/// number removed.
pub async fn delete_staging_bucket(
    store: &LocalObjectStore,
    bucket: &str,
    config: &SweepConfig,
) -> Result<usize> {
    let mut stats = SweepStats::default();
    let bucket_dir = store.layout().bucket_dir(bucket)?;
    sweep_staging(&bucket_dir.join("staging"), config, &mut stats).await?;
    Ok(stats.staging_dirs_removed)
}

/// Purpose 3 — delete expired trash directories for one bucket. Returns the
/// number removed.
pub async fn delete_trash_bucket(
    store: &LocalObjectStore,
    bucket: &str,
    config: &SweepConfig,
) -> Result<usize> {
    let mut stats = SweepStats::default();
    let bucket_dir = store.layout().bucket_dir(bucket)?;
    sweep_trash(&bucket_dir.join("trash"), config, &mut stats).await?;
    Ok(stats.trash_dirs_removed)
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
                intent_batch_size: 100,
                intent_grace_period_ms: 1,
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
                intent_batch_size: 100,
                intent_grace_period_ms: 1,
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
                trash_expiry_ms: 0,
                now_ms: now_ms(),
            },
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
            trash_expiry_ms: 1000,
            now_ms: now_ms(),
        };

        let stats = sweep_bucket(&store, "bucket", &cfg).await.unwrap();
        assert_eq!(stats.intents_resolved, 5);
        assert!(index
            .stale_intents(now_ms(), 0, 10)
            .await
            .unwrap()
            .is_empty());
    }
}
