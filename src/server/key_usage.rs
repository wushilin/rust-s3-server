//! "Last used" tracking for API access keys — `<data_root>/key_usage.rocksdb`.
//!
//! Its own database, like [`stats_store`](super::stats_store): usage stamps are
//! derived, high-churn, and have no business travelling with the IAM export.
//! Keeping them out of `admin.rocksdb` also means a request never touches
//! primary data, and that built-in (config-file) keys — which have no IAM row
//! at all — are tracked exactly like runtime-issued ones.
//!
//! | family  | key           | value             |
//! |---------|---------------|-------------------|
//! | `usage` | access-key id | [`KeyUsage`] JSON |
//!
//! The request path only ever touches an in-memory map ([`record`] is
//! synchronous and lock-cheap, so SigV4 validation stays synchronous). A
//! background flusher persists the entries that changed every
//! [`FLUSH_INTERVAL`], and once more at shutdown — a crash loses at most that
//! window of stamps, never anything that matters.
//!
//! [`record`]: KeyUsageStore::record

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded, Options, WriteBatch,
    WriteOptions,
};
use serde::{Deserialize, Serialize};

use crate::storage::errors::{Result, StorageError};

type Db = DBWithThreadMode<MultiThreaded>;

const CF_USAGE: &str = "usage";

/// How often changed stamps are written out.
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);

/// When and from where an access key last authenticated a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyUsage {
    #[serde(default)]
    pub last_used_at_ms: i64,
    /// Client IP address (see `auth::client_ip` for how proxies are handled).
    #[serde(default)]
    pub last_used_from: String,
}

#[derive(Debug)]
struct Entry {
    usage: KeyUsage,
    /// Changed since the last flush.
    dirty: bool,
}

#[derive(Clone)]
pub struct KeyUsageStore {
    db: Arc<Db>,
    entries: Arc<Mutex<HashMap<String, Entry>>>,
}

impl std::fmt::Debug for KeyUsageStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyUsageStore").finish_non_exhaustive()
    }
}

fn cf<'a>(db: &'a Db, name: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'a>>> {
    db.cf_handle(name)
        .ok_or_else(|| StorageError::Db(format!("missing column family {name}")))
}

/// Usage stamps are advisory — a WAL fsync per flush would be pure cost.
fn write_opts() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(false);
    opts
}

async fn blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(err) => Err(StorageError::Db(format!("key usage task panicked: {err}"))),
    }
}

impl KeyUsageStore {
    /// Opens (creating if absent) `<data_root>/key_usage.rocksdb` and loads
    /// every stamp into memory. The family holds one small row per access key.
    pub async fn open(data_root: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(data_root).await?;
        let db_path = data_root.join("key_usage.rocksdb");
        let (db, entries) = blocking(move || {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            let cfs = [CF_USAGE].map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
            let db = Db::open_cf_descriptors(&opts, &db_path, cfs)?;
            let mut entries = HashMap::new();
            {
                let usage_cf = cf(&db, CF_USAGE)?;
                for item in db.iterator_cf(&usage_cf, IteratorMode::Start) {
                    let (key, value) = item?;
                    // A row that no longer parses is only a lost stamp.
                    let Ok(usage) = serde_json::from_slice::<KeyUsage>(&value) else {
                        continue;
                    };
                    entries.insert(
                        String::from_utf8_lossy(&key).into_owned(),
                        Entry { usage, dirty: false },
                    );
                }
            }
            Ok((db, entries))
        })
        .await?;
        Ok(Self {
            db: Arc::new(db),
            entries: Arc::new(Mutex::new(entries)),
        })
    }

    /// Stamps `access_key` as used just now from `from`. Sync and memory-only —
    /// called on the request path after a signature verifies.
    pub fn record(&self, access_key: &str, at_ms: i64, from: &str) {
        let mut entries = self.entries.lock().unwrap();
        match entries.get_mut(access_key) {
            Some(entry) => {
                entry.usage.last_used_at_ms = at_ms;
                if entry.usage.last_used_from != from {
                    entry.usage.last_used_from = from.to_string();
                }
                entry.dirty = true;
            }
            None => {
                entries.insert(
                    access_key.to_string(),
                    Entry {
                        usage: KeyUsage {
                            last_used_at_ms: at_ms,
                            last_used_from: from.to_string(),
                        },
                        dirty: true,
                    },
                );
            }
        }
    }

    /// The latest stamp for `access_key`, if it has ever been used.
    pub fn get(&self, access_key: &str) -> Option<KeyUsage> {
        self.entries
            .lock()
            .unwrap()
            .get(access_key)
            .map(|entry| entry.usage.clone())
    }

    /// Drops the stamp of a deleted access key, in memory and on disk.
    pub async fn forget(&self, access_key: &str) -> Result<()> {
        self.entries.lock().unwrap().remove(access_key);
        let db = self.db.clone();
        let access_key = access_key.to_string();
        blocking(move || {
            let usage_cf = cf(&db, CF_USAGE)?;
            db.delete_cf_opt(&usage_cf, access_key.as_bytes(), &write_opts())?;
            Ok(())
        })
        .await
    }

    /// Writes every stamp that changed since the last flush, in one batch.
    pub async fn flush(&self) -> Result<()> {
        let pending: Vec<(String, Vec<u8>)> = {
            let mut entries = self.entries.lock().unwrap();
            entries
                .iter_mut()
                .filter(|(_, entry)| entry.dirty)
                .map(|(access_key, entry)| {
                    entry.dirty = false;
                    let value = serde_json::to_vec(&entry.usage).expect("key usage serializes");
                    (access_key.clone(), value)
                })
                .collect()
        };
        if pending.is_empty() {
            return Ok(());
        }
        let db = self.db.clone();
        blocking(move || {
            let usage_cf = cf(&db, CF_USAGE)?;
            let mut batch = WriteBatch::default();
            for (access_key, value) in &pending {
                batch.put_cf(&usage_cf, access_key.as_bytes(), value);
            }
            db.write_opt(batch, &write_opts())?;
            Ok(())
        })
        .await
    }

    /// Flushes every [`FLUSH_INTERVAL`] until `shutdown` fires. The caller
    /// flushes once more after the server has drained, so stamps recorded
    /// during the drain are not lost.
    pub fn spawn_flusher(&self, shutdown: tokio_util::sync::CancellationToken) {
        let store = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tick.tick() => {
                        if let Err(err) = store.flush().await {
                            log::warn!("could not persist access key usage: {err}");
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stamps_survive_a_flush_and_reopen_and_unflushed_ones_do_not() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = KeyUsageStore::open(dir.path()).await.unwrap();
            assert!(store.get("RSAK1").is_none());
            store.record("RSAK1", 1_000, "10.0.0.1");
            store.record("RSAK1", 2_000, "10.0.0.2");
            store.flush().await.unwrap();
            // Recorded after the flush and never written out.
            store.record("RSAK2", 3_000, "10.0.0.3");
            assert_eq!(store.get("RSAK2").unwrap().last_used_at_ms, 3_000);
        }
        let store = KeyUsageStore::open(dir.path()).await.unwrap();
        assert_eq!(
            store.get("RSAK1"),
            Some(KeyUsage {
                last_used_at_ms: 2_000,
                last_used_from: "10.0.0.2".to_string(),
            })
        );
        assert!(store.get("RSAK2").is_none());
    }

    #[tokio::test]
    async fn forget_removes_the_stamp_for_good() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = KeyUsageStore::open(dir.path()).await.unwrap();
            store.record("RSAK1", 1_000, "10.0.0.1");
            store.flush().await.unwrap();
            store.forget("RSAK1").await.unwrap();
            assert!(store.get("RSAK1").is_none());
            // Nothing dirty is left behind to resurrect the row.
            store.flush().await.unwrap();
        }
        let store = KeyUsageStore::open(dir.path()).await.unwrap();
        assert!(store.get("RSAK1").is_none());
    }
}
