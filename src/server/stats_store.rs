//! Persistence for the Runtime Stats time-series — `<data_root>/stats.rocksdb`.
//!
//! Its own database, like [`scan_store`](super::scan_store): runtime samples
//! are derived, high-churn, and have no business travelling with the IAM export
//! or the object index.
//!
//! | family    | key                | value            |
//! |-----------|--------------------|------------------|
//! | `samples` | `{epoch_ms:013}`   | [`Sample`] JSON  |
//!
//! Keys are zero-padded epoch milliseconds, so RocksDB's bytewise order is
//! chronological: a time range is one seek, retention is one ranged delete, and
//! the UI's downsample is `buckets` cheap seeks rather than a full scan.

use std::path::Path;
use std::sync::Arc;

use rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options,
    WriteBatch, WriteOptions,
};

use super::sysstat::Sample;
use crate::storage::errors::{Result, StorageError};

type Db = DBWithThreadMode<MultiThreaded>;

const CF_SAMPLES: &str = "samples";

/// Zero-padded epoch-ms key. 13 digits covers dates well past the year 2286,
/// and fixed width is what makes bytewise order match chronological order.
fn key_for(ts_ms: i64) -> String {
    format!("{:013}", ts_ms.max(0))
}

fn parse_key(key: &[u8]) -> Option<i64> {
    std::str::from_utf8(key).ok()?.parse().ok()
}

#[derive(Clone)]
pub struct StatsStore {
    db: Arc<Db>,
}

impl std::fmt::Debug for StatsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatsStore").finish_non_exhaustive()
    }
}

fn cf<'a>(db: &'a Db, name: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'a>>> {
    db.cf_handle(name)
        .ok_or_else(|| StorageError::Db(format!("missing column family {name}")))
}

/// Samples are derived data regenerated every few seconds — a WAL fsync per
/// write would be pure cost for no durability benefit.
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
        Err(err) => Err(StorageError::Db(format!("stats store task panicked: {err}"))),
    }
}

impl StatsStore {
    pub async fn open(data_root: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(data_root).await?;
        let db_path = data_root.join("stats.rocksdb");
        let db = blocking(move || {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            let cfs =
                [CF_SAMPLES].map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
            Ok(Db::open_cf_descriptors(&opts, &db_path, cfs)?)
        })
        .await?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Stores one sample under its timestamp.
    pub async fn put(&self, ts_ms: i64, sample: &Sample) -> Result<()> {
        let db = self.db.clone();
        let key = key_for(ts_ms);
        let value = serde_json::to_vec(sample)?;
        blocking(move || {
            let cf = cf(&db, CF_SAMPLES)?;
            db.put_cf_opt(&cf, key.as_bytes(), value, &write_opts())?;
            Ok(())
        })
        .await
    }

    /// Downsamples `[start_ms, end_ms)` into exactly `buckets` slots by taking
    /// the **mean of every sample** that falls in each bucket (per field,
    /// ignoring nulls), or `None` for an empty bucket — which the UI draws as a
    /// gap. Averaging (rather than picking one representative sample) is what
    /// makes a rate like requests/sec or throughput read as the bucket's true
    /// average over a wide range, instead of one arbitrary 5-second snapshot.
    /// Costs one forward pass over the window's samples.
    pub async fn sample_series(
        &self,
        start_ms: i64,
        end_ms: i64,
        buckets: usize,
    ) -> Result<Vec<Option<Sample>>> {
        let db = self.db.clone();
        blocking(move || {
            let cf = cf(&db, CF_SAMPLES)?;
            let buckets = buckets.max(1);
            let span = (end_ms - start_ms).max(1) as i128;
            let mut sums = vec![[0f64; Sample::COLS]; buckets];
            let mut counts = vec![[0u32; Sample::COLS]; buckets];

            let start_key = key_for(start_ms);
            for item in
                db.iterator_cf(&cf, IteratorMode::From(start_key.as_bytes(), Direction::Forward))
            {
                let (k, v) = item?;
                let Some(ts) = parse_key(&k) else { continue };
                if ts >= end_ms {
                    break;
                }
                if ts < start_ms {
                    continue;
                }
                let bi = (((ts - start_ms) as i128 * buckets as i128) / span) as usize;
                let bi = bi.min(buckets - 1);
                if let Ok(sample) = serde_json::from_slice::<Sample>(&v) {
                    for (j, col) in sample.to_cols().into_iter().enumerate() {
                        if let Some(x) = col {
                            sums[bi][j] += x;
                            counts[bi][j] += 1;
                        }
                    }
                }
            }

            let out = (0..buckets)
                .map(|bi| {
                    if counts[bi].iter().all(|&c| c == 0) {
                        return None;
                    }
                    let mut cols = [None; Sample::COLS];
                    for j in 0..Sample::COLS {
                        if counts[bi][j] > 0 {
                            cols[j] = Some(sums[bi][j] / counts[bi][j] as f64);
                        }
                    }
                    Some(Sample::from_cols(cols))
                })
                .collect();
            Ok(out)
        })
        .await
    }

    /// Timestamps of the oldest and newest stored samples, or `None` when the
    /// store is empty. Used to clamp a requested window to where data actually
    /// exists, so a wide range doesn't render a mostly-empty chart.
    pub async fn extent(&self) -> Result<Option<(i64, i64)>> {
        let db = self.db.clone();
        blocking(move || {
            let cf = cf(&db, CF_SAMPLES)?;
            let mut it = db.raw_iterator_cf(&cf);
            it.seek_to_first();
            let Some(first) = it.key().and_then(parse_key) else {
                return Ok(None);
            };
            it.seek_to_last();
            let last = it.key().and_then(parse_key).unwrap_or(first);
            Ok(Some((first, last)))
        })
        .await
    }

    /// Removes every sample older than `before_ms` in a single ranged delete.
    pub async fn prune(&self, before_ms: i64) -> Result<()> {
        let db = self.db.clone();
        blocking(move || {
            let cf = cf(&db, CF_SAMPLES)?;
            let from = key_for(0);
            let to = key_for(before_ms); // exclusive upper bound
            let mut batch = WriteBatch::default();
            batch.delete_range_cf(&cf, from.as_bytes(), to.as_bytes());
            db.write_opt(batch, &write_opts())?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> (tempfile::TempDir, StatsStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = StatsStore::open(tmp.path()).await.unwrap();
        (tmp, store)
    }

    fn sample(cpu: f64) -> Sample {
        Sample { cpu_sys: Some(cpu), ..Default::default() }
    }

    #[tokio::test]
    async fn downsample_aligns_buckets_and_gaps_are_none() {
        let (_tmp, store) = store().await;
        // Samples at t=0,10,20,...,90 (ms), then a gap, then t=200.
        for t in (0..100).step_by(10) {
            store.put(t, &sample(t as f64)).await.unwrap();
        }
        store.put(200, &sample(200.0)).await.unwrap();

        // 10 buckets over [0,100): each 10ms wide, one sample each.
        let series = store.sample_series(0, 100, 10).await.unwrap();
        assert_eq!(series.len(), 10);
        assert!(series.iter().all(|s| s.is_some()));
        assert_eq!(series[0].as_ref().unwrap().cpu_sys, Some(0.0));
        assert_eq!(series[5].as_ref().unwrap().cpu_sys, Some(50.0));

        // A range covering the gap: buckets between 100 and 200 are empty.
        let series = store.sample_series(100, 300, 4).await.unwrap();
        // buckets: [100,150),[150,200),[200,250),[250,300)
        assert_eq!(series[0], None);
        assert_eq!(series[1], None);
        assert_eq!(series[2].as_ref().unwrap().cpu_sys, Some(200.0));
        assert_eq!(series[3], None);
    }

    #[tokio::test]
    async fn downsample_averages_samples_within_a_bucket() {
        let (_tmp, store) = store().await;
        // Three samples collapse into one bucket → the value is their mean, not
        // one arbitrary snapshot.
        store.put(0, &sample(10.0)).await.unwrap();
        store.put(3, &sample(20.0)).await.unwrap();
        store.put(6, &sample(60.0)).await.unwrap();
        let series = store.sample_series(0, 10, 1).await.unwrap();
        assert_eq!(series[0].as_ref().unwrap().cpu_sys, Some(30.0));
    }

    #[tokio::test]
    async fn prune_removes_only_old_samples() {
        let (_tmp, store) = store().await;
        for t in [10i64, 20, 30, 40, 50] {
            store.put(t, &sample(t as f64)).await.unwrap();
        }
        store.prune(30).await.unwrap(); // drop t < 30

        let series = store.sample_series(0, 60, 6).await.unwrap();
        // buckets of width 10: [0,10),[10,20),[20,30),[30,40),[40,50),[50,60)
        assert_eq!(series[0], None);
        assert_eq!(series[1], None); // t=10 pruned
        assert_eq!(series[2], None); // t=20 pruned
        assert_eq!(series[3].as_ref().unwrap().cpu_sys, Some(30.0)); // kept
        assert_eq!(series[4].as_ref().unwrap().cpu_sys, Some(40.0));
        assert_eq!(series[5].as_ref().unwrap().cpu_sys, Some(50.0));
    }

    #[tokio::test]
    async fn roundtrip_preserves_all_fields() {
        let (_tmp, store) = store().await;
        let s = Sample {
            cpu_sys: Some(1.0),
            cpu_proc: Some(2.0),
            mem_used: Some(3.0),
            mem_total: Some(4.0),
            mem_proc_rss: Some(5.0),
            disk_proc_r: Some(6.0),
            disk_proc_w: Some(7.0),
            disk_sys_r: Some(8.0),
            disk_sys_w: Some(9.0),
            net_in: Some(10.0),
            net_out: Some(11.0),
            qps: Some(12.0),
        };
        store.put(1_700_000_000_000, &s).await.unwrap();
        let series = store
            .sample_series(1_700_000_000_000, 1_700_000_000_001, 1)
            .await
            .unwrap();
        assert_eq!(series[0].as_ref(), Some(&s));
    }
}
