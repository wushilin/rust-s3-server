//! Persistence for storage health scans — `<data_root>/scans.rocksdb`.
//!
//! Deliberately its own database, not a family inside `admin.rocksdb`: IAM is
//! exported and imported as one unit, and scan history has no business
//! travelling with it.
//!
//! ## Why two families
//!
//! A report's *summary* is small and bounded (per-bucket totals and counts). Its
//! *findings* are neither — a badly damaged store can produce millions. Keeping
//! them apart means listing history never materialises findings, and the scan
//! itself never accumulates them in memory: each finding is written as it is
//! discovered.
//!
//! | family     | key                                          | value            |
//! |------------|----------------------------------------------|------------------|
//! | `reports`  | report id                                    | summary JSON     |
//! | `findings` | `report_id ␟ kind ␟ anchor ␟ finding_id`     | [`Finding`] JSON |
//!
//! Report ids are `{epoch_ms:013}-{random}`, so RocksDB's bytewise order is
//! chronological and "newest first" is a reverse iterator. The findings key is
//! built so that every useful read is one forward scan from a prefix: all
//! findings of a report, or all findings of one kind within it, in a stable
//! order (by blob dir / object key) that paginates with a cursor. The random
//! id tail guarantees uniqueness — findings have no natural key, since a blob
//! dir with no `meta.json` has neither an object key nor a row.
//!
//! Values are self-versioned JSON carrying `"v"`, and a value stamped newer
//! than this build understands is rejected rather than misread — the same
//! fail-closed rule as the object index and the IAM store.

use std::path::Path;
use std::sync::Arc;

use rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options,
    WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};

use crate::storage::errors::{Result, StorageError};
use crate::storage::scan::{BucketReport, Finding, FindingState, FINDING_VERSION};
use crate::storage::time::now_ms;

type Db = DBWithThreadMode<MultiThreaded>;

const CF_REPORTS: &str = "reports";
const CF_FINDINGS: &str = "findings";

/// Key segment separator: ASCII unit separator. Object keys are the only
/// segment that could contain one, and it is the last sort segment before the
/// unique id, so a stray separator can shuffle two findings of the same kind
/// but can never break a prefix scan.
const SEP: u8 = 0x1f;

/// Highest report summary version this build can read.
pub const REPORT_VERSION: u32 = 1;

fn report_version() -> u32 {
    REPORT_VERSION
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanStatus {
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl ScanStatus {
    pub fn is_terminal(self) -> bool {
        !matches!(self, ScanStatus::Running)
    }
}

/// The bounded half of a report: what the UI lists, and what it shows at the
/// top of a report page. Findings live in their own family.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanReport {
    #[serde(default = "report_version")]
    pub v: u32,
    pub id: String,
    pub started_at_ms: i64,
    #[serde(default)]
    pub finished_at_ms: i64,
    pub status: ScanStatus,
    /// Who asked for it.
    pub actor: String,
    /// Buckets the operator selected, in the order they were scanned.
    pub requested_buckets: Vec<String>,
    #[serde(default)]
    pub buckets: Vec<BucketReport>,
    /// Findings by kind across every bucket — exact, and cheap to render
    /// without touching the findings family.
    #[serde(default)]
    pub findings: std::collections::HashMap<String, u64>,
    #[serde(default)]
    pub findings_total: u64,
    /// Set when the run itself failed or was cancelled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ScanReport {
    pub fn new(id: String, actor: String, requested_buckets: Vec<String>) -> Self {
        Self {
            v: REPORT_VERSION,
            id,
            started_at_ms: now_ms(),
            finished_at_ms: 0,
            status: ScanStatus::Running,
            actor,
            requested_buckets,
            buckets: Vec::new(),
            findings: std::collections::HashMap::new(),
            findings_total: 0,
            error: None,
        }
    }

    /// Folds one bucket's result into the run totals.
    pub fn absorb(&mut self, report: BucketReport) {
        for (kind, count) in &report.findings {
            *self.findings.entry(kind.clone()).or_insert(0) += count;
            self.findings_total += count;
        }
        self.buckets.push(report);
    }

    pub fn logical_bytes(&self) -> u64 {
        self.buckets.iter().map(|b| b.logical_bytes).sum()
    }

    pub fn disk_bytes(&self) -> u64 {
        self.buckets.iter().map(|b| b.total_bytes()).sum()
    }

    pub fn objects(&self) -> u64 {
        self.buckets.iter().map(|b| b.objects_indexed).sum()
    }
}

/// A finding plus the storage key it lives under — the key is the cursor for
/// pagination and the handle for an in-place update after a repair.
#[derive(Debug, Clone)]
pub struct StoredFinding {
    pub key: String,
    pub finding: Finding,
}

#[derive(Clone)]
pub struct ScanStore {
    db: Arc<Db>,
}

impl std::fmt::Debug for ScanStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanStore").finish_non_exhaustive()
    }
}

fn cf<'a>(db: &'a Db, name: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'a>>> {
    db.cf_handle(name)
        .ok_or_else(|| StorageError::Db(format!("missing column family {name}")))
}

/// Scan history is derived data — it can always be produced again by running
/// another scan — so writes don't pay for a WAL fsync each.
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
        Err(err) => Err(StorageError::Db(format!("scan store task panicked: {err}"))),
    }
}

fn reject_newer(v: u32, max: u32, what: &str) -> Result<()> {
    if v > max {
        return Err(StorageError::Db(format!(
            "{what} value is v{v}, newer than this build understands (v{max}); refusing to read"
        )));
    }
    Ok(())
}

/// `{report_id} ␟ {kind} ␟ {anchor} ␟ {finding_id}`.
fn finding_key(report_id: &str, finding: &Finding) -> Vec<u8> {
    let mut key = Vec::with_capacity(report_id.len() + finding.anchor().len() + 40);
    key.extend_from_slice(report_id.as_bytes());
    key.push(SEP);
    key.extend_from_slice(finding.kind.as_str().as_bytes());
    key.push(SEP);
    key.extend_from_slice(finding.anchor().as_bytes());
    key.push(SEP);
    key.extend_from_slice(finding.id.as_bytes());
    key
}

/// Prefix for every finding of a report, optionally narrowed to one kind.
fn finding_prefix(report_id: &str, kind: Option<&str>) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(report_id.len() + 16);
    prefix.extend_from_slice(report_id.as_bytes());
    prefix.push(SEP);
    if let Some(kind) = kind {
        prefix.extend_from_slice(kind.as_bytes());
        prefix.push(SEP);
    }
    prefix
}

/// Report ids sort chronologically, so history is a reverse iteration.
pub fn new_report_id() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let suffix: String = (0..6)
        .map(|_| std::char::from_digit(rng.gen_range(0..16), 16).unwrap_or('0'))
        .collect();
    format!("{:013}-{suffix}", now_ms())
}

impl ScanStore {
    pub async fn open(data_root: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(data_root).await?;
        let db_path = data_root.join("scans.rocksdb");
        let db = blocking(move || {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            let cfs = [CF_REPORTS, CF_FINDINGS]
                .into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
            Ok(Db::open_cf_descriptors(&opts, &db_path, cfs)?)
        })
        .await?;
        Ok(Self { db: Arc::new(db) })
    }

    // ── reports ─────────────────────────────────────────────────────────────

    pub async fn put_report(&self, report: &ScanReport) -> Result<()> {
        let db = self.db.clone();
        let id = report.id.clone();
        let value = serde_json::to_vec(report)?;
        blocking(move || {
            let reports = cf(&db, CF_REPORTS)?;
            db.put_cf_opt(&reports, id.as_bytes(), value, &write_opts())?;
            Ok(())
        })
        .await
    }

    pub async fn get_report(&self, id: &str) -> Result<Option<ScanReport>> {
        let db = self.db.clone();
        let id = id.to_string();
        blocking(move || {
            let reports = cf(&db, CF_REPORTS)?;
            match db.get_cf(&reports, id.as_bytes())? {
                Some(value) => {
                    let report: ScanReport = serde_json::from_slice(&value)?;
                    reject_newer(report.v, REPORT_VERSION, "scan report")?;
                    Ok(Some(report))
                }
                None => Ok(None),
            }
        })
        .await
    }

    /// Newest first. Summaries only — findings are never touched.
    pub async fn list_reports(&self, limit: usize) -> Result<Vec<ScanReport>> {
        let db = self.db.clone();
        blocking(move || {
            let reports = cf(&db, CF_REPORTS)?;
            let mut out = Vec::new();
            for item in db.iterator_cf(&reports, IteratorMode::End) {
                if out.len() >= limit {
                    break;
                }
                let (key, value) = item?;
                match serde_json::from_slice::<ScanReport>(&value) {
                    Ok(report) if report.v <= REPORT_VERSION => out.push(report),
                    // One unreadable row must not blind the whole history.
                    _ => log::warn!(
                        "skipping unreadable scan report id={}",
                        String::from_utf8_lossy(&key)
                    ),
                }
            }
            Ok(out)
        })
        .await
    }

    /// Deletes a report and every finding under it. The findings are removed
    /// with an explicit ranged delete over the report's prefix, so a report
    /// with a million findings costs one batch, not a million round trips.
    pub async fn delete_report(&self, id: &str) -> Result<bool> {
        let db = self.db.clone();
        let id = id.to_string();
        blocking(move || {
            let reports = cf(&db, CF_REPORTS)?;
            let findings = cf(&db, CF_FINDINGS)?;
            let existed = db.get_cf(&reports, id.as_bytes())?.is_some();
            let mut batch = WriteBatch::default();
            batch.delete_cf(&reports, id.as_bytes());
            let from = finding_prefix(&id, None);
            let mut to = from.clone();
            // Exclusive upper bound: the prefix with its last byte bumped.
            *to.last_mut().expect("prefix is never empty") += 1;
            batch.delete_range_cf(&findings, &from, &to);
            db.write_opt(batch, &write_opts())?;
            Ok(existed)
        })
        .await
    }

    // ── findings ────────────────────────────────────────────────────────────

    /// Writes a batch of findings. Called by the scan's persister as they
    /// stream in, so nothing accumulates in memory.
    pub async fn append_findings(&self, report_id: &str, findings: Vec<Finding>) -> Result<()> {
        if findings.is_empty() {
            return Ok(());
        }
        let db = self.db.clone();
        let report_id = report_id.to_string();
        blocking(move || {
            let cf = cf(&db, CF_FINDINGS)?;
            let mut batch = WriteBatch::default();
            for finding in &findings {
                batch.put_cf(&cf, finding_key(&report_id, finding), serde_json::to_vec(finding)?);
            }
            db.write_opt(batch, &write_opts())?;
            Ok(())
        })
        .await
    }

    /// One page of a report's findings, optionally of a single kind, starting
    /// after `after` (an opaque cursor — the previous page's last key).
    pub async fn list_findings(
        &self,
        report_id: &str,
        kind: Option<&str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<StoredFinding>, Option<String>)> {
        let db = self.db.clone();
        let prefix = finding_prefix(report_id, kind);
        let after = after.map(str::to_string);
        blocking(move || {
            let cf = cf(&db, CF_FINDINGS)?;
            let start = after.clone().map(|v| v.into_bytes()).unwrap_or_else(|| prefix.clone());
            let mut out = Vec::new();
            let mut next = None;
            for item in db.iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward)) {
                let (key, value) = item?;
                if !key.starts_with(&prefix) {
                    break;
                }
                if after.as_deref().map(str::as_bytes) == Some(&key) {
                    continue; // `after` is exclusive
                }
                if out.len() >= limit {
                    // The cursor is the last *returned* key and is exclusive on
                    // the next call, so no row can be skipped between pages.
                    next = out.last().map(|f: &StoredFinding| f.key.clone());
                    break;
                }
                match serde_json::from_slice::<Finding>(&value) {
                    Ok(finding) if finding.v <= FINDING_VERSION => out.push(StoredFinding {
                        key: String::from_utf8_lossy(&key).into_owned(),
                        finding,
                    }),
                    _ => log::warn!(
                        "skipping unreadable finding key={}",
                        String::from_utf8_lossy(&key)
                    ),
                }
            }
            Ok((out, next))
        })
        .await
    }

    pub async fn get_finding(&self, key: &str) -> Result<Option<Finding>> {
        let db = self.db.clone();
        let key = key.to_string();
        blocking(move || {
            let cf = cf(&db, CF_FINDINGS)?;
            match db.get_cf(&cf, key.as_bytes())? {
                Some(value) => {
                    let finding: Finding = serde_json::from_slice(&value)?;
                    reject_newer(finding.v, FINDING_VERSION, "scan finding")?;
                    Ok(Some(finding))
                }
                None => Ok(None),
            }
        })
        .await
    }

    /// Records a repair's outcome on the finding itself, so an old report shows
    /// what is still outstanding rather than a frozen snapshot.
    pub async fn record_outcome(
        &self,
        key: &str,
        state: FindingState,
        outcome: String,
    ) -> Result<()> {
        let Some(mut finding) = self.get_finding(key).await? else {
            return Ok(());
        };
        finding.state = state;
        finding.outcome = Some(outcome);
        finding.repaired_at_ms = now_ms();
        let db = self.db.clone();
        let key = key.to_string();
        let value = serde_json::to_vec(&finding)?;
        blocking(move || {
            let cf = cf(&db, CF_FINDINGS)?;
            db.put_cf_opt(&cf, key.as_bytes(), value, &write_opts())?;
            Ok(())
        })
        .await
    }

    /// Counts a report's findings by state, for the "12 of 40 repaired" line.
    pub async fn state_counts(&self, report_id: &str) -> Result<std::collections::HashMap<String, u64>> {
        let db = self.db.clone();
        let prefix = finding_prefix(report_id, None);
        blocking(move || {
            let cf = cf(&db, CF_FINDINGS)?;
            let mut counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            for item in db.iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward)) {
                let (key, value) = item?;
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Ok(finding) = serde_json::from_slice::<Finding>(&value) {
                    let state = match finding.state {
                        FindingState::Open => "open",
                        FindingState::Repaired => "repaired",
                        FindingState::Stale => "stale",
                        FindingState::Failed => "failed",
                    };
                    *counts.entry(state.to_string()).or_insert(0) += 1;
                }
            }
            Ok(counts)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::scan::FindingKind;

    async fn store() -> (tempfile::TempDir, ScanStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = ScanStore::open(tmp.path()).await.unwrap();
        (tmp, store)
    }

    fn finding(bucket: &str, kind: FindingKind, anchor: &str) -> Finding {
        serde_json::from_value(serde_json::json!({
            "v": 1,
            "id": format!("{anchor}-id"),
            "bucket": bucket,
            "kind": kind,
            "blob_dir": anchor,
            "detail": "detail",
            "bytes": 10,
            "count": 1,
            "state": "open",
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn reports_list_newest_first_and_delete_takes_findings_with_them() {
        let (_tmp, store) = store().await;
        let mut ids = Vec::new();
        for n in 0..3 {
            let id = format!("{:013}-{n}", 1_700_000_000_000i64 + n);
            let report = ScanReport::new(id.clone(), "admin".into(), vec!["bkt".into()]);
            store.put_report(&report).await.unwrap();
            store
                .append_findings(&id, vec![finding("bkt", FindingKind::OrphanBlob, "objects/AAAA/V1")])
                .await
                .unwrap();
            ids.push(id);
        }

        let listed = store.list_reports(10).await.unwrap();
        assert_eq!(
            listed.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
            ids.iter().rev().cloned().collect::<Vec<_>>()
        );

        assert!(store.delete_report(&ids[1]).await.unwrap());
        assert!(store.get_report(&ids[1]).await.unwrap().is_none());
        let (findings, _) = store.list_findings(&ids[1], None, None, 10).await.unwrap();
        assert!(findings.is_empty(), "findings must go with their report");
        // Siblings are untouched.
        let (findings, _) = store.list_findings(&ids[0], None, None, 10).await.unwrap();
        assert_eq!(findings.len(), 1);
    }

    #[tokio::test]
    async fn findings_filter_by_kind_and_paginate() {
        let (_tmp, store) = store().await;
        let id = new_report_id();
        let mut batch = Vec::new();
        for n in 0..5 {
            batch.push(finding("bkt", FindingKind::OrphanBlob, &format!("objects/A/{n:02}")));
            batch.push(finding("bkt", FindingKind::MissingBlob, &format!("objects/B/{n:02}")));
        }
        store.append_findings(&id, batch).await.unwrap();

        let (all, _) = store.list_findings(&id, None, None, 100).await.unwrap();
        assert_eq!(all.len(), 10);

        let (orphans, _) = store
            .list_findings(&id, Some("orphan_blob"), None, 100)
            .await
            .unwrap();
        assert_eq!(orphans.len(), 5);
        assert!(orphans.iter().all(|f| f.finding.kind == FindingKind::OrphanBlob));
        // Anchor ordering is stable, which is what makes the cursor safe.
        assert_eq!(orphans[0].finding.blob_dir.as_deref(), Some("objects/A/00"));

        let (page, next) = store
            .list_findings(&id, Some("orphan_blob"), None, 2)
            .await
            .unwrap();
        assert_eq!(page.len(), 2);
        let (page2, _) = store
            .list_findings(&id, Some("orphan_blob"), next.as_deref(), 2)
            .await
            .unwrap();
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].finding.blob_dir.as_deref(), Some("objects/A/02"));
    }

    #[tokio::test]
    async fn outcomes_are_recorded_on_the_finding() {
        let (_tmp, store) = store().await;
        let id = new_report_id();
        store
            .append_findings(&id, vec![finding("bkt", FindingKind::OrphanBlob, "objects/A/1")])
            .await
            .unwrap();
        let (page, _) = store.list_findings(&id, None, None, 10).await.unwrap();
        let key = page[0].key.clone();

        store
            .record_outcome(&key, FindingState::Repaired, "moved to trash".into())
            .await
            .unwrap();
        let updated = store.get_finding(&key).await.unwrap().unwrap();
        assert_eq!(updated.state, FindingState::Repaired);
        assert_eq!(updated.outcome.as_deref(), Some("moved to trash"));
        assert!(updated.repaired_at_ms > 0);

        let counts = store.state_counts(&id).await.unwrap();
        assert_eq!(counts.get("repaired"), Some(&1));
    }

    #[tokio::test]
    async fn values_newer_than_this_build_are_rejected() {
        let (_tmp, store) = store().await;
        let id = new_report_id();
        let mut report = ScanReport::new(id.clone(), "admin".into(), vec![]);
        report.v = REPORT_VERSION + 1;
        store.put_report(&report).await.unwrap();
        assert!(store.get_report(&id).await.is_err());
        // …and a poisoned row is skipped by the listing rather than failing it.
        assert!(store.list_reports(10).await.unwrap().is_empty());
    }
}
