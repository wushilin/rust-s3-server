//! Storage health scan — the expensive, admin-triggered audit of what is
//! actually on disk versus what the catalog claims.
//!
//! The request path never scans the tree; this is the one place that does. A
//! scan is strictly **read-only**: it produces findings, and every repair is a
//! separate, explicit action the operator takes afterwards (see
//! [`LocalObjectStore::repair_finding`]).
//!
//! ## What "correct" means here
//!
//! The index row is authoritative for **existence**. A blob dir under
//! `objects/` that no row references was never made visible to a client — the
//! index flip *is* the commit gate, so an uncommitted publish means the client
//! was never told the write succeeded and must be assumed to have retried. Such
//! a dir is garbage regardless of how new it looks; it is never adopted or
//! promoted over the indexed one. (Wholesale adoption is what a full index
//! rebuild does, and it only makes sense precisely because there the index is
//! gone and there is nothing left to trust.)
//!
//! `meta.json` is authoritative for **attributes**. It is what `read_object`
//! serves, so when a row's size/etag/last-modified disagree with it, listings
//! are lying about bytes a GET would return, and the fix is to resync the row.
//!
//! ## One walk, four answers
//!
//! Disk usage, corruption, both reconciliation directions and empty-fanout
//! accounting all fall out of a single pass, because they all need the same
//! thing: every `meta.json` parsed and every part file stat'ed.
//!
//! 1. **index sweep** — sum row sizes, and record `hash64(blob_dir) →
//!    fingerprint(size, etag, last_modified)` for every row.
//! 2. **tree walk** — parse each `meta.json`, stat each part, and classify the
//!    dir by looking up (and *removing*) its hash: absent → orphan candidate;
//!    present but fingerprint differs → drift candidate.
//! 3. **reverse sweep** — whatever is still in the map is a row the walk never
//!    saw on disk (missing blob), so no second set is needed.
//! 4. **verification** — every candidate is re-checked *exactly* against the
//!    index by object key. This is what makes the hashing safe (a collision
//!    resolves to "not a finding") and what filters out the false positives a
//!    live server generates: an in-flight publish is indistinguishable from an
//!    orphan until you look it up. Dirs younger than [`SCAN_GRACE_MS`] are
//!    skipped for the same reason.
//!
//! ## Memory
//!
//! Nothing per-finding is accumulated: findings are pushed down a bounded
//! channel as they are discovered and persisted by the caller, so a store with
//! a million broken objects costs the same resident memory as a healthy one
//! (and the channel's backpressure throttles the walk if the writer lags).
//!
//! The one unavoidable per-object cost is the reconciliation map — two `u64`s
//! per live object, roughly 24 MB per million with hashing overhead, versus the
//! ~120 MB the `blob_dir` strings themselves would take. Reconciliation is
//! inherently a set-difference over the whole bucket; this is the cheap way to
//! do it in one pass.
//!
//! The walk yields after every directory read and every metadata parse, and
//! checks the cancellation token at every item, so a scan of a huge bucket
//! stays a background citizen and stops promptly when the operator says so.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tokio::task::yield_now;
use tokio_util::sync::CancellationToken;

use super::errors::{Result, StorageError};
use super::index::ObjectRecord;
use super::metadata::ObjectMeta;
use super::store::{move_object_dir_to_trash, LocalObjectStore};
use super::time::now_ms;

/// Blob dirs and index rows younger than this are never reported. A publish
/// that is mid-flight right now looks exactly like an orphan; one minute is far
/// longer than the window between a rename into the live tree and the row
/// commit that follows it.
pub const SCAN_GRACE_MS: i64 = 60_000;

/// Index rows read per batch during the sweeps. Bounds peak memory and gives a
/// natural yield point.
const INDEX_BATCH: i64 = 1_000;

/// Cap on the stale-intent count sampled for the hygiene summary.
const STALE_INTENT_SAMPLE: i64 = 10_000;

// ── findings ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    /// A blob dir on disk that no index row references, and whose object key
    /// has no row at all — an uncommitted publish, or index loss.
    OrphanBlob,
    /// A blob dir on disk whose object key *does* have a row, pointing
    /// somewhere else — a stale copy left by a crashed overwrite or migration.
    SupersededBlob,
    /// An index row whose blob dir is gone (or has no `meta.json`) — the object
    /// lists but cannot be read.
    MissingBlob,
    /// A blob dir with no usable `meta.json` — absent entirely, or present but
    /// unparseable. It has no discoverable object key, so it can only be
    /// identified by its path: nothing can be looked up, locked, or resynced
    /// for it. A publish only ever renames a *complete* staged dir into the
    /// live tree, so this can never be a committed object mid-flight.
    UnreadableBlob,
    /// A blob dir whose `meta.json` parses but whose parts are missing, short,
    /// or don't add up to the declared size.
    CorruptObject,
    /// Row and `meta.json` disagree on size/etag/last-modified.
    IndexDrift,
    /// Empty fanout directories under `objects/` — wasted inodes, no data.
    /// Aggregated into one finding per bucket.
    EmptyFanout,
}

impl FindingKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FindingKind::OrphanBlob => "orphan_blob",
            FindingKind::SupersededBlob => "superseded_blob",
            FindingKind::MissingBlob => "missing_blob",
            FindingKind::UnreadableBlob => "unreadable_blob",
            FindingKind::CorruptObject => "corrupt_object",
            FindingKind::IndexDrift => "index_drift",
            FindingKind::EmptyFanout => "empty_fanout",
        }
    }

    /// Repairs offered for this kind, recommended one first.
    pub fn actions(self) -> &'static [RepairAction] {
        match self {
            // Never "adopt": an unreferenced dir was never acknowledged to a
            // client. Recovering a lost index is `rebuild_index`'s job.
            FindingKind::OrphanBlob | FindingKind::SupersededBlob => &[RepairAction::TrashBlob],
            // No object key exists to delete a row by; the bytes are all there
            // is to act on. If a row *does* point here, that row shows up
            // separately as a missing blob, with its own row-deleting repair.
            FindingKind::UnreadableBlob => &[RepairAction::TrashBlob],
            FindingKind::MissingBlob => &[RepairAction::DeleteRow],
            FindingKind::CorruptObject => &[
                RepairAction::Quarantine,
                RepairAction::TrashBlob,
                RepairAction::DeleteRow,
            ],
            FindingKind::IndexDrift => &[RepairAction::ResyncRow],
            FindingKind::EmptyFanout => &[RepairAction::ReclaimEmptyDirs],
        }
    }

    pub fn allows(self, action: RepairAction) -> bool {
        self.actions().contains(&action)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingState {
    /// Not acted on yet.
    Open,
    /// The repair ran and changed the world.
    Repaired,
    /// Re-verification found the finding no longer holds — the object was
    /// rewritten, deleted, or already fixed. Nothing was touched.
    Stale,
    /// The repair was attempted and failed.
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairAction {
    /// Move the blob dir to `trash/` (recoverable for the trash grace window).
    TrashBlob,
    /// Delete the index row through the normal delete path, keeping the object
    /// counter honest.
    DeleteRow,
    /// Both: trash the bytes and drop the row.
    Quarantine,
    /// Rewrite the row's attributes from `meta.json`.
    ResyncRow,
    /// Run one empty-fanout reclamation pass over the bucket.
    ReclaimEmptyDirs,
}

impl RepairAction {
    pub fn as_str(self) -> &'static str {
        match self {
            RepairAction::TrashBlob => "trash_blob",
            RepairAction::DeleteRow => "delete_row",
            RepairAction::Quarantine => "quarantine",
            RepairAction::ResyncRow => "resync_row",
            RepairAction::ReclaimEmptyDirs => "reclaim_empty_dirs",
        }
    }
}

/// One thing wrong with the store, with enough context to re-verify it later.
///
/// This is the JSON payload of one row in the findings column family. It
/// carries the same explicit `"v"` version field as every other persisted value
/// in the codebase, so fields can be added freely and a value written by a
/// newer build is rejected rather than misread. Rows are updated in place as
/// they are repaired, so an old report always shows what is still outstanding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    #[serde(default = "finding_version")]
    pub v: u32,
    /// Random, unique within a report — findings have no natural key (a
    /// blob dir with no meta.json has neither object key nor row, and the
    /// empty-fanout finding has neither).
    pub id: String,
    pub bucket: String,
    pub kind: FindingKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    /// Blob dir relative to the bucket dir, e.g. `objects/AB12/V1AB3F7C_9F00`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_dir: Option<String>,
    pub detail: String,
    /// Bytes this finding accounts for (reclaimable, or lost).
    #[serde(default)]
    pub bytes: u64,
    /// How many underlying items this finding represents (>1 only for the
    /// aggregated empty-fanout finding).
    #[serde(default = "one")]
    pub count: u64,
    pub state: FindingState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(default)]
    pub repaired_at_ms: i64,
    /// Per-kind extras: whatever that finding type can usefully say beyond the
    /// common fields — the live dir a superseded copy lost to, the exact part
    /// problems, the two sides of a drift. Deliberately open-ended, so a kind
    /// can be enriched (and new kinds added) without touching the schema or the
    /// storage layer. The UI renders it generically.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub data: serde_json::Map<String, serde_json::Value>,
}

fn one() -> u64 {
    1
}

/// Highest finding payload version this build can read.
pub const FINDING_VERSION: u32 = 1;

fn finding_version() -> u32 {
    FINDING_VERSION
}

/// 16 hex chars of randomness — unique enough that a finding id never needs
/// coordination, short enough to sit in a RocksDB key.
fn new_finding_id() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..16)
        .map(|_| std::char::from_digit(rng.gen_range(0..16), 16).unwrap_or('0'))
        .collect()
}

impl Finding {
    fn new(
        bucket: &str,
        kind: FindingKind,
        object_key: Option<String>,
        blob_dir: Option<String>,
        detail: impl Into<String>,
        bytes: u64,
    ) -> Self {
        Self {
            v: FINDING_VERSION,
            id: new_finding_id(),
            bucket: bucket.to_string(),
            kind,
            object_key,
            blob_dir,
            detail: detail.into(),
            bytes,
            count: 1,
            state: FindingState::Open,
            outcome: None,
            repaired_at_ms: 0,
            data: serde_json::Map::new(),
        }
    }

    /// Attaches one piece of kind-specific context.
    fn with(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
        self.data.insert(key.to_string(), value.into());
        self
    }

    /// What this finding is *about* — the blob dir when there is one, else the
    /// object key. Used as the sort segment of the storage key, so a report's
    /// findings list in a stable, human-meaningful order.
    pub fn anchor(&self) -> &str {
        self.blob_dir
            .as_deref()
            .or(self.object_key.as_deref())
            .unwrap_or("")
    }
}

/// Everything the scan learned about one bucket. Byte figures are split by
/// where the space actually went, because "the bucket holds 1.2 TiB" and "the
/// live objects total 900 GiB" are different, equally interesting numbers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketReport {
    pub bucket: String,
    /// Rows in the index.
    pub objects_indexed: u64,
    /// Sum of row sizes — the logical size of the bucket's live objects.
    pub logical_bytes: u64,
    /// Bytes physically present under `objects/` (includes orphans).
    pub objects_bytes: u64,
    pub trash_bytes: u64,
    pub staging_bytes: u64,
    pub index_bytes: u64,
    /// Blob dirs found on disk (valid or not).
    pub blob_dirs: u64,
    /// Part files stat'ed.
    pub parts_checked: u64,
    pub empty_fanout_dirs: u64,
    /// Hygiene counters — the background jobs already fix these on their own
    /// schedule; they are reported because they explain disk usage.
    pub legacy_layout_dirs: u64,
    pub stale_intents: u64,
    pub trash_dirs: u64,
    pub staging_dirs: u64,
    /// Findings by kind (exact, even when the detail rows were capped).
    pub findings: HashMap<String, u64>,
    pub duration_ms: i64,
    /// Set when the bucket could not be scanned at all (e.g. its index is
    /// outdated and needs a rebuild first).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl BucketReport {
    pub fn total_bytes(&self) -> u64 {
        self.objects_bytes + self.trash_bytes + self.staging_bytes + self.index_bytes
    }

    fn count(&mut self, kind: FindingKind) {
        *self.findings.entry(kind.as_str().to_string()).or_insert(0) += 1;
    }
}

/// Where a running scan sends findings as it discovers them. Bounded on
/// purpose: if the persister falls behind, the walk waits rather than growing
/// an unbounded queue in memory.
pub type FindingSink = tokio::sync::mpsc::Sender<Finding>;

/// Live counters a running scan publishes, polled by the job that owns it and
/// broadcast to the console. Everything is atomic so the poller never blocks
/// the scan.
#[derive(Debug, Default)]
pub struct ScanProgress {
    pub phase: Mutex<String>,
    pub bucket: Mutex<String>,
    pub buckets_total: AtomicU64,
    pub buckets_done: AtomicU64,
    /// Index rows in the bucket being scanned (0 = not yet known).
    pub objects_total: AtomicU64,
    pub objects_visited: AtomicU64,
    pub dirs_visited: AtomicU64,
    pub parts_checked: AtomicU64,
    pub bytes_seen: AtomicU64,
    pub findings: AtomicU64,
}

impl ScanProgress {
    fn enter(&self, bucket: &str, phase: &str) {
        *self.bucket.lock().unwrap() = bucket.to_string();
        *self.phase.lock().unwrap() = phase.to_string();
    }

    fn phase(&self, phase: &str) {
        *self.phase.lock().unwrap() = phase.to_string();
    }

    pub fn snapshot(&self) -> ScanProgressSnapshot {
        ScanProgressSnapshot {
            phase: self.phase.lock().unwrap().clone(),
            bucket: self.bucket.lock().unwrap().clone(),
            buckets_total: self.buckets_total.load(Ordering::Relaxed),
            buckets_done: self.buckets_done.load(Ordering::Relaxed),
            objects_total: self.objects_total.load(Ordering::Relaxed),
            objects_visited: self.objects_visited.load(Ordering::Relaxed),
            dirs_visited: self.dirs_visited.load(Ordering::Relaxed),
            parts_checked: self.parts_checked.load(Ordering::Relaxed),
            bytes_seen: self.bytes_seen.load(Ordering::Relaxed),
            findings: self.findings.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScanProgressSnapshot {
    pub phase: String,
    pub bucket: String,
    pub buckets_total: u64,
    pub buckets_done: u64,
    pub objects_total: u64,
    pub objects_visited: u64,
    pub dirs_visited: u64,
    pub parts_checked: u64,
    pub bytes_seen: u64,
    pub findings: u64,
}

// ── the scan ────────────────────────────────────────────────────────────────

/// Fingerprint of the attributes a row and its `meta.json` must agree on.
fn attr_fingerprint(size: u64, etag: &str, last_modified_ms: i64) -> u64 {
    let mut hasher = DefaultHasher::new();
    size.hash(&mut hasher);
    etag.trim_matches('"').hash(&mut hasher);
    last_modified_ms.hash(&mut hasher);
    hasher.finish()
}

fn hash64(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// A blob dir the walk could not match to its row cheaply. Resolved exactly,
/// by object key, once the walk is done.
struct Candidate {
    rel: String,
    object_key: String,
    bytes: u64,
    /// Whether the hash lookup said "no such blob dir" (orphan-ish) or "found,
    /// but the attributes differ" (drift-ish).
    unreferenced: bool,
    meta_size: u64,
    meta_etag: String,
    meta_last_modified_ms: i64,
}

impl LocalObjectStore {
    /// Scans one bucket, streaming findings into `sink` as they are discovered.
    /// Read-only; never repairs. Yields between every unit of work and stops
    /// promptly on cancellation (a partial scan is not a useful report, so a
    /// cancelled scan errors out).
    pub async fn scan_bucket(
        &self,
        bucket: &str,
        cancel: &CancellationToken,
        progress: &ScanProgress,
        sink: &FindingSink,
    ) -> Result<BucketReport> {
        let started = now_ms();
        let mut report = BucketReport {
            bucket: bucket.to_string(),
            ..BucketReport::default()
        };
        let bucket_dir = self.layout().bucket_dir(bucket)?;
        let index = self.index(bucket).await?;

        // ── pass 1: index sweep ────────────────────────────────────────────
        progress.enter(bucket, "index");
        report.objects_indexed = index.object_count().await.unwrap_or(0);
        progress
            .objects_total
            .store(report.objects_indexed, Ordering::Relaxed);
        // hash(blob_dir) -> fingerprint(size, etag, last_modified)
        let mut rows: HashMap<u64, u64> = HashMap::new();
        let mut after: Option<String> = None;
        loop {
            check_cancelled(cancel, &self.shutdown_token())?;
            let batch = index.all_entries_after(after.as_deref(), INDEX_BATCH).await?;
            if batch.is_empty() {
                break;
            }
            for row in &batch {
                report.logical_bytes += row.size;
                rows.insert(
                    hash64(&row.blob_dir),
                    attr_fingerprint(row.size, &row.etag, row.last_modified_ms),
                );
            }
            progress
                .objects_visited
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            after = batch.last().map(|r| r.object_key.clone());
            yield_now().await;
        }
        // The counter is maintained by merge operator and can drift from the
        // rows actually present; the sweep just counted them, so trust that.
        report.objects_indexed = rows.len() as u64;
        progress
            .objects_total
            .store(report.objects_indexed, Ordering::Relaxed);

        // ── pass 2: tree walk ──────────────────────────────────────────────
        progress.phase("disk");
        progress.objects_visited.store(0, Ordering::Relaxed);
        let objects_dir = bucket_dir.join("objects");
        // Candidates are the *mismatches* only — a healthy bucket accumulates
        // none, and a broken one is bounded by how broken it is, not by size.
        let mut candidates: Vec<Candidate> = Vec::new();
        // (dir, depth) — depth 1 is a fanout dir directly under objects/.
        let mut stack: Vec<(PathBuf, usize)> = vec![(objects_dir.clone(), 0)];
        while let Some((dir, depth)) = stack.pop() {
            check_cancelled(cancel, &self.shutdown_token())?;
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(entries) => entries,
                // A dir that vanished under us (concurrent retire) is not a
                // finding — it is the system working.
                Err(_) => continue,
            };
            progress.dirs_visited.fetch_add(1, Ordering::Relaxed);
            let mut files: Vec<(String, u64)> = Vec::new();
            let mut subdirs: Vec<PathBuf> = Vec::new();
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().into_owned();
                match entry.file_type().await {
                    Ok(ft) if ft.is_dir() => subdirs.push(entry.path()),
                    Ok(_) => {
                        let size = entry.metadata().await.map(|m| m.len()).unwrap_or(0);
                        files.push((name, size));
                    }
                    Err(_) => {}
                }
            }
            yield_now().await;

            let has_meta = files.iter().any(|(name, _)| name == "meta.json");
            if !has_meta {
                if files.is_empty() {
                    // Empty container. Only the fanout level is reclaimable by
                    // the existing pass; deeper empties are legacy chains the
                    // migration job cleans as it vacates them.
                    if depth == 1 && subdirs.is_empty() {
                        report.empty_fanout_dirs += 1;
                    }
                    for sub in subdirs {
                        stack.push((sub, depth + 1));
                    }
                } else if subdirs.is_empty() {
                    // Files but no meta.json: cannot be a valid publish, since
                    // blobs are staged complete before the rename. There is no
                    // object key to be had here — only the path.
                    let bytes: u64 = files.iter().map(|(_, size)| size).sum();
                    report.objects_bytes += bytes;
                    report.blob_dirs += 1;
                    if let Some(rel) = rel_of(&bucket_dir, &dir) {
                        // Deliberately *not* consumed from `rows`: if a row does
                        // point here, that row is genuinely unreadable and pass
                        // 4 should report it as a missing blob too, so the
                        // operator gets both halves of the fix (trash the
                        // bytes, delete the row).
                        let referenced = rows.contains_key(&hash64(&rel));
                        if !is_recent(&dir, started).await {
                            emit(
                                sink,
                                &mut report,
                                progress,
                                Finding::new(
                                    bucket,
                                    FindingKind::UnreadableBlob,
                                    None,
                                    Some(rel),
                                    format!(
                                        "{} file(s) but no meta.json{}",
                                        files.len(),
                                        if referenced {
                                            "; an index row points at this dir, so the object is unreadable"
                                        } else {
                                            "; unreferenced by the index"
                                        }
                                    ),
                                    bytes,
                                )
                                .with("reason", "no_meta_json")
                                .with("files", files.len() as u64)
                                .with("referenced", referenced),
                            )
                            .await?;
                        }
                    }
                } else {
                    for sub in subdirs {
                        stack.push((sub, depth + 1));
                    }
                }
                continue;
            }

            // An object dir.
            report.blob_dirs += 1;
            let Some(rel) = rel_of(&bucket_dir, &dir) else {
                continue;
            };
            let bytes: u64 = files.iter().map(|(_, size)| size).sum();
            report.objects_bytes += bytes;
            progress.bytes_seen.fetch_add(bytes, Ordering::Relaxed);
            progress.objects_visited.fetch_add(1, Ordering::Relaxed);
            if is_legacy_layout(&rel) {
                report.legacy_layout_dirs += 1;
            }
            // Consume the row's entry: whatever remains in `rows` after the
            // walk is exactly the set of rows no dir on disk accounted for.
            let row_fingerprint = rows.remove(&hash64(&rel));

            let meta: ObjectMeta = match read_meta(&dir.join("meta.json")).await {
                Ok(meta) => meta,
                Err(err) => {
                    // Unparseable meta is the same predicament as none at all:
                    // no object key, so nothing to look up or lock. Put the row
                    // entry back so pass 4 still reports the dangling row.
                    if let Some(fingerprint) = row_fingerprint {
                        rows.insert(hash64(&rel), fingerprint);
                    }
                    if !is_recent(&dir, started).await {
                        emit(
                            sink,
                            &mut report,
                            progress,
                            Finding::new(
                                bucket,
                                FindingKind::UnreadableBlob,
                                None,
                                Some(rel),
                                format!(
                                    "meta.json is unreadable: {err}{}",
                                    if row_fingerprint.is_some() {
                                        "; an index row points at this dir, so the object is unreadable"
                                    } else {
                                        "; unreferenced by the index"
                                    }
                                ),
                                bytes,
                            )
                            .with("reason", "unparseable_meta_json")
                            .with("parse_error", err.to_string())
                            .with("referenced", row_fingerprint.is_some()),
                        )
                        .await?;
                    }
                    yield_now().await;
                    continue;
                }
            };
            yield_now().await;

            // Parts: presence and size only — no bytes are read, so the cost is
            // one stat per part, which the read_dir above already paid.
            let mut problems: Vec<String> = Vec::new();
            let mut parts_bytes: u64 = 0;
            for part in &meta.parts {
                report.parts_checked += 1;
                progress.parts_checked.fetch_add(1, Ordering::Relaxed);
                parts_bytes += part.size;
                match files.iter().find(|(name, _)| *name == part.file) {
                    None => problems.push(format!("part {} ({}) missing", part.number, part.file)),
                    Some((_, on_disk)) if *on_disk != part.size => problems.push(format!(
                        "part {} is {on_disk}B on disk, meta says {}B",
                        part.number, part.size
                    )),
                    Some(_) => {}
                }
            }
            if meta.parts.is_empty() && meta.size > 0 {
                problems.push("meta declares no parts but a non-zero size".to_string());
            } else if parts_bytes != meta.size {
                problems.push(format!(
                    "parts total {parts_bytes}B, meta declares {}B",
                    meta.size
                ));
            }
            if !problems.is_empty() {
                emit(
                    sink,
                    &mut report,
                    progress,
                    Finding::new(
                        bucket,
                        FindingKind::CorruptObject,
                        Some(meta.object_key.clone()),
                        Some(rel.clone()),
                        problems.join("; "),
                        bytes,
                    )
                    .with("problems", problems)
                    .with("parts", meta.parts.len() as u64)
                    .with("meta_size", meta.size)
                    .with("parts_size", parts_bytes),
                )
                .await?;
                yield_now().await;
                continue;
            }

            // Reconcile against the index by hash; anything that doesn't match
            // cleanly becomes a candidate resolved exactly below.
            let fingerprint = attr_fingerprint(meta.size, &meta.etag, meta.last_modified_ms);
            match row_fingerprint {
                Some(row_fp) if row_fp == fingerprint => {}
                Some(_) => candidates.push(Candidate {
                    rel,
                    object_key: meta.object_key.clone(),
                    bytes,
                    unreferenced: false,
                    meta_size: meta.size,
                    meta_etag: meta.etag.clone(),
                    meta_last_modified_ms: meta.last_modified_ms,
                }),
                None => {
                    if !is_recent(&dir, started).await {
                        candidates.push(Candidate {
                            rel,
                            object_key: meta.object_key.clone(),
                            bytes,
                            unreferenced: true,
                            meta_size: meta.size,
                            meta_etag: meta.etag.clone(),
                            meta_last_modified_ms: meta.last_modified_ms,
                        });
                    }
                }
            }
            yield_now().await;
        }

        // ── pass 3: candidate verification (exact, by object key) ──────────
        progress.phase("verify");
        for candidate in candidates {
            check_cancelled(cancel, &self.shutdown_token())?;
            let row = index.get(&candidate.object_key).await?;
            let finding = match row {
                // No row at all: the publish never committed, or the index lost
                // it. Either way the dir is not a live object.
                None if candidate.unreferenced => Some(
                    Finding::new(
                        bucket,
                        FindingKind::OrphanBlob,
                        Some(candidate.object_key.clone()),
                        Some(candidate.rel.clone()),
                        "no index row for this object key".to_string(),
                        candidate.bytes,
                    )
                    .with("reason", "no_index_row")
                    .with("meta_size", candidate.meta_size)
                    .with("meta_last_modified_ms", candidate.meta_last_modified_ms),
                ),
                None => None,
                Some(row) if row.blob_dir != candidate.rel => Some(
                    Finding::new(
                        bucket,
                        FindingKind::SupersededBlob,
                        Some(candidate.object_key.clone()),
                        Some(candidate.rel.clone()),
                        format!("the live row points at {}", row.blob_dir),
                        candidate.bytes,
                    )
                    .with("live_blob_dir", row.blob_dir.clone())
                    .with("live_last_modified_ms", row.last_modified_ms)
                    .with("meta_last_modified_ms", candidate.meta_last_modified_ms),
                ),
                // Same dir, so this was the drift branch (or a hash collision
                // that resolved to nothing). Re-compare exactly.
                Some(row) => drift_detail(&row, &candidate).map(|detail| {
                    Finding::new(
                        bucket,
                        FindingKind::IndexDrift,
                        Some(candidate.object_key.clone()),
                        Some(candidate.rel.clone()),
                        detail,
                        candidate.bytes,
                    )
                    .with("index_size", row.size)
                    .with("index_etag", row.etag.trim_matches('"').to_string())
                    .with("index_last_modified_ms", row.last_modified_ms)
                    .with("meta_size", candidate.meta_size)
                    .with("meta_etag", candidate.meta_etag.trim_matches('"').to_string())
                    .with("meta_last_modified_ms", candidate.meta_last_modified_ms)
                }),
            };
            if let Some(finding) = finding {
                emit(sink, &mut report, progress, finding).await?;
            }
            yield_now().await;
        }

        // ── pass 4: reverse sweep (rows whose dir the walk never saw) ──────
        // `rows` now holds only hashes the walk never consumed, so this pass
        // just needs the index again to turn those back into keys and paths.
        progress.phase("reconcile");
        let mut after: Option<String> = None;
        loop {
            check_cancelled(cancel, &self.shutdown_token())?;
            if rows.is_empty() {
                break; // every row was accounted for on disk
            }
            let batch = index.all_entries_after(after.as_deref(), INDEX_BATCH).await?;
            if batch.is_empty() {
                break;
            }
            for row in &batch {
                if !rows.contains_key(&hash64(&row.blob_dir)) {
                    continue;
                }
                // A row written after the walk passed its dir is not missing.
                if started - row.last_modified_ms < SCAN_GRACE_MS {
                    continue;
                }
                // Exact check — the hash said "never seen", the filesystem gets
                // the final word.
                let meta_path = bucket_dir.join(&row.blob_dir).join("meta.json");
                if tokio::fs::metadata(&meta_path).await.is_ok() {
                    continue;
                }
                emit(
                    sink,
                    &mut report,
                    progress,
                    Finding::new(
                        bucket,
                        FindingKind::MissingBlob,
                        Some(row.object_key.clone()),
                        Some(row.blob_dir.clone()),
                        "the row's blob dir has no meta.json on disk".to_string(),
                        row.size,
                    )
                    .with("index_size", row.size)
                    .with("index_etag", row.etag.trim_matches('"').to_string())
                    .with("index_last_modified_ms", row.last_modified_ms),
                )
                .await?;
            }
            after = batch.last().map(|r| r.object_key.clone());
            yield_now().await;
        }

        // ── pass 5: the rest of the bucket's disk usage ────────────────────
        progress.phase("usage");
        let (trash_bytes, trash_dirs) =
            dir_usage(&bucket_dir.join("trash"), cancel, &self.shutdown_token()).await?;
        report.trash_bytes = trash_bytes;
        report.trash_dirs = trash_dirs;
        let (staging_bytes, staging_dirs) =
            dir_usage(&bucket_dir.join("staging"), cancel, &self.shutdown_token()).await?;
        report.staging_bytes = staging_bytes;
        report.staging_dirs = staging_dirs;
        let (index_bytes, _) = dir_usage(
            &super::index::index_db_path(&bucket_dir),
            cancel,
            &self.shutdown_token(),
        )
        .await?;
        report.index_bytes = index_bytes;
        report.stale_intents = index
            .stale_intents(now_ms(), SCAN_GRACE_MS, STALE_INTENT_SAMPLE)
            .await
            .map(|v| v.len() as u64)
            .unwrap_or(0);

        if report.empty_fanout_dirs > 0 {
            let mut finding = Finding::new(
                bucket,
                FindingKind::EmptyFanout,
                None,
                None,
                format!(
                    "{} empty fanout director{} under objects/",
                    report.empty_fanout_dirs,
                    if report.empty_fanout_dirs == 1 { "y" } else { "ies" }
                ),
                0,
            )
            .with("dirs", report.empty_fanout_dirs);
            finding.count = report.empty_fanout_dirs;
            emit(sink, &mut report, progress, finding).await?;
        }

        report.duration_ms = now_ms() - started;
        progress.buckets_done.fetch_add(1, Ordering::Relaxed);
        Ok(report)
    }

    /// Applies one repair. Every action re-verifies the finding under the
    /// per-key lock first: a report can be days old, and the object may have
    /// been rewritten or deleted since. A finding that no longer holds comes
    /// back [`FindingState::Stale`] with nothing touched.
    pub async fn repair_finding(&self, finding: &Finding, action: RepairAction) -> (FindingState, String) {
        if !finding.kind.allows(action) {
            return (
                FindingState::Failed,
                format!(
                    "{} is not a valid repair for {}",
                    action.as_str(),
                    finding.kind.as_str()
                ),
            );
        }
        match self.repair_inner(finding, action).await {
            Ok(outcome) => outcome,
            Err(err) => (FindingState::Failed, err.to_string()),
        }
    }

    async fn repair_inner(
        &self,
        finding: &Finding,
        action: RepairAction,
    ) -> Result<(FindingState, String)> {
        let bucket = &finding.bucket;
        if action == RepairAction::ReclaimEmptyDirs {
            let cancel = CancellationToken::new();
            let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let removed = self
                .reclaim_empty_dirs_pass(bucket, &cancel, &counter)
                .await?;
            return Ok((
                FindingState::Repaired,
                format!("reclaimed {removed} empty director{}", if removed == 1 { "y" } else { "ies" }),
            ));
        }

        // A dir with no readable meta.json has no object key, so there is
        // nothing to lock and nothing to look up — only bytes to move away.
        // That is safe without the key lock precisely *because* it has no
        // meta.json: publishes rename complete staged dirs into freshly-named
        // live paths, so no commit can ever land on this exact path, and no
        // readable object can appear here under us. The re-check below is what
        // enforces that invariant at the moment of the repair.
        if finding.kind == FindingKind::UnreadableBlob {
            return self.trash_unreadable_blob(finding).await;
        }

        let Some(object_key) = finding.object_key.clone() else {
            return Ok((
                FindingState::Failed,
                "finding has no object key to lock".to_string(),
            ));
        };
        let bucket_dir = self.layout().bucket_dir(bucket)?;
        let index = self.index(bucket).await?;
        // Serialise against publish/delete of the same key, exactly as the
        // write path does — this is what makes a repair safe on a live server.
        let _guard = self.lock_object_key(bucket, &object_key).await;
        let row = index.get(&object_key).await?;

        match action {
            RepairAction::TrashBlob | RepairAction::Quarantine => {
                let Some(rel) = finding.blob_dir.clone() else {
                    return Ok((FindingState::Failed, "finding has no blob dir".to_string()));
                };
                // Refuse to trash a dir the index now points at — that would
                // delete a live object.
                if action == RepairAction::TrashBlob {
                    if let Some(row) = &row {
                        if row.blob_dir == rel {
                            return Ok((
                                FindingState::Stale,
                                "the index now points at this dir; it is a live object".to_string(),
                            ));
                        }
                    }
                }
                let abs = bucket_dir.join(&rel);
                if tokio::fs::metadata(&abs).await.is_err() {
                    return Ok((FindingState::Stale, "the blob dir is already gone".to_string()));
                }
                let mut messages = Vec::new();
                if action == RepairAction::Quarantine {
                    // Corrupt object: drop the row first so nothing can start
                    // reading the bytes we are about to move away.
                    match &row {
                        Some(row) if row.blob_dir == rel => {
                            self.delete_row(bucket, &object_key, row).await?;
                            messages.push("index row deleted".to_string());
                        }
                        _ => messages.push("index row already gone".to_string()),
                    }
                }
                let trashed = move_object_dir_to_trash(self.layout(), bucket, &abs).await?;
                messages.push(format!(
                    "blob dir moved to {}",
                    trashed
                        .file_name()
                        .map(|v| format!("trash/{}", v.to_string_lossy()))
                        .unwrap_or_else(|| "trash".to_string())
                ));
                Ok((FindingState::Repaired, messages.join("; ")))
            }
            RepairAction::DeleteRow => {
                let Some(row) = row else {
                    return Ok((FindingState::Stale, "the index row is already gone".to_string()));
                };
                if let Some(rel) = &finding.blob_dir {
                    if &row.blob_dir != rel {
                        return Ok((
                            FindingState::Stale,
                            format!("the row now points at {}, not {rel}", row.blob_dir),
                        ));
                    }
                    // For a missing-blob finding, confirm it is still missing.
                    if finding.kind == FindingKind::MissingBlob
                        && tokio::fs::metadata(bucket_dir.join(rel).join("meta.json"))
                            .await
                            .is_ok()
                    {
                        return Ok((
                            FindingState::Stale,
                            "the blob dir is readable again".to_string(),
                        ));
                    }
                }
                self.delete_row(bucket, &object_key, &row).await?;
                Ok((FindingState::Repaired, "index row deleted".to_string()))
            }
            RepairAction::ResyncRow => {
                let Some(rel) = finding.blob_dir.clone() else {
                    return Ok((FindingState::Failed, "finding has no blob dir".to_string()));
                };
                let Some(row) = row else {
                    return Ok((FindingState::Stale, "the index row is gone".to_string()));
                };
                if row.blob_dir != rel {
                    return Ok((
                        FindingState::Stale,
                        format!("the row now points at {}, not {rel}", row.blob_dir),
                    ));
                }
                let meta: ObjectMeta = read_meta(&bucket_dir.join(&rel).join("meta.json")).await?;
                if meta.size == row.size
                    && meta.etag.trim_matches('"') == row.etag.trim_matches('"')
                    && meta.last_modified_ms == row.last_modified_ms
                {
                    return Ok((FindingState::Stale, "row and meta.json already agree".to_string()));
                }
                let updated = index
                    .update_object_attrs(
                        &object_key,
                        &rel,
                        meta.size,
                        meta.etag.trim_matches('"'),
                        meta.last_modified_ms,
                    )
                    .await?;
                self.forget_cached_meta(bucket, &object_key);
                if updated {
                    Ok((
                        FindingState::Repaired,
                        format!(
                            "row resynced from meta.json (size {}B, etag {})",
                            meta.size,
                            meta.etag.trim_matches('"')
                        ),
                    ))
                } else {
                    Ok((FindingState::Stale, "the row moved under us".to_string()))
                }
            }
            RepairAction::ReclaimEmptyDirs => unreachable!("handled above"),
        }
    }

    /// Trashes a blob dir that has no usable `meta.json`. Re-verifies that it
    /// is *still* unreadable first: if a readable meta.json has appeared, the
    /// finding is stale and nothing is touched.
    async fn trash_unreadable_blob(&self, finding: &Finding) -> Result<(FindingState, String)> {
        let Some(rel) = finding.blob_dir.clone() else {
            return Ok((FindingState::Failed, "finding has no blob dir".to_string()));
        };
        let abs = self.layout().bucket_dir(&finding.bucket)?.join(&rel);
        if tokio::fs::metadata(&abs).await.is_err() {
            return Ok((FindingState::Stale, "the blob dir is already gone".to_string()));
        }
        if read_meta(&abs.join("meta.json")).await.is_ok() {
            return Ok((
                FindingState::Stale,
                "the dir now has a readable meta.json".to_string(),
            ));
        }
        let trashed = move_object_dir_to_trash(self.layout(), &finding.bucket, &abs).await?;
        Ok((
            FindingState::Repaired,
            format!(
                "blob dir moved to {}",
                trashed
                    .file_name()
                    .map(|v| format!("trash/{}", v.to_string_lossy()))
                    .unwrap_or_else(|| "trash".to_string())
            ),
        ))
    }

    /// Removes one index row through the normal delete commit (so the object
    /// counter stays correct) and clears the retire intent — the blob dir is
    /// either already gone or handled by the caller.
    async fn delete_row(&self, bucket: &str, key: &str, row: &ObjectRecord) -> Result<()> {
        let index = self.index(bucket).await?;
        let retire_id = index.commit_delete(key, &row.blob_dir, now_ms()).await?;
        self.forget_cached_meta(bucket, key);
        let _ = index.delete_intent(retire_id).await;
        Ok(())
    }
}

/// Exact re-comparison of a row against the meta a walk read, for the drift
/// branch. `None` means they actually agree (the hash lied).
fn drift_detail(row: &ObjectRecord, candidate: &Candidate) -> Option<String> {
    let mut parts = Vec::new();
    if row.size != candidate.meta_size {
        parts.push(format!(
            "size {}B in index vs {}B in meta.json",
            row.size, candidate.meta_size
        ));
    }
    if row.etag.trim_matches('"') != candidate.meta_etag.trim_matches('"') {
        parts.push(format!(
            "etag {} in index vs {} in meta.json",
            row.etag.trim_matches('"'),
            candidate.meta_etag.trim_matches('"')
        ));
    }
    if row.last_modified_ms != candidate.meta_last_modified_ms {
        parts.push(format!(
            "last-modified {} in index vs {} in meta.json",
            row.last_modified_ms, candidate.meta_last_modified_ms
        ));
    }
    (!parts.is_empty()).then(|| parts.join("; "))
}

/// Records a finding: counted in the summary, then handed to the persister.
/// The send awaits when the sink is full — that backpressure is the point, and
/// a closed sink (the persister died, or the run was abandoned) ends the scan
/// rather than letting it grind on writing into the void.
async fn emit(
    sink: &FindingSink,
    report: &mut BucketReport,
    progress: &ScanProgress,
    finding: Finding,
) -> Result<()> {
    report.count(finding.kind);
    progress.findings.fetch_add(1, Ordering::Relaxed);
    sink.send(finding)
        .await
        .map_err(|_| StorageError::Io("scan finding sink closed".to_string()))
}

fn rel_of(bucket_dir: &Path, dir: &Path) -> Option<String> {
    dir.strip_prefix(bucket_dir)
        .ok()
        .map(|p| p.to_string_lossy().replace('\\', "/"))
}

/// True when the dir was modified inside the grace window — too new to judge.
async fn is_recent(dir: &Path, now: i64) -> bool {
    let Ok(meta) = tokio::fs::metadata(dir).await else {
        return false;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    let modified_ms = modified
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|v| v.as_millis() as i64)
        .unwrap_or(0);
    now.saturating_sub(modified_ms) < SCAN_GRACE_MS
}

async fn read_meta(path: &Path) -> Result<ObjectMeta> {
    let bytes = tokio::fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Recursive `du` for the non-object areas of a bucket: total bytes and the
/// number of top-level directories. Yields per directory and honours cancel.
async fn dir_usage(
    root: &Path,
    cancel: &CancellationToken,
    shutdown: &CancellationToken,
) -> Result<(u64, u64)> {
    let mut bytes = 0u64;
    let mut top_level = 0u64;
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    while let Some((dir, depth)) = stack.pop() {
        check_cancelled(cancel, shutdown)?;
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            match entry.file_type().await {
                Ok(ft) if ft.is_dir() => {
                    if depth == 0 {
                        top_level += 1;
                    }
                    stack.push((entry.path(), depth + 1));
                }
                Ok(_) => bytes += entry.metadata().await.map(|m| m.len()).unwrap_or(0),
                Err(_) => {}
            }
        }
        yield_now().await;
    }
    Ok((bytes, top_level))
}

fn check_cancelled(cancel: &CancellationToken, shutdown: &CancellationToken) -> Result<()> {
    if cancel.is_cancelled() || shutdown.is_cancelled() {
        return Err(StorageError::Io("scan cancelled".to_string()));
    }
    Ok(())
}

/// Matches the legacy 4-level fanout layout `objects/xx/xx/xx/xx/…`.
fn is_legacy_layout(blob_dir: &str) -> bool {
    let Some(rest) = blob_dir.strip_prefix("objects/") else {
        return false;
    };
    let mut parts = rest.split('/');
    for _ in 0..4 {
        match parts.next() {
            Some(level) if level.len() == 2 => {}
            _ => return false,
        }
    }
    parts.next().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scanned {
        report: BucketReport,
        findings: Vec<Finding>,
    }

    /// Drains the sink concurrently, the way the real persister does.
    async fn scan(store: &LocalObjectStore, bucket: &str) -> Scanned {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let collector = tokio::spawn(async move {
            let mut out = Vec::new();
            while let Some(finding) = rx.recv().await {
                out.push(finding);
            }
            out
        });
        let report = store
            .scan_bucket(bucket, &CancellationToken::new(), &ScanProgress::default(), &tx)
            .await
            .unwrap();
        drop(tx);
        let mut findings = collector.await.unwrap();
        findings.sort_by(|a, b| a.id.cmp(&b.id));
        Scanned { report, findings }
    }

    fn kinds(scan: &Scanned) -> Vec<FindingKind> {
        scan.findings.iter().map(|f| f.kind).collect()
    }

    fn only(scan: &Scanned, kind: FindingKind) -> &Finding {
        let matching: Vec<&Finding> = scan.findings.iter().filter(|f| f.kind == kind).collect();
        assert_eq!(matching.len(), 1, "expected one {kind:?} in {:?}", kinds(scan));
        matching[0]
    }

    /// Backdates a row's `last_modified_ms` out of the scan's grace window.
    /// Rows younger than that are deliberately never reported, so a test that
    /// wants a finding has to make the row old enough to judge.
    async fn age_row(store: &LocalObjectStore, bucket: &str, key: &str) -> ObjectRecord {
        let index = store.index(bucket).await.unwrap();
        let row = index.get(key).await.unwrap().unwrap();
        let aged = ObjectRecord {
            last_modified_ms: now_ms() - 10 * 60_000,
            ..row
        };
        let intent = index
            .insert_publish_intent(key, &aged.blob_dir, now_ms())
            .await
            .unwrap();
        index.commit_publish(&aged, intent, None, now_ms()).await.unwrap();
        aged
    }

    /// Backdates a path out of the grace window so the scan will judge it.
    fn age(path: &Path) {
        let old = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        let _ = filetime_set(path, old);
    }

    fn filetime_set(path: &Path, time: std::time::SystemTime) -> std::io::Result<()> {
        let file = std::fs::OpenOptions::new().write(true).open(path).or_else(|_| {
            std::fs::OpenOptions::new().read(true).open(path)
        })?;
        file.set_modified(time)
    }

    #[tokio::test]
    async fn clean_bucket_has_no_findings() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        let result = scan(&store, "bkt").await;
        assert!(result.findings.is_empty(), "{:?}", result.findings);
        assert_eq!(result.report.objects_indexed, 1);
        assert_eq!(result.report.logical_bytes, 5);
        // Physical includes meta.json, so it exceeds the logical size.
        assert!(result.report.objects_bytes > 5);
        assert_eq!(result.report.parts_checked, 1);
    }

    #[tokio::test]
    async fn missing_part_file_is_corrupt() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        let row = store.index("bkt").await.unwrap().get("a.txt").await.unwrap().unwrap();
        let dir = tmp.path().join("buckets/bkt").join(&row.blob_dir);
        std::fs::remove_file(dir.join("part.1")).unwrap();

        let result = scan(&store, "bkt").await;
        assert_eq!(kinds(&result), vec![FindingKind::CorruptObject]);
        assert!(result.findings[0].detail.contains("missing"));
    }

    #[tokio::test]
    async fn short_part_file_is_corrupt() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        let row = store.index("bkt").await.unwrap().get("a.txt").await.unwrap().unwrap();
        let dir = tmp.path().join("buckets/bkt").join(&row.blob_dir);
        std::fs::write(dir.join("part.1"), b"hi").unwrap();

        let result = scan(&store, "bkt").await;
        assert_eq!(kinds(&result), vec![FindingKind::CorruptObject]);
        assert!(result.findings[0].detail.contains("2B on disk"));
    }

    #[tokio::test]
    async fn orphan_dir_is_found_and_trashed_on_repair() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        let row = store.index("bkt").await.unwrap().get("a.txt").await.unwrap().unwrap();
        let live = tmp.path().join("buckets/bkt").join(&row.blob_dir);

        // A blob dir for a key with no row at all — an uncommitted publish.
        let orphan = tmp.path().join("buckets/bkt/objects/ZZZZ/V1ORPHAN0");
        std::fs::create_dir_all(&orphan).unwrap();
        for name in ["meta.json", "part.1"] {
            std::fs::copy(live.join(name), orphan.join(name)).unwrap();
        }
        let mut meta: ObjectMeta =
            serde_json::from_slice(&std::fs::read(orphan.join("meta.json")).unwrap()).unwrap();
        meta.object_key = "ghost.txt".to_string();
        std::fs::write(orphan.join("meta.json"), serde_json::to_vec(&meta).unwrap()).unwrap();
        age(&orphan.join("meta.json"));
        age(&orphan);

        let result = scan(&store, "bkt").await;
        assert_eq!(kinds(&result), vec![FindingKind::OrphanBlob]);

        let (state, message) = store
            .repair_finding(&result.findings[0], RepairAction::TrashBlob)
            .await;
        assert_eq!(state, FindingState::Repaired, "{message}");
        assert!(!orphan.exists());
        // The live object is untouched.
        assert!(store.read_object("bkt", "a.txt").await.is_ok());
    }

    #[tokio::test]
    async fn superseded_duplicate_never_wins_over_the_indexed_row() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        let row = store.index("bkt").await.unwrap().get("a.txt").await.unwrap().unwrap();
        let live = tmp.path().join("buckets/bkt").join(&row.blob_dir);

        // A *newer* copy of the same key that never got committed. The index
        // row still wins: the client was never told this write succeeded.
        let dup = tmp.path().join("buckets/bkt/objects/YYYY/V1NEWERCP");
        std::fs::create_dir_all(&dup).unwrap();
        for name in ["meta.json", "part.1"] {
            std::fs::copy(live.join(name), dup.join(name)).unwrap();
        }
        let mut meta: ObjectMeta =
            serde_json::from_slice(&std::fs::read(dup.join("meta.json")).unwrap()).unwrap();
        meta.last_modified_ms += 60_000;
        std::fs::write(dup.join("meta.json"), serde_json::to_vec(&meta).unwrap()).unwrap();
        age(&dup.join("meta.json"));
        age(&dup);

        let result = scan(&store, "bkt").await;
        assert_eq!(kinds(&result), vec![FindingKind::SupersededBlob]);

        let (state, message) = store
            .repair_finding(&result.findings[0], RepairAction::TrashBlob)
            .await;
        assert_eq!(state, FindingState::Repaired, "{message}");
        assert!(!dup.exists());
        assert!(live.exists());
    }

    #[tokio::test]
    async fn missing_blob_row_is_found_and_row_deleted_on_repair() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        store.put_object("bkt", "b.txt", b"world", None, None, false).await.unwrap();
        let index = store.index("bkt").await.unwrap();
        let row = age_row(&store, "bkt", "a.txt").await;
        std::fs::remove_dir_all(tmp.path().join("buckets/bkt").join(&row.blob_dir)).unwrap();

        let result = scan(&store, "bkt").await;
        let finding = only(&result, FindingKind::MissingBlob);
        assert_eq!(finding.object_key.as_deref(), Some("a.txt"));
        assert_eq!(finding.data["index_size"], 5);

        let (state, message) = store.repair_finding(finding, RepairAction::DeleteRow).await;
        assert_eq!(state, FindingState::Repaired, "{message}");
        assert!(index.get("a.txt").await.unwrap().is_none());
        assert_eq!(index.object_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn index_drift_is_found_and_resynced_from_meta() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        let index = store.index("bkt").await.unwrap();
        let row = index.get("a.txt").await.unwrap().unwrap();
        // Corrupt only the row's idea of the size.
        let intent = index.insert_publish_intent("a.txt", &row.blob_dir, now_ms()).await.unwrap();
        index
            .commit_publish(
                &ObjectRecord { size: 999, ..row.clone() },
                intent,
                None,
                now_ms(),
            )
            .await
            .unwrap();

        let result = scan(&store, "bkt").await;
        assert_eq!(kinds(&result), vec![FindingKind::IndexDrift]);

        let (state, message) = store
            .repair_finding(&result.findings[0], RepairAction::ResyncRow)
            .await;
        assert_eq!(state, FindingState::Repaired, "{message}");
        assert_eq!(index.get("a.txt").await.unwrap().unwrap().size, 5);
        // And the bucket is clean afterwards.
        assert!(scan(&store, "bkt").await.findings.is_empty());
    }

    #[tokio::test]
    async fn empty_fanout_dirs_are_reported_and_reclaimed() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        for name in ["AAAA", "BBBB"] {
            std::fs::create_dir_all(tmp.path().join("buckets/bkt/objects").join(name)).unwrap();
        }

        let result = scan(&store, "bkt").await;
        assert_eq!(result.report.empty_fanout_dirs, 2);
        assert_eq!(kinds(&result), vec![FindingKind::EmptyFanout]);

        let (state, message) = store
            .repair_finding(&result.findings[0], RepairAction::ReclaimEmptyDirs)
            .await;
        assert_eq!(state, FindingState::Repaired, "{message}");
        assert!(!tmp.path().join("buckets/bkt/objects/AAAA").exists());
        assert!(store.read_object("bkt", "a.txt").await.is_ok());
    }

    #[tokio::test]
    async fn repair_of_a_stale_finding_touches_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        let index = store.index("bkt").await.unwrap();
        let row = age_row(&store, "bkt", "a.txt").await;
        std::fs::remove_dir_all(tmp.path().join("buckets/bkt").join(&row.blob_dir)).unwrap();
        let result = scan(&store, "bkt").await;
        let finding = only(&result, FindingKind::MissingBlob).clone();

        // The object is rewritten between scan and repair.
        store.put_object("bkt", "a.txt", b"restored", None, None, false).await.unwrap();
        let (state, message) = store.repair_finding(&finding, RepairAction::DeleteRow).await;
        assert_eq!(state, FindingState::Stale, "{message}");
        assert!(index.get("a.txt").await.unwrap().is_some());
    }

    /// A dir with files but no meta.json has no object key at all — nothing to
    /// look up, lock, or delete a row by. It gets its own kind, and the only
    /// thing that can be done with it is to move the bytes away.
    #[tokio::test]
    async fn blob_dir_without_meta_is_unreadable_not_corrupt() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        let junk = tmp.path().join("buckets/bkt/objects/ZZZZ/V1NOMETA0");
        std::fs::create_dir_all(&junk).unwrap();
        std::fs::write(junk.join("part.1"), b"bytes with no metadata").unwrap();
        age(&junk.join("part.1"));
        age(&junk);

        let result = scan(&store, "bkt").await;
        let finding = only(&result, FindingKind::UnreadableBlob);
        assert!(finding.object_key.is_none(), "there is no key to know");
        assert_eq!(finding.data["reason"], "no_meta_json");
        assert_eq!(finding.data["referenced"], false);
        assert_eq!(finding.kind.actions(), &[RepairAction::TrashBlob]);

        let (state, message) = store.repair_finding(finding, RepairAction::TrashBlob).await;
        assert_eq!(state, FindingState::Repaired, "{message}");
        assert!(!junk.exists());
    }

    /// When a row *does* point at a meta-less dir, both halves are reported:
    /// the bytes to trash, and the dangling row to delete.
    #[tokio::test]
    async fn referenced_dir_without_meta_reports_both_halves() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        let row = age_row(&store, "bkt", "a.txt").await;
        let dir = tmp.path().join("buckets/bkt").join(&row.blob_dir);
        std::fs::remove_file(dir.join("meta.json")).unwrap();
        // Unlinking bumps the dir's mtime, which puts it inside the grace
        // window; on a real server the next scan a minute later reports it.
        age(&dir);

        let result = scan(&store, "bkt").await;
        let unreadable = only(&result, FindingKind::UnreadableBlob);
        assert_eq!(unreadable.data["referenced"], true);
        let missing = only(&result, FindingKind::MissingBlob).clone();
        assert_eq!(missing.object_key.as_deref(), Some("a.txt"));

        // Both repairs together leave the bucket clean.
        let (state, message) = store.repair_finding(unreadable, RepairAction::TrashBlob).await;
        assert_eq!(state, FindingState::Repaired, "{message}");
        let (state, message) = store.repair_finding(&missing, RepairAction::DeleteRow).await;
        assert_eq!(state, FindingState::Repaired, "{message}");
        assert!(store.index("bkt").await.unwrap().get("a.txt").await.unwrap().is_none());
    }

    /// An unparseable meta.json is the same predicament — no key to be had.
    #[tokio::test]
    async fn unparseable_meta_is_unreadable() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        let junk = tmp.path().join("buckets/bkt/objects/ZZZZ/V1BADMETA");
        std::fs::create_dir_all(&junk).unwrap();
        std::fs::write(junk.join("meta.json"), b"{ truncated").unwrap();
        age(&junk.join("meta.json"));
        age(&junk);

        let result = scan(&store, "bkt").await;
        let finding = only(&result, FindingKind::UnreadableBlob);
        assert_eq!(finding.data["reason"], "unparseable_meta_json");
        assert!(finding.data.contains_key("parse_error"));
    }

    /// The kind decides which repairs are legal. Asking for one it doesn't
    /// allow fails loudly rather than doing something approximate.
    #[tokio::test]
    async fn a_repair_the_kind_does_not_allow_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        store.put_object("bkt", "a.txt", b"hello", None, None, false).await.unwrap();
        let row = age_row(&store, "bkt", "a.txt").await;
        std::fs::remove_dir_all(tmp.path().join("buckets/bkt").join(&row.blob_dir)).unwrap();
        let result = scan(&store, "bkt").await;
        let finding = only(&result, FindingKind::MissingBlob);

        // A missing blob can only have its row deleted — there is nothing on
        // disk to trash and nothing to resync from.
        for action in [
            RepairAction::TrashBlob,
            RepairAction::ResyncRow,
            RepairAction::Quarantine,
            RepairAction::ReclaimEmptyDirs,
        ] {
            let (state, message) = store.repair_finding(finding, action).await;
            assert_eq!(state, FindingState::Failed, "{action:?} must be refused");
            assert!(message.contains("not a valid repair"), "{message}");
        }
        // …and the row is still there, untouched.
        assert!(store.index("bkt").await.unwrap().get("a.txt").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn recent_dirs_are_never_judged() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        // A brand-new unreferenced dir looks exactly like an in-flight publish.
        let fresh = tmp.path().join("buckets/bkt/objects/ZZZZ/V1FRESH00");
        std::fs::create_dir_all(&fresh).unwrap();
        std::fs::write(fresh.join("meta.json"), b"{ not json").unwrap();
        let result = scan(&store, "bkt").await;
        assert!(result.findings.is_empty(), "{:?}", result.findings);
    }

    #[tokio::test]
    async fn multipart_object_parts_are_all_checked() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        let upload = store
            .initiate_multipart("bkt", "big.bin", None, None)
            .await
            .unwrap();
        let five_mib = vec![7u8; 5 * 1024 * 1024];
        store
            .put_multipart_part("bkt", "big.bin", &upload, 1, &five_mib, false)
            .await
            .unwrap();
        store
            .put_multipart_part("bkt", "big.bin", &upload, 2, b"tail", false)
            .await
            .unwrap();
        let parts: Vec<crate::storage::store::CompletePartRequest> = store
            .list_parts("bkt", "big.bin", &upload)
            .await
            .unwrap()
            .into_iter()
            .map(|p| crate::storage::store::CompletePartRequest {
                number: p.number,
                etag: p.etag,
            })
            .collect();
        assert_eq!(parts.len(), 2);
        store
            .complete_multipart("bkt", "big.bin", &upload, &parts)
            .await
            .unwrap();

        let result = scan(&store, "bkt").await;
        assert!(result.findings.is_empty(), "{:?}", result.findings);
        assert_eq!(result.report.parts_checked, 2);

        // Truncate the second part: it no longer matches its PartMeta size.
        let row = store.index("bkt").await.unwrap().get("big.bin").await.unwrap().unwrap();
        let dir = tmp.path().join("buckets/bkt").join(&row.blob_dir);
        std::fs::write(dir.join("part.2"), b"x").unwrap();
        let result = scan(&store, "bkt").await;
        assert_eq!(kinds(&result), vec![FindingKind::CorruptObject]);
    }
}
