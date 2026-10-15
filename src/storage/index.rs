//! Per-bucket RocksDB catalog — the **source of truth** for object existence.
//!
//! A key in the `objects` column family *is* the object: PUT commits the
//! instant its key is written, DELETE the instant its key is removed. The
//! filesystem is an immutable blob heap addressed by `blob_dir` (stored
//! relative to the bucket directory) and is never scanned on the request path.
//!
//! The `intents` column family is a write-ahead record of planned live-tree
//! transitions: a `publish` intent is written (and committed) before a blob is
//! renamed into the live tree and deleted atomically with the object write; a
//! `retire` intent records a pending move to trash and is deleted once the move
//! completes. Crash recovery and orphan cleanup read this family only — never
//! the tree.
//!
//! ## On-disk value format
//!
//! Every stored value is JSON carrying an explicit `"v"` version field so the
//! parser knows how to read it. Today only **V1** exists. V1 parsing is
//! deliberately lenient — missing fields fall back to reasonable defaults, and
//! unknown fields are ignored — so a value written by a *future, still-V1*
//! build round-trips cleanly. A value stamped with a version *newer* than this
//! build understands is rejected (fail-closed) rather than silently
//! misinterpreted; once a field set is sealed, writers bump the version.
//!
//! ## Column-family layout
//!
//! | family      | key                    | value                       |
//! |-------------|------------------------|-----------------------------|
//! | `objects`   | object key (raw UTF-8) | [`ObjectValueV1`] JSON      |
//! | `intents`   | id (8-byte big-endian) | [`IntentValueV1`] JSON      |
//! | `counters`  | `objects`              | i64 LE, summed via merge    |
//! | `meta`      | `schema_version`       | i64 LE                      |
//! | `changelog` | seq (8-byte big-endian)| changelog entry JSON        |
//! | `tombstones`| object key (raw UTF-8) | [`Tombstone`] JSON          |
//!
//! A tombstone remembers a delete's version stamp, so that a replicated
//! write older than the delete cannot bring the object back. It is written
//! in the delete's batch, removed in the batch of the key's next publish,
//! and purged after the tombstone retention.
//!
//! The `changelog` family (and its `meta` keys) is described in
//! [`super::changelog`]. It is additive — opening an older index creates it
//! empty — so it does not bump the schema version.
//!
//! Object keys are stored as raw UTF-8 bytes, so RocksDB's bytewise ordering is
//! exactly S3 lexicographic listing order — prefix/`start-after`/delimiter
//! listing is a single forward iterator seek. The `objects` counter is
//! maintained through a summing merge operator so concurrent commits on
//! *different* keys never lose an increment (they run under distinct per-key
//! locks, so no transaction is needed — only the counter is contended, and the
//! merge operator resolves that without one).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode, MergeOperands, MultiThreaded,
    Options, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};

use super::changelog::{
    self, ChangeEvent, ChangeOp, ChangeOrigin, ChangelogPage, ChangelogStatus, PartLayout, SeqTracker,
    CF_CHANGELOG, KEY_CHANGELOG_ID, KEY_TRIMMED_THROUGH, KEY_TRIMMED_TS,
};
use super::errors::{Result, StorageError};
use super::stamp::Stamp;

/// Multi-threaded RocksDB handle: `cf_handle` yields an `Arc<BoundColumnFamily>`
/// that outlives a borrow of the DB, which suits the `Arc<DB>` +
/// `spawn_blocking` access pattern.
type Db = DBWithThreadMode<MultiThreaded>;

/// Schema version stamped into the `meta` family. Any other value requires a
/// rebuild. Tracks the *column-family layout*, independent of the per-value
/// `"v"` entity version.
pub const SCHEMA_VERSION: i64 = 2;

/// Highest entity value version this build can read. Values tagged higher are
/// rejected (fail-closed); see the module docs.
const ENTITY_VERSION: u32 = 1;

pub const INTENT_PUBLISH: &str = "publish";
pub const INTENT_RETIRE: &str = "retire";

const CF_OBJECTS: &str = "objects";
const CF_INTENTS: &str = "intents";
const CF_COUNTERS: &str = "counters";
const CF_META: &str = "meta";
const CF_TOMBSTONES: &str = "tombstones";
/// Noncurrent versions of versioned keys: `<key>\0<stamp dir>` → the
/// version (see [`VersionRecord`]). Empty unless the bucket has (or had)
/// versioning enabled.
const CF_VERSIONS: &str = "versions";
/// Separator between the object key and the version dir in `versions`
/// keys; object keys never contain it (`validate_object_key`).
const VERSION_KEY_SEP: u8 = 0;

const KEY_OBJECT_COUNT: &[u8] = b"objects";
/// `meta` keys `repl_ckpt/<link id>`: a replication link's checkpoint (its
/// position in the peer's changelog), kept beside the data it describes.
const CHECKPOINT_PREFIX: &str = "repl_ckpt/";
const KEY_SCHEMA_VERSION: &[u8] = b"schema_version";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRecord {
    pub object_key: String,
    /// Blob directory path relative to the bucket directory,
    /// e.g. `objects/aa/bb/cc/dd/V1AB3F7C_9F00`.
    pub blob_dir: String,
    pub size: u64,
    pub etag: String,
    pub last_modified_ms: i64,
    /// Upload layout; `None` on rows indexed before layouts were recorded.
    pub parts: Option<PartLayout>,
    /// Version stamp; `None` on rows indexed before stamps existed.
    pub stamp: Option<Stamp>,
    /// Written while the bucket's versioning was enabled (a real version);
    /// `false` is the key's `null` version.
    pub versioned: bool,
}

/// A deleted key's last state: the delete's stamp, and what it removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    #[serde(skip)]
    pub object_key: String,
    /// The delete's own stamp (when it happened).
    pub stamp: Stamp,
    /// The stamp of the version it removed: what the tombstone ranks by.
    /// Absent on tombstones written before deletes were version-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed: Option<Stamp>,
    pub deleted_at_ms: i64,
    /// The removed version (empty when a replicated delete arrived for a key
    /// this site never had).
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub etag: String,
    #[serde(default)]
    pub size: u64,
    /// The delete was made while versioning was enabled: a real delete
    /// marker with its stamp as its version id (else a `null` marker).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub versioned: bool,
    /// The delete marker's directory on disk (layout 3), relative to the
    /// bucket dir; absent on tombstones from before markers were written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_dir: Option<String>,
}

/// One version of a key: a live state or a delete marker, current or not.
/// The `versions` family stores the noncurrent ones; the current one is the
/// `objects` row or the `tombstones` entry, from which [`Self::from_row`] /
/// [`Self::from_tombstone`] build the same view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRecord {
    pub object_key: String,
    /// The version's (effective) stamp: its directory name and version id.
    pub stamp: Stamp,
    pub blob_dir: String,
    pub size: u64,
    pub etag: String,
    pub last_modified_ms: i64,
    pub parts: Option<PartLayout>,
    pub versioned: bool,
    pub delete_marker: bool,
    /// For a delete marker: the stamp of the version it removed.
    pub removed: Option<Stamp>,
}

impl VersionRecord {
    /// The version directory name: how the `versions` family keys it.
    pub fn version_dir(&self) -> String {
        super::layouts::v3::stamp_dir(&self.stamp)
    }

    /// The version id as S3 clients see it: the stamp dir for a real
    /// version, `null` otherwise.
    pub fn version_id(&self) -> String {
        if self.versioned {
            self.version_dir()
        } else {
            "null".to_string()
        }
    }

    /// Where this version ranks (see [`super::stamp::Rank`]).
    pub fn rank(&self) -> super::stamp::Rank {
        if self.delete_marker {
            super::stamp::Rank::deleted(self.removed.clone().unwrap_or_else(|| self.stamp.clone()))
        } else {
            super::stamp::Rank::live(self.stamp.clone())
        }
    }

    pub fn from_row(row: &ObjectRecord, stamp: Stamp) -> Self {
        Self {
            object_key: row.object_key.clone(),
            stamp,
            blob_dir: row.blob_dir.clone(),
            size: row.size,
            etag: row.etag.clone(),
            last_modified_ms: row.last_modified_ms,
            parts: row.parts.clone(),
            versioned: row.versioned,
            delete_marker: false,
            removed: None,
        }
    }

    /// `None` for a tombstone with no marker on disk (nothing to keep as a
    /// version).
    pub fn from_tombstone(t: &Tombstone) -> Option<Self> {
        Some(Self {
            object_key: t.object_key.clone(),
            stamp: t.stamp.clone(),
            blob_dir: t.blob_dir.clone()?,
            size: 0,
            etag: String::new(),
            last_modified_ms: t.stamp.ms,
            parts: None,
            versioned: t.versioned,
            delete_marker: true,
            removed: t.removed.clone(),
        })
    }

    pub fn into_row(self) -> ObjectRecord {
        ObjectRecord {
            object_key: self.object_key,
            blob_dir: self.blob_dir,
            size: self.size,
            etag: self.etag,
            last_modified_ms: self.last_modified_ms,
            parts: self.parts,
            stamp: Some(self.stamp),
            versioned: self.versioned,
        }
    }

    pub fn into_tombstone(self, deleted_at_ms: i64) -> Tombstone {
        Tombstone {
            object_key: self.object_key,
            stamp: self.stamp,
            removed: self.removed,
            deleted_at_ms,
            version: String::new(),
            etag: String::new(),
            size: 0,
            versioned: self.versioned,
            blob_dir: Some(self.blob_dir),
        }
    }
}

/// What a commit does with the states it displaces: blob dirs to retire
/// (a `retire` intent each) and versions to keep in the `versions` family.
#[derive(Debug, Clone, Default)]
pub struct Displaced {
    pub retire: Vec<String>,
    pub keep: Vec<VersionRecord>,
    /// Noncurrent versions (by version dir) to drop from the `versions`
    /// family: a `null` version a new `null` state replaces (their dirs are
    /// in `retire`).
    pub drop_versions: Vec<String>,
}

impl Displaced {
    pub fn retire(dir: &str) -> Self {
        Self { retire: vec![dir.to_string()], keep: Vec::new(), drop_versions: Vec::new() }
    }
}

/// Which version of a key a version-scoped delete removes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionTarget {
    /// The current row.
    Current,
    /// The current delete marker (the `tombstones` entry).
    Marker,
    /// A noncurrent version, by its version dir.
    Noncurrent(String),
}

/// A page of [`VersionRecord`]s in key order, newest version first per key.
#[derive(Debug, Clone, Default)]
pub struct VersionsPage {
    pub entries: Vec<(VersionRecord, bool)>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_marker: Option<String>,
}

fn versions_key(key: &str, version_dir: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 1 + version_dir.len());
    out.extend_from_slice(key.as_bytes());
    out.push(VERSION_KEY_SEP);
    out.extend_from_slice(version_dir.as_bytes());
    out
}

fn split_versions_key(raw: &[u8]) -> (String, String) {
    match raw.iter().position(|b| *b == VERSION_KEY_SEP) {
        Some(i) => (
            String::from_utf8_lossy(&raw[..i]).into_owned(),
            String::from_utf8_lossy(&raw[i + 1..]).into_owned(),
        ),
        None => (String::from_utf8_lossy(raw).into_owned(), String::new()),
    }
}

impl Tombstone {
    /// Where this delete ranks (see [`super::stamp::Rank`]).
    pub fn rank(&self) -> super::stamp::Rank {
        super::stamp::Rank::deleted(self.removed.clone().unwrap_or_else(|| self.stamp.clone()))
    }
}

fn encode_tombstone(t: &Tombstone) -> Vec<u8> {
    serde_json::to_vec(t).expect("Tombstone serializes")
}

fn decode_tombstone(key: &[u8], value: &[u8]) -> Result<Tombstone> {
    let mut t: Tombstone = serde_json::from_slice(value)?;
    t.object_key = String::from_utf8_lossy(key).into_owned();
    Ok(t)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentRecord {
    pub id: i64,
    pub op: String,
    pub object_key: String,
    pub blob_dir: String,
    pub created_at_ms: i64,
    pub attempts: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage {
    pub entries: Vec<ObjectRecord>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_after: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// fsync blobs before commit and fsync the WAL on every write: acked
    /// writes survive power loss.
    Full,
    /// No per-put blob fsync and no per-write WAL sync: power loss may drop the
    /// last acked writes; consistency is preserved via startup reconciliation.
    Relaxed,
}

/// Outcome of a rebuild batch: blob dirs that lost newer-wins adjudication and
/// must be moved to trash by the caller.
#[derive(Debug, Default)]
pub struct RebuildBatchOutcome {
    pub inserted: usize,
    pub loser_blob_dirs: Vec<String>,
}

// ── on-disk value encodings (V1) ────────────────────────────────────────────

fn default_version() -> u32 {
    ENTITY_VERSION
}

/// The `objects` family value. The object key itself is the RocksDB key and is
/// not repeated here. Every field defaults so a partial/older record still
/// decodes.
#[derive(Debug, Serialize, Deserialize)]
struct ObjectValueV1 {
    #[serde(default = "default_version")]
    v: u32,
    #[serde(default)]
    blob_dir: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    etag: String,
    #[serde(default)]
    last_modified_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parts: Option<PartLayout>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stamp: Option<Stamp>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    versioned: bool,
}

/// The `versions` family value. The key (`<object key>\0<version dir>`)
/// carries the object key and the stamp.
#[derive(Debug, Serialize, Deserialize)]
struct VersionValueV1 {
    #[serde(default = "default_version")]
    v: u32,
    #[serde(default)]
    blob_dir: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    etag: String,
    #[serde(default)]
    last_modified_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parts: Option<PartLayout>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    versioned: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    delete_marker: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    removed: Option<Stamp>,
}

fn encode_version(record: &VersionRecord) -> Vec<u8> {
    serde_json::to_vec(&VersionValueV1 {
        v: ENTITY_VERSION,
        blob_dir: record.blob_dir.clone(),
        size: record.size,
        etag: record.etag.clone(),
        last_modified_ms: record.last_modified_ms,
        parts: record.parts.clone(),
        versioned: record.versioned,
        delete_marker: record.delete_marker,
        removed: record.removed.clone(),
    })
    .expect("VersionValueV1 serializes")
}

fn decode_version(raw_key: &[u8], value: &[u8]) -> Result<VersionRecord> {
    let v: VersionValueV1 = serde_json::from_slice(value)?;
    reject_newer(v.v, "version")?;
    let (object_key, version_dir) = split_versions_key(raw_key);
    let stamp = super::layouts::v3::parse_stamp_dir(&version_dir)
        .ok_or_else(|| StorageError::Db(format!("versions key for {object_key} has a bad version dir {version_dir}")))?;
    Ok(VersionRecord {
        object_key,
        stamp,
        blob_dir: v.blob_dir,
        size: v.size,
        etag: v.etag,
        last_modified_ms: v.last_modified_ms,
        parts: v.parts,
        versioned: v.versioned,
        delete_marker: v.delete_marker,
        removed: v.removed,
    })
}

/// The `intents` family value. The id is the RocksDB key.
#[derive(Debug, Serialize, Deserialize)]
struct IntentValueV1 {
    #[serde(default = "default_version")]
    v: u32,
    #[serde(default)]
    op: String,
    #[serde(default)]
    object_key: String,
    #[serde(default)]
    blob_dir: String,
    #[serde(default)]
    created_at_ms: i64,
    #[serde(default)]
    attempts: i64,
}

fn reject_newer(v: u32, what: &str) -> Result<()> {
    if v > ENTITY_VERSION {
        return Err(StorageError::Db(format!(
            "{what} value is v{v}, newer than this build understands (v{ENTITY_VERSION}); refusing to read"
        )));
    }
    Ok(())
}

fn encode_object(record: &ObjectRecord) -> Vec<u8> {
    // `unwrap` is safe: the struct is plain owned data with no non-serializable
    // fields, so serialization cannot fail.
    serde_json::to_vec(&ObjectValueV1 {
        v: ENTITY_VERSION,
        blob_dir: record.blob_dir.clone(),
        size: record.size,
        etag: record.etag.clone(),
        last_modified_ms: record.last_modified_ms,
        parts: record.parts.clone(),
        stamp: record.stamp.clone(),
        versioned: record.versioned,
    })
    .expect("ObjectValueV1 serializes")
}

fn decode_object(key: &[u8], value: &[u8]) -> Result<ObjectRecord> {
    let v: ObjectValueV1 = serde_json::from_slice(value)?;
    reject_newer(v.v, "object")?;
    Ok(ObjectRecord {
        object_key: String::from_utf8_lossy(key).into_owned(),
        blob_dir: v.blob_dir,
        size: v.size,
        etag: v.etag,
        last_modified_ms: v.last_modified_ms,
        parts: v.parts,
        stamp: v.stamp,
        versioned: v.versioned,
    })
}

fn encode_intent(op: &str, object_key: &str, blob_dir: &str, created_at_ms: i64, attempts: i64) -> Vec<u8> {
    serde_json::to_vec(&IntentValueV1 {
        v: ENTITY_VERSION,
        op: op.to_string(),
        object_key: object_key.to_string(),
        blob_dir: blob_dir.to_string(),
        created_at_ms,
        attempts,
    })
    .expect("IntentValueV1 serializes")
}

fn decode_intent(id: i64, value: &[u8]) -> Result<IntentRecord> {
    let v: IntentValueV1 = serde_json::from_slice(value)?;
    reject_newer(v.v, "intent")?;
    Ok(IntentRecord {
        id,
        op: v.op,
        object_key: v.object_key,
        blob_dir: v.blob_dir,
        created_at_ms: v.created_at_ms,
        attempts: v.attempts,
    })
}

// ── key / counter byte encodings ────────────────────────────────────────────

fn id_key(id: i64) -> [u8; 8] {
    // Big-endian so RocksDB's bytewise order matches numeric id order (ids are
    // always positive, so the sign bit is never set).
    id.to_be_bytes()
}

fn id_from_key(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    let n = bytes.len().min(8);
    buf[8 - n..].copy_from_slice(&bytes[..n]);
    i64::from_be_bytes(buf)
}

fn decode_counter(bytes: &[u8]) -> i64 {
    if bytes.len() == 8 {
        i64::from_le_bytes(bytes.try_into().unwrap())
    } else {
        0
    }
}

/// Associative merge that treats every value/operand as an i64 delta and sums
/// them. Used for the object-count counter. Registered on the `counters`
/// family at every open (merge operators are not persisted).
fn counter_merge(_key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let mut sum = existing.map(decode_counter).unwrap_or(0);
    for op in operands.iter() {
        sum += decode_counter(op);
    }
    Some(sum.to_le_bytes().to_vec())
}

// ── the index handle ────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ObjectIndex {
    db: Arc<Db>,
    durability: Durability,
    /// In-memory intent id allocator, seeded on open from the highest live id.
    /// Single-process (guaranteed by the data-root process lock), so an atomic
    /// counter is a sound stand-in for SQLite's AUTOINCREMENT.
    intent_seq: Arc<AtomicI64>,
    /// Changelog seq allocator + commit-order watermark.
    changes: Arc<SeqTracker>,
    /// This log's identity (`meta.changelog_id`).
    log_id: Arc<String>,
    /// Called with the new watermark after every commit that logged a change.
    commit_hook: Option<CommitHook>,
}

/// Notified after a change commits (with the new changelog watermark).
pub type CommitHook = Arc<dyn Fn(u64) + Send + Sync>;

impl std::fmt::Debug for ObjectIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectIndex")
            .field("durability", &self.durability)
            .finish_non_exhaustive()
    }
}

pub fn index_db_path(bucket_dir: &Path) -> PathBuf {
    bucket_dir.join("index.rocksdb")
}

fn db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts
}

fn column_families() -> Vec<ColumnFamilyDescriptor> {
    let mut counter_opts = Options::default();
    counter_opts.set_merge_operator_associative("i64_sum", counter_merge);
    vec![
        ColumnFamilyDescriptor::new(CF_OBJECTS, Options::default()),
        ColumnFamilyDescriptor::new(CF_INTENTS, Options::default()),
        ColumnFamilyDescriptor::new(CF_COUNTERS, counter_opts),
        ColumnFamilyDescriptor::new(CF_META, Options::default()),
        ColumnFamilyDescriptor::new(CF_CHANGELOG, Options::default()),
        ColumnFamilyDescriptor::new(CF_TOMBSTONES, Options::default()),
        ColumnFamilyDescriptor::new(CF_VERSIONS, Options::default()),
    ]
}

fn open_db(path: &Path) -> Result<Db> {
    Ok(Db::open_cf_descriptors(&db_options(), path, column_families())?)
}

fn cf<'a>(db: &'a Db, name: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'a>>> {
    db.cf_handle(name)
        .ok_or_else(|| StorageError::Db(format!("missing column family {name}")))
}

fn write_opts(durability: Durability) -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(durability == Durability::Full);
    opts
}

/// Highest live intent id in the DB, or 0 if none. Used to seed the allocator.
fn max_intent_id(db: &Db) -> Result<i64> {
    let cf = cf(db, CF_INTENTS)?;
    let mut it = db.raw_iterator_cf(&cf);
    it.seek_to_last();
    it.status()?;
    Ok(match it.key() {
        Some(k) => id_from_key(k),
        None => 0,
    })
}

/// Highest changelog seq ever allocated: the newest entry, or the trim
/// floor when every entry has been trimmed.
fn last_change_seq(db: &Db) -> Result<u64> {
    let log = cf(db, CF_CHANGELOG)?;
    let mut it = db.raw_iterator_cf(&log);
    it.seek_to_last();
    it.status()?;
    let newest = it.key().map(changelog::seq_from_key).unwrap_or(0);
    Ok(newest.max(trimmed_through(db)?))
}

/// Commit time of the newest entry (0 when the log is empty): seeds the
/// non-decreasing commit clock on open.
fn last_change_ts(db: &Db) -> Result<i64> {
    let log = cf(db, CF_CHANGELOG)?;
    let mut it = db.raw_iterator_cf(&log);
    it.seek_to_last();
    it.status()?;
    Ok(match (it.key(), it.value()) {
        (Some(k), Some(v)) => changelog::decode_change(changelog::seq_from_key(k), v)
            .map(|c| c.ts_ms)
            .unwrap_or(0),
        _ => 0,
    })
}

fn trimmed_ts(db: &Db) -> Result<i64> {
    let meta = cf(db, CF_META)?;
    Ok(db
        .get_cf(&meta, KEY_TRIMMED_TS)?
        .map(|b| changelog::decode_u64(&b) as i64)
        .unwrap_or(i64::MIN))
}

fn trimmed_through(db: &Db) -> Result<u64> {
    let meta = cf(db, CF_META)?;
    Ok(db
        .get_cf(&meta, KEY_TRIMMED_THROUGH)?
        .map(|b| changelog::decode_u64(&b))
        .unwrap_or(0))
}

/// Reads the log id, minting one on first open.
fn ensure_log_id(db: &Db) -> Result<String> {
    let meta = cf(db, CF_META)?;
    if let Some(bytes) = db.get_cf(&meta, KEY_CHANGELOG_ID)? {
        return Ok(String::from_utf8_lossy(&bytes).into_owned());
    }
    use rand::Rng;
    let mut buf = [0u8; 8];
    rand::rng().fill_bytes(&mut buf);
    let id: String = buf.iter().map(|b| format!("{b:02x}")).collect();
    let mut opts = WriteOptions::default();
    opts.set_sync(true);
    db.put_cf_opt(&meta, KEY_CHANGELOG_ID, id.as_bytes(), &opts)?;
    Ok(id)
}

/// Returns true when the bucket needs a (blocking) rebuild before it can serve:
/// the database is missing while blobs exist, or the schema version is not
/// current.
pub async fn needs_rebuild(bucket_dir: &Path) -> Result<bool> {
    let db_path = index_db_path(bucket_dir);
    if !db_path.exists() {
        // Fresh DB is only acceptable when there are no blobs to lose.
        let objects_dir = bucket_dir.join("objects");
        let mut has_blobs = false;
        if let Ok(mut entries) = tokio::fs::read_dir(&objects_dir).await {
            has_blobs = entries.next_entry().await?.is_some();
        }
        return Ok(has_blobs);
    }
    let db_path = db_path.clone();
    run_blocking(move || {
        let db = open_db(&db_path)?;
        let meta = cf(&db, CF_META)?;
        let version = match db.get_cf(&meta, KEY_SCHEMA_VERSION)? {
            Some(bytes) => decode_counter(&bytes),
            None => 0,
        };
        Ok(version != SCHEMA_VERSION)
    })
    .await
}

async fn run_blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(err) => Err(StorageError::Db(format!("index task panicked: {err}"))),
    }
}

impl ObjectIndex {
    /// Opens (creating if absent) a current-schema index. Fails with
    /// [`StorageError::IndexOutdated`] if the on-disk schema is not current —
    /// callers must run a rebuild first; this function never migrates.
    ///
    /// RocksDB manages its own read concurrency, so there is no reader-pool
    /// size to configure.
    pub async fn open(bucket_dir: &Path, durability: Durability) -> Result<Self> {
        tokio::fs::create_dir_all(bucket_dir).await?;
        let db_path = index_db_path(bucket_dir);
        let existed = db_path.exists();
        let index = Self::open_at(&db_path, durability).await?;
        let version = index.schema_version().await?;
        match version {
            Some(v) if v == SCHEMA_VERSION => Ok(index),
            Some(v) if existed => {
                index.close().await;
                Err(StorageError::IndexOutdated(format!(
                    "schema version {v}, expected {SCHEMA_VERSION}"
                )))
            }
            // Absent (brand-new DB) or present-but-fresh: stamp the current
            // schema. A newer RocksDB directory would have failed the arm
            // above; a legacy SQLite `index.sqlite` is a different path
            // entirely and is handled by `needs_rebuild` (missing RocksDB dir).
            _ => {
                index.create_schema().await?;
                Ok(index)
            }
        }
    }

    /// Opens a database directly (used by the rebuild pipeline for the
    /// temporary database it constructs). Does not stamp the schema version —
    /// call [`create_schema`](Self::create_schema) for that.
    pub async fn open_at(db_path: &Path, durability: Durability) -> Result<Self> {
        let path = db_path.to_path_buf();
        let (db, seed, last_change, last_ts, log_id) = run_blocking(move || {
            let db = open_db(&path)?;
            let seed = max_intent_id(&db)?;
            let last_change = last_change_seq(&db)?;
            let last_ts = last_change_ts(&db)?;
            let log_id = ensure_log_id(&db)?;
            Ok((db, seed, last_change, last_ts, log_id))
        })
        .await?;
        Ok(Self {
            db: Arc::new(db),
            durability,
            intent_seq: Arc::new(AtomicI64::new(seed)),
            changes: Arc::new(SeqTracker::new(last_change, last_ts)),
            log_id: Arc::new(log_id),
            commit_hook: None,
        })
    }

    /// Installs the post-commit notification (the store wires this to the
    /// webhook dispatcher when it opens a bucket's index).
    pub fn with_commit_hook(mut self, hook: CommitHook) -> Self {
        self.commit_hook = Some(hook);
        self
    }

    fn notify_commit(&self) {
        if let Some(hook) = &self.commit_hook {
            hook(self.changes.watermark());
        }
    }

    async fn schema_version(&self) -> Result<Option<i64>> {
        let db = self.db.clone();
        run_blocking(move || {
            let meta = cf(&db, CF_META)?;
            Ok(db.get_cf(&meta, KEY_SCHEMA_VERSION)?.map(|b| decode_counter(&b)))
        })
        .await
    }

    pub async fn create_schema(&self) -> Result<()> {
        let db = self.db.clone();
        let durability = self.durability;
        run_blocking(move || {
            let meta = cf(&db, CF_META)?;
            db.put_cf_opt(
                &meta,
                KEY_SCHEMA_VERSION,
                SCHEMA_VERSION.to_le_bytes(),
                &write_opts(durability),
            )?;
            Ok(())
        })
        .await
    }

    /// Flushes memtables and the WAL to SST/disk. RocksDB has no WAL-checkpoint
    /// concept; the name is retained from the previous backend. The rebuild
    /// pipeline calls this before dropping the temp DB so its directory is
    /// complete before the atomic swap.
    pub async fn close(&self) {
        let db = self.db.clone();
        let _ = run_blocking(move || {
            let _ = db.flush();
            for name in [CF_OBJECTS, CF_INTENTS, CF_COUNTERS, CF_META, CF_CHANGELOG, CF_TOMBSTONES, CF_VERSIONS] {
                if let Ok(handle) = cf(&db, name) {
                    let _ = db.flush_cf(&handle);
                }
            }
            let _ = db.flush_wal(true);
            Ok(())
        })
        .await;
    }

    /// Syncs the write-ahead log to stable storage. Relaxed-durability writes
    /// leave the WAL in the OS page cache; callers that are about to act on a
    /// commit irreversibly (e.g. retire the blob it displaced) sync it first.
    pub async fn sync_wal(&self) -> Result<()> {
        let db = self.db.clone();
        run_blocking(move || Ok(db.flush_wal(true)?)).await
    }

    /// Number of live handles (clones of this index, plus in-flight blocking
    /// calls) sharing the underlying database. Used to drain in-flight users
    /// before the database is swapped out or deleted.
    pub fn handle_count(&self) -> usize {
        Arc::strong_count(&self.db)
    }

    pub async fn checkpoint_truncate(&self) -> Result<()> {
        self.close().await;
        Ok(())
    }

    // ── objects ─────────────────────────────────────────────────────────────

    pub async fn get(&self, key: &str) -> Result<Option<ObjectRecord>> {
        let db = self.db.clone();
        let key = key.to_string();
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            match db.get_cf(&objects, key.as_bytes())? {
                Some(value) => Ok(Some(decode_object(key.as_bytes(), &value)?)),
                None => Ok(None),
            }
        })
        .await
    }

    /// Repoints a key's row at a new blob dir — the atomic pivot of a layout
    /// migration (hardlink → **flip** → unlink). The `expected` guard makes it
    /// a no-op if the row moved under us (e.g. a concurrent overwrite), so it
    /// can never clobber a newer version. Returns whether a row was updated.
    /// Called under the per-key write lock, so the read-modify-write is
    /// serialized against other mutations of this key.
    pub async fn update_blob_dir(&self, key: &str, expected: &str, new_blob_dir: &str) -> Result<bool> {
        let db = self.db.clone();
        let durability = self.durability;
        let key = key.to_string();
        let expected = expected.to_string();
        let new_blob_dir = new_blob_dir.to_string();
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let Some(value) = db.get_cf(&objects, key.as_bytes())? else {
                return Ok(false);
            };
            let mut record = decode_object(key.as_bytes(), &value)?;
            if record.blob_dir != expected {
                return Ok(false);
            }
            record.blob_dir = new_blob_dir;
            db.put_cf_opt(&objects, key.as_bytes(), encode_object(&record), &write_opts(durability))?;
            Ok(true)
        })
        .await
    }

    /// Rewrites a row's attributes from the blob's `meta.json` — the health
    /// scan's drift repair. `meta.json` is what `read_object` actually serves,
    /// so when the two disagree it is the row that is wrong. The `expected`
    /// blob-dir guard makes it a no-op if the row moved under us (a concurrent
    /// overwrite), so a stale finding can never clobber a newer version.
    /// Returns whether a row was updated. Called under the per-key write lock.
    pub async fn update_object_attrs(
        &self,
        key: &str,
        expected_blob_dir: &str,
        size: u64,
        etag: &str,
        last_modified_ms: i64,
        parts: Option<PartLayout>,
        stamp: Option<Stamp>,
    ) -> Result<bool> {
        let db = self.db.clone();
        let durability = self.durability;
        let key = key.to_string();
        let expected = expected_blob_dir.to_string();
        let etag = etag.to_string();
        let changes = self.changes.clone();
        let updated = run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let Some(value) = db.get_cf(&objects, key.as_bytes())? else {
                return Ok(false);
            };
            let mut record = decode_object(key.as_bytes(), &value)?;
            if record.blob_dir != expected {
                return Ok(false);
            }
            record.size = size;
            record.etag = etag;
            record.last_modified_ms = last_modified_ms;
            record.parts = parts;
            record.stamp = stamp;
            let log = cf(&db, CF_CHANGELOG)?;
            let mut batch = WriteBatch::default();
            batch.put_cf(&objects, key.as_bytes(), encode_object(&record));
            let guard = changes.begin(super::time::now_ms());
            batch.put_cf(
                &log,
                changelog::seq_key(guard.seq),
                changelog::encode_change(&change_for(&guard, ChangeOp::Put, &record, ChangeOrigin::Repair)),
            );
            db.write_opt(batch, &write_opts(durability))?;
            Ok(true)
        })
        .await;
        if matches!(updated, Ok(true)) {
            self.notify_commit();
        }
        updated
    }


    pub async fn is_empty(&self) -> Result<bool> {
        let db = self.db.clone();
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let mut it = db.raw_iterator_cf(&objects);
            it.seek_to_first();
            it.status()?;
            Ok(it.key().is_none())
        })
        .await
    }

    pub async fn object_count(&self) -> Result<u64> {
        let db = self.db.clone();
        run_blocking(move || {
            let counters = cf(&db, CF_COUNTERS)?;
            let count = match db.get_cf(&counters, KEY_OBJECT_COUNT)? {
                Some(bytes) => decode_counter(&bytes),
                None => 0,
            };
            Ok(count.max(0) as u64)
        })
        .await
    }

    // ── intents ─────────────────────────────────────────────────────────────

    fn next_intent_id(&self) -> i64 {
        self.intent_seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Records a `publish` intent. Must be committed before any rename into the
    /// live tree.
    pub async fn insert_publish_intent(&self, key: &str, blob_dir: &str, now_ms: i64) -> Result<i64> {
        let db = self.db.clone();
        let durability = self.durability;
        let id = self.next_intent_id();
        let key = key.to_string();
        let blob_dir = blob_dir.to_string();
        run_blocking(move || {
            let intents = cf(&db, CF_INTENTS)?;
            let value = encode_intent(INTENT_PUBLISH, &key, &blob_dir, now_ms, 0);
            db.put_cf_opt(&intents, id_key(id), value, &write_opts(durability))?;
            Ok(id)
        })
        .await
    }

    /// The atomic commit point of a PUT/copy/multipart-complete: writes the
    /// object, removes the publish intent, and (for overwrites) inserts a
    /// `retire` intent for the displaced blob dir — all in one write batch.
    /// Returns the retire intent id when an old dir was displaced.
    ///
    /// Runs under the per-key write lock, so reading the prior existence of the
    /// key to decide the counter delta is race-free against other mutations of
    /// this key; cross-key contention on the counter is resolved by the merge
    /// operator.
    pub async fn commit_publish(
        &self,
        record: &ObjectRecord,
        publish_intent_id: i64,
        displaced_blob_dir: Option<&str>,
        now_ms: i64,
    ) -> Result<Option<i64>> {
        let displaced = displaced_blob_dir.map(Displaced::retire).unwrap_or_default();
        let ids = self
            .commit_publish_from(record, publish_intent_id, displaced, now_ms, ChangeOrigin::Client)
            .await?;
        Ok(ids.first().copied())
    }

    /// [`Self::commit_publish`] recording who caused the change in the
    /// changelog entry it appends to the same batch, and doing with the
    /// states it displaces what `displaced` says: a `retire` intent per dir
    /// to retire, a `versions` row per version to keep. The key's tombstone
    /// (if any) is removed: it was either kept as a version or its marker
    /// dir listed for retirement by the caller. Returns the retire intent
    /// ids, in `displaced.retire` order.
    pub async fn commit_publish_from(
        &self,
        record: &ObjectRecord,
        publish_intent_id: i64,
        displaced: Displaced,
        now_ms: i64,
        origin: ChangeOrigin,
    ) -> Result<Vec<i64>> {
        let db = self.db.clone();
        let durability = self.durability;
        let record = record.clone();
        let retire_ids: Vec<i64> = displaced.retire.iter().map(|_| self.next_intent_id()).collect();
        let changes = self.changes.clone();
        let committed = run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let intents = cf(&db, CF_INTENTS)?;
            let counters = cf(&db, CF_COUNTERS)?;
            let log = cf(&db, CF_CHANGELOG)?;
            let tombstones = cf(&db, CF_TOMBSTONES)?;
            let versions = cf(&db, CF_VERSIONS)?;
            let existed = db.get_cf(&objects, record.object_key.as_bytes())?.is_some();

            let mut batch = WriteBatch::default();
            batch.put_cf(&objects, record.object_key.as_bytes(), encode_object(&record));
            batch.delete_cf(&tombstones, record.object_key.as_bytes());
            batch.delete_cf(&intents, id_key(publish_intent_id));
            for (old_dir, id) in displaced.retire.iter().zip(&retire_ids) {
                let value = encode_intent(INTENT_RETIRE, &record.object_key, old_dir, now_ms, 0);
                batch.put_cf(&intents, id_key(*id), value);
            }
            for kept in &displaced.keep {
                batch.put_cf(&versions, versions_key(&kept.object_key, &kept.version_dir()), encode_version(kept));
            }
            for dir in &displaced.drop_versions {
                batch.delete_cf(&versions, versions_key(&record.object_key, dir));
            }
            if !existed {
                batch.merge_cf(&counters, KEY_OBJECT_COUNT, 1i64.to_le_bytes());
            }
            let guard = changes.begin(now_ms);
            let change = change_for(&guard, ChangeOp::Put, &record, origin);
            batch.put_cf(&log, changelog::seq_key(guard.seq), changelog::encode_change(&change));
            db.write_opt(batch, &write_opts(durability))?;
            Ok(retire_ids)
        })
        .await;
        if committed.is_ok() {
            self.notify_commit();
        }
        committed
    }

    /// The atomic commit point of a DELETE: removes the object and records a
    /// `retire` intent for its blob dir in one write batch.
    pub async fn commit_delete(&self, key: &str, blob_dir: &str, now_ms: i64) -> Result<i64> {
        let ids = self
            .commit_delete_from(key, Displaced::retire(blob_dir), now_ms, ChangeOrigin::Client, None, None, None, false)
            .await?;
        Ok(ids[0])
    }

    /// [`Self::commit_delete`] recording who caused the change and the
    /// delete's own version stamp, and doing with the displaced states what
    /// `displaced` says (retire dirs, keep versions). With a `stamp`, the
    /// key's tombstone is (re)written: it names the removed row, and — when
    /// the delete wrote a marker dir (`marker_dir`) — where that marker is
    /// and whether it is a real (`versioned`) delete marker. A changelog
    /// entry is appended only when a row was actually removed. Returns the
    /// retire intent ids, in `displaced.retire` order.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_delete_from(
        &self,
        key: &str,
        displaced: Displaced,
        now_ms: i64,
        origin: ChangeOrigin,
        stamp: Option<Stamp>,
        removed: Option<Stamp>,
        marker_dir: Option<String>,
        versioned: bool,
    ) -> Result<Vec<i64>> {
        let db = self.db.clone();
        let durability = self.durability;
        let retire_ids: Vec<i64> = displaced.retire.iter().map(|_| self.next_intent_id()).collect();
        let key = key.to_string();
        let changes = self.changes.clone();
        let committed = run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let intents = cf(&db, CF_INTENTS)?;
            let counters = cf(&db, CF_COUNTERS)?;
            let log = cf(&db, CF_CHANGELOG)?;
            let versions = cf(&db, CF_VERSIONS)?;
            let existing = db
                .get_cf(&objects, key.as_bytes())?
                .map(|v| decode_object(key.as_bytes(), &v))
                .transpose()?;

            let mut batch = WriteBatch::default();
            batch.delete_cf(&objects, key.as_bytes());
            for (dir, id) in displaced.retire.iter().zip(&retire_ids) {
                let value = encode_intent(INTENT_RETIRE, &key, dir, now_ms, 0);
                batch.put_cf(&intents, id_key(*id), value);
            }
            for kept in &displaced.keep {
                batch.put_cf(&versions, versions_key(&kept.object_key, &kept.version_dir()), encode_version(kept));
            }
            for dir in &displaced.drop_versions {
                batch.delete_cf(&versions, versions_key(&key, dir));
            }
            let mut guard = None;
            if let Some(existing) = &existing {
                batch.merge_cf(&counters, KEY_OBJECT_COUNT, (-1i64).to_le_bytes());
                let g = changes.begin(now_ms);
                let change = change_for_delete(&g, existing, origin, stamp.clone(), removed.clone());
                if let Some(stamp) = &stamp {
                    let tombstone = Tombstone {
                        object_key: key.clone(),
                        stamp: stamp.clone(),
                        removed: removed.clone(),
                        deleted_at_ms: now_ms,
                        version: changelog::version_of(&existing.blob_dir),
                        etag: existing.etag.clone(),
                        size: existing.size,
                        versioned,
                        blob_dir: marker_dir.clone(),
                    };
                    batch.put_cf(&cf(&db, CF_TOMBSTONES)?, key.as_bytes(), encode_tombstone(&tombstone));
                }
                batch.put_cf(&log, changelog::seq_key(g.seq), changelog::encode_change(&change));
                guard = Some(g);
            }
            db.write_opt(batch, &write_opts(durability))?;
            drop(guard);
            Ok((retire_ids, existing.is_some()))
        })
        .await;
        match committed {
            Ok((ids, logged)) => {
                if logged {
                    self.notify_commit();
                }
                Ok(ids)
            }
            Err(err) => Err(err),
        }
    }

    /// Permanently removes one version of `key` (`DeleteObject?versionId`):
    /// the current row, the current delete marker, or a noncurrent version.
    /// When the current state goes, `promote` (the newest remaining version,
    /// found by the caller) becomes current: a live version is put back as
    /// the row, a marker as the tombstone. Everything is one write batch.
    /// Logged as a `delete` of the removed version (and a `put` of a
    /// promoted live version). Returns the retire intent for the removed
    /// blob dir, or `None` when there was nothing to remove.
    pub async fn commit_remove_version(
        &self,
        key: &str,
        target: VersionTarget,
        promote: Option<VersionRecord>,
        now_ms: i64,
        origin: ChangeOrigin,
        stamp: Stamp,
    ) -> Result<Option<(i64, String)>> {
        let db = self.db.clone();
        let durability = self.durability;
        let id = self.next_intent_id();
        let key = key.to_string();
        let changes = self.changes.clone();
        let committed = run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let intents = cf(&db, CF_INTENTS)?;
            let counters = cf(&db, CF_COUNTERS)?;
            let log = cf(&db, CF_CHANGELOG)?;
            let versions = cf(&db, CF_VERSIONS)?;
            let tombstones = cf(&db, CF_TOMBSTONES)?;
            let mut batch = WriteBatch::default();
            let mut delta = 0i64;
            let mut guards = Vec::new();
            let removed_dir: Option<String> = match &target {
                VersionTarget::Current => {
                    let Some(existing) = db.get_cf(&objects, key.as_bytes())?.map(|v| decode_object(key.as_bytes(), &v)).transpose()? else {
                        return Ok(None);
                    };
                    batch.delete_cf(&objects, key.as_bytes());
                    delta -= 1;
                    let g = changes.begin(now_ms);
                    let change = change_for_delete(&g, &existing, origin, Some(stamp.clone()), existing.stamp.clone());
                    batch.put_cf(&log, changelog::seq_key(g.seq), changelog::encode_change(&change));
                    guards.push(g);
                    Some(existing.blob_dir)
                }
                VersionTarget::Marker => {
                    let Some(t) = db.get_cf(&tombstones, key.as_bytes())?.map(|v| decode_tombstone(key.as_bytes(), &v)).transpose()? else {
                        return Ok(None);
                    };
                    batch.delete_cf(&tombstones, key.as_bytes());
                    t.blob_dir
                }
                VersionTarget::Noncurrent(dir) => {
                    let raw = versions_key(&key, dir);
                    let Some(v) = db.get_cf(&versions, &raw)?.map(|v| decode_version(&raw, &v)).transpose()? else {
                        return Ok(None);
                    };
                    batch.delete_cf(&versions, &raw);
                    if !v.delete_marker {
                        let g = changes.begin(now_ms);
                        let change = change_for_delete(&g, &v.clone().into_row(), origin, Some(stamp.clone()), Some(v.stamp.clone()));
                        batch.put_cf(&log, changelog::seq_key(g.seq), changelog::encode_change(&change));
                        guards.push(g);
                    }
                    Some(v.blob_dir)
                }
            };
            if !matches!(target, VersionTarget::Noncurrent(_)) {
                if let Some(next) = promote {
                    batch.delete_cf(&versions, versions_key(&next.object_key, &next.version_dir()));
                    if next.delete_marker {
                        batch.put_cf(&tombstones, key.as_bytes(), encode_tombstone(&next.into_tombstone(now_ms)));
                    } else {
                        let row = next.into_row();
                        batch.put_cf(&objects, key.as_bytes(), encode_object(&row));
                        delta += 1;
                        let g = changes.begin(now_ms);
                        let change = change_for(&g, ChangeOp::Put, &row, origin);
                        batch.put_cf(&log, changelog::seq_key(g.seq), changelog::encode_change(&change));
                        guards.push(g);
                    }
                }
            }
            let Some(dir) = removed_dir else {
                db.write_opt(batch, &write_opts(durability))?;
                drop(guards);
                return Ok(None);
            };
            batch.put_cf(&intents, id_key(id), encode_intent(INTENT_RETIRE, &key, &dir, now_ms, 0));
            if delta != 0 {
                batch.merge_cf(&counters, KEY_OBJECT_COUNT, delta.to_le_bytes());
            }
            db.write_opt(batch, &write_opts(durability))?;
            drop(guards);
            Ok(Some((id, dir)))
        })
        .await;
        if committed.is_ok() {
            self.notify_commit();
        }
        committed
    }

    // ── versions ────────────────────────────────────────────────────────────

    /// A noncurrent version of `key` by its version dir.
    pub async fn get_version(&self, key: &str, version_dir: &str) -> Result<Option<VersionRecord>> {
        let db = self.db.clone();
        let raw = versions_key(key, version_dir);
        run_blocking(move || {
            let versions = cf(&db, CF_VERSIONS)?;
            match db.get_cf(&versions, &raw)? {
                Some(value) => Ok(Some(decode_version(&raw, &value)?)),
                None => Ok(None),
            }
        })
        .await
    }

    /// The noncurrent versions of `key`, newest first.
    pub async fn versions_of(&self, key: &str) -> Result<Vec<VersionRecord>> {
        let db = self.db.clone();
        let prefix = versions_key(key, "");
        run_blocking(move || {
            let versions = cf(&db, CF_VERSIONS)?;
            let mut out = Vec::new();
            for item in db.iterator_cf(&versions, IteratorMode::From(&prefix, Direction::Forward)) {
                let (k, v) = item?;
                if !k.starts_with(&prefix) {
                    break;
                }
                out.push(decode_version(&k, &v)?);
            }
            out.reverse();
            Ok(out)
        })
        .await
    }

    /// Up to `limit` noncurrent versions in `versions`-key order, strictly
    /// after the raw key `after` (the scan's cursor over the whole family).
    pub async fn all_versions_after(&self, after: Option<&[u8]>, limit: usize) -> Result<Vec<(Vec<u8>, VersionRecord)>> {
        let db = self.db.clone();
        let after = after.map(|a| a.to_vec());
        run_blocking(move || {
            let versions = cf(&db, CF_VERSIONS)?;
            let mode = match &after {
                Some(after) => IteratorMode::From(after, Direction::Forward),
                None => IteratorMode::Start,
            };
            let mut out = Vec::new();
            for item in db.iterator_cf(&versions, mode) {
                let (k, v) = item?;
                if after.as_deref().is_some_and(|a| a == k.as_ref()) {
                    continue;
                }
                out.push((k.to_vec(), decode_version(&k, &v)?));
                if out.len() >= limit {
                    break;
                }
            }
            Ok(out)
        })
        .await
    }

    /// Every version of every key under `prefix` in key order, newest first
    /// per key, with whether each is the key's current state. Starts after
    /// `key_marker` (and, for that key, after `version_marker`); a key's
    /// versions may be split across pages. Rows without a stamp (from before
    /// stamps) get the fallback stamp for `site`. `null` delete markers are
    /// listed only when `null_markers` (a suspended bucket's deletes).
    #[allow(clippy::too_many_arguments)]
    pub async fn list_versions_page(
        &self,
        prefix: &str,
        key_marker: Option<&str>,
        version_marker: Option<&str>,
        max: usize,
        site: &str,
        null_markers: bool,
    ) -> Result<VersionsPage> {
        let db = self.db.clone();
        let prefix = prefix.to_string();
        let key_marker = key_marker.map(str::to_string);
        let version_marker = version_marker.map(str::to_string);
        let site = site.to_string();
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let tombstones = cf(&db, CF_TOMBSTONES)?;
            let versions = cf(&db, CF_VERSIONS)?;
            let start = match &key_marker {
                Some(m) if m.as_str() > prefix.as_str() => m.clone(),
                _ => prefix.clone(),
            };
            let mut rows = db.raw_iterator_cf(&objects);
            rows.seek(start.as_bytes());
            let mut tombs = db.raw_iterator_cf(&tombstones);
            tombs.seek(start.as_bytes());
            let mut page = VersionsPage::default();
            loop {
                rows.status()?;
                tombs.status()?;
                let row_key = rows.key().map(|k| k.to_vec());
                let tomb_key = tombs.key().map(|k| k.to_vec());
                let key = match (&row_key, &tomb_key) {
                    (None, None) => break,
                    (Some(a), Some(b)) => a.min(b).clone(),
                    (Some(a), None) => a.clone(),
                    (None, Some(b)) => b.clone(),
                };
                if !key.starts_with(prefix.as_bytes()) {
                    break;
                }
                let key_str = String::from_utf8_lossy(&key).into_owned();
                // The key's current state, then its noncurrent versions.
                let mut current: Vec<VersionRecord> = Vec::new();
                if row_key.as_deref() == Some(key.as_slice()) {
                    let row = decode_object(&key, rows.value().unwrap_or_default())?;
                    let stamp = row.stamp.clone().unwrap_or_else(|| Stamp::fallback(row.last_modified_ms, &site));
                    current.push(VersionRecord::from_row(&row, stamp));
                    rows.next();
                }
                if tomb_key.as_deref() == Some(key.as_slice()) {
                    let t = decode_tombstone(&key, tombs.value().unwrap_or_default())?;
                    if t.versioned || null_markers {
                        if let Some(v) = VersionRecord::from_tombstone(&t) {
                            current.push(v);
                        }
                    }
                    tombs.next();
                }
                current.sort_by(|a, b| b.stamp.cmp(&a.stamp));
                let after_marker = key_marker.as_deref() == Some(key_str.as_str());
                if after_marker && version_marker.is_none() {
                    continue; // the whole key was listed on the previous page
                }
                let vprefix = versions_key(&key_str, "");
                let mut noncurrent = Vec::new();
                for item in db.iterator_cf(&versions, IteratorMode::From(&vprefix, Direction::Forward)) {
                    let (k, v) = item?;
                    if !k.starts_with(&vprefix) {
                        break;
                    }
                    noncurrent.push(decode_version(&k, &v)?);
                }
                noncurrent.reverse();
                let n = current.len();
                for (i, v) in current.into_iter().chain(noncurrent).enumerate() {
                    if v.delete_marker && !v.versioned && !null_markers {
                        continue;
                    }
                    let vdir = v.version_dir();
                    if after_marker && version_marker.as_deref().is_some_and(|m| vdir.as_str() >= m) {
                        continue;
                    }
                    if page.entries.len() >= max {
                        page.is_truncated = true;
                        return Ok(page);
                    }
                    page.next_key_marker = Some(key_str.clone());
                    page.next_version_marker = Some(vdir);
                    page.entries.push((v, i == 0 && n > 0));
                }
            }
            Ok(page)
        })
        .await
    }

    /// Repoints a row at its relocated blob dir and records the stamp the
    /// relocation materialised — the atomic pivot of a layout upgrade
    /// (hardlink → **flip** → unlink). The `expected` guard makes it a no-op
    /// if the row moved under us (a concurrent overwrite), so it can never
    /// clobber a newer version. Returns whether a row was updated. Called
    /// under the per-key write lock.
    pub async fn relocate_row(&self, key: &str, expected: &str, new_blob_dir: &str, stamp: &Stamp) -> Result<bool> {
        let db = self.db.clone();
        let durability = self.durability;
        let key = key.to_string();
        let expected = expected.to_string();
        let new_blob_dir = new_blob_dir.to_string();
        let stamp = stamp.clone();
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let Some(value) = db.get_cf(&objects, key.as_bytes())? else {
                return Ok(false);
            };
            let mut record = decode_object(key.as_bytes(), &value)?;
            if record.blob_dir != expected {
                return Ok(false);
            }
            record.blob_dir = new_blob_dir;
            record.stamp = Some(stamp);
            db.put_cf_opt(&objects, key.as_bytes(), encode_object(&record), &write_opts(durability))?;
            Ok(true)
        })
        .await
    }

    /// Up to `limit` keys whose row is not on the current layout, or has no
    /// stamp, strictly after `after`: what a layout upgrade still has to
    /// relocate. Empty once the upgrade is complete.
    pub async fn rows_off_layout(&self, after: Option<&str>, limit: usize) -> Result<Vec<String>> {
        let db = self.db.clone();
        let after = after.map(str::to_string);
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let mut keys = Vec::new();
            let mode = match &after {
                Some(after) => IteratorMode::From(after.as_bytes(), Direction::Forward),
                None => IteratorMode::Start,
            };
            for item in db.iterator_cf(&objects, mode) {
                let (key, value) = item?;
                if after.as_deref().is_some_and(|after| key.as_ref() == after.as_bytes()) {
                    continue; // `after` is exclusive
                }
                let record = decode_object(&key, &value)?;
                if !super::layouts::is_current(&record.blob_dir) || record.stamp.is_none() {
                    keys.push(record.object_key);
                    if keys.len() >= limit {
                        break;
                    }
                }
            }
            Ok(keys)
        })
        .await
    }

    /// Up to `limit` tombstones with no delete-marker dir on disk, strictly
    /// after `after`: what a layout upgrade still has to materialise.
    pub async fn tombstones_without_marker(&self, after: Option<&str>, limit: usize) -> Result<Vec<Tombstone>> {
        let db = self.db.clone();
        let after = after.map(str::to_string);
        run_blocking(move || {
            let tombstones = cf(&db, CF_TOMBSTONES)?;
            let mut out = Vec::new();
            let mode = match &after {
                Some(after) => IteratorMode::From(after.as_bytes(), Direction::Forward),
                None => IteratorMode::Start,
            };
            for item in db.iterator_cf(&tombstones, mode) {
                let (key, value) = item?;
                if after.as_deref().is_some_and(|after| key.as_ref() == after.as_bytes()) {
                    continue;
                }
                let t = decode_tombstone(&key, &value)?;
                if t.blob_dir.is_none() {
                    out.push(t);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
            Ok(out)
        })
        .await
    }

    /// Records where a tombstone's marker dir was written, if the tombstone
    /// is still the one given (same stamp). Returns whether it was updated.
    pub async fn set_tombstone_marker(&self, key: &str, stamp: &Stamp, blob_dir: &str) -> Result<bool> {
        let db = self.db.clone();
        let durability = self.durability;
        let key = key.to_string();
        let stamp = stamp.clone();
        let blob_dir = blob_dir.to_string();
        run_blocking(move || {
            let tombstones = cf(&db, CF_TOMBSTONES)?;
            let Some(value) = db.get_cf(&tombstones, key.as_bytes())? else {
                return Ok(false);
            };
            let mut t = decode_tombstone(key.as_bytes(), &value)?;
            if t.stamp != stamp {
                return Ok(false);
            }
            t.blob_dir = Some(blob_dir);
            db.put_cf_opt(&tombstones, key.as_bytes(), encode_tombstone(&t), &write_opts(durability))?;
            Ok(true)
        })
        .await
    }

    // ── replication checkpoints ─────────────────────────────────────────────

    /// A replication link's checkpoint, stored in this (the target) bucket's
    /// index: it rolls back with the data if the index is restored, and an
    /// index rebuild carries it over.
    pub async fn get_checkpoint(&self, link_id: &str) -> Result<Option<Vec<u8>>> {
        let db = self.db.clone();
        let key = format!("{CHECKPOINT_PREFIX}{link_id}");
        run_blocking(move || Ok(db.get_cf(&cf(&db, CF_META)?, key.as_bytes())?)).await
    }

    /// Saves (`Some`) or clears (`None`) a link's checkpoint. Synced: a
    /// checkpoint ahead of applied data would skip changes.
    pub async fn put_checkpoint(&self, link_id: &str, value: Option<Vec<u8>>) -> Result<()> {
        let db = self.db.clone();
        let key = format!("{CHECKPOINT_PREFIX}{link_id}");
        run_blocking(move || {
            let meta = cf(&db, CF_META)?;
            let mut opts = WriteOptions::default();
            opts.set_sync(true);
            match value {
                Some(value) => db.put_cf_opt(&meta, key.as_bytes(), value, &opts)?,
                None => db.delete_cf_opt(&meta, key.as_bytes(), &opts)?,
            }
            Ok(())
        })
        .await
    }

    // ── tombstones ──────────────────────────────────────────────────────────

    pub async fn get_tombstone(&self, key: &str) -> Result<Option<Tombstone>> {
        let db = self.db.clone();
        let key = key.to_string();
        run_blocking(move || {
            let tombstones = cf(&db, CF_TOMBSTONES)?;
            match db.get_cf(&tombstones, key.as_bytes())? {
                Some(value) => Ok(Some(decode_tombstone(key.as_bytes(), &value)?)),
                None => Ok(None),
            }
        })
        .await
    }

    /// Records a delete of a key that has no object here (a replicated
    /// delete arriving first, or a delete on top of a delete marker), unless
    /// a tombstone that ranks at least as high exists. The displaced
    /// tombstone's marker is retired or kept as a version per `displaced`.
    /// Returns the retire intent ids when recorded, `None` when not. Logged as a `delete` (no ETag, size
    /// 0) in the same batch: nothing visible changed here, but the delete
    /// must still reach every other site and reader — a site that has the
    /// removed version (or an older one) would otherwise keep it while this
    /// one refuses it forever. Called under the key lock.
    pub async fn record_tombstone(
        &self,
        tombstone: Tombstone,
        displaced: Displaced,
        origin: ChangeOrigin,
    ) -> Result<Option<Vec<i64>>> {
        let db = self.db.clone();
        let durability = self.durability;
        let retire_ids: Vec<i64> = displaced.retire.iter().map(|_| self.next_intent_id()).collect();
        let changes = self.changes.clone();
        let recorded = run_blocking(move || {
            let tombstones = cf(&db, CF_TOMBSTONES)?;
            let intents = cf(&db, CF_INTENTS)?;
            let versions = cf(&db, CF_VERSIONS)?;
            let key = tombstone.object_key.clone();
            let removed = tombstone.removed.clone().unwrap_or_else(|| tombstone.stamp.clone());
            if let Some(existing) = db.get_cf(&tombstones, key.as_bytes())? {
                if decode_tombstone(key.as_bytes(), &existing)?.rank() >= super::stamp::Rank::deleted(removed.clone()) {
                    return Ok(None);
                }
            }
            let now_ms = tombstone.deleted_at_ms;
            let mut batch = WriteBatch::default();
            batch.put_cf(&tombstones, key.as_bytes(), encode_tombstone(&tombstone));
            for (dir, id) in displaced.retire.iter().zip(&retire_ids) {
                batch.put_cf(&intents, id_key(*id), encode_intent(INTENT_RETIRE, &key, dir, now_ms, 0));
            }
            for kept in &displaced.keep {
                batch.put_cf(&versions, versions_key(&kept.object_key, &kept.version_dir()), encode_version(kept));
            }
            for dir in &displaced.drop_versions {
                batch.delete_cf(&versions, versions_key(&key, dir));
            }
            let guard = changes.begin(now_ms);
            let change = change_for_tombstone(&guard, &key, origin, tombstone.stamp.clone(), removed);
            batch.put_cf(&cf(&db, CF_CHANGELOG)?, changelog::seq_key(guard.seq), changelog::encode_change(&change));
            db.write_opt(batch, &write_opts(durability))?;
            drop(guard);
            Ok(Some(retire_ids))
        })
        .await?;
        if recorded.is_some() {
            self.notify_commit();
        }
        Ok(recorded)
    }

    /// Removes tombstones of deletes made before `cutoff_ms`, at most
    /// `max_entries` per call. Returns the removed tombstones, so the caller
    /// can trash their marker dirs.
    pub async fn purge_tombstones(&self, cutoff_ms: i64, max_entries: usize) -> Result<Vec<Tombstone>> {
        let db = self.db.clone();
        let durability = self.durability;
        run_blocking(move || {
            let tombstones = cf(&db, CF_TOMBSTONES)?;
            let mut batch = WriteBatch::default();
            let mut removed = Vec::new();
            for item in db.iterator_cf(&tombstones, IteratorMode::Start) {
                let (key, value) = item?;
                let t = decode_tombstone(&key, &value);
                let expired = t.as_ref().map(|t| t.deleted_at_ms < cutoff_ms).unwrap_or(true);
                if expired {
                    batch.delete_cf(&tombstones, &key);
                    if let Ok(t) = t {
                        removed.push(t);
                    }
                    if batch.len() >= max_entries {
                        break;
                    }
                }
            }
            if !batch.is_empty() {
                db.write_opt(batch, &write_opts(durability))?;
            }
            Ok(removed)
        })
        .await
    }

    /// Tombstones under `prefix`, in key order, after `after`.
    pub async fn list_tombstones(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<(Vec<Tombstone>, bool)> {
        let db = self.db.clone();
        let prefix = prefix.to_string();
        let after = after.map(str::to_string);
        run_blocking(move || {
            let tombstones = cf(&db, CF_TOMBSTONES)?;
            let start = match &after {
                Some(after) if after.as_str() > prefix.as_str() => after.clone(),
                _ => prefix.clone(),
            };
            let mut out = Vec::new();
            let mut more = false;
            for item in db.iterator_cf(&tombstones, IteratorMode::From(start.as_bytes(), Direction::Forward)) {
                let (key, value) = item?;
                if after.as_deref().is_some_and(|a| a.as_bytes() == key.as_ref()) {
                    continue;
                }
                if !key.starts_with(prefix.as_bytes()) {
                    break;
                }
                if out.len() >= limit {
                    more = true;
                    break;
                }
                out.push(decode_tombstone(&key, &value)?);
            }
            Ok((out, more))
        })
        .await
    }

    pub async fn delete_intent(&self, id: i64) -> Result<()> {
        let db = self.db.clone();
        let durability = self.durability;
        run_blocking(move || {
            let intents = cf(&db, CF_INTENTS)?;
            db.delete_cf_opt(&intents, id_key(id), &write_opts(durability))?;
            Ok(())
        })
        .await
    }

    pub async fn bump_intent_attempts(&self, id: i64) -> Result<()> {
        let db = self.db.clone();
        let durability = self.durability;
        run_blocking(move || {
            let intents = cf(&db, CF_INTENTS)?;
            if let Some(value) = db.get_cf(&intents, id_key(id))? {
                let mut record = decode_intent(id, &value)?;
                record.attempts += 1;
                let value = encode_intent(
                    &record.op,
                    &record.object_key,
                    &record.blob_dir,
                    record.created_at_ms,
                    record.attempts,
                );
                db.put_cf_opt(&intents, id_key(id), value, &write_opts(durability))?;
            }
            Ok(())
        })
        .await
    }

    /// Re-fetches an intent by id. Resolvers must confirm the intent still
    /// exists *after* taking the key lock — acting on a stale snapshot could
    /// trash a blob whose publish has since committed.
    pub async fn get_intent(&self, id: i64) -> Result<Option<IntentRecord>> {
        let db = self.db.clone();
        run_blocking(move || {
            let intents = cf(&db, CF_INTENTS)?;
            match db.get_cf(&intents, id_key(id))? {
                Some(value) => Ok(Some(decode_intent(id, &value)?)),
                None => Ok(None),
            }
        })
        .await
    }

    /// Swaps a publish intent's blob dir (rename destination collided and a
    /// fresh name was chosen before anything touched the live tree).
    pub async fn update_intent_blob_dir(&self, id: i64, blob_dir: &str) -> Result<()> {
        let db = self.db.clone();
        let durability = self.durability;
        let blob_dir = blob_dir.to_string();
        run_blocking(move || {
            let intents = cf(&db, CF_INTENTS)?;
            if let Some(value) = db.get_cf(&intents, id_key(id))? {
                let record = decode_intent(id, &value)?;
                let value = encode_intent(
                    &record.op,
                    &record.object_key,
                    &blob_dir,
                    record.created_at_ms,
                    record.attempts,
                );
                db.put_cf_opt(&intents, id_key(id), value, &write_opts(durability))?;
            }
            Ok(())
        })
        .await
    }

    /// Intents at least `min_age_ms` old — candidates for background
    /// resolution, oldest first. Pass `min_age_ms = 0` for the startup drain.
    ///
    /// Intents are keyed by ascending id, and ids increase with insertion time,
    /// so old (stale) intents cluster at the front of the family: the scan
    /// stops as soon as `limit` matches are collected. Non-stale intents are
    /// bounded by the number of in-flight operations, so this is cheap even
    /// with a large retire backlog. The collected page is sorted by
    /// `(created_at_ms, id)` to match the previous ordering exactly.
    pub async fn stale_intents(&self, now_ms: i64, min_age_ms: i64, limit: i64) -> Result<Vec<IntentRecord>> {
        self.stale_intents_after(now_ms, min_age_ms, limit, None).await
    }

    /// [`Self::stale_intents`] resuming strictly after intent id `after_id`,
    /// so a resolution pass can walk past intents that keep failing instead of
    /// re-selecting them forever (they sit at the front of the id order).
    pub async fn stale_intents_after(
        &self,
        now_ms: i64,
        min_age_ms: i64,
        limit: i64,
        after_id: Option<i64>,
    ) -> Result<Vec<IntentRecord>> {
        let db = self.db.clone();
        let cutoff = now_ms.saturating_sub(min_age_ms);
        let limit = limit.max(1) as usize;
        run_blocking(move || {
            let intents = cf(&db, CF_INTENTS)?;
            let mut out = Vec::new();
            let start = after_id.map(|id| id_key(id.saturating_add(1)));
            let mode = match &start {
                Some(start) => IteratorMode::From(start, Direction::Forward),
                None => IteratorMode::Start,
            };
            for item in db.iterator_cf(&intents, mode) {
                let (key, value) = item?;
                let record = decode_intent(id_from_key(&key), &value)?;
                if record.created_at_ms <= cutoff {
                    out.push(record);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
            out.sort_by_key(|r| (r.created_at_ms, r.id));
            Ok(out)
        })
        .await
    }

    // ── rebuild ─────────────────────────────────────────────────────────────

    /// Bulk insert used by the rebuild pipeline. Every entry is a version
    /// found on disk. Per key: every real (`versioned`) version is kept; of
    /// the `null` versions (and `null` delete markers) only the newest
    /// survives — a key has at most one `null` state, so older ones are
    /// leftovers of a crash (a blob the row flip displaced but never
    /// retired) and are reported for trashing. The newest surviving version
    /// is the key's current state (the row, or the tombstone for a marker);
    /// the rest go to the `versions` family. Entries already current from an
    /// earlier batch take part in the same adjudication. Maintains the
    /// object counter as it goes.
    pub async fn insert_rebuild_batch(&self, entries: &[VersionRecord]) -> Result<RebuildBatchOutcome> {
        let db = self.db.clone();
        let durability = self.durability;
        let entries = entries.to_vec();
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let tombstones = cf(&db, CF_TOMBSTONES)?;
            let versions = cf(&db, CF_VERSIONS)?;
            let counters = cf(&db, CF_COUNTERS)?;
            let mut outcome = RebuildBatchOutcome::default();
            let mut by_key: std::collections::BTreeMap<&str, Vec<VersionRecord>> = std::collections::BTreeMap::new();
            for entry in &entries {
                by_key.entry(entry.object_key.as_str()).or_default().push(entry.clone());
            }
            let mut batch = WriteBatch::default();
            let mut delta: i64 = 0;
            for (key, mut candidates) in by_key {
                // The current state from an earlier batch joins the contest.
                let existing_row = db.get_cf(&objects, key.as_bytes())?.map(|v| decode_object(key.as_bytes(), &v)).transpose()?;
                let existing_tomb = db.get_cf(&tombstones, key.as_bytes())?.map(|v| decode_tombstone(key.as_bytes(), &v)).transpose()?;
                let mut had_live = false;
                if let Some(row) = &existing_row {
                    had_live = true;
                    if let Some(stamp) = &row.stamp {
                        candidates.push(VersionRecord::from_row(row, stamp.clone()));
                    }
                }
                // A carried tombstone without a marker cannot be a version; it
                // stays only while nothing newer arrives.
                let mut carried_tombstone = None;
                if let Some(t) = &existing_tomb {
                    match VersionRecord::from_tombstone(t) {
                        Some(v) => candidates.push(v),
                        None => carried_tombstone = Some(t.clone()),
                    }
                }
                // The noncurrent versions already kept join too (an older
                // `null` state arriving later must lose to the one kept), and
                // are rewritten with the survivors.
                let vprefix = versions_key(key, "");
                for item in db.iterator_cf(&versions, IteratorMode::From(&vprefix, Direction::Forward)) {
                    let (k, v) = item?;
                    if !k.starts_with(&vprefix) {
                        break;
                    }
                    candidates.push(decode_version(&k, &v)?);
                    batch.delete_cf(&versions, &k);
                }
                // Newest first; equal stamps tie-break on path for determinism.
                candidates.sort_by(|a, b| b.stamp.cmp(&a.stamp).then_with(|| b.blob_dir.cmp(&a.blob_dir)));
                candidates.dedup_by(|a, b| a.blob_dir == b.blob_dir);
                let mut survivors = Vec::new();
                let mut seen_null = false;
                for v in candidates {
                    if v.versioned {
                        survivors.push(v);
                    } else if seen_null {
                        outcome.loser_blob_dirs.push(v.blob_dir);
                    } else {
                        seen_null = true;
                        survivors.push(v);
                    }
                }
                let mut survivors = survivors.into_iter();
                let Some(newest) = survivors.next() else {
                    continue;
                };
                if let Some(t) = &carried_tombstone {
                    if t.rank() >= newest.rank() {
                        // The carried tombstone outranks everything on disk.
                        for v in std::iter::once(newest).chain(survivors) {
                            batch.put_cf(&versions, versions_key(key, &v.version_dir()), encode_version(&v));
                        }
                        if had_live {
                            batch.delete_cf(&objects, key.as_bytes());
                            delta -= 1;
                        }
                        continue;
                    }
                    batch.delete_cf(&tombstones, key.as_bytes());
                }
                // Clear whichever current state is being replaced; the
                // newest survivor becomes current.
                if newest.delete_marker {
                    if had_live {
                        batch.delete_cf(&objects, key.as_bytes());
                        delta -= 1;
                    }
                    batch.put_cf(&tombstones, key.as_bytes(), encode_tombstone(&newest.clone().into_tombstone(newest.stamp.ms)));
                } else {
                    batch.delete_cf(&tombstones, key.as_bytes());
                    if !had_live {
                        delta += 1;
                    }
                    batch.put_cf(&objects, key.as_bytes(), encode_object(&newest.clone().into_row()));
                }
                batch.delete_cf(&versions, versions_key(key, &newest.version_dir()));
                outcome.inserted += 1;
                for v in survivors {
                    batch.put_cf(&versions, versions_key(key, &v.version_dir()), encode_version(&v));
                    outcome.inserted += 1;
                }
            }
            if delta != 0 {
                batch.merge_cf(&counters, KEY_OBJECT_COUNT, delta.to_le_bytes());
            }
            db.write_opt(batch, &write_opts(durability))?;
            Ok(outcome)
        })
        .await
    }

    // ── changelog ─────────────────────────────────────────────────────────────

    /// This log's identity; see [`super::changelog`].
    pub fn changelog_id(&self) -> &str {
        &self.log_id
    }

    /// The commit-order watermark: the newest seq a reader may see.
    pub fn changelog_head(&self) -> u64 {
        self.changes.watermark()
    }

    pub async fn changelog_status(&self) -> Result<ChangelogStatus> {
        let db = self.db.clone();
        let head = self.changes.watermark();
        let log_id = self.log_id.to_string();
        run_blocking(move || {
            let log = cf(&db, CF_CHANGELOG)?;
            let mut it = db.raw_iterator_cf(&log);
            it.seek_to_first();
            it.status()?;
            let oldest_ts_ms = match (it.key(), it.value()) {
                (Some(k), Some(v)) => Some(changelog::decode_change(changelog::seq_from_key(k), v)?.ts_ms),
                _ => None,
            };
            Ok(ChangelogStatus {
                log_id,
                head,
                trimmed_through: trimmed_through(&db)?,
                oldest_ts_ms,
            })
        })
        .await
    }

    /// Reads entries with `seq > after`, never past the watermark, stopping
    /// after `limit` entries or once `max_bytes` of encoded entries were read
    /// (always at least one entry).
    pub async fn read_changelog(&self, after: u64, limit: usize, max_bytes: usize) -> Result<ChangelogPage> {
        let db = self.db.clone();
        let head = self.changes.watermark();
        let limit = limit.max(1);
        run_blocking(move || {
            let mut page = ChangelogPage {
                covered_through: after.max(0),
                ..ChangelogPage::default()
            };
            if after >= head {
                page.covered_through = after;
                return Ok(page);
            }
            let log = cf(&db, CF_CHANGELOG)?;
            let mut it = db.raw_iterator_cf(&log);
            it.seek(changelog::seq_key(after.saturating_add(1)));
            let mut bytes = 0usize;
            loop {
                let Some(key) = it.key() else {
                    it.status()?;
                    page.covered_through = head;
                    break;
                };
                let seq = changelog::seq_from_key(key);
                if seq > head {
                    page.covered_through = head;
                    break;
                }
                if page.events.len() >= limit || (bytes >= max_bytes && !page.events.is_empty()) {
                    page.more = true;
                    break;
                }
                let value = it.value().unwrap_or_default();
                bytes += value.len() + key.len();
                page.events.push(changelog::decode_change(seq, value)?);
                page.covered_through = seq;
                it.next();
            }
            Ok(page)
        })
        .await
    }

    /// The position to read from to get every change committed at or after
    /// `since_ms`: the seq just before the first such change, or the head
    /// when there is none. Commit times never decrease in seq order, so this
    /// is a binary search. `None` when changes from that time may already
    /// have been trimmed (`since_ms` is at or before the newest trimmed
    /// change's commit time).
    pub async fn position_at_time(&self, since_ms: i64) -> Result<Option<u64>> {
        let db = self.db.clone();
        let head = self.changes.watermark();
        run_blocking(move || {
            let log = cf(&db, CF_CHANGELOG)?;
            let trimmed = trimmed_through(&db)?;
            if trimmed > 0 && since_ms <= trimmed_ts(&db)? {
                // Changes from that time were (or may have been) trimmed.
                return Ok(None);
            }
            // First retained entry at or after `seq` (seqs have holes).
            let entry_from = |seq: u64| -> Result<Option<(u64, i64)>> {
                let mut it = db.raw_iterator_cf(&log);
                it.seek(changelog::seq_key(seq));
                match (it.key(), it.value()) {
                    (Some(k), Some(v)) => {
                        let s = changelog::seq_from_key(k);
                        if s > head {
                            return Ok(None);
                        }
                        Ok(Some((s, changelog::decode_change(s, v)?.ts_ms)))
                    }
                    _ => {
                        it.status()?;
                        Ok(None)
                    }
                }
            };
            let Some((first_seq, first_ts)) = entry_from(trimmed + 1)? else {
                return Ok(Some(head));
            };
            if first_ts >= since_ms {
                // Everything retained qualifies (and nothing trimmed did).
                return Ok(Some(first_seq - 1));
            }
            // Invariant: the entry at `lo` is before since_ms; find the first
            // entry at or after it in (lo, head].
            let (mut lo, mut hi) = (first_seq, head + 1);
            while hi - lo > 1 {
                let mid = lo + (hi - lo) / 2;
                match entry_from(mid)? {
                    Some((seq, ts)) if ts < since_ms => lo = seq,
                    Some(_) | None => hi = mid,
                }
            }
            // `lo` is the last entry before since_ms: read after it.
            Ok(Some(lo))
        })
        .await
    }

    /// Removes entries committed before `cutoff_ms`, at most `max_entries`
    /// per call, never past the watermark. Raises `trimmed_through` in the
    /// same batch. Returns how many entries were removed.
    pub async fn trim_changelog(&self, cutoff_ms: i64, max_entries: usize) -> Result<usize> {
        let db = self.db.clone();
        let durability = self.durability;
        let head = self.changes.watermark();
        run_blocking(move || {
            let log = cf(&db, CF_CHANGELOG)?;
            let meta = cf(&db, CF_META)?;
            let mut it = db.raw_iterator_cf(&log);
            it.seek_to_first();
            let mut first = None;
            let mut last = None;
            let mut last_ts = i64::MIN;
            let mut removed = 0usize;
            while removed < max_entries {
                let (Some(key), Some(value)) = (it.key(), it.value()) else {
                    it.status()?;
                    break;
                };
                let seq = changelog::seq_from_key(key);
                if seq > head {
                    break;
                }
                // Undecodable rows are trimmed by position, not age.
                let ts = changelog::decode_change(seq, value).map(|change| change.ts_ms);
                let expired = ts.as_ref().map(|ts| *ts < cutoff_ms).unwrap_or(true);
                if !expired {
                    break;
                }
                // Undecodable rows are trimmed by position; the cutoff bounds
                // their time.
                last_ts = last_ts.max(ts.unwrap_or(cutoff_ms - 1));
                first.get_or_insert(seq);
                last = Some(seq);
                removed += 1;
                it.next();
            }
            let (Some(first), Some(last)) = (first, last) else {
                return Ok(0);
            };
            let mut batch = WriteBatch::default();
            batch.delete_range_cf(&log, changelog::seq_key(first), changelog::seq_key(last + 1));
            let floor = trimmed_through(&db)?.max(last);
            batch.put_cf(&meta, KEY_TRIMMED_THROUGH, floor.to_le_bytes());
            let floor_ts = trimmed_ts(&db)?.max(last_ts);
            batch.put_cf(&meta, KEY_TRIMMED_TS, (floor_ts as u64).to_le_bytes());
            db.write_opt(batch, &write_opts(durability))?;
            Ok(removed)
        })
        .await
    }

    /// Copies this log (entries, id and trim floor) into `dst` — the rebuild
    /// pipeline carries the changelog across the index swap so consumers'
    /// offsets stay valid. Returns the number of entries copied.
    pub async fn copy_changelog_into(&self, dst: &ObjectIndex) -> Result<usize> {
        let src = self.db.clone();
        let dst_db = dst.db.clone();
        run_blocking(move || {
            let src_log = cf(&src, CF_CHANGELOG)?;
            let dst_log = cf(&dst_db, CF_CHANGELOG)?;
            let dst_meta = cf(&dst_db, CF_META)?;
            let src_meta = cf(&src, CF_META)?;
            let mut copied = 0usize;
            let mut batch = WriteBatch::default();
            for item in src.iterator_cf(&src_log, IteratorMode::Start) {
                let (key, value) = item?;
                batch.put_cf(&dst_log, key, value);
                copied += 1;
                if batch.len() >= 10_000 {
                    dst_db.write(std::mem::take(&mut batch))?;
                }
            }
            for key in [KEY_CHANGELOG_ID, KEY_TRIMMED_THROUGH, KEY_TRIMMED_TS] {
                if let Some(value) = src.get_cf(&src_meta, key)? {
                    batch.put_cf(&dst_meta, key, value);
                }
            }
            dst_db.write(batch)?;
            Ok(copied)
        })
        .await
    }

    /// Copies this index's tombstones (what keeps a delete from being
    /// undone; those with a marker on disk are re-found by the walk anyway)
    /// and replication checkpoints into `dst`.
    pub async fn copy_tombstones_into(&self, dst: &ObjectIndex) -> Result<usize> {
        let src = self.db.clone();
        let dst_db = dst.db.clone();
        run_blocking(move || {
            let dst_meta = cf(&dst_db, CF_META)?;
            let src_meta = cf(&src, CF_META)?;
            let src_tomb = cf(&src, CF_TOMBSTONES)?;
            let dst_tomb = cf(&dst_db, CF_TOMBSTONES)?;
            let mut copied = 0usize;
            let mut batch = WriteBatch::default();
            for item in src.iterator_cf(&src_tomb, IteratorMode::Start) {
                let (key, value) = item?;
                batch.put_cf(&dst_tomb, key, value);
                copied += 1;
                if batch.len() >= 10_000 {
                    dst_db.write(std::mem::take(&mut batch))?;
                }
            }
            for item in src.iterator_cf(
                &src_meta,
                IteratorMode::From(CHECKPOINT_PREFIX.as_bytes(), Direction::Forward),
            ) {
                let (key, value) = item?;
                if !key.starts_with(CHECKPOINT_PREFIX.as_bytes()) {
                    break;
                }
                batch.put_cf(&dst_meta, key, value);
            }
            dst_db.write(batch)?;
            Ok(copied)
        })
        .await
    }

    // ── listing ───────────────────────────────────────────────────────────────

    /// Returns up to `limit` entries in ascending key order, starting after
    /// `after`.
    pub async fn all_entries_after(&self, after: Option<&str>, limit: i64) -> Result<Vec<ObjectRecord>> {
        let db = self.db.clone();
        let after = after.map(str::to_string);
        let limit = limit.max(0) as usize;
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let start = after.clone().unwrap_or_default();
            let mut out = Vec::new();
            let iter = db.iterator_cf(
                &objects,
                IteratorMode::From(start.as_bytes(), Direction::Forward),
            );
            for item in iter {
                if out.len() >= limit {
                    break;
                }
                let (key, value) = item?;
                if after.as_deref().map(str::as_bytes) == Some(&key) {
                    continue; // `after` is exclusive
                }
                // Skip (don't fail the whole scan on) a single undecodable row.
                match decode_object(&key, &value) {
                    Ok(entry) => out.push(entry),
                    Err(err) => log::warn!(
                        "skipping undecodable object row during scan key={} error={err}",
                        String::from_utf8_lossy(&key)
                    ),
                }
            }
            Ok(out)
        })
        .await
    }

    pub async fn list(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max_keys: usize,
    ) -> Result<ListPage> {
        let db = self.db.clone();
        let prefix = prefix.to_string();
        let delimiter = delimiter.filter(|v| !v.is_empty()).map(str::to_string);
        let after = after.map(str::to_string);
        run_blocking(move || list_blocking(&db, &prefix, delimiter.as_deref(), after.as_deref(), max_keys))
            .await
    }
}

fn list_blocking(
    db: &Db,
    prefix: &str,
    delimiter: Option<&str>,
    after: Option<&str>,
    max_keys: usize,
) -> Result<ListPage> {
    let objects = cf(db, CF_OBJECTS)?;

    // The scan lower bound is the larger of the prefix and the (exclusive)
    // `after` cursor; from there we skip an exact `after` match.
    let after_str = after.unwrap_or("");
    let start: &str = if after_str > prefix { after_str } else { prefix };

    if max_keys == 0 {
        // Report only whether a matching key exists beyond the cursor.
        let mut next_key: Option<String> = None;
        let iter = db.iterator_cf(&objects, IteratorMode::From(start.as_bytes(), Direction::Forward));
        for item in iter {
            let (key, _) = item?;
            if after.is_some() && key.as_ref() == after_str.as_bytes() {
                continue;
            }
            next_key = Some(String::from_utf8_lossy(&key).into_owned());
            break;
        }
        return Ok(ListPage {
            entries: Vec::new(),
            common_prefixes: Vec::new(),
            is_truncated: next_key.is_some_and(|key| key.starts_with(prefix)),
            next_after: after.map(str::to_string),
        });
    }

    let mut entries = Vec::new();
    let mut common_prefixes: Vec<String> = Vec::new();
    let mut next_after = None;
    // A cursor that falls inside a rolled-up group (S3 hands back the common
    // prefix itself as NextMarker / NextContinuationToken; older pages handed
    // back the group's last key) resumes past the whole group.
    let mut seek_to = start.as_bytes().to_vec();
    if let (Some(delimiter), Some(after)) = (delimiter, after) {
        if let Some(rest) = after.strip_prefix(prefix) {
            if let Some(idx) = rest.find(delimiter) {
                let group = format!("{}{}", prefix, &rest[..idx + delimiter.len()]);
                if let Some(past) = prefix_successor(group.as_bytes()) {
                    if past > seek_to {
                        seek_to = past;
                    }
                }
            }
        }
    }
    let mut more_matching = false;
    let mut it = db.raw_iterator_cf(&objects);
    it.seek(&seek_to);
    loop {
        let Some(key) = it.key() else {
            it.status()?;
            break;
        };
        if after.is_some() && key == after_str.as_bytes() {
            it.next();
            continue; // `after` is exclusive
        }
        if !key.starts_with(prefix.as_bytes()) {
            break;
        }
        if let Some(delimiter) = delimiter {
            let object_key = String::from_utf8_lossy(key);
            let rest = &object_key[prefix.len()..];
            if let Some(idx) = rest.find(delimiter) {
                // Roll the whole group up into one common prefix: the limit is
                // checked only for a group not yet emitted, and the scan then
                // seeks straight past the group instead of walking its keys.
                let common = format!("{}{}", prefix, &rest[..idx + delimiter.len()]);
                if entries.len() + common_prefixes.len() >= max_keys {
                    more_matching = true;
                    break;
                }
                let past = prefix_successor(common.as_bytes());
                next_after = Some(common.clone());
                common_prefixes.push(common);
                match past {
                    Some(past) => it.seek(&past),
                    None => break, // nothing sorts after the group
                }
                continue;
            }
        }
        // One undecodable row (corrupt JSON, or a value stamped with a newer
        // entity version) must not blind the whole bucket's listing — skip and
        // log it rather than failing the entire request.
        let entry = match decode_object(key, it.value().unwrap_or_default()) {
            Ok(entry) => entry,
            Err(err) => {
                log::warn!(
                    "skipping undecodable object row during list key={} error={err}",
                    String::from_utf8_lossy(key)
                );
                it.next();
                continue;
            }
        };
        if entries.len() + common_prefixes.len() >= max_keys {
            more_matching = true;
            break;
        }
        next_after = Some(entry.object_key.clone());
        entries.push(entry);
        it.next();
    }

    Ok(ListPage {
        entries,
        common_prefixes,
        is_truncated: more_matching,
        next_after,
    })
}

/// The smallest byte string greater than every string starting with `prefix`
/// (strip trailing `0xFF`s, then increment the last byte). `None` if no such
/// bound exists (empty or all-`0xFF` prefix).
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    while let Some(&last) = bound.last() {
        if last == 0xFF {
            bound.pop();
        } else {
            *bound.last_mut().unwrap() += 1;
            return Some(bound);
        }
    }
    None
}

/// Whether two relative blob dirs name the same directory, ignoring the
/// separator style: an index written on Windows may hold `objects\ab\…`
/// where the scanner derives `objects/ab/…`. A plain string compare would call
/// a live object's dir "not referenced" and let it be trashed.
pub(crate) fn same_blob_dir(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .all(|(x, y)| x == y || (matches!(x, b'/' | b'\\') && matches!(y, b'/' | b'\\')))
}

// ── changelog entries: the side effect of every commit ──────────────────
//
// Every committed change is described by exactly one of these, built from
// the committed row (never from request data), so the log can never disagree
// with the index.

/// The entry for a published state (`put`): the row's attributes and stamp.
fn change_for(guard: &changelog::SeqGuard<'_>, op: ChangeOp, record: &ObjectRecord, origin: ChangeOrigin) -> ChangeEvent {
    ChangeEvent {
        seq: guard.seq,
        op,
        key: record.object_key.clone(),
        etag: record.etag.clone(),
        size: record.size,
        last_modified_ms: record.last_modified_ms,
        version: changelog::version_of(&record.blob_dir),
        origin,
        ts_ms: guard.ts,
        parts: record.parts.clone(),
        stamp: record.stamp.clone(),
        removed_stamp: None,
    }
}

/// The entry for a removed row (`delete`): names the removed version (its
/// ETag, size, layout) and carries the delete's own `stamp` and the
/// `removed` stamp it ranks by.
fn change_for_delete(
    guard: &changelog::SeqGuard<'_>,
    removed_row: &ObjectRecord,
    origin: ChangeOrigin,
    stamp: Option<Stamp>,
    removed: Option<Stamp>,
) -> ChangeEvent {
    ChangeEvent {
        stamp,
        removed_stamp: removed,
        ..change_for(guard, ChangeOp::Delete, removed_row, origin)
    }
}

/// The entry for a delete of a key with no row here (a replicated delete
/// arriving before, or instead of, the version it removed): a `delete` with
/// no ETag, size 0 and no version — nothing visible changed here, but the
/// delete must still reach every other site and reader.
fn change_for_tombstone(
    guard: &changelog::SeqGuard<'_>,
    key: &str,
    origin: ChangeOrigin,
    stamp: Stamp,
    removed: Stamp,
) -> ChangeEvent {
    ChangeEvent {
        seq: guard.seq,
        op: ChangeOp::Delete,
        key: key.to_string(),
        etag: String::new(),
        size: 0,
        last_modified_ms: stamp.ms,
        version: String::new(),
        origin,
        ts_ms: guard.ts,
        parts: None,
        stamp: Some(stamp),
        removed_stamp: Some(removed),
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: &str, blob_dir: &str, lm: i64) -> ObjectRecord {
        ObjectRecord {
            stamp: None,
            versioned: false,
            object_key: key.to_string(),
            blob_dir: blob_dir.to_string(),
            size: 1,
            etag: "e".to_string(),
            last_modified_ms: lm,
            parts: None,
        }
    }

    async fn open_tmp(tmp: &tempfile::TempDir) -> ObjectIndex {
        ObjectIndex::open(tmp.path(), Durability::Relaxed).await.unwrap()
    }

    /// Commits an object through the normal publish path.
    async fn put(index: &ObjectIndex, key: &str, blob_dir: &str, lm: i64) {
        let intent = index.insert_publish_intent(key, blob_dir, lm).await.unwrap();
        index
            .commit_publish(&record(key, blob_dir, lm), intent, None, lm)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn every_commit_appends_one_changelog_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        put(&index, "a", "objects/aa/V1A_1", 1).await;
        put(&index, "b", "objects/bb/V1B_1", 2).await;
        index.commit_delete("a", "objects/aa/V1A_1", 3).await.unwrap();
        // Deleting an absent key logs nothing.
        index.commit_delete("zz", "objects/zz/V1Z_1", 4).await.unwrap();
        assert_eq!(index.changelog_head(), 3);
        let page = index.read_changelog(0, 100, usize::MAX).await.unwrap();
        let ops: Vec<_> = page.events.iter().map(|e| (e.seq, e.op, e.key.as_str(), e.version.as_str())).collect();
        assert_eq!(
            ops,
            [
                (1, ChangeOp::Put, "a", "V1A_1"),
                (2, ChangeOp::Put, "b", "V1B_1"),
                (3, ChangeOp::Delete, "a", "V1A_1"),
            ]
        );
        assert_eq!(page.covered_through, 3);
        assert!(!page.more);
        // Paging.
        let first = index.read_changelog(0, 2, usize::MAX).await.unwrap();
        assert_eq!((first.events.len(), first.covered_through, first.more), (2, 2, true));
        let rest = index.read_changelog(2, 2, usize::MAX).await.unwrap();
        assert_eq!((rest.events.len(), rest.covered_through, rest.more), (1, 3, false));
    }

    #[tokio::test]
    async fn trimming_keeps_the_sequence_and_log_id_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        let log_id = index.changelog_id().to_string();
        for (i, key) in ["a", "b", "c"].iter().enumerate() {
            put(&index, key, &format!("objects/x/V1{key}_1"), i as i64).await;
        }
        // Trim everything: the floor remembers where the log was.
        assert_eq!(index.trim_changelog(i64::MAX, 100).await.unwrap(), 3);
        let status = index.changelog_status().await.unwrap();
        assert_eq!((status.head, status.trimmed_through, status.oldest_ts_ms), (3, 3, None));
        index.close().await;
        drop(index);

        let index = open_tmp(&tmp).await;
        assert_eq!(index.changelog_id(), log_id, "log id is stable");
        put(&index, "d", "objects/x/V1d_1", 9).await;
        let page = index.read_changelog(3, 10, usize::MAX).await.unwrap();
        assert_eq!(page.events[0].seq, 4, "sequence continues after a full trim + reopen");
    }

    #[tokio::test]
    async fn trimming_is_by_age_and_bounded() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        for (i, key) in ["a", "b", "c", "d"].iter().enumerate() {
            put(&index, key, &format!("objects/x/V1{key}_1"), i as i64 * 1000).await;
        }
        // Entries carry their commit time (`now_ms` passed to the commit).
        assert_eq!(index.trim_changelog(1500, 100).await.unwrap(), 2);
        assert_eq!(index.changelog_status().await.unwrap().trimmed_through, 2);
        assert_eq!(index.trim_changelog(i64::MAX, 1).await.unwrap(), 1, "bounded per call");
        let page = index.read_changelog(0, 10, usize::MAX).await.unwrap();
        assert_eq!(page.events.iter().map(|e| e.seq).collect::<Vec<_>>(), [4]);
    }

    #[tokio::test]
    async fn changelog_copies_into_a_rebuilt_index() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        put(&index, "a", "objects/x/V1a_1", 1).await;
        put(&index, "b", "objects/x/V1b_1", 2).await;
        index.trim_changelog(2, 10).await.unwrap(); // trims seq 1
        let dst = ObjectIndex::open_at(&tmp.path().join("other.rocksdb"), Durability::Relaxed)
            .await
            .unwrap();
        assert_ne!(dst.changelog_id(), index.changelog_id());
        assert_eq!(index.copy_changelog_into(&dst).await.unwrap(), 1);
        dst.close().await;
        let path = tmp.path().join("other.rocksdb");
        drop(dst);
        let reopened = ObjectIndex::open_at(&path, Durability::Relaxed).await.unwrap();
        assert_eq!(reopened.changelog_id(), index.changelog_id());
        let status = reopened.changelog_status().await.unwrap();
        assert_eq!((status.head, status.trimmed_through), (2, 1));
    }

    /// A version found on disk, as the rebuild hands it to the batch insert.
    fn version(key: &str, blob_dir: &str, ms: i64, site: &str, versioned: bool, delete_marker: bool) -> VersionRecord {
        VersionRecord {
            object_key: key.to_string(),
            stamp: Stamp { ms, c: 0, site: site.into() },
            blob_dir: blob_dir.to_string(),
            size: if delete_marker { 0 } else { 1 },
            etag: if delete_marker { String::new() } else { "e".to_string() },
            last_modified_ms: ms,
            parts: None,
            versioned,
            delete_marker,
            removed: delete_marker.then(|| Stamp { ms: ms - 1, c: 0, site: site.into() }),
        }
    }

    #[tokio::test]
    async fn rebuild_batch_keeps_real_versions_and_only_the_newest_null_state() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        // Two null states (a crash leftover and the live one), two real
        // versions, and a real delete marker newest of all.
        let outcome = index
            .insert_rebuild_batch(&[
                version("k", "objects/null-old", 100, "a", false, false),
                version("k", "objects/null-new", 200, "a", false, false),
                version("k", "objects/v1", 300, "a", true, false),
                version("k", "objects/v2", 400, "b", true, false),
                version("k", "objects/marker", 500, "a", true, true),
            ])
            .await
            .unwrap();
        assert_eq!(outcome.loser_blob_dirs, vec!["objects/null-old".to_string()]);
        assert_eq!(outcome.inserted, 4);
        assert!(index.get("k").await.unwrap().is_none(), "the marker is current: no row");
        let t = index.get_tombstone("k").await.unwrap().unwrap();
        assert_eq!((t.blob_dir.as_deref(), t.versioned), (Some("objects/marker"), true));
        assert_eq!(index.object_count().await.unwrap(), 0);
        let versions: Vec<_> = index.versions_of("k").await.unwrap().into_iter().map(|v| v.blob_dir).collect();
        assert_eq!(versions, ["objects/v2", "objects/v1", "objects/null-new"], "newest first");

        // A later batch brings a newer live version: it becomes current and
        // the marker joins the noncurrent versions.
        let outcome = index
            .insert_rebuild_batch(&[version("k", "objects/v3", 600, "a", true, false)])
            .await
            .unwrap();
        assert!(outcome.loser_blob_dirs.is_empty());
        assert_eq!(index.get("k").await.unwrap().unwrap().blob_dir, "objects/v3");
        assert!(index.get_tombstone("k").await.unwrap().is_none());
        assert_eq!(index.object_count().await.unwrap(), 1);
        assert_eq!(index.versions_of("k").await.unwrap().len(), 4);
        // An older null state arriving later loses to the one already kept.
        let outcome = index
            .insert_rebuild_batch(&[version("k", "objects/null-older", 50, "a", false, false)])
            .await
            .unwrap();
        assert_eq!(outcome.loser_blob_dirs, vec!["objects/null-older".to_string()]);
    }

    #[tokio::test]
    async fn publish_commit_removes_intent_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        let intent = index.insert_publish_intent("k", "objects/x", 1).await.unwrap();
        assert_eq!(index.stale_intents(10, 0, 10).await.unwrap().len(), 1);
        let retire = index
            .commit_publish(&record("k", "objects/x", 5), intent, None, 5)
            .await
            .unwrap();
        assert!(retire.is_none());
        assert!(index.stale_intents(10, 0, 10).await.unwrap().is_empty());
        assert_eq!(index.get("k").await.unwrap().unwrap().blob_dir, "objects/x");
    }

    #[tokio::test]
    async fn object_count_tracks_committed_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        assert_eq!(index.object_count().await.unwrap(), 0);
        assert!(index.is_empty().await.unwrap());
        for key in ["a", "b", "c"] {
            put(&index, key, "objects/x", 1).await;
        }
        assert_eq!(index.object_count().await.unwrap(), 3);
        assert!(!index.is_empty().await.unwrap());
        // Overwrites do not change the count; deletes decrement it.
        put(&index, "a", "objects/y", 2).await;
        assert_eq!(index.object_count().await.unwrap(), 3);
        index.commit_delete("a", "objects/y", 3).await.unwrap();
        assert_eq!(index.object_count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn overwrite_commit_records_retire_intent() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        put(&index, "k", "objects/a", 5).await;
        let i2 = index.insert_publish_intent("k", "objects/b", 6).await.unwrap();
        let retire = index
            .commit_publish(&record("k", "objects/b", 7), i2, Some("objects/a"), 7)
            .await
            .unwrap()
            .unwrap();
        let intents = index.stale_intents(100, 0, 10).await.unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].op, INTENT_RETIRE);
        assert_eq!(intents[0].blob_dir, "objects/a");
        index.delete_intent(retire).await.unwrap();
        assert!(index.stale_intents(100, 0, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_commit_removes_row_and_records_retire() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        put(&index, "k", "objects/a", 5).await;
        let retire = index.commit_delete("k", "objects/a", 6).await.unwrap();
        assert!(index.get("k").await.unwrap().is_none());
        let intents = index.stale_intents(100, 0, 10).await.unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].id, retire);
    }

    #[tokio::test]
    async fn intent_attempts_and_blob_dir_updates_persist() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        let id = index.insert_publish_intent("k", "objects/a", 1).await.unwrap();
        index.bump_intent_attempts(id).await.unwrap();
        index.bump_intent_attempts(id).await.unwrap();
        index.update_intent_blob_dir(id, "objects/b").await.unwrap();
        let got = index.get_intent(id).await.unwrap().unwrap();
        assert_eq!(got.attempts, 2);
        assert_eq!(got.blob_dir, "objects/b");
        assert_eq!(got.op, INTENT_PUBLISH);
    }

    #[tokio::test]
    async fn update_blob_dir_guards_on_expected() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        put(&index, "k", "objects/a", 1).await;
        assert!(!index.update_blob_dir("k", "objects/WRONG", "objects/b").await.unwrap());
        assert_eq!(index.get("k").await.unwrap().unwrap().blob_dir, "objects/a");
        assert!(index.update_blob_dir("k", "objects/a", "objects/b").await.unwrap());
        assert_eq!(index.get("k").await.unwrap().unwrap().blob_dir, "objects/b");
    }

    #[tokio::test]
    async fn rows_off_layout_lists_old_layouts_and_unstamped_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        put(&index, "legacy", "objects/aa/bb/cc/dd/V1ABCDEF_9F00", 1).await;
        put(&index, "modern", "objects/aabb/V1ABCDEF_9F00", 1).await;
        let stamp = Stamp { ms: 5, c: 0, site: "0123456789abcdef".into() };
        let current = crate::storage::layouts::v3::blob_rel("bucket", "current", &stamp);
        put(&index, "current-unstamped", &current, 1).await;
        let intent = index.insert_publish_intent("current", &current, 1).await.unwrap();
        index
            .commit_publish(&ObjectRecord { stamp: Some(stamp), ..record("current", &current, 1) }, intent, None, 1)
            .await
            .unwrap();
        let keys = index.rows_off_layout(None, 100).await.unwrap();
        assert_eq!(keys, vec!["current-unstamped".to_string(), "legacy".to_string(), "modern".to_string()]);
    }


    #[tokio::test]
    async fn outdated_schema_is_rejected_and_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        // Stamp an outdated schema version directly, then close.
        {
            let db_path = index_db_path(tmp.path());
            let index = ObjectIndex::open_at(&db_path, Durability::Relaxed).await.unwrap();
            let meta = cf(&index.db, CF_META).unwrap();
            index
                .db
                .put_cf(&meta, KEY_SCHEMA_VERSION, (SCHEMA_VERSION - 1).to_le_bytes())
                .unwrap();
            index.db.flush().unwrap();
        }
        let err = ObjectIndex::open(tmp.path(), Durability::Relaxed).await;
        assert!(matches!(err, Err(StorageError::IndexOutdated(_))));
    }

    #[tokio::test]
    async fn state_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let index = open_tmp(&tmp).await;
            put(&index, "k", "objects/a", 5).await;
            index.insert_publish_intent("p", "objects/z", 9).await.unwrap();
            index.close().await;
        }
        let index = open_tmp(&tmp).await;
        assert_eq!(index.get("k").await.unwrap().unwrap().blob_dir, "objects/a");
        assert_eq!(index.object_count().await.unwrap(), 1);
        // The intent allocator resumes past the highest surviving id.
        let intents = index.stale_intents(100, 0, 10).await.unwrap();
        assert_eq!(intents.len(), 1);
        let fresh = index.insert_publish_intent("q", "objects/y", 10).await.unwrap();
        assert!(fresh > intents[0].id);
    }

    #[tokio::test]
    async fn newer_value_version_is_rejected() {
        // A value tagged v2 must fail closed, not be misread as v1.
        let json = br#"{"v":2,"blob_dir":"objects/x","size":1,"etag":"e","last_modified_ms":1}"#;
        let err = decode_object(b"k", json);
        assert!(matches!(err, Err(StorageError::Db(_))));
        // A value missing "v" is treated leniently as v1 with defaults.
        let ok = decode_object(b"k", br#"{"blob_dir":"objects/x"}"#).unwrap();
        assert_eq!(ok.blob_dir, "objects/x");
        assert_eq!(ok.size, 0);
    }

    #[tokio::test]
    async fn list_dedupes_delimiter_common_prefixes() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        for key in ["a/1", "a/2", "a/b/1", "a/b/2", "a/c/1"] {
            put(&index, key, "d", 1).await;
        }
        let page = index.list("a/", Some("/"), None, 10).await.unwrap();
        assert_eq!(
            page.entries.iter().map(|v| v.object_key.as_str()).collect::<Vec<_>>(),
            vec!["a/1", "a/2"]
        );
        assert_eq!(page.common_prefixes, vec!["a/b/", "a/c/"]);
    }

    #[tokio::test]
    async fn exact_size_page_is_not_truncated() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        for key in ["a", "b"] {
            put(&index, key, "d", 1).await;
        }
        let page = index.list("", None, None, 2).await.unwrap();
        assert_eq!(page.entries.len(), 2);
        assert!(!page.is_truncated);
    }

    #[tokio::test]
    async fn zero_max_keys_returns_no_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        put(&index, "a", "d", 1).await;
        let page = index.list("", None, None, 0).await.unwrap();
        assert!(page.entries.is_empty());
        assert!(page.common_prefixes.is_empty());
        assert!(page.is_truncated);
    }

    #[tokio::test]
    async fn all_entries_after_paginates() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        for key in ["a", "b", "c", "d"] {
            put(&index, key, "d", 1).await;
        }
        let first = index.all_entries_after(None, 2).await.unwrap();
        assert_eq!(first.iter().map(|r| r.object_key.as_str()).collect::<Vec<_>>(), vec!["a", "b"]);
        let second = index.all_entries_after(Some("b"), 2).await.unwrap();
        assert_eq!(second.iter().map(|r| r.object_key.as_str()).collect::<Vec<_>>(), vec!["c", "d"]);
    }

    #[tokio::test]
    async fn delimiter_pagination_does_not_repeat_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        for key in ["a/b/1", "a/b/2", "a/c/1"] {
            put(&index, key, "d", 1).await;
        }
        let first = index.list("a/", Some("/"), None, 1).await.unwrap();
        assert_eq!(first.common_prefixes, vec!["a/b/"]);
        assert!(first.is_truncated);

        let second = index
            .list("a/", Some("/"), first.next_after.as_deref(), 1)
            .await
            .unwrap();
        assert_eq!(second.common_prefixes, vec!["a/c/"]);
    }

    #[tokio::test]
    async fn delimiter_listing_reads_past_large_common_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        for n in 0..300 {
            put(&index, &format!("a/large/{n:04}"), "d", 1).await;
        }
        put(&index, "a/next/1", "d", 1).await;

        let page = index.list("a/", Some("/"), None, 2).await.unwrap();
        assert_eq!(page.common_prefixes, vec!["a/large/", "a/next/"]);
        assert!(!page.is_truncated);
    }

    #[tokio::test]
    async fn delimiter_listing_single_group_is_not_truncated() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        for key in ["a/b/1", "a/b/2"] {
            put(&index, key, "d", 1).await;
        }
        let page = index.list("a/", Some("/"), None, 1).await.unwrap();
        assert_eq!(page.common_prefixes, vec!["a/b/"]);
        assert!(page.entries.is_empty());
        assert!(!page.is_truncated, "the rest of the group is not a further page");
        // S3 hands the common prefix itself back as NextMarker / token.
        assert_eq!(page.next_after.as_deref(), Some("a/b/"));
    }

    /// S3's listing semantics, computed naively: every key under `prefix`,
    /// rolled up at the first `delimiter` after it. `Ok` = key, `Err` = prefix.
    fn reference_listing(keys: &[&str], prefix: &str, delimiter: Option<&str>) -> Vec<std::result::Result<String, String>> {
        let mut sorted: Vec<&str> = keys.to_vec();
        sorted.sort();
        let mut out: Vec<std::result::Result<String, String>> = Vec::new();
        for key in sorted.into_iter().filter(|k| k.starts_with(prefix)) {
            let rest = &key[prefix.len()..];
            match delimiter.and_then(|d| rest.find(d).map(|i| (d, i))) {
                Some((d, i)) => {
                    let common = format!("{prefix}{}", &rest[..i + d.len()]);
                    if out.last() != Some(&Err(common.clone())) {
                        out.push(Err(common));
                    }
                }
                None => out.push(Ok(key.to_string())),
            }
        }
        out
    }

    #[tokio::test]
    async fn delimiter_pagination_matches_s3_for_every_page_size() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        let keys = [
            "a", "a/", "a/0", "a/b/1", "a/b/2", "a/b/c/3", "a/b0", "a/c", "a/d/1", "a/d/2", "a/d/3", "a/e",
            "a/f/", "a/f/x", "b/1", "b/2", "c",
        ];
        for key in keys {
            put(&index, key, "d", 1).await;
        }
        for (prefix, delimiter) in [("", Some("/")), ("a/", Some("/")), ("a/b", Some("/")), ("a/", Some("b/")), ("a/", None)] {
            let expected = reference_listing(&keys, prefix, delimiter);
            for max_keys in 1..=expected.len() + 1 {
                let mut got: Vec<std::result::Result<String, String>> = Vec::new();
                let mut after: Option<String> = None;
                let mut pages = 0;
                loop {
                    let page = index.list(prefix, delimiter, after.as_deref(), max_keys).await.unwrap();
                    pages += 1;
                    let count = page.entries.len() + page.common_prefixes.len();
                    assert!(count <= max_keys);
                    // Merge the two lists back into key order.
                    let mut items: Vec<std::result::Result<String, String>> = page
                        .entries
                        .iter()
                        .map(|e| Ok(e.object_key.clone()))
                        .chain(page.common_prefixes.iter().cloned().map(Err))
                        .collect();
                    items.sort_by(|x, y| {
                        let k = |v: &std::result::Result<String, String>| match v {
                            Ok(s) | Err(s) => s.clone(),
                        };
                        k(x).cmp(&k(y))
                    });
                    let last = items.last().map(|v| match v {
                        Ok(s) | Err(s) => s.clone(),
                    });
                    got.extend(items);
                    if !page.is_truncated {
                        break;
                    }
                    assert_eq!(count, max_keys, "a truncated page is full");
                    assert_eq!(page.next_after, last, "cursor is the last item (key or prefix)");
                    after = page.next_after;
                    assert!(pages <= expected.len() + 1, "pagination did not terminate");
                }
                assert_eq!(got, expected, "prefix={prefix:?} delimiter={delimiter:?} max_keys={max_keys}");
                assert_eq!(pages, expected.len().div_ceil(max_keys).max(1), "no empty trailing page");
            }
        }
    }

    #[tokio::test]
    async fn delimiter_cursor_inside_a_group_resumes_past_it() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        for key in ["a/b/1", "a/b/2", "a/b/3", "a/c"] {
            put(&index, key, "d", 1).await;
        }
        // A start-after / legacy token naming a key inside a rolled-up group
        // (and the group itself, as S3's NextMarker does) skips the group.
        for after in ["a/b/1", "a/b/3", "a/b/"] {
            let page = index.list("a/", Some("/"), Some(after), 10).await.unwrap();
            assert!(page.common_prefixes.is_empty(), "after={after}");
            assert_eq!(page.entries.iter().map(|e| e.object_key.as_str()).collect::<Vec<_>>(), vec!["a/c"]);
        }
        // A cursor sorting just before the group still emits it.
        let page = index.list("a/", Some("/"), Some("a/b"), 10).await.unwrap();
        assert_eq!(page.common_prefixes, vec!["a/b/"]);
    }

    #[test]
    fn prefix_successor_bounds_the_group() {
        assert_eq!(prefix_successor(b"a/b/"), Some(b"a/b0".to_vec()));
        assert_eq!(prefix_successor(&[b'a', 0xFF]), Some(b"b".to_vec()));
        assert_eq!(prefix_successor(&[0xFF]), None);
    }

    #[test]
    fn same_blob_dir_ignores_separator_style() {
        assert!(same_blob_dir("objects/AB12/leaf", "objects/AB12/leaf"));
        assert!(same_blob_dir("objects\\AB12\\leaf", "objects/AB12/leaf"));
        assert!(!same_blob_dir("objects/AB12/leaf", "objects/AB12/leaf2"));
        assert!(!same_blob_dir("objects/AB12/leaf", "objects/AB13/leaf"));
    }

    #[tokio::test]
    async fn stale_intents_after_resumes_past_the_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        let mut ids = Vec::new();
        for n in 0..5 {
            ids.push(index.insert_publish_intent(&format!("k{n}"), "d", 10).await.unwrap());
        }
        let first = index.stale_intents_after(100, 0, 2, None).await.unwrap();
        assert_eq!(first.iter().map(|r| r.id).collect::<Vec<_>>(), ids[..2]);
        let next = index.stale_intents_after(100, 0, 2, Some(ids[1])).await.unwrap();
        assert_eq!(next.iter().map(|r| r.id).collect::<Vec<_>>(), ids[2..4]);
        let last = index.stale_intents_after(100, 0, 2, Some(ids[4])).await.unwrap();
        assert!(last.is_empty());
    }

    #[tokio::test]
    async fn rows_off_layout_resumes_after_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        for key in ["a", "b", "c"] {
            put(&index, key, &format!("objects/aa/bb/cc/dd/{key}"), 1).await;
        }
        assert_eq!(index.rows_off_layout(Some("a"), 10).await.unwrap(), vec!["b", "c"]);
        assert_eq!(index.rows_off_layout(Some("b"), 1).await.unwrap(), vec!["c"]);
        assert!(index.rows_off_layout(Some("c"), 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn zero_max_keys_ignores_lexically_later_nonmatching_key() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        put(&index, "z", "d", 1).await;
        let page = index.list("a/", None, None, 0).await.unwrap();
        assert!(!page.is_truncated);
    }
}
