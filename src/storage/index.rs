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

use super::errors::{Result, StorageError};

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

const KEY_OBJECT_COUNT: &[u8] = b"objects";
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
}

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
    /// `_reader_connections` is accepted for source compatibility with the old
    /// pooled backend and ignored: RocksDB manages its own read concurrency.
    pub async fn open(bucket_dir: &Path, durability: Durability, _reader_connections: u32) -> Result<Self> {
        tokio::fs::create_dir_all(bucket_dir).await?;
        let db_path = index_db_path(bucket_dir);
        let existed = db_path.exists();
        let index = Self::open_at(&db_path, durability, _reader_connections).await?;
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
    pub async fn open_at(db_path: &Path, durability: Durability, _reader_connections: u32) -> Result<Self> {
        let path = db_path.to_path_buf();
        let (db, seed) = run_blocking(move || {
            let db = open_db(&path)?;
            let seed = max_intent_id(&db)?;
            Ok((db, seed))
        })
        .await?;
        Ok(Self {
            db: Arc::new(db),
            durability,
            intent_seq: Arc::new(AtomicI64::new(seed)),
        })
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
            for name in [CF_OBJECTS, CF_INTENTS, CF_COUNTERS, CF_META] {
                if let Ok(handle) = cf(&db, name) {
                    let _ = db.flush_cf(&handle);
                }
            }
            let _ = db.flush_wal(true);
            Ok(())
        })
        .await;
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

    /// Up to `limit` keys still on the legacy 4-level fanout layout
    /// (`objects/xx/xx/xx/xx/…`). Empty once migration is complete. Only
    /// consulted after the cheap root-dir check finds legacy structure, so it
    /// never scans a clean bucket.
    pub async fn legacy_layout_keys(&self, limit: usize) -> Result<Vec<String>> {
        let db = self.db.clone();
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let mut keys = Vec::new();
            for item in db.iterator_cf(&objects, IteratorMode::Start) {
                let (key, value) = item?;
                let record = decode_object(&key, &value)?;
                if is_legacy_layout(&record.blob_dir) {
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
        let db = self.db.clone();
        let durability = self.durability;
        let record = record.clone();
        let displaced = displaced_blob_dir.map(str::to_string);
        let retire_id = displaced.as_ref().map(|_| self.next_intent_id());
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let intents = cf(&db, CF_INTENTS)?;
            let counters = cf(&db, CF_COUNTERS)?;
            let existed = db.get_cf(&objects, record.object_key.as_bytes())?.is_some();

            let mut batch = WriteBatch::default();
            batch.put_cf(&objects, record.object_key.as_bytes(), encode_object(&record));
            batch.delete_cf(&intents, id_key(publish_intent_id));
            if let (Some(old_dir), Some(id)) = (&displaced, retire_id) {
                let value = encode_intent(INTENT_RETIRE, &record.object_key, old_dir, now_ms, 0);
                batch.put_cf(&intents, id_key(id), value);
            }
            if !existed {
                batch.merge_cf(&counters, KEY_OBJECT_COUNT, 1i64.to_le_bytes());
            }
            db.write_opt(batch, &write_opts(durability))?;
            Ok(retire_id)
        })
        .await
    }

    /// The atomic commit point of a DELETE: removes the object and records a
    /// `retire` intent for its blob dir in one write batch.
    pub async fn commit_delete(&self, key: &str, blob_dir: &str, now_ms: i64) -> Result<i64> {
        let db = self.db.clone();
        let durability = self.durability;
        let id = self.next_intent_id();
        let key = key.to_string();
        let blob_dir = blob_dir.to_string();
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let intents = cf(&db, CF_INTENTS)?;
            let counters = cf(&db, CF_COUNTERS)?;
            let existed = db.get_cf(&objects, key.as_bytes())?.is_some();

            let mut batch = WriteBatch::default();
            batch.delete_cf(&objects, key.as_bytes());
            let value = encode_intent(INTENT_RETIRE, &key, &blob_dir, now_ms, 0);
            batch.put_cf(&intents, id_key(id), value);
            if existed {
                batch.merge_cf(&counters, KEY_OBJECT_COUNT, (-1i64).to_le_bytes());
            }
            db.write_opt(batch, &write_opts(durability))?;
            Ok(id)
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
        let db = self.db.clone();
        let cutoff = now_ms.saturating_sub(min_age_ms);
        let limit = limit.max(1) as usize;
        run_blocking(move || {
            let intents = cf(&db, CF_INTENTS)?;
            let mut out = Vec::new();
            for item in db.iterator_cf(&intents, IteratorMode::Start) {
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

    /// Bulk newer-wins insert used by the rebuild pipeline. Per key, the entry
    /// with the highest `last_modified_ms` wins (path as tie-break for
    /// determinism); every displaced blob dir is reported so the caller can
    /// trash it. Maintains the object counter as it goes.
    pub async fn insert_rebuild_batch(&self, entries: &[ObjectRecord]) -> Result<RebuildBatchOutcome> {
        let db = self.db.clone();
        let durability = self.durability;
        let entries = entries.to_vec();
        run_blocking(move || {
            let objects = cf(&db, CF_OBJECTS)?;
            let counters = cf(&db, CF_COUNTERS)?;
            let mut outcome = RebuildBatchOutcome::default();

            // Adjudicate duplicates within the batch first so the DB sees at
            // most one candidate per key.
            let mut best: std::collections::HashMap<&str, &ObjectRecord> = std::collections::HashMap::new();
            for entry in &entries {
                match best.get(entry.object_key.as_str()) {
                    Some(current) if !newer_wins(entry, current) => {
                        outcome.loser_blob_dirs.push(entry.blob_dir.clone());
                    }
                    Some(current) => {
                        outcome.loser_blob_dirs.push(current.blob_dir.clone());
                        best.insert(&entry.object_key, entry);
                    }
                    None => {
                        best.insert(&entry.object_key, entry);
                    }
                }
            }

            let mut batch = WriteBatch::default();
            let mut delta: i64 = 0;
            for entry in best.values() {
                let existing = db
                    .get_cf(&objects, entry.object_key.as_bytes())?
                    .map(|v| decode_object(entry.object_key.as_bytes(), &v))
                    .transpose()?;
                if let Some(existing) = existing {
                    if !newer_wins(entry, &existing) {
                        outcome.loser_blob_dirs.push(entry.blob_dir.clone());
                        continue;
                    }
                    outcome.loser_blob_dirs.push(existing.blob_dir);
                } else {
                    delta += 1;
                }
                batch.put_cf(&objects, entry.object_key.as_bytes(), encode_object(entry));
                outcome.inserted += 1;
            }
            if delta != 0 {
                batch.merge_cf(&counters, KEY_OBJECT_COUNT, delta.to_le_bytes());
            }
            db.write_opt(batch, &write_opts(durability))?;
            Ok(outcome)
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
                out.push(decode_object(&key, &value)?);
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
    let mut common_prefixes = Vec::new();
    let mut next_after = None;
    let mut skipped_common_prefixes = std::collections::HashSet::new();
    if let (Some(delimiter), Some(after)) = (delimiter, after) {
        if let Some(rest) = after.strip_prefix(prefix) {
            if let Some(idx) = rest.find(delimiter) {
                skipped_common_prefixes.insert(format!("{}{}", prefix, &rest[..idx + delimiter.len()]));
            }
        }
    }
    let mut more_matching = false;
    let iter = db.iterator_cf(&objects, IteratorMode::From(start.as_bytes(), Direction::Forward));
    for item in iter {
        let (key, value) = item?;
        if after.is_some() && key.as_ref() == after_str.as_bytes() {
            continue; // `after` is exclusive
        }
        let entry = decode_object(&key, &value)?;
        if !entry.object_key.starts_with(prefix) {
            break;
        }
        if entries.len() + common_prefixes.len() >= max_keys {
            more_matching = true;
            break;
        }
        if let Some(delimiter) = delimiter {
            let rest = &entry.object_key[prefix.len()..];
            if let Some(idx) = rest.find(delimiter) {
                let common = format!("{}{}", prefix, &rest[..idx + delimiter.len()]);
                if !skipped_common_prefixes.insert(common.clone()) {
                    next_after = Some(entry.object_key);
                    continue;
                }
                if common_prefixes.last() != Some(&common) {
                    common_prefixes.push(common);
                }
                next_after = Some(entry.object_key);
                continue;
            }
        }
        next_after = Some(entry.object_key.clone());
        entries.push(entry);
    }

    Ok(ListPage {
        entries,
        common_prefixes,
        is_truncated: more_matching,
        next_after,
    })
}

/// Matches the legacy 4-level fanout layout `objects/xx/xx/xx/xx/…` (four
/// exactly-two-char levels, then at least a trailing slash). Equivalent to the
/// old `blob_dir LIKE 'objects/__/__/__/__/%'`.
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
    // The `%` follows a fifth `/`, so there must be a fifth segment (possibly
    // empty, for a trailing slash).
    parts.next().is_some()
}

/// Newer-wins adjudication: highest `last_modified_ms`, blob path as a
/// deterministic tie-break.
fn newer_wins(candidate: &ObjectRecord, incumbent: &ObjectRecord) -> bool {
    (candidate.last_modified_ms, candidate.blob_dir.as_str())
        > (incumbent.last_modified_ms, incumbent.blob_dir.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: &str, blob_dir: &str, lm: i64) -> ObjectRecord {
        ObjectRecord {
            object_key: key.to_string(),
            blob_dir: blob_dir.to_string(),
            size: 1,
            etag: "e".to_string(),
            last_modified_ms: lm,
        }
    }

    async fn open_tmp(tmp: &tempfile::TempDir) -> ObjectIndex {
        ObjectIndex::open(tmp.path(), Durability::Relaxed, 4).await.unwrap()
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
    async fn legacy_layout_keys_matches_four_level_fanout_only() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        put(&index, "legacy", "objects/aa/bb/cc/dd/V1_9F00", 1).await;
        put(&index, "modern", "objects/aabb/V1_9F00", 1).await;
        let keys = index.legacy_layout_keys(100).await.unwrap();
        assert_eq!(keys, vec!["legacy".to_string()]);
    }

    #[tokio::test]
    async fn rebuild_batch_newer_wins_reports_losers() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        let outcome = index
            .insert_rebuild_batch(&[record("k", "objects/old", 5), record("k", "objects/new", 9)])
            .await
            .unwrap();
        assert_eq!(outcome.inserted, 1);
        assert_eq!(outcome.loser_blob_dirs, vec!["objects/old".to_string()]);
        assert_eq!(index.object_count().await.unwrap(), 1);
        // A later, older duplicate arriving in a separate batch also loses.
        let outcome = index
            .insert_rebuild_batch(&[record("k", "objects/older", 1)])
            .await
            .unwrap();
        assert_eq!(outcome.inserted, 0);
        assert_eq!(outcome.loser_blob_dirs, vec!["objects/older".to_string()]);
        assert_eq!(index.get("k").await.unwrap().unwrap().blob_dir, "objects/new");
        assert_eq!(index.object_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn outdated_schema_is_rejected_and_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        // Stamp an outdated schema version directly, then close.
        {
            let db_path = index_db_path(tmp.path());
            let index = ObjectIndex::open_at(&db_path, Durability::Relaxed, 1).await.unwrap();
            let meta = cf(&index.db, CF_META).unwrap();
            index
                .db
                .put_cf(&meta, KEY_SCHEMA_VERSION, (SCHEMA_VERSION - 1).to_le_bytes())
                .unwrap();
            index.db.flush().unwrap();
        }
        let err = ObjectIndex::open(tmp.path(), Durability::Relaxed, 1).await;
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
    async fn zero_max_keys_ignores_lexically_later_nonmatching_key() {
        let tmp = tempfile::tempdir().unwrap();
        let index = open_tmp(&tmp).await;
        put(&index, "z", "d", 1).await;
        let page = index.list("a/", None, None, 0).await.unwrap();
        assert!(!page.is_truncated);
    }
}
