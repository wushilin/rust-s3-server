//! Core object-storage implementation.
//!
//! [`LocalObjectStore`] is the main entry point. The per-bucket RocksDB
//! catalog is the **source of truth**: a row in `objects` is the object, and
//! its `blob_dir` column is the only way blobs are located — the filesystem
//! is never scanned on the request path.
//!
//! Every mutation follows the same commit protocol:
//!
//! 1. stage the blob completely (parts + `meta.json`) and fsync it,
//! 2. record a `publish` intent (committed before touching the live tree),
//! 3. under the per-key lock: rename into a *fresh* uniquely-named live dir,
//! 4. flip the row atomically with the intent removal (one transaction),
//! 5. retire the displaced dir to trash last.
//!
//! Any crash lands on an unreferenced, never-visible blob dir described by a
//! surviving intent; recovery is a query over `intents`, never a tree walk.
//! Reads are strictly read-only — they never repair, never delete.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use md5::{Digest, Md5};

use super::encoding::hex_lower;
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

use super::aws_chunked::{decode_aws_chunked, AwsChunkedDecoder, CHUNK_SIGNATURE_MISMATCH};
use super::cache::BoundedLruCache;
use super::config::{DurabilityMode, StorageConfig};
use super::encoding::{validate_bucket_name, validate_object_key};
use super::layouts::{self, v3};
use super::errors::{Result, StorageError};
use super::stamp::{Rank, SiteClock, Stamp};
use super::write::outranks;
use super::index::{
    self, Displaced, Durability, IntentRecord, ListPage, ObjectRecord, ObjectIndex, Tombstone, VersionRecord,
    VersionTarget, INTENT_RETIRE,
};
use super::layout::StorageLayout;
use super::locks::ObjectLockTable;
use super::metadata::{BucketLayout, BucketVersioning, CURRENT_LAYOUT, META_FORMAT, 
    content_encoding_or_none, content_language_or_none, content_type_or_default,
    storage_class_or_default, unquote_etag, BucketMeta, CorsRule, ObjectMeta, ObjectStorageKind, PartMeta,
    PutMeta, UploadMeta,
};
use super::staging::{new_staging_id, validate_staging_id};
use super::time::now_ms;

const CACHE_SHARDS: usize = 64;
const BUCKET_META_CACHE_CAPACITY: usize = 1024;
const BUCKET_META_CACHE_SHARDS: usize = 16;
const INDEX_CACHE_WARN_THRESHOLD: usize = 500;
const MIN_MULTIPART_PART_SIZE: u64 = 5 * 1024 * 1024;
const REBUILD_PROGRESS_EVERY: usize = 1000;
const COPY_BUFFER_SIZE: usize = 256 * 1024;
/// How long a rebuild / bucket delete waits for requests already holding the
/// bucket's index handle to finish before giving up (the old database must
/// not be swapped out or deleted while anyone can still commit to it).
const INDEX_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Commit listener: called with `(bucket, changelog watermark)` after every
/// commit that appended a changelog entry. Must be cheap and non-blocking.
#[derive(Clone)]
pub struct ChangeListener(pub Arc<dyn Fn(&str, u64) + Send + Sync>);

impl std::fmt::Debug for ChangeListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ChangeListener")
    }
}

/// What a "restore from peer" repair asks for.
#[derive(Debug, Clone)]
pub struct RestoreRequest {
    pub bucket: String,
    pub key: String,
    /// The lost version's stamp and ETag.
    pub lost_stamp: Stamp,
    pub lost_etag: String,
    /// Accept a peer that only has an older version.
    pub allow_older: bool,
}

/// Fetches a lost object back from a replication peer (installed by the
/// replication service): `Ok(message)` once restored, `Err(why not)`.
#[derive(Clone)]
pub struct PeerRestorer(
    pub Arc<dyn Fn(RestoreRequest) -> futures::future::BoxFuture<'static, std::result::Result<String, String>> + Send + Sync>,
);

impl std::fmt::Debug for PeerRestorer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PeerRestorer")
    }
}

/// Told about each replication link reading a bucket's changelog:
/// `(bucket, the reader's x-rs3-replicator header, record it?)`. Returns the
/// `x-rs3-merge-back` answer (a merge link here already pairs the bucket
/// with that reader's site), if any. Installed by the replication service.
#[derive(Clone)]
pub struct ReplicaReader(pub Arc<dyn Fn(&str, &str, bool) -> Option<String> + Send + Sync>);

impl std::fmt::Debug for ReplicaReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicaReader")
    }
}

/// Bucket-deletion veto: returns why `bucket` may not be deleted, if so.
#[derive(Clone)]
pub struct DeleteGuard(pub Arc<dyn Fn(&str) -> Option<String> + Send + Sync>);

impl std::fmt::Debug for DeleteGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeleteGuard")
    }
}

/// Filesystem-backed S3-compatible object store.
///
/// Cheap to clone — all inner state is wrapped in `Arc`.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    layout: StorageLayout,
    durability: Durability,
    rebuild_reader_threads: usize,
    rebuild_queue_bound: usize,
    rebuild_batch_size: usize,
    locks: ObjectLockTable,
    index_cache: Arc<Mutex<HashMap<String, ObjectIndex>>>,
    /// Per-bucket async locks that serialise the "cache miss -> open the
    /// RocksDB index" window. RocksDB takes an exclusive directory LOCK on
    /// open, so two tasks opening the same bucket concurrently would collide
    /// and one would surface a spurious 5xx; this ensures only one open is in
    /// flight per bucket and the rest await it.
    index_open_locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    meta_cache: Arc<BoundedLruCache<ObjectCacheKey, CachedObjectMeta>>,
    bucket_meta_cache: Arc<BoundedLruCache<String, BucketMeta>>,
    /// Buckets currently undergoing a blocking index rebuild; every request
    /// against them fails with [`StorageError::BucketRebuilding`] (503).
    rebuilding: Arc<Mutex<HashSet<String>>>,
    /// Live progress per rebuilding bucket, for status displays.
    rebuild_progress: Arc<Mutex<HashMap<String, RebuildProgress>>>,
    /// Cooperative shutdown signal — cancel this to stop all background tasks.
    shutdown: CancellationToken,
    /// Told `(bucket, changelog watermark)` after every logged commit; the
    /// webhook dispatcher installs itself here.
    change_listener: Arc<std::sync::RwLock<Option<ChangeListener>>>,
    /// Vetoes bucket deletion (returns the reason) — the changelog
    /// subscriptions refuse while any subscription targets the bucket.
    delete_guard: Arc<std::sync::RwLock<Vec<DeleteGuard>>>,
    /// Vetoes making a bucket read-write (a mirror replica must stay
    /// read-only).
    read_write_guard: Arc<std::sync::RwLock<Vec<DeleteGuard>>>,
    /// Restores lost objects from replication peers (health-scan repair).
    peer_restorer: Arc<std::sync::RwLock<Option<PeerRestorer>>>,
    /// Records replication links reading this server's changelogs.
    replica_reader: Arc<std::sync::RwLock<Option<ReplicaReader>>>,
    /// Per-bucket "the changelog moved" signal, for long-polling readers.
    change_signals: Arc<Mutex<HashMap<String, Arc<tokio::sync::Notify>>>>,
    /// This site's identity and version-stamp clock. Opened by
    /// [`Self::init_site`] at startup; tests get an ephemeral one lazily.
    clock: Arc<std::sync::OnceLock<Arc<SiteClock>>>,
    /// The name is set by `server.site_name` (set by [`Self::init_site`]).
    site_name_locked: Arc<std::sync::OnceLock<bool>>,
    #[cfg(test)]
    crash_points: Arc<Mutex<HashSet<String>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersionEntry {
    pub key: String,
    /// The version id S3 clients see: the stamp dir, or `null`.
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified_ms: i64,
    pub etag: String,
    pub size: u64,
    pub delete_marker: bool,
    /// The version's stamp (the `x-rs3-stamp` form).
    pub stamp: Stamp,
}

/// One page of `ListObjectVersions`, key-ordered — one live version per key,
/// see [`LocalObjectStore::list_object_versions`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectVersionPage {
    pub entries: Vec<ObjectVersionEntry>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
}

/// One version of a key as the console and version-scoped reads see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion {
    pub record: VersionRecord,
    pub is_current: bool,
}

impl ObjectVersion {
    pub fn version_id(&self) -> String {
        self.record.version_id()
    }
}

pub use super::write::{WriteOptions, WriteOrigin};

/// Returned by a successful PUT or multipart complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutResult {
    pub etag: String,
    pub size: u64,
    pub last_modified_ms: i64,
    pub stamp: Option<Stamp>,
    /// The S3 version id of the written state (`x-amz-version-id`): the
    /// stamp dir when the bucket's versioning is enabled, `null` when
    /// suspended, absent when never enabled.
    pub version_id: Option<String>,
}

/// Returned by a delete: the removed object's indexed size (`None` when
/// no object was at the key), and — on a bucket with versioning enabled or
/// suspended — the delete marker's S3 version id (`x-amz-version-id`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteResult {
    pub size: Option<u64>,
    pub version_id: Option<String>,
    /// Whether the delete left a delete marker in S3 terms (the bucket has
    /// versioning enabled or suspended).
    pub delete_marker: bool,
}

/// A conditional-write precondition, evaluated against the object currently at
/// the key under the per-key lock so the check and the write are atomic.
///
/// These mirror the S3 `PutObject` preconditions the `object_store` crate emits
/// for `PutMode::Create` and `PutMode::Update` — the primitive Delta Lake and
/// Iceberg build conflict-free commits on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// `If-None-Match: *` — the write succeeds only if no object exists.
    IfNoneMatchStar,
    /// `If-Match: "<etag>"` — the write succeeds only if the current object's
    /// ETag matches (compared ignoring surrounding quotes, weak prefix, case).
    IfMatch(String),
}

impl Precondition {
    /// Is the precondition satisfied given the ETag currently at the key
    /// (`None` when the key is empty)?
    fn is_met(&self, current_etag: Option<&str>) -> bool {
        match self {
            Precondition::IfNoneMatchStar => current_etag.is_none(),
            Precondition::IfMatch(want) => current_etag.is_some_and(|have| etag_eq(have, want)),
        }
    }
}

/// Compares two ETags for equality, ignoring surrounding quotes, an optional
/// weak `W/` prefix, and ASCII case. rusts3 stores a bare MD5 hex; a client's
/// `If-Match` header carries the quoted form it received in the ETag response.
fn etag_eq(a: &str, b: &str) -> bool {
    let norm = |s: &str| {
        s.trim()
            .trim_start_matches("W/")
            .trim_matches('"')
            .to_ascii_lowercase()
    };
    norm(a) == norm(b)
}

/// Live progress of a bucket index rebuild, for status displays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildProgress {
    pub objects_indexed: usize,
    pub dirs_trashed: usize,
    pub started_at_ms: i64,
}

/// Outcome of one stale-intent resolution pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IntentResolution {
    pub selected: usize,
    pub resolved: usize,
    pub failed: usize,
    /// Highest intent id selected — pass it back as `after_id` to continue
    /// past this batch (including any intents that failed) within a run.
    pub last_id: Option<i64>,
}

/// Result of a successful object read.
///
/// Contains only the metadata and the on-disk location; the body is never
/// buffered in memory.  Use [`ReadObject::object_dir`] plus the part file
/// names in `meta.parts` to stream content directly from disk.
#[derive(Debug, Clone)]
pub struct ReadObject {
    pub meta: ObjectMeta,
    /// Starting byte offset for each part in `meta.parts`.
    pub part_offsets: Vec<u64>,
    /// Directory that contains the part files (`part.1`, `part.2`, …).
    pub object_dir: std::path::PathBuf,
}

/// One part in a `CompleteMultipartUpload` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletePartRequest {
    pub number: u16,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ObjectCacheKey {
    bucket: String,
    key: String,
}

/// Cached parse of a blob dir's `meta.json`, valid only while the row still
/// carries the same `(etag, last_modified_ms)` — the row is re-read on every
/// request, the cache only skips the meta.json disk read.
#[derive(Debug, Clone)]
struct CachedObjectMeta {
    meta: ObjectMeta,
    part_offsets: Vec<u64>,
    etag: String,
    last_modified_ms: i64,
}

impl ObjectCacheKey {
    fn new(bucket: &str, key: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }
    }
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::from_storage_config(root, &StorageConfig::default())
    }

    pub fn from_storage_config(root: impl Into<PathBuf>, config: &StorageConfig) -> Self {
        Self::inner(StorageLayout::new(root), config)
    }

    pub fn with_layout(layout: StorageLayout) -> Self {
        Self::inner(layout, &StorageConfig::default())
    }

    fn inner(layout: StorageLayout, config: &StorageConfig) -> Self {
        Self {
            layout,
            durability: match config.durability {
                DurabilityMode::Full => Durability::Full,
                DurabilityMode::Relaxed => Durability::Relaxed,
            },
            rebuild_reader_threads: config.rebuild_reader_threads, // 0 = auto (core count)
            rebuild_queue_bound: config.rebuild_queue_bound.max(1),
            rebuild_batch_size: config.rebuild_batch_size.max(1),
            locks: ObjectLockTable::default(),
            index_cache: Arc::new(Mutex::new(HashMap::new())),
            index_open_locks: Arc::new(Mutex::new(HashMap::new())),
            meta_cache: Arc::new(BoundedLruCache::new(
                config.meta_cache_capacity.max(1),
                CACHE_SHARDS,
            )),
            bucket_meta_cache: Arc::new(BoundedLruCache::new(
                BUCKET_META_CACHE_CAPACITY,
                BUCKET_META_CACHE_SHARDS,
            )),
            rebuilding: Arc::new(Mutex::new(HashSet::new())),
            rebuild_progress: Arc::new(Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
            change_listener: Arc::new(std::sync::RwLock::new(None)),
            delete_guard: Arc::new(std::sync::RwLock::new(Vec::new())),
            read_write_guard: Arc::new(std::sync::RwLock::new(Vec::new())),
            change_signals: Arc::new(Mutex::new(HashMap::new())),
            peer_restorer: Arc::new(std::sync::RwLock::new(None)),
            replica_reader: Arc::new(std::sync::RwLock::new(None)),
            clock: Arc::new(std::sync::OnceLock::new()),
            site_name_locked: Arc::new(std::sync::OnceLock::new()),
            #[cfg(test)]
            crash_points: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Installs the commit listener (replacing any previous one). Every
    /// index reads the slot on each commit, so this also covers indexes
    /// opened earlier.
    pub fn set_change_listener(&self, listener: ChangeListener) {
        *self.change_listener.write().unwrap() = Some(listener);
    }

    /// Installs the peer restorer (the replication service).
    pub fn set_peer_restorer(&self, restorer: PeerRestorer) {
        *self.peer_restorer.write().unwrap() = Some(restorer);
    }

    /// Installs the replica-reader hook (the replication service).
    pub fn set_replica_reader(&self, reader: ReplicaReader) {
        *self.replica_reader.write().unwrap() = Some(reader);
    }

    /// A replication link (header `x-rs3-replicator`) read `bucket`'s
    /// changelog (`record`), or a link being created asks; returns the
    /// `x-rs3-merge-back` answer, if any.
    pub(crate) fn replica_read(&self, bucket: &str, replicator: &str, record: bool) -> Option<String> {
        let reader = self.replica_reader.read().unwrap().clone();
        reader.and_then(|reader| reader.0(bucket, replicator, record))
    }

    /// Fetches a lost object back from a replication peer.
    pub(crate) async fn restore_from_peer(&self, request: RestoreRequest) -> std::result::Result<String, String> {
        let restorer = self.peer_restorer.read().unwrap().clone();
        match restorer {
            Some(restorer) => restorer.0(request).await,
            None => Err("replication is not available on this server".to_string()),
        }
    }

    /// Adds a veto on making a bucket read-write. Any one refusing blocks it.
    pub fn add_read_write_guard(&self, guard: DeleteGuard) {
        self.read_write_guard.write().unwrap().push(guard);
    }

    /// Adds a bucket-deletion veto. Any one refusing blocks the delete.
    pub fn add_delete_guard(&self, guard: DeleteGuard) {
        self.delete_guard.write().unwrap().push(guard);
    }

    /// Opens this site's identity and clock (`<root>/site.json`), with an
    /// optional configured site id. Call once at startup, before any write.
    /// `configured_name` (`server.site_name`), when set, is applied at every
    /// start and locks the name; otherwise the stored (first: generated)
    /// name stands and can be changed with [`Self::set_site_name`].
    pub fn init_site(&self, configured_name: Option<&str>) -> Result<()> {
        let clock = SiteClock::open(self.layout.root())?;
        let configured = configured_name.map(str::trim).filter(|n| !n.is_empty());
        if let Some(name) = configured {
            if clock.site_name() != name {
                clock.set_site_name(name)?;
            }
        }
        let _ = self.site_name_locked.set(configured.is_some());
        log::info!(
            "site id {} name {:?}{}",
            clock.site_id(),
            clock.site_name(),
            if configured.is_some() { " (server.site_name)" } else { "" }
        );
        self.clock
            .set(Arc::new(clock))
            .map_err(|_| StorageError::Io("site clock already initialised".to_string()))
    }

    fn clock(&self) -> &Arc<SiteClock> {
        self.clock.get_or_init(|| {
            // Tests and tools that never call init_site: persisted when the
            // data root is writable, so stamps still survive a reopen.
            Arc::new(
                SiteClock::open(self.layout.root())
                    .unwrap_or_else(|_| SiteClock::ephemeral("local")),
            )
        })
    }

    /// This site's id (see `storage::stamp`): generated, never changes.
    pub fn site_id(&self) -> String {
        self.clock().site_id().to_string()
    }

    /// This site's readable name: a label only, free to change.
    pub fn site_name(&self) -> String {
        self.clock().site_name()
    }

    /// Whether the name is set by `server.site_name` (then it can only be
    /// changed there).
    pub fn site_name_locked(&self) -> bool {
        self.site_name_locked.get().copied().unwrap_or(false)
    }

    /// Renames this site (persisted; takes effect at once). Refused when
    /// `server.site_name` sets it.
    pub fn set_site_name(&self, name: &str) -> Result<String> {
        if self.site_name_locked() {
            return Err(StorageError::Io(
                "the site name is set by server.site_name in the config; change it there".to_string(),
            ));
        }
        self.clock().set_site_name(name)?;
        Ok(self.site_name())
    }

    /// The next local version stamp.
    pub fn next_stamp(&self) -> Stamp {
        self.clock().next()
    }

    /// The effective stamp of a stored state: its own, or for objects
    /// written before stamps existed, `(last_modified_ms, 0, this site)`.
    pub fn effective_stamp(&self, stamp: Option<&Stamp>, last_modified_ms: i64) -> Stamp {
        stamp
            .cloned()
            .unwrap_or_else(|| Stamp::fallback(last_modified_ms, self.clock().site_id()))
    }

    /// The bucket's changelog signal: woken (`notify_waiters`) after every
    /// logged commit. Long-polling readers `enable()` a `notified()` future
    /// before reading, then wait on it, so no commit can slip between.
    pub fn change_signal(&self, bucket: &str) -> Arc<tokio::sync::Notify> {
        self.change_signals
            .lock()
            .unwrap()
            .entry(bucket.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone()
    }

    /// Wraps a freshly opened index with the bucket's commit notification.
    fn attach_change_hook(&self, bucket: &str, index: ObjectIndex) -> ObjectIndex {
        let slot = self.change_listener.clone();
        let signal = self.change_signal(bucket);
        let bucket = bucket.to_string();
        index.with_commit_hook(Arc::new(move |watermark| {
            if let Some(listener) = slot.read().unwrap().as_ref() {
                listener.0(&bucket, watermark);
            }
            signal.notify_waiters();
        }))
    }

    /// Returns a clone of the shutdown token. Call `.cancel()` on it to stop all background tasks.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Takes the per-key write lock. Exposed for the health-scan repair path
    /// (`super::scan`), which mutates rows and blob dirs and must serialise
    /// against publish/delete of the same key exactly as the write path does.
    pub(crate) async fn lock_object_key(
        &self,
        bucket: &str,
        key: &str,
    ) -> super::locks::ObjectWriteGuard {
        self.locks.lock(bucket, key).await
    }

    /// Takes the per-upload lock that Complete and Abort hold. The staging
    /// sweeper takes it (then the key lock) before reaping a multipart
    /// upload, so it can never delete an upload mid-Complete.
    pub(crate) async fn lock_multipart_upload(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> super::locks::ObjectWriteGuard {
        self.locks.lock(bucket, &upload_lock_key(upload_id)).await
    }

    /// Drops a key's cached `meta.json` parse. Anything that changes a row out
    /// of band must call this so reads don't serve the stale parse.
    pub(crate) fn forget_cached_meta(&self, bucket: &str, key: &str) {
        self.meta_cache.remove(&ObjectCacheKey::new(bucket, key));
    }

    pub fn layout(&self) -> &StorageLayout {
        &self.layout
    }

    /// Crash injection for tests: a labeled point in a commit sequence
    /// panics when armed, simulating a crash at exactly that step.
    #[cfg(test)]
    pub fn arm_crash_point(&self, name: &str) {
        self.crash_points.lock().unwrap().insert(name.to_string());
    }

    #[cfg(test)]
    pub fn disarm_crash_point(&self, name: &str) {
        self.crash_points.lock().unwrap().remove(name);
    }

    fn crash_point(&self, name: &str) {
        #[cfg(test)]
        {
            if self.crash_points.lock().unwrap().contains(name) {
                panic!("crash injected at {name}");
            }
        }
        #[cfg(not(test))]
        let _ = name;
    }

    // ── bucket management ─────────────────────────────────────────────────────

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        tokio::fs::create_dir_all(&bucket_dir).await?;
        let bucket_meta_path = self.layout.bucket_meta_path(bucket)?;
        if !bucket_meta_path.exists() {
            let meta = BucketMeta {
                created_at_ms: now_ms(),
                storage_version: String::new(),
                layout: Some(BucketLayout::current()),
                cors: Vec::new(),
                read_only: false,
                versioning: BucketVersioning::Off,
            };
            write_json_atomic(&bucket_meta_path, &meta).await?;
            self.bucket_meta_cache.insert(bucket.to_string(), meta);
            if self.durability == Durability::Full {
                // `bucket_exists` gates every object operation on this marker.
                // Make it (and the directory entry created for it) durable
                // before we ack, so a crash can't hide a bucket whose synced
                // index and blobs already exist.
                fsync_file(&bucket_meta_path).await?;
                fsync_dir(&bucket_dir).await?;
            }
        }
        let _ = self.index(bucket).await?;
        Ok(())
    }

    pub async fn bucket_exists(&self, bucket: &str) -> bool {
        self.layout
            .bucket_meta_path(bucket)
            .ok()
            .map(|p| p.exists())
            .unwrap_or(false)
    }

    pub async fn bucket_meta(&self, bucket: &str) -> Result<BucketMeta> {
        validate_bucket_name(bucket)?;
        if let Some(meta) = self.bucket_meta_cache.get(&bucket.to_string()) {
            return Ok(meta);
        }
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        let meta: BucketMeta = read_json(&self.layout.bucket_meta_path(bucket)?).await?;
        self.bucket_meta_cache
            .insert(bucket.to_string(), meta.clone());
        Ok(meta)
    }

    pub async fn set_bucket_cors(&self, bucket: &str, cors: Vec<CorsRule>) -> Result<()> {
        validate_bucket_name(bucket)?;
        let _guard = self.locks.lock(bucket, "\0bucket-meta").await;
        let path = self.layout.bucket_meta_path(bucket)?;
        if !path.exists() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        let mut meta: BucketMeta = read_json(&path).await?;
        meta.cors = cors;
        write_json_atomic(&path, &meta).await?;
        if self.durability == Durability::Full {
            fsync_file(&path).await?;
            fsync_dir(path.parent().unwrap_or(self.layout.root())).await?;
        }
        self.bucket_meta_cache.insert(bucket.to_string(), meta);
        Ok(())
    }

    /// Marks a bucket read-only (only replicated writes accepted) or
    /// read-write.
    pub async fn set_bucket_read_only(&self, bucket: &str, read_only: bool) -> Result<()> {
        validate_bucket_name(bucket)?;
        if !read_only {
            let veto = self.read_write_guard.read().unwrap().iter().find_map(|g| g.0(bucket));
            if let Some(reason) = veto {
                return Err(StorageError::BucketInUse(reason));
            }
        }
        let _guard = self.locks.lock(bucket, "\0bucket-meta").await;
        let path = self.layout.bucket_meta_path(bucket)?;
        if !path.exists() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        let mut meta: BucketMeta = read_json(&path).await?;
        meta.read_only = read_only;
        write_json_atomic(&path, &meta).await?;
        if self.durability == Durability::Full {
            fsync_file(&path).await?;
            fsync_dir(path.parent().unwrap_or(self.layout.root())).await?;
        }
        self.bucket_meta_cache.insert(bucket.to_string(), meta);
        log::info!("bucket {bucket} is now {}", if read_only { "read-only" } else { "read-write" });
        Ok(())
    }

    /// Refuses a client (unstamped) write to a read-only bucket.
    /// Refuses a write a read-only bucket does not take: a client's. Every
    /// other origin (replication, a mirror's sweep, a repair) writes it.
    pub async fn check_write_allowed(&self, bucket: &str, origin: &WriteOrigin) -> Result<()> {
        if origin.allowed_on_read_only() {
            return Ok(());
        }
        self.ensure_writable(bucket).await
    }

    pub async fn ensure_writable(&self, bucket: &str) -> Result<()> {
        if self.bucket_meta(bucket).await.map(|m| m.read_only).unwrap_or(false) {
            return Err(StorageError::BucketReadOnly(bucket.to_string()));
        }
        Ok(())
    }

    pub async fn list_buckets(&self) -> Result<Vec<(String, BucketMeta)>> {
        tokio::fs::create_dir_all(self.layout.root().join("buckets")).await?;
        let mut entries = Vec::new();
        let mut dirs = tokio::fs::read_dir(self.layout.root().join("buckets")).await?;
        while let Some(dir) = dirs.next_entry().await? {
            let path = dir.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
                continue;
            };
            if validate_bucket_name(name).is_err() {
                continue;
            }
            let meta_path = self.layout.bucket_meta_path(name)?;
            if !meta_path.exists() {
                continue;
            }
            let meta = read_json(&meta_path).await?;
            entries.push((name.to_string(), meta));
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(entries)
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        let veto = self.delete_guard.read().unwrap().iter().find_map(|g| g.0(bucket));
        if let Some(reason) = veto {
            return Err(StorageError::BucketInUse(reason));
        }
        // Open through the normal path first (surfaces rebuild-needed etc.).
        drop(self.index(bucket).await?);
        // Raise the data-plane gate so no new request can pick up the index,
        // then drain the requests that already hold it: one that is mid-PUT
        // would otherwise commit into a database we are about to delete.
        if !self.try_begin_rebuild(bucket) {
            return Err(StorageError::BucketRebuilding(bucket.to_string()));
        }
        let _gate = RebuildGateGuard {
            store: self,
            bucket,
        };
        let open_lock = self.index_open_lock(bucket);
        let _open_guard = open_lock.lock().await;
        let index = match self.take_index_exclusive(bucket).await? {
            Some(index) => index,
            None => ObjectIndex::open(&bucket_dir, self.durability).await?,
        };
        let empty = index.is_empty().await? && !has_active_staging(&bucket_dir).await?;
        if !empty {
            self.index_cache
                .lock()
                .unwrap()
                .insert(bucket.to_string(), index);
            return Err(StorageError::InvalidMultipartUpload(
                "bucket is not empty".to_string(),
            ));
        }
        self.bucket_meta_cache.remove(&bucket.to_string());
        index.close().await;
        drop(index);
        tokio::fs::remove_dir_all(&bucket_dir).await?;
        if self.durability == Durability::Full {
            // Make the directory-entry removal durable before we ack, so a
            // crash can't resurrect the just-deleted bucket. The parent is
            // `buckets/`; if it's already gone there is nothing to sync.
            if let Some(parent) = bucket_dir.parent() {
                let _ = fsync_dir(parent).await;
            }
        }
        Ok(())
    }

    /// Opens (or returns the cached) catalog for `bucket`. Buckets whose
    /// index is missing (with blobs present) or on a legacy schema trigger a
    /// background rebuild and fail with `BucketRebuilding` until it
    /// completes.
    pub async fn index(&self, bucket: &str) -> Result<ObjectIndex> {
        self.index_impl(bucket, true).await
    }

    /// [`Self::index`] for maintenance that runs *behind* the 503 gate (a
    /// layout migration holds the gate up while it rewrites the bucket):
    /// opens the index whether or not the gate is up. Never for a request.
    pub(crate) async fn index_unchecked(&self, bucket: &str) -> Result<ObjectIndex> {
        self.index_impl(bucket, false).await
    }

    async fn index_impl(&self, bucket: &str, gated: bool) -> Result<ObjectIndex> {
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        if gated && self.rebuilding.lock().unwrap().contains(bucket) {
            return Err(StorageError::BucketRebuilding(bucket.to_string()));
        }
        if let Some(index) = self.index_cache.lock().unwrap().get(bucket).cloned() {
            return Ok(index);
        }
        // Serialise the open per bucket: RocksDB takes an exclusive directory
        // LOCK on open, so concurrent first-touches of the same bucket would
        // otherwise race and one would get a spurious error. Whoever wins the
        // lock opens and caches; everyone else re-checks the cache below.
        let open_lock = self.index_open_lock(bucket);
        let _open_guard = open_lock.lock().await;
        // Re-check under the open lock: a rebuild or bucket delete holds it
        // while it swaps/deletes the database, and raises the gate first.
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        if gated && self.rebuilding.lock().unwrap().contains(bucket) {
            return Err(StorageError::BucketRebuilding(bucket.to_string()));
        }
        if let Some(index) = self.index_cache.lock().unwrap().get(bucket).cloned() {
            return Ok(index);
        }
        if index::needs_rebuild(&bucket_dir).await? {
            self.start_rebuild_background(bucket);
            return Err(StorageError::BucketRebuilding(bucket.to_string()));
        }
        let index = match ObjectIndex::open(&bucket_dir, self.durability).await {
            Ok(index) => self.attach_change_hook(bucket, index),
            Err(StorageError::IndexOutdated(_)) => {
                self.start_rebuild_background(bucket);
                return Err(StorageError::BucketRebuilding(bucket.to_string()));
            }
            Err(err) => return Err(err),
        };
        let mut cache = self.index_cache.lock().unwrap();
        let index = cache
            .entry(bucket.to_string())
            .or_insert(index)
            .clone();
        if cache.len() > INDEX_CACHE_WARN_THRESHOLD {
            log::warn!(
                "index_cache has {} entries — consider reducing bucket count or adding eviction",
                cache.len()
            );
        }
        Ok(index)
    }

    /// The per-bucket async lock serialising index open / swap / delete.
    fn index_open_lock(&self, bucket: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.index_open_locks.lock().unwrap();
        locks
            .entry(bucket.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Removes the bucket's cached index handle and waits until every request
    /// that already cloned it has dropped its clone, so the caller holds the
    /// only handle and nobody can still commit to the database. The caller
    /// must hold the bucket's [`index_open_lock`](Self::index_open_lock) (so
    /// nothing re-opens it) and should have raised the rebuild gate (so new
    /// requests fail fast instead of queueing on that lock).
    ///
    /// A handle still in use after [`INDEX_DRAIN_TIMEOUT`] (e.g. a long
    /// storage scan) aborts the operation: the handle goes back into the
    /// cache untouched and a clear error is returned rather than hanging.
    async fn take_index_exclusive(&self, bucket: &str) -> Result<Option<ObjectIndex>> {
        let Some(index) = self.index_cache.lock().unwrap().remove(bucket) else {
            return Ok(None);
        };
        let deadline = tokio::time::Instant::now() + INDEX_DRAIN_TIMEOUT;
        while index.handle_count() > 1 {
            if tokio::time::Instant::now() >= deadline {
                let in_use = index.handle_count() - 1;
                self.index_cache
                    .lock()
                    .unwrap()
                    .insert(bucket.to_string(), index);
                return Err(StorageError::Io(format!(
                    "bucket {bucket} index is still in use by {in_use} in-flight operation(s) \
                     after {}s (a running scan?); retry once they finish",
                    INDEX_DRAIN_TIMEOUT.as_secs()
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        Ok(Some(index))
    }

    // ── puts (staging unchanged, commit rewritten) ────────────────────────────

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        aws_chunked: bool,
    ) -> Result<PutResult> {
        let decoded;
        let payload = if aws_chunked {
            decoded = decode_aws_chunked(bytes)?;
            decoded.as_slice()
        } else {
            bytes
        };
        let staging_id = self
            .stage_put(bucket, key, payload, content_type, content_encoding)
            .await?;
        self.commit_staged_put(bucket, key, &staging_id, None).await
    }

    pub async fn put_object_stream<S, E>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        aws_chunked: bool,
        expected_sha256: Option<&str>,
    ) -> Result<PutResult>
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        self.put_object_stream_with_metadata(
            bucket,
            key,
            stream,
            content_type,
            content_encoding,
            None,
            None,
            &BTreeMap::new(),
            aws_chunked,
            expected_sha256,
            None,
            WriteOptions::default(),
        )
        .await
    }

    pub async fn put_object_stream_with_metadata<S, E>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        storage_class: Option<&str>,
        content_language: Option<&str>,
        user_meta: &BTreeMap<String, String>,
        aws_chunked: bool,
        expected_sha256: Option<&str>,
        expected_decoded_len: Option<u64>,
        options: WriteOptions,
    ) -> Result<PutResult>
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let staging_id = if aws_chunked {
            self.stage_put_aws_chunked_stream_with_metadata(
                bucket,
                key,
                stream,
                content_type,
                content_encoding,
                storage_class,
                content_language,
                user_meta,
                expected_decoded_len,
            )
            .await?
        } else {
            self.stage_put_stream_with_metadata(
                bucket,
                key,
                stream,
                content_type,
                content_encoding,
                storage_class,
                content_language,
                user_meta,
                expected_sha256,
            )
            .await?
        };
        self.commit_staged_put_with(bucket, key, &staging_id, options)
            .await
    }

    pub async fn stage_put(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        content_type: Option<&str>,
        content_encoding: Option<&str>,
    ) -> Result<String> {
        self.stage_put_with_metadata(
            bucket,
            key,
            bytes,
            content_type,
            content_encoding,
            None,
            None,
            &BTreeMap::new(),
        )
        .await
    }

    pub async fn stage_put_with_metadata(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        storage_class: Option<&str>,
        content_language: Option<&str>,
        user_meta: &BTreeMap<String, String>,
    ) -> Result<String> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let staging_id = new_staging_id(now_ms());
        let staging_dir = self.layout.put_staging_dir(bucket, &staging_id)?;
        tokio::fs::create_dir_all(&staging_dir).await?;
        let cleanup = StagingDirGuard::new(&staging_dir);
        let part_path = staging_dir.join("part.1");
        let etag = write_file_with_md5(&part_path, bytes).await?;
        let meta = PutMeta {
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            write_id: staging_id.clone(),
            initiated_at_ms: now_ms(),
            size: bytes.len() as u64,
            etag,
            content_type: content_type_or_default(content_type),
            content_encoding: content_encoding_or_none(content_encoding),
            content_language: content_language_or_none(content_language),
            storage_class: storage_class_or_default(storage_class),
            user_meta: user_meta.clone(),
        };
        write_json_atomic(&staging_dir.join("put.json"), &meta).await?;
        cleanup.disarm();
        Ok(staging_id)
    }

    pub async fn stage_put_stream<S, E>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        expected_sha256: Option<&str>,
    ) -> Result<String>
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        self.stage_put_stream_with_metadata(
            bucket,
            key,
            stream,
            content_type,
            content_encoding,
            None,
            None,
            &BTreeMap::new(),
            expected_sha256,
        )
        .await
    }

    pub async fn stage_put_stream_with_metadata<S, E>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        storage_class: Option<&str>,
        content_language: Option<&str>,
        user_meta: &BTreeMap<String, String>,
        expected_sha256: Option<&str>,
    ) -> Result<String>
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        self.ensure_bucket_and_key(bucket, key).await?;
        let staging_id = new_staging_id(now_ms());
        let staging_dir = self.layout.put_staging_dir(bucket, &staging_id)?;
        tokio::fs::create_dir_all(&staging_dir).await?;
        let cleanup = StagingDirGuard::new(&staging_dir);
        let part_path = staging_dir.join("part.1");
        let written = match write_stream_with_hashes(&part_path, stream).await {
            Ok(written) => written,
            Err(err) => {
                let _ = tokio::fs::remove_dir_all(&staging_dir).await;
                return Err(err);
            }
        };
        if let Some(expected) = expected_sha256 {
            if !expected.eq_ignore_ascii_case(&written.sha256) {
                let _ = tokio::fs::remove_dir_all(&staging_dir).await;
                return Err(StorageError::PayloadHashMismatch {
                    expected: expected.to_string(),
                    actual: written.sha256,
                });
            }
        }
        let meta = PutMeta {
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            write_id: staging_id.clone(),
            initiated_at_ms: now_ms(),
            size: written.size,
            etag: written.md5,
            content_type: content_type_or_default(content_type),
            content_encoding: content_encoding_or_none(content_encoding),
            content_language: content_language_or_none(content_language),
            storage_class: storage_class_or_default(storage_class),
            user_meta: user_meta.clone(),
        };
        write_json_atomic(&staging_dir.join("put.json"), &meta).await?;
        cleanup.disarm();
        Ok(staging_id)
    }

    pub async fn stage_put_aws_chunked_stream<S, E>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
    ) -> Result<String>
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        self.stage_put_aws_chunked_stream_with_metadata(
            bucket,
            key,
            stream,
            content_type,
            content_encoding,
            None,
            None,
            &BTreeMap::new(),
            None,
        )
        .await
    }

    pub async fn stage_put_aws_chunked_stream_with_metadata<S, E>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        storage_class: Option<&str>,
        content_language: Option<&str>,
        user_meta: &BTreeMap<String, String>,
        expected_decoded_len: Option<u64>,
    ) -> Result<String>
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        self.ensure_bucket_and_key(bucket, key).await?;
        let staging_id = new_staging_id(now_ms());
        let staging_dir = self.layout.put_staging_dir(bucket, &staging_id)?;
        tokio::fs::create_dir_all(&staging_dir).await?;
        let cleanup = StagingDirGuard::new(&staging_dir);
        let part_path = staging_dir.join("part.1");
        let written = match write_aws_chunked_stream_with_hashes(&part_path, stream).await {
            Ok(written) => written,
            Err(err) => {
                let _ = tokio::fs::remove_dir_all(&staging_dir).await;
                return Err(err);
            }
        };
        // Reject a truncated (or over-long) chunked body: the client declares the
        // real payload size via `x-amz-decoded-content-length`, and a mismatch
        // means the stream did not carry what it promised. AWS answers such a
        // request with `IncompleteBody` rather than silently storing a short
        // object with an ETag over the wrong bytes.
        if let Some(expected) = expected_decoded_len {
            if written.size != expected {
                let _ = tokio::fs::remove_dir_all(&staging_dir).await;
                return Err(StorageError::InvalidAwsChunkedBody(format!(
                    "decoded body length {} does not match declared x-amz-decoded-content-length {}",
                    written.size, expected
                )));
            }
        }
        let meta = PutMeta {
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            write_id: staging_id.clone(),
            initiated_at_ms: now_ms(),
            size: written.size,
            etag: written.md5,
            content_type: content_type_or_default(content_type),
            content_encoding: content_encoding_or_none(content_encoding),
            content_language: content_language_or_none(content_language),
            storage_class: storage_class_or_default(storage_class),
            user_meta: user_meta.clone(),
        };
        write_json_atomic(&staging_dir.join("put.json"), &meta).await?;
        cleanup.disarm();
        Ok(staging_id)
    }

    pub async fn commit_staged_put(
        &self,
        bucket: &str,
        key: &str,
        staging_id: &str,
        precondition: Option<Precondition>,
    ) -> Result<PutResult> {
        self.commit_staged_put_with(bucket, key, staging_id, WriteOptions::precondition(precondition))
            .await
    }

    /// [`Self::commit_staged_put`] with full [`WriteOptions`] (a replicated
    /// stamp).
    pub async fn commit_staged_put_with(
        &self,
        bucket: &str,
        key: &str,
        staging_id: &str,
        options: WriteOptions,
    ) -> Result<PutResult> {
        validate_staging_id(staging_id)?;
        let staging_dir = self.layout.put_staging_dir(bucket, staging_id)?;
        let put_meta: PutMeta = read_json(&staging_dir.join("put.json")).await?;
        if put_meta.bucket != bucket
            || put_meta.object_key != key
            || put_meta.write_id != staging_id
        {
            return Err(StorageError::InvalidStagingId(staging_id.to_string()));
        }
        // From here the staging dir is ours to consume: every failure (a
        // missing part, a failed prepare, a cancelled request) removes it.
        let cleanup = StagingDirGuard::new(&staging_dir);
        let staged_part = staging_dir.join("part.1");
        let staged_part = if staged_part.exists() {
            staged_part
        } else {
            staging_dir.join("object").join("part.1")
        };
        ensure_file_exists(&staged_part).await?;

        let object_meta = ObjectMeta {
            format_version: META_FORMAT,
            stamp: None,
            versioned: false,
            delete_marker: false,
            removed: None,
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            storage: ObjectStorageKind::Single,
            size: put_meta.size,
            etag: put_meta.etag.clone(),
            last_modified_ms: now_ms(),
            content_type: put_meta.content_type.clone(),
            content_encoding: put_meta.content_encoding.clone(),
            content_language: put_meta.content_language.clone(),
            storage_class: put_meta.storage_class.clone(),
            user_meta: put_meta.user_meta.clone(),
            parts: vec![PartMeta {
                number: 1,
                file: "part.1".to_string(),
                size: put_meta.size,
                etag: put_meta.etag.clone(),
                last_modified_ms: now_ms(),
            }],
        };
        let publish_dir =
            prepare_single_publish_dir(&staging_dir, &staged_part, &object_meta).await?;

        // Remove staging on every path — a rejected precondition must not leak
        // the staged blob.
        let result = self
            .publish_prepared_dir(bucket, key, &publish_dir, object_meta, options)
            .await;
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
        cleanup.disarm();
        result
    }

    /// The shared commit section for PUT / CopyObject / CompleteMultipart:
    /// decide under the key lock → intent → rename into the live tree at the
    /// version's own path → atomic row flip → retire what was displaced (or
    /// keep it as a noncurrent version). `publish_dir` must already contain
    /// the complete blob (parts + meta.json).
    async fn publish_prepared_dir(
        &self,
        bucket: &str,
        key: &str,
        publish_dir: &Path,
        mut object_meta: ObjectMeta,
        options: WriteOptions,
    ) -> Result<PutResult> {
        let WriteOptions { origin, precondition, versioned: versioned_hint } = options;
        if let Err(err) = self.check_write_allowed(bucket, &origin).await {
            let _ = tokio::fs::remove_dir_all(publish_dir).await;
            return Err(err);
        }
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        let versioning = self.bucket_meta(bucket).await?.versioning;
        if self.durability == Durability::Full {
            fsync_publish_dir(publish_dir, &object_meta).await?;
        }

        let index = self.index(bucket).await?;
        let _guard = self.locks.lock(bucket, key).await;

        let old = index.get(key).await?;

        // Conditional write: evaluate the precondition against the object
        // currently at the key, atomically under the lock we already hold, so
        // the check and the publish cannot race. On failure, abandon the
        // staged blob and report 412.
        if let Some(precondition) = &precondition {
            if !precondition.is_met(old.as_ref().map(|o| o.etag.as_str())) {
                let _ = tokio::fs::remove_dir_all(publish_dir).await;
                return Err(StorageError::PreconditionFailed {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        }

        // The key's current state: the object, or its tombstone.
        let old_tombstone = index.get_tombstone(key).await?;
        let current = self.current_rank(&index, key, old.as_ref()).await?;
        let versioned = self.versioned_bit(&index, key, versioning, versioned_hint).await?;
        // On a versioning-enabled bucket a replicated state that does not
        // outrank the current one is not stale: it is history, kept as a
        // noncurrent version (every site holds the same set of versions).
        if versioning == BucketVersioning::Enabled {
            if let Some(given) = origin.given_stamp().filter(|_| origin.adjudicated()) {
                let below = current.as_ref().is_some_and(|(rank, _)| !outranks(&Rank::live(given.clone()), Some(rank)));
                if below {
                    return self
                        .insert_history_version(bucket, key, &index, publish_dir, object_meta, given.clone(), versioned, &origin)
                        .await;
                }
            }
        }
        let stamp = match self.decide(bucket, key, &origin, origin.put_rank(), current.as_ref()) {
            Ok(stamp) => stamp,
            Err(err) => {
                let _ = tokio::fs::remove_dir_all(publish_dir).await;
                return Err(err);
            }
        };
        let last_modified_ms = match origin.given_stamp() {
            // A replicated state shows its origin's time.
            Some(_) => stamp.ms,
            // Monotonic per-key clamp: newer-wins adjudication (rebuild, DR)
            // requires timestamps to order versions even across clock steps.
            None => old
                .as_ref()
                .map(|o| o.last_modified_ms.saturating_add(1))
                .unwrap_or(i64::MIN)
                .max(stamp.ms),
        };
        if last_modified_ms != object_meta.last_modified_ms
            || object_meta.stamp.as_ref() != Some(&stamp)
            || object_meta.versioned != versioned
            || object_meta.format_version != META_FORMAT
        {
            object_meta.format_version = META_FORMAT;
            object_meta.last_modified_ms = last_modified_ms;
            object_meta.stamp = Some(stamp.clone());
            object_meta.versioned = versioned;
            write_json_atomic(&publish_dir.join("meta.json"), &object_meta).await?;
            if self.durability == Durability::Full {
                fsync_file(&publish_dir.join("meta.json")).await?;
                // `write_json_atomic` swaps meta.json in via a same-dir rename;
                // fsyncing the file alone leaves that directory-entry change
                // unstable. Fsync `publish_dir` so the rewrite survives a crash
                // (the later parent fsync only covers publish_dir's own name).
                fsync_dir(publish_dir).await?;
            }
        }

        // The version's path is its stamp: decided, so the intent can name it.
        let blob_rel = v3::blob_rel(bucket, key, &stamp);
        let intent_id = index.insert_publish_intent(key, &blob_rel, now_ms()).await?;
        self.crash_point("publish_after_intent");

        // Rename into the live tree, retrying on ENOENT (an empty-leaf
        // cleaner may race dir creation — the retry recreates it). A dir
        // already at the version's path can only be a leftover (the same
        // stamp is never decided twice for a key): it is trashed first.
        let objects_dir = bucket_dir.join("objects");
        let objects_existed = tokio::fs::metadata(&objects_dir).await.is_ok();
        let mut leaf_created = false;
        let mut attempts = 0u32;
        let dest = loop {
            // Backstop against any unforeseen non-converging retry: the loop
            // runs while holding the per-key lock, so it must never spin
            // unbounded.
            attempts += 1;
            if attempts > 10_000 {
                let _ = index.delete_intent(intent_id).await;
                return Err(StorageError::Io(format!(
                    "publish of {}/{key} did not converge after {attempts} attempts",
                    bucket
                )));
            }
            let leaf = bucket_dir.join(blob_rel_parent(&blob_rel));
            if tokio::fs::metadata(&leaf).await.is_err() {
                // A new directory entry in objects/ that must itself be made
                // durable (below), or a crash can lose the whole leaf.
                leaf_created = true;
            }
            match tokio::fs::create_dir_all(&leaf).await {
                Ok(()) => {}
                // A leaf the reclaimer is deleting at this instant: Windows
                // reports the delete-pending directory as denied, not gone.
                Err(err) if is_leaf_reclaimed_race(&err) => continue,
                Err(err) => return Err(err.into()),
            }
            let dest = bucket_dir.join(&blob_rel);
            match tokio::fs::rename(publish_dir, &dest).await {
                Ok(()) => break dest,
                Err(err) if is_leaf_reclaimed_race(&err) => {
                    // ENOENT is ambiguous: it can mean the destination fanout
                    // leaf was reclaimed out from under us (recreate + retry),
                    // OR the *source* dir vanished — e.g. a racing abort/second
                    // complete removed the staging. If the source is gone there
                    // is nothing to publish; drop the now-pointless intent and
                    // fail, rather than loop forever holding the key lock.
                    let source_gone = matches!(
                        tokio::fs::metadata(publish_dir).await,
                        Err(e) if e.kind() == ErrorKind::NotFound
                    );
                    if source_gone {
                        let _ = index.delete_intent(intent_id).await;
                        return Err(StorageError::Io(format!(
                            "publish source vanished (concurrent abort/complete?): {}",
                            publish_dir.display()
                        )));
                    }
                    continue;
                }
                Err(err)
                    if err.kind() == ErrorKind::AlreadyExists
                        || err.kind() == ErrorKind::DirectoryNotEmpty =>
                {
                    log::warn!("publish bucket={bucket} key={key}: leftover dir at {blob_rel}; trashing it");
                    move_object_dir_to_trash(&self.layout, bucket, &dest).await?;
                    continue;
                }
                Err(err) => {
                    // Nothing reached the live tree; the intent is now
                    // pointless — best-effort removal, the resolver would
                    // find nothing at the path anyway.
                    let _ = index.delete_intent(intent_id).await;
                    return Err(err.into());
                }
            }
        };
        self.crash_point("publish_after_rename");
        if self.durability == Durability::Full {
            fsync_dir(dest.parent().unwrap_or(&bucket_dir)).await?;
            if leaf_created {
                fsync_dir(&objects_dir).await?;
                if !objects_existed {
                    fsync_dir(&bucket_dir).await?;
                }
            }
        }

        let record = ObjectRecord {
            object_key: key.to_string(),
            blob_dir: blob_rel.clone(),
            size: object_meta.size,
            etag: object_meta.etag.clone(),
            last_modified_ms,
            parts: Some(crate::storage::changelog::PartLayout::of(&object_meta)),
            stamp: Some(stamp.clone()),
            versioned,
        };
        // What the write displaces: the previous row and marker are kept as
        // noncurrent versions while versioning is enabled (and real versions
        // always are); `null` states are retired.
        let mut displaced = Displaced::default();
        if let Some(old) = old.as_ref().filter(|old| !index::same_blob_dir(&old.blob_dir, &blob_rel)) {
            // (A restore republishing the very version the row names lands
            // at the same path: nothing is displaced — the leftover, if
            // any, went to trash before the rename.)
            if old.versioned || versioned {
                let old_stamp = self.effective_stamp(old.stamp.as_ref(), old.last_modified_ms);
                displaced.keep.push(VersionRecord::from_row(old, old_stamp));
            } else {
                displaced.retire.push(old.blob_dir.clone());
            }
        }
        if let Some(t) = &old_tombstone {
            if let Some(marker) = VersionRecord::from_tombstone(t) {
                if t.versioned || versioned {
                    displaced.keep.push(marker);
                } else {
                    displaced.retire.push(marker.blob_dir);
                }
            }
        }
        if !versioned {
            Self::displace_null_versions(&index, key, &mut displaced).await?;
        }
        let retire_dirs = displaced.retire.clone();
        let retire_ids = index
            .commit_publish_from(&record, intent_id, displaced, now_ms(), origin.change_origin())
            .await?;
        self.crash_point("publish_after_commit");

        self.meta_cache.remove(&ObjectCacheKey::new(bucket, key));
        self.retire_displaced(bucket, key, &index, &retire_ids, &retire_dirs).await;

        Ok(PutResult {
            etag: object_meta.etag,
            size: object_meta.size,
            last_modified_ms,
            version_id: Self::version_id_for(versioning, &stamp),
            stamp: Some(stamp),
        })
    }

    /// A key has at most one `null` state: a new `null` state (a write or a
    /// delete while versioning is off or suspended) replaces a `null`
    /// version kept among the noncurrent ones too, as S3 does.
    async fn displace_null_versions(index: &ObjectIndex, key: &str, displaced: &mut Displaced) -> Result<()> {
        for v in index.versions_of(key).await? {
            if !v.versioned {
                displaced.retire.push(v.blob_dir.clone());
                displaced.drop_versions.push(v.version_dir());
            }
        }
        Ok(())
    }

    /// Whether a state written now is a real version. Only on a
    /// versioning-enabled bucket; there, a replicated state that was the
    /// `null` version on its site (`hint` `Some(false)`) stays `null` here
    /// if the key has no `null` state yet, and is a real version otherwise
    /// (a key holds one `null` state, so the history of an unversioned
    /// source is still kept). Called under the key lock.
    async fn versioned_bit(&self, index: &ObjectIndex, key: &str, versioning: BucketVersioning, hint: Option<bool>) -> Result<bool> {
        Ok(match (versioning, hint) {
            (BucketVersioning::Enabled, Some(false)) => self.has_null_state(index, key).await?,
            (BucketVersioning::Enabled, _) => true,
            _ => false,
        })
    }

    /// Whether this site already holds (or permanently removed) version
    /// `stamp` of `key`: as the current object, the current marker, a
    /// noncurrent version, or a version tombstone.
    pub async fn version_known(&self, bucket: &str, key: &str, stamp: &Stamp) -> Result<bool> {
        let index = self.index(bucket).await?;
        self.version_known_in(&index, key, stamp).await
    }

    async fn version_known_in(&self, index: &ObjectIndex, key: &str, stamp: &Stamp) -> Result<bool> {
        if let Some(row) = index.get(key).await? {
            if self.effective_stamp(row.stamp.as_ref(), row.last_modified_ms) == *stamp {
                return Ok(true);
            }
        }
        if index.get_tombstone(key).await?.is_some_and(|t| t.stamp == *stamp) {
            return Ok(true);
        }
        let dir = v3::stamp_dir(stamp);
        Ok(index.get_version(key, &dir).await?.is_some() || index.has_version_tombstone(key, &dir).await?)
    }

    /// Whether the key has a `null` state anywhere (current or kept).
    async fn has_null_state(&self, index: &ObjectIndex, key: &str) -> Result<bool> {
        if index.get(key).await?.is_some_and(|row| !row.versioned) {
            return Ok(true);
        }
        if index.get_tombstone(key).await?.is_some_and(|t| !t.versioned && t.blob_dir.is_some()) {
            return Ok(true);
        }
        Ok(index.versions_of(key).await?.iter().any(|v| !v.versioned))
    }

    /// Commits `publish_dir` as a noncurrent version `stamp` of `key` (a
    /// replicated state older than the current one, on a versioning-enabled
    /// bucket). Idempotent: a version this site already knows is dropped.
    /// A `null` state never joins a key that has one. The caller holds the
    /// key lock.
    #[allow(clippy::too_many_arguments)]
    async fn insert_history_version(
        &self,
        bucket: &str,
        key: &str,
        index: &ObjectIndex,
        publish_dir: &Path,
        mut meta: ObjectMeta,
        stamp: Stamp,
        versioned: bool,
        origin: &WriteOrigin,
    ) -> Result<PutResult> {
        let result = PutResult {
            etag: meta.etag.clone(),
            size: meta.size,
            last_modified_ms: stamp.ms,
            version_id: Some(if versioned { v3::stamp_dir(&stamp) } else { "null".to_string() }),
            stamp: Some(stamp.clone()),
        };
        if self.version_known_in(index, key, &stamp).await? || (!versioned && self.has_null_state(index, key).await?) {
            let _ = tokio::fs::remove_dir_all(publish_dir).await;
            return Ok(result);
        }
        meta.format_version = META_FORMAT;
        meta.stamp = Some(stamp.clone());
        meta.versioned = versioned;
        meta.last_modified_ms = stamp.ms;
        write_json_atomic(&publish_dir.join("meta.json"), &meta).await?;
        if self.durability == Durability::Full {
            fsync_file(&publish_dir.join("meta.json")).await?;
            fsync_dir(publish_dir).await?;
        }
        let blob_rel = v3::blob_rel(bucket, key, &stamp);
        let intent_id = index.insert_publish_intent(key, &blob_rel, now_ms()).await?;
        self.rename_into_tree(bucket, publish_dir, &blob_rel).await?;
        let record = VersionRecord {
            object_key: key.to_string(),
            stamp: stamp.clone(),
            blob_dir: blob_rel,
            size: meta.size,
            etag: meta.etag.clone(),
            last_modified_ms: stamp.ms,
            parts: Some(crate::storage::changelog::PartLayout::of(&meta)),
            versioned,
            delete_marker: false,
            removed: None,
        };
        index
            .commit_version_insert(&record, Some(intent_id), Displaced::default(), now_ms(), origin.change_origin())
            .await?;
        Ok(result)
    }

    /// Commits a delete marker `stamp` (removing `removed`) as a noncurrent
    /// version of `key`: a replicated delete older than the key's current
    /// state, on a versioning-enabled bucket. Idempotent. The caller holds
    /// the key lock.
    async fn insert_history_marker(
        &self,
        bucket: &str,
        key: &str,
        index: &ObjectIndex,
        stamp: Stamp,
        removed: Stamp,
        versioned: bool,
        origin: &WriteOrigin,
    ) -> Result<DeleteResult> {
        let result = DeleteResult {
            size: None,
            version_id: Some(if versioned { v3::stamp_dir(&stamp) } else { "null".to_string() }),
            delete_marker: true,
        };
        if self.version_known_in(index, key, &stamp).await? || (!versioned && self.has_null_state(index, key).await?) {
            return Ok(result);
        }
        let marker_rel = self.write_delete_marker(bucket, key, &stamp, &removed, versioned).await?;
        let record = VersionRecord {
            object_key: key.to_string(),
            stamp: stamp.clone(),
            blob_dir: marker_rel,
            size: 0,
            etag: String::new(),
            last_modified_ms: stamp.ms,
            parts: None,
            versioned,
            delete_marker: true,
            removed: Some(removed),
        };
        index
            .commit_version_insert(&record, None, Displaced::default(), now_ms(), origin.change_origin())
            .await?;
        Ok(result)
    }

    /// Moves a prepared blob dir to `blob_rel` in the live tree (a leftover
    /// at that path is trashed first), making the new entries durable.
    async fn rename_into_tree(&self, bucket: &str, publish_dir: &Path, blob_rel: &str) -> Result<PathBuf> {
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        let dest = bucket_dir.join(blob_rel);
        let leaf = bucket_dir.join(blob_rel_parent(blob_rel));
        for attempt in 0..100 {
            tokio::fs::create_dir_all(&leaf).await?;
            match tokio::fs::rename(publish_dir, &dest).await {
                Ok(()) => break,
                Err(err) if err.kind() == ErrorKind::AlreadyExists || err.kind() == ErrorKind::DirectoryNotEmpty => {
                    move_object_dir_to_trash(&self.layout, bucket, &dest).await?;
                }
                Err(err) if is_leaf_reclaimed_race(&err) && attempt < 99 => continue,
                Err(err) => return Err(err.into()),
            }
        }
        if self.durability == Durability::Full {
            fsync_dir(&leaf).await?;
            fsync_dir(&bucket_dir.join("objects")).await?;
        }
        Ok(dest)
    }

    /// The `x-amz-version-id` of a state written with `stamp` under
    /// `versioning`.
    fn version_id_for(versioning: BucketVersioning, stamp: &Stamp) -> Option<String> {
        match versioning {
            BucketVersioning::Off => None,
            BucketVersioning::Enabled => Some(v3::stamp_dir(stamp)),
            BucketVersioning::Suspended => Some("null".to_string()),
        }
    }

    /// Trashes the blob dirs a commit displaced, dropping each retire
    /// intent as its dir goes; a dir that cannot be moved keeps its intent
    /// for the resolver. In relaxed durability the commit may still sit in
    /// an unsynced WAL: it is synced first, or a power loss could roll the
    /// row back to a blob dir that is already in trash.
    async fn retire_displaced(&self, bucket: &str, key: &str, index: &ObjectIndex, ids: &[i64], dirs: &[String]) {
        if dirs.is_empty() {
            return;
        }
        if self.durability == Durability::Relaxed {
            if let Err(err) = index.sync_wal().await {
                log::warn!("wal sync before retire failed bucket={bucket} key={key} error={err}");
                return;
            }
        }
        let Ok(bucket_dir) = self.layout.bucket_dir(bucket) else {
            return;
        };
        for (id, dir) in ids.iter().zip(dirs) {
            match move_object_dir_to_trash(&self.layout, bucket, &bucket_dir.join(dir)).await {
                Ok(_) => {
                    let _ = index.delete_intent(*id).await;
                }
                Err(err) => {
                    // The retire intent stays; the resolver finishes the job.
                    log::warn!("retire failed bucket={bucket} key={key} dir={dir} error={err}");
                }
            }
        }
    }

    // ── reads (strictly read-only) ────────────────────────────────────────────

    /// Resolves `(bucket, key)` through its row — the single source of
    /// truth. On a missing blob the row is re-read **once** (a concurrent
    /// overwrite may have retired the dir mid-read); a row that still points
    /// at a missing blob is an internal inconsistency (`CorruptObject`,
    /// mapped to 5xx), never a 404, and never triggers any repair or
    /// deletion from the read path.
    pub async fn read_object(&self, bucket: &str, key: &str) -> Result<ReadObject> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        let index = self.index(bucket).await?;
        let cache_key = ObjectCacheKey::new(bucket, key);

        let mut last_err: Option<StorageError> = None;
        for _attempt in 0..2 {
            let Some(row) = index.get(key).await? else {
                return Err(StorageError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            };
            let object_dir = bucket_dir.join(&row.blob_dir);

            if let Some(cached) = self
                .meta_cache
                .get(&cache_key)
                .filter(|c| c.etag == row.etag && c.last_modified_ms == row.last_modified_ms)
            {
                return Ok(ReadObject {
                    meta: cached.meta,
                    part_offsets: cached.part_offsets,
                    object_dir,
                });
            }

            match read_json::<ObjectMeta>(&object_dir.join("meta.json")).await {
                Ok(meta) => {
                    validate_object_parts(&object_dir, &meta).await?;
                    let cached = CachedObjectMeta {
                        part_offsets: part_offsets(&meta),
                        meta,
                        etag: row.etag.clone(),
                        last_modified_ms: row.last_modified_ms,
                    };
                    self.meta_cache.insert(cache_key.clone(), cached.clone());
                    return Ok(ReadObject {
                        meta: cached.meta,
                        part_offsets: cached.part_offsets,
                        object_dir,
                    });
                }
                Err(err) => {
                    last_err = Some(err);
                    continue;
                }
            }
        }
        let detail = last_err.map(|e| e.to_string()).unwrap_or_default();
        Err(StorageError::CorruptObject(format!(
            "row for {bucket}/{key} references unreadable blob: {detail}"
        )))
    }

    /// The rank of `key`'s current state (see [`Rank`]): the live object's,
    /// or its tombstone's, whichever ranks higher; `None` when the key was
    /// never seen (or its tombstone expired). Also returns the newest stamp
    /// involved, for the clock to observe.
    async fn current_rank(&self, index: &ObjectIndex, key: &str, row: Option<&ObjectRecord>) -> Result<Option<(Rank, Stamp)>> {
        let tombstone = index.get_tombstone(key).await?;
        let live = row.map(|row| self.effective_stamp(row.stamp.as_ref(), row.last_modified_ms));
        let newest = match (&live, &tombstone) {
            (Some(live), Some(t)) => Some(live.clone().max(t.stamp.clone())),
            (Some(live), None) => Some(live.clone()),
            (None, Some(t)) => Some(t.stamp.clone()),
            (None, None) => None,
        };
        let rank = match (live.map(Rank::live), tombstone.map(|t| t.rank())) {
            (Some(live), Some(tomb)) => Some(live.max(tomb)),
            (live, tomb) => live.or(tomb),
        };
        Ok(rank.zip(newest))
    }

    /// The rank of `key`'s current state, for replicators deciding whether a
    /// change is newer than what is here.
    pub async fn key_rank(&self, bucket: &str, key: &str) -> Result<Option<Rank>> {
        let index = self.index(bucket).await?;
        let row = index.get(key).await?;
        Ok(self.current_rank(&index, key, row.as_ref()).await?.map(|(rank, _)| rank))
    }

    fn stale(bucket: &str, key: &str, current: &Rank) -> StorageError {
        StorageError::StaleStamp {
            bucket: bucket.to_string(),
            key: key.to_string(),
            current: if current.deleted {
                format!("{} (deleted)", current.stamp.to_header())
            } else {
                current.stamp.to_header()
            },
        }
    }

    /// The current state's stamp of `key` (see [`Self::current_stamp`]),
    /// and whether it is a tombstone. For HEAD of a deleted key and for
    /// replicators comparing states.
    pub async fn key_stamp(&self, bucket: &str, key: &str) -> Result<Option<(Stamp, bool)>> {
        let index = self.index(bucket).await?;
        if let Some(row) = index.get(key).await? {
            return Ok(Some((self.effective_stamp(row.stamp.as_ref(), row.last_modified_ms), false)));
        }
        Ok(index.get_tombstone(key).await?.map(|t| (t.stamp, true)))
    }

    /// Deletes `key` as `options.origin`. Like every write it is decided
    /// under the key lock by rank: a delete ranks by the version it
    /// `removed` (just above it), so it never removes a version newer than
    /// the one its author saw. `removed` is that version's stamp: a
    /// replicated delete brings its origin's (`x-rs3-removed-stamp`; the
    /// delete's own stamp when the peer sends none), a mirror's sweep the
    /// version it listed, and a client's delete (`None`) removes whatever
    /// is current. A delete that does not outrank the current state fails
    /// with [`StorageError::StaleStamp`] and changes nothing. A key already
    /// absent still records the tombstone when the delete names a version,
    /// so that version (or an older one) arriving later cannot come back;
    /// a client's delete of an absent key is a no-op (S3: idempotent).
    ///
    /// The delete writes a marker dir beside the key's versions (so a
    /// rebuild finds it) and, while versioning is enabled, keeps the removed
    /// state as a noncurrent version. Returns the deleted size and the
    /// marker's S3 version id (see [`DeleteResult`]).
    pub async fn delete_object_with(
        &self,
        bucket: &str,
        key: &str,
        options: WriteOptions,
        removed: Option<Stamp>,
    ) -> Result<DeleteResult> {
        let WriteOptions { origin, precondition, versioned: versioned_hint } = options;
        self.ensure_bucket_and_key(bucket, key).await?;
        self.check_write_allowed(bucket, &origin).await?;
        let versioning = self.bucket_meta(bucket).await?.versioning;
        let index = self.index(bucket).await?;
        let _guard = self.locks.lock(bucket, key).await;
        let row = index.get(key).await?;
        if let Some(precondition) = &precondition {
            if !precondition.is_met(row.as_ref().map(|r| r.etag.as_str())) {
                return Err(StorageError::PreconditionFailed {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        }
        let removed = match (removed, &row, origin.given_stamp()) {
            (Some(removed), _, _) => removed,
            // A peer that sends no removed stamp: the delete ranks by its own.
            (None, _, Some(given)) => given.clone(),
            // A local delete removes exactly the version it sees.
            (None, Some(row), None) => self.effective_stamp(row.stamp.as_ref(), row.last_modified_ms),
            (None, None, None) => return Ok(DeleteResult::default()),
        };
        let current = self.current_rank(&index, key, row.as_ref()).await?;
        let versioned = self.versioned_bit(&index, key, versioning, versioned_hint).await?;
        // History on a versioning-enabled bucket: a replicated delete that
        // does not outrank the current state is kept as a noncurrent marker.
        if versioning == BucketVersioning::Enabled {
            if let Some(given) = origin.given_stamp().filter(|_| origin.adjudicated()) {
                let below = current.as_ref().is_some_and(|(rank, _)| !outranks(&Rank::deleted(removed.clone()), Some(rank)));
                if below {
                    return self.insert_history_marker(bucket, key, &index, given.clone(), removed, versioned, &origin).await;
                }
            }
        }
        let stamp = self.decide(bucket, key, &origin, Some(Rank::deleted(removed.clone())), current.as_ref())?;
        let old_tombstone = index.get_tombstone(key).await?;
        // The marker goes to disk first: a rebuild after a crash past the
        // commit must find the delete.
        let marker_rel = self.write_delete_marker(bucket, key, &stamp, &removed, versioned).await?;
        let mut displaced = Displaced::default();
        if let Some(t) = &old_tombstone {
            if let Some(marker) = VersionRecord::from_tombstone(t) {
                if t.versioned || versioned {
                    displaced.keep.push(marker);
                } else {
                    displaced.retire.push(marker.blob_dir);
                }
            }
        }
        if !versioned {
            Self::displace_null_versions(&index, key, &mut displaced).await?;
        }
        let version_id = Self::version_id_for(versioning, &stamp);
        let size = match row {
            Some(row) => {
                if row.versioned || versioned {
                    let old_stamp = self.effective_stamp(row.stamp.as_ref(), row.last_modified_ms);
                    displaced.keep.push(VersionRecord::from_row(&row, old_stamp));
                } else {
                    displaced.retire.push(row.blob_dir.clone());
                }
                let retire_dirs = displaced.retire.clone();
                let ids = index
                    .commit_delete_from(
                        key,
                        displaced,
                        now_ms(),
                        origin.change_origin(),
                        Some(stamp),
                        Some(removed),
                        Some(marker_rel),
                        versioned,
                    )
                    .await?;
                self.crash_point("delete_after_commit");
                self.meta_cache.remove(&ObjectCacheKey::new(bucket, key));
                self.retire_displaced(bucket, key, &index, &ids, &retire_dirs).await;
                Some(row.size)
            }
            None => {
                let tombstone = Tombstone {
                    object_key: key.to_string(),
                    stamp,
                    removed: Some(removed),
                    deleted_at_ms: now_ms(),
                    version: String::new(),
                    etag: String::new(),
                    size: 0,
                    versioned,
                    blob_dir: Some(marker_rel.clone()),
                };
                let retire_dirs = displaced.retire.clone();
                match index.record_tombstone(tombstone, displaced, origin.change_origin()).await? {
                    Some(ids) => self.retire_displaced(bucket, key, &index, &ids, &retire_dirs).await,
                    None => {
                        // Not recorded (an equal or newer tombstone exists):
                        // the marker we wrote is an orphan.
                        let _ = tokio::fs::remove_dir_all(self.layout.bucket_dir(bucket)?.join(&marker_rel)).await;
                        return Ok(DeleteResult::default());
                    }
                }
                None
            }
        };
        Ok(DeleteResult { size, version_id, delete_marker: versioning != BucketVersioning::Off })
    }

    /// Writes the delete marker dir of `key` at `stamp` (a version dir whose
    /// `meta.json` says `delete_marker`, no parts) and returns its
    /// bucket-relative path. Idempotent: an existing dir at the path is
    /// rewritten.
    async fn write_delete_marker(&self, bucket: &str, key: &str, stamp: &Stamp, removed: &Stamp, versioned: bool) -> Result<String> {
        let rel = v3::blob_rel(bucket, key, stamp);
        let dir = self.layout.bucket_dir(bucket)?.join(&rel);
        let meta = ObjectMeta {
            format_version: META_FORMAT,
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            storage: ObjectStorageKind::Single,
            size: 0,
            etag: String::new(),
            last_modified_ms: stamp.ms,
            content_type: String::new(),
            content_encoding: None,
            content_language: None,
            storage_class: super::metadata::DEFAULT_STORAGE_CLASS.to_string(),
            user_meta: BTreeMap::new(),
            parts: Vec::new(),
            stamp: Some(stamp.clone()),
            versioned,
            delete_marker: true,
            removed: Some(removed.clone()),
        };
        tokio::fs::create_dir_all(&dir).await?;
        write_json_atomic(&dir.join("meta.json"), &meta).await?;
        if self.durability == Durability::Full {
            fsync_file(&dir.join("meta.json")).await?;
            fsync_dir(&dir).await?;
            if let Some(parent) = dir.parent() {
                fsync_dir(parent).await?;
            }
        }
        Ok(rel)
    }

    /// [`Self::delete_object_with`] from a request's optional `x-rs3-stamp`
    /// and `x-rs3-removed-stamp`.
    #[cfg(test)]
    pub async fn delete_object_stamped(
        &self,
        bucket: &str,
        key: &str,
        stamp: Option<Stamp>,
        removed: Option<Stamp>,
    ) -> Result<Option<u64>> {
        Ok(self.delete_object_with(bucket, key, WriteOptions::stamped(stamp), removed).await?.size)
    }

    /// A client's delete of `key`.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.delete_object_with_size(bucket, key).await.map(|_| ())
    }

    /// A client's delete, returning the indexed size when the object
    /// existed (for operation audit logging, without a metadata read).
    pub async fn delete_object_with_size(&self, bucket: &str, key: &str) -> Result<Option<u64>> {
        Ok(self.delete_object_with(bucket, key, WriteOptions::client(), None).await?.size)
    }

    /// Decides a write under the key lock — the one place every origin goes
    /// through. `incoming` is the write's rank when it has one to compare
    /// (a put with a given stamp; every delete, by the version it removes);
    /// it must outrank the key's `current` state, or the write is stale
    /// ([`StorageError::StaleStamp`]) — except for an origin that is not
    /// adjudicated (a restore). Returns the new state's stamp: the one the
    /// write brings, or a fresh local one, minted after observing the
    /// current state so it outranks what it replaces even if that came
    /// from a site whose clock runs ahead.
    fn decide(
        &self,
        bucket: &str,
        key: &str,
        origin: &WriteOrigin,
        incoming: Option<Rank>,
        current: Option<&(Rank, Stamp)>,
    ) -> Result<Stamp> {
        if origin.adjudicated() {
            if let (Some(incoming), Some((current, _))) = (&incoming, current) {
                if !outranks(incoming, Some(current)) {
                    return Err(Self::stale(bucket, key, current));
                }
            }
        }
        if let Some((_, newest)) = current {
            self.clock().observe(newest);
        }
        Ok(match origin.given_stamp() {
            Some(stamp) => {
                self.clock().observe(stamp);
                stamp.clone()
            }
            None => self.clock().next(),
        })
    }


    // ── intent resolution (the only deletion authority) ───────────────────────

    /// Resolves stale intents: trash whatever an abandoned publish left in
    /// the live tree, finish whatever retirement didn't complete. Intents
    /// are re-confirmed under the per-key lock before acting; a blob dir
    /// currently referenced by its row is never touched. Selection resumes
    /// strictly after intent id `after_id` (a cursor from the previous batch's
    /// `last_id`), so permanently failing intents can't starve the rest.
    pub async fn resolve_stale_intents(
        &self,
        bucket: &str,
        min_age_ms: i64,
        limit: usize,
        after_id: Option<i64>,
    ) -> Result<IntentResolution> {
        validate_bucket_name(bucket)?;
        let index = self.index(bucket).await?;
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        let intents = index
            .stale_intents_after(now_ms(), min_age_ms, limit.max(1) as i64, after_id)
            .await?;
        let mut outcome = IntentResolution {
            selected: intents.len(),
            last_id: intents.iter().map(|i| i.id).max(),
            ..IntentResolution::default()
        };
        for stale in intents {
            let _guard = self.locks.lock(bucket, &stale.object_key).await;
            match self.resolve_one_intent(&index, &bucket_dir, bucket, &stale).await {
                Ok(()) => outcome.resolved += 1,
                Err(err) => {
                    outcome.failed += 1;
                    let _ = index.bump_intent_attempts(stale.id).await;
                    log::warn!(
                        "intent resolution failed bucket={bucket} key={} intent={} op={} error={err}",
                        stale.object_key,
                        stale.id,
                        stale.op,
                    );
                }
            }
        }
        Ok(outcome)
    }

    async fn resolve_one_intent(
        &self,
        index: &ObjectIndex,
        bucket_dir: &Path,
        bucket: &str,
        stale: &IntentRecord,
    ) -> Result<()> {
        // Re-confirm under the key lock: the owning operation may have
        // completed (and removed the intent) between our snapshot and now.
        let Some(intent) = index.get_intent(stale.id).await? else {
            return Ok(());
        };
        let row = index.get(&intent.object_key).await?;
        let referenced = row
            .as_ref()
            .map(|r| super::index::same_blob_dir(&r.blob_dir, &intent.blob_dir))
            .unwrap_or(false);
        if referenced {
            // A live blob is never trashed. A publish intent here means the
            // commit happened; the lingering intent is the only anomaly.
            if intent.op == INTENT_RETIRE {
                log::warn!(
                    "retire intent {} references live blob {} — dropping intent without action",
                    intent.id,
                    intent.blob_dir
                );
            }
            return index.delete_intent(intent.id).await;
        }
        let age_ms = now_ms().saturating_sub(intent.created_at_ms);
        let abs = bucket_dir.join(&intent.blob_dir);
        if tokio::fs::metadata(&abs).await.is_ok() {
            move_object_dir_to_trash(&self.layout, bucket, &abs).await?;
            // op=publish: a crashed/failed publish that reached the live
            // tree but never committed its row — the orphan is retired.
            // op=retire: a retirement whose trash move never completed —
            // finished here.
            log::info!(
                "intent resolved bucket={bucket} key={} op={} intent_id={} age_ms={age_ms} attempts={} action=trashed dir={}",
                intent.object_key,
                intent.op,
                intent.id,
                intent.attempts,
                intent.blob_dir,
            );
        } else {
            // Nothing at the recorded path: the operation died before its
            // rename reached the live tree (publish), or the trash move
            // already succeeded but the intent delete didn't (retire).
            // Either way there is nothing to clean — just retire the record.
            log::info!(
                "intent resolved bucket={bucket} key={} op={} intent_id={} age_ms={age_ms} attempts={} action=no_blob_at_path dir={}",
                intent.object_key,
                intent.op,
                intent.id,
                intent.attempts,
                intent.blob_dir,
            );
        }
        index.delete_intent(intent.id).await
    }

    /// Startup drain: resolves every pending intent before the bucket
    /// serves traffic. O(operations in flight at crash time).
    pub async fn drain_intents(&self, bucket: &str) -> Result<usize> {
        let mut total = 0usize;
        let mut after_id = None;
        loop {
            let outcome = self.resolve_stale_intents(bucket, 0, 1000, after_id).await?;
            total += outcome.resolved;
            // Failures stay behind the cursor (retried next pass) instead of
            // being re-selected until the loop gives up on everything after.
            if outcome.selected < 1000 || outcome.last_id.is_none() {
                break;
            }
            after_id = outcome.last_id;
        }
        Ok(total)
    }

    // ── listing ───────────────────────────────────────────────────────────────

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max_keys: usize,
    ) -> Result<ListPage> {
        validate_bucket_name(bucket)?;
        validate_object_key(prefix).or_else(
            |err| {
                if prefix.is_empty() {
                    Ok(())
                } else {
                    Err(err)
                }
            },
        )?;
        self.index(bucket)
            .await?
            .list(prefix, delimiter, after, max_keys)
            .await
    }

    pub async fn object_count(&self, bucket: &str) -> Result<u64> {
        validate_bucket_name(bucket)?;
        self.index(bucket).await?.object_count().await
    }

    /// `ListObjectVersions`: every version of every key under `prefix`, in
    /// key order and newest first per key, from `key_marker` /
    /// `version_id_marker` on. A key's versions may be split across pages;
    /// the next markers name the last version listed. On a bucket that
    /// never had versioning each key has one `null` version (its live
    /// object).
    pub async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: &str,
        version_id_marker: &str,
        max_keys: usize,
    ) -> Result<ObjectVersionPage> {
        validate_bucket_name(bucket)?;
        validate_object_key(prefix)
            .or_else(|err| if prefix.is_empty() { Ok(()) } else { Err(err) })?;
        if max_keys == 0 {
            // A zero-sized page cannot carry the marker forward, so it must not
            // advertise a next page.
            return Ok(ObjectVersionPage::default());
        }
        let versioning = self.bucket_meta(bucket).await?.versioning;
        let index = self.index(bucket).await?;
        let key_marker = (!key_marker.is_empty()).then_some(key_marker);
        // A `null` marker resumes after the key's null version; a stamp dir
        // after that version; anything else (a stray id) after the key.
        let version_marker = match version_id_marker {
            "" => None,
            "null" => Some(String::from("null")),
            other => v3::parse_stamp_dir(other).map(|_| other.to_string()),
        };
        let page = index
            .list_versions_page(
                prefix,
                key_marker,
                version_marker.as_deref().filter(|m| *m != "null"),
                max_keys,
                &self.site_id(),
                versioning != BucketVersioning::Off,
            )
            .await?;
        let entries = page
            .entries
            .iter()
            .map(|(v, is_latest)| ObjectVersionEntry {
                key: v.object_key.clone(),
                version_id: v.version_id(),
                is_latest: *is_latest,
                last_modified_ms: v.last_modified_ms,
                etag: v.etag.clone(),
                size: v.size,
                delete_marker: v.delete_marker,
                stamp: v.stamp.clone(),
            })
            .collect::<Vec<_>>();
        let next_version_id_marker = page.is_truncated.then(|| {
            entries.last().map(|e| e.version_id.clone()).unwrap_or_default()
        });
        Ok(ObjectVersionPage {
            is_truncated: page.is_truncated,
            next_key_marker: page.is_truncated.then(|| page.next_key_marker.clone().unwrap_or_default()),
            next_version_id_marker,
            entries,
        })
    }

    // ── object versions ─────────────────────────────────────────────────────

    /// Every version of `key`, newest first: its current state (the object
    /// or its delete marker) and the noncurrent versions. Empty for a key
    /// never seen.
    pub async fn list_versions(&self, bucket: &str, key: &str) -> Result<Vec<ObjectVersion>> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let index = self.index(bucket).await?;
        let mut out = Vec::new();
        if let Some(row) = index.get(key).await? {
            let stamp = self.effective_stamp(row.stamp.as_ref(), row.last_modified_ms);
            out.push(ObjectVersion { record: VersionRecord::from_row(&row, stamp), is_current: true });
        } else if let Some(t) = index.get_tombstone(key).await? {
            if let Some(marker) = VersionRecord::from_tombstone(&t) {
                out.push(ObjectVersion { record: marker, is_current: true });
            }
        }
        for v in index.versions_of(key).await? {
            out.push(ObjectVersion { record: v, is_current: false });
        }
        Ok(out)
    }

    /// Resolves an S3 version id (`null`, or a stamp dir) to the version of
    /// `key` it names, with whether that version is current.
    /// [`StorageError::NoSuchVersion`] when there is none.
    pub async fn find_version(&self, bucket: &str, key: &str, version_id: &str) -> Result<ObjectVersion> {
        let no_such = || StorageError::NoSuchVersion {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.to_string(),
        };
        if version_id != "null" && v3::parse_stamp_dir(version_id).is_none() {
            return Err(no_such());
        }
        let versions = self.list_versions(bucket, key).await?;
        versions
            .into_iter()
            .find(|v| if version_id == "null" { !v.record.versioned } else { v.record.version_dir() == version_id })
            .ok_or_else(no_such)
    }

    /// Reads one version of `key` by its S3 version id. A delete marker is
    /// [`StorageError::VersionIsDeleteMarker`] (S3: 405).
    pub async fn read_object_version(&self, bucket: &str, key: &str, version_id: &str) -> Result<ReadObject> {
        let version = self.find_version(bucket, key, version_id).await?;
        if version.record.delete_marker {
            return Err(StorageError::VersionIsDeleteMarker {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: version.version_id(),
            });
        }
        if version.is_current {
            return self.read_object(bucket, key).await;
        }
        let object_dir = self.layout.bucket_dir(bucket)?.join(&version.record.blob_dir);
        let meta: ObjectMeta = read_json(&object_dir.join("meta.json")).await.map_err(|err| {
            StorageError::CorruptObject(format!(
                "version {} of {bucket}/{key} references unreadable blob: {err}",
                version.version_id()
            ))
        })?;
        validate_object_parts(&object_dir, &meta).await?;
        Ok(ReadObject { part_offsets: part_offsets(&meta), meta, object_dir })
    }

    /// Permanently removes one version of `key` (`DeleteObject?versionId`),
    /// live or delete marker, current or not. When the current state goes,
    /// the newest remaining version becomes current (a marker as the key's
    /// tombstone). Returns the removed version's size, or `None` when the
    /// version did not exist (S3: idempotent).
    pub async fn delete_object_version(&self, bucket: &str, key: &str, version_id: &str, origin: WriteOrigin) -> Result<Option<u64>> {
        self.ensure_bucket_and_key(bucket, key).await?;
        self.check_write_allowed(bucket, &origin).await?;
        let index = self.index(bucket).await?;
        let _guard = self.locks.lock(bucket, key).await;
        let Ok(version) = self.find_version(bucket, key, version_id).await else {
            // A replicated removal of a version this site never got (or
            // already removed): remember it, so a copy arriving later is
            // refused, and pass it on.
            if let (Some(given), Some(stamp)) = (origin.given_stamp(), v3::parse_stamp_dir(version_id)) {
                let dir = v3::stamp_dir(&stamp);
                if !index.has_version_tombstone(key, &dir).await? {
                    self.clock().observe(given);
                    index
                        .record_version_tombstone(key, &stamp, true, now_ms(), origin.change_origin())
                        .await?;
                }
            }
            return Ok(None);
        };
        let target = match (version.is_current, version.record.delete_marker) {
            (true, false) => VersionTarget::Current,
            (true, true) => VersionTarget::Marker,
            (false, _) => VersionTarget::Noncurrent(version.record.version_dir()),
        };
        let promote = if version.is_current {
            index.versions_of(key).await?.into_iter().next()
        } else {
            None
        };
        let current = self.current_rank(&index, key, index.get(key).await?.as_ref()).await?;
        let stamp = self.decide(bucket, key, &WriteOrigin::Repair, None, current.as_ref())?;
        let removed = index
            .commit_remove_version(key, target, promote, now_ms(), origin.change_origin(), stamp)
            .await?;
        self.meta_cache.remove(&ObjectCacheKey::new(bucket, key));
        if let Some((id, dir)) = removed {
            self.retire_displaced(bucket, key, &index, &[id], &[dir]).await;
        }
        Ok(Some(version.record.size))
    }

    /// The bucket's versioning state.
    pub async fn bucket_versioning(&self, bucket: &str) -> Result<BucketVersioning> {
        Ok(self.bucket_meta(bucket).await?.versioning)
    }

    /// Sets the bucket's versioning state (`PutBucketVersioning`). Enabling
    /// requires the bucket to be stable on the current layout (every object
    /// has a materialised stamp, so every version has an id); a bucket that
    /// was ever enabled can only be suspended, not turned off.
    pub async fn set_bucket_versioning(&self, bucket: &str, versioning: BucketVersioning) -> Result<()> {
        validate_bucket_name(bucket)?;
        let _guard = self.locks.lock(bucket, "\0bucket-meta").await;
        let path = self.layout.bucket_meta_path(bucket)?;
        if !path.exists() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        let mut meta: BucketMeta = read_json(&path).await?;
        if meta.versioning == versioning {
            return Ok(());
        }
        if versioning == BucketVersioning::Off {
            return Err(StorageError::BucketInUse(format!(
                "bucket {bucket}: versioning cannot be turned off once enabled, only suspended"
            )));
        }
        let layout = meta.layout.unwrap_or(BucketLayout::Stable { version: 1 });
        if !layout.is_stable_current() {
            return Err(StorageError::BucketInUse(format!(
                "bucket {bucket} is on storage layout {}; upgrade it to layout {CURRENT_LAYOUT} before enabling versioning",
                layout.target()
            )));
        }
        meta.versioning = versioning;
        write_json_atomic(&path, &meta).await?;
        if self.durability == Durability::Full {
            fsync_file(&path).await?;
            fsync_dir(path.parent().unwrap_or(self.layout.root())).await?;
        }
        self.bucket_meta_cache.insert(bucket.to_string(), meta);
        log::info!("bucket {bucket} versioning is now {}", versioning.as_str());
        Ok(())
    }

    /// Removes tombstones of deletes made before `cutoff_ms` (at most
    /// `max_entries`), trashing their marker dirs. Returns how many.
    pub async fn purge_tombstones(&self, bucket: &str, cutoff_ms: i64, max_entries: usize) -> Result<usize> {
        let index = self.index(bucket).await?;
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        let purged = index.purge_tombstones(cutoff_ms, max_entries).await?;
        index.purge_version_tombstones(cutoff_ms, max_entries).await?;
        for t in &purged {
            if let Some(dir) = &t.blob_dir {
                let _guard = self.locks.lock(bucket, &t.object_key).await;
                if let Err(err) = move_object_dir_to_trash(&self.layout, bucket, &bucket_dir.join(dir)).await {
                    log::warn!("purge of marker dir failed bucket={bucket} key={} dir={dir} error={err}", t.object_key);
                }
            }
        }
        Ok(purged.len())
    }

    // ── multipart ─────────────────────────────────────────────────────────────

    pub async fn initiate_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
    ) -> Result<String> {
        self.initiate_multipart_with_metadata(
            bucket,
            key,
            content_type,
            content_encoding,
            None,
            None,
            &BTreeMap::new(),
        )
        .await
    }

    pub async fn initiate_multipart_with_metadata(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        storage_class: Option<&str>,
        content_language: Option<&str>,
        user_meta: &BTreeMap<String, String>,
    ) -> Result<String> {
        self.initiate_multipart_as(bucket, key, content_type, content_encoding, storage_class, content_language, user_meta, false)
            .await
    }

    /// Starts a multipart upload: a client's (`replicated: false`) is
    /// refused up front on a read-only bucket, before any part is sent;
    /// replication's is accepted there (its completion carries the stamp,
    /// which is where the write's origin is decided).
    #[allow(clippy::too_many_arguments)]
    pub async fn initiate_multipart_as(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        storage_class: Option<&str>,
        content_language: Option<&str>,
        user_meta: &BTreeMap<String, String>,
        replicated: bool,
    ) -> Result<String> {
        if !replicated {
            self.ensure_writable(bucket).await?;
        }
        self.initiate_multipart_replicated(bucket, key, content_type, content_encoding, storage_class, content_language, user_meta)
            .await
    }

    /// [`Self::initiate_multipart_with_metadata`] for a replicated upload,
    /// which a read-only bucket accepts (its completion carries the stamp).
    #[allow(clippy::too_many_arguments)]
    pub async fn initiate_multipart_replicated(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        storage_class: Option<&str>,
        content_language: Option<&str>,
        user_meta: &BTreeMap<String, String>,
    ) -> Result<String> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let upload_id = new_staging_id(now_ms());
        let staging_dir = self.layout.multipart_staging_dir(bucket, &upload_id)?;
        // `staging/multipart` and `staging`: note which ancestors this call
        // creates, since their new directory entries need fsyncing too.
        let multipart_root = staging_dir.parent().unwrap_or(&staging_dir).to_path_buf();
        let staging_root = multipart_root.parent().unwrap_or(&multipart_root).to_path_buf();
        let multipart_root_existed = tokio::fs::metadata(&multipart_root).await.is_ok();
        let staging_root_existed = tokio::fs::metadata(&staging_root).await.is_ok();
        tokio::fs::create_dir_all(&staging_dir).await?;
        let cleanup = StagingDirGuard::new(&staging_dir);
        let upload = UploadMeta {
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            upload_id: upload_id.clone(),
            initiated_at_ms: now_ms(),
            content_type: content_type_or_default(content_type),
            content_encoding: content_encoding_or_none(content_encoding),
            content_language: content_language_or_none(content_language),
            storage_class: storage_class_or_default(storage_class),
            user_meta: user_meta.clone(),
        };
        let upload_json = staging_dir.join("upload.json");
        write_json_atomic(&upload_json, &upload).await?;
        if self.durability == Durability::Full {
            // The client is handed an UploadId it will upload parts against
            // for days; a crash must not make the upload vanish.
            fsync_file(&upload_json).await?;
            fsync_dir(&staging_dir).await?;
            fsync_dir(&multipart_root).await?;
            if !multipart_root_existed {
                fsync_dir(&staging_root).await?;
                if !staging_root_existed {
                    if let Some(bucket_dir) = staging_root.parent() {
                        fsync_dir(bucket_dir).await?;
                    }
                }
            }
        }
        cleanup.disarm();
        Ok(upload_id)
    }

    /// Atomically commit a fully-written, size-verified temp part into place.
    ///
    /// The part's bytes land under a fresh, unique data file name
    /// (`part.N.<id>`), and `part.N.meta.json` — which names that file — is
    /// the single atomic commit point. A crash anywhere leaves either the old
    /// meta pointing at the old (still present) bytes, or the new meta
    /// pointing at the new bytes: never new bytes under old metadata. The
    /// superseded data file is removed only after the new meta is in place.
    ///
    /// Holds the per-`(upload_id, part_number)` lock (which Complete also
    /// takes) so concurrent uploads of the *same* part number resolve
    /// last-writer-wins without interleaving, matching S3. The byte transfer
    /// runs BEFORE this call, unlocked, so uploads of different parts never
    /// contend and a big transfer never holds the lock.
    async fn commit_staged_part(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u16,
        temp_path: &Path,
        size: u64,
        etag: &str,
    ) -> Result<()> {
        let staging_dir = self.layout.multipart_staging_dir(bucket, upload_id)?;
        let file_name = format!("part.{part_number}.{}", new_staging_id(now_ms()));
        let part = PartMeta {
            number: part_number,
            file: file_name.clone(),
            size,
            etag: etag.to_string(),
            last_modified_ms: now_ms(),
        };
        let _guard = self
            .locks
            .lock(bucket, &part_lock_key(upload_id, part_number))
            .await;
        let part_meta_path = staging_dir.join(format!("part.{part_number}.meta.json"));
        let previous = read_json_if_exists::<PartMeta>(&part_meta_path)
            .await
            .ok()
            .flatten();
        let data_path = staging_dir.join(&file_name);
        tokio::fs::rename(temp_path, &data_path).await?;
        let staged = async {
            if self.durability == Durability::Full {
                // UploadPart returns an ETag the client treats as committed,
                // so the part must be on stable storage before we ack — and
                // the bytes (file + directory entry) before the meta that
                // names them, so a crash can never persist the meta alone.
                fsync_file(&data_path).await?;
                fsync_dir(&staging_dir).await?;
            }
            write_json_atomic(&part_meta_path, &part).await
        }
        .await;
        if let Err(err) = staged {
            // The meta was not swapped: the previous part (if any) stands.
            let _ = tokio::fs::remove_file(&data_path).await;
            return Err(err);
        }
        if self.durability == Durability::Full {
            fsync_file(&part_meta_path).await?;
            fsync_dir(&staging_dir).await?;
        }
        if let Some(previous) = previous {
            if previous.file != file_name && is_plain_file_name(&previous.file) {
                let _ = tokio::fs::remove_file(staging_dir.join(&previous.file)).await;
            }
        }
        Ok(())
    }

    pub async fn put_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u16,
        bytes: &[u8],
        aws_chunked: bool,
    ) -> Result<PutResult> {
        self.validate_upload(bucket, key, upload_id).await?;
        if part_number == 0 || part_number > 10_000 {
            return Err(StorageError::InvalidMultipartUpload(format!(
                "invalid part number {part_number}"
            )));
        }
        let decoded;
        let payload = if aws_chunked {
            decoded = decode_aws_chunked(bytes)?;
            decoded.as_slice()
        } else {
            bytes
        };
        let staging_dir = self.layout.multipart_staging_dir(bucket, upload_id)?;
        // Stream to a per-attempt temp, then atomically swap into place.
        let temp_path = part_temp_path(&staging_dir, part_number);
        let etag = match write_file_with_md5(&temp_path, &payload).await {
            Ok(etag) => etag,
            Err(err) => {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(err);
            }
        };
        let size = payload.len() as u64;
        if let Err(err) = self
            .commit_staged_part(bucket, upload_id, part_number, &temp_path, size, &etag)
            .await
        {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(err);
        }
        Ok(PutResult {
            etag,
            size,
            last_modified_ms: now_ms(),
            stamp: None,
            version_id: None,
        })
    }

    pub async fn put_multipart_part_stream<S, E>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u16,
        stream: S,
        aws_chunked: bool,
        expected_sha256: Option<&str>,
        expected_decoded_len: Option<u64>,
    ) -> Result<PutResult>
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        self.validate_upload(bucket, key, upload_id).await?;
        if part_number == 0 || part_number > 10_000 {
            return Err(StorageError::InvalidMultipartUpload(format!(
                "invalid part number {part_number}"
            )));
        }
        let staging_dir = self.layout.multipart_staging_dir(bucket, upload_id)?;
        // Stream to a per-attempt temp (unlocked); swap in only after the part
        // is verified complete, so a truncated/racing upload never becomes a
        // committed part.
        let temp_path = part_temp_path(&staging_dir, part_number);
        let written = match if aws_chunked {
            write_aws_chunked_stream_with_hashes(&temp_path, stream).await
        } else {
            write_stream_with_hashes(&temp_path, stream).await
        } {
            Ok(written) => written,
            Err(err) => {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(err);
            }
        };
        if let Some(expected) = expected_sha256 {
            if !expected.eq_ignore_ascii_case(&written.sha256) {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(StorageError::PayloadHashMismatch {
                    expected: expected.to_string(),
                    actual: written.sha256,
                });
            }
        }
        // Completeness gate: an aws-chunked part must decode to the declared
        // length, else it is a truncated body (S3's `IncompleteBody`). The
        // non-chunked path is covered by the body stream erroring on an
        // under-length transfer.
        if let Some(expected) = expected_decoded_len {
            if written.size != expected {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(StorageError::InvalidAwsChunkedBody(format!(
                    "decoded part length {} does not match declared x-amz-decoded-content-length {}",
                    written.size, expected
                )));
            }
        }
        if let Err(err) = self
            .commit_staged_part(
                bucket,
                upload_id,
                part_number,
                &temp_path,
                written.size,
                &written.md5,
            )
            .await
        {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(err);
        }
        Ok(PutResult {
            etag: written.md5,
            size: written.size,
            last_modified_ms: now_ms(),
            stamp: None,
            version_id: None,
        })
    }

    pub async fn copy_multipart_part(
        &self,
        dst_bucket: &str,
        dst_key: &str,
        upload_id: &str,
        part_number: u16,
        src_bucket: &str,
        src_key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<PutResult> {
        self.validate_upload(dst_bucket, dst_key, upload_id).await?;
        if part_number == 0 || part_number > 10_000 {
            return Err(StorageError::InvalidMultipartUpload(format!(
                "invalid part number {part_number}"
            )));
        }
        let staging_dir = self.layout.multipart_staging_dir(dst_bucket, upload_id)?;
        let temp_path = part_temp_path(&staging_dir, part_number);
        // Hold the source lock only while reading/copying the source bytes into
        // the temp; drop it before the (separately-locked) commit swap.
        let written = {
            let _source_guard = self.locks.lock(src_bucket, src_key).await;
            let src = self.read_object(src_bucket, src_key).await?;
            match copy_object_data_with_hashes(&src, &temp_path, range).await {
                Ok(written) => written,
                Err(err) => {
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    return Err(err);
                }
            }
        };
        if let Err(err) = self
            .commit_staged_part(
                dst_bucket,
                upload_id,
                part_number,
                &temp_path,
                written.size,
                &written.md5,
            )
            .await
        {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(err);
        }
        Ok(PutResult {
            etag: written.md5,
            size: written.size,
            last_modified_ms: now_ms(),
            stamp: None,
            version_id: None,
        })
    }

    pub async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        requested_parts: &[CompletePartRequest],
    ) -> Result<PutResult> {
        self.complete_multipart_with(bucket, key, upload_id, requested_parts, WriteOptions::default())
            .await
    }

    /// [`Self::complete_multipart`] with [`WriteOptions`] — a replicated
    /// upload takes its stamp here, at completion, not at create.
    pub async fn complete_multipart_with(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        requested_parts: &[CompletePartRequest],
        options: WriteOptions,
    ) -> Result<PutResult> {
        // A replicated upload older than what is here is refused before
        // anything is assembled, leaving the upload intact (to abort). The
        // publish re-checks under the key lock.
        if let Some(incoming) = options.origin.put_rank().filter(|_| options.origin.adjudicated()) {
            // (A versioning-enabled bucket keeps an older state as history.)
            if self.bucket_versioning(bucket).await? != BucketVersioning::Enabled {
                if let Some(current) = self.key_rank(bucket, key).await? {
                    if !outranks(&incoming, Some(&current)) {
                        return Err(Self::stale(bucket, key, &current));
                    }
                }
            }
        }
        self.validate_upload(bucket, key, upload_id).await?;
        if requested_parts.is_empty() {
            return Err(StorageError::InvalidMultipartUpload(
                "complete request has no parts".to_string(),
            ));
        }
        let mut previous = 0;
        for requested in requested_parts {
            if requested.number <= previous {
                return Err(StorageError::InvalidMultipartUpload(
                    "parts must be in ascending order".to_string(),
                ));
            }
            previous = requested.number;
        }

        // Lock order (everywhere): upload lock → part locks ascending → key
        // lock (taken inside publish). The upload lock serialises Complete
        // against another Complete, Abort and the staging sweeper; the part
        // locks exclude a concurrent UploadPart of a requested part from the
        // first meta read until the part bytes have moved into the publish
        // dir, so the meta we publish always describes the bytes we publish.
        let _upload_guard = self.locks.lock(bucket, &upload_lock_key(upload_id)).await;
        let mut _part_guards = Vec::with_capacity(requested_parts.len());
        for requested in requested_parts {
            _part_guards.push(
                self.locks
                    .lock(bucket, &part_lock_key(upload_id, requested.number))
                    .await,
            );
        }
        // Re-validate under the lock: an Abort/sweep may have won the race.
        let upload = self.validate_upload(bucket, key, upload_id).await?;
        let staging_dir = self.layout.multipart_staging_dir(bucket, upload_id)?;

        // Start clean: a previous attempt that failed or crashed may have
        // moved parts into `object/` already. Put them back (or drop them if
        // the part has since been re-uploaded) so this attempt publishes
        // exactly the currently committed parts.
        reset_multipart_publish_dir(&staging_dir).await?;

        let mut parts = Vec::with_capacity(requested_parts.len());
        for requested in requested_parts {
            let part_meta: PartMeta =
                read_json(&staging_dir.join(format!("part.{}.meta.json", requested.number)))
                    .await?;
            if part_meta.etag != unquote_etag(&requested.etag) {
                return Err(StorageError::InvalidMultipartUpload(format!(
                    "etag mismatch for part {}",
                    requested.number
                )));
            }
            if !is_plain_file_name(&part_meta.file) {
                return Err(StorageError::InvalidMultipartUpload(format!(
                    "part {} has an invalid data file name",
                    requested.number
                )));
            }
            let data = staging_dir.join(&part_meta.file);
            ensure_file_exists(&data).await?;
            // Cheap guard against bytes that do not belong to this meta
            // (e.g. a legacy-format part torn by a crash mid-upload).
            let len = tokio::fs::metadata(&data).await?.len();
            if len != part_meta.size {
                return Err(StorageError::InvalidMultipartUpload(format!(
                    "part {} data ({len} bytes) does not match its recorded size ({}); re-upload the part",
                    requested.number, part_meta.size
                )));
            }
            parts.push(part_meta);
        }
        for part in parts.iter().take(parts.len().saturating_sub(1)) {
            if part.size < MIN_MULTIPART_PART_SIZE {
                return Err(StorageError::EntityTooSmall(format!(
                    "part {} is smaller than 5 MiB",
                    part.number
                )));
            }
        }

        let size = parts.iter().map(|p| p.size).sum();
        let etag = multipart_etag(&parts)?;
        // Published parts use the canonical `part.N` names.
        let object_parts: Vec<PartMeta> = parts
            .iter()
            .map(|p| PartMeta {
                file: format!("part.{}", p.number),
                ..p.clone()
            })
            .collect();
        let object_meta = ObjectMeta {
            format_version: META_FORMAT,
            stamp: None,
            versioned: false,
            delete_marker: false,
            removed: None,
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            storage: ObjectStorageKind::Multipart,
            size,
            etag: etag.clone(),
            last_modified_ms: now_ms(),
            content_type: upload.content_type.clone(),
            content_encoding: upload.content_encoding.clone(),
            content_language: upload.content_language.clone(),
            storage_class: upload.storage_class.clone(),
            user_meta: upload.user_meta.clone(),
            parts: object_parts,
        };
        let publish_dir = prepare_multipart_publish_dir(&staging_dir, &parts, &object_meta).await?;
        self.crash_point("complete_after_prepare");

        // Multipart completion is unconditional (object_store never sends a
        // precondition with it).
        let result = self
            .publish_prepared_dir(bucket, key, &publish_dir, object_meta, options)
            .await?;
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
        Ok(result)
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<PutResult> {
        self.copy_object_with_metadata(
            src_bucket,
            src_key,
            dst_bucket,
            dst_key,
            None,
            None,
            None,
            None,
            WriteOptions::default(),
        )
        .await
    }

    /// A metadata-only update of `key` (CopyObject onto itself): a new blob
    /// dir whose part files are **hardlinks** to the current ones, with a new
    /// `meta.json`, published through the normal commit (new version, new
    /// stamp, a changelog `put`). No byte is copied, and the part layout and
    /// ETag are kept — a multipart object keeps its `-N` ETag. The old dir is
    /// retired as usual; its files live on through the links.
    ///
    /// `Ok(None)` when the files cannot be hardlinked (e.g. a filesystem
    /// without hardlinks): the caller falls back to copying the bytes.
    /// Refuses (412) if the object changed after it was read, rather than
    /// republishing the older bytes over a concurrent write.
    #[allow(clippy::too_many_arguments)]
    async fn republish_metadata_only(
        &self,
        bucket: &str,
        key: &str,
        storage_class: Option<&str>,
        replacement_user_meta: Option<&BTreeMap<String, String>>,
        replacement_content_type: Option<&str>,
        replacement_content_language: Option<&str>,
        options: WriteOptions,
    ) -> Result<Option<PutResult>> {
        // A concurrent overwrite between reading and publishing makes the
        // guard below refuse; re-read and try again (the caller asked for no
        // precondition, so a race must not surface as a 412).
        let guarded = options.precondition.is_none();
        let mut attempt = 0;
        loop {
            attempt += 1;
            let staging_id = new_staging_id(now_ms());
            let staging_dir = self.layout.put_staging_dir(bucket, &staging_id)?;
            let publish_dir = staging_dir.join("object");
            let meta = {
                // Link under the key lock: the source files cannot be retired
                // mid-link, and once linked they outlive any later retire.
                let _guard = self.locks.lock(bucket, key).await;
                let src = self.read_object(bucket, key).await?;
                tokio::fs::create_dir_all(&publish_dir).await?;
                for part in &src.meta.parts {
                    if let Err(err) =
                        tokio::fs::hard_link(src.object_dir.join(&part.file), publish_dir.join(&part.file)).await
                    {
                        log::info!(
                            "metadata-only copy falls back to a byte copy bucket={bucket} key={key}: cannot hardlink ({err})"
                        );
                        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
                        return Ok(None);
                    }
                }
                let mut meta = src.meta.clone();
                if let Some(content_type) = replacement_content_type {
                    meta.content_type = content_type.to_string();
                }
                if let Some(language) = replacement_content_language {
                    meta.content_language = Some(language.to_string());
                }
                if let Some(class) = storage_class {
                    meta.storage_class = class.to_string();
                }
                if let Some(user_meta) = replacement_user_meta {
                    meta.user_meta = user_meta.clone();
                }
                meta
            };
            let cleanup = StagingDirGuard::new(&staging_dir);
            write_json_atomic(&publish_dir.join("meta.json"), &meta).await?;
            let mut attempt_options = options.clone();
            if guarded {
                // Only republish over the state that was read.
                attempt_options.precondition = Some(Precondition::IfMatch(meta.etag.clone()));
            }
            let result = self
                .publish_prepared_dir(bucket, key, &publish_dir, meta, attempt_options)
                .await;
            let _ = tokio::fs::remove_dir_all(&staging_dir).await;
            cleanup.disarm();
            match result {
                Err(StorageError::PreconditionFailed { .. }) if guarded && attempt < 5 => continue,
                other => return other.map(Some),
            }
        }
    }

    pub async fn copy_object_with_metadata(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        storage_class: Option<&str>,
        replacement_user_meta: Option<&BTreeMap<String, String>>,
        replacement_content_type: Option<&str>,
        replacement_content_language: Option<&str>,
        options: WriteOptions,
    ) -> Result<PutResult> {
        self.ensure_bucket_and_key(dst_bucket, dst_key).await?;
        if src_bucket == dst_bucket && src_key == dst_key {
            // Copy onto itself = a metadata update: keep the bytes, the part
            // layout and the ETag (hardlinks, constant cost).
            if let Some(result) = self
                .republish_metadata_only(
                    dst_bucket,
                    dst_key,
                    storage_class,
                    replacement_user_meta,
                    replacement_content_type,
                    replacement_content_language,
                    options.clone(),
                )
                .await?
            {
                return Ok(result);
            }
        }
        let _source_guard = self.locks.lock(src_bucket, src_key).await;
        let src = self.read_object(src_bucket, src_key).await?;
        let content_type = replacement_content_type
            .map(str::to_string)
            .unwrap_or_else(|| src.meta.content_type.clone());
        let content_encoding = src.meta.content_encoding.clone();
        let content_language = replacement_content_language
            .map(str::to_string)
            .or_else(|| src.meta.content_language.clone());
        let copied_storage_class = storage_class
            .map(str::to_string)
            .unwrap_or_else(|| src.meta.storage_class.clone());
        let user_meta = replacement_user_meta
            .cloned()
            .unwrap_or_else(|| src.meta.user_meta.clone());
        let staging_id = new_staging_id(now_ms());
        let staging_dir = self.layout.put_staging_dir(dst_bucket, &staging_id)?;
        tokio::fs::create_dir_all(&staging_dir).await?;
        let cleanup = StagingDirGuard::new(&staging_dir);
        let written = copy_object_data_with_hashes(&src, &staging_dir.join("part.1"), None).await?;
        let meta = PutMeta {
            bucket: dst_bucket.to_string(),
            object_key: dst_key.to_string(),
            write_id: staging_id.clone(),
            initiated_at_ms: now_ms(),
            size: written.size,
            etag: written.md5,
            content_type,
            content_encoding,
            content_language,
            storage_class: copied_storage_class,
            user_meta,
        };
        write_json_atomic(&staging_dir.join("put.json"), &meta).await?;
        drop(_source_guard);
        // commit_staged_put owns the staging dir from here (and removes it).
        cleanup.disarm();
        self.commit_staged_put_with(dst_bucket, dst_key, &staging_id, options)
            .await
    }

    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<PartMeta>> {
        self.validate_upload(bucket, key, upload_id).await?;
        let staging_dir = self.layout.multipart_staging_dir(bucket, upload_id)?;
        let mut parts = Vec::new();
        let mut entries = tokio::fs::read_dir(&staging_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let fname = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            if fname.starts_with("part.") && fname.ends_with(".meta.json") {
                if let Ok(part) = read_json::<PartMeta>(&path).await {
                    parts.push(part);
                }
            }
        }
        parts.sort_by_key(|p| p.number);
        Ok(parts)
    }

    pub async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<UploadMeta>> {
        validate_bucket_name(bucket)?;
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        let staging_dir = self
            .layout
            .bucket_dir(bucket)?
            .join("staging")
            .join("multipart");
        let Ok(mut entries) = tokio::fs::read_dir(&staging_dir).await else {
            return Ok(Vec::new());
        };
        let mut uploads = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let upload_json = entry.path().join("upload.json");
            if let Ok(upload) = read_json::<UploadMeta>(&upload_json).await {
                uploads.push(upload);
            }
        }
        uploads.sort_by_key(|u| u.initiated_at_ms);
        Ok(uploads)
    }

    /// How many in-flight multipart uploads `bucket` holds. Same set as
    /// [`list_multipart_uploads`](Self::list_multipart_uploads), but it only
    /// stats each `upload.json` instead of reading and parsing it — the console
    /// asks for this per bucket on every rail refresh, so it stays metadata-only.
    /// An unreadable bucket counts as zero rather than erroring: this feeds a
    /// badge, and a missing staging dir simply means nothing is in flight.
    pub async fn count_multipart_uploads(&self, bucket: &str) -> usize {
        let Ok(bucket_dir) = self.layout.bucket_dir(bucket) else {
            return 0;
        };
        let staging_dir = bucket_dir.join("staging").join("multipart");
        let Ok(mut entries) = tokio::fs::read_dir(&staging_dir).await else {
            return 0;
        };
        let mut count = 0;
        while let Ok(Some(entry)) = entries.next_entry().await {
            if tokio::fs::metadata(entry.path().join("upload.json")).await.is_ok() {
                count += 1;
            }
        }
        count
    }

    pub async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
        self.validate_upload(bucket, key, upload_id).await?;
        // Same lock order as Complete: upload lock first, then the key lock.
        let _upload_guard = self.locks.lock(bucket, &upload_lock_key(upload_id)).await;
        let _guard = self.locks.lock(bucket, key).await;
        let staging_dir = self.layout.multipart_staging_dir(bucket, upload_id)?;
        match tokio::fs::remove_dir_all(&staging_dir).await {
            Ok(()) => Ok(()),
            // Only a missing upload is NoSuchUpload; a real IO failure (e.g.
            // permissions) must surface as an error, not be masked as "gone".
            Err(err) if err.kind() == ErrorKind::NotFound => {
                Err(StorageError::NoSuchUpload(upload_id.to_string()))
            }
            Err(err) => Err(err.into()),
        }
    }

    // ── rebuild (migration + disaster recovery) ───────────────────────────────

    /// Raises the data-plane 503 gate for `bucket` if it isn't already up.
    /// Returns `false` if a rebuild is already in progress. The caller owns the
    /// rebuild and must call [`end_rebuild`](Self::end_rebuild) when done — this
    /// lets the *server* layer run a rebuild as a registry-tracked job while the
    /// storage layer keeps owning the gate. `start_rebuild_background` uses the
    /// same gate, so triggers from either layer dedupe against each other.
    pub fn try_begin_rebuild(&self, bucket: &str) -> bool {
        let mut guard = self.rebuilding.lock().unwrap();
        if guard.contains(bucket) {
            return false;
        }
        guard.insert(bucket.to_string());
        true
    }

    /// Lowers the data-plane 503 gate for `bucket` (rebuild finished).
    pub fn end_rebuild(&self, bucket: &str) {
        self.rebuilding.lock().unwrap().remove(bucket);
    }

    /// One empty-directory reclamation pass: `remove_dir` each fanout dir at the
    /// `objects/` root. The single-level layout means every fanout dir is a
    /// direct child of `objects/`, and object (leaf) dirs are always valid — so
    /// there is nothing to walk or inspect. `remove_dir` (never `remove_dir_all`)
    /// removes a fanout dir **iff it is empty**; one that still holds objects
    /// fails with `DirectoryNotEmpty` and is skipped. **Every error is ignored**
    /// (best-effort): `DirectoryNotEmpty` = in use, `NotFound` = already gone, and
    /// a concurrent publish that lost a `create_dir` race retries on `ENOENT`, so
    /// no object data can ever be touched and no write ever fails.
    ///
    /// Legacy 4-level fanout chains are cleaned by the migration job as it
    /// vacates them (`migrate_object_layout` removes the chain bottom-up), so
    /// this pass never needs to descend. `reclaimed` is incremented live (for
    /// status display); the pass yields after each removal to stay low priority.
    pub async fn reclaim_empty_dirs_pass(
        &self,
        bucket: &str,
        cancel: &CancellationToken,
        reclaimed: &Arc<std::sync::atomic::AtomicUsize>,
    ) -> Result<usize> {
        use std::sync::atomic::Ordering;
        validate_bucket_name(bucket)?;
        let objects_dir = self.layout.bucket_dir(bucket)?.join("objects");
        let mut entries = match tokio::fs::read_dir(&objects_dir).await {
            Ok(entries) => entries,
            Err(_) => return Ok(0), // no objects/ dir yet
        };
        let mut removed = 0usize;
        while let Some(entry) = entries.next_entry().await? {
            if cancel.is_cancelled() || self.shutdown.is_cancelled() {
                break;
            }
            if tokio::fs::remove_dir(entry.path()).await.is_ok() {
                reclaimed.fetch_add(1, Ordering::Relaxed);
                removed += 1;
                tokio::task::yield_now().await;
            }
        }
        Ok(removed)
    }

    /// Relocates one object's blob dir to the current layout (its key's dir,
    /// named by its stamp, `meta.json` at the current format with the stamp
    /// materialised) via **hardlink → atomic index flip → unlink** — no
    /// object bytes are copied. An object from before stamps gets the
    /// fallback stamp (its Last-Modified, this site).
    ///
    /// Idempotent: `Ok(false)` if the object is gone or already current.
    /// Safe under concurrent reads (both paths hold the same inodes across
    /// the atomic flip, and `read_object` retries on a vanished path) and
    /// crashes (the tree is self-describing, so a rebuild dedups any orphan
    /// left behind). Serialised with publish/delete of the same key by the
    /// per-key lock.
    pub async fn relocate_object(&self, bucket: &str, key: &str) -> Result<bool> {
        validate_bucket_name(bucket)?;
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        let index = self.index_unchecked(bucket).await?;
        let _guard = self.locks.lock(bucket, key).await;

        let Some(row) = index.get(key).await? else {
            return Ok(false); // gone
        };
        if layouts::is_current(&row.blob_dir) && row.stamp.is_some() {
            return Ok(false); // already on the current layout
        }
        let old_dir = bucket_dir.join(&row.blob_dir);
        let mut meta: ObjectMeta = read_json(&old_dir.join("meta.json")).await?;
        let stamp = self.effective_stamp(row.stamp.as_ref().or(meta.stamp.as_ref()), row.last_modified_ms);
        let new_rel = v3::blob_rel(bucket, key, &stamp);
        let new_dir = bucket_dir.join(&new_rel);
        if new_rel == row.blob_dir {
            // Already at its place; only the stamp was missing from the row.
            return index.relocate_row(key, &row.blob_dir, &new_rel, &stamp).await;
        }

        // 1. A fresh dir at the version's path (a leftover of an interrupted
        //    attempt is replaced); hardlink every part into it and write the
        //    current-format meta.json.
        let _ = tokio::fs::remove_dir_all(&new_dir).await;
        tokio::fs::create_dir_all(&new_dir).await?;
        let linked_and_flipped: Result<bool> = async {
            let mut entries = tokio::fs::read_dir(&old_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                // Object leaves hold files only (meta.json + part.N), no subdirs.
                if entry.file_name() == "meta.json" {
                    continue;
                }
                tokio::fs::hard_link(old_dir.join(entry.file_name()), new_dir.join(entry.file_name()))
                    .await?;
            }
            meta.format_version = META_FORMAT;
            meta.stamp = Some(stamp.clone());
            write_json_atomic(&new_dir.join("meta.json"), &meta).await?;
            if self.durability == Durability::Full {
                fsync_file(&new_dir.join("meta.json")).await?;
                fsync_dir(&new_dir).await?;
                if let Some(parent) = new_dir.parent() {
                    fsync_dir(parent).await?;
                }
            }

            self.crash_point("migrate_after_link");
            // 2. Atomic pivot: repoint the index row, guarded by the old path so a
            //    concurrent overwrite (should the lock ever not hold) can't be lost.
            index.relocate_row(key, &row.blob_dir, &new_rel, &stamp).await
        }
        .await;
        match linked_and_flipped {
            Ok(true) => {}
            other => {
                // Not flipped (lost the race, or failed): abandon the orphan
                // copy so a key that keeps failing can't leak a dir per attempt.
                let _ = tokio::fs::remove_dir_all(&new_dir).await;
                return other;
            }
        }
        self.meta_cache.remove(&ObjectCacheKey::new(bucket, key));

        // 3. Clean up the old leaf. Its files are hardlinks, so unlinking just
        //    drops the extra link — the inodes live on under new_dir.
        let _ = tokio::fs::remove_dir_all(&old_dir).await;
        // Reclaim the now-empty old fanout chain bottom-up. Race-free: new
        // writes only ever create current-layout dirs, so nothing recreates
        // these old paths; a sibling object still under the chain just makes
        // `remove_dir` fail (`DirectoryNotEmpty`) and we stop.
        let objects_root = bucket_dir.join("objects");
        let mut parent = old_dir.parent().map(|p| p.to_path_buf());
        while let Some(dir) = parent {
            if dir == objects_root || tokio::fs::remove_dir(&dir).await.is_err() {
                break;
            }
            parent = dir.parent().map(|p| p.to_path_buf());
        }
        Ok(true)
    }

    /// Writes the marker dir of a tombstone that has none (from before
    /// markers were written) and records it. `Ok(false)` if the tombstone
    /// is gone or already has one.
    pub async fn materialize_marker(&self, bucket: &str, key: &str) -> Result<bool> {
        validate_bucket_name(bucket)?;
        let index = self.index_unchecked(bucket).await?;
        let _guard = self.locks.lock(bucket, key).await;
        let Some(t) = index.get_tombstone(key).await? else {
            return Ok(false);
        };
        if t.blob_dir.is_some() {
            return Ok(false);
        }
        let removed = t.removed.clone().unwrap_or_else(|| t.stamp.clone());
        let rel = self.write_delete_marker(bucket, key, &t.stamp, &removed, t.versioned).await?;
        index.set_tombstone_marker(key, &t.stamp, &rel).await
    }

    /// Up to `limit` keys in `bucket` not yet on the current layout (or
    /// without a stamp), strictly after `after`.
    pub async fn keys_off_layout(
        &self,
        bucket: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        self.index_unchecked(bucket).await?.rows_off_layout(after, limit).await
    }

    // ── bucket layout versions ─────────────────────────────────────────────

    /// The bucket's layout as recorded in its `bucket.json`, or, for a
    /// bucket from before layouts were recorded, as detected from its tree
    /// ([`Self::detect_layout`]).
    pub async fn bucket_layout(&self, bucket: &str) -> Result<BucketLayout> {
        if let Some(layout) = self.bucket_meta(bucket).await?.layout {
            return Ok(layout);
        }
        Ok(BucketLayout::Stable { version: self.detect_layout(bucket).await? })
    }

    /// The oldest layout the bucket's tree holds, by shape: a 2-char dir at
    /// the `objects/` root is layout 1; a 4-char fanout dir holding a leaf
    /// that is not a key dir is layout 2; else the current layout. A bucket
    /// from before layouts were recorded can hold a mix (new writes always
    /// went to the newest layout), and the oldest is what an upgrade must
    /// still handle.
    pub async fn detect_layout(&self, bucket: &str) -> Result<u32> {
        let objects_dir = self.layout.bucket_dir(bucket)?.join("objects");
        let mut fanouts = match tokio::fs::read_dir(&objects_dir).await {
            Ok(entries) => entries,
            Err(_) => return Ok(CURRENT_LAYOUT),
        };
        let mut oldest = CURRENT_LAYOUT;
        while let Some(entry) = fanouts.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.len() == 2 {
                return Ok(1);
            }
            if name.len() != 4 || oldest == 2 {
                continue;
            }
            let mut leaves = match tokio::fs::read_dir(entry.path()).await {
                Ok(leaves) => leaves,
                Err(_) => continue,
            };
            while let Some(leaf) = leaves.next_entry().await? {
                let leaf = leaf.file_name();
                if !layouts::is_hex(&leaf.to_string_lossy(), 40) {
                    oldest = 2;
                    break;
                }
            }
        }
        Ok(oldest)
    }

    /// Records the bucket's layout (durably).
    pub async fn set_bucket_layout(&self, bucket: &str, layout: BucketLayout) -> Result<()> {
        validate_bucket_name(bucket)?;
        let _guard = self.locks.lock(bucket, "\0bucket-meta").await;
        let path = self.layout.bucket_meta_path(bucket)?;
        if !path.exists() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        let mut meta: BucketMeta = read_json(&path).await?;
        meta.layout = Some(layout);
        write_json_atomic(&path, &meta).await?;
        fsync_file(&path).await?;
        fsync_dir(path.parent().unwrap_or(self.layout.root())).await?;
        self.bucket_meta_cache.insert(bucket.to_string(), meta);
        Ok(())
    }

    /// Drops the cached `bucket.json` (tests edit the file directly).
    #[cfg(test)]
    pub(crate) fn forget_bucket_meta(&self, bucket: &str) {
        self.bucket_meta_cache.remove(&bucket.to_string());
    }

    /// Drops the cached index handle (tests delete the index directory to
    /// simulate its loss).
    #[cfg(test)]
    pub(crate) fn forget_index(&self, bucket: &str) {
        self.index_cache.lock().unwrap().remove(bucket);
    }

    /// Whether `layout` is an older layout this build still serves, so an
    /// upgrade can be started ([`Self::start_layout_upgrade`]).
    pub fn layout_upgrade_available(layout: BucketLayout) -> bool {
        matches!(layout, BucketLayout::Stable { version } if version < CURRENT_LAYOUT)
    }

    /// The startup layout check, run before the server serves. For every
    /// bucket: records a layout that was never recorded; refuses a layout,
    /// or a migration target, newer than this build knows (the server must
    /// not start); leaves a bucket stable on an older layout serving (this
    /// build reads it; an operator upgrades it explicitly). A bucket found
    /// `migrating` — an upgrade was started and did not finish — is
    /// returned **with its 503 gate raised**: it serves nothing until
    /// [`Self::migrate_bucket_layout`] finishes and the caller lowers the
    /// gate ([`Self::end_rebuild`]).
    pub async fn check_layouts(&self) -> Result<Vec<(String, BucketLayout)>> {
        let mut resume = Vec::new();
        for (bucket, _) in self.list_buckets().await? {
            let recorded = self.bucket_meta(&bucket).await?.layout;
            let layout = match recorded {
                Some(layout) => layout,
                None => self.bucket_layout(&bucket).await?,
            };
            let unsupported = |message: String| StorageError::UnsupportedLayout { bucket: bucket.clone(), message };
            match layout {
                BucketLayout::Stable { version } if version > CURRENT_LAYOUT => {
                    return Err(unsupported(format!(
                        "layout {version} was written by a newer build; this build knows up to layout {CURRENT_LAYOUT}"
                    )))
                }
                BucketLayout::Migrating { to, .. } if to > CURRENT_LAYOUT => {
                    return Err(unsupported(format!(
                        "a migration to layout {to} was started by a newer build; this build knows up to layout {CURRENT_LAYOUT}"
                    )))
                }
                BucketLayout::Stable { .. } => {
                    if recorded.is_none() {
                        self.set_bucket_layout(&bucket, layout).await?;
                    }
                }
                BucketLayout::Migrating { from, to } => {
                    // Continue the interrupted step; an older target becomes
                    // the current layout.
                    let next = BucketLayout::Migrating { from, to: to.max(CURRENT_LAYOUT) };
                    if recorded != Some(next) {
                        self.set_bucket_layout(&bucket, next).await?;
                    }
                    self.try_begin_rebuild(&bucket);
                    resume.push((bucket, next));
                }
            }
        }
        Ok(resume)
    }

    /// Starts the upgrade of a bucket on an older layout: marks it
    /// `migrating` and raises its 503 gate, which the caller lowers once
    /// [`Self::migrate_bucket_layout`] has made it stable. Refused when the
    /// bucket is already current (`BucketInUse` with the reason) or an
    /// upgrade is in progress.
    pub async fn start_layout_upgrade(&self, bucket: &str) -> Result<BucketLayout> {
        let layout = self.bucket_layout(bucket).await?;
        let next = match layout {
            BucketLayout::Stable { version } if version < CURRENT_LAYOUT => {
                BucketLayout::Migrating { from: version, to: CURRENT_LAYOUT }
            }
            BucketLayout::Stable { .. } => {
                return Err(StorageError::BucketInUse(format!(
                    "bucket {bucket} is already on layout {CURRENT_LAYOUT}"
                )))
            }
            BucketLayout::Migrating { from, to } => {
                return Err(StorageError::BucketInUse(format!(
                    "bucket {bucket} is already being upgraded from layout {from} to {to}"
                )))
            }
        };
        if !self.try_begin_rebuild(bucket) {
            return Err(StorageError::BucketRebuilding(bucket.to_string()));
        }
        if let Err(err) = self.set_bucket_layout(bucket, next).await {
            self.end_rebuild(bucket);
            return Err(err);
        }
        Ok(next)
    }

    /// Runs the bucket's recorded migration step to completion, behind the
    /// gate the caller holds, and marks the bucket stable on the target
    /// layout. Resumable: every object is either on the old or the new
    /// layout, and a key already moved is skipped. Fails (leaving the
    /// bucket `migrating`, to be retried) if any object could not be
    /// moved. Returns the number of objects moved.
    ///
    /// The step to layout 3 is the same from layouts 1 and 2: relocate every
    /// object to its key's dir under its (materialised) stamp, rewrite its
    /// `meta.json` to the current format, and write a marker dir for every
    /// tombstone that has none.
    pub async fn migrate_bucket_layout(&self, bucket: &str, cancel: &CancellationToken) -> Result<u64> {
        let layout = self.bucket_layout(bucket).await?;
        let BucketLayout::Migrating { from, to } = layout else {
            return Ok(0);
        };
        let unsupported = |message: String| StorageError::UnsupportedLayout { bucket: bucket.to_string(), message };
        if to != CURRENT_LAYOUT {
            return Err(unsupported(format!("cannot migrate to layout {to}; this build writes layout {CURRENT_LAYOUT}")));
        }
        let moved = match from {
            1 | 2 => self.relocate_to_current_layout(bucket, cancel).await?,
            other => return Err(unsupported(format!("no migration step from layout {other} to {to}"))),
        };
        if cancel.is_cancelled() {
            return Err(StorageError::Io(format!("layout migration of bucket {bucket} cancelled")));
        }
        // Done only when nothing is left on the old layout, in the index
        // and in the tree.
        let left = self.keys_off_layout(bucket, None, 1).await?;
        if !left.is_empty() {
            return Err(StorageError::Io(format!(
                "layout migration of bucket {bucket} incomplete: {} (and possibly more) could not be moved",
                left[0]
            )));
        }
        let left = self.index_unchecked(bucket).await?.tombstones_without_marker(None, 1).await?;
        if let Some(t) = left.first() {
            return Err(StorageError::Io(format!(
                "layout migration of bucket {bucket} incomplete: the delete marker of {} could not be written",
                t.object_key
            )));
        }
        // Empty dirs of the old shape (chains a delete emptied long ago) go;
        // a dir that still holds something is an orphan the index does not
        // name, left for a rebuild to trash.
        prune_empty_dirs(&self.layout.bucket_dir(bucket)?.join("objects"), 0).await;
        if self.detect_layout(bucket).await? != CURRENT_LAYOUT {
            log::warn!("bucket {bucket}: old-layout dirs remain after migration (orphans; a rebuild trashes them)");
        }
        self.set_bucket_layout(bucket, BucketLayout::Stable { version: to }).await?;
        Ok(moved)
    }

    /// Relocates every object not on the current layout (hardlink → index
    /// flip → unlink per object) and writes every missing delete marker.
    /// Keys that fail are skipped for this pass and reported by the
    /// caller's final check.
    async fn relocate_to_current_layout(&self, bucket: &str, cancel: &CancellationToken) -> Result<u64> {
        const BATCH: usize = 500;
        /// Objects relocated at once: each move is a few fsyncs, which
        /// overlap across keys (different keys, different locks).
        const CONCURRENCY: usize = 8;
        let moved = std::sync::atomic::AtomicU64::new(0);
        let mut after: Option<String> = None;
        loop {
            if cancel.is_cancelled() {
                return Ok(moved.into_inner());
            }
            let keys = self.keys_off_layout(bucket, after.as_deref(), BATCH).await?;
            let Some(last) = keys.last().cloned() else {
                break;
            };
            after = Some(last);
            futures::stream::iter(keys)
                .for_each_concurrent(CONCURRENCY, |key| {
                    let moved = &moved;
                    async move {
                        if cancel.is_cancelled() {
                            return;
                        }
                        match self.relocate_object(bucket, &key).await {
                            Ok(true) => {
                                moved.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            Ok(false) => {}
                            Err(err) => log::warn!("layout migration bucket={bucket} key={key} error={err}"),
                        }
                        self.crash_point("migrate_after_object");
                    }
                })
                .await;
            if cancel.is_cancelled() {
                return Ok(moved.into_inner());
            }
        }
        let mut moved = moved.into_inner();
        let mut after: Option<String> = None;
        loop {
            if cancel.is_cancelled() {
                return Ok(moved);
            }
            let tombstones = self.index_unchecked(bucket).await?.tombstones_without_marker(after.as_deref(), BATCH).await?;
            let Some(last) = tombstones.last().map(|t| t.object_key.clone()) else {
                return Ok(moved);
            };
            after = Some(last);
            for t in tombstones {
                match self.materialize_marker(bucket, &t.object_key).await {
                    Ok(true) => moved += 1,
                    Ok(false) => {}
                    Err(err) => log::warn!("layout migration bucket={bucket} key={} marker error={err}", t.object_key),
                }
            }
        }
    }



    /// Buckets whose index is missing, legacy-schema, or otherwise not current
    /// and therefore need a rebuild before they can serve. Used by the server
    /// layer to launch registry-tracked startup rebuilds.
    pub async fn buckets_needing_rebuild(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for (bucket, _) in self.list_buckets().await? {
            let bucket_dir = self.layout.bucket_dir(&bucket)?;
            match index::needs_rebuild(&bucket_dir).await {
                Ok(true) => out.push(bucket),
                Ok(false) => {}
                Err(err) => log::warn!("rebuild check failed bucket={bucket} error={err}"),
            }
        }
        Ok(out)
    }

    /// Spawns a background rebuild task for `bucket`. While it runs, every
    /// request against the bucket fails with `BucketRebuilding` (503).
    /// Returns `false` (and does nothing) if a rebuild is already running.
    pub fn start_rebuild_background(&self, bucket: &str) -> bool {
        let mut guard = self.rebuilding.lock().unwrap();
        if guard.contains(bucket) {
            log::info!("rebuild trigger ignored bucket={bucket} reason=already_running");
            return false;
        }
        guard.insert(bucket.to_string());
        drop(guard);

        let store = self.clone();
        let bucket = bucket.to_string();
        tokio::spawn(async move {
            log::info!("rebuild task started bucket={bucket}");
            match store.rebuild_index(&bucket).await {
                Ok(count) => log::info!("rebuild complete bucket={bucket} objects={count}"),
                Err(err) => log::error!("rebuild failed bucket={bucket} error={err}"),
            }
            store.rebuilding.lock().unwrap().remove(&bucket);
            log::info!("rebuild task stopped bucket={bucket}");
        });
        true
    }

    /// Startup scan: triggers a (503-gated) rebuild for every bucket whose
    /// index is missing, legacy-schema, or otherwise not current.
    pub async fn start_missing_index_rebuilds(&self) -> Result<usize> {
        let buckets = self.list_buckets().await?;
        let mut started = 0usize;
        for (bucket, _) in buckets {
            let bucket_dir = self.layout.bucket_dir(&bucket)?;
            match index::needs_rebuild(&bucket_dir).await {
                Ok(false) => continue,
                Ok(true) => {
                    log::warn!("rebuild auto trigger bucket={bucket} reason=missing_or_outdated_index");
                    if self.start_rebuild_background(&bucket) {
                        started += 1;
                    }
                }
                Err(err) => {
                    log::warn!("rebuild check failed bucket={bucket} error={err}");
                }
            }
        }
        log::info!("rebuild auto trigger scan complete started={started}");
        Ok(started)
    }

    /// Current rebuild progress per bucket (empty when nothing rebuilds).
    pub fn rebuild_statuses(&self) -> Vec<(String, RebuildProgress)> {
        self.rebuild_progress
            .lock()
            .unwrap()
            .iter()
            .map(|(bucket, progress)| (bucket.clone(), *progress))
            .collect()
    }

    /// Rebuilds the catalog from the self-describing blob tree: a bounded
    /// walker feeds N meta.json readers feeding one batch writer into a
    /// temporary database that is atomically swapped in. Every version dir
    /// found is a version; per key the newest is current, real versions
    /// are kept, and of the `null` states only the newest survives (see
    /// `ObjectIndex::insert_rebuild_batch`). Delete markers rebuild the
    /// key's tombstone. Displaced duplicates and invalid dirs are trashed
    /// on the spot. `trash/` and `staging/` are never
    /// scanned (that would resurrect deleted objects).
    pub async fn rebuild_index(&self, bucket: &str) -> Result<usize> {
        self.rebuild_progress.lock().unwrap().insert(
            bucket.to_string(),
            RebuildProgress {
                objects_indexed: 0,
                dirs_trashed: 0,
                started_at_ms: now_ms(),
            },
        );
        let result = self.rebuild_index_inner(bucket).await;
        self.rebuild_progress.lock().unwrap().remove(bucket);
        result
    }

    async fn rebuild_index_inner(&self, bucket: &str) -> Result<usize> {
        validate_bucket_name(bucket)?;
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        let objects_dir = bucket_dir.join("objects");
        let live_path = index::index_db_path(&bucket_dir);

        // Hold the open lock for the whole rebuild so nothing re-opens the
        // live database under us, then take the cached handle exclusively:
        // requests that already cloned it may still be committing, and a
        // commit into a database we are about to replace would be lost. The
        // callers raised the 503 gate, so no new request picks it up.
        let open_lock = self.index_open_lock(bucket);
        let _open_guard = open_lock.lock().await;
        let old = match self.take_index_exclusive(bucket).await? {
            Some(old) => Some(old),
            None if live_path.exists() => match ObjectIndex::open_at(&live_path, Durability::Relaxed).await {
                Ok(old) => Some(old),
                Err(err) => {
                    log::warn!(
                        "rebuild cannot read prior index bucket={bucket} error={err}; pending intents ignored"
                    );
                    None
                }
            },
            None => None,
        };

        // Pending intents in the prior index describe blob dirs that are NOT
        // (or no longer) objects: a retirement whose trash move never ran, or
        // a publish that never committed. The walk below would resurrect
        // them (a deleted object coming back), so finish them first — the
        // same adjudication the intent resolver applies.
        let mut dirs_trashed_up_front = 0usize;
        if let Some(old) = &old {
            match self.retire_pending_intent_dirs(old, &bucket_dir, bucket).await {
                Ok(n) => dirs_trashed_up_front = n,
                Err(err) => {
                    log::warn!(
                        "rebuild could not read pending intents bucket={bucket} error={err}; continuing without them"
                    );
                }
            }
        }
        // A RocksDB "database" is a directory, so the temp index is a directory
        // built alongside the live one and swapped in wholesale at the end.
        let tmp_path = bucket_dir.join("index.rebuild.rocksdb");
        let _ = tokio::fs::remove_dir_all(&tmp_path).await;
        let tmp = ObjectIndex::open_at(&tmp_path, Durability::Relaxed).await?;
        tmp.create_schema().await?;

        if let Some(old) = &old {
            // Tombstones first: the walk adjudicates what it finds on disk
            // against them (a carried delete still wins over an older blob).
            match old.copy_tombstones_into(&tmp).await {
                Ok(n) => log::info!("rebuild carried tombstones bucket={bucket} entries={n}"),
                Err(err) => log::warn!("rebuild could not carry tombstones bucket={bucket} error={err}"),
            }
        }

        // One pool of workers traverses and parses concurrently over a
        // work-stealing frontier. A serial walker starves on hash-fanout
        // trees (each object owns a ~4-dir chain, so directory expansion —
        // not JSON parsing — dominates); parallelizing the traversal is
        // what makes rebuild disk-bound instead of syscall-latency-bound.
        let worker_count = match self.rebuild_reader_threads {
            0 => std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8),
            n => n,
        };
        let (entry_tx, mut entry_rx) =
            tokio::sync::mpsc::channel::<RebuildMessage>(self.rebuild_queue_bound);
        let frontier = Arc::new(RebuildFrontier::new());
        frontier.push(objects_dir.clone());
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut workers = Vec::new();
        for _ in 0..worker_count.max(1) {
            let frontier = Arc::clone(&frontier);
            let entry_tx = entry_tx.clone();
            let bucket_dir = bucket_dir.clone();
            let shutdown = self.shutdown.clone();
            let cancelled = Arc::clone(&cancelled);
            let aborted = Arc::clone(&aborted);
            workers.push(tokio::spawn(async move {
                while let Some(dir) = frontier.pop().await {
                    if shutdown.is_cancelled() {
                        cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
                        frontier.drain();
                        frontier.task_done();
                        break;
                    }
                    if aborted.load(std::sync::atomic::Ordering::SeqCst) {
                        frontier.drain();
                        frontier.task_done();
                        break;
                    }
                    let message = match rebuild_classify_dir(&dir, &bucket_dir).await {
                        Ok(RebuildDir::Descend(subdirs)) => {
                            for sub in subdirs {
                                frontier.push(sub);
                            }
                            None
                        }
                        Ok(RebuildDir::Message(message)) => Some(message),
                        Err(err) => Some(RebuildMessage::Error(err)),
                    };
                    if let Some(message) = message {
                        if entry_tx.send(message).await.is_err() {
                            frontier.drain();
                            frontier.task_done();
                            break;
                        }
                    }
                    frontier.task_done();
                }
            }));
        }
        drop(entry_tx);
        log::info!("rebuild scanning bucket={bucket} workers={worker_count}");

        // Batch writer (this task): newer-wins upserts; losers and invalid
        // dirs go to trash immediately — migration day is the one full pass
        // over the tree, so it doubles as the audit. Any I/O error aborts the
        // rebuild before the swap, leaving the prior index in place.
        let mut indexed = 0usize;
        let mut trashed = dirs_trashed_up_front;
        let written: Result<()> = async {
            let mut batch: Vec<VersionRecord> = Vec::with_capacity(self.rebuild_batch_size);
            let mut dirs_by_rel: HashMap<String, PathBuf> = HashMap::new();
            let site = self.site_id();
            loop {
                let message = entry_rx.recv().await;
                let done = message.is_none();
                let flush = match message {
                    Some(RebuildMessage::Object(meta, rel, dir)) => {
                        // A blob from before stamps gets the fallback stamp
                        // (its Last-Modified, this site), as every read does.
                        let stamp = meta.stamp.clone().unwrap_or_else(|| Stamp::fallback(meta.last_modified_ms, &site));
                        dirs_by_rel.insert(rel.clone(), dir);
                        batch.push(VersionRecord {
                            object_key: meta.object_key.clone(),
                            stamp,
                            blob_dir: rel,
                            size: meta.size,
                            etag: meta.etag.clone(),
                            last_modified_ms: meta.last_modified_ms,
                            parts: (!meta.delete_marker).then(|| crate::storage::changelog::PartLayout::of(&meta)),
                            versioned: meta.versioned,
                            delete_marker: meta.delete_marker,
                            removed: meta.removed.clone(),
                        });
                        batch.len() >= self.rebuild_batch_size
                    }
                    Some(RebuildMessage::Invalid(dir)) => {
                        log::warn!(
                            "rebuild trashing invalid blob dir bucket={bucket} dir={}",
                            dir.display()
                        );
                        move_object_dir_to_trash(&self.layout, bucket, &dir).await?;
                        trashed += 1;
                        false
                    }
                    Some(RebuildMessage::Error(err)) => return Err(err),
                    None => true,
                };
                if flush && !batch.is_empty() {
                    let outcome = tmp.insert_rebuild_batch(&batch).await?;
                    indexed += outcome.inserted;
                    for loser in &outcome.loser_blob_dirs {
                        let abs = dirs_by_rel
                            .remove(loser)
                            .unwrap_or_else(|| bucket_dir.join(loser));
                        log::info!(
                            "rebuild trashing superseded duplicate bucket={bucket} dir={loser}"
                        );
                        move_object_dir_to_trash(&self.layout, bucket, &abs).await?;
                        trashed += 1;
                    }
                    batch.clear();
                    dirs_by_rel.clear();
                    if let Some(p) = self.rebuild_progress.lock().unwrap().get_mut(bucket) {
                        p.objects_indexed = indexed;
                        p.dirs_trashed = trashed;
                    }
                    if indexed % REBUILD_PROGRESS_EVERY < self.rebuild_batch_size {
                        log::info!(
                            "rebuild progress bucket={bucket} objects_indexed={indexed} trashed={trashed}"
                        );
                    }
                }
                if done {
                    return Ok(());
                }
            }
        }
        .await;
        if written.is_err() {
            aborted.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        // Unblocks any worker parked on a full channel.
        drop(entry_rx);
        for worker in workers {
            worker
                .await
                .map_err(|err| StorageError::Io(format!("rebuild worker panicked: {err}")))?;
        }
        if let Err(err) = written {
            drop(tmp);
            let _ = tokio::fs::remove_dir_all(&tmp_path).await;
            log::error!("rebuild aborted bucket={bucket} error={err}; prior index kept");
            return Err(err);
        }
        if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(StorageError::Io("rebuild cancelled".to_string()));
        }

        if let Some(old) = old {
            // The changelog describes the prior index. It is carried across
            // (so webhook / replication offsets stay valid) only when the
            // rebuilt catalog is the same catalog: if the tree disagreed with
            // the prior index — the index was stale or corrupt — the log no
            // longer describes the state, and the new index keeps its own
            // fresh log id; consumers detect the switch and resnapshot.
            match self.rebuild_matches_prior(&old, &tmp).await {
                Ok(None) => match old.copy_changelog_into(&tmp).await {
                    Ok(n) => log::info!("rebuild carried changelog bucket={bucket} entries={n}"),
                    Err(err) => log::warn!(
                        "rebuild could not carry changelog bucket={bucket} error={err}; consumers will resnapshot"
                    ),
                },
                Ok(Some(why)) => log::warn!(
                    "rebuild changed the catalog bucket={bucket} ({why}); the changelog starts over and consumers resnapshot"
                ),
                Err(err) => log::warn!(
                    "rebuild could not compare with the prior index bucket={bucket} error={err}; the changelog starts over"
                ),
            }
            old.close().await;
            // Drop the last handle so RocksDB releases its directory lock
            // before the live directory is replaced.
            drop(old);
        }

        tmp.checkpoint_truncate().await?;
        tmp.close().await;
        // `close` flushes; the files are open until the last handle drops.
        // Windows refuses to rename a directory with open handles, so the
        // handle must be gone before the swap (harmless elsewhere).
        drop(tmp);

        // Remove the prior live index (a RocksDB directory) plus any legacy
        // SQLite leftovers from before the switch, then atomically swap the
        // freshly built directory into place.
        let _ = tokio::fs::remove_dir_all(&live_path).await;
        for suffix in ["", "-wal", "-shm"] {
            let _ = tokio::fs::remove_file(bucket_dir.join(format!("index.sqlite{suffix}"))).await;
        }
        tokio::fs::rename(&tmp_path, &live_path).await?;
        if self.durability == Durability::Full {
            fsync_dir(&bucket_dir).await?;
        }
        log::info!(
            "rebuild finished bucket={bucket} objects_indexed={indexed} dirs_trashed={trashed}"
        );
        Ok(indexed)
    }

    /// Whether the rebuilt catalog `tmp` has exactly the rows of the prior
    /// index `old` (same key → same blob dir, same count). `Some(reason)`
    /// names the first difference.
    async fn rebuild_matches_prior(&self, old: &ObjectIndex, tmp: &ObjectIndex) -> Result<Option<String>> {
        let (old_count, new_count) = (old.object_count().await?, tmp.object_count().await?);
        if old_count != new_count {
            return Ok(Some(format!("{old_count} objects before, {new_count} after")));
        }
        let mut after: Option<String> = None;
        loop {
            let batch = old.all_entries_after(after.as_deref(), 1000).await?;
            let Some(last) = batch.last().map(|r| r.object_key.clone()) else {
                return Ok(None);
            };
            for row in &batch {
                match tmp.get(&row.object_key).await? {
                    Some(now) if index::same_blob_dir(&now.blob_dir, &row.blob_dir) => {}
                    Some(now) => return Ok(Some(format!("{} moved from {} to {}", row.object_key, row.blob_dir, now.blob_dir))),
                    None => return Ok(Some(format!("{} is gone", row.object_key))),
                }
            }
            after = Some(last);
        }
    }

    /// Before a rebuild walks the tree: trashes every blob dir a pending
    /// intent in the prior index describes, unless that index's row for the
    /// key still references it (a live blob is never trashed). Returns the
    /// number of dirs trashed.
    async fn retire_pending_intent_dirs(
        &self,
        old: &ObjectIndex,
        bucket_dir: &Path,
        bucket: &str,
    ) -> Result<usize> {
        // `now = i64::MAX, min_age = 0` selects every intent.
        let intents = old.stale_intents(i64::MAX, 0, i64::MAX).await?;
        let mut trashed = 0usize;
        for intent in intents {
            let rel = intent.blob_dir.as_str();
            if !rel.starts_with("objects/") || rel.split('/').any(|c| c == ".." || c.is_empty()) {
                log::warn!(
                    "rebuild ignoring intent {} with unexpected blob dir {rel}",
                    intent.id
                );
                continue;
            }
            let live = old
                .get(&intent.object_key)
                .await?
                .is_some_and(|row| row.blob_dir == intent.blob_dir);
            if live {
                continue;
            }
            let abs = bucket_dir.join(rel);
            if tokio::fs::metadata(&abs).await.is_ok() {
                log::info!(
                    "rebuild trashing blob dir of pending intent bucket={bucket} key={} op={} dir={rel}",
                    intent.object_key,
                    intent.op
                );
                move_object_dir_to_trash(&self.layout, bucket, &abs).await?;
                trashed += 1;
            }
        }
        Ok(trashed)
    }

    async fn ensure_bucket_and_key(&self, bucket: &str, key: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        validate_object_key(key)?;
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        Ok(())
    }

    async fn validate_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<UploadMeta> {
        self.ensure_bucket_and_key(bucket, key).await?;
        validate_staging_id(upload_id)
            .map_err(|_| StorageError::NoSuchUpload(upload_id.to_string()))?;
        let staging_dir = self.layout.multipart_staging_dir(bucket, upload_id)?;
        let upload: UploadMeta = read_json(&staging_dir.join("upload.json"))
            .await
            .map_err(|_| StorageError::NoSuchUpload(upload_id.to_string()))?;
        if upload.bucket != bucket || upload.object_key != key || upload.upload_id != upload_id {
            return Err(StorageError::NoSuchUpload(upload_id.to_string()));
        }
        Ok(upload)
    }
}

enum RebuildMessage {
    Object(Box<ObjectMeta>, String, PathBuf),
    Invalid(PathBuf),
    /// An I/O failure while walking: the rebuild must abort, not guess.
    Error(StorageError),
}

enum RebuildDir {
    /// Not an object dir: walk these children.
    Descend(Vec<PathBuf>),
    Message(RebuildMessage),
}

/// Classifies one directory of the rebuild walk. I/O errors propagate — an
/// unreadable directory or `meta.json` is never mistaken for an empty or
/// invalid one (which would drop or trash a live object). A dir is `Invalid`
/// only when its listing succeeded and it either holds files but no
/// `meta.json`, or its `meta.json` was read and failed to parse.
async fn rebuild_classify_dir(dir: &Path, bucket_dir: &Path) -> Result<RebuildDir> {
    let mut has_meta = false;
    let mut subdirs = Vec::new();
    let mut has_files = false;
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        // Gone since it was queued (the empty-dir reclaimer), or no
        // `objects/` yet: nothing to index.
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return Ok(RebuildDir::Descend(Vec::new()))
        }
        Err(err) => {
            return Err(StorageError::Io(format!(
                "rebuild cannot list {}: {err}",
                dir.display()
            )))
        }
    };
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(err) => {
                return Err(StorageError::Io(format!(
                    "rebuild cannot list {}: {err}",
                    dir.display()
                )))
            }
        };
        if entry.file_name() == "meta.json" {
            has_meta = true;
        } else {
            // d_type from readdir — no extra stat.
            let ft = entry.file_type().await.map_err(|err| {
                StorageError::Io(format!(
                    "rebuild cannot stat {}: {err}",
                    entry.path().display()
                ))
            })?;
            if ft.is_dir() {
                subdirs.push(entry.path());
            } else {
                has_files = true;
            }
        }
    }
    if has_meta {
        let meta_path = dir.join("meta.json");
        let bytes = tokio::fs::read(&meta_path).await.map_err(|err| {
            StorageError::Io(format!("rebuild cannot read {}: {err}", meta_path.display()))
        })?;
        let Ok(meta) = serde_json::from_slice::<ObjectMeta>(&bytes) else {
            return Ok(RebuildDir::Message(RebuildMessage::Invalid(dir.to_path_buf())));
        };
        let rel = dir.strip_prefix(bucket_dir).map_err(|_| {
            StorageError::Io(format!("rebuild walked outside the bucket: {}", dir.display()))
        })?;
        return Ok(RebuildDir::Message(RebuildMessage::Object(
            Box::new(meta),
            rel_path_string(rel),
            dir.to_path_buf(),
        )));
    }
    if !subdirs.is_empty() || !has_files {
        Ok(RebuildDir::Descend(subdirs))
    } else {
        // Files but no meta.json: cannot be a valid publish (blobs are
        // staged complete before the rename).
        Ok(RebuildDir::Message(RebuildMessage::Invalid(dir.to_path_buf())))
    }
}

/// A bucket-relative path in the index's canonical form: components joined
/// with `/` on every platform (the request path always writes `/`).
fn rel_path_string(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Work-stealing directory frontier for the parallel rebuild traversal.
/// `pending` counts pushed-but-unfinished directories; when it reaches
/// zero every worker's `pop` returns `None` and the pool drains.
struct RebuildFrontier {
    stack: Mutex<Vec<PathBuf>>,
    pending: std::sync::atomic::AtomicUsize,
    notify: tokio::sync::Notify,
}

impl RebuildFrontier {
    fn new() -> Self {
        Self {
            stack: Mutex::new(Vec::new()),
            pending: std::sync::atomic::AtomicUsize::new(0),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn push(&self, dir: PathBuf) {
        self.pending
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.stack.lock().unwrap().push(dir);
        self.notify.notify_waiters();
    }

    /// Marks one popped directory fully processed (children already pushed).
    fn task_done(&self) {
        self.pending
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Empties the stack (cancellation), accounting each dropped entry.
    fn drain(&self) {
        let dropped: Vec<_> = std::mem::take(&mut *self.stack.lock().unwrap());
        for _ in dropped {
            self.task_done();
        }
    }

    async fn pop(&self) -> Option<PathBuf> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Enrol as a waiter BEFORE inspecting the stack / pending counter.
            // `notify_waiters()` (called by `push`/`task_done`) wakes only
            // already-enrolled waiters and stores no permit, so a notification
            // that fires between `notified()` and `.await` would otherwise be
            // lost — fatally so for the final `pending 1 -> 0` transition, which
            // has no follow-up notification to re-wake a missed worker.
            notified.as_mut().enable();
            if let Some(dir) = self.stack.lock().unwrap().pop() {
                return Some(dir);
            }
            if self.pending.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                return None;
            }
            notified.await;
        }
    }
}

/// Lowers the rebuild gate for `bucket` when dropped (every exit path).
struct RebuildGateGuard<'a> {
    store: &'a LocalObjectStore,
    bucket: &'a str,
}

impl Drop for RebuildGateGuard<'_> {
    fn drop(&mut self) {
        self.store.end_rebuild(self.bucket);
    }
}

/// Removes a staging dir on drop unless disarmed — every error (and a
/// cancelled request future) cleans up instead of leaking the staged blob.
struct StagingDirGuard {
    path: Option<PathBuf>,
}

impl StagingDirGuard {
    fn new(path: &Path) -> Self {
        Self {
            path: Some(path.to_path_buf()),
        }
    }

    /// The staging dir is handed on (or already consumed); keep it.
    fn disarm(mut self) {
        self.path = None;
    }
}

impl Drop for StagingDirGuard {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn_blocking(move || {
                    let _ = std::fs::remove_dir_all(&path);
                });
            }
            Err(_) => {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }
}

// ── free helpers ─────────────────────────────────────────────────────────────

/// The per-upload lock: taken by Complete, Abort and the staging sweeper (in
/// that order before any part or object-key lock) so they never interleave.
fn upload_lock_key(upload_id: &str) -> String {
    format!("\0mpu/{upload_id}")
}

/// The per-part lock: taken by UploadPart's commit and, for every requested
/// part (ascending), by Complete from reading the part meta until publish.
/// `\0` (illegal in object keys) keeps these off real object-key locks.
fn part_lock_key(upload_id: &str, part_number: u16) -> String {
    format!("\0mpu/{upload_id}/part.{part_number}")
}

/// A part data file name recorded in `part.N.meta.json` must be a plain
/// file name inside the upload dir.
fn is_plain_file_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains(['/', '\\'])
}


fn blob_rel_parent(rel: &str) -> &str {
    rel.rsplit_once('/').map(|(parent, _)| parent).unwrap_or("")
}


async fn fsync_file(path: &Path) -> Result<()> {
    // Windows' FlushFileBuffers needs a handle with write access; a read-only
    // one fails with "Access is denied". fsync(2) has no such requirement, and
    // there a read-only handle cannot be mistaken for a write.
    #[cfg(windows)]
    let file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
    #[cfg(not(windows))]
    let file = tokio::fs::File::open(path).await?;
    file.sync_all().await?;
    Ok(())
}

/// The errors a publish rename (or the leaf `create_dir_all` before it) gets
/// when the empty-directory reclaimer removes the destination's fanout leaf at
/// the same instant. Retrying — which recreates the leaf — converges. `ENOENT`
/// everywhere; on Windows a directory in the delete-pending state answers
/// `ERROR_ACCESS_DENIED` until the delete completes.
fn is_leaf_reclaimed_race(err: &std::io::Error) -> bool {
    err.kind() == ErrorKind::NotFound
        || (cfg!(windows) && err.kind() == ErrorKind::PermissionDenied)
}

/// Makes a directory's entries durable: opens it and fsyncs, which is how
/// POSIX persists a rename or a new name. On Windows a directory cannot be
/// opened as a `File` (`Access is denied`), and NTFS journals directory
/// metadata itself, so there is nothing to do.
#[cfg(not(windows))]
async fn fsync_dir(path: &Path) -> Result<()> {
    let file = tokio::fs::File::open(path).await?;
    file.sync_all().await?;
    Ok(())
}
#[cfg(windows)]
async fn fsync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

/// Makes a prepared publish dir durable before anything references it:
/// every part file, meta.json, and the directory itself.
async fn fsync_publish_dir(publish_dir: &Path, meta: &ObjectMeta) -> Result<()> {
    for part in &meta.parts {
        fsync_file(&publish_dir.join(&part.file)).await?;
    }
    fsync_file(&publish_dir.join("meta.json")).await?;
    fsync_dir(publish_dir).await?;
    Ok(())
}

async fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = tokio::fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn prepare_single_publish_dir(
    staging_dir: &Path,
    staged_part: &Path,
    meta: &ObjectMeta,
) -> Result<PathBuf> {
    let publish_dir = staging_dir.join("object");
    tokio::fs::create_dir_all(&publish_dir).await?;
    let publish_part = publish_dir.join("part.1");
    if staged_part != publish_part {
        // Always move the staged bytes in (the rename replaces any leftover
        // from an earlier attempt), never keep a stale `object/part.1`.
        tokio::fs::rename(staged_part, &publish_part).await?;
    }
    ensure_file_exists(&publish_part).await?;
    write_json_atomic(&publish_dir.join("meta.json"), meta).await?;
    Ok(publish_dir)
}

/// Reads and parses a JSON file, `Ok(None)` if it does not exist.
async fn read_json_if_exists<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Undoes whatever an earlier, failed or crashed Complete left in the
/// upload's `object/` publish dir, so every Complete starts from the parts as
/// currently committed. Each `object/part.N` goes back to the data file its
/// `part.N.meta.json` names — unless that file exists again (the part was
/// re-uploaded since, so the moved copy is stale) or the part no longer has a
/// meta, in which case the copy is dropped. Idempotent under crashes: a
/// half-done reset is simply finished by the next one. Caller holds the
/// upload lock.
async fn reset_multipart_publish_dir(staging_dir: &Path) -> Result<()> {
    let publish_dir = staging_dir.join("object");
    let mut entries = match tokio::fs::read_dir(&publish_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let number = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix("part."))
            .and_then(|n| n.parse::<u16>().ok());
        let restore_to = match number {
            Some(number) => {
                read_json_if_exists::<PartMeta>(
                    &staging_dir.join(format!("part.{number}.meta.json")),
                )
                .await?
                .filter(|meta| is_plain_file_name(&meta.file))
                .map(|meta| staging_dir.join(meta.file))
            }
            None => None,
        };
        match restore_to {
            Some(target) if tokio::fs::metadata(&target).await.is_err() => {
                tokio::fs::rename(&path, &target).await?;
            }
            _ => match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            },
        }
    }
    tokio::fs::remove_dir(&publish_dir).await?;
    Ok(())
}

/// Moves each part's committed data file (`parts[i].file`) into a fresh
/// `object/` publish dir under its canonical name (`meta.parts[i].file`) and
/// writes `meta.json`. The caller has reset `object/` first.
async fn prepare_multipart_publish_dir(
    staging_dir: &Path,
    parts: &[PartMeta],
    meta: &ObjectMeta,
) -> Result<PathBuf> {
    let publish_dir = staging_dir.join("object");
    tokio::fs::create_dir_all(&publish_dir).await?;
    for (source_part, object_part) in parts.iter().zip(&meta.parts) {
        let source = staging_dir.join(&source_part.file);
        let dest = publish_dir.join(&object_part.file);
        tokio::fs::rename(&source, &dest).await?;
        ensure_file_exists(&dest).await?;
    }
    write_json_atomic(&publish_dir.join("meta.json"), meta).await?;
    Ok(publish_dir)
}

/// Removes every empty directory under `dir` (bottom-up, `remove_dir` only,
/// so a dir holding anything is never touched) and returns whether `dir`
/// itself ended up removed. `depth` 0 is the `objects/` root, which stays.
async fn prune_empty_dirs(dir: &Path, depth: usize) -> bool {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return false;
    };
    let mut remaining = 0usize;
    let mut subdirs = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        match entry.file_type().await {
            Ok(ft) if ft.is_dir() => subdirs.push(entry.path()),
            _ => remaining += 1,
        }
    }
    drop(entries);
    for sub in subdirs {
        if !Box::pin(prune_empty_dirs(&sub, depth + 1)).await {
            remaining += 1;
        }
    }
    if depth == 0 || remaining > 0 {
        return false;
    }
    tokio::fs::remove_dir(dir).await.is_ok()
}

pub(crate) async fn move_object_dir_to_trash(
    layout: &StorageLayout,
    bucket: &str,
    object_dir: &Path,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(layout.trash_dir(bucket)?).await?;
    loop {
        let trash_id = new_staging_id(now_ms());
        let trash_dir = layout.object_trash_dir(bucket, &trash_id)?;
        match tokio::fs::rename(object_dir, &trash_dir).await {
            Ok(()) => return Ok(trash_dir),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(trash_dir),
            Err(err) => return Err(err.into()),
        }
    }
}

async fn write_json_atomic<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension("tmp");
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut file = tokio::fs::File::create(&tmp).await?;
    file.write_all(&bytes).await?;
    file.flush().await?;
    tokio::fs::rename(tmp, path).await?;
    Ok(())
}

/// A unique temp path for an in-flight part upload, inside the upload's own
/// folder so the swap into `part.N` is an atomic same-directory rename. The
/// `.tmp.` infix keeps it invisible to `list_parts` (which matches
/// `part.*.meta.json`) and to Complete (which reads exact `part.N`), so a
/// partial or racing upload is never observed as a committed part.
fn part_temp_path(staging_dir: &Path, part_number: u16) -> PathBuf {
    staging_dir.join(format!("part.{part_number}.tmp.{}", new_staging_id(now_ms())))
}

async fn write_file_with_md5(path: &Path, bytes: &[u8]) -> Result<String> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::File::create(path).await?;
    let mut hasher = Md5::new();
    const CHUNK: usize = 256 * 1024;
    for chunk in bytes.chunks(CHUNK) {
        hasher.update(chunk);
        file.write_all(chunk).await?;
    }
    file.flush().await?;
    Ok(hex_lower(&hasher.finalize()))
}

/// Copies an immutable object (or inclusive byte range) into one staging file.
/// A single fixed-size buffer is reused across every source part, so multipart
/// sources do not require object-sized memory or one allocation per part.
async fn copy_object_data_with_hashes(
    source: &ReadObject,
    path: &Path,
    range: Option<(u64, u64)>,
) -> Result<WrittenHashes> {
    let (range_start, range_end) = match range {
        Some((start, end)) if start <= end && end < source.meta.size => (start, end),
        Some(_) => {
            return Err(StorageError::InvalidMultipartUpload(
                "invalid copy source range".to_string(),
            ))
        }
        None if source.meta.size == 0 => (0, 0),
        None => (0, source.meta.size - 1),
    };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut output = tokio::fs::File::create(path).await?;
    let mut md5 = Md5::new();
    let mut sha256 = Sha256::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; COPY_BUFFER_SIZE];

    if source.meta.size != 0 {
        for (part, &part_start) in source.meta.parts.iter().zip(&source.part_offsets) {
            let part_end = part_start.saturating_add(part.size);
            let copy_start = range_start.max(part_start);
            let copy_end = range_end.saturating_add(1).min(part_end);
            if copy_start >= copy_end {
                continue;
            }
            let mut input = tokio::fs::File::open(source.object_dir.join(&part.file)).await?;
            let skip = copy_start - part_start;
            if skip != 0 {
                input.seek(SeekFrom::Start(skip)).await?;
            }
            let mut remaining = copy_end - copy_start;
            while remaining != 0 {
                let wanted = remaining.min(buffer.len() as u64) as usize;
                let read = input.read(&mut buffer[..wanted]).await?;
                if read == 0 {
                    return Err(StorageError::CorruptObject(format!(
                        "part {} ended before its recorded size",
                        part.file
                    )));
                }
                let chunk = &buffer[..read];
                md5.update(chunk);
                sha256.update(chunk);
                output.write_all(chunk).await?;
                size += read as u64;
                remaining -= read as u64;
            }
        }
    }
    output.flush().await?;
    Ok(WrittenHashes {
        size,
        md5: hex_lower(&md5.finalize()),
        sha256: hex_lower(&sha256.finalize()),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WrittenHashes {
    size: u64,
    md5: String,
    sha256: String,
}

async fn write_stream_with_hashes<S, E>(path: &Path, mut stream: S) -> Result<WrittenHashes>
where
    S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::File::create(path).await?;
    let mut md5 = Md5::new();
    let mut sha256 = Sha256::new();
    let mut size = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|err| StorageError::Io(err.to_string()))?;
        size += chunk.len() as u64;
        md5.update(&chunk);
        sha256.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(WrittenHashes {
        size,
        md5: hex_lower(&md5.finalize()),
        sha256: hex_lower(&sha256.finalize()),
    })
}

async fn write_aws_chunked_stream_with_hashes<S, E>(
    path: &Path,
    mut stream: S,
) -> Result<WrittenHashes>
where
    S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::File::create(path).await?;
    let mut md5 = Md5::new();
    let mut sha256 = Sha256::new();
    let mut size = 0u64;
    // Chunk data is streamed straight through to the file as it arrives; only
    // chunk-header/trailer lines are buffered, each capped by the decoder.
    let mut decoder = AwsChunkedDecoder::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|err| {
            let message = err.to_string();
            // A chunk-signature failure detected upstream (the auth layer's
            // verifying body) is a client error, not an I/O fault.
            if message.contains(CHUNK_SIGNATURE_MISMATCH) {
                StorageError::InvalidAwsChunkedBody(CHUNK_SIGNATURE_MISMATCH.to_string())
            } else {
                StorageError::Io(message)
            }
        })?;
        for range in decoder.feed(&chunk)? {
            let data = &chunk[range];
            size += data.len() as u64;
            md5.update(data);
            sha256.update(data);
            file.write_all(data).await?;
        }
        if decoder.is_done() {
            break;
        }
    }
    decoder.finish()?;

    file.flush().await?;
    Ok(WrittenHashes {
        size,
        md5: hex_lower(&md5.finalize()),
        sha256: hex_lower(&sha256.finalize()),
    })
}

async fn ensure_file_exists(path: &Path) -> Result<()> {
    let meta = tokio::fs::metadata(path).await?;
    if meta.is_file() {
        Ok(())
    } else {
        Err(StorageError::Io(format!("not a file: {}", path.display())))
    }
}

async fn validate_object_parts(object_dir: &Path, meta: &ObjectMeta) -> Result<()> {
    let mut total = 0u64;
    for part in &meta.parts {
        let path = object_dir.join(&part.file);
        let file_meta = tokio::fs::metadata(&path).await.map_err(|_| {
            StorageError::CorruptObject(format!("missing part file {}", path.display()))
        })?;
        if !file_meta.is_file() {
            return Err(StorageError::CorruptObject(format!(
                "part path is not a file {}",
                path.display()
            )));
        }
        let len = file_meta.len();
        if len != part.size {
            return Err(StorageError::CorruptObject(format!(
                "part {} size mismatch: meta={} actual={}",
                part.file, part.size, len
            )));
        }
        total += len;
    }
    if total != meta.size {
        return Err(StorageError::CorruptObject(format!(
            "object size mismatch: meta={} actual={total}",
            meta.size
        )));
    }
    Ok(())
}

fn part_offsets(meta: &ObjectMeta) -> Vec<u64> {
    let mut offsets = Vec::with_capacity(meta.parts.len());
    let mut offset = 0u64;
    for part in &meta.parts {
        offsets.push(offset);
        offset = offset.saturating_add(part.size);
    }
    offsets
}

async fn has_active_staging(bucket_dir: &Path) -> Result<bool> {
    for kind in ["put", "multipart"] {
        let path = bucket_dir.join("staging").join(kind);
        let Ok(mut entries) = tokio::fs::read_dir(path).await else {
            continue;
        };
        if entries.next_entry().await?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn md5_hex(bytes: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

fn multipart_etag(parts: &[PartMeta]) -> Result<String> {
    let mut combined = Vec::with_capacity(parts.len() * 16);
    for part in parts {
        combined.extend(hex_to_bytes(&part.etag)?);
    }
    Ok(format!("{}-{}", md5_hex(&combined), parts.len()))
}

fn hex_to_bytes(value: &str) -> Result<Vec<u8>> {
    let clean = value.trim();
    if clean.len() % 2 != 0 {
        return Err(StorageError::InvalidMultipartUpload(format!(
            "invalid hex etag {value}"
        )));
    }
    let mut bytes = Vec::with_capacity(clean.len() / 2);
    for i in (0..clean.len()).step_by(2) {
        let byte = u8::from_str_radix(&clean[i..i + 2], 16).map_err(|_| {
            StorageError::InvalidMultipartUpload(format!("invalid hex etag {value}"))
        })?;
        bytes.push(byte);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Read-only rebuild phase benchmark against a REAL `objects/` tree. Reuses
    /// the production parallel walker but never trashes dirs or swaps any index,
    /// so it's safe to run against a live bucket. Attributes wall time to the
    /// three phases: FS traversal, meta.json read, SQLite insert.
    ///
    ///   BENCH_OBJECTS_DIR=/opt/rusts3/rusts3-data/buckets/doris/objects \
    ///   cargo test -p rust-s3-server --release bench_rebuild_phases -- --ignored --nocapture
    ///
    /// Optional: BENCH_WORKERS=N (default = CPU count), BENCH_BATCH=N (default 1000).
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn bench_rebuild_phases() {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::time::Instant;

        let objects_dir = std::path::PathBuf::from(
            std::env::var("BENCH_OBJECTS_DIR").expect("set BENCH_OBJECTS_DIR"),
        );
        let bucket_dir = objects_dir.parent().unwrap().to_path_buf();
        let workers_n: usize = std::env::var("BENCH_WORKERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)
            });
        let batch_size: usize = std::env::var("BENCH_BATCH")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        let tmp = tempfile::tempdir().unwrap();
        let idx =
            ObjectIndex::open_at(&tmp.path().join("bench.rocksdb"), Durability::Relaxed)
                .await
                .unwrap();
        idx.create_schema().await.unwrap();

        let walk_nanos = Arc::new(AtomicU64::new(0));
        let json_nanos = Arc::new(AtomicU64::new(0));
        let dirs = Arc::new(AtomicUsize::new(0));
        let objs = Arc::new(AtomicUsize::new(0));

        let (tx, mut rx) = tokio::sync::mpsc::channel::<VersionRecord>(4096);
        let frontier = Arc::new(RebuildFrontier::new());
        frontier.push(objects_dir.clone());

        let t0 = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..workers_n.max(1) {
            let frontier = Arc::clone(&frontier);
            let tx = tx.clone();
            let bucket_dir = bucket_dir.clone();
            let (walk_nanos, json_nanos, dirs, objs) = (
                Arc::clone(&walk_nanos),
                Arc::clone(&json_nanos),
                Arc::clone(&dirs),
                Arc::clone(&objs),
            );
            handles.push(tokio::spawn(async move {
                while let Some(dir) = frontier.pop().await {
                    dirs.fetch_add(1, Ordering::Relaxed);
                    // ── phase: FS traversal (read_dir + per-entry file_type) ──
                    let w = Instant::now();
                    let mut has_meta = false;
                    let mut subdirs = Vec::new();
                    if let Ok(mut entries) = tokio::fs::read_dir(&dir).await {
                        while let Ok(Some(entry)) = entries.next_entry().await {
                            if entry.file_name() == "meta.json" {
                                has_meta = true;
                            } else if matches!(entry.file_type().await, Ok(ft) if ft.is_dir()) {
                                subdirs.push(entry.path());
                            }
                        }
                    }
                    walk_nanos.fetch_add(w.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    if has_meta {
                        // ── phase: meta.json read + parse ──
                        let j = Instant::now();
                        let parsed = read_json::<ObjectMeta>(&dir.join("meta.json")).await;
                        json_nanos.fetch_add(j.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        if let Ok(meta) = parsed {
                            objs.fetch_add(1, Ordering::Relaxed);
                            let rel = dir
                                .strip_prefix(&bucket_dir)
                                .map(|p| p.to_string_lossy().to_string())
                                .unwrap_or_default();
                            let _ = tx
                                .send(VersionRecord {
                                    stamp: meta.stamp.clone().unwrap_or_else(|| Stamp::fallback(meta.last_modified_ms, "0000000000000000")),
                                    object_key: meta.object_key.clone(),
                                    blob_dir: rel,
                                    size: meta.size,
                                    etag: meta.etag.clone(),
                                    last_modified_ms: meta.last_modified_ms,
                                    parts: None,
                                    versioned: false,
                                    delete_marker: false,
                                    removed: None,
                                })
                                .await;
                        }
                    } else {
                        for sub in subdirs {
                            frontier.push(sub);
                        }
                    }
                    frontier.task_done();
                }
            }));
        }
        drop(tx);

        // ── phase: SQLite batched insert (serial writer) ──
        let mut sqlite_nanos = 0u64;
        let mut batch: Vec<VersionRecord> = Vec::with_capacity(batch_size);
        let mut inserted = 0usize;
        while let Some(rec) = rx.recv().await {
            batch.push(rec);
            if batch.len() >= batch_size {
                let s = Instant::now();
                inserted += idx.insert_rebuild_batch(&batch).await.unwrap().inserted;
                sqlite_nanos += s.elapsed().as_nanos() as u64;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            let s = Instant::now();
            inserted += idx.insert_rebuild_batch(&batch).await.unwrap().inserted;
            sqlite_nanos += s.elapsed().as_nanos() as u64;
        }
        for h in handles {
            h.await.unwrap();
        }
        let wall = t0.elapsed();

        let ms = |n: u64| n / 1_000_000;
        let per_worker = |n: u64| ms(n) / workers_n.max(1) as u64;
        eprintln!("──────── rebuild phase benchmark ────────");
        eprintln!("workers={workers_n} batch={batch_size}");
        eprintln!("dirs scanned : {}", dirs.load(Ordering::Relaxed));
        eprintln!("objects      : {} (inserted {inserted})", objs.load(Ordering::Relaxed));
        eprintln!("WALL         : {wall:?}");
        eprintln!(
            "walk  (fs)   : {} ms aggregate (~{} ms/worker) — read_dir + file_type",
            ms(walk_nanos.load(Ordering::Relaxed)),
            per_worker(walk_nanos.load(Ordering::Relaxed))
        );
        eprintln!(
            "json  (read) : {} ms aggregate (~{} ms/worker) — meta.json read+parse",
            ms(json_nanos.load(Ordering::Relaxed)),
            per_worker(json_nanos.load(Ordering::Relaxed))
        );
        eprintln!("sqlite(write): {} ms (serial)", ms(sqlite_nanos));
    }

    async fn store_and_bucket() -> (tempfile::TempDir, LocalObjectStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        (tmp, store)
    }

    #[tokio::test]
    async fn reclaim_removes_empty_fanout_dirs_but_never_objects() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (_tmp, store) = store_and_bucket().await;
        // A live object — its fanout dir holds it and must survive.
        store.put_object("bucket", "keep/me", b"hi", None, None, false).await.unwrap();
        let objects = store.layout().bucket_dir("bucket").unwrap().join("objects");
        // Empty single-level fanout dirs, as a delete leaves behind.
        tokio::fs::create_dir(objects.join("dead")).await.unwrap();
        tokio::fs::create_dir(objects.join("beef")).await.unwrap();

        let cancel = tokio_util::sync::CancellationToken::new();
        let reclaimed = std::sync::Arc::new(AtomicUsize::new(0));
        // One pass (no drain): reclaim is single-level now.
        let removed = store.reclaim_empty_dirs_pass("bucket", &cancel, &reclaimed).await.unwrap();

        assert_eq!(read_body(&store, "bucket", "keep/me").await, b"hi", "object untouched");
        assert!(!tokio::fs::try_exists(objects.join("dead")).await.unwrap(), "empty dir reclaimed");
        assert!(!tokio::fs::try_exists(objects.join("beef")).await.unwrap());
        assert!(tokio::fs::try_exists(&objects).await.unwrap(), "objects/ root kept");
        assert!(removed >= 2 && reclaimed.load(Ordering::Relaxed) >= 2);
        assert_invariants(&store, "bucket").await;
    }

    // The critical race: an aggressive reclaimer running concurrently with a
    // stream of PUT/DELETEs must never fail a PUT (publish retries on ENOENT)
    // and never lose an object (remove_dir can't touch a non-empty dir).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_publish_and_reclaim_lose_nothing_and_fail_no_put() {
        use std::sync::atomic::AtomicUsize;
        let (_tmp, store) = store_and_bucket().await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let reclaimed = std::sync::Arc::new(AtomicUsize::new(0));

        // Reclaimer hammering the tree as fast as it can.
        let reclaimer = {
            let store = store.clone();
            let cancel = cancel.clone();
            let reclaimed = std::sync::Arc::clone(&reclaimed);
            tokio::spawn(async move {
                while !cancel.is_cancelled() {
                    let _ = store.reclaim_empty_dirs_pass("bucket", &cancel, &reclaimed).await;
                    tokio::task::yield_now().await;
                }
            })
        };

        // Overlapping keys so overwrites/deletes constantly empty fanout dirs
        // that new puts must re-create — maximising the race window.
        for i in 0..300 {
            let key = format!("dir{}/obj{}", i % 8, i % 20);
            store
                .put_object("bucket", &key, format!("v{i}").as_bytes(), None, None, false)
                .await
                .unwrap_or_else(|e| panic!("PUT {i} failed under concurrent reclaim: {e}"));
            if i % 3 == 0 {
                store.delete_object("bucket", &key).await.unwrap();
            }
        }

        cancel.cancel();
        let _ = reclaimer.await;

        // Every object still indexed must be fully readable (dir intact).
        let page = store.list_objects("bucket", "", None, None, 10_000).await.unwrap();
        for entry in &page.entries {
            let _ = read_body(&store, "bucket", &entry.object_key).await;
        }
        assert_invariants(&store, "bucket").await;
    }

    /// Writes an object (single-level), then relocates it to a legacy 4-level
    /// path + repoints the index — simulating a pre-migration object.
    async fn make_legacy_object(store: &LocalObjectStore, key: &str, data: &[u8]) -> String {
        store.put_object("bucket", key, data, None, None, false).await.unwrap();
        let index = store.index("bucket").await.unwrap();
        let row = index.get(key).await.unwrap().unwrap();
        let bucket_dir = store.layout().bucket_dir("bucket").unwrap();
        let leaf = row.blob_dir.rsplit('/').next().unwrap();
        let legacy_rel = format!("objects/aa/bb/cc/dd/{leaf}");
        let legacy_dir = bucket_dir.join(&legacy_rel);
        tokio::fs::create_dir_all(legacy_dir.parent().unwrap()).await.unwrap();
        tokio::fs::rename(bucket_dir.join(&row.blob_dir), &legacy_dir).await.unwrap();
        // The now-empty key dir of the current layout goes too: a legacy
        // tree never had one.
        let _ = tokio::fs::remove_dir(bucket_dir.join(&row.blob_dir).parent().unwrap()).await;
        index.update_blob_dir(key, &row.blob_dir, &legacy_rel).await.unwrap();
        legacy_rel
    }

    #[tokio::test]
    async fn migrate_relocates_legacy_object_and_is_idempotent() {
        let (_tmp, store) = store_and_bucket().await;
        let legacy_rel = make_legacy_object(&store, "docs/a", b"hello").await;
        let bucket_dir = store.layout().bucket_dir("bucket").unwrap();
        assert_eq!(layouts::layout_of(&legacy_rel), Some(1));
        assert_eq!(read_body(&store, "bucket", "docs/a").await, b"hello");
        assert!(tokio::fs::try_exists(bucket_dir.join(&legacy_rel)).await.unwrap());

        assert!(store.relocate_object("bucket", "docs/a").await.unwrap());

        let row = store.index("bucket").await.unwrap().get("docs/a").await.unwrap().unwrap();
        assert!(layouts::is_current(&row.blob_dir), "now on the current layout: {}", row.blob_dir);
        let meta: ObjectMeta = read_json(&bucket_dir.join(&row.blob_dir).join("meta.json")).await.unwrap();
        assert_eq!((meta.format_version, meta.stamp.as_ref()), (META_FORMAT, row.stamp.as_ref()), "meta rewritten at the current format with the stamp");
        assert_eq!(read_body(&store, "bucket", "docs/a").await, b"hello", "content intact");
        assert!(
            !tokio::fs::try_exists(bucket_dir.join(&legacy_rel)).await.unwrap(),
            "old leaf removed"
        );
        // Idempotent: a second call is a no-op.
        assert!(!store.relocate_object("bucket", "docs/a").await.unwrap());
        assert_invariants(&store, "bucket").await;
    }

    // A read concurrent with a migration of the same object may *transiently*
    // fail (the old path can vanish between resolve and open — this is allowed),
    // but it must never return wrong/partial content, and every object must end
    // up migrated and fully readable. This guards against loss/corruption, not
    // the accepted transient window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn migration_under_concurrent_reads_loses_nothing() {
        let (_tmp, store) = store_and_bucket().await;
        for i in 0..40 {
            make_legacy_object(&store, &format!("k{i}"), format!("v{i}").as_bytes()).await;
        }
        let stop = tokio_util::sync::CancellationToken::new();
        let reader = {
            let store = store.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                while !stop.is_cancelled() {
                    for i in 0..40 {
                        let want = format!("v{i}").into_bytes();
                        // Tolerate a transient failure (old path vanished mid-read);
                        // only a *successful* read is held to correctness.
                        if let Ok(read) = store.read_object("bucket", &format!("k{i}")).await {
                            let mut body = Vec::new();
                            let mut complete = true;
                            for part in &read.meta.parts {
                                match tokio::fs::read(read.object_dir.join(&part.file)).await {
                                    Ok(bytes) => body.extend(bytes),
                                    Err(_) => {
                                        complete = false;
                                        break;
                                    }
                                }
                            }
                            if complete {
                                assert_eq!(body, want, "a successful read must return correct content");
                            }
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
        };
        for i in 0..40 {
            store.relocate_object("bucket", &format!("k{i}")).await.unwrap();
        }
        stop.cancel();
        reader.await.unwrap();

        // Every object migrated and fully readable afterwards — no loss.
        for i in 0..40 {
            let row = store.index("bucket").await.unwrap().get(&format!("k{i}")).await.unwrap().unwrap();
            assert!(layouts::is_current(&row.blob_dir), "all migrated: {}", row.blob_dir);
            assert_eq!(read_body(&store, "bucket", &format!("k{i}")).await, format!("v{i}").into_bytes());
        }
        assert_invariants(&store, "bucket").await;
    }

    /// The two machine-checkable invariants: every row's blob dir is intact
    /// and matches the row, and every intent resolves to nothing.
    async fn assert_invariants(store: &LocalObjectStore, bucket: &str) {
        let index = store.index(bucket).await.unwrap();
        let bucket_dir = store.layout().bucket_dir(bucket).unwrap();
        let rows = index.all_entries_after(None, i64::MAX).await.unwrap();
        for row in rows {
            let dir = bucket_dir.join(&row.blob_dir);
            let meta: ObjectMeta = read_json(&dir.join("meta.json"))
                .await
                .unwrap_or_else(|e| panic!("row {} references broken blob: {e}", row.object_key));
            assert_eq!(meta.etag, row.etag, "meta/row etag mismatch");
            assert_eq!(meta.size, row.size);
            validate_object_parts(&dir, &meta).await.unwrap();
        }
        store.drain_intents(bucket).await.unwrap();
        let index = store.index(bucket).await.unwrap();
        assert!(
            index.stale_intents(now_ms(), 0, 10).await.unwrap().is_empty(),
            "intents must drain to empty"
        );
    }

    async fn read_body(store: &LocalObjectStore, bucket: &str, key: &str) -> Vec<u8> {
        let read = store.read_object(bucket, key).await.unwrap();
        let mut body = Vec::new();
        for part in &read.meta.parts {
            body.extend(tokio::fs::read(read.object_dir.join(&part.file)).await.unwrap());
        }
        body
    }

    #[tokio::test]
    async fn put_get_list_roundtrip() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "a/b", b"hello", None, None, false)
            .await
            .unwrap();
        assert_eq!(read_body(&store, "bucket", "a/b").await, b"hello");
        let page = store.list_objects("bucket", "", None, None, 10).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].object_key, "a/b");
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn overwrite_serves_new_content_and_retires_old_dir() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "k", b"one", None, None, false)
            .await
            .unwrap();
        let first = store.index("bucket").await.unwrap().get("k").await.unwrap().unwrap();
        store
            .put_object("bucket", "k", b"two", None, None, false)
            .await
            .unwrap();
        let second = store.index("bucket").await.unwrap().get("k").await.unwrap().unwrap();
        assert_ne!(first.blob_dir, second.blob_dir, "publish must use a fresh dir");
        assert!(second.last_modified_ms > first.last_modified_ms, "monotonic clamp");
        assert_eq!(read_body(&store, "bucket", "k").await, b"two");
        // Old dir retired out of the live tree.
        let old_abs = store.layout().bucket_dir("bucket").unwrap().join(&first.blob_dir);
        assert!(!old_abs.exists());
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn delete_is_idempotent_and_removes_row() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "k", b"data", None, None, false)
            .await
            .unwrap();
        store.delete_object("bucket", "k").await.unwrap();
        assert!(matches!(
            store.read_object("bucket", "k").await,
            Err(StorageError::ObjectNotFound { .. })
        ));
        // Idempotent second delete.
        store.delete_object("bucket", "k").await.unwrap();
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn read_never_repairs_missing_blob_is_5xx_not_404() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "k", b"data", None, None, false)
            .await
            .unwrap();
        let row = store.index("bucket").await.unwrap().get("k").await.unwrap().unwrap();
        let abs = store.layout().bucket_dir("bucket").unwrap().join(&row.blob_dir);
        tokio::fs::remove_dir_all(&abs).await.unwrap();
        // Externally-damaged object: internal error, not 404, and the row survives.
        assert!(matches!(
            store.read_object("bucket", "k").await,
            Err(StorageError::CorruptObject(_))
        ));
        assert!(store.index("bucket").await.unwrap().get("k").await.unwrap().is_some());
    }

    // ── crash-point matrix ────────────────────────────────────────────────────

    async fn crash_put(store: &LocalObjectStore, key: &str, body: &'static [u8], point: &str) {
        store.arm_crash_point(point);
        let store2 = store.clone();
        let key = key.to_string();
        let result = tokio::spawn(async move {
            store2
                .put_object("bucket", &key, body, None, None, false)
                .await
        })
        .await;
        assert!(result.unwrap_err().is_panic(), "crash point must fire");
    }

    #[tokio::test]
    async fn crash_after_intent_leaves_no_object_and_drains_clean() {
        let (_tmp, store) = store_and_bucket().await;
        crash_put(&store, "k", b"data", "publish_after_intent").await;
        assert!(matches!(
            store.read_object("bucket", "k").await,
            Err(StorageError::ObjectNotFound { .. })
        ));
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn crash_after_rename_orphan_is_resolved_to_trash() {
        let (_tmp, store) = store_and_bucket().await;
        crash_put(&store, "k", b"data", "publish_after_rename").await;
        // Blob reached the live tree but the row never committed.
        assert!(matches!(
            store.read_object("bucket", "k").await,
            Err(StorageError::ObjectNotFound { .. })
        ));
        let resolved = store.drain_intents("bucket").await.unwrap();
        assert_eq!(resolved, 1, "publish intent must resolve the orphan");
        assert_invariants(&store, "bucket").await;
        // The orphan is out of the live tree (in trash).
        let page = store.list_objects("bucket", "", None, None, 10).await.unwrap();
        assert!(page.entries.is_empty());
    }

    #[tokio::test]
    async fn crash_after_commit_on_overwrite_old_dir_is_retired_by_resolver() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "k", b"one", None, None, false)
            .await
            .unwrap();
        let first = store.index("bucket").await.unwrap().get("k").await.unwrap().unwrap();
        crash_put(&store, "k", b"two", "publish_after_commit").await;
        // The new version is committed and readable even before resolution.
        assert_eq!(read_body(&store, "bucket", "k").await, b"two");
        // Old dir is still in the live tree, covered by a retire intent.
        let old_abs = store.layout().bucket_dir("bucket").unwrap().join(&first.blob_dir);
        assert!(old_abs.exists());
        store.drain_intents("bucket").await.unwrap();
        assert!(!old_abs.exists(), "resolver must finish the retirement");
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn crash_after_delete_commit_blob_is_retired_by_resolver() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "k", b"data", None, None, false)
            .await
            .unwrap();
        let row = store.index("bucket").await.unwrap().get("k").await.unwrap().unwrap();
        store.arm_crash_point("delete_after_commit");
        let store2 = store.clone();
        let result =
            tokio::spawn(async move { store2.delete_object("bucket", "k").await }).await;
        assert!(result.unwrap_err().is_panic());
        // Deleted for readers immediately; the blob dir lingers until resolved.
        assert!(matches!(
            store.read_object("bucket", "k").await,
            Err(StorageError::ObjectNotFound { .. })
        ));
        let abs = store.layout().bucket_dir("bucket").unwrap().join(&row.blob_dir);
        assert!(abs.exists());
        store.drain_intents("bucket").await.unwrap();
        assert!(!abs.exists());
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn resolver_never_trashes_a_live_blob() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "k", b"data", None, None, false)
            .await
            .unwrap();
        let index = store.index("bucket").await.unwrap();
        let row = index.get("k").await.unwrap().unwrap();
        // Simulate a bogus retire intent pointing at the live dir.
        index
            .commit_delete("k", &row.blob_dir, now_ms())
            .await
            .unwrap();
        // Re-insert the row (as if a concurrent PUT recommitted it).
        let intent = index
            .insert_publish_intent("k", &row.blob_dir, now_ms())
            .await
            .unwrap();
        index.commit_publish(&row, intent, None, now_ms()).await.unwrap();
        store.drain_intents("bucket").await.unwrap();
        // The live blob survived the stale retire intent.
        assert_eq!(read_body(&store, "bucket", "k").await, b"data");
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn concurrent_puts_same_key_last_writer_wins_consistently() {
        let (_tmp, store) = store_and_bucket().await;
        let mut handles = Vec::new();
        for i in 0..8u32 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let body = format!("body-{i}");
                store
                    .put_object("bucket", "k", body.as_bytes(), None, None, false)
                    .await
            }));
        }
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        // Some version is fully readable; exactly one row; everything else retired.
        let body = read_body(&store, "bucket", "k").await;
        assert!(body.starts_with(b"body-"));
        let page = store.list_objects("bucket", "", None, None, 10).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_invariants(&store, "bucket").await;
    }

    // ── version listing ───────────────────────────────────────────────────────

    /// A page reads its own key range only — it must not materialise the bucket.
    #[tokio::test]
    async fn version_page_reads_only_the_requested_key_range() {
        let (_tmp, store) = store_and_bucket().await;
        for i in 0..20 {
            store
                .put_object("bucket", &format!("obj-{i:02}"), b"x", None, None, false)
                .await
                .unwrap();
        }
        let page = store.list_object_versions("bucket", "", "", "", 3).await.unwrap();
        assert_eq!(page.entries.len(), 3, "only one page of versions is read");
        assert!(page.is_truncated);
        assert_eq!(page.next_key_marker.as_deref(), Some("obj-02"));
        assert_eq!(page.next_version_id_marker.as_deref(), Some("null"));

        let resumed = store
            .list_object_versions("bucket", "", "obj-02", "null", 3)
            .await
            .unwrap();
        assert_eq!(
            resumed
                .entries
                .iter()
                .map(|v| v.key.as_str())
                .collect::<Vec<_>>(),
            vec!["obj-03", "obj-04", "obj-05"],
        );
    }

    /// Retired blobs in trash are not versions: they have no addressable id and
    /// would duplicate the live object's `VersionId=null`, so overwrites must
    /// not add rows.
    #[tokio::test]
    async fn version_page_ignores_retired_blobs_in_trash() {
        let (_tmp, store) = store_and_bucket().await;
        // Overwrites retire blobs to trash; only the live version is reportable.
        for (key, writes) in [("a", 3), ("b", 1), ("c", 2)] {
            for n in 0..writes {
                store
                    .put_object("bucket", key, format!("v{n}").as_bytes(), None, None, false)
                    .await
                    .unwrap();
            }
        }

        let mut seen: Vec<String> = Vec::new();
        let mut marker = String::new();
        let mut pages = 0usize;
        loop {
            let page = store
                .list_object_versions("bucket", "", &marker, if marker.is_empty() { "" } else { "null" }, 2)
                .await
                .unwrap();
            pages += 1;
            let keys: Vec<String> = page
                .entries
                .iter()
                .map(|v| v.key.clone())
                .collect();
            assert!(keys.len() <= 2, "a page holds at most max_keys rows");
            for key in &keys {
                assert!(!seen.contains(key), "key {key} reported twice");
            }
            seen.extend(keys);
            if !page.is_truncated {
                break;
            }
            marker = page.next_key_marker.expect("truncated page needs a marker");
            assert!(pages < 6, "pagination did not terminate");
        }

        assert_eq!(seen, vec!["a", "b", "c"], "one live version per key");
        // Every row is the live version, and the trash blobs are still on disk —
        // they are simply not listable.
        let page = store.list_object_versions("bucket", "", "", "", 10).await.unwrap();
        assert!(page.entries.iter().all(|v| v.is_latest));
        let trash = store.layout().trash_dir("bucket").unwrap();
        let mut trash_dirs = tokio::fs::read_dir(&trash).await.unwrap();
        assert!(
            trash_dirs.next_entry().await.unwrap().is_some(),
            "retired blobs must still be recoverable on disk"
        );
    }

    // ── rebuild ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn rebuild_restores_index_from_blob_tree() {
        let (_tmp, store) = store_and_bucket().await;
        for key in ["a/1", "a/2", "b/1"] {
            store
                .put_object("bucket", key, key.as_bytes(), None, None, false)
                .await
                .unwrap();
        }
        // Simulate total index loss.
        let bucket_dir = store.layout().bucket_dir("bucket").unwrap();
        store.index_cache.lock().unwrap().clear();
        let _ = tokio::fs::remove_dir_all(bucket_dir.join("index.rocksdb")).await;
        let count = store.rebuild_index("bucket").await.unwrap();
        assert_eq!(count, 3);
        assert_eq!(read_body(&store, "bucket", "a/1").await, b"a/1");
        let page = store.list_objects("bucket", "", None, None, 10).await.unwrap();
        assert_eq!(page.entries.len(), 3);
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn rebuild_adjudicates_duplicates_newer_wins_and_trashes_loser() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "k", b"new", None, None, false)
            .await
            .unwrap();
        let bucket_dir = store.layout().bucket_dir("bucket").unwrap();
        let row = store.index("bucket").await.unwrap().get("k").await.unwrap().unwrap();
        // Fabricate an older duplicate dir for the same key (an
        // interrupted-overwrite leftover from the old regime).
        let older = Stamp { ms: row.stamp.as_ref().unwrap().ms - 10_000, c: 0, site: store.site_id() };
        let dup_rel = v3::blob_rel("bucket", "k", &older);
        let dup_abs = bucket_dir.join(&dup_rel);
        tokio::fs::create_dir_all(&dup_abs).await.unwrap();
        tokio::fs::write(dup_abs.join("part.1"), b"old").await.unwrap();
        let mut old_meta: ObjectMeta =
            read_json(&bucket_dir.join(&row.blob_dir).join("meta.json")).await.unwrap();
        old_meta.last_modified_ms -= 10_000;
        old_meta.stamp = Some(older.clone());
        old_meta.size = 3;
        old_meta.etag = md5_hex(b"old");
        old_meta.parts[0].size = 3;
        old_meta.parts[0].etag = md5_hex(b"old");
        write_json_atomic(&dup_abs.join("meta.json"), &old_meta).await.unwrap();

        store.index_cache.lock().unwrap().clear();
        let _ = tokio::fs::remove_dir_all(bucket_dir.join("index.rocksdb")).await;
        let count = store.rebuild_index("bucket").await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(read_body(&store, "bucket", "k").await, b"new");
        assert!(!dup_abs.exists(), "older duplicate must be trashed");
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn missing_index_with_blobs_gates_bucket_and_rebuilds() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "k", b"data", None, None, false)
            .await
            .unwrap();
        let bucket_dir = store.layout().bucket_dir("bucket").unwrap();
        store.index_cache.lock().unwrap().clear();
        let _ = tokio::fs::remove_dir_all(bucket_dir.join("index.rocksdb")).await;
        // First access trips the gate and starts a background rebuild.
        let err = store.read_object("bucket", "k").await;
        assert!(matches!(err, Err(StorageError::BucketRebuilding(_))));
        // Wait for the background rebuild to finish.
        for _ in 0..2000 {
            if !store.rebuilding.lock().unwrap().contains("bucket") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(read_body(&store, "bucket", "k").await, b"data");
    }

    #[tokio::test]
    async fn multipart_complete_commits_atomically() {
        let (_tmp, store) = store_and_bucket().await;
        let upload_id = store
            .initiate_multipart("bucket", "big", None, None)
            .await
            .unwrap();
        let part1 = vec![1u8; MIN_MULTIPART_PART_SIZE as usize];
        let e1 = store
            .put_multipart_part("bucket", "big", &upload_id, 1, &part1, false)
            .await
            .unwrap();
        let e2 = store
            .put_multipart_part("bucket", "big", &upload_id, 2, b"tail", false)
            .await
            .unwrap();
        let result = store
            .complete_multipart(
                "bucket",
                "big",
                &upload_id,
                &[
                    CompletePartRequest { number: 1, etag: e1.etag },
                    CompletePartRequest { number: 2, etag: e2.etag },
                ],
            )
            .await
            .unwrap();
        assert_eq!(result.size, MIN_MULTIPART_PART_SIZE + 4);
        let read = store.read_object("bucket", "big").await.unwrap();
        assert_eq!(read.meta.parts.len(), 2);
        assert_invariants(&store, "bucket").await;
    }

    // ── multipart / rebuild / staging regressions ─────────────────────────────

    async fn upload_two_parts(store: &LocalObjectStore, key: &str, tail: &[u8]) -> (String, Vec<CompletePartRequest>) {
        let upload_id = store.initiate_multipart("bucket", key, None, None).await.unwrap();
        let part1 = vec![7u8; MIN_MULTIPART_PART_SIZE as usize];
        let e1 = store
            .put_multipart_part("bucket", key, &upload_id, 1, &part1, false)
            .await
            .unwrap();
        let e2 = store
            .put_multipart_part("bucket", key, &upload_id, 2, tail, false)
            .await
            .unwrap();
        let parts = vec![
            CompletePartRequest { number: 1, etag: e1.etag },
            CompletePartRequest { number: 2, etag: e2.etag },
        ];
        (upload_id, parts)
    }

    async fn crash_complete(store: &LocalObjectStore, key: &str, upload_id: &str, parts: &[CompletePartRequest]) {
        store.arm_crash_point("complete_after_prepare");
        let (store2, key, upload_id, parts) =
            (store.clone(), key.to_string(), upload_id.to_string(), parts.to_vec());
        let result = tokio::spawn(async move {
            store2.complete_multipart("bucket", &key, &upload_id, &parts).await
        })
        .await;
        assert!(result.unwrap_err().is_panic(), "crash point must fire");
        store.disarm_crash_point("complete_after_prepare");
    }

    async fn tail_of(store: &LocalObjectStore, key: &str) -> Vec<u8> {
        let body = read_body(store, "bucket", key).await;
        body[MIN_MULTIPART_PART_SIZE as usize..].to_vec()
    }

    #[tokio::test]
    async fn complete_retry_after_crash_publishes_the_same_parts() {
        let (_tmp, store) = store_and_bucket().await;
        let (upload_id, parts) = upload_two_parts(&store, "k", b"tail").await;
        crash_complete(&store, "k", &upload_id, &parts).await;
        // Parts were moved into object/ by the crashed attempt; a retry must
        // still find and publish them.
        store.complete_multipart("bucket", "k", &upload_id, &parts).await.unwrap();
        assert_eq!(tail_of(&store, "k").await, b"tail");
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn complete_retry_after_reupload_publishes_new_part_bytes() {
        let (_tmp, store) = store_and_bucket().await;
        let (upload_id, mut parts) = upload_two_parts(&store, "k", b"old!").await;
        crash_complete(&store, "k", &upload_id, &parts).await;
        // The client re-uploads part 2 (same length, different bytes) and
        // completes with the new ETag: the new bytes must be what is served,
        // never the stale copy the crashed attempt left in object/.
        let e2 = store
            .put_multipart_part("bucket", "k", &upload_id, 2, b"new!", false)
            .await
            .unwrap();
        parts[1].etag = e2.etag.clone();
        let result = store.complete_multipart("bucket", "k", &upload_id, &parts).await.unwrap();
        assert_eq!(tail_of(&store, "k").await, b"new!");
        let read = store.read_object("bucket", "k").await.unwrap();
        assert_eq!(read.meta.parts[1].etag, e2.etag);
        assert_eq!(read.meta.etag, result.etag);
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn complete_waits_for_in_flight_upload_of_a_requested_part() {
        let (_tmp, store) = store_and_bucket().await;
        let (upload_id, parts) = upload_two_parts(&store, "k", b"tail").await;
        // An UploadPart of part 2 mid-commit holds this lock.
        let held = store.locks.lock("bucket", &part_lock_key(&upload_id, 2)).await;
        let (store2, id2, parts2) = (store.clone(), upload_id.clone(), parts.clone());
        let complete = tokio::spawn(async move {
            store2.complete_multipart("bucket", "k", &id2, &parts2).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!complete.is_finished(), "Complete must not read part 2 while it is being committed");
        drop(held);
        complete.await.unwrap().unwrap();
        assert_eq!(tail_of(&store, "k").await, b"tail");
    }

    #[tokio::test]
    async fn torn_part_upload_never_publishes_new_bytes_under_old_meta() {
        let (_tmp, store) = store_and_bucket().await;
        let (upload_id, parts) = upload_two_parts(&store, "k", b"good").await;
        // Simulate a crash in a re-upload of part 2 after its bytes landed but
        // before its meta was swapped: a new data file with no meta naming it.
        let staging = store.layout().multipart_staging_dir("bucket", &upload_id).unwrap();
        tokio::fs::write(staging.join(format!("part.2.{}", new_staging_id(now_ms()))), b"EVIL")
            .await
            .unwrap();
        store.complete_multipart("bucket", "k", &upload_id, &parts).await.unwrap();
        assert_eq!(tail_of(&store, "k").await, b"good");
    }

    #[tokio::test]
    async fn complete_rejects_part_data_that_disagrees_with_its_meta() {
        let (_tmp, store) = store_and_bucket().await;
        let (upload_id, parts) = upload_two_parts(&store, "k", b"good").await;
        let staging = store.layout().multipart_staging_dir("bucket", &upload_id).unwrap();
        let meta: PartMeta = read_json(&staging.join("part.2.meta.json")).await.unwrap();
        tokio::fs::write(staging.join(&meta.file), b"longer bytes").await.unwrap();
        let err = store.complete_multipart("bucket", "k", &upload_id, &parts).await;
        assert!(matches!(err, Err(StorageError::InvalidMultipartUpload(_))), "{err:?}");
    }

    #[tokio::test]
    async fn rebuild_waits_for_in_flight_index_holders() {
        let (_tmp, store) = store_and_bucket().await;
        store.put_object("bucket", "a", b"a", None, None, false).await.unwrap();
        let held = store.index("bucket").await.unwrap();
        assert!(store.try_begin_rebuild("bucket"));
        let store2 = store.clone();
        let rebuild = tokio::spawn(async move { store2.rebuild_index("bucket").await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!rebuild.is_finished(), "rebuild must not swap the index while it is in use");
        // The in-flight holder can still commit to the (still live) index...
        held.get("a").await.unwrap().unwrap();
        drop(held);
        assert_eq!(rebuild.await.unwrap().unwrap(), 1);
        store.end_rebuild("bucket");
        assert_eq!(read_body(&store, "bucket", "a").await, b"a");
    }

    #[tokio::test]
    async fn delete_bucket_waits_for_in_flight_index_holders() {
        let (_tmp, store) = store_and_bucket().await;
        let held = store.index("bucket").await.unwrap();
        let store2 = store.clone();
        let delete = tokio::spawn(async move { store2.delete_bucket("bucket").await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!delete.is_finished(), "delete must not remove the index while it is in use");
        drop(held);
        delete.await.unwrap().unwrap();
        assert!(!store.bucket_exists("bucket").await);
        // The bucket can be recreated (no lingering RocksDB lock).
        store.create_bucket("bucket").await.unwrap();
        store.put_object("bucket", "x", b"x", None, None, false).await.unwrap();
    }

    #[tokio::test]
    async fn rebuild_does_not_resurrect_object_with_pending_retire() {
        let (_tmp, store) = store_and_bucket().await;
        store.put_object("bucket", "k", b"data", None, None, false).await.unwrap();
        store.arm_crash_point("delete_after_commit");
        let store2 = store.clone();
        let result = tokio::spawn(async move { store2.delete_object("bucket", "k").await }).await;
        assert!(result.unwrap_err().is_panic());
        store.disarm_crash_point("delete_after_commit");
        // The row is gone but the blob is still in the live tree, described
        // by a pending retire intent.
        store.rebuild_index("bucket").await.unwrap();
        assert!(matches!(
            store.read_object("bucket", "k").await,
            Err(StorageError::ObjectNotFound { .. })
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rebuild_aborts_on_unreadable_meta_and_keeps_old_index() {
        use std::os::unix::fs::PermissionsExt;
        let (_tmp, store) = store_and_bucket().await;
        store.put_object("bucket", "k", b"data", None, None, false).await.unwrap();
        let bucket_dir = store.layout().bucket_dir("bucket").unwrap();
        let row = store.index("bucket").await.unwrap().get("k").await.unwrap().unwrap();
        let meta_path = bucket_dir.join(&row.blob_dir).join("meta.json");
        std::fs::set_permissions(&meta_path, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read(&meta_path).is_ok() {
            // Running as root: permissions don't restrict reads.
            std::fs::set_permissions(&meta_path, std::fs::Permissions::from_mode(0o644)).unwrap();
            return;
        }
        let result = store.rebuild_index("bucket").await;
        std::fs::set_permissions(&meta_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(result.is_err(), "an unreadable meta.json must abort the rebuild");
        assert!(meta_path.exists(), "the live blob must not be trashed");
        assert_eq!(read_body(&store, "bucket", "k").await, b"data");
    }

    #[test]
    fn rebuild_rel_paths_use_forward_slashes() {
        let rel: PathBuf = ["objects", "abcd", "V1x_y"].iter().collect();
        assert_eq!(rel_path_string(&rel), "objects/abcd/V1x_y");
    }

    #[tokio::test]
    async fn failed_commit_does_not_leak_staging() {
        let (_tmp, store) = store_and_bucket().await;
        let staging_id = store.stage_put("bucket", "k", b"data", None, None).await.unwrap();
        let staging_dir = store.layout().put_staging_dir("bucket", &staging_id).unwrap();
        tokio::fs::remove_file(staging_dir.join("part.1")).await.unwrap();
        assert!(store.commit_staged_put("bucket", "k", &staging_id, None).await.is_err());
        for _ in 0..200 {
            if !staging_dir.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(!staging_dir.exists(), "failed commit leaked its staging dir");
    }

    #[tokio::test]
    async fn staging_sweep_waits_for_in_flight_complete() {
        let (_tmp, store) = store_and_bucket().await;
        let upload_id = store.initiate_multipart("bucket", "k", None, None).await.unwrap();
        let staging = store.layout().multipart_staging_dir("bucket", &upload_id).unwrap();
        let held = store.lock_multipart_upload("bucket", &upload_id).await;
        let config = super::super::sweeper::SweepConfig {
            multipart_expiry_ms: 1_000,
            ..Default::default()
        };
        let store2 = store.clone();
        let sweep = tokio::spawn(async move {
            super::super::sweeper::delete_staging_bucket(&store2, "bucket", &config, now_ms() + 10_000)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!sweep.is_finished(), "sweep must wait for the upload lock");
        assert!(staging.exists());
        drop(held);
        assert_eq!(sweep.await.unwrap().unwrap(), 1);
        assert!(!staging.exists());
    }

    #[tokio::test]
    async fn concurrent_same_part_upload_stays_consistent() {
        // Two concurrent uploads of the SAME part number must resolve
        // last-writer-wins with the committed part.N and its meta.json always
        // agreeing — never an interleaved byte-mix. Repeat to shake out races.
        let (_tmp, store) = store_and_bucket().await;
        for round in 0..25u32 {
            let upload_id = store
                .initiate_multipart("bucket", "k", None, None)
                .await
                .unwrap();
            // Equal-length, distinct payloads — the dangerous case (a byte-mix
            // would still pass a length check).
            let a = vec![b'A'; 4096];
            let b = vec![b'B'; 4096];
            let (sa, sb) = (store.clone(), store.clone());
            let (ida, idb) = (upload_id.clone(), upload_id.clone());
            let ha = tokio::spawn(async move {
                sa.put_multipart_part("bucket", "k", &ida, 1, &a, false).await
            });
            let hb = tokio::spawn(async move {
                sb.put_multipart_part("bucket", "k", &idb, 1, &b, false).await
            });
            ha.await.unwrap().unwrap();
            hb.await.unwrap().unwrap();

            let staging = store
                .layout()
                .multipart_staging_dir("bucket", &upload_id)
                .unwrap();
            let meta: PartMeta =
                read_json(&staging.join("part.1.meta.json")).await.unwrap();
            let data = tokio::fs::read(staging.join(&meta.file)).await.unwrap();
            let mut hasher = Md5::new();
            hasher.update(&data);
            let data_etag = hex_lower(&hasher.finalize());

            assert!(
                data == vec![b'A'; 4096] || data == vec![b'B'; 4096],
                "round {round}: part.1 is an interleaved mix, not one whole upload"
            );
            assert_eq!(
                meta.size,
                data.len() as u64,
                "round {round}: meta size disagrees with part.1"
            );
            assert_eq!(
                meta.etag, data_etag,
                "round {round}: meta etag does not match part.1 bytes"
            );
            // No stray temp files left behind.
            let mut entries = tokio::fs::read_dir(&staging).await.unwrap();
            while let Some(e) = entries.next_entry().await.unwrap() {
                let name = e.file_name().into_string().unwrap();
                assert!(
                    !name.contains(".tmp."),
                    "round {round}: leftover temp file {name}"
                );
            }
        }
    }

    #[tokio::test]
    async fn copy_object_copies_content_and_metadata() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "src", b"payload", Some("text/plain"), None, false)
            .await
            .unwrap();
        store
            .copy_object("bucket", "src", "bucket", "dst")
            .await
            .unwrap();
        assert_eq!(read_body(&store, "bucket", "dst").await, b"payload");
        let read = store.read_object("bucket", "dst").await.unwrap();
        assert_eq!(read.meta.content_type, "text/plain");
        assert_invariants(&store, "bucket").await;
    }

    #[tokio::test]
    async fn copy_object_to_same_key_does_not_deadlock() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "key", b"payload", None, None, false)
            .await
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            store.copy_object("bucket", "key", "bucket", "key"),
        )
        .await
        .expect("self-copy must release its source guard before publishing")
        .unwrap();
        assert_eq!(read_body(&store, "bucket", "key").await, b"payload");
    }

    #[tokio::test]
    async fn upload_part_copy_streams_inclusive_range() {
        let (_tmp, store) = store_and_bucket().await;
        store
            .put_object("bucket", "src", b"0123456789", None, None, false)
            .await
            .unwrap();
        let upload_id = store
            .initiate_multipart("bucket", "dst", None, None)
            .await
            .unwrap();
        let copied = store
            .copy_multipart_part(
                "bucket",
                "dst",
                &upload_id,
                1,
                "bucket",
                "src",
                Some((2, 6)),
            )
            .await
            .unwrap();
        assert_eq!(copied.size, 5);
        let staging = store.layout.multipart_staging_dir("bucket", &upload_id).unwrap();
        let meta: PartMeta = read_json(&staging.join("part.1.meta.json")).await.unwrap();
        assert_eq!(tokio::fs::read(staging.join(&meta.file)).await.unwrap(), b"23456");
    }

    #[tokio::test]
    async fn streamed_aws_chunked_rejects_size_overflow_without_panicking() {
        let tmp = tempfile::tempdir().unwrap();
        let body = format!("{:x}\r\nx\r\n", usize::MAX);
        let stream = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(body))]);
        let result = write_aws_chunked_stream_with_hashes(&tmp.path().join("part"), stream).await;
        assert!(matches!(result, Err(StorageError::InvalidAwsChunkedBody(_))));
    }
}
