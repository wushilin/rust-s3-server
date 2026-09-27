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
use super::encoding::{
    fanout_segment, object_dir_prefix, object_dir_random_suffix, validate_bucket_name,
    validate_object_key,
};
use super::errors::{Result, StorageError};
use super::index::{
    self, Durability, IntentRecord, ListPage, ObjectRecord, ObjectIndex, INTENT_RETIRE,
};
use super::layout::StorageLayout;
use super::locks::ObjectLockTable;
use super::metadata::{
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
    #[cfg(test)]
    crash_points: Arc<Mutex<HashSet<String>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersionEntry {
    pub meta: ObjectMeta,
    pub version_id: String,
    pub is_latest: bool,
}

/// One page of `ListObjectVersions`, key-ordered — one live version per key,
/// see [`LocalObjectStore::list_object_versions`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectVersionPage {
    pub entries: Vec<ObjectVersionEntry>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
}

/// Returned by a successful PUT or multipart complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutResult {
    pub etag: String,
    pub size: u64,
    pub last_modified_ms: i64,
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
            #[cfg(test)]
            crash_points: Arc::new(Mutex::new(HashSet::new())),
        }
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
                storage_version: "v2".to_string(),
                cors: Vec::new(),
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
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        if self.rebuilding.lock().unwrap().contains(bucket) {
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
        if self.rebuilding.lock().unwrap().contains(bucket) {
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
            Ok(index) => index,
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
            None,
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
        precondition: Option<Precondition>,
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
        self.commit_staged_put(bucket, key, &staging_id, precondition)
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
            format_version: 1,
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
            .publish_prepared_dir(bucket, key, &publish_dir, object_meta, precondition)
            .await;
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
        cleanup.disarm();
        result
    }

    /// The shared commit section for PUT / CopyObject / CompleteMultipart:
    /// intent → fresh-name rename into the live tree → atomic row flip →
    /// retire the displaced dir. `publish_dir` must already contain the
    /// complete blob (parts + meta.json).
    async fn publish_prepared_dir(
        &self,
        bucket: &str,
        key: &str,
        publish_dir: &Path,
        mut object_meta: ObjectMeta,
        precondition: Option<Precondition>,
    ) -> Result<PutResult> {
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        if self.durability == Durability::Full {
            fsync_publish_dir(publish_dir, &object_meta).await?;
        }

        let index = self.index(bucket).await?;
        let mut blob_rel = new_blob_rel(bucket, key);
        let intent_id = index
            .insert_publish_intent(key, &blob_rel, now_ms())
            .await?;
        self.crash_point("publish_after_intent");

        let _guard = self.locks.lock(bucket, key).await;

        let old = index.get(key).await?;

        // Conditional write: evaluate the precondition against the object
        // currently at the key, atomically under the lock we already hold, so
        // the check and the publish cannot race. On failure, abandon the intent
        // and the staged blob and report 412.
        if let Some(precondition) = &precondition {
            if !precondition.is_met(old.as_ref().map(|o| o.etag.as_str())) {
                let _ = index.delete_intent(intent_id).await;
                let _ = tokio::fs::remove_dir_all(publish_dir).await;
                return Err(StorageError::PreconditionFailed {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        }

        // Monotonic per-key clamp: newer-wins adjudication (rebuild, DR)
        // requires timestamps to order versions even across clock steps.
        let last_modified_ms = old
            .as_ref()
            .map(|o| o.last_modified_ms.saturating_add(1))
            .unwrap_or(i64::MIN)
            .max(now_ms());
        if last_modified_ms != object_meta.last_modified_ms {
            object_meta.last_modified_ms = last_modified_ms;
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

        // Rename into the live tree, retrying on ENOENT (an empty-leaf
        // cleaner may race dir creation — the retry recreates it) and
        // choosing a fresh name on collision (never publish into an
        // existing dir).
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
                    blob_rel = new_blob_rel(bucket, key);
                    index.update_intent_blob_dir(intent_id, &blob_rel).await?;
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
        };
        let retire_id = index
            .commit_publish(
                &record,
                intent_id,
                old.as_ref().map(|o| o.blob_dir.as_str()),
                now_ms(),
            )
            .await?;
        self.crash_point("publish_after_commit");

        self.meta_cache.remove(&ObjectCacheKey::new(bucket, key));

        if let (Some(old), Some(retire_id)) = (&old, retire_id) {
            // Relaxed mode: the commit may still sit in an unsynced WAL. Sync
            // it before retiring the displaced blob, or a power loss can roll
            // the row back to a blob dir that is already in trash. (If the
            // sync fails, leave the retire to the resolver.)
            if self.durability == Durability::Relaxed {
                if let Err(err) = index.sync_wal().await {
                    log::warn!("wal sync before retire failed bucket={bucket} key={key} error={err}");
                    return Ok(PutResult {
                        etag: object_meta.etag,
                        size: object_meta.size,
                        last_modified_ms,
                    });
                }
            }
            let old_abs = bucket_dir.join(&old.blob_dir);
            match move_object_dir_to_trash(&self.layout, bucket, &old_abs).await {
                Ok(_) => {
                    let _ = index.delete_intent(retire_id).await;
                }
                Err(err) => {
                    // The retire intent stays; the resolver finishes the job.
                    log::warn!(
                        "retire failed bucket={bucket} key={key} dir={} error={err}",
                        old.blob_dir
                    );
                }
            }
        }

        Ok(PutResult {
            etag: object_meta.etag,
            size: object_meta.size,
            last_modified_ms,
        })
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

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.delete_object_with_size(bucket, key).await.map(|_| ())
    }

    /// Deletes an object and returns its indexed size when it existed. This
    /// avoids an extra metadata read for operation audit logging.
    pub async fn delete_object_with_size(&self, bucket: &str, key: &str) -> Result<Option<u64>> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let index = self.index(bucket).await?;
        let _guard = self.locks.lock(bucket, key).await;
        let Some(row) = index.get(key).await? else {
            return Ok(None); // Idempotent: deleting an absent key succeeds.
        };
        let deleted_size = row.size;
        let retire_id = index.commit_delete(key, &row.blob_dir, now_ms()).await?;
        self.crash_point("delete_after_commit");
        self.meta_cache.remove(&ObjectCacheKey::new(bucket, key));
        if self.durability == Durability::Relaxed {
            // Same as publish: never trash a blob a not-yet-durable commit
            // displaced. On failure the retire intent stays for the resolver.
            if let Err(err) = index.sync_wal().await {
                log::warn!("wal sync before retire failed bucket={bucket} key={key} error={err}");
                return Ok(Some(deleted_size));
            }
        }
        let abs = self.layout.bucket_dir(bucket)?.join(&row.blob_dir);
        match move_object_dir_to_trash(&self.layout, bucket, &abs).await {
            Ok(_) => {
                let _ = index.delete_intent(retire_id).await;
            }
            Err(err) => {
                log::warn!(
                    "retire failed bucket={bucket} key={key} dir={} error={err}",
                    row.blob_dir
                );
            }
        }
        Ok(Some(deleted_size))
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

    /// One page of `ListObjectVersions`, resuming strictly after `key_marker`.
    ///
    /// This server is unversioned, so a key has exactly one version — the live
    /// one — and that is all the listing reports, matching what S3 returns for
    /// an unversioned bucket. Retired blobs sitting in trash are deliberately
    /// omitted: no API can address them (there is no `?versionId` route), they
    /// would all report `VersionId=null` like the live object, and a
    /// version-aware client acting on such a row would hit the live object
    /// instead. Trash is a recovery buffer for operators, not an API surface.
    ///
    /// Only the page's own key range is touched: one bounded index scan of
    /// `max_keys + 1` rows, and blob metadata for those rows alone.
    pub async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: &str,
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
        let index = self.index(bucket).await?;
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        let after = (!key_marker.is_empty()).then_some(key_marker);

        // One extra row tells us whether a further key exists without a second
        // query.
        let live = index.list(prefix, None, after, max_keys + 1).await?;
        let has_more = live.entries.len() > max_keys;

        let mut versions: Vec<ObjectVersionEntry> = Vec::new();
        for row in live.entries.iter().take(max_keys) {
            let dir = bucket_dir.join(&row.blob_dir);
            let Ok(meta) = read_json::<ObjectMeta>(&dir.join("meta.json")).await else {
                log::warn!(
                    "list_object_versions unreadable blob bucket={bucket} key={} dir={}",
                    row.object_key,
                    row.blob_dir
                );
                continue;
            };
            let version_id = dir
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or("null")
                .to_string();
            versions.push(ObjectVersionEntry {
                meta,
                version_id,
                is_latest: true,
            });
        }

        // The index scan is already key-ordered, so the page needs no sorting.
        let mut next_key_marker = versions.last().map(|v| v.meta.object_key.clone());
        if has_more && next_key_marker.is_none() {
            // Every blob in this window was unreadable. Hand back the last key
            // we scanned so the client skips the hole instead of replaying it.
            next_key_marker = live
                .entries
                .get(max_keys - 1)
                .map(|row| row.object_key.clone());
        }

        Ok(ObjectVersionPage {
            // Without a marker to hand back there is no way to make progress,
            // so the response must not claim there is more.
            is_truncated: has_more && next_key_marker.is_some(),
            next_key_marker,
            entries: versions,
        })
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
        })
    }

    pub async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        requested_parts: &[CompletePartRequest],
    ) -> Result<PutResult> {
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
            format_version: 1,
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
            .publish_prepared_dir(bucket, key, &publish_dir, object_meta, None)
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
            src_bucket, src_key, dst_bucket, dst_key, None, None, None, None,
        )
        .await
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
    ) -> Result<PutResult> {
        self.ensure_bucket_and_key(dst_bucket, dst_key).await?;
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
        self.commit_staged_put(dst_bucket, dst_key, &staging_id, None)
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

    /// Relocates one object's blob dir from the legacy 4-level fanout layout to
    /// the current single-level layout via **hardlink → atomic index flip →
    /// unlink** — metadata-only, no object bytes are copied.
    ///
    /// Idempotent: `Ok(false)` if the object is gone or already current.
    /// Safe under concurrent reads (both paths hold the same inodes across the
    /// atomic flip, and `read_object` retries on a vanished path) and crashes
    /// (the tree is self-describing, so a rebuild dedups any orphan left behind).
    /// Serialised with publish/delete of the same key by the per-key lock.
    pub async fn migrate_object_layout(&self, bucket: &str, key: &str) -> Result<bool> {
        validate_bucket_name(bucket)?;
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        let index = self.index(bucket).await?;
        let _guard = self.locks.lock(bucket, key).await;

        let Some(row) = index.get(key).await? else {
            return Ok(false); // gone
        };
        if !is_legacy_layout(&row.blob_dir) {
            return Ok(false); // already on the current layout
        }
        let old_dir = bucket_dir.join(&row.blob_dir);

        // 1. Fresh single-level dir; hardlink every file from the old dir into
        //    it. Retry the random leaf name on the (astronomically rare) clash.
        let (new_rel, new_dir) = loop {
            let new_rel = new_blob_rel(bucket, key);
            let new_dir = bucket_dir.join(&new_rel);
            tokio::fs::create_dir_all(bucket_dir.join(blob_rel_parent(&new_rel))).await?;
            match tokio::fs::create_dir(&new_dir).await {
                Ok(()) => break (new_rel, new_dir),
                Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
            }
        };
        let linked_and_flipped: Result<bool> = async {
            let mut entries = tokio::fs::read_dir(&old_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                // Object leaves hold files only (meta.json + part.N), no subdirs.
                tokio::fs::hard_link(old_dir.join(entry.file_name()), new_dir.join(entry.file_name()))
                    .await?;
            }
            if self.durability == Durability::Full {
                fsync_dir(&new_dir).await?;
            }

            // 2. Atomic pivot: repoint the index row, guarded by the old path so a
            //    concurrent overwrite (should the lock ever not hold) can't be lost.
            index.update_blob_dir(key, &row.blob_dir, &new_rel).await
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
        // Reclaim the now-empty legacy 4-level fanout chain bottom-up. This is
        // race-free: new writes only ever create single-level dirs, so nothing
        // recreates these old paths; a sibling object still under the chain just
        // makes `remove_dir` fail (`DirectoryNotEmpty`) and we stop. The reclaim
        // job doesn't descend, so migration owns cleaning what it vacates.
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

    /// Up to `limit` keys in `bucket` still on the legacy 4-level layout,
    /// strictly after `after`.
    pub async fn legacy_layout_keys(
        &self,
        bucket: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        self.index(bucket).await?.legacy_layout_keys(after, limit).await
    }

    /// Cheap check for whether `bucket` still has any legacy layout structure:
    /// legacy fanout dirs at `objects/` root have 2-char names, the current
    /// layout uses 4-char names. One `read_dir` of the root (early-returns on
    /// the first 2-char entry) — far cheaper than a `LIKE` scan of the index,
    /// and lets migration skip clean buckets without touching SQLite.
    pub async fn has_legacy_layout_dirs(&self, bucket: &str) -> Result<bool> {
        let objects_dir = self.layout.bucket_dir(bucket)?.join("objects");
        let mut entries = match tokio::fs::read_dir(&objects_dir).await {
            Ok(entries) => entries,
            Err(_) => return Ok(false),
        };
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_name().as_encoded_bytes().len() == 2 {
                return Ok(true);
            }
        }
        Ok(false)
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
    /// temporary database that is atomically swapped in. Per key the
    /// highest `last_modified_ms` wins; displaced duplicates and invalid
    /// dirs are trashed on the spot. `trash/` and `staging/` are never
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
        if let Some(old) = old {
            old.close().await;
            // Drop the last handle so RocksDB releases its directory lock
            // before the live directory is replaced.
            drop(old);
        }

        // A RocksDB "database" is a directory, so the temp index is a directory
        // built alongside the live one and swapped in wholesale at the end.
        let tmp_path = bucket_dir.join("index.rebuild.rocksdb");
        let _ = tokio::fs::remove_dir_all(&tmp_path).await;
        let tmp = ObjectIndex::open_at(&tmp_path, Durability::Relaxed).await?;
        tmp.create_schema().await?;

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
            let mut batch: Vec<ObjectRecord> = Vec::with_capacity(self.rebuild_batch_size);
            let mut dirs_by_rel: HashMap<String, PathBuf> = HashMap::new();
            loop {
                let message = entry_rx.recv().await;
                let done = message.is_none();
                let flush = match message {
                    Some(RebuildMessage::Object(record, dir)) => {
                        dirs_by_rel.insert(record.blob_dir.clone(), dir);
                        batch.push(record);
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
    Object(ObjectRecord, PathBuf),
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
            ObjectRecord {
                object_key: meta.object_key.clone(),
                blob_dir: rel_path_string(rel),
                size: meta.size,
                etag: meta.etag.clone(),
                last_modified_ms: meta.last_modified_ms,
            },
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

/// Fresh, unique, opaque blob dir path relative to the bucket dir:
/// `objects/<4hex>/V1XXXXXX_YYYY` — a single 4-char fanout level. Every publish
/// gets its own name (deterministic folder + prefix, random suffix), so old and
/// new versions coexist until the row flip retires the old one. Legacy objects
/// on the old 4-level layout are read via their stored path and migrated by the
/// `migrate_layout` job.
fn new_blob_rel(bucket: &str, key: &str) -> String {
    format!(
        "objects/{}/{}_{}",
        fanout_segment(bucket, key),
        object_dir_prefix(key),
        object_dir_random_suffix()
    )
}

fn blob_rel_parent(rel: &str) -> &str {
    rel.rsplit_once('/').map(|(parent, _)| parent).unwrap_or("")
}

/// A blob dir is on the legacy 4-level layout iff it has more path components
/// than the current single-level `objects/<seg>/<leaf>` (3 components). Used by
/// the layout migration to decide whether a row needs relocating.
fn is_legacy_layout(blob_rel: &str) -> bool {
    blob_rel.split('/').filter(|s| !s.is_empty()).count() > 3
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

        let (tx, mut rx) = tokio::sync::mpsc::channel::<ObjectRecord>(4096);
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
                                .send(ObjectRecord {
                                    object_key: meta.object_key.clone(),
                                    blob_dir: rel,
                                    size: meta.size,
                                    etag: meta.etag.clone(),
                                    last_modified_ms: meta.last_modified_ms,
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
        let mut batch: Vec<ObjectRecord> = Vec::with_capacity(batch_size);
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
        index.update_blob_dir(key, &row.blob_dir, &legacy_rel).await.unwrap();
        legacy_rel
    }

    #[tokio::test]
    async fn migrate_relocates_legacy_object_and_is_idempotent() {
        let (_tmp, store) = store_and_bucket().await;
        let legacy_rel = make_legacy_object(&store, "docs/a", b"hello").await;
        let bucket_dir = store.layout().bucket_dir("bucket").unwrap();
        assert!(is_legacy_layout(&legacy_rel));
        assert_eq!(read_body(&store, "bucket", "docs/a").await, b"hello");
        assert!(tokio::fs::try_exists(bucket_dir.join(&legacy_rel)).await.unwrap());

        assert!(store.migrate_object_layout("bucket", "docs/a").await.unwrap());

        let row = store.index("bucket").await.unwrap().get("docs/a").await.unwrap().unwrap();
        assert!(!is_legacy_layout(&row.blob_dir), "now single-level: {}", row.blob_dir);
        assert_eq!(read_body(&store, "bucket", "docs/a").await, b"hello", "content intact");
        assert!(
            !tokio::fs::try_exists(bucket_dir.join(&legacy_rel)).await.unwrap(),
            "old leaf removed"
        );
        // Idempotent: a second call is a no-op.
        assert!(!store.migrate_object_layout("bucket", "docs/a").await.unwrap());
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
            store.migrate_object_layout("bucket", &format!("k{i}")).await.unwrap();
        }
        stop.cancel();
        reader.await.unwrap();

        // Every object migrated and fully readable afterwards — no loss.
        for i in 0..40 {
            let row = store.index("bucket").await.unwrap().get(&format!("k{i}")).await.unwrap().unwrap();
            assert!(!is_legacy_layout(&row.blob_dir), "all migrated: {}", row.blob_dir);
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
        let page = store.list_object_versions("bucket", "", "", 3).await.unwrap();
        assert_eq!(page.entries.len(), 3, "only one page of versions is read");
        assert!(page.is_truncated);
        assert_eq!(page.next_key_marker.as_deref(), Some("obj-02"));

        let resumed = store
            .list_object_versions("bucket", "", "obj-02", 3)
            .await
            .unwrap();
        assert_eq!(
            resumed
                .entries
                .iter()
                .map(|v| v.meta.object_key.as_str())
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
                .list_object_versions("bucket", "", &marker, 2)
                .await
                .unwrap();
            pages += 1;
            let keys: Vec<String> = page
                .entries
                .iter()
                .map(|v| v.meta.object_key.clone())
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
        let page = store.list_object_versions("bucket", "", "", 10).await.unwrap();
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
        let dup_rel = new_blob_rel("bucket", "k");
        let dup_abs = bucket_dir.join(&dup_rel);
        tokio::fs::create_dir_all(&dup_abs).await.unwrap();
        tokio::fs::write(dup_abs.join("part.1"), b"old").await.unwrap();
        let mut old_meta: ObjectMeta =
            read_json(&bucket_dir.join(&row.blob_dir).join("meta.json")).await.unwrap();
        old_meta.last_modified_ms -= 10_000;
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
        for _ in 0..200 {
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
