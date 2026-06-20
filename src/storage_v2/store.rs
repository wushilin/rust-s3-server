//! Core object-storage implementation.
//!
//! [`LocalObjectStore`] is the main entry point.  All state is persisted on
//! disk; an SQLite index per bucket is used for fast listings.  The store is
//! `Clone + Send + Sync` and can be shared freely across async tasks.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use tokio_util::sync::CancellationToken;

use md5::{Digest, Md5};
use sha2::Sha256;
use tokio::io::AsyncWriteExt;

use super::aws_chunked::decode_aws_chunked;
use super::cache::BoundedLruCache;
use super::encoding::{
    fanout_segments, physical_id_for_key, validate_bucket_name, validate_object_key,
};
use super::errors::{Result, StorageError};
use super::index::{ListPage, ObjectIndexEntry, SqliteObjectIndex};
use super::layout::StorageLayout;
use super::locks::ObjectLockTable;
use super::metadata::{
    content_type_or_default, unquote_etag, BucketMeta, ObjectMeta, ObjectStorageKind, PartMeta,
    PutMeta, UploadMeta,
};
use super::staging::{new_staging_id, validate_staging_id};
use super::time::now_ms;

const SQLITE_REPAIR_CACHE_TTL_MS: i64 = 2 * 60 * 60 * 1000;
const CACHE_SHARDS: usize = 64;
const DEFAULT_META_CACHE_CAPACITY: usize = 200_000;
const DEFAULT_SQLITE_REPAIR_CACHE_CAPACITY: usize = 200_000;
const INDEX_CACHE_WARN_THRESHOLD: usize = 500;

/// Filesystem-backed S3-compatible object store.
///
/// Cheap to clone — all inner state is wrapped in `Arc`.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    layout: StorageLayout,
    sqlite_max_connections: u32,
    locks: ObjectLockTable,
    fanout_locks: Arc<Mutex<HashMap<FanoutLockKey, Arc<RwLock<()>>>>>,
    index_cache: Arc<Mutex<HashMap<String, SqliteObjectIndex>>>,
    meta_cache: Arc<BoundedLruCache<ObjectCacheKey, CachedObjectMeta>>,
    sqlite_repair_cache: Arc<BoundedLruCache<ObjectCacheKey, i64>>,
    /// Tracks which buckets are currently undergoing a SQLite rebuild.
    rebuilding: Arc<Mutex<HashSet<String>>>,
    /// Cooperative shutdown signal — cancel this to stop all background tasks.
    shutdown: CancellationToken,
}

/// Returned by a successful PUT or multipart complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutResult {
    pub etag: String,
    pub size: u64,
    pub last_modified_ms: i64,
}

/// Result of a successful object read.
///
/// Contains only the metadata and the on-disk location; the body is never
/// buffered in memory.  Use [`LocalObjectStore::object_dir`] plus the part
/// file names in `meta.parts` to stream content directly from disk.
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

#[derive(Debug, Clone)]
struct CachedObjectMeta {
    meta: ObjectMeta,
    part_offsets: Vec<u64>,
    meta_modified: SystemTime,
    meta_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FanoutLockKey {
    bucket: String,
    top: String,
}

impl FanoutLockKey {
    fn new(bucket: &str, top: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            top: top.to_string(),
        }
    }
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
        Self::new_with_sqlite_max_connections(root, 50)
    }

    pub fn new_with_sqlite_max_connections(
        root: impl Into<PathBuf>,
        sqlite_max_connections: u32,
    ) -> Self {
        Self::inner(
            StorageLayout::new(root),
            sqlite_max_connections,
            DEFAULT_META_CACHE_CAPACITY,
            DEFAULT_SQLITE_REPAIR_CACHE_CAPACITY,
        )
    }

    pub fn from_storage_config(
        root: impl Into<PathBuf>,
        config: &super::config::StorageConfig,
    ) -> Self {
        Self::inner(
            StorageLayout::new(root),
            config.sqlite_max_connections,
            config.meta_cache_capacity,
            config.sqlite_repair_cache_capacity,
        )
    }

    pub fn with_layout(layout: StorageLayout) -> Self {
        Self::with_layout_and_sqlite_max_connections(layout, 50)
    }

    pub fn with_layout_and_sqlite_max_connections(
        layout: StorageLayout,
        sqlite_max_connections: u32,
    ) -> Self {
        Self::inner(
            layout,
            sqlite_max_connections,
            DEFAULT_META_CACHE_CAPACITY,
            DEFAULT_SQLITE_REPAIR_CACHE_CAPACITY,
        )
    }

    fn inner(
        layout: StorageLayout,
        sqlite_max_connections: u32,
        meta_cache_capacity: usize,
        sqlite_repair_cache_capacity: usize,
    ) -> Self {
        Self {
            layout,
            sqlite_max_connections: sqlite_max_connections.max(1),
            locks: ObjectLockTable::default(),
            fanout_locks: Arc::new(Mutex::new(HashMap::new())),
            index_cache: Arc::new(Mutex::new(HashMap::new())),
            meta_cache: Arc::new(BoundedLruCache::new(meta_cache_capacity, CACHE_SHARDS)),
            sqlite_repair_cache: Arc::new(BoundedLruCache::new(
                sqlite_repair_cache_capacity,
                CACHE_SHARDS,
            )),
            rebuilding: Arc::new(Mutex::new(HashSet::new())),
            shutdown: CancellationToken::new(),
        }
    }

    /// Returns a clone of the shutdown token. Call `.cancel()` on it to stop all background tasks.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub fn layout(&self) -> &StorageLayout {
        &self.layout
    }

    fn fanout_lock(&self, bucket: &str, top: &str) -> Arc<RwLock<()>> {
        let mut locks = self.fanout_locks.lock().unwrap();
        locks
            .entry(FanoutLockKey::new(bucket, top))
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    async fn object_fanout_read_lock(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<OwnedRwLockReadGuard<()>> {
        let [top, _, _, _] = fanout_segments(bucket, key);
        Ok(self.fanout_lock(bucket, &top).read_owned().await)
    }

    pub(crate) async fn fanout_shard_write_lock(
        &self,
        bucket: &str,
        top: &str,
    ) -> OwnedRwLockWriteGuard<()> {
        self.fanout_lock(bucket, top).write_owned().await
    }

    fn invalidate_object_caches(&self, cache_key: &ObjectCacheKey) {
        self.meta_cache.remove(cache_key);
        self.sqlite_repair_cache.remove(cache_key);
    }

    fn mark_sqlite_repaired(&self, cache_key: ObjectCacheKey) {
        self.sqlite_repair_cache.insert(cache_key, now_ms());
    }

    fn sqlite_repair_recently_checked(&self, cache_key: &ObjectCacheKey) -> bool {
        let Some(checked_at_ms) = self.sqlite_repair_cache.get(cache_key) else {
            return false;
        };
        if now_ms().saturating_sub(checked_at_ms) <= SQLITE_REPAIR_CACHE_TTL_MS {
            return true;
        }
        self.sqlite_repair_cache.remove(cache_key);
        false
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        tokio::fs::create_dir_all(&bucket_dir).await?;
        let bucket_meta_path = self.layout.bucket_meta_path(bucket)?;
        if !bucket_meta_path.exists() {
            let meta = BucketMeta {
                created_at_ms: now_ms(),
            };
            write_json_atomic(&bucket_meta_path, &meta).await?;
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
        if !self.index(bucket).await?.is_empty().await? || has_active_staging(&bucket_dir).await? {
            return Err(StorageError::InvalidMultipartUpload(
                "bucket is not empty".to_string(),
            ));
        }
        tokio::fs::remove_dir_all(bucket_dir).await?;
        self.index_cache.lock().unwrap().remove(bucket);
        Ok(())
    }

    pub async fn index(&self, bucket: &str) -> Result<SqliteObjectIndex> {
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        if let Some(index) = self.index_cache.lock().unwrap().get(bucket).cloned() {
            return Ok(index);
        }
        let index =
            SqliteObjectIndex::open_with_max_connections(&bucket_dir, self.sqlite_max_connections)
                .await?;
        let mut cache = self.index_cache.lock().unwrap();
        cache.insert(bucket.to_string(), index.clone());
        if cache.len() > INDEX_CACHE_WARN_THRESHOLD {
            log::warn!(
                "index_cache has {} entries — consider reducing bucket count or adding eviction",
                cache.len()
            );
        }
        Ok(index)
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        content_type: Option<&str>,
        aws_chunked: bool,
    ) -> Result<PutResult> {
        let payload = if aws_chunked {
            decode_aws_chunked(bytes)?
        } else {
            bytes.to_vec()
        };
        let staging_id = self.stage_put(bucket, key, &payload, content_type).await?;
        self.commit_staged_put(bucket, key, &staging_id).await
    }

    pub async fn put_object_stream<S, E>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_type: Option<&str>,
        aws_chunked: bool,
        expected_sha256: Option<&str>,
    ) -> Result<PutResult>
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let staging_id = if aws_chunked {
            self.stage_put_aws_chunked_stream(bucket, key, stream, content_type)
                .await?
        } else {
            self.stage_put_stream(bucket, key, stream, content_type, expected_sha256)
                .await?
        };
        self.commit_staged_put(bucket, key, &staging_id).await
    }

    pub async fn stage_put(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        content_type: Option<&str>,
    ) -> Result<String> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let staging_id = new_staging_id(now_ms());
        let staging_dir = self.layout.put_staging_dir(bucket, &staging_id)?;
        tokio::fs::create_dir_all(&staging_dir).await?;
        let part_path = staging_dir.join("part.1");
        let etag = write_file_with_md5(&part_path, bytes).await?;
        let meta = PutMeta {
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            physical_id: physical_id_for_key(key, 255)?,
            write_id: staging_id.clone(),
            initiated_at_ms: now_ms(),
            size: bytes.len() as u64,
            etag,
            content_type: content_type_or_default(content_type),
        };
        write_json_atomic(&staging_dir.join("put.json"), &meta).await?;
        Ok(staging_id)
    }

    pub async fn stage_put_stream<S, E>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_type: Option<&str>,
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
            physical_id: physical_id_for_key(key, 255)?,
            write_id: staging_id.clone(),
            initiated_at_ms: now_ms(),
            size: written.size,
            etag: written.md5,
            content_type: content_type_or_default(content_type),
        };
        write_json_atomic(&staging_dir.join("put.json"), &meta).await?;
        Ok(staging_id)
    }

    pub async fn stage_put_aws_chunked_stream<S, E>(
        &self,
        bucket: &str,
        key: &str,
        stream: S,
        content_type: Option<&str>,
    ) -> Result<String>
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        self.ensure_bucket_and_key(bucket, key).await?;
        let staging_id = new_staging_id(now_ms());
        let staging_dir = self.layout.put_staging_dir(bucket, &staging_id)?;
        tokio::fs::create_dir_all(&staging_dir).await?;
        let part_path = staging_dir.join("part.1");
        let written = match write_aws_chunked_stream_with_hashes(&part_path, stream).await {
            Ok(written) => written,
            Err(err) => {
                let _ = tokio::fs::remove_dir_all(&staging_dir).await;
                return Err(err);
            }
        };
        let meta = PutMeta {
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            physical_id: physical_id_for_key(key, 255)?,
            write_id: staging_id.clone(),
            initiated_at_ms: now_ms(),
            size: written.size,
            etag: written.md5,
            content_type: content_type_or_default(content_type),
        };
        write_json_atomic(&staging_dir.join("put.json"), &meta).await?;
        Ok(staging_id)
    }

    pub async fn commit_staged_put(
        &self,
        bucket: &str,
        key: &str,
        staging_id: &str,
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
        let staged_part = staging_dir.join("part.1");
        ensure_file_exists(&staged_part).await?;

        let _guard = self.locks.lock(bucket, key).await;
        let _fanout_guard = self.object_fanout_read_lock(bucket, key).await?;
        let object_path = self.layout.object_path(bucket, key)?;
        let index = self.index(bucket).await?;
        let cache_key = ObjectCacheKey::new(bucket, key);
        self.invalidate_object_caches(&cache_key);

        let _ = tokio::fs::remove_file(&object_path.meta_path).await;
        index.delete(key).await?;
        tokio::fs::create_dir_all(&object_path.object_dir).await?;
        let final_part = object_path.object_dir.join("part.1");
        tokio::fs::rename(&staged_part, &final_part).await?;

        let last_modified_ms = now_ms();
        index
            .put(&ObjectIndexEntry {
                object_key: key.to_string(),
                physical_id: object_path.physical_id.clone(),
                size: put_meta.size,
                etag: put_meta.etag.clone(),
                last_modified_ms,
            })
            .await?;

        let object_meta = ObjectMeta {
            format_version: 1,
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            physical_id: object_path.physical_id,
            storage: ObjectStorageKind::Single,
            size: put_meta.size,
            etag: put_meta.etag.clone(),
            last_modified_ms,
            content_type: put_meta.content_type,
            parts: vec![PartMeta {
                number: 1,
                file: "part.1".to_string(),
                size: put_meta.size,
                etag: put_meta.etag.clone(),
            }],
        };
        write_json_atomic(&object_path.meta_path, &object_meta).await?;
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
        Ok(PutResult {
            etag: put_meta.etag,
            size: put_meta.size,
            last_modified_ms,
        })
    }

    /// Reads object metadata from disk.  The part files are **not** loaded into
    /// memory; the caller streams them via [`ReadObject::object_dir`].
    ///
    /// If `meta.json` is missing the orphaned SQLite entry and physical
    /// directory are cleaned up and `ObjectNotFound` is returned.  If the
    /// SQLite index is missing an entry for an otherwise healthy object it is
    /// repaired transparently.
    pub async fn read_object(&self, bucket: &str, key: &str) -> Result<ReadObject> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let object_path = self.layout.object_path(bucket, key)?;
        let cache_key = ObjectCacheKey::new(bucket, key);
        let meta_file = match tokio::fs::metadata(&object_path.meta_path).await {
            Ok(meta_file) if meta_file.is_file() => meta_file,
            _ => {
                self.invalidate_object_caches(&cache_key);
                // Proactively clean up any orphaned SQLite entry and physical directory.
                if let Ok(index) = self.index(bucket).await {
                    let _ = index.delete(key).await;
                }
                let _ = tokio::fs::remove_dir_all(&object_path.object_dir).await;
                return Err(StorageError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };
        let meta_modified = meta_file.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let meta_len = meta_file.len();
        let cached = self
            .meta_cache
            .get(&cache_key)
            .filter(|entry| entry.meta_modified == meta_modified && entry.meta_len == meta_len);
        let cached = match cached {
            Some(cached) => cached,
            None => {
                let meta: ObjectMeta = read_json(&object_path.meta_path).await?;
                validate_object_parts(&object_path.object_dir, &meta).await?;
                let cached = CachedObjectMeta {
                    part_offsets: part_offsets(&meta),
                    meta,
                    meta_modified,
                    meta_len,
                };
                self.meta_cache.insert(cache_key.clone(), cached.clone());
                cached
            }
        };

        // Repair SQLite if a row is missing (e.g. after a crash during commit).
        if !self.sqlite_repair_recently_checked(&cache_key) {
            let index = self.index(bucket).await?;
            if index.get(key).await?.is_none() {
                index
                    .put(&ObjectIndexEntry {
                        object_key: key.to_string(),
                        physical_id: cached.meta.physical_id.clone(),
                        size: cached.meta.size,
                        etag: cached.meta.etag.clone(),
                        last_modified_ms: cached.meta.last_modified_ms,
                    })
                    .await?;
                log::info!(
                    "sqlite repair fixed missing index row bucket={} key={} physical_id={} size={} etag={}",
                    bucket,
                    key,
                    cached.meta.physical_id,
                    cached.meta.size,
                    cached.meta.etag,
                );
            }
            self.mark_sqlite_repaired(cache_key.clone());
        }
        Ok(ReadObject {
            meta: cached.meta,
            part_offsets: cached.part_offsets,
            object_dir: object_path.object_dir,
        })
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let _guard = self.locks.lock(bucket, key).await;
        let object_path = self.layout.object_path(bucket, key)?;
        let cache_key = ObjectCacheKey::new(bucket, key);
        self.invalidate_object_caches(&cache_key);
        // Remove meta.json first — this makes the object invisible atomically.
        let _ = tokio::fs::remove_file(&object_path.meta_path).await;
        self.index(bucket).await?.delete(key).await?;
        // Clean up the physical directory immediately; sweeper handles any crash leftovers.
        let _ = tokio::fs::remove_dir_all(&object_path.object_dir).await;
        Ok(())
    }

    /// Spawns a background rebuild task for `bucket`.
    /// Returns `false` (and does nothing) if a rebuild for that bucket is already running.
    pub fn start_rebuild_background(&self, bucket: &str) -> bool {
        let mut guard = self.rebuilding.lock().unwrap();
        if guard.contains(bucket) {
            log::info!("rebuild_sqlite trigger ignored bucket={bucket} reason=already_running");
            return false;
        }
        guard.insert(bucket.to_string());
        drop(guard);

        let store = self.clone();
        let bucket = bucket.to_string();
        tokio::spawn(async move {
            log::info!("rebuild_sqlite background task started bucket={bucket}");
            match store.rebuild_sqlite(&bucket).await {
                Ok(count) => log::info!("rebuild_sqlite complete bucket={bucket} repaired={count}"),
                Err(err) => log::error!("rebuild_sqlite failed bucket={bucket} error={err}"),
            }
            store.rebuilding.lock().unwrap().remove(&bucket);
            log::info!("rebuild_sqlite background task stopped bucket={bucket}");
        });
        true
    }

    pub async fn start_missing_index_rebuilds(&self) -> Result<usize> {
        let buckets = self.list_buckets().await?;
        let mut started = 0usize;
        for (bucket, _) in buckets {
            let index_path = self.layout.bucket_dir(&bucket)?.join("index.sqlite");
            if index_path.exists() {
                continue;
            }
            log::warn!(
                "rebuild_sqlite auto trigger bucket={} reason=missing_index path={}",
                bucket,
                index_path.display()
            );
            if self.start_rebuild_background(&bucket) {
                started += 1;
            }
        }
        log::info!("rebuild_sqlite auto trigger scan complete started={started}");
        Ok(started)
    }

    pub async fn rebuild_sqlite(&self, bucket: &str) -> Result<usize> {
        validate_bucket_name(bucket)?;
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        let bucket_dir = self.layout.bucket_dir(bucket)?;
        let objects_dir = bucket_dir.join("objects");
        let index = self.index(bucket).await?;
        let mut entries = Vec::new();
        log::info!("rebuild_sqlite scan started bucket={bucket}");
        Box::pin(rebuild_walk(
            bucket,
            &objects_dir,
            &mut entries,
            &self.shutdown,
        ))
        .await?;
        let count = entries.len();
        log::info!("rebuild_sqlite applying index bucket={bucket} objects={count}");
        index.replace_all(&entries).await?;
        log::info!("rebuild_sqlite applied index bucket={bucket} objects={count}");
        Ok(count)
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        max_keys: usize,
    ) -> Result<ListPage> {
        validate_bucket_name(bucket)?;
        validate_object_key(prefix, 255).or_else(|err| {
            if prefix.is_empty() {
                Ok(())
            } else {
                Err(err)
            }
        })?;
        self.index(bucket)
            .await?
            .list(prefix, delimiter, after, max_keys)
            .await
    }

    pub async fn initiate_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> Result<String> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let upload_id = new_staging_id(now_ms());
        let staging_dir = self.layout.multipart_staging_dir(bucket, &upload_id)?;
        tokio::fs::create_dir_all(&staging_dir).await?;
        let upload = UploadMeta {
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            upload_id: upload_id.clone(),
            initiated_at_ms: now_ms(),
            physical_id: physical_id_for_key(key, 255)?,
            content_type: content_type_or_default(content_type),
        };
        write_json_atomic(&staging_dir.join("upload.json"), &upload).await?;
        Ok(upload_id)
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
        let payload = if aws_chunked {
            decode_aws_chunked(bytes)?
        } else {
            bytes.to_vec()
        };
        let staging_dir = self.layout.multipart_staging_dir(bucket, upload_id)?;
        let file_name = format!("part.{part_number}");
        let etag = write_file_with_md5(&staging_dir.join(&file_name), &payload).await?;
        let part = PartMeta {
            number: part_number,
            file: file_name,
            size: payload.len() as u64,
            etag: etag.clone(),
        };
        write_json_atomic(
            &staging_dir.join(format!("part.{part_number}.meta.json")),
            &part,
        )
        .await?;
        Ok(PutResult {
            etag,
            size: payload.len() as u64,
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
        let file_name = format!("part.{part_number}");
        let part_path = staging_dir.join(&file_name);
        let written = if aws_chunked {
            write_aws_chunked_stream_with_hashes(&part_path, stream).await?
        } else {
            write_stream_with_hashes(&part_path, stream).await?
        };
        if let Some(expected) = expected_sha256 {
            if !expected.eq_ignore_ascii_case(&written.sha256) {
                let _ = tokio::fs::remove_file(&part_path).await;
                return Err(StorageError::PayloadHashMismatch {
                    expected: expected.to_string(),
                    actual: written.sha256,
                });
            }
        }
        let part = PartMeta {
            number: part_number,
            file: file_name,
            size: written.size,
            etag: written.md5.clone(),
        };
        write_json_atomic(
            &staging_dir.join(format!("part.{part_number}.meta.json")),
            &part,
        )
        .await?;
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
        let upload = self.validate_upload(bucket, key, upload_id).await?;
        if requested_parts.is_empty() {
            return Err(StorageError::InvalidMultipartUpload(
                "complete request has no parts".to_string(),
            ));
        }
        let staging_dir = self.layout.multipart_staging_dir(bucket, upload_id)?;
        let mut parts = Vec::with_capacity(requested_parts.len());
        let mut previous = 0;
        for requested in requested_parts {
            if requested.number <= previous {
                return Err(StorageError::InvalidMultipartUpload(
                    "parts must be in ascending order".to_string(),
                ));
            }
            previous = requested.number;
            let part_meta: PartMeta =
                read_json(&staging_dir.join(format!("part.{}.meta.json", requested.number)))
                    .await?;
            if part_meta.etag != unquote_etag(&requested.etag) {
                return Err(StorageError::InvalidMultipartUpload(format!(
                    "etag mismatch for part {}",
                    requested.number
                )));
            }
            ensure_file_exists(&staging_dir.join(&part_meta.file)).await?;
            parts.push(part_meta);
        }

        let _guard = self.locks.lock(bucket, key).await;
        let _fanout_guard = self.object_fanout_read_lock(bucket, key).await?;
        let object_path = self.layout.object_path(bucket, key)?;
        let index = self.index(bucket).await?;
        let cache_key = ObjectCacheKey::new(bucket, key);
        self.invalidate_object_caches(&cache_key);
        let _ = tokio::fs::remove_file(&object_path.meta_path).await;
        index.delete(key).await?;
        tokio::fs::create_dir_all(&object_path.object_dir).await?;
        for part in &parts {
            tokio::fs::rename(
                staging_dir.join(&part.file),
                object_path.object_dir.join(&part.file),
            )
            .await?;
        }

        let size = parts.iter().map(|p| p.size).sum();
        let etag = multipart_etag(&parts)?;
        let last_modified_ms = now_ms();
        index
            .put(&ObjectIndexEntry {
                object_key: key.to_string(),
                physical_id: object_path.physical_id.clone(),
                size,
                etag: etag.clone(),
                last_modified_ms,
            })
            .await?;
        let object_meta = ObjectMeta {
            format_version: 1,
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            physical_id: upload.physical_id,
            storage: ObjectStorageKind::Multipart,
            size,
            etag: etag.clone(),
            last_modified_ms,
            content_type: upload.content_type,
            parts,
        };
        write_json_atomic(&object_path.meta_path, &object_meta).await?;
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
        Ok(PutResult {
            etag,
            size,
            last_modified_ms,
        })
    }

    pub async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
        self.validate_upload(bucket, key, upload_id).await?;
        let _guard = self.locks.lock(bucket, key).await;
        let staging_dir = self.layout.multipart_staging_dir(bucket, upload_id)?;
        tokio::fs::remove_dir_all(&staging_dir)
            .await
            .map_err(|_| StorageError::NoSuchUpload(upload_id.to_string()))?;
        Ok(())
    }

    async fn ensure_bucket_and_key(&self, bucket: &str, key: &str) -> Result<()> {
        validate_bucket_name(bucket)?;
        validate_object_key(key, 255)?;
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
        validate_staging_id(upload_id)?;
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

async fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = tokio::fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
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
    Ok(format!("{:x}", hasher.finalize()))
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
        md5: format!("{:x}", md5.finalize()),
        sha256: format!("{:x}", sha256.finalize()),
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
    let mut buffer = Vec::<u8>::new();
    let mut saw_final_chunk = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|err| StorageError::Io(err.to_string()))?;
        buffer.extend_from_slice(&chunk);

        loop {
            let Some(header_end) = find_bytes(&buffer, b"\r\n") else {
                break;
            };
            let header = std::str::from_utf8(&buffer[..header_end]).map_err(|_| {
                StorageError::InvalidAwsChunkedBody("invalid chunk header utf8".into())
            })?;
            let size_hex = header.split(';').next().unwrap_or("");
            let chunk_size = usize::from_str_radix(size_hex, 16).map_err(|_| {
                StorageError::InvalidAwsChunkedBody(format!("invalid chunk size {size_hex}"))
            })?;
            let data_start = header_end + 2;

            if chunk_size == 0 {
                let trailer = &buffer[data_start..];
                if trailer == b"\r\n" || find_bytes(trailer, b"\r\n\r\n").is_some() {
                    saw_final_chunk = true;
                    buffer.clear();
                    break;
                }
                break;
            }

            let data_end = data_start + chunk_size;
            if buffer.len() < data_end + 2 {
                break;
            }
            if &buffer[data_end..data_end + 2] != b"\r\n" {
                return Err(StorageError::InvalidAwsChunkedBody(
                    "chunk data missing trailing CRLF".into(),
                ));
            }
            let data = &buffer[data_start..data_end];
            size += data.len() as u64;
            md5.update(data);
            sha256.update(data);
            file.write_all(data).await?;
            buffer.drain(..data_end + 2);
        }

        if saw_final_chunk {
            break;
        }
    }

    if !saw_final_chunk {
        return Err(StorageError::InvalidAwsChunkedBody(
            "missing final chunk".to_string(),
        ));
    }

    file.flush().await?;
    Ok(WrittenHashes {
        size,
        md5: format!("{:x}", md5.finalize()),
        sha256: format!("{:x}", sha256.finalize()),
    })
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
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

async fn rebuild_walk(
    bucket: &str,
    dir: &Path,
    entries_out: &mut Vec<ObjectIndexEntry>,
    shutdown: &CancellationToken,
) -> Result<()> {
    if shutdown.is_cancelled() {
        log::info!(
            "rebuild_sqlite cancelled before scanning dir={}",
            dir.display()
        );
        return Err(StorageError::Io("rebuild cancelled".to_string()));
    }
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        log::info!("rebuild_sqlite scan skipped missing dir={}", dir.display());
        return Ok(());
    };
    let mut subdirs = Vec::new();
    let mut has_meta = false;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.file_name().and_then(|n| n.to_str()) == Some("meta.json") {
            has_meta = true;
        } else if path.is_dir() {
            subdirs.push(path);
        }
    }
    if has_meta {
        if let Ok(meta) = read_json::<ObjectMeta>(&dir.join("meta.json")).await {
            // Guard against misplaced meta.json files: the leaf directory name must
            // match the physical_id recorded inside the file.
            let dir_name = dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if meta.physical_id != dir_name {
                log::warn!(
                    "rebuild_sqlite: skipping {}: physical_id {:?} != directory {:?}",
                    dir.display(),
                    meta.physical_id,
                    dir_name,
                );
            } else {
                entries_out.push(ObjectIndexEntry {
                    object_key: meta.object_key.clone(),
                    physical_id: meta.physical_id,
                    size: meta.size,
                    etag: meta.etag,
                    last_modified_ms: meta.last_modified_ms,
                });
                if let Some(entry) = entries_out.last() {
                    log::info!(
                        "rebuild_sqlite discovered object bucket={} key={} physical_id={} size={} etag={}",
                        bucket,
                        entry.object_key,
                        entry.physical_id,
                        entry.size,
                        entry.etag,
                    );
                }
                if entries_out.len() % 100 == 0 {
                    log::info!(
                        "rebuild_sqlite progress bucket={} objects_found={}",
                        bucket,
                        entries_out.len()
                    );
                    tokio::task::yield_now().await;
                    if shutdown.is_cancelled() {
                        log::info!(
                            "rebuild_sqlite cancelled bucket={} objects_found={}",
                            bucket,
                            entries_out.len()
                        );
                        return Err(StorageError::Io("rebuild cancelled".to_string()));
                    }
                }
            }
        } else {
            log::warn!("rebuild_sqlite failed to read meta dir={}", dir.display());
        }
    } else {
        for subdir in subdirs {
            Box::pin(rebuild_walk(bucket, &subdir, entries_out, shutdown)).await?;
        }
    }
    Ok(())
}

fn md5_hex(bytes: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
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

    #[tokio::test]
    async fn put_writes_sqlite_before_visible_meta_and_read_repairs_missing_index() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        let result = store
            .put_object(
                "bucket",
                "doris/1722372774777722231",
                b"hello",
                Some("text/plain"),
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.size, 5);
        let object_path = store
            .layout()
            .object_path("bucket", "doris/1722372774777722231")
            .unwrap();
        assert!(object_path.meta_path.exists());
        assert!(object_path.object_dir.join("part.1").exists());

        let index = store.index("bucket").await.unwrap();
        index.delete("doris/1722372774777722231").await.unwrap();
        assert!(index
            .get("doris/1722372774777722231")
            .await
            .unwrap()
            .is_none());
        let read = store
            .read_object("bucket", "doris/1722372774777722231")
            .await
            .unwrap();
        let bytes = tokio::fs::read(read.object_dir.join("part.1"))
            .await
            .unwrap();
        assert_eq!(bytes, b"hello");
        assert!(index
            .get("doris/1722372774777722231")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn index_pool_is_reused_per_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new_with_sqlite_max_connections(tmp.path(), 7);
        store.create_bucket("bucket").await.unwrap();

        let _ = store.index("bucket").await.unwrap();
        let _ = store.index("bucket").await.unwrap();
        assert_eq!(store.index_cache.lock().unwrap().len(), 1);
    }

    #[test]
    fn sqlite_repair_cooldown_has_absolute_ttl() {
        let store = LocalObjectStore::new("/tmp/rusts3-test-unused");
        let key = ObjectCacheKey::new("bucket", "object");

        store.mark_sqlite_repaired(key.clone());
        assert!(store.sqlite_repair_recently_checked(&key));

        store
            .sqlite_repair_cache
            .insert(key.clone(), now_ms() - SQLITE_REPAIR_CACHE_TTL_MS - 1);
        assert!(!store.sqlite_repair_recently_checked(&key));
        assert!(store.sqlite_repair_cache.get(&key).is_none());
    }

    #[tokio::test]
    async fn sqlite_row_without_meta_is_treated_as_orphan_not_visible_object() {
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
        assert!(matches!(
            store.read_object("bucket", "key").await,
            Err(StorageError::ObjectNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn delete_removes_meta_sqlite_and_object_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "key", b"hello", None, false)
            .await
            .unwrap();
        let object_path = store.layout().object_path("bucket", "key").unwrap();
        assert!(object_path.meta_path.exists());
        store.delete_object("bucket", "key").await.unwrap();
        assert!(!object_path.meta_path.exists());
        assert!(!object_path.object_dir.exists());
        assert!(store
            .index("bucket")
            .await
            .unwrap()
            .get("key")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn read_missing_meta_cleans_up_sqlite_and_object_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "key", b"hello", None, false)
            .await
            .unwrap();
        let object_path = store.layout().object_path("bucket", "key").unwrap();
        // Simulate crash after write but meta.json was removed externally
        tokio::fs::remove_file(&object_path.meta_path)
            .await
            .unwrap();
        assert!(object_path.object_dir.exists());

        let result = store.read_object("bucket", "key").await;
        assert!(matches!(result, Err(StorageError::ObjectNotFound { .. })));
        // Both orphan SQLite entry and physical dir must be gone
        assert!(store
            .index("bucket")
            .await
            .unwrap()
            .get("key")
            .await
            .unwrap()
            .is_none());
        assert!(!object_path.object_dir.exists());
    }

    #[tokio::test]
    async fn abort_multipart_leaves_no_sqlite_or_object_trace() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        let upload_id = store
            .initiate_multipart("bucket", "large.bin", None)
            .await
            .unwrap();
        store
            .put_multipart_part("bucket", "large.bin", &upload_id, 1, b"data", false)
            .await
            .unwrap();
        store
            .abort_multipart("bucket", "large.bin", &upload_id)
            .await
            .unwrap();

        // No SQLite entry
        assert!(store
            .index("bucket")
            .await
            .unwrap()
            .get("large.bin")
            .await
            .unwrap()
            .is_none());
        // No object meta
        let object_path = store.layout().object_path("bucket", "large.bin").unwrap();
        assert!(!object_path.meta_path.exists());
        // Staging dir cleaned up by abort
        let staging_dir = store
            .layout()
            .multipart_staging_dir("bucket", &upload_id)
            .unwrap();
        assert!(!staging_dir.exists());
    }

    #[tokio::test]
    async fn rebuild_sqlite_restores_index_from_meta_json() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "a/1", b"foo", None, false)
            .await
            .unwrap();
        store
            .put_object("bucket", "a/2", b"bar", None, false)
            .await
            .unwrap();
        store
            .index("bucket")
            .await
            .unwrap()
            .clear_all()
            .await
            .unwrap();
        assert!(store
            .index("bucket")
            .await
            .unwrap()
            .is_empty()
            .await
            .unwrap());

        let count = store.rebuild_sqlite("bucket").await.unwrap();
        assert_eq!(count, 2);
        assert!(store
            .index("bucket")
            .await
            .unwrap()
            .get("a/1")
            .await
            .unwrap()
            .is_some());
        assert!(store
            .index("bucket")
            .await
            .unwrap()
            .get("a/2")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn missing_sqlite_file_on_restart_triggers_background_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let store = LocalObjectStore::new(tmp.path());
            store.create_bucket("bucket").await.unwrap();
            store
                .put_object("bucket", "a/1", b"foo", None, false)
                .await
                .unwrap();
        }

        let index_path = tmp
            .path()
            .join("buckets")
            .join("bucket")
            .join("index.sqlite");
        tokio::fs::remove_file(&index_path).await.unwrap();
        assert!(!index_path.exists());

        let restarted = LocalObjectStore::new(tmp.path());
        assert_eq!(restarted.start_missing_index_rebuilds().await.unwrap(), 1);

        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        loop {
            let restored = restarted
                .index("bucket")
                .await
                .unwrap()
                .get("a/1")
                .await
                .unwrap()
                .is_some();
            if restored {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "missing index was not rebuilt"
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn cancelled_rebuild_keeps_existing_index() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "a/1", b"foo", None, false)
            .await
            .unwrap();
        let index = store.index("bucket").await.unwrap();
        assert!(index.get("a/1").await.unwrap().is_some());

        store.shutdown_token().cancel();
        assert!(store.rebuild_sqlite("bucket").await.is_err());
        assert!(index.get("a/1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn multipart_stores_ordered_parts_without_composing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        let upload_id = store
            .initiate_multipart("bucket", "large.bin", None)
            .await
            .unwrap();
        let p1 = store
            .put_multipart_part("bucket", "large.bin", &upload_id, 1, b"abc", false)
            .await
            .unwrap();
        let p3 = store
            .put_multipart_part("bucket", "large.bin", &upload_id, 3, b"defg", false)
            .await
            .unwrap();
        let completed = store
            .complete_multipart(
                "bucket",
                "large.bin",
                &upload_id,
                &[
                    CompletePartRequest {
                        number: 1,
                        etag: p1.etag,
                    },
                    CompletePartRequest {
                        number: 3,
                        etag: p3.etag,
                    },
                ],
            )
            .await
            .unwrap();
        assert_eq!(completed.size, 7);
        let read = store.read_object("bucket", "large.bin").await.unwrap();
        // Read bytes directly from the part files on disk.
        let mut bytes = Vec::new();
        for part in &read.meta.parts {
            bytes.extend(
                tokio::fs::read(read.object_dir.join(&part.file))
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(bytes, b"abcdefg");
        assert_eq!(read.meta.parts.len(), 2);
        assert_eq!(read.meta.parts[1].number, 3);
    }
}
