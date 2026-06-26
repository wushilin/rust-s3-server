//! Core object-storage implementation.
//!
//! [`LocalObjectStore`] is the main entry point.  All state is persisted on
//! disk; an SQLite index per bucket is used for fast listings.  The store is
//! `Clone + Send + Sync` and can be shared freely across async tasks.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio_util::sync::CancellationToken;

use md5::{Digest, Md5};
use sha2::Sha256;
use tokio::io::AsyncWriteExt;

use super::aws_chunked::decode_aws_chunked;
use super::cache::BoundedLruCache;
use super::encoding::{
    fanout_segments, is_object_dir_name, object_dir_prefix, validate_bucket_name,
    validate_object_key,
};
use super::errors::{Result, StorageError};
use super::index::{ListPage, ObjectIndexEntry, SqliteObjectIndex};
use super::layout::StorageLayout;
use super::locks::ObjectLockTable;
use super::metadata::{
    content_encoding_or_none, content_language_or_none, content_type_or_default,
    storage_class_or_default, unquote_etag, BucketMeta, ObjectMeta, ObjectStorageKind, PartMeta,
    PutMeta, UploadMeta,
};
use super::resolver::{allocate_object_dir, resolve_object_dir};
use super::staging::{new_staging_id, validate_staging_id};
use super::time::now_ms;

const SQLITE_REPAIR_CACHE_TTL_MS: i64 = 2 * 60 * 60 * 1000;
const CACHE_SHARDS: usize = 64;
const DEFAULT_META_CACHE_CAPACITY: usize = 200_000;
const DEFAULT_SQLITE_REPAIR_CACHE_CAPACITY: usize = 200_000;
const INDEX_CACHE_WARN_THRESHOLD: usize = 500;
const MIN_MULTIPART_PART_SIZE: u64 = 5 * 1024 * 1024;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersionEntry {
    pub meta: ObjectMeta,
    pub version_id: String,
    pub is_latest: bool,
}

/// Returned by a successful PUT or multipart complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutResult {
    pub etag: String,
    pub size: u64,
    pub last_modified_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VisibilityRepairBatch {
    pub selected: usize,
    pub repaired: usize,
    pub failed: usize,
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
                storage_version: "v1".to_string(),
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
        content_encoding: Option<&str>,
        aws_chunked: bool,
    ) -> Result<PutResult> {
        let payload = if aws_chunked {
            decode_aws_chunked(bytes)?
        } else {
            bytes.to_vec()
        };
        let staging_id = self
            .stage_put(bucket, key, &payload, content_type, content_encoding)
            .await?;
        self.commit_staged_put(bucket, key, &staging_id).await
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
        self.commit_staged_put(bucket, key, &staging_id).await
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
        let staged_part = if staged_part.exists() {
            staged_part
        } else {
            staging_dir.join("object").join("part.1")
        };
        ensure_file_exists(&staged_part).await?;

        let last_modified_ms = now_ms();
        let object_meta = ObjectMeta {
            format_version: 1,
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            storage: ObjectStorageKind::Single,
            size: put_meta.size,
            etag: put_meta.etag.clone(),
            last_modified_ms,
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
            }],
        };
        let publish_dir =
            prepare_single_publish_dir(&staging_dir, &staged_part, &object_meta).await?;

        let _guard = self.locks.lock(bucket, key).await;
        let _fanout_guard = self.object_fanout_read_lock(bucket, key).await?;

        let old_object_dir = resolve_object_dir(&self.layout, bucket, key).await?;
        let new_object_dir =
            prepare_live_publish_path(&self.layout, bucket, key, old_object_dir.as_ref()).await?;

        let index = self.index(bucket).await?;
        let cache_key = ObjectCacheKey::new(bucket, key);
        self.invalidate_object_caches(&cache_key);
        index.enqueue_visibility_repair(key, now_ms()).await?;

        if let Some(old_dir) = old_object_dir {
            move_object_dir_to_trash(&self.layout, bucket, &old_dir).await?;
        }
        match tokio::fs::rename(&publish_dir, &new_object_dir).await {
            Ok(()) => {}
            Err(err) => {
                let _ = index.enqueue_visibility_repair(key, now_ms()).await;
                return Err(err.into());
            }
        }
        match index
            .put(&ObjectIndexEntry {
                object_key: key.to_string(),
                size: put_meta.size,
                etag: put_meta.etag.clone(),
                last_modified_ms,
            })
            .await
        {
            Ok(()) => {
                let _ = index.delete_visibility_repair(key).await;
            }
            Err(err) => {
                let _ = index.enqueue_visibility_repair(key, now_ms()).await;
                return Err(err);
            }
        }

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
        let cache_key = ObjectCacheKey::new(bucket, key);

        let object_dir = match resolve_object_dir(&self.layout, bucket, key).await? {
            Some(dir) => dir,
            None => {
                self.invalidate_object_caches(&cache_key);
                match resolve_object_dir(&self.layout, bucket, key).await? {
                    Some(dir) => dir,
                    None => {
                        if let Ok(index) = self.index(bucket).await {
                            let _ = index.delete(key).await;
                            let _ = index.delete_visibility_repair(key).await;
                        }
                        cleanup_invisible_candidate_dirs(&self.layout, bucket, key).await;
                        return Err(StorageError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        });
                    }
                }
            }
        };

        let meta_path = object_dir.join("meta.json");
        let meta_file = match tokio::fs::metadata(&meta_path).await {
            Ok(mf) if mf.is_file() => mf,
            _ => {
                self.invalidate_object_caches(&cache_key);
                if let Ok(index) = self.index(bucket).await {
                    let _ = index.delete(key).await;
                }
                let _ = tokio::fs::remove_dir_all(&object_dir).await;
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
            .filter(|e| e.meta_modified == meta_modified && e.meta_len == meta_len);
        let cached = match cached {
            Some(c) => c,
            None => {
                let meta: ObjectMeta = read_json(&meta_path).await?;
                validate_object_parts(&object_dir, &meta).await?;
                let c = CachedObjectMeta {
                    part_offsets: part_offsets(&meta),
                    meta,
                    meta_modified,
                    meta_len,
                };
                self.meta_cache.insert(cache_key.clone(), c.clone());
                c
            }
        };

        // Repair SQLite if a row is missing (e.g. after a crash during commit).
        if !self.sqlite_repair_recently_checked(&cache_key) {
            let index = self.index(bucket).await?;
            let existing = index.get(key).await?;
            let needs_repair = existing.as_ref().map_or(true, |entry| {
                entry.size != cached.meta.size
                    || entry.etag != cached.meta.etag
                    || entry.last_modified_ms != cached.meta.last_modified_ms
            });
            if needs_repair {
                index
                    .put(&ObjectIndexEntry {
                        object_key: key.to_string(),
                        size: cached.meta.size,
                        etag: cached.meta.etag.clone(),
                        last_modified_ms: cached.meta.last_modified_ms,
                    })
                    .await?;
                log::info!(
                    "sqlite repair fixed index row bucket={} key={} size={} etag={}",
                    bucket,
                    key,
                    cached.meta.size,
                    cached.meta.etag,
                );
            }
            self.mark_sqlite_repaired(cache_key.clone());
        }
        Ok(ReadObject {
            meta: cached.meta,
            part_offsets: cached.part_offsets,
            object_dir,
        })
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let _guard = self.locks.lock(bucket, key).await;
        let cache_key = ObjectCacheKey::new(bucket, key);
        self.invalidate_object_caches(&cache_key);
        let index = self.index(bucket).await?;
        index.enqueue_visibility_repair(key, now_ms()).await?;
        // Find the actual directory before removing.
        if let Some(object_dir) = resolve_object_dir(&self.layout, bucket, key).await? {
            move_object_dir_to_trash(&self.layout, bucket, &object_dir).await?;
        }
        match index.delete(key).await {
            Ok(()) => {
                let _ = index.delete_visibility_repair(key).await;
            }
            Err(err) => {
                let _ = index.enqueue_visibility_repair(key, now_ms()).await;
                return Err(err);
            }
        }
        Ok(())
    }

    pub async fn enqueue_visibility_repair(&self, bucket: &str, key: &str) -> Result<()> {
        self.ensure_bucket_and_key(bucket, key).await?;
        self.index(bucket)
            .await?
            .enqueue_visibility_repair(key, now_ms())
            .await
    }

    pub async fn reconcile_object_index(&self, bucket: &str, key: &str) -> Result<()> {
        self.ensure_bucket_and_key(bucket, key).await?;
        let _guard = self.locks.lock(bucket, key).await;
        let cache_key = ObjectCacheKey::new(bucket, key);
        self.invalidate_object_caches(&cache_key);
        let index = self.index(bucket).await?;
        match resolve_object_dir(&self.layout, bucket, key).await? {
            Some(object_dir) => {
                let meta: ObjectMeta = read_json(&object_dir.join("meta.json")).await?;
                validate_object_parts(&object_dir, &meta).await?;
                index
                    .put(&ObjectIndexEntry {
                        object_key: key.to_string(),
                        size: meta.size,
                        etag: meta.etag,
                        last_modified_ms: meta.last_modified_ms,
                    })
                    .await?;
            }
            None => {
                index.delete(key).await?;
            }
        }
        index.delete_visibility_repair(key).await?;
        self.mark_sqlite_repaired(cache_key);
        Ok(())
    }

    pub async fn process_visibility_repairs(
        &self,
        bucket: &str,
        min_age_ms: i64,
        max_repairs: usize,
    ) -> Result<VisibilityRepairBatch> {
        validate_bucket_name(bucket)?;
        let index = self.index(bucket).await?;
        let keys = index
            .due_visibility_repairs(now_ms(), min_age_ms, max_repairs.max(1) as i64)
            .await?;
        let mut batch = VisibilityRepairBatch {
            selected: keys.len(),
            ..VisibilityRepairBatch::default()
        };
        for key in keys {
            match self.reconcile_object_index(bucket, &key).await {
                Ok(()) => batch.repaired += 1,
                Err(err) => {
                    batch.failed += 1;
                    let _ = index.mark_visibility_repair_attempt(&key, now_ms()).await;
                    log::warn!(
                        "visibility repair failed bucket={} key={} error={}",
                        bucket,
                        key,
                        err,
                    );
                }
            }
        }
        Ok(batch)
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
        let scan_started_at_ms = now_ms();
        let mut count = 0usize;
        log::info!(
            "rebuild_sqlite scan started bucket={bucket} scan_started_at_ms={scan_started_at_ms}"
        );
        Box::pin(rebuild_walk(
            bucket,
            &objects_dir,
            &index,
            scan_started_at_ms,
            &mut count,
            &self.shutdown,
        ))
        .await?;
        let stale_removed = index
            .delete_entries_not_seen_since(scan_started_at_ms)
            .await?;
        log::info!(
            "rebuild_sqlite applied index bucket={bucket} objects_seen={count} stale_removed={stale_removed}"
        );
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

    pub async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<ObjectVersionEntry>> {
        validate_bucket_name(bucket)?;
        validate_object_key(prefix)
            .or_else(|err| if prefix.is_empty() { Ok(()) } else { Err(err) })?;
        if !self.bucket_exists(bucket).await {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }

        let mut versions = Vec::new();
        let objects_dir = self.layout.bucket_dir(bucket)?.join("objects");
        collect_object_versions_from_dir(&objects_dir, prefix, true, &mut versions).await?;
        let trash_dir = self.layout.trash_dir(bucket)?;
        collect_object_versions_from_dir(&trash_dir, prefix, false, &mut versions).await?;
        versions.sort_by(|a, b| {
            a.meta
                .object_key
                .cmp(&b.meta.object_key)
                .then_with(|| b.meta.last_modified_ms.cmp(&a.meta.last_modified_ms))
        });
        Ok(versions)
    }

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
        tokio::fs::create_dir_all(&staging_dir).await?;
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
        let src = self.read_object(src_bucket, src_key).await?;
        let mut bytes: Vec<u8> = Vec::with_capacity(src.meta.size as usize);
        for part in &src.meta.parts {
            let data = tokio::fs::read(src.object_dir.join(&part.file)).await?;
            bytes.extend_from_slice(&data);
        }
        if let Some((start, end)) = range {
            if start > end || end >= bytes.len() as u64 {
                return Err(StorageError::InvalidMultipartUpload(
                    "invalid copy source range".to_string(),
                ));
            }
            bytes = bytes[start as usize..=end as usize].to_vec();
        }
        self.put_multipart_part(dst_bucket, dst_key, upload_id, part_number, &bytes, false)
            .await
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
        let last_modified_ms = now_ms();
        let object_meta = ObjectMeta {
            format_version: 1,
            bucket: bucket.to_string(),
            object_key: key.to_string(),
            storage: ObjectStorageKind::Multipart,
            size,
            etag: etag.clone(),
            last_modified_ms,
            content_type: upload.content_type.clone(),
            content_encoding: upload.content_encoding.clone(),
            content_language: upload.content_language.clone(),
            storage_class: upload.storage_class.clone(),
            user_meta: upload.user_meta.clone(),
            parts: parts.clone(),
        };
        let publish_dir = prepare_multipart_publish_dir(&staging_dir, &parts, &object_meta).await?;

        let _guard = self.locks.lock(bucket, key).await;
        let _fanout_guard = self.object_fanout_read_lock(bucket, key).await?;

        let old_object_dir = resolve_object_dir(&self.layout, bucket, key).await?;
        let new_object_dir =
            prepare_live_publish_path(&self.layout, bucket, key, old_object_dir.as_ref()).await?;

        let index = self.index(bucket).await?;
        let cache_key = ObjectCacheKey::new(bucket, key);
        self.invalidate_object_caches(&cache_key);
        index.enqueue_visibility_repair(key, now_ms()).await?;
        if let Some(old_dir) = old_object_dir {
            move_object_dir_to_trash(&self.layout, bucket, &old_dir).await?;
        }
        match tokio::fs::rename(&publish_dir, &new_object_dir).await {
            Ok(()) => {}
            Err(err) => {
                let _ = index.enqueue_visibility_repair(key, now_ms()).await;
                return Err(err.into());
            }
        }
        match index
            .put(&ObjectIndexEntry {
                object_key: key.to_string(),
                size,
                etag: etag.clone(),
                last_modified_ms,
            })
            .await
        {
            Ok(()) => {
                let _ = index.delete_visibility_repair(key).await;
            }
            Err(err) => {
                let _ = index.enqueue_visibility_repair(key, now_ms()).await;
                return Err(err);
            }
        }
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
        Ok(PutResult {
            etag,
            size,
            last_modified_ms,
        })
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
        let src = self.read_object(src_bucket, src_key).await?;
        let mut bytes: Vec<u8> = Vec::with_capacity(src.meta.size as usize);
        for part in &src.meta.parts {
            let data = tokio::fs::read(src.object_dir.join(&part.file)).await?;
            bytes.extend_from_slice(&data);
        }
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
        drop(src);
        let staging_id = self
            .stage_put_with_metadata(
                dst_bucket,
                dst_key,
                &bytes,
                Some(&content_type),
                content_encoding.as_deref(),
                Some(&copied_storage_class),
                content_language.as_deref(),
                &user_meta,
            )
            .await?;
        self.commit_staged_put(dst_bucket, dst_key, &staging_id)
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
    if staged_part != publish_part && !publish_part.exists() {
        tokio::fs::rename(staged_part, &publish_part).await?;
    }
    ensure_file_exists(&publish_part).await?;
    write_json_atomic(&publish_dir.join("meta.json"), meta).await?;
    Ok(publish_dir)
}

async fn prepare_multipart_publish_dir(
    staging_dir: &Path,
    parts: &[PartMeta],
    meta: &ObjectMeta,
) -> Result<PathBuf> {
    let publish_dir = staging_dir.join("object");
    tokio::fs::create_dir_all(&publish_dir).await?;
    for part in parts {
        let source = staging_dir.join(&part.file);
        let dest = publish_dir.join(&part.file);
        if !dest.exists() {
            tokio::fs::rename(&source, &dest).await?;
        }
        ensure_file_exists(&dest).await?;
    }
    write_json_atomic(&publish_dir.join("meta.json"), meta).await?;
    Ok(publish_dir)
}

async fn prepare_live_publish_path(
    layout: &StorageLayout,
    bucket: &str,
    key: &str,
    old_object_dir: Option<&PathBuf>,
) -> Result<PathBuf> {
    if let Some(old_dir) = old_object_dir {
        return Ok(old_dir.clone());
    }
    cleanup_invisible_candidate_dirs(layout, bucket, key).await;
    let new_dir = allocate_object_dir(layout, bucket, key).await?;
    match tokio::fs::remove_dir(&new_dir).await {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    Ok(new_dir)
}

async fn move_object_dir_to_trash(
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

async fn cleanup_invisible_candidate_dirs(layout: &StorageLayout, bucket: &str, key: &str) {
    let Ok(leaf) = layout.fanout_leaf_dir(bucket, key) else {
        return;
    };
    let prefix = object_dir_prefix(key);
    let Ok(mut entries) = tokio::fs::read_dir(&leaf).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(&prefix) || !is_object_dir_name(&name) {
            continue;
        }
        let dir = entry.path();
        if tokio::fs::metadata(dir.join("meta.json")).await.is_err() {
            let _ = tokio::fs::remove_dir_all(dir).await;
        }
    }
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

async fn collect_object_versions_from_dir(
    root: &Path,
    prefix: &str,
    is_latest: bool,
    out: &mut Vec<ObjectVersionEntry>,
) -> Result<()> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
            continue;
        };
        let mut has_meta = false;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.file_name().and_then(|n| n.to_str()) == Some("meta.json") {
                has_meta = true;
            } else if path.is_dir() {
                stack.push(path);
            }
        }
        if !has_meta {
            continue;
        }
        let Ok(meta) = read_json::<ObjectMeta>(&dir.join("meta.json")).await else {
            continue;
        };
        if !meta.object_key.starts_with(prefix) {
            continue;
        }
        let version_id = dir
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("null")
            .to_string();
        out.push(ObjectVersionEntry {
            meta,
            version_id,
            is_latest,
        });
    }
    Ok(())
}

async fn rebuild_walk(
    bucket: &str,
    dir: &Path,
    index: &SqliteObjectIndex,
    scan_started_at_ms: i64,
    entries_seen: &mut usize,
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
            let entry = ObjectIndexEntry {
                object_key: meta.object_key.clone(),
                size: meta.size,
                etag: meta.etag,
                last_modified_ms: meta.last_modified_ms,
            };
            index.put_seen_at(&entry, scan_started_at_ms).await?;
            *entries_seen += 1;
            log::info!(
                "rebuild_sqlite discovered object bucket={} key={} size={} etag={}",
                bucket,
                entry.object_key,
                entry.size,
                entry.etag,
            );
            if *entries_seen % 100 == 0 {
                log::info!(
                    "rebuild_sqlite progress bucket={} objects_found={}",
                    bucket,
                    *entries_seen
                );
                tokio::task::yield_now().await;
                if shutdown.is_cancelled() {
                    log::info!(
                        "rebuild_sqlite cancelled bucket={} objects_found={}",
                        bucket,
                        *entries_seen
                    );
                    return Err(StorageError::Io("rebuild cancelled".to_string()));
                }
            }
        } else {
            log::warn!("rebuild_sqlite failed to read meta dir={}", dir.display());
        }
    } else {
        for subdir in subdirs {
            Box::pin(rebuild_walk(
                bucket,
                &subdir,
                index,
                scan_started_at_ms,
                entries_seen,
                shutdown,
            ))
            .await?;
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
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.size, 5);
        let read = store
            .read_object("bucket", "doris/1722372774777722231")
            .await
            .unwrap();
        assert!(read.object_dir.join("meta.json").exists());
        assert!(read.object_dir.join("part.1").exists());

        let index = store.index("bucket").await.unwrap();
        index.delete("doris/1722372774777722231").await.unwrap();
        // Evict the repair cooldown so the next read_object re-checks SQLite.
        store
            .sqlite_repair_cache
            .remove(&ObjectCacheKey::new("bucket", "doris/1722372774777722231"));
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
            .put_object("bucket", "key", b"hello", None, None, false)
            .await
            .unwrap();
        let read = store.read_object("bucket", "key").await.unwrap();
        tokio::fs::remove_file(read.object_dir.join("meta.json"))
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
            .put_object("bucket", "key", b"hello", None, None, false)
            .await
            .unwrap();
        let read = store.read_object("bucket", "key").await.unwrap();
        let object_dir = read.object_dir.clone();
        assert!(object_dir.join("meta.json").exists());
        store.delete_object("bucket", "key").await.unwrap();
        assert!(!object_dir.join("meta.json").exists());
        assert!(!object_dir.exists());
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
    async fn overwrite_moves_old_object_to_trash_and_publishes_clean_live_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "key", b"old", None, None, false)
            .await
            .unwrap();
        let old_read = store.read_object("bucket", "key").await.unwrap();
        let old_dir = old_read.object_dir.clone();

        store
            .put_object("bucket", "key", b"new", None, None, false)
            .await
            .unwrap();

        let new_read = store.read_object("bucket", "key").await.unwrap();
        assert_eq!(new_read.object_dir, old_dir);
        assert_eq!(new_read.meta.size, 3);
        assert_eq!(
            tokio::fs::read(new_read.object_dir.join("part.1"))
                .await
                .unwrap(),
            b"new"
        );
        assert!(new_read.object_dir.join("meta.json").exists());
        assert!(!new_read.object_dir.join("put.json").exists());

        let trash_dir = store.layout().trash_dir("bucket").unwrap();
        let mut entries = tokio::fs::read_dir(&trash_dir).await.unwrap();
        let trash_entry = entries.next_entry().await.unwrap().unwrap();
        assert_eq!(
            tokio::fs::read(trash_entry.path().join("part.1"))
                .await
                .unwrap(),
            b"old"
        );
    }

    #[tokio::test]
    async fn read_missing_meta_cleans_up_sqlite_and_object_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "key", b"hello", None, None, false)
            .await
            .unwrap();
        let read = store.read_object("bucket", "key").await.unwrap();
        let object_dir = read.object_dir.clone();
        // Simulate crash after write but meta.json was removed externally
        tokio::fs::remove_file(object_dir.join("meta.json"))
            .await
            .unwrap();
        assert!(object_dir.exists());

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
        assert!(!object_dir.exists());
    }

    #[tokio::test]
    async fn abort_multipart_leaves_no_sqlite_or_object_trace() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        let upload_id = store
            .initiate_multipart("bucket", "large.bin", None, None)
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
        // No committed object exists
        assert!(matches!(
            store.read_object("bucket", "large.bin").await,
            Err(StorageError::ObjectNotFound { .. })
        ));
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
            .put_object("bucket", "a/1", b"foo", None, None, false)
            .await
            .unwrap();
        store
            .put_object("bucket", "a/2", b"bar", None, None, false)
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
    async fn rebuild_sqlite_removes_rows_not_seen_in_live_objects() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store
            .put_object("bucket", "live", b"foo", None, None, false)
            .await
            .unwrap();
        let index = store.index("bucket").await.unwrap();
        index
            .put_seen_at(
                &ObjectIndexEntry {
                    object_key: "stale".to_string(),
                    size: 5,
                    etag: "stale".to_string(),
                    last_modified_ms: 1,
                },
                0,
            )
            .await
            .unwrap();

        let count = store.rebuild_sqlite("bucket").await.unwrap();
        assert_eq!(count, 1);
        assert!(index.get("live").await.unwrap().is_some());
        assert!(index.get("stale").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn missing_sqlite_file_on_restart_triggers_background_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let store = LocalObjectStore::new(tmp.path());
            store.create_bucket("bucket").await.unwrap();
            store
                .put_object("bucket", "a/1", b"foo", None, None, false)
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
            .put_object("bucket", "a/1", b"foo", None, None, false)
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
            .initiate_multipart("bucket", "large.bin", None, None)
            .await
            .unwrap();
        let first_part = vec![b'a'; MIN_MULTIPART_PART_SIZE as usize];
        let p1 = store
            .put_multipart_part("bucket", "large.bin", &upload_id, 1, &first_part, false)
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
        assert_eq!(completed.size, MIN_MULTIPART_PART_SIZE + 4);
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
        assert_eq!(bytes.len(), first_part.len() + 4);
        assert!(bytes.starts_with(&first_part));
        assert!(bytes.ends_with(b"defg"));
        assert_eq!(read.meta.parts.len(), 2);
        assert_eq!(read.meta.parts[1].number, 3);
    }

    #[tokio::test]
    async fn complete_multipart_rejects_small_non_final_parts() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        let upload_id = store
            .initiate_multipart("bucket", "large.bin", None, None)
            .await
            .unwrap();
        let p1 = store
            .put_multipart_part("bucket", "large.bin", &upload_id, 1, b"abc", false)
            .await
            .unwrap();
        let p2 = store
            .put_multipart_part("bucket", "large.bin", &upload_id, 2, b"def", false)
            .await
            .unwrap();
        let err = store
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
                        number: 2,
                        etag: p2.etag,
                    },
                ],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::EntityTooSmall(_)));
    }
}
