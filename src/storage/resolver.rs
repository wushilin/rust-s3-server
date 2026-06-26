//! Object directory resolution for the current on-disk layout.
//!
//! Object directories are named:
//! `V1<SHA1(key)[0..6].uppercase_hex>[_<4hex_suffix>]`, e.g. `V1AB3F7C`.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use super::encoding::{is_object_dir_name, object_dir_prefix, object_dir_random_suffix};
use super::errors::Result;
use super::layout::StorageLayout;
use super::metadata::ObjectMeta;

/// Scan disk for the object directory of `(bucket, key)`.
///
/// Returns `object_dir` on success, `None` if not found.
/// Does **not** consult any in-memory cache — the caller handles that.
pub async fn resolve_object_dir(
    layout: &StorageLayout,
    bucket: &str,
    key: &str,
) -> Result<Option<PathBuf>> {
    let leaf = layout.fanout_leaf_dir(bucket, key)?;
    probe_object_dirs(&leaf, key).await
}

/// Allocate an object directory slot for `(bucket, key)`.
///
/// * If an entry for this key already exists (verified via `meta.json`), the
///   existing directory is returned — supporting in-place overwrites.
/// * If the base prefix slot is free, it is returned.
/// * If the base slot is taken by a **different** key (collision), random
///   4-hex suffixes are tried until a free slot is found.
pub async fn allocate_object_dir(
    layout: &StorageLayout,
    bucket: &str,
    key: &str,
) -> Result<PathBuf> {
    let leaf = layout.fanout_leaf_dir(bucket, key)?;
    tokio::fs::create_dir_all(&leaf).await?;
    let prefix = object_dir_prefix(key);

    // Re-use an existing dir for this key (overwrite path).
    if let Ok(mut entries) = tokio::fs::read_dir(&leaf).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.starts_with(&prefix) || !is_object_dir_name(&name) {
                continue;
            }
            if meta_key_matches(&entry.path(), key).await {
                return Ok(entry.path());
            }
        }
    }

    // No existing dir — try base prefix first.
    let base = leaf.join(&prefix);
    match tokio::fs::create_dir(&base).await {
        Ok(()) => return Ok(base),
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {}
        Err(err) => return Err(err.into()),
    }

    // Collision: pick random suffixes until a free slot is found.
    loop {
        let suffix = object_dir_random_suffix();
        let name = format!("{prefix}_{suffix}");
        let dir = leaf.join(&name);
        match tokio::fs::create_dir(&dir).await {
            Ok(()) => return Ok(dir),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err.into()),
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

async fn probe_object_dirs(leaf: &Path, key: &str) -> Result<Option<PathBuf>> {
    let prefix = object_dir_prefix(key);
    let Ok(mut entries) = tokio::fs::read_dir(leaf).await else {
        return Ok(None);
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(&prefix) || !is_object_dir_name(&name) {
            continue;
        }
        if meta_key_matches(&entry.path(), key).await {
            return Ok(Some(entry.path()));
        }
    }
    Ok(None)
}

async fn meta_key_matches(dir: &Path, key: &str) -> bool {
    let Ok(bytes) = tokio::fs::read(dir.join("meta.json")).await else {
        return false;
    };
    serde_json::from_slice::<ObjectMeta>(&bytes)
        .ok()
        .map_or(false, |m| m.object_key == key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_layout(tmp: &TempDir) -> StorageLayout {
        StorageLayout::new(tmp.path())
    }

    #[tokio::test]
    async fn resolve_returns_none_for_nonexistent_object() {
        let tmp = TempDir::new().unwrap();
        let layout = make_layout(&tmp);
        // Create bucket dir so fanout_leaf_dir can be computed (no actual bucket needed for resolve)
        let result = resolve_object_dir(&layout, "testbucket", "no/such/key")
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn allocate_object_dir_returns_base_prefix_when_no_collision() {
        let tmp = TempDir::new().unwrap();
        let layout = make_layout(&tmp);
        let dir = allocate_object_dir(&layout, "mybucket", "mykey")
            .await
            .unwrap();
        let prefix = object_dir_prefix("mykey");
        assert!(dir.ends_with(&prefix));
    }

    #[tokio::test]
    async fn allocate_object_dir_reuses_existing_dir_for_same_key() {
        use crate::storage::metadata::{ObjectMeta, ObjectStorageKind};
        use tokio::io::AsyncWriteExt;

        let tmp = TempDir::new().unwrap();
        let layout = make_layout(&tmp);

        let dir = allocate_object_dir(&layout, "mybucket", "mykey")
            .await
            .unwrap();

        // Write a minimal meta.json with the right key
        let meta = ObjectMeta {
            format_version: 1,
            bucket: "mybucket".to_string(),
            object_key: "mykey".to_string(),
            storage: ObjectStorageKind::Single,
            size: 0,
            etag: "etag".to_string(),
            last_modified_ms: 0,
            content_type: "application/octet-stream".to_string(),
            content_encoding: None,
            content_language: None,
            storage_class: "STANDARD".to_string(),
            user_meta: std::collections::BTreeMap::new(),
            parts: vec![],
        };
        let bytes = serde_json::to_vec(&meta).unwrap();
        let mut f = tokio::fs::File::create(dir.join("meta.json"))
            .await
            .unwrap();
        f.write_all(&bytes).await.unwrap();

        // Second allocation for same key should return the same dir
        let dir2 = allocate_object_dir(&layout, "mybucket", "mykey")
            .await
            .unwrap();
        assert_eq!(dir, dir2);
    }
}
