use std::path::{Path, PathBuf};

use super::encoding::{fanout_segments, validate_bucket_name};
use super::errors::Result;

#[derive(Debug, Clone)]
pub struct StorageLayout {
    root: PathBuf,
}

impl StorageLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn bucket_dir(&self, bucket: &str) -> Result<PathBuf> {
        validate_bucket_name(bucket)?;
        Ok(self.root.join("buckets").join(bucket))
    }

    pub fn bucket_meta_path(&self, bucket: &str) -> Result<PathBuf> {
        Ok(self.bucket_dir(bucket)?.join("bucket.json"))
    }

    /// Returns the 4-level fanout leaf directory for (bucket, key).
    /// Does not include the object directory component — that requires a resolver scan.
    pub fn fanout_leaf_dir(&self, bucket: &str, key: &str) -> Result<PathBuf> {
        let bucket_dir = self.bucket_dir(bucket)?;
        let [a, b, c, d] = fanout_segments(bucket, key);
        Ok(bucket_dir.join("objects").join(a).join(b).join(c).join(d))
    }

    pub fn put_staging_dir(&self, bucket: &str, staging_id: &str) -> Result<PathBuf> {
        Ok(self
            .bucket_dir(bucket)?
            .join("staging")
            .join("put")
            .join(staging_id))
    }

    pub fn multipart_staging_dir(&self, bucket: &str, upload_id: &str) -> Result<PathBuf> {
        Ok(self
            .bucket_dir(bucket)?
            .join("staging")
            .join("multipart")
            .join(upload_id))
    }

    pub fn trash_dir(&self, bucket: &str) -> Result<PathBuf> {
        Ok(self.bucket_dir(bucket)?.join("trash"))
    }

    pub fn object_trash_dir(&self, bucket: &str, trash_id: &str) -> Result<PathBuf> {
        Ok(self.trash_dir(bucket)?.join(trash_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fanout_leaf_dir_has_4_fanout_levels_under_objects() {
        let layout = StorageLayout::new("/tmp/root");
        let leaf = layout.fanout_leaf_dir("bucket", "logs/2026/1").unwrap();
        let parts: Vec<_> = leaf
            .iter()
            .map(|v| v.to_string_lossy().to_string())
            .collect();
        assert_eq!(parts[parts.len() - 5], "objects");
        assert_eq!(parts[parts.len() - 4].len(), 2); // fanout a
        assert_eq!(parts[parts.len() - 3].len(), 2); // fanout b
        assert_eq!(parts[parts.len() - 2].len(), 2); // fanout c
        assert_eq!(parts[parts.len() - 1].len(), 2); // fanout d
    }
}
