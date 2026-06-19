use std::path::{Path, PathBuf};

use super::encoding::{fanout_segments, physical_id_for_key, validate_bucket_name};
use super::errors::Result;

#[derive(Debug, Clone)]
pub struct StorageLayout {
    root: PathBuf,
    component_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPath {
    pub bucket_dir: PathBuf,
    pub object_dir: PathBuf,
    pub meta_path: PathBuf,
    pub physical_id: String,
}

impl StorageLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            component_limit: 255,
        }
    }

    pub fn with_component_limit(mut self, component_limit: usize) -> Self {
        self.component_limit = component_limit;
        self
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

    pub fn object_path(&self, bucket: &str, key: &str) -> Result<ObjectPath> {
        let bucket_dir = self.bucket_dir(bucket)?;
        let physical_id = physical_id_for_key(key, self.component_limit)?;
        let [a, b, c, d] = fanout_segments(bucket, key);
        let object_dir = bucket_dir
            .join("objects")
            .join(a)
            .join(b)
            .join(c)
            .join(d)
            .join(&physical_id);
        let meta_path = object_dir.join("meta.json");
        Ok(ObjectPath {
            bucket_dir,
            object_dir,
            meta_path,
            physical_id,
        })
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_path_uses_hash_fanout_and_physical_id_leaf() {
        let layout = StorageLayout::new("/tmp/root");
        let object = layout.object_path("bucket", "logs/2026/1").unwrap();
        let parts: Vec<_> = object
            .object_dir
            .iter()
            .map(|v| v.to_string_lossy().to_string())
            .collect();
        assert_eq!(parts[parts.len() - 1], object.physical_id);
        assert_eq!(parts[parts.len() - 6], "objects");
        assert_eq!(object.object_dir.join("meta.json"), object.meta_path);
    }
}
