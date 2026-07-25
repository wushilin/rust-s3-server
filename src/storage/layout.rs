use std::path::{Path, PathBuf};

use super::encoding::validate_bucket_name;
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
    fn bucket_dir_is_under_buckets_root() {
        let layout = StorageLayout::new("/tmp/root");
        let dir = layout.bucket_dir("bucket").unwrap();
        assert_eq!(dir, std::path::Path::new("/tmp/root/buckets/bucket"));
    }
}
