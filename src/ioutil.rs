use crate::fsapi::Bucket;
use std::error::Error;

pub fn delete_file_and_meta(path: &str) -> Result<(), Box<dyn Error>> {
    let meta = format!("{}{}", path, Bucket::META_SUFFIX);
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(meta);
    return Ok(());
}