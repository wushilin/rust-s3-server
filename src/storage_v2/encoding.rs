use sha1::{Digest, Sha1};

use super::errors::{Result, StorageError};

const BASE32_LOWER: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

pub const MAX_S3_KEY_BYTES: usize = 1024;

pub fn validate_bucket_name(bucket: &str) -> Result<()> {
    if bucket.len() < 3 || bucket.len() > 63 {
        return Err(StorageError::InvalidBucketName(bucket.to_string()));
    }
    if matches!(bucket, "_staging" | "objects" | "staging") {
        return Err(StorageError::InvalidBucketName(bucket.to_string()));
    }
    if bucket.parse::<std::net::Ipv4Addr>().is_ok() {
        return Err(StorageError::InvalidBucketName(bucket.to_string()));
    }
    let bytes = bucket.as_bytes();
    let is_alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    if !is_alnum(bytes[0]) || !is_alnum(bytes[bytes.len() - 1]) {
        return Err(StorageError::InvalidBucketName(bucket.to_string()));
    }
    if !bytes.iter().all(|b| is_alnum(*b) || *b == b'-') {
        return Err(StorageError::InvalidBucketName(bucket.to_string()));
    }
    Ok(())
}

pub fn validate_object_key(key: &str, component_limit: usize) -> Result<()> {
    if key.is_empty() || key.as_bytes().contains(&0) || key.as_bytes().len() > MAX_S3_KEY_BYTES {
        return Err(StorageError::InvalidObjectKey(key.to_string()));
    }
    let encoded_len = base32_encoded_len(key.as_bytes().len());
    if encoded_len > component_limit {
        return Err(StorageError::PhysicalIdTooLong {
            encoded_len,
            limit: component_limit,
        });
    }
    Ok(())
}

pub fn physical_id_for_key(key: &str, component_limit: usize) -> Result<String> {
    validate_object_key(key, component_limit)?;
    Ok(base32_lower_no_pad(key.as_bytes()))
}

pub fn fanout_id(bucket: &str, key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(bucket.as_bytes());
    hasher.update([0]);
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
    }
    out
}

pub fn fanout_segments(bucket: &str, key: &str) -> [String; 4] {
    let id = fanout_id(bucket, key);
    [
        id[0..2].to_string(),
        id[2..4].to_string(),
        id[4..6].to_string(),
        id[6..8].to_string(),
    ]
}

fn base32_encoded_len(bytes: usize) -> usize {
    (bytes * 8 + 4) / 5
}

fn base32_lower_no_pad(input: &[u8]) -> String {
    let mut out = String::with_capacity(base32_encoded_len(input.len()));
    let mut buffer: u16 = 0;
    let mut bits_left: u8 = 0;
    for byte in input {
        buffer = (buffer << 8) | (*byte as u16);
        bits_left += 8;
        while bits_left >= 5 {
            let idx = ((buffer >> (bits_left - 5)) & 0b11111) as usize;
            out.push(BASE32_LOWER[idx] as char);
            bits_left -= 5;
        }
    }
    if bits_left > 0 {
        let idx = ((buffer << (5 - bits_left)) & 0b11111) as usize;
        out.push(BASE32_LOWER[idx] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_validation_rejects_unsafe_names() {
        for bucket in [
            "AA", "Upper", "has.dot", "1.2.3.4", "-bad", "bad-", "bad/name",
        ] {
            assert!(validate_bucket_name(bucket).is_err(), "{bucket}");
        }
        assert!(validate_bucket_name("abc-123").is_ok());
    }

    #[test]
    fn physical_id_is_base32_lower_and_filesystem_safe() {
        let encoded = physical_id_for_key("abc/def/af/好.txt", 255).unwrap();
        assert!(encoded
            .chars()
            .all(|c| c.is_ascii_lowercase() || ('2'..='7').contains(&c)));
        assert!(!encoded.contains('/'));
    }

    #[test]
    fn long_keys_are_rejected_before_filesystem_io() {
        let key = "x".repeat(200);
        assert!(matches!(
            physical_id_for_key(&key, 255),
            Err(StorageError::PhysicalIdTooLong { .. })
        ));
    }

    #[test]
    fn fanout_changes_for_sequential_suffixes() {
        let a = fanout_segments("bucket", "1722372774777722231");
        let b = fanout_segments("bucket", "1722372774777722232");
        assert_ne!(a, b);
    }
}
