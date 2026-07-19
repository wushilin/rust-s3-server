use rand::Rng;
use sha1::{Digest, Sha1};

use super::errors::{Result, StorageError};

const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

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
    if !bytes
        .iter()
        .all(|b| is_alnum(*b) || *b == b'-' || *b == b'.')
    {
        return Err(StorageError::InvalidBucketName(bucket.to_string()));
    }
    if bucket.contains("..") || bucket.contains(".-") || bucket.contains("-.") {
        return Err(StorageError::InvalidBucketName(bucket.to_string()));
    }
    Ok(())
}

pub fn validate_object_key(key: &str) -> Result<()> {
    if key.is_empty() || key.as_bytes().contains(&0) || key.as_bytes().len() > MAX_S3_KEY_BYTES {
        return Err(StorageError::InvalidObjectKey(key.to_string()));
    }
    Ok(())
}

/// Returns the object directory prefix for `key`: `"V1" + SHA1(key)[0..6].to_uppercase()`.
/// The prefix is 8 chars: "V1" + 6 uppercase hex digits.
pub fn object_dir_prefix(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(8);
    out.push_str("V1");
    for b in &digest[..3] {
        out.push(HEX_UPPER[(b >> 4) as usize] as char);
        out.push(HEX_UPPER[(b & 0xf) as usize] as char);
    }
    out
}

/// Returns a 4-char random uppercase-hex collision suffix, e.g. `"A3F1"`.
pub fn object_dir_random_suffix() -> String {
    let mut rng = rand::thread_rng();
    (0..4)
        .map(|_| HEX_UPPER[rng.gen_range(0..16)] as char)
        .collect()
}

/// Returns true if `name` is an object directory name (`V1` followed by 6 uppercase hex,
/// optionally `_` and 4 uppercase hex).
pub fn is_object_dir_name(name: &str) -> bool {
    if !name.starts_with("V1") || name.len() < 8 {
        return false;
    }
    let rest = &name[2..];
    let (prefix_hex, suffix) = if let Some(idx) = rest.find('_') {
        (&rest[..idx], Some(&rest[idx + 1..]))
    } else {
        (rest, None)
    };
    let valid_hex = |s: &str| {
        s.len() == 6
            && s.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase())
    };
    let valid_suffix = |s: &str| {
        s.len() == 4
            && s.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase())
    };
    valid_hex(prefix_hex) && suffix.map_or(true, valid_suffix)
}

// ── Shared ────────────────────────────────────────────────────────────────────

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

/// Single-level fanout segment: the first 4 hex chars (16 bits → 65,536 slots)
/// of the key's hash. The current on-disk layout places each object at
/// `objects/<segment>/<leaf>`. Legacy objects use the 4-level
/// [`fanout_segments`] layout and coexist untouched — the index records each
/// object's real path, so reads never recompute the fanout.
pub fn fanout_segment(bucket: &str, key: &str) -> String {
    fanout_id(bucket, key)[0..4].to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_validation_rejects_unsafe_names() {
        for bucket in [
            "AA",
            "Upper",
            "1.2.3.4",
            "-bad",
            "bad-",
            "bad/name",
            "bad..name",
            "bad.-name",
            "bad-.name",
            ".bad",
            "bad.",
        ] {
            assert!(validate_bucket_name(bucket).is_err(), "{bucket}");
        }
        assert!(validate_bucket_name("abc-123").is_ok());
        assert!(validate_bucket_name("abc.123").is_ok());
        assert!(validate_bucket_name("minio-java-test.withperiod").is_ok());
    }

    #[test]
    fn object_dir_prefix_is_8_chars_starting_with_v1() {
        let p = object_dir_prefix("my/key.txt");
        assert_eq!(p.len(), 8);
        assert!(p.starts_with("V1"));
        assert!(p[2..]
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase()));
    }

    #[test]
    fn object_dir_prefix_is_deterministic() {
        assert_eq!(object_dir_prefix("key"), object_dir_prefix("key"));
    }

    #[test]
    fn object_dir_prefix_differs_for_different_keys() {
        assert_ne!(object_dir_prefix("key1"), object_dir_prefix("key2"));
    }

    #[test]
    fn object_dir_random_suffix_is_4_uppercase_hex() {
        let s = object_dir_random_suffix();
        assert_eq!(s.len(), 4);
        assert!(s
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase()));
    }

    #[test]
    fn is_object_dir_name_accepts_valid_forms() {
        assert!(is_object_dir_name("V1AB3F7C"));
        assert!(is_object_dir_name("V1AB3F7C_A1B2"));
        assert!(!is_object_dir_name("V1ab3f7c")); // lowercase hex
        assert!(!is_object_dir_name("v1AB3F7C")); // lowercase v
        assert!(!is_object_dir_name("abcdef"));
        assert!(!is_object_dir_name("V1AB3F")); // too short (only 4 hex, need 6)
    }

    #[test]
    fn long_keys_accepted_by_validate_object_key() {
        let key = "x".repeat(1024);
        assert!(validate_object_key(&key).is_ok());
        let too_long = "x".repeat(1025);
        assert!(validate_object_key(&too_long).is_err());
    }

    #[test]
    fn fanout_changes_for_sequential_suffixes() {
        let a = fanout_segments("bucket", "1722372774777722231");
        let b = fanout_segments("bucket", "1722372774777722232");
        assert_ne!(a, b);
    }
}
