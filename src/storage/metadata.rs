use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const DEFAULT_STORAGE_CLASS: &str = "STANDARD";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ObjectStorageKind {
    Single,
    Multipart,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartMeta {
    pub number: u16,
    pub file: String,
    pub size: u64,
    pub etag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectMeta {
    pub format_version: u32,
    pub bucket: String,
    pub object_key: String,
    pub storage: ObjectStorageKind,
    pub size: u64,
    pub etag: String,
    pub last_modified_ms: i64,
    pub content_type: String,
    #[serde(default)]
    pub content_encoding: Option<String>,
    #[serde(default)]
    pub content_language: Option<String>,
    #[serde(default = "default_storage_class")]
    pub storage_class: String,
    #[serde(default)]
    pub user_meta: BTreeMap<String, String>,
    pub parts: Vec<PartMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutMeta {
    pub bucket: String,
    pub object_key: String,
    pub write_id: String,
    pub initiated_at_ms: i64,
    pub size: u64,
    pub etag: String,
    pub content_type: String,
    #[serde(default)]
    pub content_encoding: Option<String>,
    #[serde(default)]
    pub content_language: Option<String>,
    #[serde(default = "default_storage_class")]
    pub storage_class: String,
    #[serde(default)]
    pub user_meta: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadMeta {
    pub bucket: String,
    pub object_key: String,
    pub upload_id: String,
    pub initiated_at_ms: i64,
    pub content_type: String,
    #[serde(default)]
    pub content_encoding: Option<String>,
    #[serde(default)]
    pub content_language: Option<String>,
    #[serde(default = "default_storage_class")]
    pub storage_class: String,
    #[serde(default)]
    pub user_meta: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketMeta {
    pub created_at_ms: i64,
    /// On-disk storage layout version. Current unreleased format is "v1".
    #[serde(default)]
    pub storage_version: String,
}

pub fn content_type_or_default(value: Option<&str>) -> String {
    value
        .filter(|v| !v.trim().is_empty())
        .unwrap_or("application/octet-stream")
        .to_string()
}

pub fn content_encoding_or_none(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

pub fn content_language_or_none(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

pub fn storage_class_or_default(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(DEFAULT_STORAGE_CLASS)
        .to_string()
}

pub fn default_storage_class() -> String {
    DEFAULT_STORAGE_CLASS.to_string()
}

/// S3 storage classes accepted on the `x-amz-storage-class` request header.
const VALID_STORAGE_CLASSES: &[&str] = &[
    "STANDARD",
    "REDUCED_REDUNDANCY",
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER",
    "DEEP_ARCHIVE",
    "GLACIER_IR",
    "SNOW",
    "EXPRESS_ONEZONE",
    "OUTPOSTS",
];

/// Validates a client-supplied storage class. An absent or blank value is
/// treated as valid (the server defaults it to STANDARD); a present value must
/// match one of the known S3 storage classes exactly.
pub fn is_valid_storage_class(value: Option<&str>) -> bool {
    match value.map(str::trim).filter(|v| !v.is_empty()) {
        None => true,
        Some(class) => VALID_STORAGE_CLASSES.contains(&class),
    }
}

pub fn quote_etag(etag: &str) -> String {
    let trimmed = etag.trim_matches('"');
    format!("\"{trimmed}\"")
}

pub fn unquote_etag(etag: &str) -> String {
    etag.trim().trim_matches('"').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_class_validation_accepts_known_classes_and_rejects_unknown() {
        // Absent header is always valid (defaults to STANDARD).
        assert!(is_valid_storage_class(None));
        // Known S3 storage classes are accepted.
        assert!(is_valid_storage_class(Some("STANDARD")));
        assert!(is_valid_storage_class(Some("REDUCED_REDUNDANCY")));
        assert!(is_valid_storage_class(Some("GLACIER")));
        assert!(is_valid_storage_class(Some("INTELLIGENT_TIERING")));
        // Surrounding whitespace is tolerated.
        assert!(is_valid_storage_class(Some("  STANDARD  ")));
        // Unknown values are rejected (mint sends "REDUCED" expecting failure).
        assert!(!is_valid_storage_class(Some("REDUCED")));
        assert!(!is_valid_storage_class(Some("NONSENSE123")));
        assert!(!is_valid_storage_class(Some("standard")));
    }

    #[test]
    fn etag_quote_roundtrip_uses_unquoted_internal_form() {
        assert_eq!(quote_etag("abc"), "\"abc\"");
        assert_eq!(quote_etag("\"abc\""), "\"abc\"");
        assert_eq!(unquote_etag("\"abc\""), "abc");
    }

    #[test]
    fn content_type_defaults() {
        assert_eq!(content_type_or_default(None), "application/octet-stream");
        assert_eq!(
            content_type_or_default(Some("")),
            "application/octet-stream"
        );
        assert_eq!(content_type_or_default(Some("text/plain")), "text/plain");
    }

    #[test]
    fn object_meta_serializes_storage_as_lowercase() {
        let meta = ObjectMeta {
            format_version: 1,
            bucket: "bucket".to_string(),
            object_key: "key".to_string(),
            storage: ObjectStorageKind::Single,
            size: 3,
            etag: "etag".to_string(),
            last_modified_ms: 1,
            content_type: "text/plain".to_string(),
            content_encoding: None,
            content_language: None,
            storage_class: DEFAULT_STORAGE_CLASS.to_string(),
            user_meta: BTreeMap::new(),
            parts: vec![PartMeta {
                number: 1,
                file: "part.1".to_string(),
                size: 3,
                etag: "etag".to_string(),
            }],
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("\"storage\":\"single\""));
    }

    #[test]
    fn object_meta_deserializes_without_content_encoding() {
        let json = r#"{
            "format_version":1,
            "bucket":"bucket",
            "object_key":"key",
            "storage":"single",
            "size":3,
            "etag":"etag",
            "last_modified_ms":1,
            "content_type":"text/plain",
            "parts":[{"number":1,"file":"part.1","size":3,"etag":"etag"}]
        }"#;
        let meta: ObjectMeta = serde_json::from_str(json).unwrap();
        assert_eq!(meta.content_encoding, None);
        assert_eq!(meta.content_language, None);
        assert_eq!(meta.storage_class, DEFAULT_STORAGE_CLASS);
        assert!(meta.user_meta.is_empty());
        assert_eq!(meta.object_key, "key");
    }
}
