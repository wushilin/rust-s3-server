use serde::{Deserialize, Serialize};

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
    pub physical_id: String,
    pub storage: ObjectStorageKind,
    pub size: u64,
    pub etag: String,
    pub last_modified_ms: i64,
    pub content_type: String,
    pub parts: Vec<PartMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutMeta {
    pub bucket: String,
    pub object_key: String,
    pub physical_id: String,
    pub write_id: String,
    pub initiated_at_ms: i64,
    pub size: u64,
    pub etag: String,
    pub content_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadMeta {
    pub bucket: String,
    pub object_key: String,
    pub upload_id: String,
    pub initiated_at_ms: i64,
    pub physical_id: String,
    pub content_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketMeta {
    pub created_at_ms: i64,
}

pub fn content_type_or_default(value: Option<&str>) -> String {
    value
        .filter(|v| !v.trim().is_empty())
        .unwrap_or("application/octet-stream")
        .to_string()
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
            physical_id: "abc".to_string(),
            storage: ObjectStorageKind::Single,
            size: 3,
            etag: "etag".to_string(),
            last_modified_ms: 1,
            content_type: "text/plain".to_string(),
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
}
