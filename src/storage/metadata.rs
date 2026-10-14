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
    /// When this part was uploaded. `ListParts` reports it per part, and AWS
    /// always sends it, so an SDK's `Part::last_modified()` is empty without
    /// it. Defaulted so part metadata written before this field existed still
    /// deserializes; such a part reports the Unix epoch rather than failing
    /// the whole listing.
    #[serde(default)]
    pub last_modified_ms: i64,
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
    /// Version stamp (see `storage::stamp`); `None` on objects written
    /// before stamps existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stamp: Option<super::stamp::Stamp>,
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

/// The blob layout this build writes and serves: single-level fanout
/// (`objects/<4hex>/<leaf>`). Layout 1 was the 4-level fanout
/// (`objects/aa/bb/cc/dd/<leaf>`).
pub const CURRENT_LAYOUT: u32 = 2;

/// Where a bucket stands with respect to the on-disk layout, recorded in
/// its `bucket.json`. A bucket serves only while `stable` on the current
/// layout: at startup the server marks every bucket on an older layout
/// `migrating` and migrates it behind the 503 gate; a bucket found
/// `migrating` (the previous run stopped half way) resumes the same step
/// before it serves. A layout newer than the build knows refuses to start.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum BucketLayout {
    /// The bucket is entirely on layout `version`.
    Stable { version: u32 },
    /// An upgrade from layout `from` to layout `to` was started and has
    /// not finished: objects may be on either.
    Migrating { from: u32, to: u32 },
}

impl BucketLayout {
    pub fn current() -> Self {
        BucketLayout::Stable { version: CURRENT_LAYOUT }
    }

    /// The layout version the bucket is (ending up) on.
    pub fn target(self) -> u32 {
        match self {
            BucketLayout::Stable { version } => version,
            BucketLayout::Migrating { to, .. } => to,
        }
    }

    pub fn is_stable_current(self) -> bool {
        self == Self::current()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketMeta {
    pub created_at_ms: i64,
    /// Superseded by `layout`; older files carry `"v1"` or `"v2"`, read by
    /// nothing. Kept so they still parse.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub storage_version: String,
    /// The bucket's on-disk layout and whether an upgrade is in progress.
    /// Absent in files written before layouts were recorded: the startup
    /// check detects the layout from the tree and records it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<BucketLayout>,
    /// User-managed bucket CORS rules. The management console's own origin is
    /// an implicit server rule and is deliberately not persisted here.
    #[serde(default)]
    pub cors: Vec<CorsRule>,
    /// A read-only bucket accepts only replicated writes (carrying a version
    /// stamp — replication links, or `s3:ReplicateWrite` clients); every
    /// other write is refused. For replica buckets.
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorsRule {
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub allowed_methods: Vec<String>,
    #[serde(default)]
    pub allowed_headers: Vec<String>,
    #[serde(default)]
    pub expose_headers: Vec<String>,
    #[serde(default)]
    pub max_age_seconds: Option<u32>,
}

/// What S3 stores when a client sends no `Content-Type`.
///
/// `binary/octet-stream`, not the standard `application/octet-stream`: this is
/// a long-standing AWS quirk, verified on the wire rather than assumed, and a
/// client that round-trips an object through both services would otherwise see
/// the type change. The standard spelling is still honoured when a client
/// sends it -- this is only the default.
pub const DEFAULT_CONTENT_TYPE: &str = "binary/octet-stream";

pub fn content_type_or_default(value: Option<&str>) -> String {
    value
        .filter(|v| !v.trim().is_empty())
        .unwrap_or(DEFAULT_CONTENT_TYPE)
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
    #[test]
    fn bucket_layout_is_recorded_as_a_tagged_state() {
        let stable = serde_json::to_value(BucketLayout::current()).unwrap();
        assert_eq!(stable, serde_json::json!({"state": "stable", "version": CURRENT_LAYOUT}));
        let migrating = serde_json::to_value(BucketLayout::Migrating { from: 1, to: 2 }).unwrap();
        assert_eq!(migrating, serde_json::json!({"state": "migrating", "from": 1, "to": 2}));
        let back: BucketLayout = serde_json::from_value(migrating).unwrap();
        assert_eq!(back, BucketLayout::Migrating { from: 1, to: 2 });
        assert_eq!(back.target(), 2);
        // A bucket.json from before layouts were recorded still parses, with no layout.
        let old: BucketMeta = serde_json::from_str(r#"{"created_at_ms":1,"storage_version":"v2"}"#).unwrap();
        assert_eq!(old.layout, None);
        let written = serde_json::to_string(&BucketMeta { created_at_ms: 1, storage_version: String::new(), layout: Some(BucketLayout::current()), cors: vec![], read_only: false }).unwrap();
        assert!(written.contains(r#""layout":{"state":"stable","version":2}"#) && !written.contains("storage_version"), "{written}");
    }

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
        assert_eq!(content_type_or_default(None), "binary/octet-stream");
        assert_eq!(
            content_type_or_default(Some("")),
            "binary/octet-stream"
        );
        assert_eq!(content_type_or_default(Some("text/plain")), "text/plain");
    }

    #[test]
    fn object_meta_serializes_storage_as_lowercase() {
        let meta = ObjectMeta {
            stamp: None,
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
                last_modified_ms: 1,
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
