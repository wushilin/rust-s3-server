use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    InvalidBucketName(String),
    InvalidObjectKey(String),
    InvalidStagingId(String),
    InvalidRange,
    UnsatisfiableRange { total_size: u64 },
    InvalidAwsChunkedBody(String),
    Io(String),
    Json(String),
    Db(String),
    BucketNotFound(String),
    ObjectNotFound { bucket: String, key: String },
    NoSuchUpload(String),
    InvalidMultipartUpload(String),
    EntityTooSmall(String),
    PayloadHashMismatch { expected: String, actual: String },
    CorruptObject(String),
    /// The bucket's index is being rebuilt, or its storage layout upgraded;
    /// all data requests get 503 until done.
    BucketRebuilding(String),
    /// The on-disk index schema is not current; a rebuild is required.
    IndexOutdated(String),
    /// A conditional write's precondition (If-None-Match / If-Match) was not
    /// met against the object currently at the key. Maps to HTTP 412.
    PreconditionFailed { bucket: String, key: String },
    /// The bucket cannot be deleted while something still depends on it
    /// (changelog subscriptions). Maps to HTTP 409.
    BucketInUse(String),
    /// A replicated write or delete (carrying a version stamp) that is not
    /// newer than the key's current state — the object or its tombstone.
    /// Nothing was changed. Maps to HTTP 412.
    StaleStamp { bucket: String, key: String, current: String },
    /// A client write to a read-only bucket (only replicated writes are
    /// accepted). Maps to HTTP 403.
    BucketReadOnly(String),
    /// The bucket's recorded layout is one this build does not know (a
    /// newer build wrote it) or a migration step this build cannot run.
    /// The server refuses to start.
    UnsupportedLayout { bucket: String, message: String },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::InvalidBucketName(v) => write!(f, "invalid bucket name: {v}"),
            StorageError::InvalidObjectKey(v) => write!(f, "invalid object key: {v}"),
            StorageError::InvalidStagingId(v) => write!(f, "invalid staging id: {v}"),
            StorageError::InvalidRange => write!(f, "invalid range"),
            StorageError::UnsatisfiableRange { total_size } => {
                write!(f, "range not satisfiable for object size {total_size}")
            }
            StorageError::InvalidAwsChunkedBody(v) => write!(f, "invalid aws-chunked body: {v}"),
            StorageError::Io(v) => write!(f, "io error: {v}"),
            StorageError::Json(v) => write!(f, "json error: {v}"),
            StorageError::Db(v) => write!(f, "db error: {v}"),
            StorageError::BucketNotFound(v) => write!(f, "bucket not found: {v}"),
            StorageError::ObjectNotFound { bucket, key } => {
                write!(f, "object not found: {bucket}/{key}")
            }
            StorageError::NoSuchUpload(v) => write!(f, "multipart upload not found: {v}"),
            StorageError::InvalidMultipartUpload(v) => write!(f, "invalid multipart upload: {v}"),
            StorageError::EntityTooSmall(v) => write!(f, "entity too small: {v}"),
            StorageError::PayloadHashMismatch { expected, actual } => {
                write!(
                    f,
                    "payload hash mismatch: expected {expected}, actual {actual}"
                )
            }
            StorageError::CorruptObject(v) => write!(f, "corrupt object: {v}"),
            StorageError::BucketRebuilding(v) => {
                write!(f, "bucket {v} is unavailable while its index is rebuilt or its storage layout upgraded")
            }
            StorageError::IndexOutdated(v) => write!(f, "index schema outdated: {v}"),
            StorageError::PreconditionFailed { bucket, key } => {
                write!(f, "precondition failed for {bucket}/{key}")
            }
            StorageError::BucketInUse(v) => write!(f, "{v}"),
            StorageError::BucketReadOnly(bucket) => write!(
                f,
                "bucket {bucket} is read-only: it accepts only replicated writes"
            ),
            StorageError::StaleStamp { bucket, key, current } => write!(
                f,
                "{bucket}/{key} already has a state at least as new (stamp {current}); nothing changed"
            ),
            StorageError::UnsupportedLayout { bucket, message } => {
                write!(f, "bucket {bucket}: unsupported storage layout: {message}")
            }
        }
    }
}

impl std::error::Error for StorageError {}

pub type Result<T> = std::result::Result<T, StorageError>;

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        StorageError::Io(value.to_string())
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(value: serde_json::Error) -> Self {
        StorageError::Json(value.to_string())
    }
}

impl From<rocksdb::Error> for StorageError {
    fn from(value: rocksdb::Error) -> Self {
        StorageError::Db(value.to_string())
    }
}
