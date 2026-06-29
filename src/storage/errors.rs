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
    Sqlite(String),
    BucketNotFound(String),
    ObjectNotFound { bucket: String, key: String },
    NoSuchUpload(String),
    InvalidMultipartUpload(String),
    EntityTooSmall(String),
    PayloadHashMismatch { expected: String, actual: String },
    CorruptObject(String),
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
            StorageError::Sqlite(v) => write!(f, "sqlite error: {v}"),
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

impl From<sqlx::Error> for StorageError {
    fn from(value: sqlx::Error) -> Self {
        StorageError::Sqlite(value.to_string())
    }
}
