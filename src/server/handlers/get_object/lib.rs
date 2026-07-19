//! `GET` / `HEAD /{bucket}/{key}` — the complete object read path: conditional
//! requests, `Range` handling, and zero-copy streaming of one or many part
//! files. All read-specific logic lives here; only cross-cutting helpers
//! (error mapping, empty responses) come from the `server` library.

use std::io::SeekFrom;
use std::path::Path as FsPath;

use axum::body::Body;
use axum::http::{header, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::server::range::{parse_range_header, RangeSelection};
use crate::storage::errors::StorageError;
use crate::storage::metadata::{quote_etag, ObjectMeta};
use crate::storage::store::LocalObjectStore;
use crate::storage::time::{http_date_ms, parse_http_date_ms};

const STREAM_CHUNK_SIZE: usize = 256 * 1024;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, _body: Body) -> Response {
    let ObjectCtx {
        bucket,
        key,
        query,
        headers,
        method,
        ..
    } = ctx;

    let object = match store.read_object(&bucket, &key).await {
        Ok(object) => object,
        Err(err) => return srv::storage_error_response(err, &format!("/{bucket}/{key}")),
    };

    // Conditional request checks (If-None-Match / If-Modified-Since).
    let etag_quoted = quote_etag(&object.meta.etag);
    if let Some(inm) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        let client_etag = inm.trim().trim_matches('"');
        let server_etag = object.meta.etag.trim_matches('"');
        if client_etag == "*" || client_etag == server_etag {
            let mut resp = srv::empty_response(StatusCode::NOT_MODIFIED);
            resp.headers_mut()
                .insert(header::ETAG, HeaderValue::from_str(&etag_quoted).unwrap());
            return resp;
        }
    }
    if let Some(ims) = headers
        .get(header::IF_MODIFIED_SINCE)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(since_ms) = parse_http_date_ms(ims) {
            if object.meta.last_modified_ms <= since_ms {
                let mut resp = srv::empty_response(StatusCode::NOT_MODIFIED);
                resp.headers_mut()
                    .insert(header::ETAG, HeaderValue::from_str(&etag_quoted).unwrap());
                return resp;
            }
        }
    }

    let total_size = object.meta.size;
    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let selection = parse_range_header(range_header, total_size);

    // Unsatisfiable range: return 416 immediately.
    if let RangeSelection::Unsatisfiable { total_size } = selection {
        let mut response = srv::empty_response(StatusCode::RANGE_NOT_SATISFIABLE);
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes */{total_size}")).unwrap(),
        );
        return response;
    }

    let (status, range_start, range_len, content_range) = match &selection {
        RangeSelection::Full => (StatusCode::OK, 0u64, total_size, None),
        RangeSelection::Single {
            start,
            end_inclusive,
        } => (
            StatusCode::PARTIAL_CONTENT,
            *start,
            end_inclusive - start + 1,
            Some(format!("bytes {start}-{end_inclusive}/{total_size}")),
        ),
        RangeSelection::Unsatisfiable { .. } => unreachable!(),
    };

    let etag = etag_quoted;
    let content_type = query
        .get("response-content-type")
        .cloned()
        .unwrap_or_else(|| object.meta.content_type.clone());
    let content_encoding = object.meta.content_encoding.clone();
    let content_language = object.meta.content_language.clone();
    let storage_class = object.meta.storage_class.clone();
    let user_meta = object.meta.user_meta.clone();
    let last_modified = http_date_ms(object.meta.last_modified_ms);

    let mut builder = Response::builder()
        .status(status)
        .header(header::ETAG, etag)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, range_len.to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::LAST_MODIFIED, last_modified)
        .header("x-amz-storage-class", storage_class)
        .header("x-amz-request-id", "rust-s3-server");
    if let Some(content_encoding) = content_encoding {
        builder = builder.header(header::CONTENT_ENCODING, content_encoding);
    }
    if let Some(content_language) = content_language {
        builder = builder.header(header::CONTENT_LANGUAGE, content_language);
    }
    if let Some(value) = query.get("response-content-language") {
        builder = builder.header(header::CONTENT_LANGUAGE, value);
    }
    if let Some(value) = query.get("response-cache-control") {
        builder = builder.header(header::CACHE_CONTROL, value);
    }
    if let Some(value) = query.get("response-content-disposition") {
        builder = builder.header(header::CONTENT_DISPOSITION, value);
    }
    if let Some(value) = query.get("response-content-encoding") {
        builder = builder.header(header::CONTENT_ENCODING, value);
    }
    if let Some(value) = query.get("response-expires") {
        builder = builder.header(header::EXPIRES, value);
    }
    for (key, value) in user_meta {
        if let Ok(header_name) = HeaderName::from_bytes(format!("x-amz-meta-{key}").as_bytes()) {
            builder = builder.header(header_name, value);
        }
    }
    if let Some(cr) = content_range {
        builder = builder.header(header::CONTENT_RANGE, cr);
    }

    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        match stream_object_range(
            &object.meta,
            &object.part_offsets,
            &object.object_dir,
            range_start,
            range_len,
        )
        .await
        {
            Ok(body) => body,
            Err(err) => return srv::storage_error_response(err, &format!("/{bucket}/{key}")),
        }
    };
    builder.body(body).unwrap()
}

/// A directory descriptor that keeps identifying this exact object snapshot
/// even after delete/overwrite renames it to trash, so an in-flight GET is
/// never truncated.
#[cfg(unix)]
struct StableObjectDir(std::fs::File);

#[cfg(unix)]
impl StableObjectDir {
    fn open(path: &FsPath) -> std::io::Result<Self> {
        std::fs::File::open(path).map(Self)
    }

    fn open_part(&self, name: &str) -> std::io::Result<tokio::fs::File> {
        use std::os::fd::{AsRawFd, FromRawFd};

        let name = std::ffi::CString::new(name)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL in part name"))?;
        // SAFETY: `self.0` owns a live directory fd, `name` is NUL-terminated,
        // and a successful fd is transferred exactly once into `File`.
        let fd = unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        Ok(tokio::fs::File::from_std(file))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartReadSegment {
    index: usize,
    skip: u64,
    take: u64,
}

/// Streams `range_len` bytes starting at `range_start` from the object's part
/// files without loading the full content into memory. Single-part objects are
/// seeked and wrapped directly; multi-part objects are piped through an
/// in-process duplex channel into one contiguous stream.
async fn stream_object_range(
    meta: &ObjectMeta,
    part_offsets: &[u64],
    object_dir: &FsPath,
    range_start: u64,
    range_len: u64,
) -> Result<Body, StorageError> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    use tokio_util::io::ReaderStream;

    if range_len == 0 {
        return Ok(Body::empty());
    }

    // Fast path: single-part object.
    if meta.parts.len() == 1 {
        let path = object_dir.join(&meta.parts[0].file);
        match tokio::fs::File::open(&path).await {
            Ok(mut file) => {
                if range_start > 0 {
                    if file.seek(SeekFrom::Start(range_start)).await.is_err() {
                        return Err(StorageError::CorruptObject(format!(
                            "failed to seek {}",
                            path.display()
                        )));
                    }
                }
                return Ok(Body::from_stream(ReaderStream::with_capacity(
                    file.take(range_len),
                    STREAM_CHUNK_SIZE,
                )));
            }
            Err(err) => {
                return Err(StorageError::CorruptObject(format!(
                    "failed to open {}: {err}",
                    path.display()
                )))
            }
        }
    }

    let mut segments = multipart_range_segments(meta, part_offsets, range_start, range_len)?
        .into_iter()
        .map(|segment| {
            (
                meta.parts[segment.index].file.clone(),
                segment.skip,
                segment.take,
            )
        })
        .collect::<Vec<_>>();

    if segments.len() == 1 {
        let (name, skip, take) = segments.pop().unwrap();
        let path = object_dir.join(name);
        match tokio::fs::File::open(&path).await {
            Ok(mut file) => {
                if skip > 0 && file.seek(SeekFrom::Start(skip)).await.is_err() {
                    return Err(StorageError::CorruptObject(format!(
                        "failed to seek {}",
                        path.display()
                    )));
                }
                return Ok(Body::from_stream(ReaderStream::with_capacity(
                    file.take(take),
                    STREAM_CHUNK_SIZE,
                )));
            }
            Err(err) => {
                return Err(StorageError::CorruptObject(format!(
                    "failed to open {}: {err}",
                    path.display()
                )))
            }
        }
    }

    let stable_dir = StableObjectDir::open(object_dir).map_err(|err| {
        StorageError::CorruptObject(format!(
            "failed to open object directory {}: {err}",
            object_dir.display()
        ))
    })?;
    let mut segments = segments.into_iter();
    let (first_name, first_skip, first_take) = segments.next().unwrap();
    let mut first_file = stable_dir.open_part(&first_name).map_err(|err| {
        StorageError::CorruptObject(format!("failed to open part {first_name}: {err}"))
    })?;
    if first_skip > 0 {
        first_file
            .seek(SeekFrom::Start(first_skip))
            .await
            .map_err(|_| StorageError::CorruptObject(format!("failed to seek part {first_name}")))?;
    }
    let (mut writer, reader) = tokio::io::duplex(STREAM_CHUNK_SIZE);
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut limited = first_file.take(first_take);
        if tokio::io::copy(&mut limited, &mut writer).await.is_err() {
            return;
        }
        for (name, skip, take) in segments {
            let Ok(mut file) = stable_dir.open_part(&name) else {
                return;
            };
            if skip > 0 && file.seek(SeekFrom::Start(skip)).await.is_err() {
                return;
            }
            let mut limited = file.take(take);
            if tokio::io::copy(&mut limited, &mut writer).await.is_err() {
                return;
            }
        }
    });

    Ok(Body::from_stream(ReaderStream::with_capacity(
        reader,
        STREAM_CHUNK_SIZE,
    )))
}

fn multipart_range_segments(
    meta: &ObjectMeta,
    part_offsets: &[u64],
    range_start: u64,
    range_len: u64,
) -> Result<Vec<PartReadSegment>, StorageError> {
    if range_len == 0 {
        return Ok(Vec::new());
    }
    if meta.parts.is_empty() || meta.parts.len() != part_offsets.len() {
        return Err(StorageError::CorruptObject(
            "object part offset index is invalid".to_string(),
        ));
    }

    let range_end = range_start.saturating_add(range_len);
    let start_index = part_offsets
        .partition_point(|offset| *offset <= range_start)
        .saturating_sub(1);
    let end_index_exclusive = part_offsets
        .partition_point(|offset| *offset < range_end)
        .min(meta.parts.len());

    let mut segments = Vec::with_capacity(end_index_exclusive.saturating_sub(start_index));
    for index in start_index..end_index_exclusive {
        let part = &meta.parts[index];
        let part_start = part_offsets[index];
        let part_end = part_start.saturating_add(part.size);
        if part_start >= range_end {
            break;
        }
        if part_end > range_start {
            let read_from = range_start.max(part_start);
            let read_to = range_end.min(part_end);
            let take = read_to.saturating_sub(read_from);
            if take > 0 {
                segments.push(PartReadSegment {
                    index,
                    skip: read_from - part_start,
                    take,
                });
            }
        }
    }

    if segments.is_empty() {
        return Err(StorageError::CorruptObject(
            "object range maps to no parts".to_string(),
        ));
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::metadata::{ObjectStorageKind, PartMeta};

    fn multipart_meta(sizes: &[u64]) -> ObjectMeta {
        ObjectMeta {
            format_version: 1,
            bucket: "bucket".to_string(),
            object_key: "key".to_string(),
            storage: ObjectStorageKind::Multipart,
            size: sizes.iter().sum(),
            etag: "etag".to_string(),
            last_modified_ms: 1,
            content_type: "application/octet-stream".to_string(),
            content_encoding: None,
            content_language: None,
            storage_class: "STANDARD".to_string(),
            user_meta: std::collections::BTreeMap::new(),
            parts: sizes
                .iter()
                .enumerate()
                .map(|(i, size)| PartMeta {
                    number: (i + 1) as u16,
                    file: format!("part.{}", i + 1),
                    size: *size,
                    etag: format!("etag{}", i + 1),
                })
                .collect(),
        }
    }

    #[test]
    fn multipart_range_segments_binary_searches_to_touched_parts() {
        let meta = multipart_meta(&[5, 5, 5]);
        let offsets = [0, 5, 10];

        assert_eq!(
            multipart_range_segments(&meta, &offsets, 2, 6).unwrap(),
            vec![
                PartReadSegment { index: 0, skip: 2, take: 3 },
                PartReadSegment { index: 1, skip: 0, take: 3 },
            ]
        );
        assert_eq!(
            multipart_range_segments(&meta, &offsets, 5, 5).unwrap(),
            vec![PartReadSegment { index: 1, skip: 0, take: 5 }]
        );
        assert_eq!(
            multipart_range_segments(&meta, &offsets, 12, 3).unwrap(),
            vec![PartReadSegment { index: 2, skip: 2, take: 3 }]
        );
    }

    #[test]
    fn multipart_range_segments_handles_invalid_boundaries_without_panic() {
        let empty_meta = multipart_meta(&[]);
        assert!(multipart_range_segments(&empty_meta, &[], 0, 1).is_err());
        assert_eq!(
            multipart_range_segments(&empty_meta, &[], 0, 0).unwrap(),
            Vec::<PartReadSegment>::new()
        );

        let meta = multipart_meta(&[5]);
        assert!(multipart_range_segments(&meta, &[], 0, 1).is_err());
        assert!(multipart_range_segments(&meta, &[0], 99, 1).is_err());
        assert_eq!(
            multipart_range_segments(&meta, &[0], u64::MAX - 1, 1)
                .unwrap_err()
                .to_string(),
            StorageError::CorruptObject("object range maps to no parts".to_string()).to_string()
        );
    }
}
