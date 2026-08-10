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
use crate::storage::metadata::{quote_etag, ObjectMeta, ObjectStorageKind};
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

    // Conditional request checks. Per RFC 7232 the precedence is:
    //   1. If-Match             -> 412 when it does not match
    //   2. If-Unmodified-Since  -> 412 when the object was modified after
    //   3. If-None-Match        -> 304 when it matches
    //   4. If-Modified-Since    -> 304 when not modified (ignored if If-None-Match present)
    let etag_quoted = quote_etag(&object.meta.etag);
    let server_etag = object.meta.etag.trim_matches('"');

    // 1. If-Match: the client pins a specific ETag (or `*` = "any current
    // version"). A mismatch means the object changed under it -> 412.
    if let Some(im) = headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok()) {
        let matched = im
            .split(',')
            .map(|tok| tok.trim().trim_matches('"'))
            .any(|tok| tok == "*" || tok == server_etag);
        if !matched {
            return srv::s3_error_detailed(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold",
                &format!("/{bucket}/{key}"),
                vec![("Condition".to_string(), "If-Match".to_string())],
            );
        }
    }

    // 2. If-Unmodified-Since: 412 if the object was modified after the given date.
    if let Some(ius) = headers
        .get(header::IF_UNMODIFIED_SINCE)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(since_ms) = parse_http_date_ms(ius) {
            if object.meta.last_modified_ms > since_ms {
                return srv::s3_error_detailed(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold",
                    &format!("/{bucket}/{key}"),
                    vec![("Condition".to_string(), "If-Unmodified-Since".to_string())],
                );
            }
        }
    }

    // 3. If-None-Match.
    let has_if_none_match = headers.contains_key(header::IF_NONE_MATCH);
    if let Some(inm) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    {
        let matched = inm
            .split(',')
            .map(|tok| tok.trim().trim_matches('"'))
            .any(|tok| tok == "*" || tok == server_etag);
        if matched {
            let mut resp = srv::empty_response(StatusCode::NOT_MODIFIED);
            resp.headers_mut()
                .insert(header::ETAG, HeaderValue::from_str(&etag_quoted).unwrap());
            return resp;
        }
    }

    // 4. If-Modified-Since — only consulted when If-None-Match is absent.
    if !has_if_none_match {
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
    }

    let total_size = object.meta.size;

    // `partNumber` reads one part of a completed multipart upload. It is the
    // only way to learn an object's part boundaries from a plain GET/HEAD:
    // `ListParts` needs an `uploadId` and so stops answering the moment the
    // upload completes, yet the object's composite ETag
    // (`md5(md5(part₁) ‖ …)-N`) goes on depending on those boundaries. One
    // HEAD with `partNumber=1` therefore answers both questions a verifying
    // downloader has -- is this object multipart, and if so how is it split --
    // since `x-amz-mp-parts-count` appears only for a multipart object.
    let part_selection = match query.get("partNumber") {
        None => None,
        Some(raw) => {
            // Range and partNumber are two different selectors for the same
            // slot. Honouring one and dropping the other silently returns
            // bytes the caller did not ask for, so refuse the combination.
            if headers.contains_key(header::RANGE) {
                return srv::s3_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    "Cannot specify both Range header and partNumber query parameter",
                    &format!("/{bucket}/{key}"),
                );
            }
            match resolve_part(&object.meta, &object.part_offsets, raw) {
                Ok(selection) => Some(selection),
                // Not a part number at all: the caller's mistake, and no
                // object could have satisfied it.
                Err(PartError::NotAPartNumber) => {
                    return srv::s3_error_detailed(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "Part number must be an integer between 1 and 10000, inclusive",
                        &format!("/{bucket}/{key}"),
                        vec![
                            ("ArgumentName".to_string(), "partNumber".to_string()),
                            ("ArgumentValue".to_string(), raw.to_string()),
                        ],
                    )
                }
                // Well-formed, but this object does not go that far. The reply
                // says how far it does go, so a caller need not probe.
                Err(PartError::OutOfRange { requested, actual }) => {
                    return srv::s3_error_detailed(
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        "InvalidPartNumber",
                        "The requested partnumber is not satisfiable",
                        &format!("/{bucket}/{key}"),
                        vec![
                            ("PartNumberRequested".to_string(), requested.to_string()),
                            ("ActualPartCount".to_string(), actual.to_string()),
                        ],
                    )
                }
            }
        }
    };

    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let selection = parse_range_header(range_header, total_size);

    // Unsatisfiable range: return 416 immediately, with the error document
    // AWS sends. An empty 416 body leaves an SDK with a status and nothing to
    // report -- and real S3 does not send `Content-Range` here either, it
    // names the numbers in the body instead.
    if let RangeSelection::Unsatisfiable { total_size } = selection {
        return srv::s3_error_detailed(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "InvalidRange",
            "The requested range is not satisfiable",
            &format!("/{bucket}/{key}"),
            vec![
                (
                    "RangeRequested".to_string(),
                    range_header.unwrap_or_default().to_string(),
                ),
                ("ActualObjectSize".to_string(), total_size.to_string()),
            ],
        );
    }

    let (status, range_start, range_len, content_range) = match &part_selection {
        // A part is served as the ranged read it is -- 206 with a
        // Content-Range -- whether or not the object is multipart. AWS answers
        // `partNumber=1` on an ordinary PUT object the same way, with a range
        // spanning the whole object; it is `x-amz-mp-parts-count`, not the
        // status, that distinguishes the two cases.
        Some(part) if part.len > 0 => (
            StatusCode::PARTIAL_CONTENT,
            part.start,
            part.len,
            Some(format!(
                "bytes {}-{}/{total_size}",
                part.start,
                part.start + part.len - 1
            )),
        ),
        // A zero-length part names no satisfiable byte range, so it is
        // answered plainly rather than with a Content-Range that would have to
        // claim a byte that isn't there.
        Some(part) => (StatusCode::OK, part.start, part.len, None),
        None => match &selection {
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
        },
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
        .header("x-amz-request-id", "rust-s3-server");
    // AWS sends this header only for a non-default class; a STANDARD object
    // carries no `x-amz-storage-class` at all.
    if storage_class != "STANDARD" {
        builder = builder.header("x-amz-storage-class", storage_class);
    }
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
    // Present only for a genuine multipart object, which makes its presence
    // the single-probe answer to "is this object multipart?".
    if let Some(count) = part_selection.as_ref().and_then(|part| part.parts_count) {
        builder = builder.header("x-amz-mp-parts-count", count.to_string());
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

/// The byte span one `partNumber` resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartSelection {
    start: u64,
    len: u64,
    /// The object's total part count, and `None` when the object is not a
    /// multipart one at all. Drives the `x-amz-mp-parts-count` header, whose
    /// presence is therefore exactly "this object is multipart" — the single
    /// fact a verifying downloader needs before deciding whether its ETag is a
    /// plain MD5 or a composite.
    parts_count: Option<usize>,
}

/// Why a `partNumber` could not be served. The two outcomes are different
/// statuses on AWS, and conflating them would tell a client the wrong thing:
/// a malformed number is the caller's mistake (400, retrying won't help), a
/// number past the end is a fact about this object (416, and the response says
/// how many parts there actually are).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartError {
    /// Outside 1..=10_000, or not an integer at all.
    NotAPartNumber,
    /// A well-formed part number that this object does not have.
    OutOfRange { requested: u32, actual: usize },
}

/// AWS's inclusive ceiling on part numbers.
const MAX_PART_NUMBER: u32 = 10_000;

/// Resolves a raw `partNumber` value against an object.
///
/// A single-part object still answers `partNumber=1`, with the whole object:
/// something PUT rather than assembled is one part by definition. It reports
/// no part count, and that absence is what tells a caller its ETag needs no
/// composite reconstruction.
fn resolve_part(
    meta: &ObjectMeta,
    part_offsets: &[u64],
    raw: &str,
) -> Result<PartSelection, PartError> {
    let number = raw
        .trim()
        .parse::<u32>()
        .ok()
        .filter(|n| (1..=MAX_PART_NUMBER).contains(n))
        .ok_or(PartError::NotAPartNumber)?;

    if meta.storage != ObjectStorageKind::Multipart {
        // One part, so only part one exists. AWS answers it as a range over
        // the whole object -- a 206 with a full-width Content-Range -- rather
        // than as an unranged read.
        return match number {
            1 => Ok(PartSelection {
                start: 0,
                len: meta.size,
                parts_count: None,
            }),
            _ => Err(PartError::OutOfRange {
                requested: number,
                actual: 1,
            }),
        };
    }
    // The offsets are computed alongside `meta.parts`; a mismatch means the
    // index is inconsistent, and inventing a span from half of it would serve
    // the wrong bytes under a correct-looking Content-Range.
    if part_offsets.len() != meta.parts.len() {
        return Err(PartError::OutOfRange {
            requested: number,
            actual: meta.parts.len(),
        });
    }
    let index = number as usize - 1;
    match (meta.parts.get(index), part_offsets.get(index)) {
        (Some(part), Some(start)) => Ok(PartSelection {
            start: *start,
            len: part.size,
            parts_count: Some(meta.parts.len()),
        }),
        _ => Err(PartError::OutOfRange {
            requested: number,
            actual: meta.parts.len(),
        }),
    }
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
                    last_modified_ms: 1,
                })
                .collect(),
        }
    }

    #[test]
    fn resolve_part_maps_each_part_number_to_its_span() {
        let meta = multipart_meta(&[5, 7, 3]);
        let offsets = [0, 5, 12];

        assert_eq!(
            resolve_part(&meta, &offsets, "1").unwrap(),
            PartSelection { start: 0, len: 5, parts_count: Some(3) }
        );
        assert_eq!(
            resolve_part(&meta, &offsets, "2").unwrap(),
            PartSelection { start: 5, len: 7, parts_count: Some(3) }
        );
        // The last part is the one whose size differs; getting its offset
        // right is what makes the reported span usable as a byte range.
        assert_eq!(
            resolve_part(&meta, &offsets, "3").unwrap(),
            PartSelection { start: 12, len: 3, parts_count: Some(3) }
        );
    }

    #[test]
    fn resolve_part_separates_a_bad_number_from_a_missing_part() {
        // AWS makes these different statuses -- 400 vs 416 -- because they say
        // different things: one is the caller's mistake, the other is a fact
        // about this object, reported with the count it actually has.
        let meta = multipart_meta(&[5, 5]);
        let offsets = [0, 5];

        assert_eq!(
            resolve_part(&meta, &offsets, "3"),
            Err(PartError::OutOfRange { requested: 3, actual: 2 })
        );
        for bad in ["0", "-1", "abc", "", "10001", "99999999999999999999"] {
            assert_eq!(
                resolve_part(&meta, &offsets, bad),
                Err(PartError::NotAPartNumber),
                "partNumber={bad} is not a part number"
            );
        }
        // 10,000 is inclusive, so it is a *valid* number that this object
        // merely does not have.
        assert_eq!(
            resolve_part(&meta, &offsets, "10000"),
            Err(PartError::OutOfRange { requested: 10_000, actual: 2 })
        );
    }

    #[test]
    fn resolve_part_treats_a_single_part_object_as_one_whole_part() {
        // A PUT object answers partNumber=1 with the entire object and no
        // part count -- the absence is what tells a client its ETag is a
        // plain MD5 rather than a composite.
        let mut meta = multipart_meta(&[400]);
        meta.storage = ObjectStorageKind::Single;

        assert_eq!(
            resolve_part(&meta, &[0], "1").unwrap(),
            PartSelection { start: 0, len: 400, parts_count: None }
        );
        assert_eq!(
            resolve_part(&meta, &[0], "2"),
            Err(PartError::OutOfRange { requested: 2, actual: 1 })
        );
    }

    #[test]
    fn resolve_part_refuses_an_inconsistent_offset_index() {
        // Serving a span computed from a half-valid index would hand back the
        // wrong bytes under a correct-looking Content-Range.
        let meta = multipart_meta(&[5, 5]);
        assert_eq!(
            resolve_part(&meta, &[0], "1"),
            Err(PartError::OutOfRange { requested: 1, actual: 2 })
        );
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
