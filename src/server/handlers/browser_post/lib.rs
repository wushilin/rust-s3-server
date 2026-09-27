//! `POST /{bucket}` with `multipart/form-data` — browser-style form upload.
//!
//! This module owns its multipart parsing end to end. Authorization (form
//! SigV4 POST-policy verification) runs before any storage write; with auth
//! disabled the upload is accepted like any other open-access request.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use crate::server as srv;
use crate::server::auth::{authorize_browser_post, BrowserPostGrant};
use crate::server::handlers::BucketCtx;
use crate::server::policy::Requirement;
use crate::storage::metadata::quote_etag;
use crate::storage::store::LocalObjectStore;

/// Most non-file form fields accepted.
const MAX_FORM_FIELDS: usize = 128;
/// Total bytes of all non-file form fields (names and values).
const MAX_FORM_FIELDS_TOTAL_BYTES: usize = 256 * 1024;
/// Largest object a form POST may upload (S3's single-PUT limit).
const MAX_POST_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// True for a `multipart/form-data` request — the router uses this to pick
/// this verb.
pub(crate) fn is_form(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("multipart/form-data"))
        .unwrap_or(false)
}

fn field<'a>(fields: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, body: Body) -> Response {
    let bucket = ctx.bucket.clone();
    let bad = |code: &str, message: String| {
        srv::s3_error(StatusCode::BAD_REQUEST, code, message, &format!("/{bucket}"))
    };
    // A form POST that also names a multipart operation (`?uploads`,
    // `?uploadId`) is not a browser upload: S3 has no such bucket operation.
    // Such a request was authenticated through the header path against
    // `bucket/*` only, so accepting it here would store a form-chosen key
    // without the key-level policy check.
    if ctx.query.contains_key("uploads") || ctx.query.contains_key("uploadId") {
        return bad(
            "InvalidRequest",
            "A browser POST upload cannot select a multipart operation".to_string(),
        );
    }
    let boundary = match multipart_boundary(&ctx.headers) {
        Some(v) if !v.is_empty() => v,
        _ => return bad("InvalidRequest", "Missing multipart boundary".to_string()),
    };
    let mut multipart = multer::Multipart::new(body.into_data_stream(), boundary);
    // Fields first: S3 requires the file to be the last field, so everything
    // the policy covers is known before a single file byte is read.
    let (fields, file) = match read_form_fields(&mut multipart).await {
        Ok(v) => v,
        Err(err) => return bad("MalformedPOSTRequest", err),
    };
    let key = match fields.get("key").filter(|v| !v.is_empty()) {
        Some(v) => v.clone(),
        None => return bad("InvalidArgument", "Missing form key field".to_string()),
    };
    let Some(file) = file else {
        return bad("InvalidArgument", "Missing form file field".to_string());
    };

    // S3 POST uploads support the `${filename}` variable in the key, replaced
    // with the uploaded file's own name (e.g. `uploads/${filename}` ->
    // `uploads/photo.jpg`). Without this the literal `${filename}` is stored.
    let key = if key.contains("${filename}") {
        key.replace("${filename}", file.filename.as_deref().unwrap_or(""))
    } else {
        key
    };
    let resource = format!("/{bucket}/{key}");

    // Authorize (signature + policy) before reading the file or touching
    // storage.
    let grant = if let Some(state) = &ctx.auth_state {
        match authorize_browser_post(state, &fields, &bucket, &key) {
            Ok(grant) => {
                state.record_key_use(grant.actor.access_key.as_deref(), ctx.client_ip.as_deref());
                grant
            }
            Err(resp) => return resp,
        }
    } else {
        // Not deferred by the auth layer: either auth is disabled (no
        // identity) or the request was header-authenticated, in which case its
        // identity must be allowed to write this very key.
        if let Some(identity) = &ctx.identity {
            if !identity.authorize(&[Requirement::object("s3:PutObject", &bucket, &key)]) {
                return srv::s3_error(
                    StatusCode::FORBIDDEN,
                    "AccessDenied",
                    "Access Denied",
                    &resource,
                );
            }
        }
        BrowserPostGrant::default()
    };

    let (min_len, max_len) = match grant.content_length_range {
        Some((min, max)) => (min, max.min(MAX_POST_OBJECT_BYTES)),
        None => (0, MAX_POST_OBJECT_BYTES),
    };
    // The spool is a `NamedTempFile`: every early return below drops (and so
    // deletes) it.
    let spooled = match spool_file(file.field, max_len).await {
        Ok(v) => v,
        Err(SpoolError::TooLarge) => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "EntityTooLarge",
                "Your proposed upload exceeds the maximum allowed size",
                &resource,
            )
        }
        Err(SpoolError::Other(err)) => return bad("MalformedPOSTRequest", err),
    };
    if spooled.size < min_len {
        return srv::s3_error(
            StatusCode::BAD_REQUEST,
            "EntityTooSmall",
            "Your proposed upload is smaller than the minimum allowed size",
            &resource,
        );
    }

    let mut user_meta = BTreeMap::new();
    for (name, value) in &fields {
        if let Some(meta_key) = name.to_ascii_lowercase().strip_prefix("x-amz-meta-") {
            user_meta.insert(meta_key.to_string(), value.clone());
        }
    }
    let storage_class = field(&fields, "x-amz-storage-class");
    if let Some(resp) = srv::reject_invalid_storage_class(storage_class, &resource) {
        return resp;
    }
    // The `Content-Type` form field (what a POST policy can constrain) wins
    // over the file part's own header, as in S3.
    let content_type = field(&fields, "Content-Type").or(file.content_type.as_deref());
    let content_language = field(&fields, "Content-Language");
    let input = match tokio::fs::File::open(spooled.temp.path()).await {
        Ok(file) => file,
        Err(err) => return srv::storage_error_response(err.into(), &resource),
    };
    let staging_id = match store
        .stage_put_stream_with_metadata(
            &bucket,
            &key,
            ReaderStream::new(input),
            content_type,
            None,
            storage_class,
            content_language,
            &user_meta,
            None,
        )
        .await
    {
        Ok(v) => v,
        Err(err) => return srv::storage_error_response(err, &resource),
    };
    let mut response = match store.commit_staged_put(&bucket, &key, &staging_id, None).await {
        Ok(result) => srv::with_measure(
            srv::xml_response(
                StatusCode::CREATED,
                post_object_xml(&resource, &bucket, &key, &result.etag),
            ),
            srv::OperationMeasure::Bytes(result.size),
        ),
        Err(err) => srv::storage_error_response(err, &resource),
    };
    response.extensions_mut().insert(grant.actor);
    response
}

struct FilePart {
    field: multer::Field<'static>,
    content_type: Option<String>,
    filename: Option<String>,
}

struct SpooledFile {
    temp: tempfile::NamedTempFile,
    size: u64,
}

enum SpoolError {
    TooLarge,
    Other(String),
}

fn multipart_boundary(headers: &HeaderMap) -> Option<String> {
    let content_type = headers.get(header::CONTENT_TYPE)?.to_str().ok()?;
    content_type.split(';').find_map(|part| {
        let part = part.trim();
        part.strip_prefix("boundary=")
            .map(|v| v.trim_matches('"').to_string())
    })
}

/// Reads the non-file form fields up to (not into) the file part, bounded in
/// count and total size. Fields after the file are never read (S3 ignores
/// them). A field name repeated (case-insensitively) is rejected, so the
/// policy check and the upload can never read different occurrences.
async fn read_form_fields(
    multipart: &mut multer::Multipart<'static>,
) -> Result<(BTreeMap<String, String>, Option<FilePart>), String> {
    let mut fields = BTreeMap::new();
    let mut total = 0usize;
    while let Some(mut part) = multipart
        .next_field()
        .await
        .map_err(|err| format!("Invalid multipart body: {err}"))?
    {
        let Some(name) = part.name().map(str::to_string) else {
            continue;
        };
        let filename = part.file_name().map(str::to_string);
        if filename.is_some() || name.eq_ignore_ascii_case("file") {
            let content_type = part.content_type().map(ToString::to_string);
            return Ok((
                fields,
                Some(FilePart {
                    field: part,
                    content_type,
                    filename,
                }),
            ));
        }
        if fields.len() >= MAX_FORM_FIELDS {
            return Err("Too many multipart form fields".to_string());
        }
        if fields.keys().any(|k: &String| k.eq_ignore_ascii_case(&name)) {
            return Err(format!("Multipart field {name} is repeated"));
        }
        total = total.saturating_add(name.len());
        let mut value = Vec::new();
        while let Some(chunk) = part
            .chunk()
            .await
            .map_err(|err| format!("Invalid multipart field data: {err}"))?
        {
            total = total.saturating_add(chunk.len());
            if total > MAX_FORM_FIELDS_TOTAL_BYTES {
                return Err("Multipart form fields are too large".to_string());
            }
            value.extend_from_slice(&chunk);
        }
        let value = String::from_utf8(value)
            .map_err(|_| format!("Multipart field {name} is not UTF-8"))?;
        fields.insert(name, value);
    }
    Ok((fields, None))
}

/// Spools the file part to a temp file, failing as soon as it exceeds
/// `max_len` bytes. On any error the temp file is dropped, which deletes it.
async fn spool_file(mut part: multer::Field<'static>, max_len: u64) -> Result<SpooledFile, SpoolError> {
    let temp = tempfile::NamedTempFile::new()
        .map_err(|err| SpoolError::Other(format!("Could not create upload spool: {err}")))?;
    let std_file = temp
        .reopen()
        .map_err(|err| SpoolError::Other(format!("Could not open upload spool: {err}")))?;
    let mut output = tokio::fs::File::from_std(std_file);
    let mut size = 0u64;
    while let Some(chunk) = part
        .chunk()
        .await
        .map_err(|err| SpoolError::Other(format!("Invalid multipart file data: {err}")))?
    {
        size = size.saturating_add(chunk.len() as u64);
        if size > max_len {
            return Err(SpoolError::TooLarge);
        }
        output
            .write_all(&chunk)
            .await
            .map_err(|err| SpoolError::Other(format!("Could not spool upload: {err}")))?;
    }
    output
        .flush()
        .await
        .map_err(|err| SpoolError::Other(format!("Could not flush upload spool: {err}")))?;
    Ok(SpooledFile { temp, size })
}

fn post_object_xml(location: &str, bucket: &str, key: &str, etag: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><PostResponse><Location>{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></PostResponse>"#,
        escape_xml_local(location),
        escape_xml_local(bucket),
        escape_xml_local(key),
        escape_xml_local(&quote_etag(etag)),
    )
}

fn escape_xml_local(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}


#[cfg(test)]
mod tests {
    use super::*;

    fn multipart(body: &'static str) -> multer::Multipart<'static> {
        multer::Multipart::new(Body::from(body).into_data_stream(), "BOUNDARY")
    }

    #[tokio::test]
    async fn multipart_boundary_text_inside_file_is_preserved() {
        let body = concat!(
            "--BOUNDARY\r\n",
            "Content-Disposition: form-data; name=\"key\"\r\n\r\n",
            "object\r\n",
            "--BOUNDARY\r\n",
            "Content-Disposition: form-data; name=\"file\"; filename=\"x.bin\"\r\n",
            "Content-Type: application/octet-stream\r\n\r\n",
            "abc--BOUNDARYxyz\r\n",
            "--BOUNDARY--\r\n",
        );
        let mut form = multipart(body);
        let (fields, file) = read_form_fields(&mut form).await.unwrap();
        assert_eq!(fields.get("key").map(String::as_str), Some("object"));
        let spooled = spool_file(file.unwrap().field, u64::MAX).await.ok().unwrap();
        assert_eq!(tokio::fs::read(spooled.temp.path()).await.unwrap(), b"abc--BOUNDARYxyz");
    }

    #[tokio::test]
    async fn file_over_the_limit_is_rejected_while_spooling() {
        let body = concat!(
            "--BOUNDARY\r\n",
            "Content-Disposition: form-data; name=\"file\"; filename=\"x.bin\"\r\n\r\n",
            "0123456789\r\n",
            "--BOUNDARY--\r\n",
        );
        let mut form = multipart(body);
        let (_, file) = read_form_fields(&mut form).await.unwrap();
        assert!(matches!(spool_file(file.unwrap().field, 5).await, Err(SpoolError::TooLarge)));
    }

    #[tokio::test]
    async fn field_count_size_and_repeats_are_bounded() {
        let mut many = String::new();
        for i in 0..=MAX_FORM_FIELDS {
            many.push_str(&format!("--BOUNDARY\r\nContent-Disposition: form-data; name=\"f{i}\"\r\n\r\nv\r\n"));
        }
        many.push_str("--BOUNDARY--\r\n");
        let mut form = multer::Multipart::new(Body::from(many).into_data_stream(), "BOUNDARY");
        assert!(read_form_fields(&mut form).await.is_err());

        let big = format!(
            "--BOUNDARY\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\n{}\r\n--BOUNDARY--\r\n",
            "x".repeat(MAX_FORM_FIELDS_TOTAL_BYTES + 1)
        );
        let mut form = multer::Multipart::new(Body::from(big).into_data_stream(), "BOUNDARY");
        assert!(read_form_fields(&mut form).await.is_err());

        let repeated = concat!(
            "--BOUNDARY\r\nContent-Disposition: form-data; name=\"Content-Type\"\r\n\r\na\r\n",
            "--BOUNDARY\r\nContent-Disposition: form-data; name=\"content-type\"\r\n\r\nb\r\n",
            "--BOUNDARY--\r\n",
        );
        assert!(read_form_fields(&mut multipart(repeated)).await.is_err());
    }
}
