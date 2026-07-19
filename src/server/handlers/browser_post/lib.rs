//! `POST /{bucket}` with `multipart/form-data` — browser-style form upload.
//!
//! This module owns its multipart parsing end to end. Authorization (form
//! SigV4 POST-policy verification) runs before any storage write; with auth
//! disabled the upload is accepted like any other open-access request.

use std::collections::BTreeMap;

use axum::body::{to_bytes, Body};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;

use crate::server as srv;
use crate::server::auth::authorize_browser_post;
use crate::server::handlers::BucketCtx;
use crate::server::OperationActor;
use crate::storage::metadata::quote_etag;
use crate::storage::store::LocalObjectStore;

/// True for a `multipart/form-data` request — the router uses this to pick
/// this verb.
pub(crate) fn is_form(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("multipart/form-data"))
        .unwrap_or(false)
}

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, body: Body) -> Response {
    let bucket = ctx.bucket.clone();
    let boundary = match multipart_boundary(&ctx.headers) {
        Some(v) => v,
        None => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing multipart boundary",
                &format!("/{bucket}"),
            )
        }
    };
    let raw = match to_bytes(body, 128 * 1024 * 1024).await {
        Ok(v) => v,
        Err(err) => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                err.to_string(),
                &format!("/{bucket}"),
            )
        }
    };
    let form = match parse_multipart_form(&raw, &boundary) {
        Ok(v) => v,
        Err(message) => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "MalformedPOSTRequest",
                message,
                &format!("/{bucket}"),
            )
        }
    };
    let key = match form.fields.get("key").filter(|v| !v.is_empty()) {
        Some(v) => v.clone(),
        None => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Missing form key field",
                &format!("/{bucket}"),
            )
        }
    };
    let file = match form.file {
        Some(v) => v,
        None => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Missing form file field",
                &format!("/{bucket}"),
            )
        }
    };

    // Authorize before touching storage.
    let actor = if let Some(state) = &ctx.auth_state {
        match authorize_browser_post(state, &form.fields, &bucket, &key) {
            Ok(actor) => actor,
            Err(resp) => return resp,
        }
    } else {
        OperationActor::default()
    };

    let mut user_meta = BTreeMap::new();
    for (name, value) in &form.fields {
        if let Some(meta_key) = name.strip_prefix("x-amz-meta-") {
            user_meta.insert(meta_key.to_ascii_lowercase(), value.clone());
        }
    }
    let storage_class = form.fields.get("x-amz-storage-class").map(String::as_str);
    if let Some(resp) = srv::reject_invalid_storage_class(storage_class, &format!("/{bucket}/{key}"))
    {
        return resp;
    }
    let content_type = file
        .content_type
        .as_deref()
        .or_else(|| form.fields.get("Content-Type").map(String::as_str))
        .or_else(|| form.fields.get("content-type").map(String::as_str));
    let content_language = form
        .fields
        .get("Content-Language")
        .or_else(|| form.fields.get("content-language"))
        .map(String::as_str);
    let staging_id = match store
        .stage_put_with_metadata(
            &bucket,
            &key,
            &file.bytes,
            content_type,
            None,
            storage_class,
            content_language,
            &user_meta,
        )
        .await
    {
        Ok(v) => v,
        Err(err) => return srv::storage_error_response(err, &format!("/{bucket}/{key}")),
    };
    let mut response = match store.commit_staged_put(&bucket, &key, &staging_id).await {
        Ok(result) => {
            let location = format!("/{bucket}/{key}");
            srv::with_measure(
                srv::xml_response(
                    StatusCode::CREATED,
                    post_object_xml(&location, &bucket, &key, &result.etag),
                ),
                srv::OperationMeasure::Bytes(result.size),
            )
        }
        Err(err) => srv::storage_error_response(err, &format!("/{bucket}/{key}")),
    };
    response.extensions_mut().insert(actor);
    response
}

#[derive(Debug)]
struct MultipartForm {
    fields: BTreeMap<String, String>,
    file: Option<FormFile>,
}

#[derive(Debug)]
struct FormFile {
    content_type: Option<String>,
    bytes: Vec<u8>,
}

fn multipart_boundary(headers: &HeaderMap) -> Option<String> {
    let content_type = headers.get(header::CONTENT_TYPE)?.to_str().ok()?;
    content_type.split(';').find_map(|part| {
        let part = part.trim();
        part.strip_prefix("boundary=")
            .map(|v| v.trim_matches('"').to_string())
    })
}

fn parse_multipart_form(raw: &[u8], boundary: &str) -> Result<MultipartForm, &'static str> {
    let marker = format!("--{boundary}").into_bytes();
    let mut fields = BTreeMap::new();
    let mut file = None;
    for mut part in split_bytes(raw, &marker).into_iter().skip(1) {
        part = trim_prefix_bytes(part, b"\r\n");
        if part.starts_with(b"--") {
            break;
        }
        let Some(header_end) = find_bytes(part, b"\r\n\r\n") else {
            continue;
        };
        let headers_raw = &part[..header_end];
        let mut body_raw = &part[header_end + 4..];
        body_raw = trim_suffix_bytes(body_raw, b"\r\n");
        let mut name = None;
        let mut filename = None;
        let mut content_type = None;
        let headers_text =
            std::str::from_utf8(headers_raw).map_err(|_| "Multipart headers are not UTF-8")?;
        for line in headers_text.split("\r\n") {
            let Some((header_name, header_value)) = line.split_once(':') else {
                continue;
            };
            if header_name.eq_ignore_ascii_case("content-disposition") {
                for attr in header_value.split(';').map(str::trim) {
                    if let Some(v) = attr.strip_prefix("name=") {
                        name = Some(v.trim_matches('"').to_string());
                    } else if let Some(v) = attr.strip_prefix("filename=") {
                        filename = Some(v.trim_matches('"').to_string());
                    }
                }
            } else if header_name.eq_ignore_ascii_case("content-type") {
                content_type = Some(header_value.trim().to_string());
            }
        }
        let Some(name) = name else {
            continue;
        };
        if filename.is_some() || name == "file" {
            file = Some(FormFile {
                content_type,
                bytes: body_raw.to_vec(),
            });
        } else {
            fields.insert(
                name,
                String::from_utf8(body_raw.to_vec())
                    .map_err(|_| "Multipart form field is not UTF-8")?,
            );
        }
    }
    Ok(MultipartForm { fields, file })
}

fn split_bytes<'a>(haystack: &'a [u8], needle: &[u8]) -> Vec<&'a [u8]> {
    let mut parts = Vec::new();
    let mut start = 0;
    while let Some(pos) = find_bytes(&haystack[start..], needle) {
        parts.push(&haystack[start..start + pos]);
        start += pos + needle.len();
    }
    parts.push(&haystack[start..]);
    parts
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn trim_prefix_bytes<'a>(value: &'a [u8], prefix: &[u8]) -> &'a [u8] {
    value.strip_prefix(prefix).unwrap_or(value)
}

fn trim_suffix_bytes<'a>(value: &'a [u8], suffix: &[u8]) -> &'a [u8] {
    value.strip_suffix(suffix).unwrap_or(value)
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
