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
use crate::server::auth::authorize_browser_post;
use crate::server::handlers::BucketCtx;
use crate::server::OperationActor;
use crate::storage::metadata::quote_etag;
use crate::storage::store::LocalObjectStore;

const MAX_FORM_FIELD_BYTES: usize = 2 * 1024 * 1024;

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
    let form = match parse_multipart_form(body, &boundary).await {
        Ok(v) => v,
        Err(err) => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "MalformedPOSTRequest",
                err,
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
    let input = match tokio::fs::File::open(file.temp.path()).await {
        Ok(file) => file,
        Err(err) => return srv::storage_error_response(err.into(), &format!("/{bucket}/{key}")),
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
    temp: tempfile::NamedTempFile,
}

fn multipart_boundary(headers: &HeaderMap) -> Option<String> {
    let content_type = headers.get(header::CONTENT_TYPE)?.to_str().ok()?;
    content_type.split(';').find_map(|part| {
        let part = part.trim();
        part.strip_prefix("boundary=")
            .map(|v| v.trim_matches('"').to_string())
    })
}

async fn parse_multipart_form(body: Body, boundary: &str) -> Result<MultipartForm, String> {
    if boundary.is_empty() {
        return Err("Multipart boundary is empty".to_string());
    }
    let mut multipart = multer::Multipart::new(body.into_data_stream(), boundary);
    let mut fields = BTreeMap::new();
    let mut file = None;
    while let Some(mut part) = multipart
        .next_field()
        .await
        .map_err(|err| format!("Invalid multipart body: {err}"))?
    {
        let Some(name) = part.name().map(str::to_string) else {
            continue;
        };
        let is_file = part.file_name().is_some() || name == "file";
        let content_type = part.content_type().map(ToString::to_string);
        if is_file {
            let temp = tempfile::NamedTempFile::new()
                .map_err(|err| format!("Could not create upload spool: {err}"))?;
            let std_file = temp
                .reopen()
                .map_err(|err| format!("Could not open upload spool: {err}"))?;
            let mut output = tokio::fs::File::from_std(std_file);
            while let Some(chunk) = part
                .chunk()
                .await
                .map_err(|err| format!("Invalid multipart file data: {err}"))?
            {
                output
                    .write_all(&chunk)
                    .await
                    .map_err(|err| format!("Could not spool upload: {err}"))?;
            }
            output
                .flush()
                .await
                .map_err(|err| format!("Could not flush upload spool: {err}"))?;
            file = Some(FormFile {
                content_type,
                temp,
            });
        } else {
            let mut value = Vec::new();
            while let Some(chunk) = part
                .chunk()
                .await
                .map_err(|err| format!("Invalid multipart field data: {err}"))?
            {
                if value.len().saturating_add(chunk.len()) > MAX_FORM_FIELD_BYTES {
                    return Err(format!("Multipart field {name} is too large"));
                }
                value.extend_from_slice(&chunk);
            }
            let value = String::from_utf8(value)
                .map_err(|_| format!("Multipart field {name} is not UTF-8"))?;
            fields.insert(name, value);
        }
    }
    Ok(MultipartForm { fields, file })
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
        let parsed = parse_multipart_form(Body::from(body), "BOUNDARY")
            .await
            .unwrap();
        assert_eq!(parsed.fields.get("key").map(String::as_str), Some("object"));
        let file = parsed.file.unwrap();
        assert_eq!(tokio::fs::read(file.temp.path()).await.unwrap(), b"abc--BOUNDARYxyz");
    }
}
