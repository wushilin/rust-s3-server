//! `POST /{bucket}/{key}?uploadId=…` — complete a multipart upload.

use axum::body::{to_bytes, Body};
use axum::http::{header, StatusCode};
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::server::xml::complete_multipart_xml;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, body: Body) -> Response {
    let resource = ctx.resource();
    let upload_id = ctx.query.get("uploadId").cloned().unwrap_or_default();
    let raw = match to_bytes(body, 1024 * 1024).await {
        Ok(raw) => raw,
        Err(err) => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                err.to_string(),
                &resource,
            )
        }
    };
    let parts = match srv::parse_complete_parts_xml(&String::from_utf8_lossy(&raw)) {
        Ok(parts) => parts,
        Err(message) => {
            return srv::s3_error(StatusCode::BAD_REQUEST, "MalformedXML", message, &resource)
        }
    };
    match store
        .complete_multipart(&ctx.bucket, &ctx.key, &upload_id, &parts)
        .await
    {
        Ok(result) => {
            let host = ctx
                .headers
                .get(header::HOST)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("127.0.0.1");
            let location = format!("http://{host}/{}/{}", ctx.bucket, ctx.key);
            srv::with_measure(
                srv::xml_response(
                    StatusCode::OK,
                    complete_multipart_xml(&location, &ctx.bucket, &ctx.key, &result.etag),
                ),
                srv::OperationMeasure::Bytes(result.size),
            )
        }
        Err(err) => srv::storage_error_response(err, &resource),
    }
}
