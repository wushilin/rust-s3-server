//! `PUT /{bucket}/{key}?uploadId=…&partNumber=…` with `x-amz-copy-source` —
//! upload a multipart part by copying (a byte range of) an existing object.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::server::xml::upload_part_copy_xml;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, _body: Body) -> Response {
    let resource = ctx.resource();
    let upload_id = ctx.query.get("uploadId").cloned().unwrap_or_default();
    let part_number = match ctx.query.get("partNumber").and_then(|v| v.parse::<u16>().ok()) {
        Some(n) => n,
        None => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid partNumber",
                &resource,
            )
        }
    };
    let copy_source = ctx
        .headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let (src_bucket, src_key) = match srv::parse_copy_source(&copy_source) {
        Some(v) => v,
        None => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid x-amz-copy-source",
                &resource,
            )
        }
    };
    let range = match srv::parse_copy_source_range(&ctx.headers) {
        Ok(range) => range,
        Err(message) => {
            return srv::s3_error(StatusCode::BAD_REQUEST, "InvalidArgument", message, &resource)
        }
    };
    match store
        .copy_multipart_part(
            &ctx.bucket,
            &ctx.key,
            &upload_id,
            part_number,
            &src_bucket,
            &src_key,
            range,
        )
        .await
    {
        Ok(result) => srv::with_measure(
            srv::xml_response(
                StatusCode::OK,
                upload_part_copy_xml(&result.etag, result.last_modified_ms),
            ),
            srv::OperationMeasure::Bytes(result.size),
        ),
        Err(err) => srv::storage_error_response(err, &resource),
    }
}
