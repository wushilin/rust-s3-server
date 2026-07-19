//! `PUT /{bucket}/{key}?uploadId=…&partNumber=…` — upload one multipart part.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, body: Body) -> Response {
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
    let aws_chunked = srv::is_aws_chunked(&ctx.headers);
    let expected_sha256 = srv::expected_payload_sha256(&ctx.headers, aws_chunked);
    match store
        .put_multipart_part_stream(
            &ctx.bucket,
            &ctx.key,
            &upload_id,
            part_number,
            body.into_data_stream(),
            aws_chunked,
            expected_sha256.as_deref(),
        )
        .await
    {
        Ok(result) => srv::with_measure(
            srv::empty_response_with_etag(StatusCode::OK, &result.etag),
            srv::OperationMeasure::Bytes(result.size),
        ),
        Err(err) => srv::storage_error_response(err, &resource),
    }
}
