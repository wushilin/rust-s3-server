//! `GET /{bucket}?uploads` — list in-progress multipart uploads.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::BucketCtx;
use crate::server::xml::list_multipart_uploads_xml;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, _body: Body) -> Response {
    match store.list_multipart_uploads(&ctx.bucket).await {
        Ok(uploads) => {
            let count = uploads.len();
            srv::with_measure(
                srv::xml_response(
                    StatusCode::OK,
                    list_multipart_uploads_xml(&ctx.bucket, &uploads),
                ),
                srv::OperationMeasure::Uploads(count),
            )
        }
        Err(err) => srv::storage_error_response(err, &ctx.resource()),
    }
}
