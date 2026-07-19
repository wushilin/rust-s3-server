//! `DELETE /{bucket}` — delete an empty bucket.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::BucketCtx;
use crate::storage::errors::StorageError;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, _body: Body) -> Response {
    match store.delete_bucket(&ctx.bucket).await {
        Ok(()) => srv::empty_response(StatusCode::NO_CONTENT),
        Err(StorageError::InvalidMultipartUpload(_)) => srv::s3_error(
            StatusCode::CONFLICT,
            "BucketNotEmpty",
            "Bucket is not empty",
            &ctx.bucket,
        ),
        Err(err) => srv::storage_error_response(err, &ctx.resource()),
    }
}
