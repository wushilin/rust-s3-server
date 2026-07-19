//! `HEAD /{bucket}` — check whether a bucket exists.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::BucketCtx;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, _body: Body) -> Response {
    if store.bucket_exists(&ctx.bucket).await {
        srv::empty_response(StatusCode::OK)
    } else {
        srv::s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "The specified bucket does not exist",
            &ctx.resource(),
        )
    }
}
