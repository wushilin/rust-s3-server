//! `GET /{bucket}?location` — return the bucket region.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::BucketCtx;
use crate::storage::errors::StorageError;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, _body: Body) -> Response {
    if !store.bucket_exists(&ctx.bucket).await {
        return srv::storage_error_response(
            StorageError::BucketNotFound(ctx.bucket.clone()),
            &ctx.resource(),
        );
    }
    // Empty LocationConstraint means "default region / us-east-1". Returning a
    // real AWS region name makes some clients (mc) redirect to that AWS
    // regional endpoint, where the bucket does not exist.
    srv::xml_response(
        StatusCode::OK,
        r#"<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>"#.to_string(),
    )
}
