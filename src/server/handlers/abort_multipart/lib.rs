//! `DELETE /{bucket}/{key}?uploadId=…` — abort an in-progress multipart upload.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::storage::errors::StorageError;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, _body: Body) -> Response {
    let resource = ctx.resource();
    let upload_id = ctx.query.get("uploadId").cloned().unwrap_or_default();
    match store.abort_multipart(&ctx.bucket, &ctx.key, &upload_id).await {
        Ok(()) => srv::empty_response(StatusCode::NO_CONTENT),
        // Aborting an unknown upload is idempotent.
        Err(StorageError::NoSuchUpload(_)) => srv::empty_response(StatusCode::NO_CONTENT),
        Err(err) => srv::storage_error_response(err, &resource),
    }
}
