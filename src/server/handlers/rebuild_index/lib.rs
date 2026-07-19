//! `POST /{bucket}?rebuildIndex` — rebuild the bucket's SQLite index from the
//! on-disk blob tree (admin-only; IAM users are refused at the auth layer).

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::BucketCtx;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, _body: Body) -> Response {
    // Launch the rebuild as a registry-tracked job when a registry is present
    // (the normal server path); fall back to the storage-layer background
    // trigger otherwise.
    let started = match ctx.tasks.clone() {
        Some(tasks) => crate::server::jobs::rebuild_index::spawn(store, ctx.bucket.clone(), tasks),
        None => store.start_rebuild_background(&ctx.bucket),
    };
    if started {
        srv::empty_response(StatusCode::ACCEPTED)
    } else {
        srv::s3_error(
            StatusCode::CONFLICT,
            "RebuildInProgress",
            "A rebuild is already running for this bucket",
            &ctx.resource(),
        )
    }
}
