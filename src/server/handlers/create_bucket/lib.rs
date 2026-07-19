//! `PUT /{bucket}` — create a bucket.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::BucketCtx;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, _body: Body) -> Response {
    match store.create_bucket(&ctx.bucket).await {
        Ok(()) => srv::empty_response(StatusCode::OK),
        Err(err) => srv::storage_error_response(err, &ctx.resource()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::handlers::BucketCtx;

    fn ctx(bucket: &str) -> BucketCtx {
        BucketCtx {
            request_id: "test".into(),
            identity: None,
            auth_state: None,
            tasks: None,
            bucket: bucket.into(),
            query: std::collections::HashMap::new(),
            headers: axum::http::HeaderMap::new(),
            method: axum::http::Method::PUT,
        }
    }

    #[tokio::test]
    async fn create_then_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        let resp = handle(store.clone(), ctx("bucket"), Body::empty()).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(store.bucket_exists("bucket").await);
    }

    #[tokio::test]
    async fn invalid_bucket_name_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        let resp = handle(store, ctx("A"), Body::empty()).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
