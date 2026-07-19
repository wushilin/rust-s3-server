//! `DELETE /{bucket}/{key}` — delete an object (idempotent).

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, _body: Body) -> Response {
    let resource = ctx.resource();
    match store.delete_object_with_size(&ctx.bucket, &ctx.key).await {
        Ok(size) => srv::with_measure(
            srv::empty_response(StatusCode::NO_CONTENT),
            srv::OperationMeasure::Bytes(size.unwrap_or(0)),
        ),
        Err(err) => srv::storage_error_response(err, &resource),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(bucket: &str, key: &str) -> ObjectCtx {
        ObjectCtx {
            request_id: "test".into(),
            identity: None,
            bucket: bucket.into(),
            key: key.into(),
            query: std::collections::HashMap::new(),
            headers: axum::http::HeaderMap::new(),
            method: axum::http::Method::DELETE,
        }
    }

    #[tokio::test]
    async fn delete_is_idempotent_no_content() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store.put_object("bucket", "k", b"x", None, None, false).await.unwrap();

        let resp = handle(store.clone(), ctx("bucket", "k"), Body::empty()).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(store.read_object("bucket", "k").await.is_err());

        // Deleting an absent key still succeeds.
        let resp = handle(store, ctx("bucket", "k"), Body::empty()).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }
}
