//! `PUT /{bucket}/{key}` — upload an object (streaming, optional aws-chunked).

use axum::body::Body;
use axum::http::{header, StatusCode};
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, body: Body) -> Response {
    let resource = ctx.resource();
    let aws_chunked = srv::is_aws_chunked(&ctx.headers);
    let content_type = ctx
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let content_encoding = srv::object_content_encoding(&ctx.headers);
    let content_language = ctx
        .headers
        .get(header::CONTENT_LANGUAGE)
        .and_then(|v| v.to_str().ok());
    let storage_class = srv::storage_class_header(&ctx.headers);
    if let Some(resp) = srv::reject_invalid_storage_class(storage_class, &resource) {
        return resp;
    }
    let user_meta = match srv::extract_user_meta(&ctx.headers) {
        Ok(meta) => meta,
        Err(message) => {
            return srv::s3_error(StatusCode::BAD_REQUEST, "InvalidArgument", message, &resource)
        }
    };
    let expected_sha256 = srv::expected_payload_sha256(&ctx.headers, aws_chunked);
    match store
        .put_object_stream_with_metadata(
            &ctx.bucket,
            &ctx.key,
            body.into_data_stream(),
            content_type,
            content_encoding.as_deref(),
            storage_class,
            content_language,
            &user_meta,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::identity::Identity;

    fn ctx(bucket: &str, key: &str) -> ObjectCtx {
        ObjectCtx {
            request_id: "test".into(),
            identity: Some(Identity::root(None, None)),
            bucket: bucket.into(),
            key: key.into(),
            query: std::collections::HashMap::new(),
            headers: axum::http::HeaderMap::new(),
            method: axum::http::Method::PUT,
        }
    }

    #[tokio::test]
    async fn put_writes_object_and_returns_etag() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();

        let resp = handle(store.clone(), ctx("bucket", "k"), Body::from("hello")).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get(header::ETAG).is_some());

        let read = store.read_object("bucket", "k").await.unwrap();
        assert_eq!(read.meta.size, 5);
    }

    #[tokio::test]
    async fn put_to_missing_bucket_is_404() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        let resp = handle(store, ctx("nope", "k"), Body::from("x")).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
