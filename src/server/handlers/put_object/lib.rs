//! `PUT /{bucket}/{key}` — upload an object (streaming, optional aws-chunked).

use axum::body::Body;
use axum::http::{header, StatusCode};
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::storage::store::{LocalObjectStore, Precondition};

/// Parses the conditional-write preconditions S3 (and the `object_store` crate)
/// place on `PutObject`. `If-None-Match: *` means create-only; `If-Match:
/// "<etag>"` means overwrite-only-if-unchanged. If-None-Match takes precedence
/// when both are present.
fn parse_precondition(headers: &axum::http::HeaderMap) -> Option<Precondition> {
    if let Some(v) = headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) {
        if v.trim() == "*" {
            return Some(Precondition::IfNoneMatchStar);
        }
    }
    if let Some(v) = headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok()) {
        let v = v.trim();
        // A specific ETag ("<etag>"); `*` (any existing object) is not what
        // object_store emits, so it is left unconditional here.
        if !v.is_empty() && v != "*" {
            return Some(Precondition::IfMatch(v.to_string()));
        }
    }
    None
}

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
    // For aws-chunked uploads the client declares the true payload size here;
    // the storage layer rejects a body that decodes to a different length.
    let expected_decoded_len = if aws_chunked {
        ctx.headers
            .get("x-amz-decoded-content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
    } else {
        None
    };
    let precondition = parse_precondition(&ctx.headers);
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
            expected_decoded_len,
            precondition,
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

    fn ctx_with(bucket: &str, key: &str, name: header::HeaderName, value: &str) -> ObjectCtx {
        let mut c = ctx(bucket, key);
        c.headers.insert(name, value.parse().unwrap());
        c
    }

    #[tokio::test]
    async fn if_none_match_star_creates_only_when_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("condbkt").await.unwrap();

        // First conditional create succeeds against an empty key.
        let resp = handle(
            store.clone(),
            ctx_with("condbkt", "k", header::IF_NONE_MATCH, "*"),
            Body::from("one"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // A second create with If-None-Match: * is rejected — the object exists.
        let resp = handle(
            store.clone(),
            ctx_with("condbkt", "k", header::IF_NONE_MATCH, "*"),
            Body::from("second write"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);

        // …and the original content is untouched.
        let read = store.read_object("condbkt", "k").await.unwrap();
        assert_eq!(read.meta.size, 3);
    }

    #[tokio::test]
    async fn if_match_requires_matching_etag() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("condbkt").await.unwrap();

        let resp = handle(store.clone(), ctx("condbkt", "k"), Body::from("orig")).await;
        let etag = resp
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Correct ETag → the update goes through.
        let resp = handle(
            store.clone(),
            ctx_with("condbkt", "k", header::IF_MATCH, &etag),
            Body::from("updated!"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(store.read_object("condbkt", "k").await.unwrap().meta.size, 8);

        // Stale ETag → rejected.
        let resp = handle(
            store.clone(),
            ctx_with("condbkt", "k", header::IF_MATCH, "\"deadbeefdeadbeefdeadbeefdeadbeef\""),
            Body::from("no"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(store.read_object("condbkt", "k").await.unwrap().meta.size, 8);
    }

    #[tokio::test]
    async fn if_match_on_absent_object_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("condbkt").await.unwrap();
        let resp = handle(
            store,
            ctx_with("condbkt", "ghost", header::IF_MATCH, "\"anything\""),
            Body::from("x"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::PRECONDITION_FAILED);
    }
}
