//! The bucket directory: a token-only, read-only view of which buckets exist,
//! for infrastructure that has to know that and nothing more — a DNS server
//! answering `<bucket>.<public_hostname>`, a certificate issuer checking a
//! name before it signs for it.
//!
//! - `GET /_rs3/buckets` → `{"buckets":[{"name":…,"created_at_ms":…},…]}`
//! - `GET|HEAD /_rs3/buckets/{bucket}` → `200` with the entry, or `404`
//!
//! Both take `x-list-bucket-token: <token>`, matched against
//! `auth.bucket_list_tokens`. A token is not an identity: it carries no IAM
//! rights, cannot sign S3 requests, and reaches nothing but these two reads.
//! With no tokens configured the routes are not mounted at all.
//!
//! Served on the S3 port beside the S3 router, after the virtual-host rewrite
//! (so `bucket.host/_rs3/buckets` stays an object key). Path-style `/_rs3/…`
//! can never be a bucket: bucket names start with a letter or digit.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use serde_json::json;

use crate::storage::errors::StorageError;
use crate::storage::store::LocalObjectStore;

use super::logging::TARGET_AUTH;

pub const PATH: &str = "/_rs3/buckets";
pub const TOKEN_HEADER: &str = "x-list-bucket-token";

#[derive(Clone)]
struct DirectoryState {
    store: LocalObjectStore,
    tokens: Arc<Vec<String>>,
}

#[derive(Serialize)]
struct Entry {
    name: String,
    created_at_ms: i64,
}

/// The directory routes, or `None` when no token is configured.
pub fn router(store: LocalObjectStore, tokens: &[String]) -> Option<Router> {
    if tokens.is_empty() {
        return None;
    }
    let state = DirectoryState { store, tokens: Arc::new(tokens.to_vec()) };
    Some(
        Router::new()
            .route(PATH, get(list))
            .route(&format!("{PATH}/{{bucket}}"), get(lookup))
            .with_state(state),
    )
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// Checks the token header, logging a refusal (with where it came from) to
/// the auth log. Every configured token is compared, so the time taken does
/// not say which one came close.
fn authorized(state: &DirectoryState, request: &Request<axum::body::Body>) -> Result<(), Response> {
    let presented = request.headers().get(TOKEN_HEADER).and_then(|v| v.to_str().ok()).map(str::trim);
    let ok = presented.is_some_and(|presented| {
        state.tokens.iter().fold(false, |ok, token| constant_time_eq(token, presented) | ok)
    });
    if ok {
        return Ok(());
    }
    let from = super::auth::client_ip(request.extensions(), request.headers()).unwrap_or_else(|| "unknown".to_string());
    log::warn!(
        target: TARGET_AUTH,
        "bucket directory DENY path={} from={from} reason={}",
        request.uri().path(),
        if presented.is_some() { "invalid token" } else { "missing token" },
    );
    Err(error(StatusCode::UNAUTHORIZED, &format!("a valid {TOKEN_HEADER} header is required")))
}

fn error(status: StatusCode, message: &str) -> Response {
    let mut response = (status, Json(json!({ "error": message }))).into_response();
    response.headers_mut().insert("cache-control", HeaderValue::from_static("no-store"));
    response
}

fn ok(body: serde_json::Value) -> Response {
    let mut response = Json(body).into_response();
    response.headers_mut().insert("cache-control", HeaderValue::from_static("no-store"));
    response
}

async fn list(State(state): State<DirectoryState>, request: Request<axum::body::Body>) -> Response {
    if let Err(response) = authorized(&state, &request) {
        return response;
    }
    match state.store.list_buckets().await {
        Ok(buckets) => {
            let buckets: Vec<Entry> = buckets
                .into_iter()
                .map(|(name, meta)| Entry { name, created_at_ms: meta.created_at_ms })
                .collect();
            log::debug!(target: TARGET_AUTH, "bucket directory list ({} buckets)", buckets.len());
            ok(json!({ "buckets": buckets }))
        }
        Err(err) => {
            log::error!("bucket directory: listing buckets failed: {err}");
            error(StatusCode::INTERNAL_SERVER_ERROR, "listing buckets failed")
        }
    }
}

async fn lookup(
    State(state): State<DirectoryState>,
    Path(bucket): Path<String>,
    request: Request<axum::body::Body>,
) -> Response {
    if let Err(response) = authorized(&state, &request) {
        return response;
    }
    match state.store.bucket_meta(&bucket).await {
        Ok(meta) => ok(json!(Entry { name: bucket, created_at_ms: meta.created_at_ms })),
        // An invalid name is a bucket that does not exist, as far as a DNS
        // server or certificate issuer needs to know.
        Err(StorageError::BucketNotFound(_) | StorageError::InvalidBucketName(_)) => {
            error(StatusCode::NOT_FOUND, "no such bucket")
        }
        Err(err) => {
            log::error!("bucket directory: looking up {bucket:?} failed: {err}");
            error(StatusCode::INTERNAL_SERVER_ERROR, "bucket lookup failed")
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::server::config::{AppConfig, Credential};
    use crate::storage::store::LocalObjectStore;

    const TOKEN: &str = "directory-token-0123456789";

    async fn app(tokens: &[&str]) -> (tempfile::TempDir, axum::Router) {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("alpha").await.unwrap();
        store.create_bucket("beta").await.unwrap();
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        config.auth.credentials = vec![Credential { access_key: "AK".into(), secret_key: "SK".into() }];
        config.auth.bucket_list_tokens = tokens.iter().map(|t| t.to_string()).collect();
        (tmp, crate::server::router(store, std::sync::Arc::new(config)))
    }

    async fn call(app: &axum::Router, method: &str, uri: &str, token: Option<&str>) -> (StatusCode, String) {
        let mut request = Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            request = request.header(super::TOKEN_HEADER, token);
        }
        let response = app.clone().oneshot(request.body(Body::empty()).unwrap()).await.unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn a_token_lists_buckets_and_looks_one_up() {
        let (_tmp, app) = app(&["another-token-abcdefghij", TOKEN]).await;
        let (status, body) = call(&app, "GET", "/_rs3/buckets", Some(TOKEN)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        let names: Vec<&str> = json["buckets"].as_array().unwrap().iter().map(|b| b["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["alpha", "beta"]);
        assert!(json["buckets"][0]["created_at_ms"].as_i64().unwrap() > 0);

        let (status, body) = call(&app, "GET", "/_rs3/buckets/alpha", Some(TOKEN)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap()["name"], "alpha");
        assert_eq!(call(&app, "HEAD", "/_rs3/buckets/beta", Some(TOKEN)).await.0, StatusCode::OK);
        assert_eq!(call(&app, "GET", "/_rs3/buckets/gamma", Some(TOKEN)).await.0, StatusCode::NOT_FOUND);
        assert_eq!(call(&app, "HEAD", "/_rs3/buckets/gamma", Some(TOKEN)).await.0, StatusCode::NOT_FOUND);
        assert_eq!(call(&app, "GET", "/_rs3/buckets/..", Some(TOKEN)).await.0, StatusCode::NOT_FOUND);
        assert_eq!(call(&app, "GET", "/_rs3/buckets/Not_Valid", Some(TOKEN)).await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn without_the_right_token_nothing_is_revealed() {
        let (_tmp, app) = app(&[TOKEN]).await;
        for token in [None, Some(""), Some("wrong-token-0123456789xx"), Some(&TOKEN[..TOKEN.len() - 1])] {
            assert_eq!(call(&app, "GET", "/_rs3/buckets", token).await.0, StatusCode::UNAUTHORIZED, "{token:?}");
            assert_eq!(call(&app, "GET", "/_rs3/buckets/alpha", token).await.0, StatusCode::UNAUTHORIZED, "{token:?}");
        }
        // A wrong method is not a read.
        let (status, _) = call(&app, "PUT", "/_rs3/buckets/alpha", Some(TOKEN)).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn the_token_grants_nothing_on_the_s3_api() {
        let (_tmp, app) = app(&[TOKEN]).await;
        for (method, uri) in [("GET", "/"), ("GET", "/alpha"), ("GET", "/alpha/key"), ("PUT", "/gamma"), ("DELETE", "/alpha")] {
            let (status, body) = call(&app, method, uri, Some(TOKEN)).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}: {body}");
        }
    }

    #[tokio::test]
    async fn with_no_token_configured_the_directory_is_not_served() {
        let (_tmp, app) = app(&[]).await;
        let (status, body) = call(&app, "GET", "/_rs3/buckets", Some(TOKEN)).await;
        assert_ne!(status, StatusCode::OK, "{body}");
        assert!(!body.contains("alpha"), "{body}");
    }
}
