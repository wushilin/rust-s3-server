//! Regression tests for authentication/authorization parser differentials and
//! signature-coverage gaps, driven through the real router with auth enabled.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

use super::auth::{
    compute_auth_header_payload, compute_auth_header_with_headers, presign_query, AuthState,
};
use super::config::{AppConfig, Credential};
use super::iam::IamStore;
use super::policy::requirements_for_request;
use super::{registry, router, router_with_metrics, TrafficMetrics};
use crate::storage::store::LocalObjectStore;

const AK: &str = "TESTKEY";
const SK: &str = "TESTSECRET";
const REGION: &str = "us-east-1";
const HOST: &str = "localhost";

fn config() -> AppConfig {
    let mut config = AppConfig::default();
    config.auth.enabled = true;
    config.auth.credentials.push(Credential {
        access_key: AK.to_string(),
        secret_key: SK.to_string(),
    });
    config.auth.public_hostname = Some(HOST.to_string());
    config
}

fn app(tmp: &tempfile::TempDir) -> (axum::Router, LocalObjectStore) {
    let store = LocalObjectStore::new(tmp.path());
    (router(store.clone(), Arc::new(config())), store)
}

fn now() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}

/// A root-signed request; `signed` headers are signed, `unsigned` ones are
/// merely sent.
async fn send(
    app: &axum::Router,
    method: &str,
    path: &str,
    query: &str,
    signed: &[(&str, &str)],
    unsigned: &[(&str, &str)],
    body: Body,
) -> (StatusCode, String) {
    let datetime = now();
    let auth = compute_auth_header_with_headers(
        method, path, query, HOST, AK, SK, REGION, &datetime, signed,
    );
    let uri = if query.is_empty() { path.to_string() } else { format!("{path}?{query}") };
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("host", HOST)
        .header("x-amz-date", &datetime)
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
        .header("authorization", auth);
    for (name, value) in signed.iter().chain(unsigned) {
        request = request.header(*name, *value);
    }
    let response = app.clone().oneshot(request.body(body).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

async fn raw(app: &axum::Router, method: &str, uri: &str, body: Body) -> StatusCode {
    app.clone()
        .oneshot(Request::builder().method(method).uri(uri).header("host", HOST).body(body).unwrap())
        .await
        .unwrap()
        .status()
}

// ── 1. health/metrics bypass is exact and read-only ──────────────────────────

#[tokio::test]
async fn unrouted_minio_metrics_paths_are_not_an_auth_bypass() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, store) = app(&tmp);
    store.create_bucket("minio").await.unwrap();
    store.put_object("minio", "v2/metrics/x", b"secret", None, None, false).await.unwrap();

    assert_eq!(raw(&app, "GET", "/minio/v2/metrics/x", Body::empty()).await, StatusCode::FORBIDDEN);
    assert_eq!(
        raw(&app, "PUT", "/minio/v2/metrics/evil", Body::from("pwned")).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(raw(&app, "DELETE", "/minio/v2/metrics/x", Body::empty()).await, StatusCode::FORBIDDEN);
    assert!(store.read_object("minio", "v2/metrics/evil").await.is_err());
    // The real probes still answer without credentials.
    assert_eq!(raw(&app, "GET", "/minio/health/live", Body::empty()).await, StatusCode::OK);
    assert_eq!(raw(&app, "GET", "/minio/v2/metrics/cluster", Body::empty()).await, StatusCode::OK);
}

// ── 2. policy and router read the query/copy-source identically ───────────────

#[tokio::test]
async fn a_repeated_query_parameter_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, store) = app(&tmp);
    store.create_bucket("bkt").await.unwrap();
    let (status, body) =
        send(&app, "GET", "/bkt", "prefix=a&prefix=b", &[], &[], Body::empty()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("InvalidArgument"));
    // Also when the repeat is only visible after percent-decoding.
    let (status, _) =
        send(&app, "GET", "/bkt/k", "uploadId=x&%75ploadId=y", &[], &[], Body::empty()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Unauthenticated too — the check runs before auth and routing.
    assert_eq!(raw(&app, "GET", "/bkt?a&a", Body::empty()).await, StatusCode::BAD_REQUEST);
}

#[test]
fn policy_requirements_decode_the_query_and_bucket_like_the_router() {
    let action = |m: &str, p: &str, q: &str| {
        requirements_for_request(m, p, q, None).unwrap()[0].action
    };
    assert_eq!(action("POST", "/bkt", "rebuild%49ndex"), "s3:RebuildIndex");
    assert_eq!(action("GET", "/bkt", "c%6Frs"), "s3:GetBucketCORS");
    assert_eq!(action("GET", "/bkt/k", "%75ploadId=1"), "s3:ListMultipartUploadParts");
    assert_eq!(action("GET", "/bkt/k", "%61ttributes"), "s3:GetObjectAttributes");
    assert_eq!(action("DELETE", "/bkt/k", "%75ploadId=1"), "s3:AbortMultipartUpload");
    // HEAD is always routed to the object read.
    assert_eq!(action("HEAD", "/bkt/k", "attributes"), "s3:GetObject");
    // The bucket segment is percent-decoded like axum's `Path`.
    let reqs = requirements_for_request("GET", "/%73ecret/k", "", None).unwrap();
    assert_eq!(reqs[0].resource, "arn:aws:s3:::secret/k");
}

#[test]
fn copy_source_is_authorized_as_the_handler_parses_it() {
    let reqs =
        requirements_for_request("PUT", "/dst/k", "", Some("/src/a.txt?versionId=abc")).unwrap();
    assert_eq!(reqs[1].action, "s3:GetObject");
    assert_eq!(reqs[1].resource, "arn:aws:s3:::src/a.txt");
    let reqs = requirements_for_request("PUT", "/dst/k", "", Some("src%2Fa%20b")).unwrap();
    assert_eq!(reqs[1].resource, "arn:aws:s3:::src/a b");
    // An unparseable source is denied, not skipped.
    assert!(requirements_for_request("PUT", "/dst/k", "", Some("/nokey")).is_none());
}

// ── 3. every x-amz-* header present must be signed ────────────────────────────

#[tokio::test]
async fn unsigned_amz_headers_are_rejected_for_header_auth() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, store) = app(&tmp);
    store.create_bucket("bkt").await.unwrap();
    store.put_object("bkt", "src", b"data", None, None, false).await.unwrap();

    let (status, _) = send(
        &app, "PUT", "/bkt/dst", "", &[], &[("x-amz-copy-source", "/bkt/src")], Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) =
        send(&app, "PUT", "/bkt/m", "", &[], &[("x-amz-meta-evil", "1")], Body::from("x")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) =
        send(&app, "PUT", "/bkt/m", "", &[], &[("content-md5", "1B2M2Y8AsgTpgAmY7PhCfg==")], Body::from("")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Signed, the same headers are fine.
    let (status, body) = send(
        &app, "PUT", "/bkt/dst", "", &[("x-amz-copy-source", "/bkt/src")], &[], Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn unsigned_amz_headers_are_rejected_for_presigned_urls() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, store) = app(&tmp);
    store.create_bucket("bkt").await.unwrap();
    let query = presign_query("PUT", "/bkt/k", HOST, AK, SK, REGION, &now(), 300, &[]);
    let put = |extra: Option<(&'static str, &'static str)>| {
        let mut request = Request::builder()
            .method("PUT")
            .uri(format!("/bkt/k?{query}"))
            .header("host", HOST);
        if let Some((name, value)) = extra {
            request = request.header(name, value);
        }
        app.clone().oneshot(request.body(Body::from("hello")).unwrap())
    };
    assert_eq!(put(Some(("x-amz-meta-evil", "1"))).await.unwrap().status(), StatusCode::FORBIDDEN);
    // x-amz-content-sha256 is tolerated unsigned on presigned requests.
    assert_eq!(
        put(Some(("x-amz-content-sha256", "UNSIGNED-PAYLOAD"))).await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(put(None).await.unwrap().status(), StatusCode::OK);
}

// ── 5. a form POST cannot smuggle an upload through `?uploads` ────────────────

#[tokio::test]
async fn a_header_signed_form_post_with_an_operation_query_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, store) = app(&tmp);
    store.create_bucket("bkt").await.unwrap();
    let content_type = "multipart/form-data; boundary=X";
    let body = concat!(
        "--X\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nsneaky.txt\r\n",
        "--X\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a\"\r\n\r\npwned\r\n",
        "--X--\r\n",
    );
    for query in ["uploads", "uploadId=1"] {
        let (status, _) = send(
            &app, "POST", "/bkt", query, &[], &[("content-type", content_type)], Body::from(body),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "?{query}");
    }
    assert!(store.read_object("bkt", "sneaky.txt").await.is_err());
}

// ── 4/7. browser POST: policy enforced before the file is stored ─────────────

fn post_form(conditions: serde_json::Value, fields: &[(&str, &str)], file: &str) -> Vec<u8> {
    let date = chrono::Utc::now().format("%Y%m%d").to_string();
    let expiration = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    let policy = serde_json::json!({"expiration": expiration, "conditions": conditions});
    let policy_b64 = BASE64_STANDARD.encode(policy.to_string());
    let key = |k: &[u8], d: &[u8]| {
        let mut mac = Hmac::<Sha256>::new_from_slice(k).unwrap();
        mac.update(d);
        mac.finalize().into_bytes().to_vec()
    };
    let signing = key(
        &key(&key(&key(format!("AWS4{SK}").as_bytes(), date.as_bytes()), REGION.as_bytes()), b"s3"),
        b"aws4_request",
    );
    let signature: String =
        key(&signing, policy_b64.as_bytes()).iter().map(|b| format!("{b:02x}")).collect();
    let credential = format!("{AK}/{date}/{REGION}/s3/aws4_request");
    let mut all: Vec<(&str, String)> = vec![
        ("policy", policy_b64),
        ("x-amz-algorithm", "AWS4-HMAC-SHA256".to_string()),
        ("x-amz-credential", credential),
        ("x-amz-signature", signature),
    ];
    all.extend(fields.iter().map(|(k, v)| (*k, v.to_string())));
    let mut body = String::new();
    for (name, value) in all {
        body.push_str(&format!("--X\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"));
    }
    body.push_str(&format!(
        "--X\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f\"\r\n\r\n{file}\r\n--X--\r\n"
    ));
    body.into_bytes()
}

async fn post(app: &axum::Router, body: Vec<u8>) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/bkt")
                .header("content-type", "multipart/form-data; boundary=X")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn browser_post_policy_conditions_are_enforced() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, store) = app(&tmp);
    store.create_bucket("bkt").await.unwrap();
    let base = |extra: serde_json::Value| {
        let mut c = vec![serde_json::json!({"bucket": "bkt"}), serde_json::json!(["starts-with", "$key", "up/"])];
        if let serde_json::Value::Array(items) = extra {
            c.extend(items);
        }
        serde_json::Value::Array(c)
    };
    // Accepted: everything covered and within range.
    let ok = post_form(
        base(serde_json::json!([["content-length-range", 1, 10], {"Content-Type": "text/plain"}])),
        &[("key", "up/a"), ("Content-Type", "text/plain")],
        "hello",
    );
    assert_eq!(post(&app, ok).await, StatusCode::CREATED);
    // File larger than content-length-range.
    let big = post_form(
        base(serde_json::json!([["content-length-range", 1, 3]])),
        &[("key", "up/big")],
        "hello",
    );
    assert_eq!(post(&app, big).await, StatusCode::BAD_REQUEST);
    assert!(store.read_object("bkt", "up/big").await.is_err());
    // A form field the policy never mentions.
    let uncovered = post_form(base(serde_json::json!([])), &[("key", "up/u"), ("x-amz-meta-evil", "1")], "x");
    assert_eq!(post(&app, uncovered).await, StatusCode::FORBIDDEN);
    // A condition on a non-key field that does not hold.
    let wrong_type = post_form(
        base(serde_json::json!([["eq", "$Content-Type", "image/png"]])),
        &[("key", "up/t"), ("Content-Type", "text/html")],
        "x",
    );
    assert_eq!(post(&app, wrong_type).await, StatusCode::FORBIDDEN);
    // An unknown operator no longer passes silently.
    let unknown_op = post_form(
        base(serde_json::json!([["matches-regex", "$key", ".*"]])),
        &[("key", "up/o")],
        "x",
    );
    assert_eq!(post(&app, unknown_op).await, StatusCode::FORBIDDEN);
    // x-ignore-* fields need no condition.
    let ignored = post_form(base(serde_json::json!([])), &[("key", "up/i"), ("x-ignore-me", "1")], "x");
    assert_eq!(post(&app, ignored).await, StatusCode::CREATED);
}

// ── 8. the verified key, not the claimed one, is attributed ───────────────────

#[tokio::test]
async fn key_usage_is_attributed_to_the_key_that_verified() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path());
    store.create_bucket("bkt").await.unwrap();
    store.put_object("bkt", "k", b"data", None, None, false).await.unwrap();
    let usage = super::key_usage::KeyUsageStore::open(tmp.path()).await.unwrap();
    let app = router_with_metrics(
        store,
        AuthState {
            config: Arc::new(config()),
            iam: Some(IamStore::open(tmp.path()).await.unwrap()),
            usage: Some(usage.clone()),
        },
        Arc::new(TrafficMetrics::default()),
        registry::TaskRegistry::new(),
    );
    // Valid presigned query credentials plus a bogus header naming another key.
    let query = presign_query("GET", "/bkt/k", HOST, AK, SK, REGION, &now(), 300, &[]);
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/bkt/k?{query}"))
                .header("host", HOST)
                .header(
                    "authorization",
                    "AWS4-HMAC-SHA256 Credential=OTHERKEY/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let actor = response.extensions().get::<super::OperationActor>().unwrap();
    assert_eq!(actor.access_key.as_deref(), Some(AK));
    assert!(usage.get(AK).is_some());
    assert!(usage.get("OTHERKEY").is_none());
}

// ── 9. streaming chunk signatures are verified ────────────────────────────────

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn hmac_hex(key: &[u8], data: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

fn signing_key(date: &str) -> Vec<u8> {
    let step = |k: &[u8], d: &[u8]| {
        let mut mac = Hmac::<Sha256>::new_from_slice(k).unwrap();
        mac.update(d);
        mac.finalize().into_bytes().to_vec()
    };
    step(&step(&step(&step(format!("AWS4{SK}").as_bytes(), date.as_bytes()), REGION.as_bytes()), b"s3"), b"aws4_request")
}

fn signed_chunks(datetime: &str, seed: &str, chunks: &[&[u8]]) -> Vec<u8> {
    let scope = format!("{}/{REGION}/s3/aws4_request", &datetime[..8]);
    let key = signing_key(&datetime[..8]);
    let mut prev = seed.to_string();
    let mut body = Vec::new();
    for chunk in chunks.iter().copied().chain(std::iter::once(&b""[..])) {
        let hash: String = Sha256::digest(chunk).iter().map(|b| format!("{b:02x}")).collect();
        let sts = format!("AWS4-HMAC-SHA256-PAYLOAD\n{datetime}\n{scope}\n{prev}\n{EMPTY_SHA256}\n{hash}");
        let sig = hmac_hex(&key, sts.as_bytes());
        body.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", chunk.len()).as_bytes());
        body.extend_from_slice(chunk);
        body.extend_from_slice(b"\r\n");
        prev = sig;
    }
    body
}

async fn streaming_put(app: &axum::Router, path: &str, tamper: impl Fn(&mut Vec<u8>)) -> StatusCode {
    let datetime = now();
    let decoded_len = "11";
    let (auth, seed) = compute_auth_header_payload(
        "PUT",
        path,
        "",
        HOST,
        AK,
        SK,
        REGION,
        &datetime,
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        &[("x-amz-decoded-content-length", decoded_len)],
    );
    let mut body = signed_chunks(&datetime, &seed, &[b"hello", b" world"]);
    tamper(&mut body);
    app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path)
                .header("host", HOST)
                .header("x-amz-date", &datetime)
                .header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .header("x-amz-decoded-content-length", decoded_len)
                .header("content-encoding", "aws-chunked")
                .header("authorization", auth)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn streaming_chunk_signatures_are_verified() {
    let tmp = tempfile::tempdir().unwrap();
    let (app, store) = app(&tmp);
    store.create_bucket("bkt").await.unwrap();

    assert_eq!(streaming_put(&app, "/bkt/ok", |_| {}).await, StatusCode::OK);
    let read = store.read_object("bkt", "ok").await.unwrap();
    assert_eq!(read.meta.size, 11);

    // Same-length data swap: framing and declared length still line up, only
    // the chunk signature can catch it.
    let status = streaming_put(&app, "/bkt/bad", |body| {
        let pos = body.windows(5).position(|w| w == b"hello").unwrap();
        body[pos..pos + 5].copy_from_slice(b"HELLO");
    })
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(store.read_object("bkt", "bad").await.is_err());
}
