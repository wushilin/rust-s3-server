//! SigV4 authentication middleware.
//!
//! Validates both regular `Authorization: AWS4-HMAC-SHA256 …` requests and
//! pre-signed URLs (`?X-Amz-Signature=…`).  When `auth.enabled` is false in
//! the config the middleware is a no-op.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::{DateTime, NaiveDateTime, Utc};
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::{Digest, Sha256};

use super::config::AppConfig;
use super::iam::{IamStore, Principal};
use super::identity::Identity;
use super::logging::{TARGET_AUTH, TARGET_AUTHZ};
use super::policy::{is_authorized, requirements_for_request, PolicyDocument};
use super::xml::{error_xml, S3ErrorXml};
use super::OperationActor;

type HmacSha256 = Hmac<Sha256>;
type HmacSha1 = Hmac<Sha1>;

const MAX_SIGNATURE_CLOCK_SKEW_SECS: i64 = 15 * 60;

/// Shared state for the auth middleware: static config credentials (root,
/// unrestricted) plus the IAM store (policy-bound access keys).
#[derive(Clone)]
pub struct AuthState {
    pub config: Arc<AppConfig>,
    pub iam: Option<IamStore>,
}

impl AuthState {
    /// Resolves an access key to `(secret, principal)`. Config credentials
    /// are root; IAM keys carry their owning user; hidden `RSWEB_…` signing
    /// keys resolve to their owner's access (so console-generated share links
    /// are authorized exactly as the user who created them).
    fn lookup(&self, access_key: &str) -> Option<(String, Principal)> {
        if let Some(secret) = self.config.find_secret(access_key) {
            return Some((secret.to_string(), Principal::Root));
        }
        let iam = self.iam.as_ref()?;
        if let Some((secret, username)) = iam.find_key(access_key) {
            return Some((secret, Principal::IamUser(username)));
        }
        if let Some((secret, username, is_builtin)) = iam.find_web_key(access_key) {
            if is_builtin {
                // A built-in admin's web key is only honored while that admin
                // still exists in config (removing them revokes their shares).
                return self
                    .config
                    .find_builtin_user(&username)
                    .map(|_| (secret, Principal::Root));
            }
            return Some((secret, Principal::IamUser(username)));
        }
        None
    }
}

// ─── Public middleware ────────────────────────────────────────────────────────

/// Tower middleware: validates SigV4 auth when `auth.enabled = true`, then
/// enforces the caller's IAM policy (root credentials are unrestricted).
pub async fn auth_middleware(
    State(state): State<AuthState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let rid = request_id(&request);
    if !state.config.auth.enabled {
        log::debug!(target: TARGET_AUTH, "[{rid}] authn skipped (auth disabled)");
        return next.run(request).await;
    }

    // Browser-based POST uploads (multipart/form-data to a bucket) carry their
    // SigV4 authorization inside the form body (`policy` + `x-amz-signature`),
    // which this header-only middleware cannot inspect. Hand such requests to
    // the bucket handler with the auth state attached so it can verify the
    // form signature itself. This is deliberately scoped to *bucket-level*
    // POSTs: object-level POSTs (e.g. multipart-upload completion) still go
    // through normal header validation below.
    if is_browser_post_upload(&request) {
        log::debug!(target: TARGET_AUTH, "[{rid}] authn deferred to browser-POST form verification");
        request.extensions_mut().insert(state.clone());
        return next.run(request).await;
    }

    // Phase 1 — authentication: prove the caller holds a valid credential.
    let authn_start = std::time::Instant::now();
    let principal = match validate_request(&state, &request) {
        Ok(principal) => principal,
        Err(msg) => {
            // Explicit "not proceeding" record for every rejected request.
            log::warn!(
                target: TARGET_AUTH,
                "[{rid}] authn DENY method={} path={} reason={msg} ({}µs)",
                request.method(),
                request.uri().path(),
                authn_start.elapsed().as_micros(),
            );
            let access_key = claimed_access_key(&request);
            let resolved = access_key
                .as_deref()
                .and_then(|key| state.lookup(key).map(|(_, principal)| principal));
            let actor = operation_actor(&state, resolved.as_ref(), access_key);
            return with_operation_actor(deny(msg), actor);
        }
    };
    log::debug!(
        target: TARGET_AUTH,
        "[{rid}] authn ok principal={principal:?} ({}µs)",
        authn_start.elapsed().as_micros()
    );
    let actor = operation_actor(&state, Some(&principal), claimed_access_key(&request));

    // Phase 2 — authorization: enforce the IAM policy bound to the caller
    // (root config credentials are unrestricted and skip this), then attach the
    // resolved identity so body-aware handlers (e.g. multi-object delete) can
    // authorize per item through the same `Identity::authorize` path.
    let identity = match &principal {
        Principal::Root => Identity::root(actor.username.clone(), actor.access_key.clone()),
        Principal::IamUser(username) => {
            let authz_start = std::time::Instant::now();
            let Some(policy) = state.iam.as_ref().and_then(|iam| iam.policy_for(username)) else {
                log::warn!(target: TARGET_AUTHZ, "[{rid}] authz DENY user={username} reason=no_policy_attached");
                return with_operation_actor(access_denied(), actor);
            };
            if !authorize_iam(&policy, &request) {
                log::warn!(
                    target: TARGET_AUTHZ,
                    "[{rid}] authz DENY user={username} method={} uri={} ({}µs)",
                    request.method(),
                    request.uri(),
                    authz_start.elapsed().as_micros(),
                );
                return with_operation_actor(access_denied(), actor);
            }
            log::debug!(
                target: TARGET_AUTHZ,
                "[{rid}] authz ok user={username} ({}µs)",
                authz_start.elapsed().as_micros()
            );
            Identity::iam(username.clone(), Some(policy))
        }
    };
    request.extensions_mut().insert(identity);
    with_operation_actor(next.run(request).await, actor)
}

/// The correlation id injected by the outer logging layer, or `"-"` if this
/// request somehow bypassed it (e.g. a unit test calling the middleware
/// directly).
fn request_id(request: &Request<Body>) -> String {
    request
        .extensions()
        .get::<super::RequestId>()
        .map(|id| id.0.clone())
        .unwrap_or_else(|| "-".to_string())
}

fn with_operation_actor(mut response: Response, actor: OperationActor) -> Response {
    response.extensions_mut().insert(actor);
    response
}

fn operation_actor(
    state: &AuthState,
    principal: Option<&Principal>,
    access_key: Option<String>,
) -> OperationActor {
    let username = match principal {
        Some(Principal::IamUser(username)) => Some(username.clone()),
        Some(Principal::Root) => access_key.as_deref().and_then(|access_key| {
            state
                .config
                .auth
                .users
                .iter()
                .find(|user| user.api_keys.iter().any(|key| key.ak == access_key))
                .map(|user| user.user.clone())
        }),
        None => None,
    };
    OperationActor {
        username,
        access_key,
    }
}

fn claimed_access_key(request: &Request<Body>) -> Option<String> {
    if let Some(auth) = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        if let Some(parsed) = parse_auth_header(auth) {
            return Some(parsed.access_key);
        }
        if let Some(v2) = auth.strip_prefix("AWS ") {
            return v2.split_once(':').map(|(access_key, _)| access_key.to_string());
        }
    }
    request.uri().query().and_then(|query| {
        query.split('&').find_map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            let key = urlencoding::decode(key).ok()?;
            let value = urlencoding::decode(value).ok()?;
            match key.as_ref() {
                "X-Amz-Credential" => value.split('/').next().map(str::to_string),
                "AWSAccessKeyId" => Some(value.into_owned()),
                _ => None,
            }
        })
    })
}

/// Evaluates the IAM user's policy against the request. No policy attached
/// means deny-everything; admin-only operations are never IAM-authorized.
fn authorize_iam(policy: &PolicyDocument, request: &Request<Body>) -> bool {
    let copy_source = request
        .headers()
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok());
    let Some(requirements) = requirements_for_request(
        request.method().as_str(),
        request.uri().path(),
        request.uri().query().unwrap_or(""),
        copy_source,
    ) else {
        return false; // admin-only operation
    };
    is_authorized(policy, &requirements)
}

/// True for a bucket-level `multipart/form-data` POST — a browser-style form
/// upload whose credentials live in the body, not the headers.
fn is_browser_post_upload(request: &Request<Body>) -> bool {
    if request.method() != axum::http::Method::POST {
        return false;
    }
    let is_multipart = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("multipart/form-data"))
        .unwrap_or(false);
    if !is_multipart {
        return false;
    }
    // Bucket-level path only: `/bucket` or `/bucket/`, never `/bucket/key`.
    let trimmed = request.uri().path().trim_matches('/');
    !trimmed.is_empty() && !trimmed.contains('/')
}

/// Verifies a browser POST upload's SigV4 form signature and authorizes it.
///
/// Returns the resolved [`OperationActor`] on success, or a ready-to-send
/// error response on failure. The form fields are the parsed `multipart/
/// form-data` values; the signature covers the base64 `policy` field, per the
/// S3 POST-upload signing scheme.
pub(crate) fn authorize_browser_post(
    state: &AuthState,
    fields: &std::collections::BTreeMap<String, String>,
    bucket: &str,
    key: &str,
) -> Result<OperationActor, Response> {
    if !state.config.auth.enabled {
        return Ok(OperationActor::default());
    }
    let field = |name: &str| {
        fields
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    };

    let algorithm = field("x-amz-algorithm").unwrap_or("");
    if algorithm != "AWS4-HMAC-SHA256" {
        return Err(deny("Unsupported or missing POST x-amz-algorithm"));
    }
    let policy_b64 = field("policy").ok_or_else(|| deny("Missing POST policy"))?;
    let signature = field("x-amz-signature").ok_or_else(|| deny("Missing x-amz-signature"))?;
    let credential = field("x-amz-credential").ok_or_else(|| deny("Missing x-amz-credential"))?;

    let mut parts = credential.splitn(5, '/');
    let access_key = parts.next().unwrap_or("");
    let date = parts.next().ok_or_else(|| deny("Invalid x-amz-credential"))?;
    let region = parts.next().ok_or_else(|| deny("Invalid x-amz-credential"))?;
    let service = parts.next().ok_or_else(|| deny("Invalid x-amz-credential"))?;
    let _terminator = parts.next().ok_or_else(|| deny("Invalid x-amz-credential"))?;

    let (secret, principal) = state
        .lookup(access_key)
        .ok_or_else(|| deny("Unknown access key"))?;

    // The POST string-to-sign is the base64 policy document verbatim.
    let signing_key = derive_signing_key(&secret, date, region, service);
    let expected = hex_hmac(&signing_key, policy_b64.as_bytes());
    if !constant_time_eq(&expected, signature) {
        return Err(deny("POST signature does not match"));
    }

    // Enforce the policy document: it must be unexpired and its conditions
    // must actually cover this bucket/key, so a captured signature cannot be
    // replayed against a different target.
    verify_post_policy_document(policy_b64, bucket, key)?;

    // An IAM principal is still bound by its user policy.
    if let Principal::IamUser(username) = &principal {
        let Some(policy) = state.iam.as_ref().and_then(|iam| iam.policy_for(username)) else {
            return Err(access_denied());
        };
        if !is_authorized(
            &policy,
            &[super::policy::Requirement::object("s3:PutObject", bucket, key)],
        ) {
            log::warn!(target: TARGET_AUTHZ, "s3 browser POST denied by policy user={username} bucket={bucket} key={key}");
            return Err(access_denied());
        }
    }

    let username = match &principal {
        Principal::IamUser(username) => Some(username.clone()),
        Principal::Root => state
            .config
            .auth
            .users
            .iter()
            .find(|user| user.api_keys.iter().any(|k| k.ak == access_key))
            .map(|user| user.user.clone()),
    };
    Ok(OperationActor {
        username,
        access_key: Some(access_key.to_string()),
    })
}

/// Decodes and enforces the base64 POST policy: rejects an absent/expired
/// `expiration`, and requires the `conditions` to match the target bucket and
/// key (exact `eq`, `starts-with`, or the `{"bucket": …}` form).
fn verify_post_policy_document(policy_b64: &str, bucket: &str, key: &str) -> Result<(), Response> {
    let raw = BASE64_STANDARD
        .decode(policy_b64.as_bytes())
        .map_err(|_| deny("POST policy is not valid base64"))?;
    let doc: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|_| deny("POST policy is not valid JSON"))?;

    let expiration = doc
        .get("expiration")
        .and_then(|v| v.as_str())
        .ok_or_else(|| deny("POST policy has no expiration"))?;
    let expires_at = DateTime::parse_from_rfc3339(expiration)
        .map_err(|_| deny("POST policy expiration is malformed"))?
        .with_timezone(&Utc);
    if Utc::now() > expires_at {
        return Err(deny("POST policy has expired"));
    }

    let conditions = doc
        .get("conditions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| deny("POST policy has no conditions"))?;

    let mut bucket_ok = false;
    let mut key_ok = false;
    for condition in conditions {
        match condition {
            // Object form: {"bucket": "name"} or {"key": "value"}.
            serde_json::Value::Object(map) => {
                if let Some(v) = map.get("bucket").and_then(|v| v.as_str()) {
                    if v != bucket {
                        return Err(deny("POST policy bucket condition does not match"));
                    }
                    bucket_ok = true;
                }
                if let Some(v) = map.get("key").and_then(|v| v.as_str()) {
                    if v != key {
                        return Err(deny("POST policy key condition does not match"));
                    }
                    key_ok = true;
                }
            }
            // Array form: ["eq", "$key", "value"] or ["starts-with", "$key", "prefix"].
            serde_json::Value::Array(items) => {
                let op = items.first().and_then(|v| v.as_str()).unwrap_or("");
                let target = items.get(1).and_then(|v| v.as_str()).unwrap_or("");
                let value = items.get(2).and_then(|v| v.as_str()).unwrap_or("");
                let subject = match target {
                    "$key" => Some((&mut key_ok, key)),
                    "$bucket" => Some((&mut bucket_ok, bucket)),
                    _ => None,
                };
                if let Some((flag, actual)) = subject {
                    let matches = match op {
                        "eq" => actual == value,
                        "starts-with" => actual.starts_with(value),
                        _ => true, // unrecognized op on a known field — don't constrain
                    };
                    if !matches {
                        return Err(deny("POST policy condition does not match request"));
                    }
                    *flag = true;
                }
            }
            _ => {}
        }
    }
    if !bucket_ok || !key_ok {
        return Err(deny("POST policy does not constrain bucket and key"));
    }
    Ok(())
}

// ─── Core validator ───────────────────────────────────────────────────────────

fn validate_request(state: &AuthState, request: &Request<Body>) -> Result<Principal, &'static str> {
    if matches!(
        request.uri().path(),
        "/minio/health/live" | "/minio/health/ready"
    ) || request.uri().path().starts_with("/minio/v2/metrics/")
        || request.uri().path() == "/minio/prometheus/metrics"
    {
        return Ok(Principal::Root);
    }

    let uri_str = request.uri().to_string();

    if uri_str.contains("X-Amz-Signature=") {
        return validate_presigned(state, request);
    }
    if uri_str.contains("AWSAccessKeyId=") && uri_str.contains("Signature=") {
        return validate_signature_v2_query(state, request);
    }

    let auth = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or("Missing Authorization header")?;

    if let Some(v2) = auth.strip_prefix("AWS ") {
        return validate_signature_v2(state, request, v2);
    }

    if !auth.starts_with("AWS4-HMAC-SHA256 ") {
        return Err("Unsupported auth scheme");
    }

    let parsed = parse_auth_header(auth).ok_or("Malformed Authorization header")?;
    let (secret, principal) = state
        .lookup(&parsed.access_key)
        .ok_or("Unknown access key")?;

    let date = request
        .headers()
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .ok_or("Missing x-amz-date header")?;

    let payload_hash = request
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");

    let signed_at = parse_sigv4_time(date)?;
    let now = Utc::now();
    if (now - signed_at).num_seconds().abs() > MAX_SIGNATURE_CLOCK_SKEW_SECS {
        return Err("Request timestamp is outside the allowed clock skew");
    }
    let date_only = &date[..8]; // "YYYYMMDD"
    if parsed.scope_date != date_only
        || parsed.service != "s3"
        || parsed.terminator != "aws4_request"
    {
        return Err("Invalid credential scope");
    }

    let canonical = build_canonical_request(request, &parsed.signed_headers, payload_hash);
    let string_to_sign = build_string_to_sign(date, &parsed.credential_scope, &canonical);
    let signing_key = derive_signing_key(&secret, date_only, &parsed.region, "s3");
    let expected = hex_hmac(&signing_key, string_to_sign.as_bytes());

    if !constant_time_eq(&expected, &parsed.signature) {
        return Err("Signature does not match");
    }
    Ok(principal)
}

fn validate_signature_v2(
    state: &AuthState,
    request: &Request<Body>,
    value: &str,
) -> Result<Principal, &'static str> {
    let (access_key, signature) = value
        .split_once(':')
        .ok_or("Malformed Authorization header")?;
    let (secret, principal) = state.lookup(access_key).ok_or("Unknown access key")?;
    let string_to_sign = signature_v2_string_to_sign(request);
    let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(string_to_sign.as_bytes());
    let expected = BASE64_STANDARD.encode(mac.finalize().into_bytes());
    if !constant_time_eq(&expected, signature) {
        return Err("Signature does not match");
    }
    Ok(principal)
}

fn validate_signature_v2_query(
    state: &AuthState,
    request: &Request<Body>,
) -> Result<Principal, &'static str> {
    let query = request.uri().query().ok_or("Missing query string")?;
    let access_key = query_param(query, "AWSAccessKeyId").ok_or("Missing AWSAccessKeyId")?;
    let signature = query_param(query, "Signature").ok_or("Missing Signature")?;
    let expires = query_param(query, "Expires").ok_or("Missing Expires")?;
    let expires_epoch = expires
        .parse::<i64>()
        .map_err(|_| "Invalid Expires value")?;
    if Utc::now().timestamp() > expires_epoch {
        return Err("Presigned URL expired");
    }
    let (secret, principal) = state.lookup(&access_key).ok_or("Unknown access key")?;
    let string_to_sign = signature_v2_query_string_to_sign(request, &expires);
    let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(string_to_sign.as_bytes());
    let expected = BASE64_STANDARD.encode(mac.finalize().into_bytes());
    if !constant_time_eq(&expected, &signature) {
        return Err("Signature does not match");
    }
    Ok(principal)
}

fn signature_v2_string_to_sign(request: &Request<Body>) -> String {
    let headers = request.headers();
    let content_md5 = header_str(headers, "content-md5").unwrap_or("");
    let content_type = header_str(headers, "content-type").unwrap_or("");
    let date = if headers.contains_key("x-amz-date") {
        ""
    } else {
        header_str(headers, "date").unwrap_or("")
    };
    let amz_headers = canonicalized_amz_headers(headers);
    let resource = canonicalized_resource(request);
    format!(
        "{}\n{}\n{}\n{}\n{}{}",
        request.method().as_str(),
        content_md5,
        content_type,
        date,
        amz_headers,
        resource
    )
}

fn signature_v2_query_string_to_sign(request: &Request<Body>, expires: &str) -> String {
    let headers = request.headers();
    let content_md5 = header_str(headers, "content-md5").unwrap_or("");
    let content_type = header_str(headers, "content-type").unwrap_or("");
    let amz_headers = canonicalized_amz_headers(headers);
    let resource = canonicalized_resource(request);
    format!(
        "{}\n{}\n{}\n{}\n{}{}",
        request.method().as_str(),
        content_md5,
        content_type,
        expires,
        amz_headers,
        resource
    )
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn canonicalized_amz_headers(headers: &HeaderMap) -> String {
    let mut pairs = Vec::new();
    for (name, value) in headers {
        let name = name.as_str().to_ascii_lowercase();
        if !name.starts_with("x-amz-") {
            continue;
        }
        let value = value
            .to_str()
            .unwrap_or("")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        pairs.push((name, value));
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect()
}

fn canonicalized_resource(request: &Request<Body>) -> String {
    const SUBRESOURCES: &[&str] = &[
        "acl",
        "cors",
        "delete",
        "lifecycle",
        "location",
        "logging",
        "notification",
        "partNumber",
        "policy",
        "requestPayment",
        "response-cache-control",
        "response-content-disposition",
        "response-content-encoding",
        "response-content-language",
        "response-content-type",
        "response-expires",
        "tagging",
        "torrent",
        "uploadId",
        "uploads",
        "versionId",
        "versioning",
        "versions",
        "website",
    ];
    let mut resource = request.uri().path().to_string();
    let Some(query) = request.uri().query() else {
        return resource;
    };
    let mut params = query
        .split('&')
        .filter_map(|part| {
            let mut it = part.splitn(2, '=');
            let key_raw = it.next()?;
            let key = percent_decode(key_raw);
            if !SUBRESOURCES.contains(&key.as_str()) {
                return None;
            }
            let value = it.next().map(percent_decode).unwrap_or_default();
            Some((key, value, part.contains('=')))
        })
        .collect::<Vec<_>>();
    params.sort_by(|a, b| a.0.cmp(&b.0));
    if !params.is_empty() {
        resource.push('?');
        resource.push_str(
            &params
                .into_iter()
                .map(|(k, v, had_eq)| if had_eq { format!("{k}={v}") } else { k })
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    resource
}

fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        if percent_decode(key) == name {
            Some(percent_decode(value))
        } else {
            None
        }
    })
}

// ─── Pre-signed URL full SigV4 validation ────────────────────────────────────

/// Validates a pre-signed URL by:
/// 1. Checking that the URL has not expired (`X-Amz-Date + X-Amz-Expires`).
/// 2. Reconstructing the canonical request exactly as the signer did.
/// 3. Verifying the HMAC-SHA256 signature.
fn validate_presigned(state: &AuthState, request: &Request<Body>) -> Result<Principal, &'static str> {
    let raw_query = request.uri().query().unwrap_or("");

    // Decode all query parameters once.
    let params = parse_query_params(raw_query);

    let algorithm = params
        .get("X-Amz-Algorithm")
        .map(String::as_str)
        .unwrap_or("");
    if algorithm != "AWS4-HMAC-SHA256" {
        return Err("Unsupported presigned algorithm");
    }

    let credential = params
        .get("X-Amz-Credential")
        .ok_or("Missing X-Amz-Credential")?;
    let date_time_str = params.get("X-Amz-Date").ok_or("Missing X-Amz-Date")?;
    let expires_str = params.get("X-Amz-Expires").ok_or("Missing X-Amz-Expires")?;
    let signed_headers_str = params
        .get("X-Amz-SignedHeaders")
        .ok_or("Missing X-Amz-SignedHeaders")?;
    let signature = params
        .get("X-Amz-Signature")
        .ok_or("Missing X-Amz-Signature")?;

    // ── Parse credential scope ────────────────────────────────────────────
    let mut cred_parts = credential.splitn(6, '/');
    let access_key = cred_parts.next().ok_or("Invalid X-Amz-Credential")?;
    let date = cred_parts.next().ok_or("Invalid X-Amz-Credential")?;
    let region = cred_parts.next().ok_or("Invalid X-Amz-Credential")?;
    let service = cred_parts.next().ok_or("Invalid X-Amz-Credential")?;
    let terminator = cred_parts.next().ok_or("Invalid X-Amz-Credential")?;
    if service != "s3" || terminator != "aws4_request" {
        return Err("Invalid X-Amz-Credential scope");
    }
    let credential_scope = format!("{date}/{region}/{service}/{terminator}");

    let (secret, principal) = state.lookup(access_key).ok_or("Unknown access key")?;

    // ── Expiry check ──────────────────────────────────────────────────────
    let signed_at = NaiveDateTime::parse_from_str(date_time_str, "%Y%m%dT%H%M%SZ")
        .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
        .map_err(|_| "Invalid X-Amz-Date format")?;
    if date != signed_at.format("%Y%m%d").to_string() {
        return Err("X-Amz-Date does not match credential scope");
    }
    let expires_secs: i64 = expires_str
        .parse()
        .map_err(|_| "Invalid X-Amz-Expires value")?;
    // AWS rejects presigned URLs whose lifetime exceeds 7 days; enforce the
    // same ceiling so an over-long X-Amz-Expires can't mint a near-permanent
    // bearer URL.
    if expires_secs < 0 || expires_secs > 604_800 {
        return Err("X-Amz-Expires is out of range");
    }
    let expires_at = signed_at + chrono::Duration::seconds(expires_secs);
    let now = Utc::now();
    if signed_at > now + chrono::Duration::seconds(MAX_SIGNATURE_CLOCK_SKEW_SECS) {
        return Err("X-Amz-Date is too far in the future");
    }
    if now > expires_at {
        return Err("Presigned URL has expired");
    }

    // ── Canonical query string: all params except X-Amz-Signature, sorted ─
    let canonical_query = presigned_canonical_query(raw_query);

    // ── Canonical headers: only the signed headers ─────────────────────────
    // If a public_hostname is configured, substitute it for the `host` signed
    // header so verification is proxy-safe (the incoming Host header may have
    // been rewritten by a reverse proxy, but both client and server agree on
    // the configured public hostname).
    let signed_headers: Vec<String> = signed_headers_str.split(';').map(str::to_string).collect();
    let host_override: Option<HeaderMap> =
        state.config.auth.public_hostname.as_deref().and_then(|hostname| {
            if signed_headers
                .iter()
                .any(|h| h.eq_ignore_ascii_case("host"))
            {
                let mut m = request.headers().clone();
                if let Ok(v) = HeaderValue::from_str(hostname) {
                    m.insert(header::HOST, v);
                }
                Some(m)
            } else {
                None
            }
        });
    let headers_for_canon = host_override.as_ref().unwrap_or_else(|| request.headers());
    for signed_header in &signed_headers {
        if signed_header.eq_ignore_ascii_case("host") {
            continue;
        }
        if !headers_for_canon.contains_key(signed_header.as_str()) {
            return Err("Signed header is missing");
        }
    }
    let (canonical_hdrs, signed_hdrs_str) = canonical_headers(headers_for_canon, &signed_headers);

    // ── Build canonical request ────────────────────────────────────────────
    let method = request.method().as_str();
    let uri = canonical_uri(request.uri().path());
    let payload_hash = presigned_payload_hash(request.headers(), &signed_headers);
    let canonical = format!(
        "{method}\n{uri}\n{canonical_query}\n{canonical_hdrs}\n{signed_hdrs_str}\n{payload_hash}"
    );

    let string_to_sign = build_string_to_sign(date_time_str, &credential_scope, &canonical);
    let signing_key = derive_signing_key(&secret, date, region, service);
    let expected = hex_hmac(&signing_key, string_to_sign.as_bytes());

    if !constant_time_eq(&expected, signature) {
        return Err("Presigned signature does not match");
    }
    Ok(principal)
}

fn presigned_payload_hash(headers: &HeaderMap, signed_headers: &[String]) -> String {
    if signed_headers
        .iter()
        .any(|h| h.eq_ignore_ascii_case("x-amz-content-sha256"))
    {
        return headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("UNSIGNED-PAYLOAD")
            .to_string();
    }
    "UNSIGNED-PAYLOAD".to_string()
}

/// Builds the canonical query string for a presigned URL (excludes `X-Amz-Signature`).
///
/// Each parameter is decoded from the raw query, then re-encoded with standard
/// percent-encoding, and the pairs are sorted by encoded key.
fn presigned_canonical_query(raw_query: &str) -> String {
    let mut pairs: Vec<(String, String)> = raw_query
        .split('&')
        .filter_map(|part| {
            let mut it = part.splitn(2, '=');
            let k = it.next()?;
            let v = it.next().unwrap_or("");
            let dk = decode(k);
            if dk == "X-Amz-Signature" {
                return None;
            }
            Some((
                urlencoding::encode(&dk).into_owned(),
                urlencoding::encode(&decode(v)).into_owned(),
            ))
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Parses a raw query string into a `HashMap` of decoded key → decoded value.
fn parse_query_params(raw: &str) -> std::collections::HashMap<String, String> {
    raw.split('&')
        .filter_map(|part| {
            let mut it = part.splitn(2, '=');
            let k = it.next()?;
            let v = it.next().unwrap_or("");
            Some((decode(k), decode(v)))
        })
        .collect()
}

fn decode(s: &str) -> String {
    urlencoding::decode(s)
        .unwrap_or_else(|_| s.into())
        .into_owned()
}

fn percent_decode(s: &str) -> String {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ─── Canonical request construction ──────────────────────────────────────────

fn build_canonical_request(
    request: &Request<Body>,
    signed_headers: &[String],
    payload_hash: &str,
) -> String {
    let method = request.method().as_str();
    let uri = canonical_uri(request.uri().path());
    let query = canonical_query_string(request.uri().query().unwrap_or(""));
    let (canonical_hdrs, signed_hdrs_str) = canonical_headers(request.headers(), signed_headers);

    format!("{method}\n{uri}\n{query}\n{canonical_hdrs}\n{signed_hdrs_str}\n{payload_hash}")
}

/// Decodes each path segment and re-encodes it, preserving `/` separators.
fn canonical_uri(path: &str) -> String {
    path.split('/')
        .map(|seg| {
            let decoded = urlencoding::decode(seg).unwrap_or_else(|_| seg.into());
            urlencoding::encode(&decoded).into_owned()
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Decodes, re-encodes, and sorts all query parameters for the canonical form.
fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter_map(|part| {
            let mut it = part.splitn(2, '=');
            let k = it.next()?;
            let v = it.next().unwrap_or("");
            Some((
                urlencoding::encode(&decode(k)).into_owned(),
                urlencoding::encode(&decode(v)).into_owned(),
            ))
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(headers: &HeaderMap, signed_list: &[String]) -> (String, String) {
    let mut pairs: Vec<(String, String)> = signed_list
        .iter()
        .map(|name| {
            let value = headers
                .get(name.as_str())
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            (name.to_lowercase(), value)
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical = pairs
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect::<String>();
    let signed_str = pairs
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    (canonical, signed_str)
}

// ─── String to sign ───────────────────────────────────────────────────────────

fn build_string_to_sign(date_time: &str, credential_scope: &str, canonical: &str) -> String {
    let hash = hex_sha256(canonical.as_bytes());
    format!("AWS4-HMAC-SHA256\n{date_time}\n{credential_scope}\n{hash}")
}

// ─── Signing key derivation ───────────────────────────────────────────────────

/// Derives the SigV4 signing key via the four-step HMAC chain:
/// `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service), "aws4_request")`.
fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex_hmac(key: &[u8], data: &[u8]) -> String {
    hex::encode(hmac_sha256(key, data))
}

fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

// ─── Auth header parser ───────────────────────────────────────────────────────

struct ParsedAuth {
    access_key: String,
    credential_scope: String,
    scope_date: String,
    region: String,
    service: String,
    terminator: String,
    signed_headers: Vec<String>,
    signature: String,
}

fn parse_auth_header(header: &str) -> Option<ParsedAuth> {
    let body = header.strip_prefix("AWS4-HMAC-SHA256 ")?;
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in body.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v.trim());
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v.trim());
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v.trim().to_string());
        }
    }

    let credential = credential?;
    let mut parts = credential.splitn(6, '/');
    let access_key = parts.next()?.to_string();
    let date = parts.next()?.to_string();
    let region = parts.next()?.to_string();
    let service = parts.next()?.to_string();
    let terminator = parts.next()?.to_string();
    if parts.next().is_some() {
        return None;
    }
    let credential_scope = format!("{date}/{region}/{service}/{terminator}");

    let headers = signed_headers?.split(';').map(str::to_string).collect();

    Some(ParsedAuth {
        access_key,
        credential_scope,
        scope_date: date,
        region,
        service,
        terminator,
        signed_headers: headers,
        signature: signature?,
    })
}

fn parse_sigv4_time(value: &str) -> Result<DateTime<Utc>, &'static str> {
    NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ")
        .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc))
        .map_err(|_| "Invalid x-amz-date header")
}

// ─── Constant-time compare ────────────────────────────────────────────────────

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

// ─── Error response ───────────────────────────────────────────────────────────

fn access_denied() -> Response {
    let body = error_xml(&S3ErrorXml {
        code: "AccessDenied".to_string(),
        message: "Access Denied by user policy".to_string(),
        resource: "/".to_string(),
        request_id: "rust-s3-server".to_string(),
    });
    axum::response::Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header("content-type", "application/xml")
        .header("x-amz-request-id", "rust-s3-server")
        .body(Body::from(body))
        .unwrap()
}

fn deny(message: &'static str) -> Response {
    let body = error_xml(&S3ErrorXml {
        code: "SignatureDoesNotMatch".to_string(),
        message: message.to_string(),
        resource: "/".to_string(),
        request_id: "rust-s3-server".to_string(),
    });
    axum::response::Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header("content-type", "application/xml")
        .header("x-amz-request-id", "rust-s3-server")
        .body(Body::from(body))
        .unwrap()
}

// ─── Inline hex encoder ───────────────────────────────────────────────────────

mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Generates the query string for a SigV4 presigned URL (test helper).
///
/// Returns the full query string including `X-Amz-Signature`, ready to be
/// appended to a URL as `?<returned-string>`.
pub(crate) fn presign_query(
    method: &str,
    path: &str,
    host: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    datetime: &str, // "YYYYMMDDTHHMMSSZ"
    expires_secs: u64,
    extra_query: &[(&str, &str)],
) -> String {
    let date = &datetime[..8];
    let credential_scope = format!("{date}/{region}/s3/aws4_request");
    let credential = format!("{access_key}/{credential_scope}");

    // Collect all params that will be signed (everything except X-Amz-Signature).
    let mut params: Vec<(&str, String)> = vec![
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_string()),
        ("X-Amz-Credential", credential),
        ("X-Amz-Date", datetime.to_string()),
        ("X-Amz-Expires", expires_secs.to_string()),
        ("X-Amz-SignedHeaders", "host".to_string()),
    ];
    for (k, v) in extra_query {
        params.push((k, v.to_string()));
    }

    // Encode and sort — must match what presigned_canonical_query() produces.
    let mut encoded: Vec<(String, String)> = params
        .iter()
        .map(|(k, v)| {
            (
                urlencoding::encode(k).into_owned(),
                urlencoding::encode(v).into_owned(),
            )
        })
        .collect();
    encoded.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let canonical_query = encoded
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    // Canonical headers: only `host` is signed for presigned URLs.
    let canonical_hdrs = format!("host:{host}\n");

    // Canonical URI — mirrors canonical_uri().
    let uri: String = path
        .split('/')
        .map(|seg| {
            urlencoding::encode(&urlencoding::decode(seg).unwrap_or_else(|_| seg.into()))
                .into_owned()
        })
        .collect::<Vec<_>>()
        .join("/");

    let canonical =
        format!("{method}\n{uri}\n{canonical_query}\n{canonical_hdrs}\nhost\nUNSIGNED-PAYLOAD");

    let string_to_sign = build_string_to_sign(datetime, &credential_scope, &canonical);
    let signing_key = derive_signing_key(secret_key, date, region, "s3");
    let signature = hex_hmac(&signing_key, string_to_sign.as_bytes());

    format!("{canonical_query}&X-Amz-Signature={signature}")
}

#[cfg(test)]
pub(crate) fn presign_query_with_signed_headers(
    method: &str,
    path: &str,
    _host: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    datetime: &str,
    expires_secs: u64,
    signed_headers: &[(&str, &str)],
) -> String {
    let date = &datetime[..8];
    let credential_scope = format!("{date}/{region}/s3/aws4_request");
    let credential = format!("{access_key}/{credential_scope}");
    let signed_names = signed_headers
        .iter()
        .map(|(name, _)| name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let signed_header_string = signed_names.join(";");
    let mut params: Vec<(&str, String)> = vec![
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_string()),
        ("X-Amz-Credential", credential),
        ("X-Amz-Date", datetime.to_string()),
        ("X-Amz-Expires", expires_secs.to_string()),
        ("X-Amz-SignedHeaders", signed_header_string.clone()),
    ];

    let mut encoded: Vec<(String, String)> = params
        .iter_mut()
        .map(|(k, v)| {
            (
                urlencoding::encode(k).into_owned(),
                urlencoding::encode(v).into_owned(),
            )
        })
        .collect();
    encoded.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let canonical_query = encoded
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let mut canonical_headers = signed_headers
        .iter()
        .map(|(name, value)| {
            (
                name.to_ascii_lowercase(),
                value.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect::<Vec<_>>();
    canonical_headers.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical_hdrs = canonical_headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let payload_hash = canonical_headers
        .iter()
        .find(|(name, _)| name == "x-amz-content-sha256")
        .map(|(_, value)| value.as_str())
        .unwrap_or("UNSIGNED-PAYLOAD");

    let uri: String = path
        .split('/')
        .map(|seg| {
            urlencoding::encode(&urlencoding::decode(seg).unwrap_or_else(|_| seg.into()))
                .into_owned()
        })
        .collect::<Vec<_>>()
        .join("/");

    let canonical = format!(
        "{method}\n{uri}\n{canonical_query}\n{canonical_hdrs}\n{signed_header_string}\n{payload_hash}"
    );
    let string_to_sign = build_string_to_sign(datetime, &credential_scope, &canonical);
    let signing_key = derive_signing_key(secret_key, date, region, "s3");
    let signature = hex_hmac(&signing_key, string_to_sign.as_bytes());

    format!("{canonical_query}&X-Amz-Signature={signature}")
}

/// Generates an `Authorization` header value for a regular SigV4 request (test helper).
///
/// Signs `host` and `x-amz-date` with `UNSIGNED-PAYLOAD` as the payload hash.
/// The caller must send both `host: <host>` and `x-amz-date: <datetime>` headers.
#[cfg(test)]
pub(crate) fn compute_auth_header(
    method: &str,
    path: &str,
    query: &str,
    host: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    datetime: &str,
) -> String {
    let date = &datetime[..8];
    let credential_scope = format!("{date}/{region}/s3/aws4_request");

    let canonical_query = canonical_query_string(query);
    let uri = canonical_uri(path);

    // Sign host + x-amz-date, use UNSIGNED-PAYLOAD as the body hash.
    let canonical_hdrs = format!("host:{host}\nx-amz-date:{datetime}\n");
    let canonical = format!(
        "{method}\n{uri}\n{canonical_query}\n{canonical_hdrs}\nhost;x-amz-date\nUNSIGNED-PAYLOAD"
    );

    let string_to_sign = build_string_to_sign(datetime, &credential_scope, &canonical);
    let signing_key = derive_signing_key(secret_key, date, region, "s3");
    let signature = hex_hmac(&signing_key, string_to_sign.as_bytes());

    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders=host;x-amz-date, Signature={signature}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_uri_decodes_then_reencodes_each_segment() {
        assert_eq!(canonical_uri("/bucket/my%20key"), "/bucket/my%20key");
        assert_eq!(canonical_uri("/bucket/a+b"), "/bucket/a%2Bb");
    }

    #[test]
    fn canonical_query_sorts_params() {
        let q = canonical_query_string("b=2&a=1&a=0");
        assert!(q.starts_with("a="));
        let parts: Vec<_> = q.split('&').collect();
        assert!(parts.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn canonical_query_decodes_then_reencodes_values() {
        // A pre-encoded credential value must round-trip correctly.
        let q = canonical_query_string(
            "X-Amz-Credential=AKID%2F20130524%2Fus-east-1%2Fs3%2Faws4_request",
        );
        assert!(q.contains("X-Amz-Credential=AKID%2F20130524%2Fus-east-1%2Fs3%2Faws4_request"));
    }

    #[test]
    fn presigned_canonical_query_excludes_signature() {
        let raw =
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc123&X-Amz-Date=20130524T000000Z";
        let q = presigned_canonical_query(raw);
        assert!(!q.contains("X-Amz-Signature"));
        assert!(q.contains("X-Amz-Algorithm"));
        assert!(q.contains("X-Amz-Date"));
    }

    #[test]
    fn signing_key_derivation_is_deterministic() {
        let k1 = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
            "s3",
        );
        let k2 = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
            "s3",
        );
        assert_eq!(k1, k2);
        assert!(!k1.is_empty());
    }

    #[test]
    fn parse_auth_header_extracts_fields() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-date,Signature=abc123";
        let parsed = parse_auth_header(header).unwrap();
        assert_eq!(parsed.access_key, "AKID");
        assert_eq!(parsed.region, "us-east-1");
        assert_eq!(parsed.signature, "abc123");
        assert_eq!(parsed.signed_headers, vec!["host", "x-amz-date"]);
    }

    #[test]
    fn signature_v2_auth_header_is_accepted() {
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        config
            .auth
            .credentials
            .push(super::super::config::Credential {
                access_key: "AKID".to_string(),
                secret_key: "secret".to_string(),
            });
        let unsigned = Request::builder()
            .method("PUT")
            .uri("/bucket")
            .header("date", "Tue, 27 Mar 2007 19:36:42 +0000")
            .body(Body::empty())
            .unwrap();
        let string_to_sign = signature_v2_string_to_sign(&unsigned);
        let mut mac = HmacSha1::new_from_slice(b"secret").unwrap();
        mac.update(string_to_sign.as_bytes());
        let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());
        let request = Request::builder()
            .method("PUT")
            .uri("/bucket")
            .header("date", "Tue, 27 Mar 2007 19:36:42 +0000")
            .header("authorization", format!("AWS AKID:{signature}"))
            .body(Body::empty())
            .unwrap();
        let state = AuthState { config: Arc::new(config), iam: None };
        assert_eq!(validate_request(&state, &request), Ok(Principal::Root));
    }

    #[test]
    fn signature_v2_query_auth_is_accepted() {
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        config
            .auth
            .credentials
            .push(super::super::config::Credential {
                access_key: "AKID".to_string(),
                secret_key: "secret".to_string(),
            });
        let expires = "4102444800";
        let unsigned = Request::builder()
            .method("GET")
            .uri(format!("/bucket/key?AWSAccessKeyId=AKID&Expires={expires}"))
            .body(Body::empty())
            .unwrap();
        let string_to_sign = signature_v2_query_string_to_sign(&unsigned, expires);
        let mut mac = HmacSha1::new_from_slice(b"secret").unwrap();
        mac.update(string_to_sign.as_bytes());
        let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());
        let request = Request::builder()
            .method("GET")
            .uri(format!(
                "/bucket/key?AWSAccessKeyId=AKID&Expires={expires}&Signature={}",
                urlencoding::encode(&signature)
            ))
            .body(Body::empty())
            .unwrap();
        let state = AuthState { config: Arc::new(config), iam: None };
        assert_eq!(validate_request(&state, &request), Ok(Principal::Root));
    }

    #[test]
    fn minio_health_and_metrics_paths_bypass_auth() {
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        let state = AuthState { config: Arc::new(config), iam: None };
        for path in [
            "/minio/health/live",
            "/minio/health/ready",
            "/minio/v2/metrics/cluster",
            "/minio/v2/metrics/node",
            "/minio/v2/metrics/bucket",
            "/minio/v2/metrics/resource",
            "/minio/prometheus/metrics",
        ] {
            let request = Request::builder()
                .method("GET")
                .uri(path)
                .body(Body::empty())
                .unwrap();
            assert_eq!(validate_request(&state, &request), Ok(Principal::Root), "{path}");
        }
    }

    #[test]
    fn short_x_amz_date_is_rejected_not_panicked() {
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        config
            .auth
            .credentials
            .push(super::super::config::Credential {
                access_key: "AKID".to_string(),
                secret_key: "secret".to_string(),
            });
        let request = Request::builder()
            .method("GET")
            .uri("/")
            .header("x-amz-date", "1")
            .header("authorization", "AWS4-HMAC-SHA256 Credential=AKID/1/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123")
            .body(Body::empty())
            .unwrap();
        let state = AuthState { config: Arc::new(config), iam: None };
        assert_eq!(
            validate_request(&state, &request),
            Err("Invalid x-amz-date header")
        );
    }

    #[test]
    fn regular_sigv4_rejects_stale_replay() {
        let state = auth_state_with_root_key("AKID", "secret");
        let datetime = (Utc::now() - chrono::Duration::hours(1))
            .format("%Y%m%dT%H%M%SZ")
            .to_string();
        let authorization = compute_auth_header(
            "GET",
            "/bucket/key",
            "",
            "localhost",
            "AKID",
            "secret",
            "us-east-1",
            &datetime,
        );
        let request = Request::builder()
            .method("GET")
            .uri("/bucket/key")
            .header(header::HOST, "localhost")
            .header("x-amz-date", datetime)
            .header(header::AUTHORIZATION, authorization)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            validate_request(&state, &request),
            Err("Request timestamp is outside the allowed clock skew")
        );
    }

    fn signed_post_fields(
        access_key: &str,
        secret: &str,
        bucket: &str,
        key_prefix: &str,
        expiration: &str,
    ) -> std::collections::BTreeMap<String, String> {
        let region = "us-east-1";
        let date = "20260719";
        let policy_json = format!(
            r#"{{"expiration":"{expiration}","conditions":[{{"bucket":"{bucket}"}},["starts-with","$key","{key_prefix}"]]}}"#
        );
        let policy_b64 = BASE64_STANDARD.encode(policy_json.as_bytes());
        let signing_key = derive_signing_key(secret, date, region, "s3");
        let signature = hex_hmac(&signing_key, policy_b64.as_bytes());
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("policy".to_string(), policy_b64);
        fields.insert("x-amz-algorithm".to_string(), "AWS4-HMAC-SHA256".to_string());
        fields.insert(
            "x-amz-credential".to_string(),
            format!("{access_key}/{date}/{region}/s3/aws4_request"),
        );
        fields.insert("x-amz-signature".to_string(), signature);
        fields
    }

    fn auth_state_with_root_key(access_key: &str, secret: &str) -> AuthState {
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        config.auth.credentials.push(super::super::config::Credential {
            access_key: access_key.to_string(),
            secret_key: secret.to_string(),
        });
        AuthState {
            config: Arc::new(config),
            iam: None,
        }
    }

    #[test]
    fn browser_post_accepts_valid_signature_and_rejects_tampering() {
        let state = auth_state_with_root_key("AKID", "secret");
        let bucket = "b";
        let key = "uploads/photo.jpg";
        let expiration = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        let fields = signed_post_fields("AKID", "secret", bucket, "uploads/", &expiration);

        // Correctly signed and scoped: authorized.
        assert!(authorize_browser_post(&state, &fields, bucket, key).is_ok());

        // Missing signature: rejected.
        let mut no_sig = fields.clone();
        no_sig.remove("x-amz-signature");
        assert_eq!(
            authorize_browser_post(&state, &no_sig, bucket, key)
                .unwrap_err()
                .status(),
            StatusCode::FORBIDDEN
        );

        // Tampered signature: rejected.
        let mut bad_sig = fields.clone();
        bad_sig.insert("x-amz-signature".to_string(), "deadbeef".to_string());
        assert!(authorize_browser_post(&state, &bad_sig, bucket, key).is_err());

        // Unknown access key: rejected.
        let wrong_key = signed_post_fields("NOPE", "secret", bucket, "uploads/", &expiration);
        assert!(authorize_browser_post(&state, &wrong_key, bucket, key).is_err());
    }

    #[test]
    fn browser_post_signature_cannot_be_retargeted_or_replayed() {
        let state = auth_state_with_root_key("AKID", "secret");
        let expiration = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        let fields = signed_post_fields("AKID", "secret", "b", "uploads/", &expiration);

        // Same (validly signed) form, different bucket than the policy allows.
        assert!(authorize_browser_post(&state, &fields, "other", "uploads/x").is_err());
        // Key outside the signed prefix.
        assert!(authorize_browser_post(&state, &fields, "b", "secret/x").is_err());

        // Expired policy is rejected even with a valid signature.
        let past = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        let expired = signed_post_fields("AKID", "secret", "b", "uploads/", &past);
        assert!(authorize_browser_post(&state, &expired, "b", "uploads/x").is_err());
    }

    #[test]
    fn browser_post_upload_is_bucket_level_multipart_post_only() {
        let multipart_bucket = Request::builder()
            .method("POST")
            .uri("/my-bucket")
            .header(header::CONTENT_TYPE, "multipart/form-data; boundary=x")
            .body(Body::empty())
            .unwrap();
        assert!(is_browser_post_upload(&multipart_bucket));

        // Object-level POST (multipart completion) must NOT be treated as a
        // browser upload — it still goes through header signature validation.
        let object_post = Request::builder()
            .method("POST")
            .uri("/my-bucket/key?uploadId=abc")
            .header(header::CONTENT_TYPE, "multipart/form-data; boundary=x")
            .body(Body::empty())
            .unwrap();
        assert!(!is_browser_post_upload(&object_post));

        // Non-multipart bucket POST is not a browser upload.
        let json_post = Request::builder()
            .method("POST")
            .uri("/my-bucket")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::empty())
            .unwrap();
        assert!(!is_browser_post_upload(&json_post));
    }

    #[test]
    fn presigned_expires_over_seven_days_is_rejected() {
        let state = auth_state_with_root_key("AKID", "secret");
        let datetime = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let query = presign_query(
            "GET",
            "/bucket/key",
            "localhost",
            "AKID",
            "secret",
            "us-east-1",
            &datetime,
            604_801, // one second over the 7-day ceiling
            &[],
        );
        let request = Request::builder()
            .method("GET")
            .uri(format!("/bucket/key?{query}"))
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            validate_request(&state, &request),
            Err("X-Amz-Expires is out of range")
        );
    }

    #[test]
    fn presigned_url_rejects_a_far_future_start_time() {
        let state = auth_state_with_root_key("AKID", "secret");
        let datetime = (Utc::now() + chrono::Duration::hours(1))
            .format("%Y%m%dT%H%M%SZ")
            .to_string();
        let query = presign_query(
            "GET",
            "/bucket/key",
            "localhost",
            "AKID",
            "secret",
            "us-east-1",
            &datetime,
            60,
            &[],
        );
        let request = Request::builder()
            .method("GET")
            .uri(format!("/bucket/key?{query}"))
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            validate_request(&state, &request),
            Err("X-Amz-Date is too far in the future")
        );
    }

    #[test]
    fn claimed_access_key_is_extracted_without_logging_secrets() {
        let v4 = Request::builder()
            .uri("/bucket/key")
            .header(
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AK323434/20260719/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=secret-signature",
            )
            .body(Body::empty())
            .unwrap();
        assert_eq!(claimed_access_key(&v4).as_deref(), Some("AK323434"));

        let presigned = Request::builder()
            .uri("/bucket/key?X-Amz-Credential=AKPRESIGNED%2F20260719%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=secret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            claimed_access_key(&presigned).as_deref(),
            Some("AKPRESIGNED")
        );
    }
}
