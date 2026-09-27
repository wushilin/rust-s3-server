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
use hmac::{Hmac, KeyInit, Mac};
use sha1::Sha1;
use sha2::{Digest, Sha256};

use super::config::AppConfig;
use super::iam::{IamStore, Principal};
use super::key_usage::KeyUsageStore;
use super::identity::Identity;
use super::logging::{TARGET_AUTH, TARGET_AUTHZ};
use super::policy::{is_authorized, requirements_for_request, PolicyDocument};
use super::xml::{error_xml, S3ErrorXml};
use crate::storage::aws_chunked::{AwsChunkedDecoder, ChunkSigner};
use super::OperationActor;

type HmacSha256 = Hmac<Sha256>;
type HmacSha1 = Hmac<Sha1>;

const MAX_SIGNATURE_CLOCK_SKEW_SECS: i64 = 15 * 60;
/// Longest lifetime of a presigned URL (SigV4 `X-Amz-Expires`, SigV2 `Expires`).
const MAX_PRESIGNED_EXPIRES_SECS: i64 = 7 * 24 * 60 * 60;

/// Shared state for the auth middleware: static config credentials (root,
/// unrestricted) plus the IAM store (policy-bound access keys).
#[derive(Clone)]
pub struct AuthState {
    pub config: Arc<AppConfig>,
    pub iam: Option<IamStore>,
    /// "Last used" stamps for access keys; `None` disables tracking.
    pub usage: Option<KeyUsageStore>,
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

    /// Stamps `access_key` as used just now. Call only once its signature has
    /// verified, so a request merely *naming* a key never counts as a use.
    /// Hidden `RSWEB_…` console signing keys are never listed, so not tracked.
    pub(crate) fn record_key_use(&self, access_key: Option<&str>, from: Option<&str>) {
        let (Some(usage), Some(access_key)) = (self.usage.as_ref(), access_key) else {
            return;
        };
        if access_key.starts_with("RSWEB_") {
            return;
        }
        usage.record(access_key, crate::storage::time::now_ms(), from.unwrap_or("unknown"));
    }
}

/// The caller's address (see [`client_ip`]), attached to a request whose
/// authentication is deferred to a handler that no longer has the connection
/// info — the browser-POST form upload.
#[derive(Debug, Clone)]
pub(crate) struct ClientIp(pub String);

/// The address a request came from, for display.
///
/// That is the TCP peer — unless the peer is on a loopback or private network,
/// in which case it may be a reverse proxy, and the client address it reported
/// is used instead: the *last* `X-Forwarded-For` entry (the one that proxy
/// appended; earlier entries are whatever the client claimed), else
/// `X-Real-IP`. A peer on a public address is never allowed to speak for
/// anyone else, so an internet client cannot forge this.
pub(crate) fn client_ip(extensions: &axum::http::Extensions, headers: &HeaderMap) -> Option<String> {
    let peer = extensions
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|info| info.0.ip().to_canonical());
    if peer.is_some_and(|ip| !may_be_proxy(&ip)) {
        return peer.map(|ip| ip.to_string());
    }
    let forwarded = header_str(headers, "x-forwarded-for")
        .and_then(|value| value.rsplit(',').next())
        .or_else(|| header_str(headers, "x-real-ip"))
        .and_then(|value| value.trim().parse::<std::net::IpAddr>().ok())
        .map(|ip| ip.to_canonical());
    forwarded.or(peer).map(|ip| ip.to_string())
}

/// Loopback, private-range, and link-local peers — where a reverse proxy lives.
pub(crate) fn may_be_proxy(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique local fc00::/7
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
        }
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

    // Browser-based POST uploads carry their SigV4 authorization inside the
    // form body (`policy` + `x-amz-signature`), which this header-only
    // middleware cannot inspect. Hand multipart POSTs to the router with the
    // auth state attached: bucket-level POSTs verify the form signature in the
    // browser-POST handler, while object-level form POSTs return S3's
    // MethodNotAllowed response.
    // Only a genuine bucket-level browser form upload may defer authentication
    // to the form-signature check. A multipart/form-data POST that *also*
    // selects a real S3 operation via its query string (`?delete`, `?uploads`,
    // `?uploadId`, `?rebuildIndex`) must NOT be deferred — those routes are
    // dispatched before the form handler and perform no auth of their own, so
    // deferring them would let an attacker run them unauthenticated merely by
    // setting a `multipart/form-data` Content-Type header.
    if is_browser_post_upload(&request) && !post_selects_operation(&request) {
        log::debug!(target: TARGET_AUTH, "[{rid}] authn deferred to browser-POST form verification");
        request.extensions_mut().insert(state.clone());
        if let Some(ip) = client_ip(request.extensions(), request.headers()) {
            request.extensions_mut().insert(ClientIp(ip));
        }
        return next.run(request).await;
    }

    // Phase 1 — authentication: prove the caller holds a valid credential.
    let authn_start = std::time::Instant::now();
    let verified = match validate_request(&state, &request) {
        Ok(verified) => verified,
        Err(msg) => {
            // Explicit "not proceeding" record for every rejected request.
            log::warn!(
                target: TARGET_AUTH,
                "[{rid}] authn DENY method={} path={} from={} reason={msg} ({}µs)",
                request.method(),
                request.uri().path(),
                client_ip(request.extensions(), request.headers()).as_deref().unwrap_or("unknown"),
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
    let Verified {
        principal,
        access_key,
        chunk_signer,
    } = verified;
    log::debug!(
        target: TARGET_AUTH,
        "[{rid}] authn ok principal={principal:?} ({}µs)",
        authn_start.elapsed().as_micros()
    );
    // Attribute the request to the key whose signature actually verified —
    // not to whichever credential `claimed_access_key` happens to read first
    // when a request carries both a header and query credentials.
    let actor = operation_actor(&state, Some(&principal), access_key);
    // A `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` body is signed chunk by chunk;
    // the header signature covers only the seed. Verify every chunk as the
    // handler reads the body, so a tampered or spliced chunk fails the upload.
    if let Some(signer) = chunk_signer {
        let (parts, body) = request.into_parts();
        request = Request::from_parts(parts, verify_signed_chunks(body, signer));
    }
    let from = client_ip(request.extensions(), request.headers());
    state.record_key_use(actor.access_key.as_deref(), from.as_deref());
    let from = from.unwrap_or_else(|| "unknown".to_string());

    // Phase 2 — authorization: enforce the IAM policy bound to the caller
    // (root config credentials are unrestricted and skip this), then attach the
    // resolved identity so body-aware handlers (e.g. multi-object delete) can
    // authorize per item through the same `Identity::authorize` path.
    let identity = match &principal {
        Principal::Root => Identity::root(actor.username.clone(), actor.access_key.clone()),
        Principal::IamUser(username) => {
            let authz_start = std::time::Instant::now();
            let identity = match state.iam.as_ref() {
                Some(iam) => iam.identity_for(username),
                None => Identity::iam(username.clone(), None),
            };
            // Members of the admin group are root: nothing to evaluate.
            if identity.is_unrestricted() {
                log::debug!(target: TARGET_AUTHZ, "[{rid}] authz ok user={username} (admin)");
                request.extensions_mut().insert(identity);
                return with_operation_actor(next.run(request).await, actor);
            }
            let Some(policy) = identity.policy().cloned() else {
                log::warn!(target: TARGET_AUTHZ, "[{rid}] authz DENY user={username} from={from} reason=no_policy_attached");
                return with_operation_actor(access_denied(), actor);
            };
            if !authorize_iam(&policy, &request) {
                log::warn!(
                    target: TARGET_AUTHZ,
                    "[{rid}] authz DENY user={username} from={from} method={} uri={} ({}µs)",
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

/// The access key a request *names* — used only to attribute a request that
/// failed authentication. A verified request uses [`Verified::access_key`].
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

/// True when a POST's query string selects a concrete S3 operation whose
/// handler runs before (and instead of) the browser-POST form handler. Such
/// requests must always be authenticated through the header path; they can
/// never legitimately carry their credentials in a form body.
fn post_selects_operation(request: &Request<Body>) -> bool {
    request
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .any(|part| {
            // Decode the key with the SAME normalization the router uses
            // (`parse_s3_query` percent-decodes keys). Matching the raw bytes
            // here would let `?%64elete` (decodes to `delete`) slip past this
            // gate while the router still dispatches the operation — a parser
            // differential that reopens the unauthenticated-operation bypass.
            let raw_key = part.split('=').next().unwrap_or("");
            let key = super::percent_decode(raw_key);
            matches!(key.as_str(), "delete" | "uploads" | "uploadId" | "rebuildIndex")
        })
}

/// True for a `multipart/form-data` POST whose credentials may live in the
/// body, not the headers.
fn is_browser_post_upload(request: &Request<Body>) -> bool {
    if request.method() != axum::http::Method::POST {
        return false;
    }
    request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("multipart/form-data"))
        .unwrap_or(false)
}

/// What a verified browser POST may do: who it runs as and, when the policy
/// carries a `content-length-range` condition, the allowed file size.
#[derive(Debug, Default)]
pub(crate) struct BrowserPostGrant {
    pub actor: OperationActor,
    pub content_length_range: Option<(u64, u64)>,
}

/// Verifies a browser POST upload's SigV4 form signature and authorizes it.
///
/// Returns the resolved grant on success, or a ready-to-send error response on
/// failure. The form fields are the parsed `multipart/form-data` values that
/// precede the file part; the signature covers the base64 `policy` field, per
/// the S3 POST-upload signing scheme.
pub(crate) fn authorize_browser_post(
    state: &AuthState,
    fields: &std::collections::BTreeMap<String, String>,
    bucket: &str,
    key: &str,
) -> Result<BrowserPostGrant, Response> {
    if !state.config.auth.enabled {
        return Ok(BrowserPostGrant::default());
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
    let terminator = parts.next().ok_or_else(|| deny("Invalid x-amz-credential"))?;
    if service != "s3" || terminator != "aws4_request" {
        return Err(deny("Invalid x-amz-credential scope"));
    }

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
    // must actually cover this bucket/key and every other form field, so a
    // captured signature cannot be replayed against a different target or
    // with different metadata.
    let content_length_range = verify_post_policy_document(policy_b64, fields, bucket, key)?;

    // An IAM principal is still bound by its user policy (admins are root).
    if let Principal::IamUser(username) = &principal {
        let identity = match state.iam.as_ref() {
            Some(iam) => iam.identity_for(username),
            None => Identity::iam(username.clone(), None),
        };
        if !identity.authorize(&[super::policy::Requirement::object("s3:PutObject", bucket, key)]) {
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
    Ok(BrowserPostGrant {
        actor: OperationActor {
            username,
            access_key: Some(access_key.to_string()),
        },
        content_length_range,
    })
}

/// Form fields a POST policy need not mention: the signature machinery itself
/// (verified separately), the file, and `x-ignore-*` (as in S3). `bucket` is
/// implied by the URL and checked against it.
fn post_field_exempt_from_policy(name: &str) -> bool {
    matches!(
        name,
        "policy"
            | "file"
            | "bucket"
            | "x-amz-signature"
            | "x-amz-algorithm"
            | "x-amz-credential"
            | "x-amz-date"
            | "x-amz-security-token"
            | "signature"
            | "awsaccesskeyid"
    ) || name.starts_with("x-ignore-")
}

fn policy_u64(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Decodes and enforces the base64 POST policy: rejects an absent/expired
/// `expiration`; requires every condition (`{"field": "value"}`,
/// `["eq", "$field", v]`, `["starts-with", "$field", prefix]`,
/// `["content-length-range", min, max]`) to hold, rejecting any other
/// operator; requires `bucket` and `key` to be constrained; and requires every
/// submitted form field (bar [`post_field_exempt_from_policy`]) to be named by
/// some condition. Returns the `content-length-range`, if any, for the caller
/// to enforce against the file it receives.
fn verify_post_policy_document(
    policy_b64: &str,
    fields: &std::collections::BTreeMap<String, String>,
    bucket: &str,
    key: &str,
) -> Result<Option<(u64, u64)>, Response> {
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

    // A `bucket` form field, if sent, must name the bucket being posted to.
    if let Some((_, value)) = fields.iter().find(|(k, _)| k.eq_ignore_ascii_case("bucket")) {
        if value != bucket {
            return Err(deny("POST bucket field does not match the request bucket"));
        }
    }
    // The value a condition on `name` is checked against. Absent fields are
    // the empty string (so `starts-with ""` allows anything, `eq` fails).
    let actual = |name: &str| -> String {
        if name.eq_ignore_ascii_case("bucket") {
            return bucket.to_string();
        }
        if name.eq_ignore_ascii_case("key") {
            return key.to_string();
        }
        fields
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };

    let mut covered = std::collections::HashSet::new();
    let mut content_length_range = None;
    for condition in conditions {
        match condition {
            // Object form: {"field": "value"} — exact match.
            serde_json::Value::Object(map) => {
                for (name, expected) in map {
                    let expected = expected
                        .as_str()
                        .ok_or_else(|| deny("POST policy condition value is not a string"))?;
                    if actual(name) != expected {
                        return Err(deny("POST policy condition does not match request"));
                    }
                    covered.insert(name.to_ascii_lowercase());
                }
            }
            serde_json::Value::Array(items) => {
                let op = items
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| deny("POST policy condition is malformed"))?
                    .to_ascii_lowercase();
                if items.len() != 3 {
                    return Err(deny("POST policy condition is malformed"));
                }
                match op.as_str() {
                    "eq" | "starts-with" => {
                        let name = items[1]
                            .as_str()
                            .and_then(|t| t.strip_prefix('$'))
                            .ok_or_else(|| deny("POST policy condition is malformed"))?;
                        let expected = items[2]
                            .as_str()
                            .ok_or_else(|| deny("POST policy condition is malformed"))?;
                        let value = actual(name);
                        let matches = if op == "eq" {
                            value == expected
                        } else {
                            value.starts_with(expected)
                        };
                        if !matches {
                            return Err(deny("POST policy condition does not match request"));
                        }
                        covered.insert(name.to_ascii_lowercase());
                    }
                    "content-length-range" => {
                        let min = policy_u64(&items[1])
                            .ok_or_else(|| deny("POST policy content-length-range is malformed"))?;
                        let max = policy_u64(&items[2])
                            .ok_or_else(|| deny("POST policy content-length-range is malformed"))?;
                        if min > max {
                            return Err(deny("POST policy content-length-range is malformed"));
                        }
                        // Several ranges all have to hold: keep their intersection.
                        content_length_range = Some(match content_length_range {
                            Some((lo, hi)) => (min.max(lo), max.min(hi)),
                            None => (min, max),
                        });
                    }
                    _ => return Err(deny("POST policy has an unsupported condition operator")),
                }
            }
            _ => return Err(deny("POST policy condition is malformed")),
        }
    }
    if !covered.contains("bucket") || !covered.contains("key") {
        return Err(deny("POST policy does not constrain bucket and key"));
    }
    for name in fields.keys() {
        let name = name.to_ascii_lowercase();
        if !post_field_exempt_from_policy(&name) && !covered.contains(&name) {
            return Err(deny("POST form field is not covered by the policy"));
        }
    }
    Ok(content_length_range)
}

// ─── Core validator ───────────────────────────────────────────────────────────

/// A request whose credentials verified.
#[derive(Debug)]
struct Verified {
    principal: Principal,
    /// The access key whose signature verified (`None` for the unauthenticated
    /// health/metrics probes).
    access_key: Option<String>,
    /// Set for a `STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]` request: the
    /// context that verifies its per-chunk signatures, seeded with the header
    /// signature.
    chunk_signer: Option<ChunkSigner>,
}

impl Verified {
    fn key(principal: Principal, access_key: &str) -> Self {
        Self {
            principal,
            access_key: Some(access_key.to_string()),
            chunk_signer: None,
        }
    }
}

/// The first header that is present but not covered by `signed`: `host` must
/// always be signed, as must every `x-amz-*` header (bar `exempt`) and, when
/// `require_content_md5`, a `Content-MD5`. SigV4 only protects what is signed;
/// an unsigned `x-amz-copy-source`, `x-amz-meta-*`, `x-amz-decoded-content-length`
/// … could otherwise be added to, or altered on, a captured request.
fn first_unsigned_header(
    headers: &HeaderMap,
    signed: &[String],
    require_content_md5: bool,
    exempt: &[&str],
) -> Option<String> {
    let is_signed = |name: &str| signed.iter().any(|h| h.eq_ignore_ascii_case(name));
    if !is_signed("host") {
        return Some("host".to_string());
    }
    headers
        .keys()
        .map(|name| name.as_str())
        .find(|name| {
            (name.starts_with("x-amz-") || (require_content_md5 && *name == "content-md5"))
                && !exempt.contains(name)
                && !is_signed(name)
        })
        .map(str::to_string)
}

/// True for a concrete (hex SHA-256) payload hash.
fn is_concrete_payload_hash(value: &str) -> bool {
    value != "UNSIGNED-PAYLOAD" && !value.starts_with("STREAMING-")
}

fn validate_request(state: &AuthState, request: &Request<Body>) -> Result<Verified, &'static str> {
    // Only the exact health/metrics routes, and only for reads, are public.
    // Any other `/minio/...` path is an ordinary bucket (`minio`) request and
    // must authenticate like one.
    if matches!(
        *request.method(),
        axum::http::Method::GET | axum::http::Method::HEAD
    ) && super::is_probe_path(request.uri().path())
    {
        return Ok(Verified {
            principal: Principal::Root,
            access_key: None,
            chunk_signer: None,
        });
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
    // As in S3: every x-amz-* header present (and Content-MD5) must be signed.
    if first_unsigned_header(request.headers(), &parsed.signed_headers, true, &[]).is_some() {
        return Err("There were headers present in the request which were not signed");
    }
    // An aws-chunked body is only integrity-protected by a STREAMING-* payload
    // mode; paired with a concrete hash the body would go unchecked (the hash
    // is not comparable to the framed bytes).
    if is_concrete_payload_hash(payload_hash) && super::is_aws_chunked(request.headers()) {
        return Err("aws-chunked content requires a STREAMING x-amz-content-sha256");
    }
    let signed_streaming = match payload_hash {
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => true,
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => false,
        other if other.starts_with("STREAMING-") => {
            return Err("Unsupported streaming x-amz-content-sha256");
        }
        _ => false,
    };

    let canonical = build_canonical_request(request, &parsed.signed_headers, payload_hash);
    let string_to_sign = build_string_to_sign(date, &parsed.credential_scope, &canonical);
    let signing_key = derive_signing_key(&secret, date_only, &parsed.region, "s3");
    let expected = hex_hmac(&signing_key, string_to_sign.as_bytes());

    if !constant_time_eq(&expected, &parsed.signature) {
        return Err("Signature does not match");
    }
    let chunk_signer = signed_streaming.then(|| ChunkSigner {
        signing_key,
        amz_date: date.to_string(),
        scope: parsed.credential_scope.clone(),
        prev_signature: parsed.signature.clone(),
    });
    Ok(Verified {
        principal,
        access_key: Some(parsed.access_key),
        chunk_signer,
    })
}

/// Wraps a `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` body so every chunk signature
/// is verified as the body is read. Bytes pass through unchanged (the handler
/// still decodes the framing); a bad signature, malformed framing, or a body
/// that ends before its signed final chunk surfaces as a body error, which
/// fails the write before anything is committed.
fn verify_signed_chunks(body: Body, signer: ChunkSigner) -> Body {
    use futures::StreamExt;
    let stream = body.into_data_stream();
    let decoder = AwsChunkedDecoder::with_signer(signer);
    Body::from_stream(futures::stream::unfold(
        Some((stream, decoder)),
        |state| async move {
            let (mut stream, mut decoder) = state?;
            match stream.next().await {
                Some(Ok(bytes)) => match decoder.feed(&bytes) {
                    Ok(_) => Some((Ok(bytes), Some((stream, decoder)))),
                    Err(err) => Some((Err(std::io::Error::other(err.to_string())), None)),
                },
                Some(Err(err)) => Some((Err(std::io::Error::other(err.to_string())), None)),
                None => match decoder.finish() {
                    Ok(()) => None,
                    Err(err) => Some((Err(std::io::Error::other(err.to_string())), None)),
                },
            }
        },
    ))
}

fn validate_signature_v2(
    state: &AuthState,
    request: &Request<Body>,
    value: &str,
) -> Result<Verified, &'static str> {
    let (access_key, signature) = value
        .split_once(':')
        .ok_or("Malformed Authorization header")?;
    let (secret, principal) = state.lookup(access_key).ok_or("Unknown access key")?;

    // Freshness: bound the replay window using the signed date header (the same
    // value that goes into the string-to-sign). Without this, a captured SigV2
    // header request could be replayed verbatim forever — every other auth path
    // enforces skew/expiry, so this closes the one gap.
    let date_str = if request.headers().contains_key("x-amz-date") {
        header_str(request.headers(), "x-amz-date")
    } else {
        header_str(request.headers(), "date")
    }
    .ok_or("Missing Date header")?;
    let signed_at = DateTime::parse_from_rfc2822(date_str)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| "Invalid Date header")?;
    if (Utc::now() - signed_at).num_seconds().abs() > MAX_SIGNATURE_CLOCK_SKEW_SECS {
        return Err("Request timestamp is outside the allowed clock skew");
    }

    let string_to_sign = signature_v2_string_to_sign(request);
    let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(string_to_sign.as_bytes());
    let expected = BASE64_STANDARD.encode(mac.finalize().into_bytes());
    if !constant_time_eq(&expected, signature) {
        return Err("Signature does not match");
    }
    Ok(Verified::key(principal, access_key))
}

fn validate_signature_v2_query(
    state: &AuthState,
    request: &Request<Body>,
) -> Result<Verified, &'static str> {
    let query = request.uri().query().ok_or("Missing query string")?;
    let access_key = query_param(query, "AWSAccessKeyId").ok_or("Missing AWSAccessKeyId")?;
    let signature = query_param(query, "Signature").ok_or("Missing Signature")?;
    let expires = query_param(query, "Expires").ok_or("Missing Expires")?;
    let expires_epoch = expires
        .parse::<i64>()
        .map_err(|_| "Invalid Expires value")?;
    let now = Utc::now().timestamp();
    if now > expires_epoch {
        return Err("Presigned URL expired");
    }
    // Same 7-day ceiling as SigV4 presigning: a far-future `Expires` would
    // otherwise mint a practically permanent bearer URL.
    if expires_epoch - now > MAX_PRESIGNED_EXPIRES_SECS {
        return Err("Expires is too far in the future");
    }
    let (secret, principal) = state.lookup(&access_key).ok_or("Unknown access key")?;
    let string_to_sign = signature_v2_query_string_to_sign(request, &expires);
    let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(string_to_sign.as_bytes());
    let expected = BASE64_STANDARD.encode(mac.finalize().into_bytes());
    if !constant_time_eq(&expected, &signature) {
        return Err("Signature does not match");
    }
    Ok(Verified::key(principal, &access_key))
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
    let mut pairs: Vec<(String, Vec<String>)> = Vec::new();
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
        // Iterating a `HeaderMap` yields one entry per *value*, so a header
        // sent twice arrives as two pairs. SigV2, like SigV4, folds them into
        // a single comma-joined line; emitting two lines would sign a string
        // no client produces.
        match pairs.iter_mut().find(|(existing, _)| *existing == name) {
            Some((_, values)) => values.push(value),
            None => pairs.push((name, vec![value])),
        }
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}:{}\n", v.join(",")))
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
fn validate_presigned(state: &AuthState, request: &Request<Body>) -> Result<Verified, &'static str> {
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
    if expires_secs < 0 || expires_secs > MAX_PRESIGNED_EXPIRES_SECS {
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
    // Host-style requests are detected from the Host header itself, so the
    // host the client signed is exactly what arrived — never substitute it.
    let signed_headers: Vec<String> = signed_headers_str.split(';').map(str::to_string).collect();
    let host_style = request.extensions().get::<super::HostStyleRewrite>().is_some();
    let host_override: Option<HeaderMap> = if host_style { None } else {
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
        })
    };
    let headers_for_canon = host_override.as_ref().unwrap_or_else(|| request.headers());
    // Every x-amz-* header sent with a presigned URL must be one it signed.
    // `x-amz-content-sha256` is exempt: presigned payloads are UNSIGNED unless
    // the URL signs that header, and some tools attach it regardless; left
    // unsigned it can only make the server *reject* a body whose hash differs.
    if first_unsigned_header(request.headers(), &signed_headers, false, &["x-amz-content-sha256"])
        .is_some()
    {
        return Err("There were headers present in the request which were not signed");
    }
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
    let uri = canonical_uri(signed_path(request));
    let payload_hash = presigned_payload_hash(request.headers(), &signed_headers);
    if is_concrete_payload_hash(&payload_hash) && super::is_aws_chunked(request.headers()) {
        return Err("aws-chunked content requires a STREAMING x-amz-content-sha256");
    }
    let canonical = format!(
        "{method}\n{uri}\n{canonical_query}\n{canonical_hdrs}\n{signed_hdrs_str}\n{payload_hash}"
    );

    let string_to_sign = build_string_to_sign(date_time_str, &credential_scope, &canonical);
    let signing_key = derive_signing_key(&secret, date, region, service);
    let expected = hex_hmac(&signing_key, string_to_sign.as_bytes());

    if !constant_time_eq(&expected, signature) {
        return Err("Presigned signature does not match");
    }
    Ok(Verified::key(principal, access_key))
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

/// The path the client actually signed. Virtual-hosted-style requests carry
/// the bucket in the Host header and sign a bucket-less path; the rewrite
/// middleware folds the bucket into the routing path but preserves the
/// original in a [`super::HostStyleRewrite`] extension for verification.
fn signed_path(request: &Request<Body>) -> &str {
    request
        .extensions()
        .get::<super::HostStyleRewrite>()
        .map(|r| r.original_path.as_str())
        .unwrap_or_else(|| request.uri().path())
}

fn build_canonical_request(
    request: &Request<Body>,
    signed_headers: &[String],
    payload_hash: &str,
) -> String {
    let method = request.method().as_str();
    let uri = canonical_uri(signed_path(request));
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
            // A header sent more than once contributes *all* of its values,
            // comma-joined in the order received -- SigV4 folds repeats into one
            // canonical entry, and taking only the first would compute a
            // different string to sign than the client did. Real clients do send
            // repeats: the AWS SDK emits one `x-amz-object-attributes` header per
            // requested attribute rather than one comma-joined header.
            let value = headers
                .get_all(name.as_str())
                .iter()
                .filter_map(|v| v.to_str().ok())
                .map(|v| v.split_whitespace().collect::<Vec<_>>().join(" "))
                .collect::<Vec<_>>()
                .join(",");
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
        request_id: "rust-s3-server".to_string(),
        details: Vec::new(),
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
        request_id: "rust-s3-server".to_string(),
        details: Vec::new(),
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
/// Signs `host`, `x-amz-content-sha256` (`UNSIGNED-PAYLOAD`) and `x-amz-date`.
/// The caller must send those three headers with exactly these values.
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
    compute_auth_header_with_headers(
        method, path, query, host, access_key, secret_key, region, datetime, &[],
    )
}

/// [`compute_auth_header`] that also signs `extra` headers (which the caller
/// must send verbatim).
#[cfg(test)]
pub(crate) fn compute_auth_header_with_headers(
    method: &str,
    path: &str,
    query: &str,
    host: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    datetime: &str,
    extra: &[(&str, &str)],
) -> String {
    compute_auth_header_payload(
        method, path, query, host, access_key, secret_key, region, datetime, "UNSIGNED-PAYLOAD", extra,
    )
    .0
}

/// Returns `(authorization, seed_signature)` for a request whose
/// `x-amz-content-sha256` is `payload_hash`.
#[cfg(test)]
pub(crate) fn compute_auth_header_payload(
    method: &str,
    path: &str,
    query: &str,
    host: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    datetime: &str,
    payload_hash: &str,
    extra: &[(&str, &str)],
) -> (String, String) {
    let date = &datetime[..8];
    let credential_scope = format!("{date}/{region}/s3/aws4_request");

    let canonical_query = canonical_query_string(query);
    let uri = canonical_uri(path);

    let mut headers: Vec<(String, String)> = vec![
        ("host".to_string(), host.to_string()),
        ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
        ("x-amz-date".to_string(), datetime.to_string()),
    ];
    for (name, value) in extra {
        headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
    }
    headers.sort();
    let canonical_hdrs: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed = headers.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
    let canonical =
        format!("{method}\n{uri}\n{canonical_query}\n{canonical_hdrs}\n{signed}\n{payload_hash}");

    let string_to_sign = build_string_to_sign(datetime, &credential_scope, &canonical);
    let signing_key = derive_signing_key(secret_key, date, region, "s3");
    let signature = hex_hmac(&signing_key, string_to_sign.as_bytes());

    (
        format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed}, Signature={signature}"
        ),
        signature,
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
    fn canonical_headers_folds_a_repeated_header_into_one_comma_joined_entry() {
        // The AWS SDK sends one `x-amz-object-attributes` header per requested
        // attribute. Signing only the first value produces a different string
        // to sign than the client used, and every multi-attribute
        // GetObjectAttributes request fails with SignatureDoesNotMatch.
        let mut headers = HeaderMap::new();
        headers.append("x-amz-object-attributes", "ETag".parse().unwrap());
        headers.append("x-amz-object-attributes", "ObjectSize".parse().unwrap());
        headers.append("x-amz-object-attributes", "ObjectParts".parse().unwrap());
        headers.insert("host", "127.0.0.1:9000".parse().unwrap());

        let signed = vec!["host".to_string(), "x-amz-object-attributes".to_string()];
        let (canonical, signed_str) = canonical_headers(&headers, &signed);
        assert_eq!(
            canonical,
            "host:127.0.0.1:9000\nx-amz-object-attributes:ETag,ObjectSize,ObjectParts\n"
        );
        // One entry in the signed-headers list, however many values arrived.
        assert_eq!(signed_str, "host;x-amz-object-attributes");
    }

    #[test]
    fn canonicalized_amz_headers_folds_repeats_for_sigv2_too() {
        // Iterating a HeaderMap yields one pair per value, so the SigV2 path
        // would otherwise emit the same header name on two separate lines.
        let mut headers = HeaderMap::new();
        headers.append("x-amz-object-attributes", "ETag".parse().unwrap());
        headers.append("x-amz-object-attributes", "ObjectParts".parse().unwrap());
        headers.insert("x-amz-date", "20130524T000000Z".parse().unwrap());

        assert_eq!(
            canonicalized_amz_headers(&headers),
            "x-amz-date:20130524T000000Z\nx-amz-object-attributes:ETag,ObjectParts\n"
        );
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
        // Use a current date: SigV2 now enforces a clock-skew window, so a fixed
        // historical timestamp would (correctly) be rejected as a replay.
        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S +0000").to_string();
        let unsigned = Request::builder()
            .method("PUT")
            .uri("/bucket")
            .header("date", &date)
            .body(Body::empty())
            .unwrap();
        let string_to_sign = signature_v2_string_to_sign(&unsigned);
        let mut mac = HmacSha1::new_from_slice(b"secret").unwrap();
        mac.update(string_to_sign.as_bytes());
        let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());
        let request = Request::builder()
            .method("PUT")
            .uri("/bucket")
            .header("date", &date)
            .header("authorization", format!("AWS AKID:{signature}"))
            .body(Body::empty())
            .unwrap();
        let state = AuthState { config: Arc::new(config), iam: None, usage: None };
        assert_eq!(validate_request(&state, &request).map(|v| v.principal), Ok(Principal::Root));
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
        let expires_at = (Utc::now().timestamp() + 3600).to_string();
        let expires = expires_at.as_str();
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
        let state = AuthState { config: Arc::new(config), iam: None, usage: None };
        assert_eq!(validate_request(&state, &request).map(|v| v.principal), Ok(Principal::Root));
    }

    #[test]
    fn minio_health_and_metrics_paths_bypass_auth() {
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        let state = AuthState { config: Arc::new(config), iam: None, usage: None };
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
            assert_eq!(validate_request(&state, &request).map(|v| v.principal), Ok(Principal::Root), "{path}");
        }
    }

    #[test]
    fn probe_bypass_is_exact_and_read_only() {
        let state = auth_state_with_root_key("AKID", "secret");
        for (method, path) in [
            ("PUT", "/minio/health/live"),
            ("POST", "/minio/v2/metrics/cluster"),
            ("GET", "/minio/v2/metrics/x"),
            ("GET", "/minio/v2/metrics/cluster/"),
        ] {
            let request = Request::builder().method(method).uri(path).body(Body::empty()).unwrap();
            assert!(validate_request(&state, &request).is_err(), "{method} {path}");
        }
        let head = Request::builder().method("HEAD").uri("/minio/health/ready").body(Body::empty()).unwrap();
        assert!(validate_request(&state, &head).is_ok());
    }

    #[test]
    fn signature_v2_query_expires_is_capped_at_seven_days() {
        let state = auth_state_with_root_key("AKID", "secret");
        let sign = |expires: &str| {
            let unsigned = Request::builder()
                .uri(format!("/bucket/key?AWSAccessKeyId=AKID&Expires={expires}"))
                .body(Body::empty())
                .unwrap();
            let mut mac = HmacSha1::new_from_slice(b"secret").unwrap();
            mac.update(signature_v2_query_string_to_sign(&unsigned, expires).as_bytes());
            let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());
            Request::builder()
                .uri(format!(
                    "/bucket/key?AWSAccessKeyId=AKID&Expires={expires}&Signature={}",
                    urlencoding::encode(&signature)
                ))
                .body(Body::empty())
                .unwrap()
        };
        let far = (Utc::now().timestamp() + MAX_PRESIGNED_EXPIRES_SECS + 60).to_string();
        assert_eq!(
            validate_request(&state, &sign(&far)).map(|v| v.principal),
            Err("Expires is too far in the future")
        );
        let near = (Utc::now().timestamp() + 60).to_string();
        let verified = validate_request(&state, &sign(&near)).unwrap();
        assert_eq!(verified.access_key.as_deref(), Some("AKID"));
    }

    #[test]
    fn post_policy_rejects_unknown_operators_and_uncovered_fields() {
        let expiration = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        let encode = |conditions: serde_json::Value| {
            BASE64_STANDARD.encode(
                serde_json::json!({"expiration": expiration, "conditions": conditions}).to_string(),
            )
        };
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("key".to_string(), "a/b".to_string());
        let base = serde_json::json!([{"bucket": "b"}, ["starts-with", "$key", "a/"]]);
        assert_eq!(verify_post_policy_document(&encode(base.clone()), &fields, "b", "a/b").ok(), Some(None));

        let bad_op = serde_json::json!([{"bucket": "b"}, ["starts-with", "$key", "a/"], ["bogus", "$key", "x"]]);
        assert!(verify_post_policy_document(&encode(bad_op), &fields, "b", "a/b").is_err());

        let mut extra = fields.clone();
        extra.insert("success_action_redirect".to_string(), "https://evil".to_string());
        assert!(verify_post_policy_document(&encode(base), &extra, "b", "a/b").is_err());

        let ranged = serde_json::json!([{"bucket": "b"}, {"key": "a/b"}, ["content-length-range", "1", 100]]);
        assert_eq!(
            verify_post_policy_document(&encode(ranged), &fields, "b", "a/b").ok(),
            Some(Some((1, 100)))
        );
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
        let state = AuthState { config: Arc::new(config), iam: None, usage: None };
        assert_eq!(
            validate_request(&state, &request).map(|v| v.principal),
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
            validate_request(&state, &request).map(|v| v.principal),
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
            usage: None,
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

    fn ip_of(peer: &str, headers: &[(&str, &str)]) -> Option<String> {
        let mut extensions = axum::http::Extensions::new();
        extensions.insert(axum::extract::ConnectInfo(
            peer.parse::<std::net::SocketAddr>().unwrap(),
        ));
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        client_ip(&extensions, &map)
    }

    #[test]
    fn client_ip_trusts_forwarding_headers_only_from_a_private_peer() {
        // A public peer speaks only for itself, whatever it claims.
        assert_eq!(
            ip_of("203.0.113.9:4000", &[("x-forwarded-for", "10.1.1.1")]).as_deref(),
            Some("203.0.113.9")
        );
        // Behind a proxy: the entry the proxy appended (the last), never the
        // client-supplied ones before it.
        assert_eq!(
            ip_of("127.0.0.1:4000", &[("x-forwarded-for", "6.6.6.6, 198.51.100.7")]).as_deref(),
            Some("198.51.100.7")
        );
        assert_eq!(
            ip_of("192.168.44.1:4000", &[("x-real-ip", "198.51.100.8")]).as_deref(),
            Some("198.51.100.8")
        );
        // A private peer with no (or junk) forwarding info is a LAN client.
        assert_eq!(ip_of("192.168.44.62:4000", &[]).as_deref(), Some("192.168.44.62"));
        assert_eq!(
            ip_of("10.0.0.5:4000", &[("x-forwarded-for", "not-an-ip")]).as_deref(),
            Some("10.0.0.5")
        );
        // IPv4-mapped peers print as plain IPv4.
        assert_eq!(ip_of("[::ffff:203.0.113.9]:4000", &[]).as_deref(), Some("203.0.113.9"));
        // No connect info at all (a unit-tested router): nothing to report.
        assert_eq!(client_ip(&axum::http::Extensions::new(), &HeaderMap::new()), None);
    }

    #[tokio::test]
    async fn key_use_is_recorded_for_real_keys_but_not_console_signing_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = auth_state_with_root_key("AKROOT", "secret");
        state.usage = Some(KeyUsageStore::open(dir.path()).await.unwrap());
        let usage = state.usage.clone().unwrap();

        state.record_key_use(Some("AKROOT"), Some("198.51.100.7"));
        state.record_key_use(Some("RSWEB_alice_abc"), Some("198.51.100.7"));
        state.record_key_use(None, Some("198.51.100.7"));
        state.record_key_use(Some("RSAKNOIP"), None);

        let root = usage.get("AKROOT").unwrap();
        assert_eq!(root.last_used_from, "198.51.100.7");
        assert!(root.last_used_at_ms > 0);
        assert!(usage.get("RSWEB_alice_abc").is_none());
        assert_eq!(usage.get("RSAKNOIP").unwrap().last_used_from, "unknown");
    }

    #[test]
    fn browser_post_upload_is_multipart_post() {
        let multipart_bucket = Request::builder()
            .method("POST")
            .uri("/my-bucket")
            .header(header::CONTENT_TYPE, "multipart/form-data; boundary=x")
            .body(Body::empty())
            .unwrap();
        assert!(is_browser_post_upload(&multipart_bucket));

        // Object-level multipart/form-data POSTs also defer to routing. The
        // object handler returns MethodNotAllowed, matching S3's response for
        // browser POST uploads sent to an object resource.
        let object_post = Request::builder()
            .method("POST")
            .uri("/my-bucket/key?uploadId=abc")
            .header(header::CONTENT_TYPE, "multipart/form-data; boundary=x")
            .body(Body::empty())
            .unwrap();
        assert!(is_browser_post_upload(&object_post));

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
            validate_request(&state, &request).map(|v| v.principal),
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
            validate_request(&state, &request).map(|v| v.principal),
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
