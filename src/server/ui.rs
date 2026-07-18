//! Management UI — served on its **own port**, fully separate from the S3
//! API. Web logins use username/password only (built-in admin users from
//! the config file, or IAM users from `admin.sqlite`); the S3 API uses
//! access keys only. IAM users' policies are enforced on every UI object
//! operation exactly as they are on the S3 API.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use super::auth::presign_query;
use super::config::AppConfig;
use super::TrafficMetrics;
use super::iam::IamStore;
use super::policy::{is_authorized, PolicyDocument, Requirement};
use crate::storage::errors::StorageError;
use crate::storage::store::LocalObjectStore;

const SESSION_COOKIE: &str = "rusts3_ui_session";

#[derive(Clone)]
pub struct UiState {
    pub store: LocalObjectStore,
    pub iam: IamStore,
    pub config: Arc<AppConfig>,
    pub(crate) metrics: Arc<TrafficMetrics>,
}

/// A resolved UI session: `(username, is_root)`.
#[derive(Debug, Clone)]
struct UiSession {
    username: String,
    is_root: bool,
}

pub fn router(state: UiState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/users", get(list_users).post(create_user))
        .route("/api/users/:name", delete(delete_user))
        .route("/api/users/:name/password", put(reset_password))
        .route("/api/users/:name/policy", put(set_policy))
        .route("/api/users/:name/keys", get(list_keys).post(create_key))
        .route("/api/keys/:ak", delete(delete_key))
        .route("/api/buckets", get(list_buckets).post(create_bucket))
        .route("/api/buckets/:name", delete(delete_bucket))
        .route("/api/buckets/:name/stats", get(bucket_stats))
        .route(
            "/api/object",
            get(download_object)
                .put(upload_object)
                .delete(delete_object),
        )
        .route("/api/objects", get(list_objects))
        .route("/api/presign", post(presign))
        .route("/api/status", get(server_status))
        .layer(DefaultBodyLimit::max(5 * 1024 * 1024 * 1024))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("ui.html"))
}

// ── session helpers ──────────────────────────────────────────────────────────

fn session_of(state: &UiState, headers: &HeaderMap) -> Option<UiSession> {
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    let token = cookies.split(';').find_map(|c| {
        let (name, value) = c.trim().split_once('=')?;
        (name == SESSION_COOKIE).then(|| value.to_string())
    })?;
    let (username, is_root) = state.iam.resolve_session(&token)?;
    Some(UiSession { username, is_root })
}

fn require_session(state: &UiState, headers: &HeaderMap) -> Result<UiSession, Response> {
    session_of(state, headers).ok_or_else(|| error_response(StatusCode::UNAUTHORIZED, "not logged in"))
}

fn require_root(state: &UiState, headers: &HeaderMap) -> Result<UiSession, Response> {
    let session = require_session(state, headers)?;
    if !session.is_root {
        return Err(error_response(StatusCode::FORBIDDEN, "admin only"));
    }
    Ok(session)
}

/// Root sessions bypass policy; IAM sessions are evaluated exactly like
/// their access keys would be on the S3 API.
fn authorize(state: &UiState, session: &UiSession, requirements: &[Requirement]) -> bool {
    if session.is_root {
        return true;
    }
    let allowed = match state.iam.policy_for(&session.username) {
        Some(policy) => is_authorized(&policy, requirements),
        None => false,
    };
    if !allowed {
        for r in requirements {
            log::warn!(
                "ui access denied by policy user={} action={} resource={}",
                session.username, r.action, r.resource,
            );
        }
    }
    allowed
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

fn storage_error(err: StorageError) -> Response {
    let status = match err {
        StorageError::ObjectNotFound { .. } | StorageError::BucketNotFound(_) => {
            StatusCode::NOT_FOUND
        }
        StorageError::BucketRebuilding(_) => StatusCode::SERVICE_UNAVAILABLE,
        StorageError::InvalidMultipartUpload(_) => StatusCode::CONFLICT,
        StorageError::InvalidBucketName(_) | StorageError::InvalidObjectKey(_) => {
            StatusCode::BAD_REQUEST
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    error_response(status, err.to_string())
}

// ── auth endpoints ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

async fn login(State(state): State<UiState>, Json(req): Json<LoginRequest>) -> Response {
    // Built-in admin users (config file) first — they always win over any
    // same-named sqlite user and are unrestricted.
    if let Some(builtin) = state.config.find_builtin_user(&req.username) {
        let ok = builtin
            .password
            .as_deref()
            .map(|p| verify_builtin_password(p, &req.password))
            .unwrap_or(false);
        if !ok {
            log::warn!("ui login failed user={} kind=builtin", req.username);
            return error_response(StatusCode::UNAUTHORIZED, "invalid credentials");
        }
        log::info!("ui login user={} kind=builtin root=true", req.username);
        return session_response(&state, &req.username, true);
    }
    match state.iam.verify_password(&req.username, &req.password).await {
        Ok(true) => {
            log::info!("ui login user={} kind=iam root=false", req.username);
            session_response(&state, &req.username, false)
        }
        Ok(false) => {
            log::warn!("ui login failed user={} kind=iam", req.username);
            error_response(StatusCode::UNAUTHORIZED, "invalid credentials")
        }
        Err(err) => storage_error(err),
    }
}

fn session_response(state: &UiState, username: &str, is_root: bool) -> Response {
    let token = state.iam.create_session(username, is_root);
    let cookie = format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/");
    (
        [(header::SET_COOKIE, cookie)],
        Json(json!({ "username": username, "is_root": is_root })),
    )
        .into_response()
}

async fn logout(State(state): State<UiState>, headers: HeaderMap) -> Response {
    if let Some(cookies) = headers.get(header::COOKIE).and_then(|v| v.to_str().ok()) {
        if let Some(token) = cookies.split(';').find_map(|c| {
            let (name, value) = c.trim().split_once('=')?;
            (name == SESSION_COOKIE).then(|| value.to_string())
        }) {
            state.iam.destroy_session(&token);
        }
    }
    let clear = format!("{SESSION_COOKIE}=; Max-Age=0; HttpOnly; SameSite=Strict; Path=/");
    ([(header::SET_COOKIE, clear)], Json(json!({"ok": true}))).into_response()
}

async fn me(State(state): State<UiState>, headers: HeaderMap) -> Response {
    match require_session(&state, &headers) {
        Ok(s) => Json(json!({ "username": s.username, "is_root": s.is_root })).into_response(),
        Err(resp) => resp,
    }
}

// ── user management (root only, sqlite-backed IAM users) ─────────────────────

async fn list_users(State(state): State<UiState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_root(&state, &headers) {
        return resp;
    }
    let builtin: Vec<_> = state
        .config
        .auth
        .users
        .iter()
        .map(|u| json!({ "username": u.user, "builtin": true, "keys": u.api_keys.len() }))
        .collect();
    match state.iam.list_users().await {
        Ok(users) => {
            let mut out = builtin;
            out.extend(users.iter().map(|u| {
                json!({
                    "username": u.username,
                    "builtin": false,
                    "has_policy": u.policy.is_some(),
                    "policy": u.policy,
                    "created_at_ms": u.created_at_ms,
                })
            }));
            Json(json!({ "users": out })).into_response()
        }
        Err(err) => storage_error(err),
    }
}

#[derive(Deserialize)]
struct CreateUserRequest {
    username: String,
    password: String,
}

async fn create_user(
    State(state): State<UiState>,
    headers: HeaderMap,
    Json(req): Json<CreateUserRequest>,
) -> Response {
    let actor = match require_root(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if state.config.find_builtin_user(&req.username).is_some() {
        return error_response(
            StatusCode::CONFLICT,
            "name is reserved by a built-in config user",
        );
    }
    match state.iam.create_user(&req.username, &req.password).await {
        Ok(()) => {
            log::info!("ui audit actor={} action=create_user target={}", actor.username, req.username);
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn delete_user(
    State(state): State<UiState>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let actor = match require_root(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if state.config.find_builtin_user(&name).is_some() {
        return error_response(
            StatusCode::CONFLICT,
            "built-in users are config-managed and cannot be deleted at runtime",
        );
    }
    match state.iam.delete_user(&name).await {
        Ok(()) => {
            log::info!("ui audit actor={} action=delete_user target={name}", actor.username);
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => storage_error(err),
    }
}

#[derive(Deserialize)]
struct ResetPasswordRequest {
    password: String,
}

async fn reset_password(
    State(state): State<UiState>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<ResetPasswordRequest>,
) -> Response {
    let actor = match require_root(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if state.config.find_builtin_user(&name).is_some() {
        return error_response(
            StatusCode::CONFLICT,
            "built-in user passwords are config-managed",
        );
    }
    match state.iam.set_password(&name, &req.password).await {
        Ok(()) => {
            log::info!(
                "ui audit actor={} action=reset_password target={name}",
                actor.username,
            );
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn set_policy(
    State(state): State<UiState>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(policy): Json<Option<PolicyDocument>>,
) -> Response {
    let actor = match require_root(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if state.config.find_builtin_user(&name).is_some() {
        return error_response(
            StatusCode::CONFLICT,
            "built-in users are unrestricted and config-managed; policies cannot be attached",
        );
    }
    let attached = policy.is_some();
    match state.iam.set_policy(&name, policy.as_ref()).await {
        Ok(()) => {
            log::info!(
                "ui audit actor={} action={} target={name}",
                actor.username,
                if attached { "attach_policy" } else { "detach_policy" },
            );
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

// ── access keys ──────────────────────────────────────────────────────────────

fn may_manage_keys(session: &UiSession, target_user: &str) -> bool {
    session.is_root || session.username == target_user
}

async fn list_keys(
    State(state): State<UiState>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if !may_manage_keys(&session, &name) {
        return error_response(StatusCode::FORBIDDEN, "not your keys");
    }
    // Built-in users' keys are visible (access key only) but config-managed.
    if let Some(builtin) = state.config.find_builtin_user(&name) {
        return Json(json!({
            "keys": builtin.api_keys.iter().map(|k| json!({
                "access_key": k.ak,
                "builtin": true,
            })).collect::<Vec<_>>()
        }))
        .into_response();
    }
    match state.iam.list_access_keys(&name).await {
        Ok(keys) => Json(json!({
            "keys": keys.iter().map(|k| json!({
                "access_key": k.access_key,
                "created_at_ms": k.created_at_ms,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(err) => storage_error(err),
    }
}

async fn create_key(
    State(state): State<UiState>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if !may_manage_keys(&session, &name) {
        return error_response(StatusCode::FORBIDDEN, "not your keys");
    }
    if state.config.find_builtin_user(&name).is_some() {
        return error_response(
            StatusCode::CONFLICT,
            "built-in users are config-managed; add api_keys in the config file",
        );
    }
    match state.iam.create_access_key(&name).await {
        // The secret is returned exactly once, at creation.
        Ok(key) => {
            log::info!(
                "ui audit actor={} action=create_access_key target={name} ak={}",
                session.username,
                key.access_key,
            );
            Json(json!({
                "access_key": key.access_key,
                "secret_key": key.secret_key,
            }))
            .into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn delete_key(
    State(state): State<UiState>,
    headers: HeaderMap,
    axum::extract::Path(ak): axum::extract::Path<String>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if state.config.find_secret(&ak).is_some() {
        return error_response(
            StatusCode::CONFLICT,
            "built-in api keys are config-managed and cannot be deleted at runtime",
        );
    }
    let owner = state.iam.find_key(&ak).map(|(_, user)| user);
    let Some(owner) = owner else {
        return error_response(StatusCode::NOT_FOUND, "no such access key");
    };
    if !may_manage_keys(&session, &owner) {
        return error_response(StatusCode::FORBIDDEN, "not your key");
    }
    match state.iam.delete_access_key(&ak).await {
        Ok(()) => {
            log::info!(
                "ui audit actor={} action=delete_access_key owner={owner} ak={ak}",
                session.username,
            );
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => storage_error(err),
    }
}

// ── buckets & objects (policy-enforced) ──────────────────────────────────────

async fn list_buckets(State(state): State<UiState>, headers: HeaderMap) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match state.store.list_buckets().await {
        Ok(buckets) => {
            let visible: Vec<_> = buckets
                .into_iter()
                .filter(|(name, _)| {
                    authorize(
                        &state,
                        &session,
                        &[Requirement::bucket("s3:ListBucket", name)],
                    )
                })
                .map(|(name, meta)| json!({ "name": name, "created_at_ms": meta.created_at_ms }))
                .collect();
            Json(json!({ "buckets": visible })).into_response()
        }
        Err(err) => storage_error(err),
    }
}

#[derive(Deserialize)]
struct CreateBucketRequest {
    name: String,
}

async fn create_bucket(
    State(state): State<UiState>,
    headers: HeaderMap,
    Json(req): Json<CreateBucketRequest>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if !authorize(
        &state,
        &session,
        &[Requirement::bucket("s3:CreateBucket", &req.name)],
    ) {
        return error_response(StatusCode::FORBIDDEN, "denied by policy");
    }
    match state.store.create_bucket(&req.name).await {
        Ok(()) => {
            log::info!("ui audit actor={} action=create_bucket bucket={}", session.username, req.name);
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => storage_error(err),
    }
}

async fn delete_bucket(
    State(state): State<UiState>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if !authorize(
        &state,
        &session,
        &[Requirement::bucket("s3:DeleteBucket", &name)],
    ) {
        return error_response(StatusCode::FORBIDDEN, "denied by policy");
    }
    match state.store.delete_bucket(&name).await {
        Ok(()) => {
            log::info!(
                "ui audit actor={} action=delete_bucket bucket={name}",
                session.username,
            );
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => storage_error(err),
    }
}

async fn bucket_stats(
    State(state): State<UiState>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if !authorize(
        &state,
        &session,
        &[Requirement::bucket("s3:ListBucket", &name)],
    ) {
        return error_response(StatusCode::FORBIDDEN, "denied by policy");
    }
    match state.store.object_count(&name).await {
        Ok(objects) => Json(json!({"objects": objects})).into_response(),
        Err(err) => storage_error(err),
    }
}

#[derive(Deserialize)]
struct ListObjectsQuery {
    bucket: String,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    recursive: bool,
}

async fn list_objects(
    State(state): State<UiState>,
    headers: HeaderMap,
    Query(q): Query<ListObjectsQuery>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if !authorize(
        &state,
        &session,
        &[Requirement::bucket("s3:ListBucket", &q.bucket)],
    ) {
        return error_response(StatusCode::FORBIDDEN, "denied by policy");
    }
    match state
        .store
        .list_objects(
            &q.bucket,
            &q.prefix,
            if q.recursive { None } else { Some("/") },
            q.after.as_deref(),
            if q.recursive { 500 } else { 200 },
        )
        .await
    {
        Ok(page) => Json(json!({
            "entries": page.entries.iter().map(|e| json!({
                "key": e.object_key,
                "size": e.size,
                "etag": e.etag,
                "last_modified_ms": e.last_modified_ms,
            })).collect::<Vec<_>>(),
            "common_prefixes": page.common_prefixes,
            "is_truncated": page.is_truncated,
            "next_after": page.next_after,
        }))
        .into_response(),
        Err(err) => storage_error(err),
    }
}

#[derive(Deserialize)]
struct ObjectQuery {
    bucket: String,
    key: String,
}

async fn download_object(
    State(state): State<UiState>,
    headers: HeaderMap,
    Query(q): Query<ObjectQuery>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if !authorize(
        &state,
        &session,
        &[Requirement::object("s3:GetObject", &q.bucket, &q.key)],
    ) {
        return error_response(StatusCode::FORBIDDEN, "denied by policy");
    }
    let read = match state.store.read_object(&q.bucket, &q.key).await {
        Ok(read) => read,
        Err(err) => return storage_error(err),
    };
    log::info!(
        "ui audit actor={} action=download bucket={} key={} size={}",
        session.username, q.bucket, q.key, read.meta.size,
    );
    // Open every part before responding: open FDs survive a concurrent
    // overwrite retiring the dir mid-download.
    let mut files = Vec::with_capacity(read.meta.parts.len());
    for part in &read.meta.parts {
        match tokio::fs::File::open(read.object_dir.join(&part.file)).await {
            Ok(file) => files.push(file),
            Err(err) => return storage_error(err.into()),
        }
    }
    use tokio::io::AsyncReadExt;
    let mut reader: Box<dyn tokio::io::AsyncRead + Send + Unpin> = Box::new(tokio::io::empty());
    for file in files {
        reader = Box::new(reader.chain(file));
    }
    let active = state.metrics.begin_get();
    let stream = futures::StreamExt::map(
        tokio_util::io::ReaderStream::with_capacity(reader, 256 * 1024),
        move |chunk| {
            let _hold = &active;
            chunk
        },
    );
    let filename = q.key.rsplit('/').next().unwrap_or(&q.key).to_string();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, read.meta.content_type.clone())
        .header(header::CONTENT_LENGTH, read.meta.size)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename.replace('"', "")),
        )
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn upload_object(
    State(state): State<UiState>,
    headers: HeaderMap,
    Query(q): Query<ObjectQuery>,
    body: Body,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if !authorize(
        &state,
        &session,
        &[Requirement::object("s3:PutObject", &q.bucket, &q.key)],
    ) {
        return error_response(StatusCode::FORBIDDEN, "denied by policy");
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let _active = state.metrics.begin_put();
    let stream = body.into_data_stream();
    match state
        .store
        .put_object_stream(
            &q.bucket,
            &q.key,
            stream,
            content_type.as_deref(),
            None,
            false,
            None,
        )
        .await
    {
        Ok(result) => {
            log::info!(
                "ui audit actor={} action=upload bucket={} key={} size={}",
                session.username, q.bucket, q.key, result.size,
            );
            Json(json!({ "etag": result.etag, "size": result.size })).into_response()
        }
        Err(err) => storage_error(err),
    }
}

async fn delete_object(
    State(state): State<UiState>,
    headers: HeaderMap,
    Query(q): Query<ObjectQuery>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if !authorize(
        &state,
        &session,
        &[Requirement::object("s3:DeleteObject", &q.bucket, &q.key)],
    ) {
        return error_response(StatusCode::FORBIDDEN, "denied by policy");
    }
    match state.store.delete_object(&q.bucket, &q.key).await {
        Ok(()) => {
            log::info!(
                "ui audit actor={} action=delete_object bucket={} key={}",
                session.username, q.bucket, q.key,
            );
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => storage_error(err),
    }
}

// ── live server activity (top-bar status strip) ──────────────────────────────

/// What the server is busy with right now: rebuilds in progress (with live
/// counters) and in-flight transfers. Bucket names in the rebuild list are
/// filtered by the caller's ListBucket authority; counts are aggregate.
async fn server_status(State(state): State<UiState>, headers: HeaderMap) -> Response {
    let session = match require_root(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let now = crate::storage::time::now_ms();
    let rebuilds: Vec<_> = state
        .store
        .rebuild_statuses()
        .into_iter()
        .map(|(bucket, p)| {
            let visible = authorize(
                &state,
                &session,
                &[Requirement::bucket("s3:ListBucket", &bucket)],
            );
            json!({
                "bucket": if visible { bucket } else { "(bucket)".to_string() },
                "objects_indexed": p.objects_indexed,
                "dirs_trashed": p.dirs_trashed,
                "running_secs": now.saturating_sub(p.started_at_ms) / 1000,
            })
        })
        .collect();
    let (gets, puts) = state.metrics.active_transfers();
    let (bytes_in, bytes_out) = state.metrics.byte_totals();
    Json(json!({
        "rebuilds": rebuilds,
        "active": { "downloads": gets, "uploads": puts },
        "totals": { "bytes_in": bytes_in, "bytes_out": bytes_out },
        "now_ms": now,
    }))
    .into_response()
}

// ── presigned URL generation (share) ─────────────────────────────────────────

#[derive(Deserialize)]
struct PresignRequest {
    bucket: String,
    key: String,
    #[serde(default = "default_presign_expiry")]
    expires_secs: u64,
}

fn default_presign_expiry() -> u64 {
    3600
}

async fn presign(
    State(state): State<UiState>,
    headers: HeaderMap,
    Json(req): Json<PresignRequest>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    // The share link grants GetObject; the sharer must hold it themselves.
    if !authorize(
        &state,
        &session,
        &[Requirement::object("s3:GetObject", &req.bucket, &req.key)],
    ) {
        return error_response(StatusCode::FORBIDDEN, "denied by policy");
    }
    let Some(host) = state.config.auth.public_hostname.as_deref() else {
        return error_response(
            StatusCode::PRECONDITION_FAILED,
            "set auth.public_hostname in the config to enable share links",
        );
    };
    let base_url = format!(
        "{}://{}",
        state.config.auth.public_scheme.as_str(),
        host.trim_end_matches('/'),
    );

    // Sign with the caller's own credentials so the link carries exactly
    // the caller's authority (IAM policies keep applying at download time).
    let (access_key, secret_key) = if session.is_root {
        match state.config.first_root_key() {
            Some((ak, sk)) => (ak.to_string(), sk.to_string()),
            None => {
                return error_response(
                    StatusCode::PRECONDITION_FAILED,
                    "no root api_keys configured to sign with",
                )
            }
        }
    } else {
        match state.iam.list_access_keys(&session.username).await {
            Ok(keys) if !keys.is_empty() => {
                let k = &keys[0];
                (k.access_key.clone(), k.secret_key.clone())
            }
            Ok(_) => {
                return error_response(
                    StatusCode::PRECONDITION_FAILED,
                    "create an access key first — share links are signed with your key",
                )
            }
            Err(err) => return storage_error(err),
        }
    };

    let encoded_key = req
        .key
        .split('/')
        .map(|seg| urlencoding::encode(seg).into_owned())
        .collect::<Vec<_>>()
        .join("/");
    let path = format!("/{}/{}", req.bucket, encoded_key);
    let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let expires = req.expires_secs.clamp(1, 7 * 24 * 3600);
    let query = presign_query(
        "GET",
        &path,
        host,
        &access_key,
        &secret_key,
        "us-east-1",
        &datetime,
        expires,
        &[],
    );
    log::info!(
        "ui audit actor={} action=presign bucket={} key={} expires_secs={expires} signed_with={access_key}",
        session.username, req.bucket, req.key,
    );
    Json(json!({ "url": format!("{base_url}{path}?{query}") })).into_response()
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

fn verify_builtin_password(configured: &str, candidate: &str) -> bool {
    bcrypt::verify(candidate, configured).unwrap_or(false) || constant_time_eq(configured, candidate)
}

#[cfg(test)]
mod password_tests {
    use super::verify_builtin_password;

    #[test]
    fn builtin_password_accepts_bcrypt_and_plaintext() {
        let hash = bcrypt::hash("correct horse", bcrypt::DEFAULT_COST).unwrap();
        assert!(verify_builtin_password(&hash, "correct horse"));
        assert!(!verify_builtin_password(&hash, "wrong"));
        assert!(verify_builtin_password("legacy-cleartext", "legacy-cleartext"));
        assert!(!verify_builtin_password("legacy-cleartext", "wrong"));
    }
}
