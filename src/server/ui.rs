//! Management UI — served on its **own port**, fully separate from the S3
//! API. Web logins use username/password only (built-in admin users from
//! the config file, or IAM users from `admin.sqlite`); the S3 API uses
//! access keys only. IAM users' policies are enforced on every UI object
//! operation exactly as they are on the S3 API.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Extension, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use super::auth::presign_query;
use super::config::AppConfig;
use super::identity::Identity;
use super::logging::{TARGET_AUDIT, TARGET_AUTH, TARGET_AUTHZ};
use super::TrafficMetrics;
use super::iam::{Group, IamStore};
use super::policy::{
    compile_rules, decompile_rules, PolicyDocument, PolicyRule, Requirement,
};
use crate::storage::errors::StorageError;
use crate::storage::store::LocalObjectStore;

const SESSION_COOKIE: &str = "rusts3_ui_session";

#[derive(Clone)]
pub struct UiState {
    pub store: LocalObjectStore,
    pub iam: IamStore,
    pub config: Arc<AppConfig>,
    pub(crate) metrics: Arc<TrafficMetrics>,
    pub(crate) tasks: Arc<super::registry::TaskRegistry>,
}

/// A resolved UI session. Managed admin membership is resolved on every
/// request so promotions and demotions take effect immediately.
#[derive(Debug, Clone)]
struct UiSession {
    username: String,
    is_builtin: bool,
    is_admin: bool,
}

pub fn router(state: UiState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/tasks", get(tasks_page))
        .route("/assets/:file", get(ui_asset))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/ping", get(server_ping))
        .route("/api/users", get(list_users).post(create_user))
        .route("/api/users/:name", delete(delete_user))
        .route("/api/users/:name/password", put(reset_password))
        .route("/api/users/:name/policy", put(set_policy))
        .route("/api/users/:name/policy/rules", put(set_user_policy_rules))
        .route("/api/users/:name/groups", get(list_user_groups).put(set_user_groups))
        .route("/api/users/:name/keys", get(list_keys).post(create_key))
        .route("/api/groups", get(list_groups).post(create_group))
        .route("/api/groups/:name", delete(delete_group))
        .route("/api/groups/:name/policy", put(set_group_policy))
        .route("/api/groups/:name/policy/rules", put(set_group_policy_rules))
        .route("/api/policies/compile", post(compile_policy_rules))
        .route("/api/policies/decompile", post(decompile_policy_rules))
        .route("/api/keys/:ak", delete(delete_key))
        .route("/api/buckets", get(list_buckets).post(create_bucket))
        .route("/api/buckets/:name", delete(delete_bucket))
        .route("/api/buckets/:name/stats", get(bucket_stats))
        .route("/api/buckets/:name/rebuild", post(rebuild_bucket))
        .route(
            "/api/object",
            get(download_object)
                .put(upload_object)
                .delete(delete_object),
        )
        .route("/api/objects", get(list_objects))
        .route("/api/presign", post(presign))
        .route("/api/tasks", get(list_tasks))
        .route("/api/tasks/ws", get(tasks_ws))
        .route("/api/tasks/:id/cancel", post(cancel_task))
        .layer(DefaultBodyLimit::max(5 * 1024 * 1024 * 1024))
        // Outermost: give every UI request a correlation id (echoed back as
        // `x-amz-request-id`) and an access-log line — the same tracing/audit
        // path the S3 API gets from `log_middleware`. So console actions,
        // upload included, are traceable and auditable, not second-class.
        .layer(middleware::from_fn_with_state(state.clone(), ui_log_middleware))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("ui.html"))
}

/// The standalone "open in a new tab" task monitor. It reuses the same
/// `core.js` + `tasks.js` modules the console loads, so the live view is
/// identical; the page itself gates on the admin session client-side and the
/// `/api/tasks*` endpoints enforce it server-side.
async fn tasks_page() -> Html<&'static str> {
    Html(include_str!("assets/tasks.html"))
}

/// Serves the modular front-end assets. The JavaScript is split by concern
/// (core / tasks / objects / iam / main) and embedded in the binary — no build
/// step, no bundler, no external requests — then stitched back together by the
/// browser via ordered `<script src>` tags. Everything here is public client
/// code (the API endpoints it calls do the authorization).
async fn ui_asset(Path(file): Path<String>) -> Response {
    let body: &'static str = match file.as_str() {
        "core.js" => include_str!("assets/core.js"),
        "tasks.js" => include_str!("assets/tasks.js"),
        "objects.js" => include_str!("assets/objects.js"),
        "iam.js" => include_str!("assets/iam.js"),
        "main.js" => include_str!("assets/main.js"),
        _ => return error_response(StatusCode::NOT_FOUND, "asset not found"),
    };
    (
        [(header::CONTENT_TYPE, "application/javascript; charset=utf-8")],
        body,
    )
        .into_response()
}

// ── request tracing ──────────────────────────────────────────────────────────

/// Paths that are pure polling / health / static and would only flood the audit
/// log. They still receive a request id (cheap); they just don't get a per-
/// request audit line.
fn is_audit_noise(path: &str) -> bool {
    path.starts_with("/assets/")
        || matches!(
            path,
            "/" | "/tasks" | "/api/ping" | "/api/me" | "/api/tasks" | "/api/tasks/ws"
        )
}

/// The console counterpart to the S3 `log_middleware`: injects a correlation id
/// early (so handlers/pipeline share it), echoes it back as `x-amz-request-id`,
/// and writes one audit line per meaningful request — the same tracing/audit
/// path the S3 API already has. Runs outermost, before any handler.
async fn ui_log_middleware(
    State(state): State<UiState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let request_id = super::new_request_id();
    request
        .extensions_mut()
        .insert(super::RequestId(request_id.clone()));
    // Best-effort actor for the audit line; unauthenticated requests show "-".
    let actor = session_of(&state, request.headers())
        .map(|s| s.username)
        .unwrap_or_else(|| "-".to_string());
    let start = std::time::Instant::now();
    let mut response = next.run(request).await;
    let elapsed_ms = start.elapsed().as_millis();
    let status = response.status();
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-amz-request-id", value);
    }
    if !is_audit_noise(&path) {
        // Same positional shape as the S3 access log (mod.rs `log_middleware`):
        // `[request_id] actor operation target result suffix`. Here the request
        // access record uses the HTTP method as the operation and the path as
        // the target; the semantic verb/resource is emitted by `audit()` in the
        // same shape.
        let result = if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            "Denied".to_string()
        } else if status.is_success() || status.is_redirection() {
            "OK".to_string()
        } else {
            format!("ERROR({})", status.as_u16())
        };
        if status.is_client_error() || status.is_server_error() {
            log::warn!(target: TARGET_AUDIT, "[{request_id}] {actor} {method} {path} {result} in {elapsed_ms}ms");
        } else {
            log::info!(target: TARGET_AUDIT, "[{request_id}] {actor} {method} {path} {result} in {elapsed_ms}ms");
        }
    }
    response
}

// ── session helpers ──────────────────────────────────────────────────────────

fn session_of(state: &UiState, headers: &HeaderMap) -> Option<UiSession> {
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    let token = cookies.split(';').find_map(|c| {
        let (name, value) = c.trim().split_once('=')?;
        (name == SESSION_COOKIE).then(|| value.to_string())
    })?;
    let (username, is_builtin) = state.iam.resolve_session(&token)?;
    let is_admin = is_builtin || state.iam.is_admin(&username);
    Some(UiSession {
        username,
        is_builtin,
        is_admin,
    })
}

fn require_session(state: &UiState, headers: &HeaderMap) -> Result<UiSession, Response> {
    session_of(state, headers).ok_or_else(|| error_response(StatusCode::UNAUTHORIZED, "not logged in"))
}

fn require_root(state: &UiState, headers: &HeaderMap) -> Result<UiSession, Response> {
    let session = require_session(state, headers)?;
    if !session.is_admin {
        return Err(error_response(StatusCode::FORBIDDEN, "admin only"));
    }
    Ok(session)
}


/// Resolves the console session to the same [`Identity`] the S3 API uses, so
/// both front doors authorize through one code path. Built-in sessions are
/// unrestricted; IAM sessions carry their effective policy.
fn identity_of(state: &UiState, session: &UiSession) -> Identity {
    if session.is_builtin {
        Identity::root(Some(session.username.clone()), None)
    } else {
        Identity::iam(
            session.username.clone(),
            state.iam.policy_for(&session.username),
        )
    }
}

/// Built-in sessions bypass policy; IAM sessions are evaluated exactly like
/// their access keys would be on the S3 API — both via [`Identity::authorize`].
fn authorize(state: &UiState, session: &UiSession, requirements: &[Requirement]) -> bool {
    let allowed = identity_of(state, session).authorize(requirements);
    if !allowed {
        for r in requirements {
            log::warn!(
                target: TARGET_AUTHZ,
                "ui access denied by policy user={} action={} resource={}",
                session.username, r.action, r.resource,
            );
        }
    }
    allowed
}

/// Runs a console data verb through the shared pipeline: resolves the session
/// identity, authorizes it, and registers a task (broadcast) for the verb's
/// duration. Returns the [`TaskGuard`] to hold across the work, or a ready 403
/// on denial. This is how the console "eats its own dog food" — the same
/// authorize→broadcast path the S3 API uses, minus the HTTP layer.
fn begin_verb(
    state: &UiState,
    session: &UiSession,
    request_id: &str,
    op: &'static str,
    target: String,
    requirements: &[Requirement],
) -> Result<super::registry::TaskGuard, Response> {
    let identity = identity_of(state, session);
    super::pipeline::begin(
        &state.tasks,
        &identity,
        &session.username,
        request_id,
        super::registry::TaskKind::S3,
        op,
        target,
        requirements,
    )
    .map_err(|_| error_response(StatusCode::FORBIDDEN, "denied by policy"))
}

/// Records a semantic audit event: one line to the audit log (with the request
/// id) **and** an `Event::Audit` onto the hub, so control-plane actions are
/// both logged and available to external subscribers (webhooks/Kafka).
fn audit(state: &UiState, request_id: &str, actor: &str, action: &str, target: impl Into<String>) {
    let target = target.into();
    // Same positional shape as the S3 audit line: `[rid] actor op target result`.
    log::info!(target: TARGET_AUDIT, "[{request_id}] {actor} {action} {target} OK");
    state.tasks.publish(super::event_hub::Event::Audit {
        actor: actor.to_string(),
        action: action.to_string(),
        target,
        allowed: true,
        request_id: request_id.to_string(),
    });
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
            log::warn!(target: TARGET_AUTH, "ui login failed user={} kind=builtin", req.username);
            return error_response(StatusCode::UNAUTHORIZED, "invalid credentials");
        }
        log::info!(target: TARGET_AUTH, "ui login user={} kind=builtin root=true", req.username);
        return session_response(&state, &req.username, true);
    }
    match state.iam.verify_password(&req.username, &req.password).await {
        Ok(true) => {
            log::info!(target: TARGET_AUTH, "ui login user={} kind=iam root=false", req.username);
            session_response(&state, &req.username, false)
        }
        Ok(false) => {
            log::warn!(target: TARGET_AUTH, "ui login failed user={} kind=iam", req.username);
            error_response(StatusCode::UNAUTHORIZED, "invalid credentials")
        }
        Err(err) => storage_error(err),
    }
}

fn session_response(state: &UiState, username: &str, is_builtin: bool) -> Response {
    let token = state.iam.create_session(username, is_builtin);
    let is_admin = is_builtin || state.iam.is_admin(username);
    let cookie = format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/");
    (
        [(header::SET_COOKIE, cookie)],
        Json(json!({ "username": username, "is_admin": is_admin, "is_builtin": is_builtin })),
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
        Ok(s) => Json(json!({
            "username": s.username,
            "is_admin": s.is_admin,
            "is_builtin": s.is_builtin,
        }))
        .into_response(),
        Err(resp) => resp,
    }
}

async fn server_ping(State(state): State<UiState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_session(&state, &headers) {
        return resp;
    }
    let mut response = Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "now_ms": crate::storage::time::now_ms(),
    }))
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("static cache-control value"),
    );
    response
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
        .map(|u| json!({
            "username": u.user,
            "builtin": true,
            "is_admin": true,
            "groups": [Group::Admin.name()],
            "keys": u.api_keys.len(),
        }))
        .collect();
    match state.iam.list_users().await {
        Ok(users) => {
            let mut out = builtin;
            out.extend(users.iter().map(|u| {
                let groups = state.iam.groups_for(&u.username);
                json!({
                    "username": u.username,
                    "builtin": false,
                    "is_admin": state.iam.is_admin(&u.username),
                    "groups": groups,
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
    Extension(rid): Extension<super::RequestId>,
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
            audit(&state, &rid.0, &actor.username, "create_user", &req.username);
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn delete_user(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
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
    if actor.username == name && !actor.is_builtin {
        return error_response(
            StatusCode::CONFLICT,
            "administrators cannot delete their own account",
        );
    }
    match state.iam.delete_user(&name).await {
        Ok(()) => {
            audit(&state, &rid.0, &actor.username, "delete_user", &name);
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
    Extension(rid): Extension<super::RequestId>,
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
            audit(&state, &rid.0, &actor.username, "reset_password", &name);
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn set_policy(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
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
            audit(&state, &rid.0, &actor.username, if attached { "attach_policy" } else { "detach_policy" }, &name);
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

#[derive(Deserialize)]
struct PolicyRulesRequest {
    rules: Vec<PolicyRule>,
}

async fn compile_policy_rules(
    State(state): State<UiState>,
    headers: HeaderMap,
    Json(req): Json<PolicyRulesRequest>,
) -> Response {
    if let Err(resp) = require_root(&state, &headers) {
        return resp;
    }
    match compile_rules(&req.rules) {
        Ok(policy) => Json(json!({"policy": policy})).into_response(),
        Err(message) => error_response(StatusCode::BAD_REQUEST, message),
    }
}

async fn decompile_policy_rules(
    State(state): State<UiState>,
    headers: HeaderMap,
    Json(policy): Json<PolicyDocument>,
) -> Response {
    if let Err(resp) = require_root(&state, &headers) {
        return resp;
    }
    if let Err(message) = policy.validate() {
        return error_response(StatusCode::BAD_REQUEST, message);
    }
    match decompile_rules(&policy) {
        Ok(rules) => Json(json!({"rules": rules})).into_response(),
        Err(message) => error_response(StatusCode::CONFLICT, message),
    }
}

async fn set_user_policy_rules(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<PolicyRulesRequest>,
) -> Response {
    let actor = match require_root(&state, &headers) {
        Ok(session) => session,
        Err(resp) => return resp,
    };
    if state.config.find_builtin_user(&name).is_some() {
        return error_response(
            StatusCode::CONFLICT,
            "built-in users are config-managed",
        );
    }
    let policy = match compile_rules(&req.rules) {
        Ok(policy) => policy,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    match state.iam.set_policy(&name, Some(&policy)).await {
        Ok(()) => {
            audit(&state, &rid.0, &actor.username, "set_user_policy_rules", &name);
            Json(json!({"ok": true, "policy": policy})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

// ── IAM groups (administrators only) ─────────────────────────────────────────

async fn list_groups(State(state): State<UiState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_root(&state, &headers) {
        return resp;
    }
    match state.iam.list_groups().await {
        Ok(groups) => {
            let builtin_admins = state.config.auth.users.len() as u64;
            Json(json!({
                "groups": groups.iter().map(|group| json!({
                    "name": group.group.name(),
                    "policy": group.policy,
                    "has_policy": group.policy.is_some(),
                    "is_system": matches!(group.group, Group::Admin),
                    "members": group.members + match group.group {
                        Group::Admin => builtin_admins,
                        Group::Named(_) => 0,
                    },
                    "created_at_ms": group.created_at_ms,
                })).collect::<Vec<_>>()
            }))
            .into_response()
        }
        Err(err) => storage_error(err),
    }
}

#[derive(Deserialize)]
struct CreateGroupRequest {
    name: String,
    #[serde(default)]
    policy: Option<PolicyDocument>,
}

async fn create_group(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
    Json(req): Json<CreateGroupRequest>,
) -> Response {
    let actor = match require_root(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match state.iam.create_group(&req.name, req.policy.as_ref()).await {
        Ok(()) => {
            audit(&state, &rid.0, &actor.username, "create_group", &req.name);
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn set_group_policy(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(policy): Json<Option<PolicyDocument>>,
) -> Response {
    let actor = match require_root(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match state.iam.set_group_policy(&name, policy.as_ref()).await {
        Ok(()) => {
            audit(&state, &rid.0, &actor.username, "set_group_policy", &name);
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn set_group_policy_rules(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<PolicyRulesRequest>,
) -> Response {
    let actor = match require_root(&state, &headers) {
        Ok(session) => session,
        Err(resp) => return resp,
    };
    let policy = match compile_rules(&req.rules) {
        Ok(policy) => policy,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    match state.iam.set_group_policy(&name, Some(&policy)).await {
        Ok(()) => {
            audit(&state, &rid.0, &actor.username, "set_group_policy_rules", &name);
            Json(json!({"ok": true, "policy": policy})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn delete_group(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let actor = match require_root(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match state.iam.delete_group(&name).await {
        Ok(()) => {
            audit(&state, &rid.0, &actor.username, "delete_group", &name);
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

async fn list_user_groups(
    State(state): State<UiState>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    if let Err(resp) = require_root(&state, &headers) {
        return resp;
    }
    if state.config.find_builtin_user(&name).is_some() {
        return Json(json!({"groups": [Group::Admin.name()]})).into_response();
    }
    if !state.iam.user_exists(&name) {
        return error_response(StatusCode::NOT_FOUND, "no such user");
    }
    Json(json!({"groups": state.iam.groups_for(&name)})).into_response()
}

#[derive(Deserialize)]
struct SetUserGroupsRequest {
    groups: Vec<String>,
}

async fn set_user_groups(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Json(req): Json<SetUserGroupsRequest>,
) -> Response {
    let actor = match require_root(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if state.config.find_builtin_user(&name).is_some() {
        return error_response(StatusCode::CONFLICT, "built-in admin membership is immutable");
    }
    let parsed = match state.iam.resolve_groups(&req.groups).await {
        Ok(groups) => groups,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err.to_string()),
    };
    if actor.username == name
        && !actor.is_builtin
        && !parsed.iter().any(|group| matches!(group, Group::Admin))
    {
        return error_response(
            StatusCode::CONFLICT,
            "administrators cannot remove their own admin membership",
        );
    }
    match state.iam.set_user_groups(&name, &parsed).await {
        Ok(()) => {
            audit(&state, &rid.0, &actor.username, "set_user_groups", format!("{name} groups={:?}", req.groups));
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

// ── access keys ──────────────────────────────────────────────────────────────

fn may_manage_keys(session: &UiSession, target_user: &str) -> bool {
    session.is_admin || session.username == target_user
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
    Extension(rid): Extension<super::RequestId>,
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
            audit(&state, &rid.0, &session.username, "create_access_key", format!("{name} ak={}", key.access_key));
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
    Extension(rid): Extension<super::RequestId>,
    axum::extract::Path(ak): axum::extract::Path<String>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    // Hidden console signing keys (`RSWEB_…`) are never listed and cannot be
    // deleted individually — they live in a separate table and only die when
    // the owning user is deleted. Reject any attempt so the invariant is
    // explicit (defense in depth; `find_key` would already miss them).
    if ak.starts_with("RSWEB_") {
        return error_response(StatusCode::NOT_FOUND, "no such access key");
    }
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
            audit(&state, &rid.0, &session.username, "delete_access_key", format!("{owner} ak={ak}"));
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => storage_error(err),
    }
}

// ── buckets & objects (policy-enforced) ──────────────────────────────────────

async fn list_buckets(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    // Listing buckets itself isn't gated (per-bucket visibility is filtered
    // below); the empty requirement set just registers the verb for broadcast.
    let _guard = match begin_verb(&state, &session, &rid.0, "LIST_BUCKETS", "/".into(), &[]) {
        Ok(g) => g,
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
    Extension(rid): Extension<super::RequestId>,
    Json(req): Json<CreateBucketRequest>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let _guard = match begin_verb(
        &state,
        &session,
        &rid.0,
        "CREATE_BUCKET",
        format!("/{}", req.name),
        &[Requirement::bucket("s3:CreateBucket", &req.name)],
    ) {
        Ok(g) => g,
        Err(resp) => return resp,
    };
    match state.store.create_bucket(&req.name).await {
        Ok(()) => {
            audit(&state, &rid.0, &session.username, "create_bucket", format!("/{}", req.name));
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => storage_error(err),
    }
}

async fn delete_bucket(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let _guard = match begin_verb(
        &state,
        &session,
        &rid.0,
        "DELETE_BUCKET",
        format!("/{name}"),
        &[Requirement::bucket("s3:DeleteBucket", &name)],
    ) {
        Ok(g) => g,
        Err(resp) => return resp,
    };
    match state.store.delete_bucket(&name).await {
        Ok(()) => {
            audit(&state, &rid.0, &session.username, "delete_bucket", format!("/{name}"));
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => storage_error(err),
    }
}

async fn bucket_stats(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let _guard = match begin_verb(
        &state,
        &session,
        &rid.0,
        "BUCKET_STATS",
        format!("/{name}"),
        &[Requirement::bucket("s3:ListBucket", &name)],
    ) {
        Ok(g) => g,
        Err(resp) => return resp,
    };
    match state.store.object_count(&name).await {
        Ok(objects) => Json(json!({"objects": objects})).into_response(),
        Err(err) => storage_error(err),
    }
}

/// Triggers a registry-tracked index rebuild for a bucket. Authorized by the
/// `s3:RebuildIndex` action, so admin-group IAM users (or anyone granted it)
/// can run it — not just built-in root. Shows up in the task panel as a
/// `rebuild_index` job.
async fn rebuild_bucket(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
    Path(name): Path<String>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if !authorize(
        &state,
        &session,
        &[Requirement::bucket("s3:RebuildIndex", &name)],
    ) {
        return error_response(StatusCode::FORBIDDEN, "denied by policy");
    }
    if !state.store.bucket_exists(&name).await {
        return storage_error(StorageError::BucketNotFound(name));
    }
    // The rebuild runs as its own registry-tracked job (self-broadcasting), so
    // this handler just authorizes and spawns it.
    if super::jobs::rebuild_index::spawn(state.store.clone(), name.clone(), state.tasks.clone()) {
        audit(&state, &rid.0, &session.username, "rebuild_index", format!("/{name}"));
        (StatusCode::ACCEPTED, Json(json!({ "ok": true }))).into_response()
    } else {
        error_response(
            StatusCode::CONFLICT,
            "a rebuild is already running for this bucket",
        )
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
    Extension(rid): Extension<super::RequestId>,
    Query(q): Query<ListObjectsQuery>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let _guard = match begin_verb(
        &state,
        &session,
        &rid.0,
        "LIST",
        format!("/{}/{}", q.bucket, q.prefix),
        &[Requirement::bucket("s3:ListBucket", &q.bucket)],
    ) {
        Ok(g) => g,
        Err(resp) => return resp,
    };
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
    Extension(rid): Extension<super::RequestId>,
    Query(q): Query<ObjectQuery>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let guard = match begin_verb(
        &state,
        &session,
        &rid.0,
        "DOWNLOAD",
        format!("/{}/{}", q.bucket, q.key),
        &[Requirement::object("s3:GetObject", &q.bucket, &q.key)],
    ) {
        Ok(g) => g,
        Err(resp) => return resp,
    };
    let read = match state.store.read_object(&q.bucket, &q.key).await {
        Ok(read) => read,
        Err(err) => return storage_error(err),
    };
    audit(&state, &rid.0, &session.username, "download", format!("/{}/{}", q.bucket, q.key));
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
    // The task guard (from the pipeline above) rides inside the response stream
    // so the task stays visible for the whole transfer and a cancel stops
    // sending.
    let progress = guard.progress();
    progress.set_total(read.meta.size);
    let cancel = guard.cancel_token();
    let active = state.metrics.begin_get();
    let metrics = state.metrics.clone();
    let stream = futures::StreamExt::map(
        tokio_util::io::ReaderStream::with_capacity(reader, 256 * 1024),
        move |chunk| {
            let _hold = &active;
            let _task = &guard;
            if cancel.is_cancelled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "download cancelled by operator",
                ));
            }
            match chunk {
                Ok(bytes) => {
                    progress.add_done(bytes.len() as u64);
                    metrics.add_out(bytes.len() as u64);
                    Ok(bytes)
                }
                Err(err) => Err(err),
            }
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
    Extension(rid): Extension<super::RequestId>,
    body: Body,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let guard = match begin_verb(
        &state,
        &session,
        &rid.0,
        "UPLOAD",
        format!("/{}/{}", q.bucket, q.key),
        &[Requirement::object("s3:PutObject", &q.bucket, &q.key)],
    ) {
        Ok(g) => g,
        Err(resp) => return resp,
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    // The task guard (from the pipeline above) is held across the write; a
    // cancel errors the stream mid-transfer (staging reclaimed by the sweeper)
    // but cannot interrupt the atomic commit once the body is fully read.
    let progress = guard.progress();
    if let Some(total) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
    {
        progress.set_total(total);
    }
    let cancel = guard.cancel_token();
    let _active = state.metrics.begin_put();
    // Feed the shared byte counter so console traffic shows in the throughput
    // stats too (one relaxed atomic add per chunk — no allocation).
    let metrics = state.metrics.clone();
    let stream = futures::StreamExt::map(body.into_data_stream(), move |result| {
        if cancel.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "upload cancelled by operator",
            ));
        }
        match result {
            Ok(chunk) => {
                progress.add_done(chunk.len() as u64);
                metrics.add_in(chunk.len() as u64);
                Ok(chunk)
            }
            Err(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, err.to_string())),
        }
    });
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
            audit(&state, &rid.0, &session.username, "upload", format!("/{}/{}", q.bucket, q.key));
            Json(json!({ "etag": result.etag, "size": result.size })).into_response()
        }
        Err(err) => storage_error(err),
    }
}

async fn delete_object(
    State(state): State<UiState>,
    headers: HeaderMap,
    Extension(rid): Extension<super::RequestId>,
    Query(q): Query<ObjectQuery>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let _guard = match begin_verb(
        &state,
        &session,
        &rid.0,
        "DELETE",
        format!("/{}/{}", q.bucket, q.key),
        &[Requirement::object("s3:DeleteObject", &q.bucket, &q.key)],
    ) {
        Ok(g) => g,
        Err(resp) => return resp,
    };
    match state.store.delete_object(&q.bucket, &q.key).await {
        Ok(()) => {
            audit(&state, &rid.0, &session.username, "delete_object", format!("/{}/{}", q.bucket, q.key));
            Json(json!({"ok": true})).into_response()
        }
        Err(err) => storage_error(err),
    }
}

// ── active tasks (admin panel) ───────────────────────────────────────────────

/// Active-task list for the admin "what's running" panel. Every verb (S3
/// request or background job) that is in flight appears here with its type,
/// start time, and self-rendered status line.
fn task_to_json(task: &super::registry::TaskSnapshot) -> serde_json::Value {
    json!({
        "id": task.id,
        "kind": task.kind.as_str(),
        "op": task.op,
        "target": task.target,
        "started_at_ms": task.started_at_ms,
        "status": task.status,
        "cancellable": task.cancellable,
        "done": task.done,
        "total": task.total,
        "completed": task.completed,
    })
}

/// The `snapshot` envelope shared by the polling endpoint and the WebSocket
/// push: active + recently-finished tasks, plus cumulative byte totals (the
/// client derives up/down rates from successive deltas).
fn tasks_payload(state: &UiState) -> serde_json::Value {
    let tasks: Vec<_> = state.tasks.snapshot().iter().map(task_to_json).collect();
    let (bytes_in, bytes_out) = state.metrics.byte_totals();
    json!({
        "type": "snapshot",
        "now_ms": crate::storage::time::now_ms(),
        "tasks": tasks,
        "bytes_in": bytes_in,
        "bytes_out": bytes_out,
    })
}

/// Answers a client `probe` with a `reply` correlated by the same `id`: for
/// each requested task id, the current task object, or `null` if it's gone.
fn probe_reply(state: &UiState, req_id: &str, ids: &[String]) -> serde_json::Value {
    let snapshot = state.tasks.snapshot();
    let mut tasks = serde_json::Map::new();
    for id in ids {
        let found = snapshot
            .iter()
            .find(|t| &t.id == id)
            .map(task_to_json)
            .unwrap_or(serde_json::Value::Null);
        tasks.insert(id.clone(), found);
    }
    json!({ "type": "reply", "id": req_id, "tasks": tasks })
}

async fn list_tasks(State(state): State<UiState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_root(&state, &headers) {
        return resp;
    }
    Json(tasks_payload(&state)).into_response()
}

/// Live task stream. Pushes a `snapshot` envelope on every task entry/exit
/// (immediately) and on a 1s heartbeat (for progress), broadcast to every open
/// admin session. Also answers client `probe` messages with a correlated
/// `reply` on the same wire, so a client can reconcile a task it fears is
/// stale. Missed pushes are self-healing: the next snapshot is authoritative.
async fn tasks_ws(
    State(state): State<UiState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if require_root(&state, &headers).is_err() {
        return error_response(StatusCode::UNAUTHORIZED, "admin only");
    }
    ws.on_upgrade(move |socket| tasks_socket(socket, state))
}

#[derive(Deserialize)]
struct ProbeMsg {
    #[serde(rename = "type")]
    kind: String,
    id: String,
    #[serde(default)]
    tasks: Vec<String>,
}

async fn tasks_socket(mut socket: WebSocket, state: UiState) {
    use tokio::sync::broadcast::error::RecvError;
    let mut events = state.tasks.subscribe();
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(1));
    if socket
        .send(Message::Text(tasks_payload(&state).to_string()))
        .await
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if socket.send(Message::Text(tasks_payload(&state).to_string())).await.is_err() { break; }
            }
            recv = events.recv() => {
                match recv {
                    Err(RecvError::Closed) => break,
                    // A task started/finished, or we lagged — self-heal with a
                    // fresh snapshot.
                    Ok(super::event_hub::Event::TasksChanged) | Err(RecvError::Lagged(_)) => {
                        if socket.send(Message::Text(tasks_payload(&state).to_string())).await.is_err() { break; }
                    }
                    // Audit events share the bus (for external subscribers) but
                    // aren't a task change — the console panel ignores them.
                    Ok(super::event_hub::Event::Audit { .. }) => {}
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(probe) = serde_json::from_str::<ProbeMsg>(&text) {
                            if probe.kind == "probe" {
                                let reply = probe_reply(&state, &probe.id, &probe.tasks);
                                if socket.send(Message::Text(reply.to_string())).await.is_err() { break; }
                            }
                        }
                    }
                    Some(Ok(_)) => {}      // ping/pong/binary — ignore
                    _ => break,            // client closed or errored
                }
            }
        }
    }
}

/// Cooperatively cancels an active task by its correlation id, returning
/// explicit feedback the console surfaces to the operator.
async fn cancel_task(
    State(state): State<UiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = require_root(&state, &headers) {
        return resp;
    }
    match state.tasks.cancel(&id) {
        super::registry::CancelResult::Cancelled(n) => {
            log::info!("ui task cancel id={id} tasks={n}");
            (StatusCode::OK, Json(json!({ "cancelled": n }))).into_response()
        }
        super::registry::CancelResult::Refused => {
            error_response(StatusCode::CONFLICT, "This task cannot be cancelled")
        }
        super::registry::CancelResult::NotFound => {
            error_response(StatusCode::NOT_FOUND, "Task not found or already finished")
        }
    }
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
    Extension(rid): Extension<super::RequestId>,
    Json(req): Json<PresignRequest>,
) -> Response {
    let session = match require_session(&state, &headers) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    // The share link grants GetObject; the sharer must hold it themselves.
    let _guard = match begin_verb(
        &state,
        &session,
        &rid.0,
        "PRESIGN",
        format!("/{}/{}", req.bucket, req.key),
        &[Requirement::object("s3:GetObject", &req.bucket, &req.key)],
    ) {
        Ok(g) => g,
        Err(resp) => return resp,
    };
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

    // Always sign with the caller's hidden `RSWEB_…` signing key (created on
    // first use). Nobody has to configure a real access key just to share, and
    // the link carries exactly the caller's authority: it resolves to the
    // caller on verification, so their IAM policy is re-checked at download time
    // (revocation and policy changes apply, and a deleted user's links stop
    // working even if the key row lingers).
    let (access_key, secret_key) = match state
        .iam
        .web_key_for(&session.username, session.is_builtin)
        .await
    {
        Ok(pair) => pair,
        Err(err) => return storage_error(err),
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
    audit(&state, &rid.0, &session.username, "presign", format!("/{}/{} (expires_secs={expires})", req.bucket, req.key));
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
