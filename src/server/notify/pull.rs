//! The replication pull API, served on the S3 port beside (not through) the
//! S3 router: it authenticates with a replication token only, never with
//! SigV4, so it cannot reach any S3 handler.
//!
//! - `GET  /_rs3/changelog[?wait=<secs>]` — the next batch (JSON) with an
//!   `ack` value; `wait` long-polls up to 60s when there is nothing new.
//! - `POST /_rs3/changelog/ack` body `{"ack": "<value>"}` — acknowledge it.
//!
//! Both take `Authorization: Bearer rs3cl_…`. A paused subscription answers
//! `410 Gone` with the reason. Bucket names cannot start with `_`, so the
//! prefix never shadows a bucket.

use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use super::{NotifyError, NotifyService};

pub const PATH_PREFIX: &str = "/_rs3/changelog";
const MAX_WAIT_SECS: u64 = 60;

pub fn router(service: NotifyService) -> Router {
    Router::new()
        .route(PATH_PREFIX, get(pull))
        .route(&format!("{PATH_PREFIX}/ack"), post(ack))
        .with_state(service)
}

#[derive(Debug, Deserialize)]
struct PullQuery {
    #[serde(default)]
    wait: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct AckBody {
    ack: String,
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

pub fn error_response(err: NotifyError) -> Response {
    let status = match &err {
        NotifyError::NotFound(_) => StatusCode::NOT_FOUND,
        NotifyError::BadRequest(_) => StatusCode::BAD_REQUEST,
        NotifyError::Conflict(_) => StatusCode::CONFLICT,
        NotifyError::Gone(_) => StatusCode::GONE,
        NotifyError::Unauthorized => StatusCode::UNAUTHORIZED,
        NotifyError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(serde_json::json!({ "error": err.to_string() }))).into_response()
}

async fn pull(State(service): State<NotifyService>, headers: HeaderMap, Query(query): Query<PullQuery>) -> Response {
    let Some(token) = bearer(&headers) else {
        return error_response(NotifyError::Unauthorized);
    };
    let wait = Duration::from_secs(query.wait.unwrap_or(0).min(MAX_WAIT_SECS));
    match service.pull(token, wait).await {
        Ok(batch) => Json(batch).into_response(),
        Err(err) => error_response(err),
    }
}

async fn ack(State(service): State<NotifyService>, headers: HeaderMap, Json(body): Json<AckBody>) -> Response {
    let Some(token) = bearer(&headers) else {
        return error_response(NotifyError::Unauthorized);
    };
    match service.ack(token, &body.ack).await {
        Ok(view) => Json(serde_json::json!({
            "offset": view.offset,
            "phase": view.phase,
            "head": view.head,
            "lag": view.lag,
        }))
        .into_response(),
        Err(err) => error_response(err),
    }
}
