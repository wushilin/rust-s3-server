//! The replication-token front door to the changelog, served on the S3 port
//! beside the S3 router (after the virtual-host rewrite, so it never shadows
//! an object key). It authenticates with a replication token only, never
//! SigV4, and can reach nothing but these reads.
//!
//! - `GET /_rs3/changelog?after=<seq>[&limit=N][&wait=secs][&log_id=…]`
//! - `GET /_rs3/changelog/snapshot[?after=<key>][&limit=N]`
//!
//! Both take `Authorization: Bearer rs3cl_…`. The token's subscription
//! supplies the bucket and the filter. Like the SigV4 reads
//! (`?x-rs3-changelog`), they are stateless: the reader keeps its position.

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use super::{read, NotifyError, NotifyService};

pub const PATH_PREFIX: &str = "/_rs3/changelog";

pub fn router(service: NotifyService) -> Router {
    Router::new()
        .route(PATH_PREFIX, get(changes))
        .route(&format!("{PATH_PREFIX}/snapshot"), get(snapshot))
        .with_state(service)
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
        NotifyError::Forbidden(_) => StatusCode::FORBIDDEN,
        NotifyError::Unauthorized => StatusCode::UNAUTHORIZED,
        NotifyError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(serde_json::json!({ "error": err.to_string() }))).into_response()
}

async fn changes(
    State(service): State<NotifyService>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let Some(token) = bearer(&headers) else {
        return error_response(NotifyError::Unauthorized);
    };
    let (bucket, filter) = match service.token_scope(token).await {
        Ok(scope) => scope,
        Err(err) => return error_response(err),
    };
    let mut params = match read::read_params(&query) {
        Ok(params) => params,
        Err(err) => return err.into_response(),
    };
    // The token's filter is the scope; a reader cannot widen it.
    params.filter = filter;
    match read::read(&service.inner.store, &bucket, &params).await {
        Ok(page) => {
            service.record_read(token, page.next).await;
            Json(page).into_response()
        }
        Err(err) => err.into_response(),
    }
}

async fn snapshot(
    State(service): State<NotifyService>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let Some(token) = bearer(&headers) else {
        return error_response(NotifyError::Unauthorized);
    };
    let (bucket, filter) = match service.token_scope(token).await {
        Ok(scope) => scope,
        Err(err) => return error_response(err),
    };
    let mut params = match read::snapshot_params(&query) {
        Ok(params) => params,
        Err(err) => return err.into_response(),
    };
    params.filter = filter;
    match read::snapshot(&service.inner.store, &bucket, &params).await {
        Ok(page) => Json(page).into_response(),
        Err(err) => err.into_response(),
    }
}
