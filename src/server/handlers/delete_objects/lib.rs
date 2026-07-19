//! `POST /{bucket}?delete` — bulk delete. Each key is authorized individually
//! against the caller's identity after the request body is parsed.

use axum::body::{to_bytes, Body};
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::BucketCtx;
use crate::server::policy::Requirement;
use crate::server::xml::{delete_objects_xml, DeleteObjectResult};
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, body: Body) -> Response {
    let resource = ctx.resource();
    let raw = match to_bytes(body, 1024 * 1024).await {
        Ok(b) => b,
        Err(err) => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                err.to_string(),
                &resource,
            )
        }
    };
    let (keys, quiet) = srv::parse_delete_objects_xml(&String::from_utf8_lossy(&raw));
    if keys.is_empty() {
        return srv::s3_error(
            StatusCode::BAD_REQUEST,
            "MalformedXML",
            "No Object keys found in Delete request",
            &resource,
        );
    }

    // Authorize each key. No identity (auth disabled) means allow.
    let allowed = keys
        .iter()
        .map(|key| {
            ctx.identity
                .as_ref()
                .map(|identity| {
                    identity.authorize(&[Requirement::object("s3:DeleteObject", &ctx.bucket, key)])
                })
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    if !allowed.iter().any(|allowed| *allowed) {
        return srv::s3_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "Access Denied by IAM policy",
            &resource,
        );
    }

    let requested_count = keys.len();
    let mut results = Vec::with_capacity(keys.len());
    for (key, allowed) in keys.into_iter().zip(allowed.iter().copied()) {
        if !allowed {
            log::warn!(
                "[{}] s3 batch delete denied user={} bucket={} key={}",
                ctx.request_id,
                ctx.identity
                    .as_ref()
                    .and_then(|identity| identity.username())
                    .unwrap_or("?"),
                ctx.bucket,
                key,
            );
            results.push(DeleteObjectResult {
                key,
                error: Some((
                    "AccessDenied".to_string(),
                    "Access Denied by IAM policy".to_string(),
                )),
            });
            continue;
        }
        let err = store.delete_object(&ctx.bucket, &key).await.err();
        results.push(DeleteObjectResult {
            key,
            error: err.map(|e| ("InternalError".to_string(), e.to_string())),
        });
    }

    let mut response = srv::with_measure(
        srv::xml_response(StatusCode::OK, delete_objects_xml(&results, quiet)),
        srv::OperationMeasure::Objects(requested_count),
    );
    if allowed.iter().any(|allowed| !*allowed) {
        response
            .extensions_mut()
            .insert(srv::OperationDisposition::Partial);
    }
    response
}
