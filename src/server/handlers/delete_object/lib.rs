//! `DELETE /{bucket}/{key}` — delete an object (idempotent).

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::server::policy::Requirement;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, _body: Body) -> Response {
    let resource = ctx.resource();
    if force_delete_enabled(&ctx) {
        return force_delete_prefix(store, ctx).await;
    }
    match store.delete_object_with_size(&ctx.bucket, &ctx.key).await {
        Ok(size) => srv::with_measure(
            srv::empty_response(StatusCode::NO_CONTENT),
            srv::OperationMeasure::Bytes(size.unwrap_or(0)),
        ),
        Err(err) => srv::storage_error_response(err, &resource),
    }
}

fn force_delete_enabled(ctx: &ObjectCtx) -> bool {
    let query_enabled = ctx
        .query
        .get("forceDelete")
        .map(|v| v.is_empty() || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let header_enabled = ctx
        .headers
        .get("x-minio-force-delete")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.is_empty() || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    query_enabled || header_enabled
}

async fn force_delete_prefix(store: LocalObjectStore, ctx: ObjectCtx) -> Response {
    let prefix = ctx.key.trim_start_matches('/');
    let resource = ctx.resource();
    let mut keys = Vec::new();
    let mut after = None;
    loop {
        let page = match store
            .list_objects(&ctx.bucket, prefix, None, after.as_deref(), 1000)
            .await
        {
            Ok(page) => page,
            Err(err) => return srv::storage_error_response(err, &resource),
        };
        for entry in &page.entries {
            keys.push(entry.object_key.clone());
        }
        if !page.is_truncated {
            break;
        }
        after = page.next_after;
    }

    if let Some(identity) = &ctx.identity {
        if keys.iter().any(|key| {
            !identity.authorize(&[Requirement::object(
                "s3:DeleteObject",
                &ctx.bucket,
                key,
            )])
        }) {
            return srv::s3_error(
                StatusCode::FORBIDDEN,
                "AccessDenied",
                "Access Denied by IAM policy",
                &resource,
            );
        }
    }

    let mut deleted_bytes = 0;
    for key in keys {
        match store.delete_object_with_size(&ctx.bucket, &key).await {
            Ok(size) => deleted_bytes += size.unwrap_or(0),
            Err(err) => return srv::storage_error_response(err, &resource),
        }
    }

    srv::with_measure(
        srv::empty_response(StatusCode::NO_CONTENT),
        srv::OperationMeasure::Bytes(deleted_bytes),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(bucket: &str, key: &str) -> ObjectCtx {
        ObjectCtx {
            request_id: "test".into(),
            identity: None,
            bucket: bucket.into(),
            key: key.into(),
            query: std::collections::HashMap::new(),
            headers: axum::http::HeaderMap::new(),
            method: axum::http::Method::DELETE,
        }
    }

    #[tokio::test]
    async fn delete_is_idempotent_no_content() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bucket").await.unwrap();
        store.put_object("bucket", "k", b"x", None, None, false).await.unwrap();

        let resp = handle(store.clone(), ctx("bucket", "k"), Body::empty()).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(store.read_object("bucket", "k").await.is_err());

        // Deleting an absent key still succeeds.
        let resp = handle(store, ctx("bucket", "k"), Body::empty()).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }
}
