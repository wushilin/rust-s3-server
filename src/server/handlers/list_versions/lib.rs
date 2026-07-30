//! `GET /{bucket}?versions` — list object versions (this server keeps only the
//! latest live version plus retired versions still in trash).

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::BucketCtx;
use crate::server::xml::list_object_versions_xml;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, _body: Body) -> Response {
    let prefix = ctx.query.get("prefix").map(String::as_str).unwrap_or("");
    let encoding_type = ctx.query.get("encoding-type").map(String::as_str);
    let key_marker = ctx.query.get("key-marker").map(String::as_str).unwrap_or("");
    // `max-keys` is capped at S3's 1000 ceiling; anything unparseable falls back
    // to the default.
    let max_keys = ctx
        .query
        .get("max-keys")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000)
        .min(1000);
    match store.list_object_versions(&ctx.bucket, prefix).await {
        Ok(mut versions) => {
            // Order by key, then newest-first within a key (S3's ordering), and
            // resume strictly after `key-marker`.
            versions.sort_by(|a, b| {
                a.meta
                    .object_key
                    .cmp(&b.meta.object_key)
                    .then(b.meta.last_modified_ms.cmp(&a.meta.last_modified_ms))
            });
            let mut page: Vec<_> = versions
                .into_iter()
                .filter(|v| key_marker.is_empty() || v.meta.object_key.as_str() > key_marker)
                .collect();
            // `max_keys == 0` can never advance the key marker, so reporting
            // truncation would make a paginating client replay forever.
            let is_truncated = max_keys > 0 && page.len() > max_keys;
            page.truncate(max_keys);
            let next_key_marker = if is_truncated {
                page.last().map(|v| v.meta.object_key.clone())
            } else {
                None
            };
            let count = page.len();
            srv::with_measure(
                srv::xml_response(
                    StatusCode::OK,
                    list_object_versions_xml(
                        &ctx.bucket,
                        prefix,
                        encoding_type,
                        &page,
                        max_keys,
                        key_marker,
                        is_truncated,
                        next_key_marker.as_deref(),
                    ),
                ),
                srv::OperationMeasure::Objects(count),
            )
        }
        Err(err) => srv::storage_error_response(err, &ctx.resource()),
    }
}
