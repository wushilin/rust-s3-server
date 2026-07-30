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
    // The store resolves ordering, the `key-marker` cursor and truncation: only
    // this page's key range is read, and a key's versions are never split
    // across pages.
    match store
        .list_object_versions(&ctx.bucket, prefix, key_marker, max_keys)
        .await
    {
        Ok(page) => {
            let is_truncated = page.is_truncated;
            let next_key_marker = page.next_key_marker;
            let page = page.entries;
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
