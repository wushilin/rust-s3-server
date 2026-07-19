//! `GET /{bucket}` — list objects (ListObjectsV2 by `list-type=2` or v2-only
//! params, otherwise ListObjectsV1). Supports `prefix`, `delimiter`,
//! `max-keys`, and continuation via `continuation-token`/`start-after`/`marker`.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::BucketCtx;
use crate::server::xml::{list_objects_v1_xml, list_objects_v2_xml};
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, _body: Body) -> Response {
    let query = &ctx.query;
    let prefix = query.get("prefix").map(String::as_str).unwrap_or("");
    let delimiter = query
        .get("delimiter")
        .map(String::as_str)
        .filter(|v| !v.is_empty());
    let encoding_type = query.get("encoding-type").map(String::as_str);
    let max_keys = match query.get("max-keys") {
        Some(value) => match value.parse::<usize>() {
            Ok(value) => value.min(1000),
            Err(_) => {
                return srv::s3_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "Invalid max-keys",
                    &ctx.resource(),
                )
            }
        },
        None => 1000,
    };

    // ListObjectsV2 is identified by list-type=2 or the use of v2-only params;
    // ListObjectsV1 uses `marker`.
    let is_v2 = query.get("list-type").map(|v| v == "2").unwrap_or(false)
        || query.contains_key("continuation-token")
        || query.contains_key("start-after");

    let after = if is_v2 {
        query
            .get("continuation-token")
            .or_else(|| query.get("start-after"))
            .map(String::as_str)
    } else {
        query.get("marker").map(String::as_str)
    };

    match store
        .list_objects(&ctx.bucket, prefix, delimiter, after, max_keys)
        .await
    {
        Ok(page) => {
            let object_count = page.entries.len() + page.common_prefixes.len();
            let xml = if is_v2 {
                list_objects_v2_xml(
                    &ctx.bucket,
                    prefix,
                    delimiter,
                    query.get("continuation-token").map(String::as_str),
                    query.get("start-after").map(String::as_str),
                    encoding_type,
                    max_keys,
                    &page,
                )
            } else {
                list_objects_v1_xml(
                    &ctx.bucket,
                    prefix,
                    delimiter,
                    query.get("marker").map(String::as_str),
                    encoding_type,
                    max_keys,
                    &page,
                )
            };
            srv::with_measure(
                srv::xml_response(StatusCode::OK, xml),
                srv::OperationMeasure::Objects(object_count),
            )
        }
        Err(err) => srv::storage_error_response(err, &ctx.resource()),
    }
}
