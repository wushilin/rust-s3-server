//! `GET /{bucket}?versions` — list object versions: every version of every
//! key, newest first per key, with delete markers. On a bucket that never
//! had versioning each key has exactly one `null` version: the live object.

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
    if encoding_type.is_some_and(|v| v != "url") {
        return srv::s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "Invalid Encoding Method specified in Request",
            &ctx.resource(),
        );
    }
    let key_marker = ctx.query.get("key-marker").map(String::as_str).unwrap_or("");
    let version_id_marker = ctx.query.get("version-id-marker").map(String::as_str).unwrap_or("");
    let delimiter = ctx.query.get("delimiter").map(String::as_str).filter(|d| !d.is_empty());
    // A version-id marker names a version of the key marker: it needs one,
    // and must be `null` or a version id.
    if !version_id_marker.is_empty() {
        if key_marker.is_empty() {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "A version-id marker cannot be specified without a key marker.",
                &ctx.resource(),
            );
        }
        if version_id_marker != "null" && crate::storage::layouts::v3::parse_stamp_dir(version_id_marker).is_none() {
            return srv::s3_error(StatusCode::BAD_REQUEST, "InvalidArgument", "Invalid version id specified", &ctx.resource());
        }
    }
    // `max-keys` is capped at S3's 1000 ceiling; anything unparseable falls back
    // to the default.
    let max_keys = ctx
        .query
        .get("max-keys")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000)
        .min(1000);
    // The store resolves ordering, the markers and truncation: only this
    // page's key range is read.
    match store
        .list_object_versions(&ctx.bucket, prefix, delimiter, key_marker, version_id_marker, max_keys)
        .await
    {
        Ok(page) => {
            let count = page.entries.len();
            srv::with_measure(
                srv::xml_response(
                    StatusCode::OK,
                    list_object_versions_xml(
                        &ctx.bucket,
                        prefix,
                        delimiter,
                        encoding_type,
                        &page.entries,
                        &page.common_prefixes,
                        max_keys,
                        key_marker,
                        version_id_marker,
                        page.is_truncated,
                        page.next_key_marker.as_deref(),
                        page.next_version_id_marker.as_deref(),
                    ),
                ),
                srv::OperationMeasure::Objects(count),
            )
        }
        Err(err) => srv::storage_error_response(err, &ctx.resource()),
    }
}
