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
    match store.list_object_versions(&ctx.bucket, prefix).await {
        Ok(versions) => {
            let count = versions.len();
            srv::with_measure(
                srv::xml_response(
                    StatusCode::OK,
                    list_object_versions_xml(&ctx.bucket, prefix, encoding_type, &versions),
                ),
                srv::OperationMeasure::Objects(count),
            )
        }
        Err(err) => srv::storage_error_response(err, &ctx.resource()),
    }
}
