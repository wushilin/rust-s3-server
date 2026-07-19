//! `GET /{bucket}/{key}?uploadId=…` — list the parts uploaded so far.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::server::xml::list_parts_xml;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, _body: Body) -> Response {
    let resource = ctx.resource();
    let upload_id = ctx.query.get("uploadId").cloned().unwrap_or_default();
    match store.list_parts(&ctx.bucket, &ctx.key, &upload_id).await {
        Ok(parts) => {
            let count = parts.len();
            srv::with_measure(
                srv::xml_response(
                    StatusCode::OK,
                    list_parts_xml(&ctx.bucket, &ctx.key, &upload_id, &parts),
                ),
                srv::OperationMeasure::Parts(count),
            )
        }
        Err(err) => srv::storage_error_response(err, &resource),
    }
}
