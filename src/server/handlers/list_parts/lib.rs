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
    let part_number_marker = ctx
        .query
        .get("part-number-marker")
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(0);
    let max_parts = ctx
        .query
        .get("max-parts")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000)
        .min(1000);
    match store.list_parts(&ctx.bucket, &ctx.key, &upload_id).await {
        Ok(parts) => {
            // `list_parts` returns parts sorted ascending; resume strictly after
            // `part-number-marker` and cap at `max-parts`.
            let mut page: Vec<_> = parts
                .into_iter()
                .filter(|p| p.number > part_number_marker)
                .collect();
            let is_truncated = page.len() > max_parts;
            page.truncate(max_parts);
            let next_part_number_marker = if is_truncated {
                page.last().map(|p| p.number)
            } else {
                None
            };
            let count = page.len();
            srv::with_measure(
                srv::xml_response(
                    StatusCode::OK,
                    list_parts_xml(
                        &ctx.bucket,
                        &ctx.key,
                        &upload_id,
                        &page,
                        max_parts,
                        part_number_marker,
                        is_truncated,
                        next_part_number_marker,
                    ),
                ),
                srv::OperationMeasure::Parts(count),
            )
        }
        Err(err) => srv::storage_error_response(err, &resource),
    }
}
