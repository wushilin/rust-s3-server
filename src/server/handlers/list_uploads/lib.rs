//! `GET /{bucket}?uploads` — list in-progress multipart uploads.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::BucketCtx;
use crate::server::xml::list_multipart_uploads_xml;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: BucketCtx, _body: Body) -> Response {
    let prefix = ctx.query.get("prefix").map(String::as_str).unwrap_or("");
    let key_marker = ctx.query.get("key-marker").map(String::as_str).unwrap_or("");
    let upload_id_marker = ctx
        .query
        .get("upload-id-marker")
        .map(String::as_str)
        .unwrap_or("");
    let max_uploads = ctx
        .query
        .get("max-uploads")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000)
        .min(1000);
    match store.list_multipart_uploads(&ctx.bucket).await {
        Ok(mut uploads) => {
            // Order by (key, uploadId) and resume strictly after the
            // (key-marker, upload-id-marker) pair, honouring `prefix`.
            uploads.sort_by(|a, b| {
                a.object_key
                    .cmp(&b.object_key)
                    .then(a.upload_id.cmp(&b.upload_id))
            });
            let after_marker = |u: &crate::storage::metadata::UploadMeta| {
                if key_marker.is_empty() {
                    return true;
                }
                match u.object_key.as_str().cmp(key_marker) {
                    std::cmp::Ordering::Greater => true,
                    std::cmp::Ordering::Equal => u.upload_id.as_str() > upload_id_marker,
                    std::cmp::Ordering::Less => false,
                }
            };
            let mut page: Vec<_> = uploads
                .into_iter()
                .filter(|u| prefix.is_empty() || u.object_key.starts_with(prefix))
                .filter(after_marker)
                .collect();
            let is_truncated = page.len() > max_uploads;
            page.truncate(max_uploads);
            let (next_key_marker, next_upload_id_marker) = if is_truncated {
                page.last()
                    .map(|u| (Some(u.object_key.clone()), Some(u.upload_id.clone())))
                    .unwrap_or((None, None))
            } else {
                (None, None)
            };
            let count = page.len();
            srv::with_measure(
                srv::xml_response(
                    StatusCode::OK,
                    list_multipart_uploads_xml(
                        &ctx.bucket,
                        &page,
                        prefix,
                        max_uploads,
                        key_marker,
                        upload_id_marker,
                        is_truncated,
                        next_key_marker.as_deref(),
                        next_upload_id_marker.as_deref(),
                    ),
                ),
                srv::OperationMeasure::Uploads(count),
            )
        }
        Err(err) => srv::storage_error_response(err, &ctx.resource()),
    }
}
