//! `POST /{bucket}/{key}?uploads` — initiate a multipart upload.

use axum::body::Body;
use axum::http::{header, StatusCode};
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::server::xml::initiate_multipart_xml;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, _body: Body) -> Response {
    let resource = ctx.resource();
    let content_type = ctx
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let content_encoding = srv::object_content_encoding(&ctx.headers);
    let content_language = ctx
        .headers
        .get(header::CONTENT_LANGUAGE)
        .and_then(|v| v.to_str().ok());
    let storage_class = srv::storage_class_header(&ctx.headers);
    if let Some(resp) = srv::reject_invalid_storage_class(storage_class, &resource) {
        return resp;
    }
    let user_meta = match srv::extract_user_meta(&ctx.headers) {
        Ok(meta) => meta,
        Err(message) => {
            return srv::s3_error(StatusCode::BAD_REQUEST, "InvalidArgument", message, &resource)
        }
    };
    match store
        .initiate_multipart_with_metadata(
            &ctx.bucket,
            &ctx.key,
            content_type,
            content_encoding.as_deref(),
            storage_class,
            content_language,
            &user_meta,
        )
        .await
    {
        Ok(upload_id) => srv::xml_response(
            StatusCode::OK,
            initiate_multipart_xml(&ctx.bucket, &ctx.key, &upload_id),
        ),
        Err(err) => srv::storage_error_response(err, &resource),
    }
}
