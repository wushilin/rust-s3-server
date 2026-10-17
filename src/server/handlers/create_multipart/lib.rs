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
    // `x-rs3-replicate: true` starts a replicated upload (the stamp the
    // object keeps comes with the completion), which a read-only bucket
    // accepts; a client's create is refused there before any part is sent.
    let replicated = match srv::replicated_upload(ctx.identity.as_ref(), &ctx.headers, &ctx.bucket, &ctx.key, &resource) {
        Ok(replicated) => replicated,
        Err(response) => return response,
    };
    let tags = match srv::extract_tags(&ctx.headers) {
        Ok(tags) => tags.unwrap_or_default(),
        Err(message) => return srv::s3_error(StatusCode::BAD_REQUEST, "InvalidTag", message, &resource),
    };
    match store
        .initiate_multipart_as(
            &ctx.bucket,
            &ctx.key,
            content_type,
            content_encoding.as_deref(),
            storage_class,
            content_language,
            &user_meta,
            &tags,
            replicated,
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
