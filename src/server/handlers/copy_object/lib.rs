//! `PUT /{bucket}/{key}` with `x-amz-copy-source` — server-side object copy.

use axum::body::Body;
use axum::http::{header, StatusCode};
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::server::xml::copy_object_xml;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, _body: Body) -> Response {
    let resource = ctx.resource();
    let copy_source = ctx
        .headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let (src_bucket, src_key) = match srv::parse_copy_source(&copy_source) {
        Some(v) => v,
        None => {
            return srv::s3_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid x-amz-copy-source",
                &resource,
            )
        }
    };
    let storage_class = srv::storage_class_header(&ctx.headers);
    if let Some(resp) = srv::reject_invalid_storage_class(storage_class, &resource) {
        return resp;
    }
    let replace_metadata = ctx
        .headers
        .get("x-amz-metadata-directive")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("REPLACE"))
        .unwrap_or(false);
    let user_meta = if replace_metadata {
        match srv::extract_user_meta(&ctx.headers) {
            Ok(meta) => Some(meta),
            Err(message) => {
                return srv::s3_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    message,
                    &resource,
                )
            }
        }
    } else {
        None
    };
    let replacement_content_type = if replace_metadata {
        ctx.headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok())
    } else {
        None
    };
    let replacement_content_language = if replace_metadata {
        ctx.headers
            .get(header::CONTENT_LANGUAGE)
            .and_then(|v| v.to_str().ok())
    } else {
        None
    };
    // `?versionId=` on the source copies that version (current or not).
    let src_version = srv::parse_copy_source_version(&copy_source);
    let src_read = match &src_version {
        Some(version) => store.read_object_version(&src_bucket, &src_key, version).await,
        None => store.read_object(&src_bucket, &src_key).await,
    };
    let src_object = match src_read {
        Ok(object) => object,
        Err(err) => return srv::storage_error_response(err, &format!("/{src_bucket}/{src_key}")),
    };
    if !srv::copy_source_preconditions_match(
        &ctx.headers,
        &src_object.meta.etag,
        src_object.meta.last_modified_ms,
    ) {
        return srv::s3_error(
            StatusCode::PRECONDITION_FAILED,
            "PreconditionFailed",
            "At least one of the preconditions you specified did not hold",
            &format!("/{src_bucket}/{src_key}"),
        );
    }
    drop(src_object);
    // `x-amz-tagging-directive: REPLACE` takes the tags from the request
    // (none when the header is absent); COPY, the default, keeps the
    // source's.
    let replace_tags = ctx
        .headers
        .get("x-amz-tagging-directive")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("REPLACE"));
    let tags = match srv::extract_tags(&ctx.headers) {
        Ok(tags) if replace_tags => Some(tags.unwrap_or_default()),
        Ok(_) => None,
        Err(message) => return srv::s3_error(StatusCode::BAD_REQUEST, "InvalidTag", message, &resource),
    };
    let options = match srv::write_options(&store, &ctx, &resource).await {
        Ok(options) => options.with_tags(tags),
        Err(response) => return response,
    };
    match store
        .copy_object_version(
            &src_bucket,
            &src_key,
            src_version.as_deref(),
            &ctx.bucket,
            &ctx.key,
            storage_class,
            user_meta.as_ref(),
            replacement_content_type,
            replacement_content_language,
            options,
        )
        .await
    {
        Ok(result) => srv::with_measure(
            srv::with_version_id(
                srv::xml_response(
                    StatusCode::OK,
                    copy_object_xml(&result.etag, result.last_modified_ms),
                ),
                result.version_id.as_deref(),
            ),
            srv::OperationMeasure::Bytes(result.size),
        ),
        Err(err) => srv::storage_error_response(err, &resource),
    }
}
