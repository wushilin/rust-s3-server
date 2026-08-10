//! `GET /{bucket}/{key}?attributes` — GetObjectAttributes.
//!
//! Reports an object's metadata *without* its body: ETag, size, storage class,
//! and — the reason this verb exists — the exact part layout of a multipart
//! object.
//!
//! That last one is not available any other way. `ListParts` only answers for
//! an upload still in progress (it needs an `uploadId`), so once
//! `CompleteMultipartUpload` runs the part boundaries become invisible even
//! though the ETag goes on depending on them: a multipart ETag is
//! `md5(md5(part₁) ‖ md5(part₂) ‖ …)-N`, which no client can recompute from
//! the object alone — `-N` and `Content-Length` together do not determine the
//! split. Handing back the boundaries is what makes an end-to-end ETag check
//! possible for a downloader that wants to prove what it received.
//!
//! Both inputs to that check are already on disk. `ObjectMeta.parts` carries
//! each part's `size` and its `etag`, and a part's ETag *is* its MD5 — the
//! same digests `multipart_etag` concatenates to form the composite. So this
//! handler adds no I/O beyond the metadata read every object request already
//! does.
//!
//! Divergences from AWS, all in the direction of "we don't have it" rather
//! than "we made something up":
//!
//! - **Top-level `Checksum` is never returned.** AWS reports additional
//!   whole-object checksums (CRC32/CRC32C/SHA1/SHA256/…) only for objects
//!   uploaded with one, and this server stores none, so the element is omitted
//!   exactly as AWS omits it for an object without them. `Checksum` is still
//!   accepted as a requested attribute name — asking for something the object
//!   does not have is not an error.
//! - **The `Part` list is always returned for a multipart object**, where AWS
//!   returns it only when the upload used a *composite* checksum. This was
//!   measured, not assumed: a plain multipart upload to real S3 (which now
//!   defaults to a `FULL_OBJECT` CRC64NVME checksum) comes back as bare
//!   `<ObjectParts><PartsCount>2</PartsCount></ObjectParts>` — no sizes, no
//!   boundaries — while the same upload with `--checksum-type COMPOSITE`
//!   returns every part with its `PartNumber`, `Size` and checksum.
//!
//!   Following that restriction here would defeat the operation's only
//!   purpose. This server has no notion of full-object checksums: *every*
//!   multipart object it stores has a composite ETag built from per-part MD5s,
//!   which is structurally the same situation as AWS's COMPOSITE case, and the
//!   per-part data is already in `ObjectMeta`. Withholding it would leave
//!   callers with a `PartsCount` and no way to use it.
//! - **Per-part `ChecksumMD5` is always returned**, where AWS returns whichever
//!   algorithm the upload declared. See
//!   [`crate::server::xml::ObjectPartXml::checksum_md5`]: the value is exactly
//!   what the field claims to be, we always have it, and it lets a downloader
//!   verify each part as it lands rather than only checking the composite
//!   after the whole object is on disk.
//! - **`ObjectParts` is omitted for a single-part object**, matching AWS. An
//!   object that never went through a multipart upload has no part structure
//!   to report, and its ETag is a plain MD5 the client can verify unaided.
//!   Reporting `PartsCount: 1` for it would send a client down the composite
//!   path for an ETag that is not composite.
//! - **`VersionId`/`DeleteMarker` are absent** — this server is unversioned.
//!
//! One shape callers must tolerate: the `<ETag>` in this body is **unquoted**,
//! alone among every ETag S3 emits — the HTTP header is quoted, `ListObjects`
//! and `ListParts` quote theirs, and this one does not. That was confirmed
//! against real S3, not inferred. Since a comparison against a `HeadObject`
//! ETag must normalise one side regardless, the portable client rule is
//! unchanged: trim `"` before comparing.

use axum::body::Body;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::Response;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::server::xml::{object_attributes_xml, ObjectPartXml, ObjectPartsPage};
use crate::storage::metadata::ObjectStorageKind;
use crate::storage::store::LocalObjectStore;
use crate::storage::time::http_date_ms;

/// What the caller asked to see. Absent fields are omitted from the response
/// entirely — this operation is a projection, not a fixed record.
#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct Requested {
    pub etag: bool,
    pub object_parts: bool,
    pub storage_class: bool,
    pub object_size: bool,
}

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, _body: Body) -> Response {
    let resource = ctx.resource();

    // The SDK sends one `x-amz-object-attributes` header per requested value
    // rather than a single comma-joined one; other clients do the opposite.
    // Accept both: every value of the header, each split on commas.
    let raw: Vec<String> = ctx
        .headers
        .get_all("x-amz-object-attributes")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().trim_matches('"').to_string())
        .filter(|value| !value.is_empty())
        .collect();
    if raw.is_empty() {
        return srv::s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "The x-amz-object-attributes header specifying the attributes to \
             be retrieved is either missing or empty",
            &resource,
        );
    }
    let requested = match parse_attributes(&raw) {
        Ok(requested) => requested,
        Err(unknown) => {
            // AWS keeps the offending value out of the message and in
            // typed elements, so an SDK that models them can report it.
            return srv::s3_error_detailed(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "Invalid attribute name specified.",
                &resource,
                vec![
                    (
                        "ArgumentName".to_string(),
                        "x-amz-object-attributes".to_string(),
                    ),
                    ("ArgumentValue".to_string(), unknown),
                ],
            )
        }
    };

    // Pagination for `ObjectParts` arrives as headers, not query parameters —
    // this operation puts its inputs where `ListParts` puts them in the query.
    let part_number_marker = ctx
        .headers
        .get("x-amz-part-number-marker")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u16>().ok())
        .unwrap_or(0);
    let max_parts = ctx
        .headers
        .get("x-amz-max-parts")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(1000)
        .min(1000);

    let object = match store.read_object(&ctx.bucket, &ctx.key).await {
        Ok(object) => object,
        Err(err) => return srv::storage_error_response(err, &resource),
    };
    let meta = object.meta;
    let is_multipart = meta.storage == ObjectStorageKind::Multipart;

    let page: Vec<ObjectPartXml> = meta
        .parts
        .iter()
        .filter(|part| part.number > part_number_marker)
        .map(|part| ObjectPartXml {
            number: part.number,
            size: part.size,
            checksum_md5: md5_hex_to_base64(&part.etag),
        })
        .collect();
    // `max_parts == 0` can never advance the marker, so reporting truncation
    // would make a paginating client replay the same empty page forever.
    let is_truncated = max_parts > 0 && page.len() > max_parts;
    let page = &page[..page.len().min(max_parts)];
    // The last part number on this page, or the incoming marker when the page
    // is empty. Sent whether or not the page is truncated, as AWS does.
    let next_part_number_marker = page.last().map_or(part_number_marker, |part| part.number);

    let body = object_attributes_xml(
        requested.etag.then(|| meta.etag.trim_matches('"')),
        requested
            .storage_class
            .then_some(meta.storage_class.as_str()),
        requested.object_size.then_some(meta.size),
        (requested.object_parts && is_multipart).then_some(ObjectPartsPage {
            total_parts_count: meta.parts.len(),
            part_number_marker,
            next_part_number_marker,
            max_parts,
            is_truncated,
            parts: page,
        }),
    );

    let mut response = srv::with_measure(
        srv::xml_response(StatusCode::OK, body),
        srv::OperationMeasure::Parts(page.len()),
    );
    if let Ok(value) = HeaderValue::from_str(&http_date_ms(meta.last_modified_ms)) {
        response.headers_mut().insert(header::LAST_MODIFIED, value);
    }
    response
}

/// Re-encodes a stored hex MD5 as the base64 form `ChecksumMD5` is defined to
/// carry. Returns `None` for anything that is not a 32-character hex digest,
/// so a part whose ETag came from somewhere unexpected is reported without a
/// checksum rather than with a corrupted one.
fn md5_hex_to_base64(hex: &str) -> Option<String> {
    let hex = hex.trim().trim_matches('"');
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let digest: Vec<u8> = (0..16)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16))
        .collect::<Result<_, _>>()
        .ok()?;
    Some(BASE64_STANDARD.encode(digest))
}

/// Maps requested attribute names onto the projection, rejecting the first
/// name AWS does not define. Case-sensitive, as AWS is.
fn parse_attributes(names: &[String]) -> Result<Requested, String> {
    let mut requested = Requested::default();
    for name in names {
        match name.as_str() {
            "ETag" => requested.etag = true,
            "ObjectParts" => requested.object_parts = true,
            "StorageClass" => requested.storage_class = true,
            "ObjectSize" => requested.object_size = true,
            // Understood, but this server stores no whole-object checksum, so
            // it contributes nothing to the response.
            "Checksum" => {}
            other => return Err(other.to_string()),
        }
    }
    Ok(requested)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    #[test]
    fn parse_attributes_projects_each_known_name() {
        assert_eq!(
            parse_attributes(&names(&["ETag", "ObjectSize"])).unwrap(),
            Requested {
                etag: true,
                object_size: true,
                ..Requested::default()
            }
        );
        assert_eq!(
            parse_attributes(&names(&["ObjectParts", "StorageClass"])).unwrap(),
            Requested {
                object_parts: true,
                storage_class: true,
                ..Requested::default()
            }
        );
    }

    #[test]
    fn parse_attributes_accepts_checksum_but_projects_nothing() {
        // Understood, not an error -- we simply have no checksum to report.
        assert_eq!(
            parse_attributes(&names(&["Checksum"])).unwrap(),
            Requested::default()
        );
    }

    #[test]
    fn parse_attributes_rejects_unknown_and_miscased_names() {
        assert_eq!(
            parse_attributes(&names(&["ETag", "Bogus"])).unwrap_err(),
            "Bogus"
        );
        // AWS attribute names are case-sensitive; silently accepting `etag`
        // would let a typo look like a working request here and fail against
        // real S3.
        assert_eq!(parse_attributes(&names(&["etag"])).unwrap_err(), "etag");
    }

    #[test]
    fn md5_hex_to_base64_round_trips_a_real_digest() {
        // md5("") = d41d8cd98f00b204e9800998ecf8427e
        assert_eq!(
            md5_hex_to_base64("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            "1B2M2Y8AsgTpgAmY7PhCfg=="
        );
        // Quoted ETags are normalised, not rejected.
        assert_eq!(
            md5_hex_to_base64("\"d41d8cd98f00b204e9800998ecf8427e\"").unwrap(),
            "1B2M2Y8AsgTpgAmY7PhCfg=="
        );
    }

    #[test]
    fn md5_hex_to_base64_declines_anything_that_is_not_a_digest() {
        // A composite ETag is not a part digest, and neither is junk: better
        // to report no checksum than a wrong one.
        assert_eq!(
            md5_hex_to_base64("d41d8cd98f00b204e9800998ecf8427e-2"),
            None
        );
        assert_eq!(md5_hex_to_base64(""), None);
        assert_eq!(md5_hex_to_base64("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), None);
    }
}
