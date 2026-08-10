use crate::storage::index::ListPage;
use crate::storage::metadata::{quote_etag, PartMeta, UploadMeta};
use crate::storage::store::ObjectVersionEntry;
use crate::storage::time::iso_utc_ms;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketListEntry {
    pub name: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ErrorXml {
    pub code: String,
    pub message: String,
    pub request_id: String,
    /// Extra elements this particular error carries, emitted between
    /// `<Message>` and `<RequestId>` in the order given.
    ///
    /// AWS attaches error-specific detail rather than folding everything into
    /// the message: `NoSuchKey` names the `<Key>`, `NoSuchBucket` the
    /// `<BucketName>`, `InvalidArgument` the offending
    /// `<ArgumentName>`/`<ArgumentValue>`, `InvalidPartNumber` the
    /// `<PartNumberRequested>`/`<ActualPartCount>`, `InvalidRange` the
    /// `<RangeRequested>`/`<ActualObjectSize>`, and `PreconditionFailed` the
    /// `<Condition>` that failed. Both humans and SDKs that model these fields
    /// read them, and folding them into prose leaves the modelled field
    /// `None`.
    pub details: Vec<(String, String)>,
}

/// Renders an S3 `<Error>` document.
///
/// Deliberately *without* a `<Resource>` element: real S3 does not emit one —
/// it names the offending thing with a typed element instead (see
/// [`S3ErrorXml::details`]) — and this was checked against
/// `s3.ap-southeast-1.amazonaws.com` rather than assumed. The resource is
/// still threaded through the error helpers for logging, just not put on the
/// wire.
pub fn error_xml(error: &S3ErrorXml) -> String {
    let details: String = error
        .details
        .iter()
        .map(|(name, value)| format!("<{name}>{}</{name}>", escape_xml(value)))
        .collect();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{}</Code><Message>{}</Message>{details}<RequestId>{}</RequestId><HostId>{}</HostId></Error>"#,
        escape_xml(&error.code),
        escape_xml(&error.message),
        escape_xml(&error.request_id),
        escape_xml(&error.request_id),
    )
}

pub fn list_buckets_xml(buckets: &[BucketListEntry]) -> String {
    let mut body = String::new();
    for bucket in buckets {
        body.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            escape_xml(&bucket.name),
            iso_utc_ms(bucket.created_at_ms),
        ));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>rust-s3-server</ID><DisplayName>rust-s3-server</DisplayName></Owner><Buckets>{body}</Buckets></ListAllMyBucketsResult>"#
    )
}

pub fn list_objects_v2_xml(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    continuation_token: Option<&str>,
    start_after: Option<&str>,
    encoding_type: Option<&str>,
    max_keys: usize,
    page: &ListPage,
) -> String {
    let encode_keys = encoding_type == Some("url");
    let mut contents = String::new();
    for entry in &page.entries {
        contents.push_str(&format!(
            // v1 carries `<Owner>` unconditionally; v2 omits it unless the
            // caller passes `fetch-owner=true`. AWS sends no `DisplayName`.
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            escape_xml(&encode_list_value(&entry.object_key, encode_keys)),
            iso_utc_ms(entry.last_modified_ms),
            escape_xml(&quote_etag(&entry.etag)),
            entry.size,
        ));
    }
    let mut prefixes = String::new();
    for p in &page.common_prefixes {
        prefixes.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape_xml(&encode_list_value(p, encode_keys))
        ));
    }
    // Continuation tokens are opaque: `encoding-type=url` covers Key, Prefix,
    // Delimiter and StartAfter, never the token. Clients hand the token back
    // verbatim (url-encoding it into the query string), so encoding it here
    // would return a double-encoded token that matches no key — the scan would
    // restart from the top and the client would list forever.
    let next = page
        .next_after
        .as_ref()
        .filter(|_| page.is_truncated)
        .map(|v| format!("<NextContinuationToken>{}</NextContinuationToken>", escape_xml(v)))
        .unwrap_or_default();
    let token = continuation_token
        .map(|v| format!("<ContinuationToken>{}</ContinuationToken>", escape_xml(v)))
        .unwrap_or_default();
    let start_after_xml = start_after
        .map(|v| {
            format!(
                "<StartAfter>{}</StartAfter>",
                escape_xml(&encode_list_value(v, encode_keys))
            )
        })
        .unwrap_or_default();
    let delimiter_xml = delimiter
        .map(|v| {
            format!(
                "<Delimiter>{}</Delimiter>",
                escape_xml(&encode_list_value(v, encode_keys))
            )
        })
        .unwrap_or_default();
    let encoding_xml = encoding_type
        .map(|v| format!("<EncodingType>{}</EncodingType>", escape_xml(v)))
        .unwrap_or_default();
    let key_count = page.entries.len() + page.common_prefixes.len();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{}</Name><Prefix>{}</Prefix>{token}{start_after_xml}{next}{encoding_xml}<KeyCount>{key_count}</KeyCount><MaxKeys>{max_keys}</MaxKeys>{delimiter_xml}<IsTruncated>{}</IsTruncated>{contents}{prefixes}</ListBucketResult>"#,
        escape_xml(bucket),
        escape_xml(&encode_list_value(prefix, encode_keys)),
        page.is_truncated,
    )
}

pub fn list_objects_v1_xml(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    marker: Option<&str>,
    encoding_type: Option<&str>,
    max_keys: usize,
    page: &ListPage,
) -> String {
    let encode_keys = encoding_type == Some("url");
    let mut contents = String::new();
    for entry in &page.entries {
        contents.push_str(&format!(
            // v1 carries `<Owner>` unconditionally; v2 omits it unless the
            // caller passes `fetch-owner=true`. AWS sends no `DisplayName`.
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><Owner><ID>rust-s3-server</ID></Owner><StorageClass>STANDARD</StorageClass></Contents>",
            escape_xml(&encode_list_value(&entry.object_key, encode_keys)),
            iso_utc_ms(entry.last_modified_ms),
            escape_xml(&quote_etag(&entry.etag)),
            entry.size,
        ));
    }
    let mut prefixes = String::new();
    for p in &page.common_prefixes {
        prefixes.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape_xml(&encode_list_value(p, encode_keys))
        ));
    }
    // Per S3 spec: NextMarker is only included when a delimiter is used and the response is
    // truncated.  Without a delimiter, clients use the last <Key> as the next marker.
    let next_marker = if delimiter.is_some() && page.is_truncated {
        page.next_after
            .as_ref()
            .map(|v| {
                format!(
                    "<NextMarker>{}</NextMarker>",
                    escape_xml(&encode_list_value(v, encode_keys))
                )
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    let marker_xml = marker
        .map(|v| {
            format!(
                "<Marker>{}</Marker>",
                escape_xml(&encode_list_value(v, encode_keys))
            )
        })
        .unwrap_or_else(|| "<Marker></Marker>".to_string());
    let delimiter_xml = delimiter
        .map(|v| {
            format!(
                "<Delimiter>{}</Delimiter>",
                escape_xml(&encode_list_value(v, encode_keys))
            )
        })
        .unwrap_or_default();
    let encoding_xml = encoding_type
        .map(|v| format!("<EncodingType>{}</EncodingType>", escape_xml(v)))
        .unwrap_or_default();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{}</Name><Prefix>{}</Prefix>{marker_xml}{next_marker}{encoding_xml}<MaxKeys>{max_keys}</MaxKeys>{delimiter_xml}<IsTruncated>{}</IsTruncated>{contents}{prefixes}</ListBucketResult>"#,
        escape_xml(bucket),
        escape_xml(&encode_list_value(prefix, encode_keys)),
        page.is_truncated,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn list_object_versions_xml(
    bucket: &str,
    prefix: &str,
    encoding_type: Option<&str>,
    versions: &[ObjectVersionEntry],
    max_keys: usize,
    key_marker: &str,
    is_truncated: bool,
    next_key_marker: Option<&str>,
) -> String {
    let encode_keys = encoding_type == Some("url");
    let mut body = String::new();
    for version in versions {
        body.push_str(&format!(
            "<Version><Key>{}</Key><VersionId>{}</VersionId><IsLatest>{}</IsLatest><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>{}</StorageClass><Owner><ID>rust-s3-server</ID></Owner></Version>",
            escape_xml(&encode_list_value(&version.meta.object_key, encode_keys)),
            // This server is non-versioned; S3 reports the version id as the literal
            // "null" for objects in unversioned buckets. The internal storage
            // directory name on `version.version_id` is never exposed to clients.
            "null",
            version.is_latest,
            iso_utc_ms(version.meta.last_modified_ms),
            escape_xml(&quote_etag(&version.meta.etag)),
            version.meta.size,
            escape_xml(&version.meta.storage_class),
        ));
    }
    let encoding_xml = encoding_type
        .map(|v| format!("<EncodingType>{}</EncodingType>", escape_xml(v)))
        .unwrap_or_default();
    let next_markers_xml = if is_truncated {
        format!(
            "<NextKeyMarker>{}</NextKeyMarker><NextVersionIdMarker>null</NextVersionIdMarker>",
            escape_xml(&encode_list_value(next_key_marker.unwrap_or(""), encode_keys)),
        )
    } else {
        String::new()
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{}</Name><Prefix>{}</Prefix>{encoding_xml}<KeyMarker>{}</KeyMarker><VersionIdMarker></VersionIdMarker><MaxKeys>{max_keys}</MaxKeys><IsTruncated>{is_truncated}</IsTruncated>{next_markers_xml}{body}</ListVersionsResult>"#,
        escape_xml(bucket),
        escape_xml(&encode_list_value(prefix, encode_keys)),
        escape_xml(&encode_list_value(key_marker, encode_keys)),
    )
}

pub fn initiate_multipart_xml(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>"#,
        escape_xml(bucket),
        escape_xml(key),
        escape_xml(upload_id),
    )
}

pub fn complete_multipart_xml(location: &str, bucket: &str, key: &str, etag: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>"#,
        escape_xml(location),
        escape_xml(bucket),
        escape_xml(key),
        escape_xml(&quote_etag(etag)),
    )
}

pub fn copy_object_xml(etag: &str, last_modified_ms: i64) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ETag>{}</ETag><LastModified>{}</LastModified></CopyObjectResult>"#,
        escape_xml(&quote_etag(etag)),
        iso_utc_ms(last_modified_ms),
    )
}

pub fn upload_part_copy_xml(etag: &str, last_modified_ms: i64) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LastModified>{}</LastModified><ETag>{}</ETag></CopyPartResult>"#,
        iso_utc_ms(last_modified_ms),
        escape_xml(&quote_etag(etag)),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn list_parts_xml(
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[PartMeta],
    max_parts: usize,
    part_number_marker: u16,
    is_truncated: bool,
    next_part_number_marker: Option<u16>,
) -> String {
    let mut body = String::new();
    for part in parts {
        // AWS reports LastModified per part; omitting it leaves an SDK's
        // `last_modified()` empty for every part.
        body.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size></Part>",
            part.number,
            iso_utc_ms(part.last_modified_ms),
            escape_xml(&quote_etag(&part.etag)),
            part.size,
        ));
    }
    // Sent whether or not the page is truncated -- AWS treats it as "the last
    // part number in this page", matching `GetObjectAttributes`.
    let next_marker = next_part_number_marker
        .or_else(|| parts.last().map(|p| p.number))
        .unwrap_or(part_number_marker);
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId>{OWNERSHIP}<StorageClass>STANDARD</StorageClass><PartNumberMarker>{part_number_marker}</PartNumberMarker><NextPartNumberMarker>{next_marker}</NextPartNumberMarker><MaxParts>{max_parts}</MaxParts><IsTruncated>{is_truncated}</IsTruncated>{body}</ListPartsResult>"#,
        escape_xml(bucket),
        escape_xml(key),
        escape_xml(upload_id),
        OWNERSHIP = ownership_xml(),
    )
}

/// The `<Initiator>`/`<Owner>` pair AWS puts in `ListParts`.
///
/// This server has no per-user object ownership, so both name the server
/// itself. They are emitted rather than omitted because an SDK models them,
/// and a missing `Owner` reads as "ownership unknown" rather than "ownership
/// is not a concept here". `Owner` carries no `DisplayName`: AWS omits it.
fn ownership_xml() -> String {
    "<Initiator><ID>rust-s3-server</ID><DisplayName>rust-s3-server</DisplayName></Initiator>\
     <Owner><ID>rust-s3-server</ID></Owner>"
        .replace("     ", "")
}

#[allow(clippy::too_many_arguments)]
pub fn list_multipart_uploads_xml(
    bucket: &str,
    uploads: &[UploadMeta],
    prefix: &str,
    max_uploads: usize,
    key_marker: &str,
    upload_id_marker: &str,
    is_truncated: bool,
    next_key_marker: Option<&str>,
    next_upload_id_marker: Option<&str>,
) -> String {
    let mut body = String::new();
    for upload in uploads {
        body.push_str(&format!(
            "<Upload><Key>{}</Key><UploadId>{}</UploadId>{}<StorageClass>STANDARD</StorageClass><Initiated>{}</Initiated></Upload>",
            escape_xml(&upload.object_key),
            escape_xml(&upload.upload_id),
            ownership_xml(),
            iso_utc_ms(upload.initiated_at_ms),
        ));
    }
    let prefix_xml = if prefix.is_empty() {
        String::new()
    } else {
        format!("<Prefix>{}</Prefix>", escape_xml(prefix))
    };
    // Sent on every page, truncated or not: AWS reports the last key and
    // upload id *in this page*, so a final page still carries them. Falling
    // back to the last listed upload keeps that true when the caller did not
    // supply an explicit continuation.
    let next_markers_xml = format!(
        "<NextKeyMarker>{}</NextKeyMarker><NextUploadIdMarker>{}</NextUploadIdMarker>",
        escape_xml(
            next_key_marker.unwrap_or(uploads.last().map_or("", |u| u.object_key.as_str()))
        ),
        escape_xml(
            next_upload_id_marker.unwrap_or(uploads.last().map_or("", |u| u.upload_id.as_str()))
        ),
    );
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{}</Bucket><KeyMarker>{}</KeyMarker><UploadIdMarker>{}</UploadIdMarker>{next_markers_xml}{prefix_xml}<MaxUploads>{max_uploads}</MaxUploads><IsTruncated>{is_truncated}</IsTruncated>{body}</ListMultipartUploadsResult>"#,
        escape_xml(bucket),
        escape_xml(key_marker),
        escape_xml(upload_id_marker),
    )
}

pub struct DeleteObjectResult {
    pub key: String,
    pub error: Option<(String, String)>, // (code, message)
}

pub fn delete_objects_xml(results: &[DeleteObjectResult], quiet: bool) -> String {
    let mut body = String::new();
    for r in results {
        match &r.error {
            None => {
                if !quiet {
                    body.push_str(&format!(
                        "<Deleted><Key>{}</Key></Deleted>",
                        escape_xml(&r.key)
                    ));
                }
            }
            Some((code, message)) => {
                body.push_str(&format!(
                    "<Error><Key>{}</Key><Code>{}</Code><Message>{}</Message></Error>",
                    escape_xml(&r.key),
                    escape_xml(code),
                    escape_xml(message),
                ));
            }
        }
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{body}</DeleteResult>"#
    )
}

/// One page of a multipart object's part layout, for
/// [`object_attributes_xml`]. Mirrors AWS's `GetObjectAttributesParts`.
pub struct ObjectPartsPage<'a> {
    /// Total parts in the object, not in this page.
    pub total_parts_count: usize,
    pub part_number_marker: u16,
    /// The last part number in this page. AWS sends this whether or not the
    /// page is truncated, so it is not a continuation-only field.
    pub next_part_number_marker: u16,
    pub max_parts: usize,
    pub is_truncated: bool,
    pub parts: &'a [ObjectPartXml],
}

/// One part as reported by GetObjectAttributes.
pub struct ObjectPartXml {
    pub number: u16,
    pub size: u64,
    /// Base64 of the part's raw 128-bit MD5 digest.
    ///
    /// AWS populates `ChecksumMD5` only when the upload declared an MD5
    /// checksum algorithm, and leaves it absent otherwise. This server always
    /// has the value -- a part's MD5 is what its stored ETag *is*, and the
    /// object's composite ETag is built from exactly these digests -- so it is
    /// always reported. The claim is the literal one the field makes ("the
    /// 128-bit MD5 digest of this part"), and it is what lets a downloader
    /// check each part on arrival instead of only checking the composite once
    /// the whole object has landed.
    pub checksum_md5: Option<String>,
}

/// `GetObjectAttributesResponse`. Every field is optional because the
/// operation is a projection: the caller names the attributes it wants via
/// `x-amz-object-attributes`, and anything it did not ask for is omitted
/// rather than sent empty.
///
/// The shape here was checked against real S3 rather than inferred, because
/// two details are surprising and neither is guessable from the API reference:
///
/// - **The root element is `GetObjectAttributesResponse`**, not the
///   `…Output` name the SDK uses for the modelled shape.
/// - **`<ETag>` is unquoted**, alone among every ETag S3 emits — the header is
///   quoted, `ListObjects` and `ListParts` quote theirs, and this one does
///   not. Callers must therefore trim `"` before comparing against a
///   `HeadObject` ETag, which is the portable rule regardless.
///
/// Observed from `s3.ap-southeast-1.amazonaws.com` on 2026-08-10:
///
/// ```text
/// <GetObjectAttributesResponse xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
///   <ETag>b30e35fa7fcee018c17d0b9152178c4d-2</ETag>
///   <ObjectParts><PartsCount>2</PartsCount><PartNumberMarker>0</PartNumberMarker>
///   <NextPartNumberMarker>2</NextPartNumberMarker><MaxParts>1000</MaxParts>
///   <IsTruncated>false</IsTruncated>
///   <Part><PartNumber>1</PartNumber><Size>5242880</Size><ChecksumSHA256>…</ChecksumSHA256></Part>
///   …</ObjectParts>
///   <StorageClass>STANDARD</StorageClass><ObjectSize>6291456</ObjectSize>
/// </GetObjectAttributesResponse>
/// ```
///
/// Note `NextPartNumberMarker` is present even though `IsTruncated` is false —
/// AWS emits it as "the last part number in this page", not as a
/// continuation-only field, so it is emitted unconditionally here too.
pub fn object_attributes_xml(
    etag: Option<&str>,
    storage_class: Option<&str>,
    object_size: Option<u64>,
    object_parts: Option<ObjectPartsPage<'_>>,
) -> String {
    let mut body = String::new();
    if let Some(etag) = etag {
        body.push_str(&format!("<ETag>{}</ETag>", escape_xml(etag)));
    }
    if let Some(page) = object_parts {
        // `TotalPartsCount` is carried on the wire as `<PartsCount>` -- the
        // element name and the modelled member name differ for this one field,
        // and an SDK will silently report `None` for the count if it is sent
        // under the member name instead.
        body.push_str(&format!(
            "<ObjectParts><PartsCount>{}</PartsCount><PartNumberMarker>{}</PartNumberMarker>",
            page.total_parts_count, page.part_number_marker
        ));
        body.push_str(&format!(
            "<NextPartNumberMarker>{}</NextPartNumberMarker>",
            page.next_part_number_marker
        ));
        body.push_str(&format!(
            "<MaxParts>{}</MaxParts><IsTruncated>{}</IsTruncated>",
            page.max_parts, page.is_truncated
        ));
        for part in page.parts {
            body.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><Size>{}</Size>",
                part.number, part.size
            ));
            if let Some(md5) = &part.checksum_md5 {
                body.push_str(&format!("<ChecksumMD5>{}</ChecksumMD5>", escape_xml(md5)));
            }
            body.push_str("</Part>");
        }
        body.push_str("</ObjectParts>");
    }
    if let Some(storage_class) = storage_class {
        body.push_str(&format!(
            "<StorageClass>{}</StorageClass>",
            escape_xml(storage_class)
        ));
    }
    if let Some(size) = object_size {
        body.push_str(&format!("<ObjectSize>{size}</ObjectSize>"));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><GetObjectAttributesResponse xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{body}</GetObjectAttributesResponse>"#
    )
}

pub(crate) fn escape_xml(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        // Characters illegal in XML 1.0 (control chars other than tab/LF/CR) are
        // not well-formed even when entity-escaped, and object keys may legally
        // contain them. Drop them so one bad key can't make the whole response
        // unparseable to strict SDK XML parsers.
        if ch.is_control() && ch != '\t' && ch != '\n' && ch != '\r' {
            continue;
        }
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

fn encode_list_value(value: &str, encode: bool) -> String {
    if encode {
        urlencoding::encode(value).into_owned()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::ObjectRecord;
    use crate::storage::metadata::ObjectMeta;

    #[test]
    fn list_v2_includes_s3_required_fields() {
        let xml = list_objects_v2_xml(
            "bucket",
            "a/",
            Some("/"),
            Some("a/1"),
            Some("a/0"),
            None,
            100,
            &ListPage {
                entries: vec![ObjectRecord {
                    object_key: "a/file".to_string(),
                    blob_dir: "objects/aa/bb/cc/dd/V1ABCDEF_0001".to_string(),
                    size: 3,
                    etag: "abc".to_string(),
                    last_modified_ms: 0,
                }],
                common_prefixes: vec!["a/b/".to_string()],
                is_truncated: true,
                next_after: Some("a/file".to_string()),
            },
        );
        assert!(xml.contains("<KeyCount>2</KeyCount>"));
        assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));
        assert!(xml.contains("<NextContinuationToken>a/file</NextContinuationToken>"));
        assert!(xml.contains("<ContinuationToken>a/1</ContinuationToken>"));
        assert!(xml.contains("<StartAfter>a/0</StartAfter>"));
        assert!(xml.contains("<ETag>&quot;abc&quot;</ETag>"));
    }

    #[test]
    fn list_v2_url_encoding_preserves_plus_and_space() {
        let xml = list_objects_v2_xml(
            "bucket",
            "a+b ",
            Some("+"),
            Some("a+b "),
            Some("z z"),
            Some("url"),
            100,
            &ListPage {
                entries: vec![ObjectRecord {
                    object_key: "a+b c".to_string(),
                    blob_dir: "objects/aa/bb/cc/dd/V1ABCDEF_0002".to_string(),
                    size: 3,
                    etag: "abc".to_string(),
                    last_modified_ms: 0,
                }],
                common_prefixes: vec!["a+b/".to_string()],
                is_truncated: true,
                next_after: Some("a+b c".to_string()),
            },
        );
        assert!(xml.contains("<EncodingType>url</EncodingType>"));
        assert!(xml.contains("<Prefix>a%2Bb%20</Prefix>"));
        assert!(xml.contains("<Delimiter>%2B</Delimiter>"));
        assert!(xml.contains("<Key>a%2Bb%20c</Key>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>a%2Bb%2F</Prefix></CommonPrefixes>"));
        // Continuation tokens are opaque and stay unencoded even under
        // encoding-type=url — clients replay them verbatim.
        assert!(xml.contains("<NextContinuationToken>a+b c</NextContinuationToken>"));
        assert!(xml.contains("<ContinuationToken>a+b </ContinuationToken>"));
        assert!(xml.contains("<StartAfter>z%20z</StartAfter>"));
    }

    #[test]
    fn list_object_versions_reports_null_version_id() {
        let meta = ObjectMeta {
            format_version: 1,
            bucket: "bucket".to_string(),
            object_key: "my-prefix/datafile".to_string(),
            storage: crate::storage::metadata::ObjectStorageKind::Single,
            size: 5,
            etag: "abc".to_string(),
            last_modified_ms: 0,
            content_type: "text/plain".to_string(),
            content_encoding: None,
            content_language: None,
            storage_class: "STANDARD".to_string(),
            user_meta: std::collections::BTreeMap::new(),
            parts: vec![],
        };
        let versions = vec![ObjectVersionEntry {
            meta,
            // Internal on-disk storage directory name; must never leak to clients.
            version_id: "V1ED6836".to_string(),
            is_latest: true,
        }];
        let xml =
            list_object_versions_xml("bucket", "my-prefix", None, &versions, 1000, "", false, None);
        assert!(xml.contains("<VersionId>null</VersionId>"));
        assert!(!xml.contains("V1ED6836"));
        assert!(xml.contains("<IsLatest>true</IsLatest>"));
    }

    #[test]
    fn list_multipart_uploads_includes_owner_and_initiator_ids() {
        let uploads = vec![UploadMeta {
            bucket: "bucket".to_string(),
            object_key: "key".to_string(),
            upload_id: "upload-id".to_string(),
            initiated_at_ms: 1,
            content_type: "application/octet-stream".to_string(),
            content_encoding: None,
            content_language: None,
            storage_class: "STANDARD".to_string(),
            user_meta: std::collections::BTreeMap::new(),
        }];
        let xml =
            list_multipart_uploads_xml("bucket", &uploads, "", 1000, "", "", false, None, None);
        assert!(xml.contains("<Initiator><ID>rust-s3-server</ID>"));
        assert!(xml.contains("<Owner><ID>rust-s3-server</ID>"));
    }
}
