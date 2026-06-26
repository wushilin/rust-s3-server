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
    pub resource: String,
    pub request_id: String,
}

pub fn error_xml(error: &S3ErrorXml) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{}</Code><Message>{}</Message><Resource>{}</Resource><RequestId>{}</RequestId><HostId>{}</HostId></Error>"#,
        escape_xml(&error.code),
        escape_xml(&error.message),
        escape_xml(&error.resource),
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
    let next = page
        .next_after
        .as_ref()
        .filter(|_| page.is_truncated)
        .map(|v| {
            format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                escape_xml(&encode_list_value(v, encode_keys))
            )
        })
        .unwrap_or_default();
    let token = continuation_token
        .map(|v| {
            format!(
                "<ContinuationToken>{}</ContinuationToken>",
                escape_xml(&encode_list_value(v, encode_keys))
            )
        })
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

pub fn list_object_versions_xml(
    bucket: &str,
    prefix: &str,
    encoding_type: Option<&str>,
    versions: &[ObjectVersionEntry],
) -> String {
    let encode_keys = encoding_type == Some("url");
    let mut body = String::new();
    for version in versions {
        body.push_str(&format!(
            "<Version><Key>{}</Key><VersionId>{}</VersionId><IsLatest>{}</IsLatest><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>{}</StorageClass><Owner><ID>rust-s3-server</ID><DisplayName>rust-s3-server</DisplayName></Owner></Version>",
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
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{}</Name><Prefix>{}</Prefix>{encoding_xml}<KeyMarker></KeyMarker><VersionIdMarker></VersionIdMarker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{body}</ListVersionsResult>"#,
        escape_xml(bucket),
        escape_xml(&encode_list_value(prefix, encode_keys)),
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

pub fn list_parts_xml(bucket: &str, key: &str, upload_id: &str, parts: &[PartMeta]) -> String {
    let mut body = String::new();
    for part in parts {
        body.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag><Size>{}</Size></Part>",
            part.number,
            escape_xml(&quote_etag(&part.etag)),
            part.size,
        ));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId><StorageClass>STANDARD</StorageClass><IsTruncated>false</IsTruncated>{body}</ListPartsResult>"#,
        escape_xml(bucket),
        escape_xml(key),
        escape_xml(upload_id),
    )
}

pub fn list_multipart_uploads_xml(bucket: &str, uploads: &[UploadMeta]) -> String {
    let mut body = String::new();
    for upload in uploads {
        body.push_str(&format!(
            "<Upload><Key>{}</Key><UploadId>{}</UploadId><Initiator><ID>rust-s3-server</ID><DisplayName>rust-s3-server</DisplayName></Initiator><Owner><ID>rust-s3-server</ID><DisplayName>rust-s3-server</DisplayName></Owner><StorageClass>STANDARD</StorageClass><Initiated>{}</Initiated></Upload>",
            escape_xml(&upload.object_key),
            escape_xml(&upload.upload_id),
            iso_utc_ms(upload.initiated_at_ms),
        ));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{}</Bucket><KeyMarker></KeyMarker><UploadIdMarker></UploadIdMarker><MaxUploads>1000</MaxUploads><IsTruncated>false</IsTruncated>{body}</ListMultipartUploadsResult>"#,
        escape_xml(bucket),
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

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
    use crate::storage::index::ObjectIndexEntry;
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
                entries: vec![ObjectIndexEntry {
                    object_key: "a/file".to_string(),
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
                entries: vec![ObjectIndexEntry {
                    object_key: "a+b c".to_string(),
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
        assert!(xml.contains("<NextContinuationToken>a%2Bb%20c</NextContinuationToken>"));
        assert!(xml.contains("<ContinuationToken>a%2Bb%20</ContinuationToken>"));
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
        let xml = list_object_versions_xml("bucket", "my-prefix", None, &versions);
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
        let xml = list_multipart_uploads_xml("bucket", &uploads);
        assert!(xml.contains("<Initiator><ID>rust-s3-server</ID>"));
        assert!(xml.contains("<Owner><ID>rust-s3-server</ID>"));
    }
}
