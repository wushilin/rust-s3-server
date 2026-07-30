use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::create_multipart_upload::builders::CreateMultipartUploadFluentBuilder;
use aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_types::byte_stream::Length;
use aws_smithy_types::date_time::Format as DateTimeFormat;
use futures::stream::{self, StreamExt};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

use crate::config::client_for_alias;
use crate::urls::parse_s3_url;

/// A handful of PutObject/CreateMultipartUpload builder methods that
/// `apply_attrs` needs on both request types -- the aws-sdk-s3 generated
/// builders don't share a trait, so this one is hand-rolled just for the
/// system-header/`.metadata()` fields `--attr` can touch ([SEM] §2).
trait AttrTarget: Sized {
    fn attr_cache_control(self, v: String) -> Self;
    fn attr_content_type(self, v: String) -> Self;
    fn attr_content_encoding(self, v: String) -> Self;
    fn attr_content_disposition(self, v: String) -> Self;
    fn attr_content_language(self, v: String) -> Self;
    fn attr_expires(self, v: aws_smithy_types::DateTime) -> Self;
    fn attr_metadata(self, k: String, v: String) -> Self;
}

macro_rules! impl_attr_target {
    ($ty:ty) => {
        impl AttrTarget for $ty {
            fn attr_cache_control(self, v: String) -> Self {
                self.cache_control(v)
            }
            fn attr_content_type(self, v: String) -> Self {
                self.content_type(v)
            }
            fn attr_content_encoding(self, v: String) -> Self {
                self.content_encoding(v)
            }
            fn attr_content_disposition(self, v: String) -> Self {
                self.content_disposition(v)
            }
            fn attr_content_language(self, v: String) -> Self {
                self.content_language(v)
            }
            fn attr_expires(self, v: aws_smithy_types::DateTime) -> Self {
                self.expires(v)
            }
            fn attr_metadata(self, k: String, v: String) -> Self {
                self.metadata(k, v)
            }
        }
    };
}

impl_attr_target!(PutObjectFluentBuilder);
impl_attr_target!(CreateMultipartUploadFluentBuilder);

/// Routes a parsed `--attr` map onto a PutObject/CreateMultipartUpload
/// builder: keys that (case-insensitively) match a known S3 system header
/// set the corresponding builder field, everything else becomes user
/// metadata via `.metadata()` with a leading `X-Amz-Meta-` stripped (the SDK
/// re-adds the wire prefix) -- no auto-prefixing is applied to keys that
/// aren't already spelled that way ([SEM] §2).
fn apply_attrs<T: AttrTarget>(mut req: T, metadata: &BTreeMap<String, String>) -> Result<T> {
    for (k, v) in metadata {
        let lower = k.to_ascii_lowercase();
        req = match lower.as_str() {
            "cache-control" => req.attr_cache_control(v.clone()),
            "content-type" => req.attr_content_type(v.clone()),
            "content-encoding" => req.attr_content_encoding(v.clone()),
            "content-disposition" => req.attr_content_disposition(v.clone()),
            "content-language" => req.attr_content_language(v.clone()),
            "expires" => {
                let dt = aws_smithy_types::DateTime::from_str(v, DateTimeFormat::HttpDate)
                    .map_err(|err| anyhow!("invalid Expires value `{v}`: {err}"))?;
                req.attr_expires(dt)
            }
            _ if lower.starts_with("x-amz-meta-") => {
                req.attr_metadata(k["X-Amz-Meta-".len()..].to_string(), v.clone())
            }
            _ => req.attr_metadata(k.clone(), v.clone()),
        };
    }
    Ok(req)
}

const COPY_SOURCE_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'/')
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

const MAX_SINGLE_COPY: u64 = 5 * 1024 * 1024 * 1024; // AWS CopyObject ceiling

/// Result of a single [`upload_file`] call: enough for a caller to build its
/// own `CopyMessage`/`MirrorMessage` (this module intentionally prints
/// nothing itself -- message shape/type is the caller's call, since `cp`/
/// `put`/`get` want `CopyMessage` while `mirror` wants `MirrorMessage`).
pub(crate) struct UploadOutcome {
    /// Fully resolved `alias/bucket/key` the object now lives at.
    pub target: String,
    pub size: u64,
}

pub(crate) fn encode_copy_source(bucket: &str, key: &str) -> String {
    format!("{bucket}/{}", utf8_percent_encode(key, COPY_SOURCE_ENCODE))
}

pub(crate) fn same_endpoint(a: &crate::config::Alias, b: &crate::config::Alias) -> bool {
    a.url == b.url && a.access_key == b.access_key
}

#[derive(Debug)]
pub(crate) struct UploadedPart {
    part_number: i32,
    etag: Option<String>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn upload_file(
    source: &Path,
    target: &str,
    part_size: u64,
    parallel: usize,
    disable_multipart: bool,
    storage_class: Option<&str>,
    metadata: &BTreeMap<String, String>,
    if_not_exists: bool,
    preserve: bool,
) -> Result<UploadOutcome> {
    #[cfg(not(unix))]
    if preserve {
        return Err(anyhow!("--preserve is not supported on this platform"));
    }
    let parsed = parse_s3_url(target)?;
    let bucket = parsed
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in target `{target}`"))?;
    let source_name = source
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("invalid source filename"))?;
    let key = match parsed.key {
        Some(k) if k.ends_with('/') => format!("{k}{source_name}"),
        Some(k) if k.is_empty() => source_name.to_string(),
        Some(k) => k,
        None => source_name.to_string(),
    };
    let file_meta = fs::metadata(source)
        .await
        .with_context(|| format!("stat {}", source.display()))?;
    #[cfg_attr(not(unix), allow(unused_mut))]
    let mut metadata = metadata.clone();
    #[cfg(unix)]
    if preserve {
        metadata.insert(
            "Mc-Attrs".to_string(),
            crate::attr::encode_fs_attrs(&file_meta),
        );
    }
    let metadata = &metadata;
    let (client, _) = client_for_alias(&parsed.alias).await?;
    if disable_multipart || file_meta.len() <= part_size {
        let mut req = client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from_path(source).await?);
        if let Some(sc) = storage_class {
            req = req.storage_class(aws_sdk_s3::types::StorageClass::from(sc));
        }
        req = apply_attrs(req, metadata)?;
        if if_not_exists {
            req = req.if_none_match("*");
        }
        req.send().await?;
    } else {
        multipart_upload(
            &client,
            source,
            &bucket,
            &key,
            file_meta.len(),
            part_size,
            parallel.max(1),
            storage_class,
            metadata,
            if_not_exists,
        )
        .await?;
    }
    Ok(UploadOutcome {
        target: format!("{}/{bucket}/{key}", parsed.alias),
        size: file_meta.len(),
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn multipart_upload(
    client: &Client,
    source: &Path,
    bucket: &str,
    key: &str,
    total_size: u64,
    part_size: u64,
    parallel: usize,
    storage_class: Option<&str>,
    metadata: &BTreeMap<String, String>,
    if_not_exists: bool,
) -> Result<()> {
    if part_size < 5 * 1024 * 1024 {
        return Err(anyhow!("multipart part size must be at least 5MiB"));
    }
    let mut create = client.create_multipart_upload().bucket(bucket).key(key);
    if let Some(sc) = storage_class {
        create = create.storage_class(aws_sdk_s3::types::StorageClass::from(sc));
    }
    create = apply_attrs(create, metadata)?;
    let created = create.send().await?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| anyhow!("server did not return upload id"))?
        .to_string();
    let part_count = total_size.div_ceil(part_size);
    let part_numbers = 1..=part_count;
    let uploads = stream::iter(part_numbers.map(|part_index| {
        let client = client.clone();
        let source = source.to_path_buf();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.clone();
        async move {
            let offset = (part_index - 1) * part_size;
            let len = (total_size - offset).min(part_size);
            let body = ByteStream::read_from()
                .path(source)
                .offset(offset)
                .length(Length::Exact(len))
                .build()
                .await?;
            let part_number = part_index as i32;
            let resp = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(body)
                .send()
                .await?;
            Ok::<UploadedPart, anyhow::Error>(UploadedPart {
                part_number,
                etag: resp.e_tag().map(String::from),
            })
        }
    }))
    .buffer_unordered(parallel.max(1));
    let mut completed_uploads = uploads.collect::<Vec<_>>().await;
    if let Some(err) = completed_uploads.iter().find_map(|res| res.as_ref().err()) {
        let _ = client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        return Err(anyhow!("{err}"));
    }
    let mut uploaded_parts = completed_uploads.drain(..).collect::<Result<Vec<_>>>()?;
    uploaded_parts.sort_by_key(|part| part.part_number);
    let completed = uploaded_parts
        .into_iter()
        .map(|part| {
            CompletedPart::builder()
                .part_number(part.part_number)
                .set_e_tag(part.etag)
                .build()
        })
        .collect::<Vec<_>>();
    let mut complete = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        );
    if if_not_exists {
        complete = complete.if_none_match("*");
    }
    complete.send().await?;
    Ok(())
}

pub(crate) async fn multipart_copy_s3_to_s3(
    source_client: &Client,
    source_bucket: &str,
    source_key: &str,
    target_client: &Client,
    target_bucket: &str,
    target_key: &str,
    total_size: u64,
    part_size: u64,
    parallel: usize,
) -> Result<()> {
    if part_size < 5 * 1024 * 1024 {
        return Err(anyhow!("multipart part size must be at least 5MiB"));
    }
    let created = target_client
        .create_multipart_upload()
        .bucket(target_bucket)
        .key(target_key)
        .send()
        .await?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| anyhow!("server did not return upload id"))?
        .to_string();
    let part_count = total_size.div_ceil(part_size);
    let uploads = stream::iter((1..=part_count).map(|part_index| {
        let source_client = source_client.clone();
        let target_client = target_client.clone();
        let source_bucket = source_bucket.to_string();
        let source_key = source_key.to_string();
        let target_bucket = target_bucket.to_string();
        let target_key = target_key.to_string();
        let upload_id = upload_id.clone();
        async move {
            let start = (part_index - 1) * part_size;
            let end = (total_size - 1).min(start + part_size - 1);
            let range = format!("bytes={start}-{end}");
            let body = source_client
                .get_object()
                .bucket(source_bucket)
                .key(source_key)
                .range(range)
                .send()
                .await?
                .body;
            let part_number = part_index as i32;
            let resp = target_client
                .upload_part()
                .bucket(target_bucket)
                .key(target_key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(body)
                .send()
                .await?;
            Ok::<UploadedPart, anyhow::Error>(UploadedPart {
                part_number,
                etag: resp.e_tag().map(String::from),
            })
        }
    }))
    .buffer_unordered(parallel.max(1));
    let mut completed_uploads = uploads.collect::<Vec<_>>().await;
    if let Some(err) = completed_uploads.iter().find_map(|res| res.as_ref().err()) {
        let _ = target_client
            .abort_multipart_upload()
            .bucket(target_bucket)
            .key(target_key)
            .upload_id(&upload_id)
            .send()
            .await;
        return Err(anyhow!("{err}"));
    }
    let mut uploaded_parts = completed_uploads.drain(..).collect::<Result<Vec<_>>>()?;
    uploaded_parts.sort_by_key(|part| part.part_number);
    let completed = uploaded_parts
        .into_iter()
        .map(|part| {
            CompletedPart::builder()
                .part_number(part.part_number)
                .set_e_tag(part.etag)
                .build()
        })
        .collect::<Vec<_>>();
    target_client
        .complete_multipart_upload()
        .bucket(target_bucket)
        .key(target_key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn transfer_object_between_s3(
    source_client: &Client,
    source_alias: &crate::config::Alias,
    source_bucket: &str,
    source_key: &str,
    target_client: &Client,
    target_alias: &crate::config::Alias,
    target_bucket: &str,
    target_key: &str,
    size: u64,
    part_size: u64,
    disable_multipart: bool,
    parallel: usize,
    preserve: bool,
) -> Result<()> {
    if same_endpoint(source_alias, target_alias) {
        // Same-endpoint copies go through server-side CopyObject, whose
        // default `x-amz-metadata-directive: COPY` already carries the
        // source object's user metadata (including any `Mc-Attrs`) onto the
        // target -- nothing extra to do here for `--preserve` ([SEM] §2).
        let single_limit = part_size.min(MAX_SINGLE_COPY);
        if disable_multipart || size <= single_limit {
            target_client
                .copy_object()
                .bucket(target_bucket)
                .key(target_key)
                .copy_source(encode_copy_source(source_bucket, source_key))
                .send()
                .await?;
        } else {
            multipart_server_side_copy(
                target_client,
                source_bucket,
                source_key,
                target_bucket,
                target_key,
                size,
                part_size,
                parallel,
            )
            .await?;
        }
        return Ok(());
    }
    // The streaming cross-endpoint path below has no equivalent of
    // CopyObject's metadata-directive carry-over -- it would need source
    // metadata fetched and threaded through both the single-shot PUT and
    // the multipart create-upload call. Out of scope for now; hard-error
    // rather than silently drop `--preserve` (matches the existing
    // cross-endpoint `--attr` rejection in `main.rs`/`mirror.rs`).
    if preserve {
        return Err(anyhow!(
            "--preserve for cross-endpoint S3-to-S3 copies is not implemented yet"
        ));
    }
    if std::env::var("RS3_DEBUG_COPY").is_ok() {
        eprintln!("rs3: falling back to streaming copy");
    }
    if disable_multipart || size <= part_size {
        let resp = source_client
            .get_object()
            .bucket(source_bucket)
            .key(source_key)
            .send()
            .await?;
        target_client
            .put_object()
            .bucket(target_bucket)
            .key(target_key)
            .content_length(size as i64)
            .body(resp.body)
            .send()
            .await?;
    } else {
        multipart_copy_s3_to_s3(
            source_client,
            source_bucket,
            source_key,
            target_client,
            target_bucket,
            target_key,
            size,
            part_size,
            parallel,
        )
        .await?;
    }
    Ok(())
}

async fn multipart_server_side_copy(
    target_client: &Client,
    source_bucket: &str,
    source_key: &str,
    target_bucket: &str,
    target_key: &str,
    total_size: u64,
    part_size: u64,
    parallel: usize,
) -> Result<()> {
    if part_size < 5 * 1024 * 1024 {
        return Err(anyhow!("multipart part size must be at least 5MiB"));
    }
    // `upload_part_copy` has no equivalent of `copy_object`'s
    // `x-amz-metadata-directive: COPY` -- `create_multipart_upload` starts
    // a brand-new object with no metadata at all unless it's set
    // explicitly here, so a same-endpoint copy above the single-copy
    // threshold would otherwise silently drop `Content-Type` and every
    // user-metadata key (including `Mc-Attrs`) that the single-shot
    // `copy_object` path above carries over for free ([SEM] §2's
    // `getAllMetadata`). `target_client` is safe to use for this HEAD:
    // this function is only reached from the same-endpoint branch of
    // `transfer_object_between_s3`, so it's the same credentials/endpoint
    // as `source_client` would be.
    let head = target_client
        .head_object()
        .bucket(source_bucket)
        .key(source_key)
        .send()
        .await
        .map_err(|err| anyhow!("stat `{source_bucket}/{source_key}`: {err}"))?;
    let mut create = target_client
        .create_multipart_upload()
        .bucket(target_bucket)
        .key(target_key);
    if let Some(content_type) = head.content_type() {
        create = create.content_type(content_type.to_string());
    }
    if let Some(metadata) = head.metadata() {
        for (k, v) in metadata {
            create = create.metadata(k.clone(), v.clone());
        }
    }
    let created = create.send().await?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| anyhow!("server did not return upload id"))?
        .to_string();
    let copy_source = encode_copy_source(source_bucket, source_key);
    let part_count = total_size.div_ceil(part_size);
    let uploads = stream::iter((1..=part_count).map(|part_index| {
        let client = target_client.clone();
        let copy_source = copy_source.clone();
        let target_bucket = target_bucket.to_string();
        let target_key = target_key.to_string();
        let upload_id = upload_id.clone();
        async move {
            let start = (part_index - 1) * part_size;
            let end = (total_size - 1).min(start + part_size - 1);
            let part_number = part_index as i32;
            let resp = client
                .upload_part_copy()
                .bucket(target_bucket)
                .key(target_key)
                .upload_id(upload_id)
                .part_number(part_number)
                .copy_source(copy_source)
                .copy_source_range(format!("bytes={start}-{end}"))
                .send()
                .await?;
            Ok::<UploadedPart, anyhow::Error>(UploadedPart {
                part_number,
                etag: resp
                    .copy_part_result()
                    .and_then(|r| r.e_tag())
                    .map(String::from),
            })
        }
    }))
    .buffer_unordered(parallel.max(1));
    let mut results = uploads.collect::<Vec<_>>().await;
    if let Some(err) = results.iter().find_map(|r| r.as_ref().err()) {
        let _ = target_client
            .abort_multipart_upload()
            .bucket(target_bucket)
            .key(target_key)
            .upload_id(&upload_id)
            .send()
            .await;
        return Err(anyhow!("{err}"));
    }
    let mut parts = results.drain(..).collect::<Result<Vec<_>>>()?;
    parts.sort_by_key(|p| p.part_number);
    let completed = parts
        .into_iter()
        .map(|p| {
            CompletedPart::builder()
                .part_number(p.part_number)
                .set_e_tag(p.etag)
                .build()
        })
        .collect::<Vec<_>>();
    target_client
        .complete_multipart_upload()
        .bucket(target_bucket)
        .key(target_key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_source_encodes_specials_keeps_slashes() {
        assert_eq!(
            encode_copy_source("bkt", "dir/obj name+x.bin"),
            "bkt/dir/obj%20name%2Bx.bin"
        );
    }

    fn dummy_client(endpoint: &str) -> Client {
        let cfg = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "k", "s", None, None, "test",
            ))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build();
        Client::from_conf(cfg)
    }

    fn dummy_alias(url: &str) -> crate::config::Alias {
        crate::config::Alias {
            url: url.to_string(),
            access_key: "k".to_string(),
            secret_key: "s".to_string(),
            api: "s3v4".to_string(),
            path: "auto".to_string(),
            region: None,
        }
    }

    // [SEM] §2: same-endpoint S3-to-S3 copies preserve metadata for free via
    // server-side CopyObject's default COPY directive, but the streaming
    // cross-endpoint fallback has no equivalent carry-over, so `--preserve`
    // is hard-rejected there rather than silently dropped. This must be
    // checked before any network I/O -- both aliases here point at
    // unreachable endpoints, so a hang/timeout would mean the check ran too
    // late (or not at all).
    #[test]
    fn preserve_is_rejected_for_cross_endpoint_s3_to_s3_before_any_io() {
        let source_client = dummy_client("http://127.0.0.1:1");
        let source_alias = dummy_alias("http://127.0.0.1:1");
        let target_client = dummy_client("http://127.0.0.1:2");
        let target_alias = dummy_alias("http://127.0.0.1:2");
        let result = futures::executor::block_on(transfer_object_between_s3(
            &source_client,
            &source_alias,
            "bkt",
            "key",
            &target_client,
            &target_alias,
            "bkt",
            "key",
            1,
            5 * 1024 * 1024,
            false,
            1,
            true,
        ));
        assert!(
            result.is_err(),
            "cross-endpoint --preserve must be rejected"
        );
        assert!(
            result.unwrap_err().to_string().contains("not implemented"),
            "error should say the feature is unimplemented"
        );
    }
}

/// Downloads `bucket/key` to `output`, returning the transferred size so
/// callers can build their own `CopyMessage`/`MirrorMessage` (this module
/// intentionally prints nothing itself -- see [`UploadOutcome`]).
pub(crate) async fn download_key_to_path(
    client: &Client,
    bucket: &str,
    key: &str,
    output: &Path,
    part_size: u64,
    parallel: usize,
    preserve: bool,
) -> Result<u64> {
    #[cfg(not(unix))]
    if preserve {
        return Err(anyhow!("--preserve is not supported on this platform"));
    }
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|err| anyhow!("stat `{bucket}/{key}`: {err}"))?;
    let size = head.content_length().unwrap_or_default() as u64;
    if let Some(parent) = output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }
    let tmp = {
        let mut name = output.file_name().unwrap_or_default().to_os_string();
        name.push(".rs3.part");
        output.with_file_name(name)
    };
    let result = download_to_temp(client, bucket, key, &tmp, size, part_size, parallel).await;
    match result {
        Ok(()) => {
            fs::rename(&tmp, output).await?;
            #[cfg(unix)]
            if preserve {
                if let Some(encoded) = head
                    .metadata()
                    .and_then(|m| m.iter().find(|(k, _)| k.eq_ignore_ascii_case("mc-attrs")))
                    .map(|(_, v)| v)
                {
                    if let Err(err) = crate::attr::apply_fs_attrs(output, encoded) {
                        eprintln!(
                            "rs3: warning: could not preserve attributes for `{}`: {err:#}",
                            output.display()
                        );
                    }
                }
            }
            Ok(size)
        }
        Err(err) => {
            let _ = fs::remove_file(&tmp).await;
            Err(err)
        }
    }
}

async fn download_to_temp(
    client: &Client,
    bucket: &str,
    key: &str,
    tmp: &Path,
    size: u64,
    part_size: u64,
    parallel: usize,
) -> Result<()> {
    if size <= part_size {
        let resp = client.get_object().bucket(bucket).key(key).send().await?;
        let mut reader = resp.body.into_async_read();
        let file = fs::File::create(tmp).await?;
        let mut writer = BufWriter::new(file);
        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.flush().await?;
        return Ok(());
    }
    let file = fs::File::create(tmp).await?;
    file.set_len(size).await?;
    drop(file);
    let part_count = size.div_ceil(part_size);
    let downloads = stream::iter((0..part_count).map(|part_index| {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let tmp = tmp.to_path_buf();
        async move {
            let start = part_index * part_size;
            let end = (size - 1).min(start + part_size - 1);
            let resp = client
                .get_object()
                .bucket(bucket)
                .key(key)
                .range(format!("bytes={start}-{end}"))
                .send()
                .await?;
            let mut file = fs::OpenOptions::new().write(true).open(&tmp).await?;
            file.seek(SeekFrom::Start(start)).await?;
            let mut reader = resp.body.into_async_read();
            let copied = tokio::io::copy(&mut reader, &mut file).await?;
            let expected = end - start + 1;
            if copied != expected {
                return Err(anyhow!(
                    "short range read: got {copied} of {expected} bytes at offset {start}"
                ));
            }
            file.flush().await?;
            Ok::<(), anyhow::Error>(())
        }
    }))
    .buffer_unordered(parallel.max(1));
    let results = downloads.collect::<Vec<_>>().await;
    for result in results {
        result?;
    }
    Ok(())
}

pub(crate) async fn download_object(
    source: &str,
    target: Option<PathBuf>,
    part_size: u64,
    parallel: usize,
    session: &crate::messages::TransferSession,
    preserve: bool,
) -> Result<()> {
    let parsed = parse_s3_url(source)?;
    let bucket = parsed
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in source `{source}`"))?;
    let key = parsed
        .key
        .ok_or_else(|| anyhow!("object key is required in source `{source}`"))?;
    let (client, _) = client_for_alias(&parsed.alias).await?;
    let output = match target {
        Some(path) if path.is_dir() => path.join(key.rsplit('/').next().unwrap_or(&key)),
        Some(path) => path,
        None => PathBuf::from(key.rsplit('/').next().unwrap_or(&key)),
    };
    let size = download_key_to_path(
        &client, &bucket, &key, &output, part_size, parallel, preserve,
    )
    .await?;
    session.add_total(size);
    let (total_count, total_size) = session.totals();
    let msg = crate::messages::CopyMessage {
        source: source.to_string(),
        target: output.display().to_string(),
        size,
        total_count,
        total_size,
    };
    session.object_done(&msg, size);
    Ok(())
}
