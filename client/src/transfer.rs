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
use futures::TryStreamExt;
use futures::stream::{self, FuturesUnordered, StreamExt};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::config::client_for_alias;
use crate::progress::ui_eprintln;
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
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
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
    if let Some(ui) = progress {
        ui.add_object(file_meta.len());
    }
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
        let _permit = budget.acquire().await;
        let unit = match progress {
            Some(ui) => ui.start(crate::progress::ProgressAwareTask::bytes(
                crate::progress::TransferLabel {
                    verb: crate::progress::Verb::Uploading,
                    path: source.display().to_string(),
                    part: None,
                },
                file_meta.len(),
            )),
            None => crate::progress::ProgressNotifier::noop(),
        };
        let body = crate::progress::instrument_body(ByteStream::from_path(source).await?, &unit);
        let mut req = client.put_object().bucket(&bucket).key(&key).body(body);
        if let Some(sc) = storage_class {
            req = req.storage_class(aws_sdk_s3::types::StorageClass::from(sc));
        }
        req = apply_attrs(req, metadata)?;
        if if_not_exists {
            req = req.if_none_match("*");
        }
        req.send().await?;
        unit.finish();
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
            budget,
            progress,
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
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) -> Result<()> {
    if part_size < 5 * 1024 * 1024 {
        return Err(anyhow!("multipart part size must be at least 5MiB"));
    }
    let source_label = source.display().to_string();
    let mut create = client.create_multipart_upload().bucket(bucket).key(key);
    if let Some(sc) = storage_class {
        create = create.storage_class(aws_sdk_s3::types::StorageClass::from(sc));
    }
    create = apply_attrs(create, metadata)?;
    let created = crate::budget::dispatch(
        budget,
        progress,
        crate::progress::TransferLabel {
            verb: crate::progress::Verb::Creating,
            path: source_label.clone(),
            part: None,
        },
        "CreateMultipartUpload",
        create.send(),
    )
    .await?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| anyhow!("server did not return upload id"))?
        .to_string();
    let part_count = total_size.div_ceil(part_size);
    let part_numbers = 1..=part_count;
    let progress = progress.cloned();
    let budget = budget.clone();
    let uploads = stream::iter(part_numbers.map(|part_index| {
        let client = client.clone();
        let source = source.to_path_buf();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.clone();
        let progress = progress.clone();
        let budget = budget.clone();
        let source_label = source_label.clone();
        async move {
            let _permit = budget.acquire().await;
            let offset = (part_index - 1) * part_size;
            let len = (total_size - offset).min(part_size);
            let unit = match &progress {
                Some(ui) => ui.start(crate::progress::ProgressAwareTask::bytes(
                    crate::progress::TransferLabel {
                        verb: crate::progress::Verb::Uploading,
                        path: source_label,
                        part: Some((part_index, part_count)),
                    },
                    len,
                )),
                None => crate::progress::ProgressNotifier::noop(),
            };
            let body = crate::progress::instrument_body(
                ByteStream::read_from()
                    .path(source)
                    .offset(offset)
                    .length(Length::Exact(len))
                    .build()
                    .await?,
                &unit,
            );
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
            unit.finish();
            Ok::<UploadedPart, anyhow::Error>(UploadedPart {
                part_number,
                etag: resp.e_tag().map(String::from),
            })
        }
    }))
    .buffer_unordered(parallel.max(1));
    let mut completed_uploads = uploads.collect::<Vec<_>>().await;
    if let Some(err) = completed_uploads.iter().find_map(|res| res.as_ref().err()) {
        let _ = crate::budget::dispatch(
            &budget,
            progress.as_ref(),
            crate::progress::TransferLabel {
                verb: crate::progress::Verb::Aborting,
                path: source_label,
                part: None,
            },
            "AbortMultipartUpload",
            client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send(),
        )
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
    crate::budget::dispatch(
        &budget,
        progress.as_ref(),
        crate::progress::TransferLabel {
            verb: crate::progress::Verb::Completing,
            path: source_label,
            part: None,
        },
        "CompleteMultipartUpload",
        complete.send(),
    )
    .await?;
    Ok(())
}

/// Reads from `reader` until `buf` is completely filled or EOF, returning
/// the number of bytes actually read (`< buf.len()` iff EOF was reached
/// first). A plain single `AsyncReadExt::read` can return a short read well
/// before EOF (pipes, sockets), so this loops rather than trusting one call
/// to fill the buffer.
async fn fill_buffer<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

/// Streams `reader` (`pipe`'s stdin -- total size unknown up front, [SEM]
/// §8) to `bucket/key`, filling `part_size`-byte buffers as it goes. If EOF
/// arrives before the very first buffer is completely full, the whole
/// input was small enough for a single `PutObject`; otherwise it's a
/// streaming multipart upload, uploading parts as buffers fill and keeping
/// at most `concurrent` `UploadPart` calls in flight at once (mc's `pipe
/// --concurrent`). Returns the total byte count transferred, for the
/// caller's `pipeMessage`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn upload_stream<R>(
    client: &Client,
    reader: &mut R,
    bucket: &str,
    key: &str,
    part_size: u64,
    concurrent: usize,
    metadata: &BTreeMap<String, String>,
    storage_class: Option<&str>,
) -> Result<u64>
where
    R: tokio::io::AsyncRead + Unpin,
{
    if part_size < 5 * 1024 * 1024 {
        return Err(anyhow!("multipart part size must be at least 5MiB"));
    }
    let part_size = usize::try_from(part_size).unwrap_or(usize::MAX);
    let concurrent = concurrent.max(1);

    let mut first_buf = vec![0u8; part_size];
    let first_filled = fill_buffer(reader, &mut first_buf).await?;
    if first_filled < part_size {
        // EOF before the first buffer even filled up: the whole object
        // fits in memory already and its size is now known, so just do a
        // single-shot PutObject rather than paying for a multipart upload.
        first_buf.truncate(first_filled);
        let total = first_buf.len() as u64;
        let mut req = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(first_buf));
        if let Some(sc) = storage_class {
            req = req.storage_class(aws_sdk_s3::types::StorageClass::from(sc));
        }
        req = apply_attrs(req, metadata)?;
        req.send().await?;
        return Ok(total);
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

    let result = stream_parts(
        client, reader, bucket, key, &upload_id, part_size, concurrent, first_buf,
    )
    .await;
    let (total, mut uploaded_parts) = match result {
        Ok(v) => v,
        Err(err) => {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(err);
        }
    };

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
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await?;
    Ok(total)
}

/// Reads and uploads part 2 onward (part 1's buffer -- already known to be
/// completely full, since that's what routed the caller into the
/// multipart path -- is passed in as `first_body`), keeping at most
/// `concurrent` `UploadPart` calls in flight via [`FuturesUnordered`].
/// Reading is inherently sequential (there's only one `reader`), so this
/// interleaves it with uploading: once `concurrent` uploads are
/// outstanding, the next read waits for one to finish first, which also
/// bounds how many `part_size` buffers are held in memory at once. With
/// `concurrent == 1` this degenerates to plain sequential read-then-upload
/// without any special-cased branch, since a single already-outstanding
/// upload always blocks the next read.
// Every parameter is an independent input to one multipart stream; bundling
// them into a struct would only move the same list one level away.
#[allow(clippy::too_many_arguments)]
async fn stream_parts<R>(
    client: &Client,
    reader: &mut R,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_size: usize,
    concurrent: usize,
    first_body: Vec<u8>,
) -> Result<(u64, Vec<UploadedPart>)>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut total = first_body.len() as u64;
    let mut next_part_number = 2i32;
    let mut uploaded = Vec::new();
    let mut in_flight = FuturesUnordered::new();
    let mut eof = false;

    let spawn_upload = |part_number: i32, body: Vec<u8>| {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        async move {
            let resp = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(body))
                .send()
                .await?;
            Ok::<UploadedPart, anyhow::Error>(UploadedPart {
                part_number,
                etag: resp.e_tag().map(String::from),
            })
        }
    };

    in_flight.push(spawn_upload(1, first_body));

    loop {
        if !eof && in_flight.len() < concurrent {
            let mut buf = vec![0u8; part_size];
            let filled = fill_buffer(reader, &mut buf).await?;
            if filled == 0 {
                eof = true;
            } else {
                buf.truncate(filled);
                total += filled as u64;
                in_flight.push(spawn_upload(next_part_number, buf));
                next_part_number += 1;
            }
            continue;
        }
        match in_flight.next().await {
            Some(res) => uploaded.push(res?),
            None => break,
        }
    }

    Ok((total, uploaded))
}

#[allow(clippy::too_many_arguments)]
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
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) -> Result<()> {
    if part_size < 5 * 1024 * 1024 {
        return Err(anyhow!("multipart part size must be at least 5MiB"));
    }
    let source_key_label = source_key.to_string();
    let created = crate::budget::dispatch(
        budget,
        progress,
        crate::progress::TransferLabel {
            verb: crate::progress::Verb::Creating,
            path: source_key_label.clone(),
            part: None,
        },
        "CreateMultipartUpload",
        target_client
            .create_multipart_upload()
            .bucket(target_bucket)
            .key(target_key)
            .send(),
    )
    .await?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| anyhow!("server did not return upload id"))?
        .to_string();
    let part_count = total_size.div_ceil(part_size);
    let progress = progress.cloned();
    let budget = budget.clone();
    let uploads = stream::iter((1..=part_count).map(|part_index| {
        let source_client = source_client.clone();
        let target_client = target_client.clone();
        let source_bucket = source_bucket.to_string();
        let source_key = source_key.to_string();
        let target_bucket = target_bucket.to_string();
        let target_key = target_key.to_string();
        let upload_id = upload_id.clone();
        let progress = progress.clone();
        let budget = budget.clone();
        let source_key_label = source_key_label.clone();
        async move {
            let _permit = budget.acquire().await;
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
            let unit = match &progress {
                Some(ui) => ui.start(crate::progress::ProgressAwareTask::bytes(
                    crate::progress::TransferLabel {
                        verb: crate::progress::Verb::Copying,
                        path: source_key_label,
                        part: Some((part_index, part_count)),
                    },
                    end - start + 1,
                )),
                None => crate::progress::ProgressNotifier::noop(),
            };
            let body = crate::progress::instrument_body(body, &unit);
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
            unit.finish();
            Ok::<UploadedPart, anyhow::Error>(UploadedPart {
                part_number,
                etag: resp.e_tag().map(String::from),
            })
        }
    }))
    .buffer_unordered(parallel.max(1));
    let mut completed_uploads = uploads.collect::<Vec<_>>().await;
    if let Some(err) = completed_uploads.iter().find_map(|res| res.as_ref().err()) {
        let _ = crate::budget::dispatch(
            &budget,
            progress.as_ref(),
            crate::progress::TransferLabel {
                verb: crate::progress::Verb::Aborting,
                path: source_key_label,
                part: None,
            },
            "AbortMultipartUpload",
            target_client
                .abort_multipart_upload()
                .bucket(target_bucket)
                .key(target_key)
                .upload_id(&upload_id)
                .send(),
        )
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
    crate::budget::dispatch(
        &budget,
        progress.as_ref(),
        crate::progress::TransferLabel {
            verb: crate::progress::Verb::Completing,
            path: source_key_label,
            part: None,
        },
        "CompleteMultipartUpload",
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
            .send(),
    )
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
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) -> Result<()> {
    if let Some(ui) = progress {
        ui.add_object(size);
    }
    if same_endpoint(source_alias, target_alias) {
        // Same-endpoint copies go through server-side CopyObject, whose
        // default `x-amz-metadata-directive: COPY` already carries the
        // source object's user metadata (including any `Mc-Attrs`) onto the
        // target -- nothing extra to do here for `--preserve` ([SEM] §2).
        let single_limit = part_size.min(MAX_SINGLE_COPY);
        if disable_multipart || size <= single_limit {
            let _permit = budget.acquire().await;
            let unit = match progress {
                Some(ui) => ui.start(crate::progress::ProgressAwareTask::bytes(
                    crate::progress::TransferLabel {
                        verb: crate::progress::Verb::Copying,
                        path: source_key.to_string(),
                        part: None,
                    },
                    size,
                )),
                None => crate::progress::ProgressNotifier::noop(),
            };
            target_client
                .copy_object()
                .bucket(target_bucket)
                .key(target_key)
                .copy_source(encode_copy_source(source_bucket, source_key))
                .send()
                .await?;
            unit.finish();
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
                budget,
                progress,
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
        ui_eprintln!("rs3: falling back to streaming copy");
    }
    if disable_multipart || size <= part_size {
        // One pipeline (GET streamed straight into the PUT body) = one
        // permit, held across both calls -- not two acquisitions.
        let _permit = budget.acquire().await;
        let unit = match progress {
            Some(ui) => ui.start(crate::progress::ProgressAwareTask::bytes(
                crate::progress::TransferLabel {
                    verb: crate::progress::Verb::Copying,
                    path: source_key.to_string(),
                    part: None,
                },
                size,
            )),
            None => crate::progress::ProgressNotifier::noop(),
        };
        let resp = source_client
            .get_object()
            .bucket(source_bucket)
            .key(source_key)
            .send()
            .await?;
        let body = crate::progress::instrument_body(resp.body, &unit);
        target_client
            .put_object()
            .bucket(target_bucket)
            .key(target_key)
            .content_length(size as i64)
            .body(body)
            .send()
            .await?;
        unit.finish();
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
            budget,
            progress,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn multipart_server_side_copy(
    target_client: &Client,
    source_bucket: &str,
    source_key: &str,
    target_bucket: &str,
    target_key: &str,
    total_size: u64,
    part_size: u64,
    parallel: usize,
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) -> Result<()> {
    if part_size < 5 * 1024 * 1024 {
        return Err(anyhow!("multipart part size must be at least 5MiB"));
    }
    let source_key_label = source_key.to_string();
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
    let head = crate::budget::dispatch(
        budget,
        progress,
        crate::progress::TransferLabel {
            verb: crate::progress::Verb::Inspecting,
            path: source_key_label.clone(),
            part: None,
        },
        "HeadObject",
        target_client
            .head_object()
            .bucket(source_bucket)
            .key(source_key)
            .send(),
    )
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
    let created = crate::budget::dispatch(
        budget,
        progress,
        crate::progress::TransferLabel {
            verb: crate::progress::Verb::Creating,
            path: source_key_label.clone(),
            part: None,
        },
        "CreateMultipartUpload",
        create.send(),
    )
    .await?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| anyhow!("server did not return upload id"))?
        .to_string();
    let copy_source = encode_copy_source(source_bucket, source_key);
    let part_count = total_size.div_ceil(part_size);
    let progress = progress.cloned();
    let budget = budget.clone();
    let uploads = stream::iter((1..=part_count).map(|part_index| {
        let client = target_client.clone();
        let copy_source = copy_source.clone();
        let target_bucket = target_bucket.to_string();
        let target_key = target_key.to_string();
        let upload_id = upload_id.clone();
        let progress = progress.clone();
        let budget = budget.clone();
        let source_key_label = source_key_label.clone();
        async move {
            let _permit = budget.acquire().await;
            let start = (part_index - 1) * part_size;
            let end = (total_size - 1).min(start + part_size - 1);
            let unit = match &progress {
                Some(ui) => ui.start(crate::progress::ProgressAwareTask::bytes(
                    crate::progress::TransferLabel {
                        verb: crate::progress::Verb::Copying,
                        path: source_key_label,
                        part: Some((part_index, part_count)),
                    },
                    end - start + 1,
                )),
                None => crate::progress::ProgressNotifier::noop(),
            };
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
            unit.finish();
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
        let _ = crate::budget::dispatch(
            &budget,
            progress.as_ref(),
            crate::progress::TransferLabel {
                verb: crate::progress::Verb::Aborting,
                path: source_key_label,
                part: None,
            },
            "AbortMultipartUpload",
            target_client
                .abort_multipart_upload()
                .bucket(target_bucket)
                .key(target_key)
                .upload_id(&upload_id)
                .send(),
        )
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
    crate::budget::dispatch(
        &budget,
        progress.as_ref(),
        crate::progress::TransferLabel {
            verb: crate::progress::Verb::Completing,
            path: source_key_label,
            part: None,
        },
        "CompleteMultipartUpload",
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
            .send(),
    )
    .await?;
    Ok(())
}

/// Downloads `bucket/key` to `output`, returning the transferred size so
/// callers can build their own `CopyMessage`/`MirrorMessage` (this module
/// intentionally prints nothing itself -- see [`UploadOutcome`]).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn download_key_to_path(
    client: &Client,
    bucket: &str,
    key: &str,
    output: &Path,
    part_size: u64,
    parallel: usize,
    preserve: bool,
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) -> Result<u64> {
    #[cfg(not(unix))]
    if preserve {
        return Err(anyhow!("--preserve is not supported on this platform"));
    }
    let head = crate::budget::dispatch(
        budget,
        progress,
        crate::progress::TransferLabel {
            verb: crate::progress::Verb::Inspecting,
            path: format!("{bucket}/{key}"),
            part: None,
        },
        "HeadObject",
        client.head_object().bucket(bucket).key(key).send(),
    )
    .await
    .map_err(|err| anyhow!("stat `{bucket}/{key}`: {err}"))?;
    let size = head.content_length().unwrap_or_default() as u64;
    if let Some(ui) = progress {
        ui.add_object(size);
    }
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).await?;
    }
    let tmp = {
        let mut name = output.file_name().unwrap_or_default().to_os_string();
        name.push(".rs3.part");
        output.with_file_name(name)
    };
    let result = download_to_temp(
        client,
        bucket,
        key,
        &tmp,
        size,
        part_size,
        parallel,
        head.e_tag(),
        budget,
        progress,
    )
    .await;
    match result {
        Ok(()) => {
            fs::rename(&tmp, output).await?;
            #[cfg(unix)]
            if preserve
                && let Some(encoded) = head
                    .metadata()
                    .and_then(|m| m.iter().find(|(k, _)| k.eq_ignore_ascii_case("mc-attrs")))
                    .map(|(_, v)| v)
                && let Err(err) = crate::attr::apply_fs_attrs(output, encoded)
            {
                ui_eprintln!(
                    "rs3: warning: could not preserve attributes for `{}`: {err:#}",
                    output.display()
                );
            }
            Ok(size)
        }
        Err(err) => {
            let _ = fs::remove_file(&tmp).await;
            Err(err)
        }
    }
}

/// Consecutive attempts one range may make *without gaining a byte* before
/// the download gives up on it. Attempts that do gain ground reset this: a
/// link that is moving, however badly, is worth staying with.
const RANGE_MAX_STALLED_ATTEMPTS: u32 = 3;
/// Hard ceiling on attempts for a single range, so a connection that
/// delivers a trickle and then dies every time still terminates.
const RANGE_MAX_ATTEMPTS: u32 = 12;

/// One attempt at `bytes=from-end`, writing at offset `from` and adding
/// whatever it manages to `written` -- including on the error path, which is
/// what makes resumption possible.
#[allow(clippy::too_many_arguments)]
async fn stream_range_into(
    client: &Client,
    bucket: &str,
    key: &str,
    etag: Option<&str>,
    tmp: &Path,
    from: u64,
    end: u64,
    unit: &crate::progress::ProgressNotifier,
    written: &mut u64,
) -> Result<()> {
    let mut file = fs::OpenOptions::new().write(true).open(tmp).await?;
    file.seek(SeekFrom::Start(from)).await?;
    let mut req = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(format!("bytes={from}-{end}"));
    // Pins the object version for the life of the download. Without it, a
    // replacement between two ranged GETs -- of the same range across a
    // retry, or simply of two different parts fetched minutes apart -- would
    // splice two objects into one file, and the result would pass every
    // check we make on it.
    if let Some(etag) = etag {
        req = req.if_match(etag);
    }
    let resp = req.send().await?;
    let mut reader = resp.body.into_async_read();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = tokio::io::AsyncReadExt::read(&mut reader, &mut buf).await?;
        if n == 0 {
            break;
        }
        tokio::io::AsyncWriteExt::write_all(&mut file, &buf[..n]).await?;
        *written += n as u64;
        unit.advance(n as u64);
    }
    file.flush().await?;
    Ok(())
}

/// Streams `bytes=start-end` into `tmp` at offset `start`, resuming from
/// wherever it got to if the body breaks part-way.
///
/// The SDK's own retry stops at `send()`: a response body cannot be rewound,
/// so a connection that dies at 80% of a range is a hard error to it. Uploads
/// never had this problem -- a file-backed *request* body is retryable, so the
/// orchestrator re-streams it -- and this closes the same gap on the download
/// side.
///
/// Resumption is exact rather than a re-fetch. Every byte already written came
/// from this same range, in this same process, at a known offset, so the next
/// attempt asks only for `start + written ..= end`. Nothing is re-downloaded,
/// and the progress notifier is deliberately *not* rewound -- unlike an upload
/// retry, nothing it has already counted is about to be counted again.
///
/// This is in-session only. The `.rs3.part` file records nothing about which
/// ranges landed, and is deleted on failure, so a later run cannot trust it
/// and starts over.
#[allow(clippy::too_many_arguments)]
async fn download_range_resumable(
    client: &Client,
    bucket: &str,
    key: &str,
    etag: Option<&str>,
    tmp: &Path,
    start: u64,
    end: u64,
    unit: &crate::progress::ProgressNotifier,
) -> Result<()> {
    let expected = end - start + 1;
    let mut written = 0u64;
    let mut attempt = 1u32;
    let mut stalled = 0u32;
    loop {
        let before = written;
        let result = stream_range_into(
            client,
            bucket,
            key,
            etag,
            tmp,
            start + written,
            end,
            unit,
            &mut written,
        )
        .await;
        if result.is_ok() && written >= expected {
            return Ok(());
        }
        // A body that ended clean but short is as much a failure as an error,
        // and resumes identically -- the sparse file would otherwise keep a
        // hole that reads back as zeros.
        let reason = match result {
            Err(err) => format!("{err:#}"),
            Ok(()) => format!("body ended {} bytes short", expected - written),
        };
        if written > before {
            stalled = 0;
        } else {
            stalled += 1;
        }
        if stalled >= RANGE_MAX_STALLED_ATTEMPTS || attempt >= RANGE_MAX_ATTEMPTS {
            return Err(anyhow!(
                "`{bucket}/{key}` bytes {start}-{end}: gave up after {attempt} attempt(s) \
                 with {written} of {expected} bytes: {reason}"
            ));
        }
        ui_eprintln!(
            "rs3: `{bucket}/{key}` bytes {start}-{end}: resuming at {} after: {reason}",
            start + written
        );
        // The SDK's own backoff shape: 1s doubling to a 16s ceiling.
        tokio::time::sleep(std::time::Duration::from_secs(
            1u64 << (attempt - 1).min(4),
        ))
        .await;
        attempt += 1;
    }
}

#[allow(clippy::too_many_arguments)]
async fn download_to_temp(
    client: &Client,
    bucket: &str,
    key: &str,
    tmp: &Path,
    size: u64,
    part_size: u64,
    parallel: usize,
    etag: Option<&str>,
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) -> Result<()> {
    if size == 0 {
        // No range to ask for -- `bytes=0-` on an empty object is not a
        // request any of this can express.
        fs::File::create(tmp).await?;
        return Ok(());
    }
    let file = fs::File::create(tmp).await?;
    file.set_len(size).await?;
    drop(file);
    // One range when the object fits in a part, N when it doesn't -- the
    // single-range case is the same code path, just without a part label.
    let part_count = size.div_ceil(part_size);
    let progress = progress.cloned();
    let budget = budget.clone();
    let key_label = key.to_string();
    let etag = etag.map(str::to_string);
    let downloads = stream::iter((0..part_count).map(|part_index| {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let tmp = tmp.to_path_buf();
        let progress = progress.clone();
        let budget = budget.clone();
        let key_label = key_label.clone();
        let etag = etag.clone();
        async move {
            let _permit = budget.acquire().await;
            let start = part_index * part_size;
            let end = (size - 1).min(start + part_size - 1);
            let unit = match &progress {
                Some(ui) => ui.start(crate::progress::ProgressAwareTask::bytes(
                    crate::progress::TransferLabel {
                        verb: crate::progress::Verb::Downloading,
                        path: key_label,
                        part: (part_count > 1).then_some((part_index + 1, part_count)),
                    },
                    end - start + 1,
                )),
                None => crate::progress::ProgressNotifier::noop(),
            };
            download_range_resumable(
                &client,
                &bucket,
                &key,
                etag.as_deref(),
                &tmp,
                start,
                end,
                &unit,
            )
            .await?;
            unit.finish();
            Ok::<(), anyhow::Error>(())
        }
    }))
    .buffer_unordered(parallel.max(1));
    // Fail fast. `collect()` here would drain the whole stream first, so a
    // range that died early still cost the full bandwidth of every other
    // range before anyone was told -- and then the temp file was deleted
    // anyway. `try_collect` drops the stream on the first error, cancelling
    // whatever is still in flight.
    downloads.try_collect::<Vec<_>>().await?;
    Ok(())
}

pub(crate) async fn download_object(
    source: &str,
    target: Option<PathBuf>,
    part_size: u64,
    parallel: usize,
    budget: &crate::budget::StreamBudget,
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
        &client,
        &bucket,
        &key,
        &output,
        part_size,
        parallel,
        preserve,
        budget,
        session.ui(),
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
            extra: std::collections::BTreeMap::new(),
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
            &crate::budget::StreamBudget::new(1),
            None,
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
