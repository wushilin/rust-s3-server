use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_types::byte_stream::Length;
use futures::stream::{self, StreamExt};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

use crate::config::client_for_alias;
use crate::urls::parse_s3_url;

const COPY_SOURCE_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'/')
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

const MAX_SINGLE_COPY: u64 = 5 * 1024 * 1024 * 1024; // AWS CopyObject ceiling

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

pub(crate) async fn upload_file(
    source: &Path,
    target: &str,
    part_size: u64,
    parallel: usize,
    disable_multipart: bool,
    storage_class: Option<&str>,
) -> Result<()> {
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
    let metadata = fs::metadata(source)
        .await
        .with_context(|| format!("stat {}", source.display()))?;
    let (client, _) = client_for_alias(&parsed.alias).await?;
    if disable_multipart || metadata.len() <= part_size {
        let mut req = client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from_path(source).await?);
        if let Some(sc) = storage_class {
            req = req.storage_class(aws_sdk_s3::types::StorageClass::from(sc));
        }
        req.send().await?;
    } else {
        multipart_upload(
            &client,
            source,
            &bucket,
            &key,
            metadata.len(),
            part_size,
            parallel.max(1),
            storage_class,
        )
        .await?;
    }
    println!("Uploaded `{}` to `{}/{}`.", source.display(), bucket, key);
    Ok(())
}

pub(crate) async fn multipart_upload(
    client: &Client,
    source: &Path,
    bucket: &str,
    key: &str,
    total_size: u64,
    part_size: u64,
    parallel: usize,
    storage_class: Option<&str>,
) -> Result<()> {
    if part_size < 5 * 1024 * 1024 {
        return Err(anyhow!("multipart part size must be at least 5MiB"));
    }
    let mut create = client.create_multipart_upload().bucket(bucket).key(key);
    if let Some(sc) = storage_class {
        create = create.storage_class(aws_sdk_s3::types::StorageClass::from(sc));
    }
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
) -> Result<()> {
    if same_endpoint(source_alias, target_alias) {
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
}

pub(crate) async fn download_key_to_path(
    client: &Client,
    bucket: &str,
    key: &str,
    output: &Path,
    part_size: u64,
    parallel: usize,
) -> Result<()> {
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
            Ok(())
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
    download_key_to_path(&client, &bucket, &key, &output, part_size, parallel).await?;
    println!("Downloaded `{source}` to `{}`.", output.display());
    Ok(())
}
