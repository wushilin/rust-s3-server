//! The signed S3 client a replication link uses with its peer. Reads: the
//! changelog (`?x-rs3-changelog`), the snapshot listings, and object data
//! pinned to an exact version (`x-rs3-if-version`). Writes (a merge link's
//! push): PUT / multipart / metadata-only copy / DELETE, each carrying the
//! change's own stamp (`x-rs3-stamp`, needing `s3:ReplicateWrite`).
//! Requests are SigV4-signed with the link's access key, exactly as the peer
//! verifies them.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use reqwest::{Method, StatusCode};

use crate::server::notify::read::{ReadResponse, SnapshotResponse};
use crate::server::notify::model::WireEvent;
use crate::storage::stamp::{Rank, Stamp};

/// Request header naming the replication link that reads a changelog:
/// `<site>;<mode>;<link id>;<url-encoded link name>`.
pub const HEADER_REPLICATOR: &str = "x-rs3-replicator";
/// Response header: the answering site already has a merge link pairing
/// this bucket with the reader's site (`<link id>;<url-encoded name>`).
pub const HEADER_MERGE_BACK: &str = "x-rs3-merge-back";

/// Where a link reads from.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// `http(s)://host[:port]`, no path.
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    /// Sent as [`HEADER_REPLICATOR`] on changelog reads.
    pub replicator: Option<String>,
}

#[derive(Debug)]
pub enum PeerError {
    /// The position is older than the peer's retention (410).
    Gone(String),
    /// The position does not belong to the peer's log (409).
    LogMismatch(String),
    /// The object moved on since the event (412 on a version-pinned read):
    /// a later event carries its current state.
    Superseded,
    /// The object is gone (404).
    NotFound,
    /// A stamped write lost: the peer's state ranks at or above it (412).
    Stale,
    /// Anything else: retried later.
    Other(String),
}

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerError::Gone(m) | PeerError::LogMismatch(m) | PeerError::Other(m) => f.write_str(m),
            PeerError::Superseded => f.write_str("the object changed on the peer since the event"),
            PeerError::NotFound => f.write_str("the object is not on the peer"),
            PeerError::Stale => f.write_str("the peer has a newer state"),
        }
    }
}

/// Object metadata as the peer reports it on a GET / HEAD.
#[derive(Debug, Clone, Default)]
pub struct ObjectHeaders {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub storage_class: Option<String>,
    pub user_meta: BTreeMap<String, String>,
    pub etag: String,
    pub size: u64,
}

pub struct PeerClient {
    config: PeerConfig,
    host: String,
    http: reqwest::Client,
    /// The last changelog read's [`HEADER_MERGE_BACK`] answer.
    merge_back: std::sync::Mutex<Option<String>>,
}

/// AWS URI encoding of one path segment. `.` / `..` segments are escaped
/// so no URL parser normalises them away.
fn encode_segment(segment: &str) -> String {
    match segment {
        "." => "%2E".to_string(),
        ".." => "%2E%2E".to_string(),
        other => urlencoding::encode(other).into_owned(),
    }
}

fn object_path(bucket: &str, key: &str) -> String {
    let encoded: Vec<String> = key.split('/').map(encode_segment).collect();
    format!("/{}/{}", encode_segment(bucket), encoded.join("/"))
}

fn query_string(params: &[(&str, String)]) -> String {
    params
        .iter()
        .map(|(k, v)| {
            if v.is_empty() {
                format!("{}=", urlencoding::encode(k))
            } else {
                format!("{}={}", urlencoding::encode(k), urlencoding::encode(v))
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

impl PeerClient {
    pub fn new(config: PeerConfig) -> Result<Self, String> {
        let url = reqwest::Url::parse(&config.endpoint).map_err(|err| format!("invalid peer endpoint: {err}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err("peer endpoint must be http:// or https://".to_string());
        }
        let host = match (url.host_str(), url.port()) {
            (Some(host), Some(port)) => format!("{host}:{port}"),
            (Some(host), None) => host.to_string(),
            (None, _) => return Err("peer endpoint has no host".to_string()),
        };
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("rusts3-replication/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|err| err.to_string())?;
        Ok(Self { config, host, http, merge_back: std::sync::Mutex::new(None) })
    }

    fn signed(&self, method: Method, path: &str, query: &str, extra: &[(&str, &str)]) -> reqwest::RequestBuilder {
        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let (authorization, _) = crate::server::auth::compute_auth_header_payload(
            method.as_str(),
            path,
            query,
            &self.host,
            &self.config.access_key,
            &self.config.secret_key,
            &self.config.region,
            &datetime,
            "UNSIGNED-PAYLOAD",
            extra,
        );
        let url = match query.is_empty() {
            true => format!("{}{path}", self.config.endpoint.trim_end_matches('/')),
            false => format!("{}{path}?{query}", self.config.endpoint.trim_end_matches('/')),
        };
        let mut builder = self
            .http
            .request(method, url)
            .header("host", &self.host)
            .header("x-amz-date", datetime)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .header("authorization", authorization);
        for (name, value) in extra {
            builder = builder.header(*name, *value);
        }
        builder
    }

    /// Whether the peer reported (on the last changelog read) a merge link of
    /// its own pairing this bucket with us: `<link id>;<name>`.
    pub fn merge_back(&self) -> Option<String> {
        self.merge_back.lock().unwrap().clone()
    }

    async fn json<T: serde::de::DeserializeOwned>(&self, query: &str, timeout: Duration) -> Result<T, PeerError> {
        let path = format!("/{}", encode_segment(&self.config.bucket));
        let mut request = self.signed(Method::GET, &path, query, &[]).timeout(timeout);
        if let Some(replicator) = &self.config.replicator {
            request = request.header(HEADER_REPLICATOR, replicator);
        }
        let response = request
            .send()
            .await
            .map_err(|err| PeerError::Other(format!("peer unreachable: {err}")))?;
        *self.merge_back.lock().unwrap() = response
            .headers()
            .get(HEADER_MERGE_BACK)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let status = response.status();
        let body = response.bytes().await.map_err(|err| PeerError::Other(err.to_string()))?;
        let message = || {
            serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v["error"].as_str().map(str::to_string))
                .or_else(|| s3_error_text(&String::from_utf8_lossy(&body)))
                .unwrap_or_else(|| String::from_utf8_lossy(&body).chars().take(300).collect())
        };
        match status {
            StatusCode::OK => serde_json::from_slice(&body).map_err(|err| PeerError::Other(format!("bad peer response: {err}"))),
            StatusCode::GONE => Err(PeerError::Gone(message())),
            StatusCode::CONFLICT => Err(PeerError::LogMismatch(message())),
            other => Err(PeerError::Other(format!("peer answered {other}: {}", message()))),
        }
    }

    /// Changes after `after` (long-polls up to `wait`).
    pub async fn changes(
        &self,
        after: u64,
        limit: usize,
        wait: Duration,
        prefix: &str,
        log_id: Option<&str>,
    ) -> Result<ReadResponse, PeerError> {
        let mut params = vec![
            ("after", after.to_string()),
            ("limit", limit.to_string()),
            ("wait", wait.as_secs().to_string()),
            (crate::server::notify::read::QUERY_CHANGELOG, String::new()),
        ];
        if !prefix.is_empty() {
            params.push(("prefix", prefix.to_string()));
        }
        if let Some(log_id) = log_id {
            params.push(("log_id", log_id.to_string()));
        }
        self.json(&query_string(&params), wait + Duration::from_secs(30)).await
    }

    /// One page of the peer's objects (or tombstones with `deleted`).
    pub async fn snapshot(&self, after: Option<&str>, limit: usize, prefix: &str, deleted: bool) -> Result<SnapshotResponse, PeerError> {
        let mut params = vec![
            ("limit", limit.to_string()),
            (crate::server::notify::read::QUERY_SNAPSHOT, String::new()),
        ];
        if let Some(after) = after {
            params.push(("after", after.to_string()));
        }
        if !prefix.is_empty() {
            params.push(("prefix", prefix.to_string()));
        }
        if deleted {
            params.push(("deleted", "1".to_string()));
        }
        self.json(&query_string(&params), Duration::from_secs(60)).await
    }

    /// GET (optionally a byte range) of exactly `version` of `key`.
    async fn get(&self, key: &str, version: &str, range: Option<(u64, u64)>, method: Method) -> Result<reqwest::Response, PeerError> {
        let path = object_path(&self.config.bucket, key);
        let range_value = range.map(|(start, end)| format!("bytes={start}-{end}"));
        let mut extra: Vec<(&str, &str)> = vec![("x-rs3-if-version", version)];
        if let Some(range) = &range_value {
            extra.push(("range", range.as_str()));
        }
        let response = self
            .signed(method, &path, "", &extra)
            .send()
            .await
            .map_err(|err| PeerError::Other(format!("peer unreachable: {err}")))?;
        match response.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(response),
            StatusCode::PRECONDITION_FAILED => Err(PeerError::Superseded),
            StatusCode::NOT_FOUND => Err(PeerError::NotFound),
            other => Err(PeerError::Other(format!("peer answered {other} for {key}"))),
        }
    }

    fn headers_of(response: &reqwest::Response) -> ObjectHeaders {
        let h = response.headers();
        let text = |name: &str| h.get(name).and_then(|v| v.to_str().ok()).map(str::to_string);
        let mut user_meta = BTreeMap::new();
        for (name, value) in h {
            if let Some(meta) = name.as_str().strip_prefix("x-amz-meta-") {
                if let Ok(value) = value.to_str() {
                    user_meta.insert(meta.to_string(), value.to_string());
                }
            }
        }
        ObjectHeaders {
            content_type: text("content-type"),
            content_encoding: text("content-encoding"),
            content_language: text("content-language"),
            storage_class: text("x-amz-storage-class"),
            user_meta,
            etag: text("etag").unwrap_or_default().trim_matches('"').to_string(),
            size: text("content-length").and_then(|v| v.parse().ok()).unwrap_or(0),
        }
    }

    /// Metadata of exactly `version` of `key`.
    pub async fn head(&self, key: &str, version: &str) -> Result<ObjectHeaders, PeerError> {
        let response = self.get(key, version, None, Method::HEAD).await?;
        Ok(Self::headers_of(&response))
    }

    /// The bytes (all, or `range`) of exactly `version` of `key`.
    pub async fn body(
        &self,
        key: &str,
        version: &str,
        range: Option<(u64, u64)>,
    ) -> Result<(ObjectHeaders, impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin), PeerError> {
        let response = self.get(key, version, range, Method::GET).await?;
        let headers = Self::headers_of(&response);
        Ok((headers, response.bytes_stream()))
    }

    // ── writes (a merge link's push) ──────────────────────────────────────

    /// The peer's current state of `key`: its rank and, for a live object,
    /// its headers. `None` when the peer never saw the key (or its tombstone
    /// expired).
    pub async fn state(&self, key: &str) -> Result<Option<(Rank, Option<ObjectHeaders>)>, PeerError> {
        let path = object_path(&self.config.bucket, key);
        let response = self
            .signed(Method::HEAD, &path, "", &[])
            .send()
            .await
            .map_err(|err| PeerError::Other(format!("peer unreachable: {err}")))?;
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| Stamp::parse_header(v).ok())
        };
        match response.status() {
            StatusCode::OK => match header("x-rs3-stamp") {
                Some(stamp) => Ok(Some((Rank::live(stamp), Some(Self::headers_of(&response))))),
                None => Err(PeerError::Other(format!("the peer sent no x-rs3-stamp for {key}: not a rusts3 peer?"))),
            },
            StatusCode::NOT_FOUND => {
                let deleted = response.headers().contains_key("x-rs3-deleted");
                match (deleted, header("x-rs3-removed-stamp").or_else(|| header("x-rs3-stamp"))) {
                    (true, Some(removed)) => Ok(Some((Rank::deleted(removed), None))),
                    _ => Ok(None),
                }
            }
            other => Err(PeerError::Other(format!("peer answered {other} for HEAD {key}"))),
        }
    }

    /// `x-amz-*` headers carrying an object's metadata and the stamp (all
    /// signed, values whitespace-folded as the signature canonicalises them).
    fn write_headers(meta: &ObjectHeaders, stamp: &Stamp) -> Vec<(String, String)> {
        let mut headers = Self::meta_headers(meta);
        headers.push(("x-rs3-stamp".to_string(), stamp.to_header()));
        headers
    }

    fn meta_headers(meta: &ObjectHeaders) -> Vec<(String, String)> {
        let fold = |v: &str| v.split_whitespace().collect::<Vec<_>>().join(" ");
        let mut headers = Vec::new();
        for (name, value) in &meta.user_meta {
            headers.push((format!("x-amz-meta-{name}"), fold(value)));
        }
        if let Some(class) = meta.storage_class.as_deref().filter(|c| *c != "STANDARD") {
            headers.push(("x-amz-storage-class".to_string(), class.to_string()));
        }
        headers
    }

    fn with_content_headers(mut builder: reqwest::RequestBuilder, meta: &ObjectHeaders) -> reqwest::RequestBuilder {
        if let Some(v) = &meta.content_type {
            builder = builder.header("content-type", v);
        }
        if let Some(v) = &meta.content_encoding {
            builder = builder.header("content-encoding", v);
        }
        if let Some(v) = &meta.content_language {
            builder = builder.header("content-language", v);
        }
        builder
    }

    async fn send_write(&self, builder: reqwest::RequestBuilder, what: &str) -> Result<reqwest::Response, PeerError> {
        let response = builder
            .send()
            .await
            .map_err(|err| PeerError::Other(format!("peer unreachable: {err}")))?;
        match response.status() {
            status if status.is_success() => Ok(response),
            StatusCode::PRECONDITION_FAILED => Err(PeerError::Stale),
            status => {
                let body = response.text().await.unwrap_or_default();
                let message = s3_error_text(&body).unwrap_or_else(|| body.chars().take(300).collect());
                Err(PeerError::Other(format!("peer answered {status} for {what}: {message}")))
            }
        }
    }

    /// PUT `key` with `body` (`len` bytes), its metadata and `stamp`.
    pub async fn put(&self, key: &str, meta: &ObjectHeaders, stamp: &Stamp, len: u64, body: reqwest::Body) -> Result<(), PeerError> {
        let path = object_path(&self.config.bucket, key);
        let headers = Self::write_headers(meta, stamp);
        let extra: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let builder = Self::with_content_headers(self.signed(Method::PUT, &path, "", &extra), meta)
            .header("content-length", len)
            .body(body);
        self.send_write(builder, &format!("PUT {key}")).await.map(|_| ())
    }

    /// Replaces `key`'s metadata and stamp without moving data (a self-copy
    /// with `REPLACE`; the peer keeps the bytes it has).
    pub async fn copy_metadata(&self, key: &str, meta: &ObjectHeaders, stamp: &Stamp) -> Result<(), PeerError> {
        let path = object_path(&self.config.bucket, key);
        let mut headers = Self::write_headers(meta, stamp);
        headers.push(("x-amz-copy-source".to_string(), path.clone()));
        headers.push(("x-amz-metadata-directive".to_string(), "REPLACE".to_string()));
        let extra: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let builder = Self::with_content_headers(self.signed(Method::PUT, &path, "", &extra), meta).header("content-length", 0);
        let response = self.send_write(builder, &format!("copy {key}")).await?;
        let body = response.text().await.unwrap_or_default();
        if body.contains("<Error>") {
            if xml_tag(&body, "Code").as_deref() == Some("PreconditionFailed") {
                return Err(PeerError::Stale);
            }
            return Err(PeerError::Other(format!("copy {key}: {}", xml_tag(&body, "Message").unwrap_or(body))));
        }
        Ok(())
    }

    /// DELETE `key` with the delete's `stamp` and the stamp of the version
    /// it `removed`.
    pub async fn delete(&self, key: &str, stamp: &Stamp, removed: &Stamp) -> Result<(), PeerError> {
        let path = object_path(&self.config.bucket, key);
        let stamp = stamp.to_header();
        let removed = removed.to_header();
        let extra = [("x-rs3-stamp", stamp.as_str()), ("x-rs3-removed-stamp", removed.as_str())];
        let builder = self.signed(Method::DELETE, &path, "", &extra);
        self.send_write(builder, &format!("DELETE {key}")).await.map(|_| ())
    }

    /// Starts a multipart upload of `key` with its metadata.
    pub async fn create_multipart(&self, key: &str, meta: &ObjectHeaders) -> Result<String, PeerError> {
        let path = object_path(&self.config.bucket, key);
        let headers = Self::meta_headers(meta);
        let extra: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let builder = Self::with_content_headers(self.signed(Method::POST, &path, "uploads=", &extra), meta);
        let response = self.send_write(builder, &format!("create multipart {key}")).await?;
        let body = response.text().await.unwrap_or_default();
        xml_tag(&body, "UploadId").ok_or_else(|| PeerError::Other(format!("no UploadId for {key}: {body}")))
    }

    /// Uploads part `number`; returns its ETag.
    pub async fn upload_part(&self, key: &str, upload_id: &str, number: u16, len: u64, body: reqwest::Body) -> Result<String, PeerError> {
        let path = object_path(&self.config.bucket, key);
        let query = query_string(&[("partNumber", number.to_string()), ("uploadId", upload_id.to_string())]);
        let builder = self
            .signed(Method::PUT, &path, &query, &[])
            .header("content-length", len)
            .body(body);
        let response = self.send_write(builder, &format!("upload part {number} of {key}")).await?;
        response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim_matches('"').to_string())
            .ok_or_else(|| PeerError::Other(format!("no ETag for part {number} of {key}")))
    }

    /// Completes a multipart upload with `stamp`.
    pub async fn complete_multipart(&self, key: &str, upload_id: &str, etags: &[String], stamp: &Stamp) -> Result<String, PeerError> {
        let path = object_path(&self.config.bucket, key);
        let query = query_string(&[("uploadId", upload_id.to_string())]);
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (i, etag) in etags.iter().enumerate() {
            xml.push_str(&format!("<Part><PartNumber>{}</PartNumber><ETag>\"{etag}\"</ETag></Part>", i + 1));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let stamp = stamp.to_header();
        let builder = self
            .signed(Method::POST, &path, &query, &[("x-rs3-stamp", stamp.as_str())])
            .header("content-type", "application/xml")
            .body(xml);
        let response = self.send_write(builder, &format!("complete {key}")).await?;
        let body = response.text().await.unwrap_or_default();
        if body.contains("<Error>") {
            if xml_tag(&body, "Code").as_deref() == Some("PreconditionFailed") {
                return Err(PeerError::Stale);
            }
            return Err(PeerError::Other(format!("complete {key}: {}", xml_tag(&body, "Message").unwrap_or(body))));
        }
        Ok(xml_tag(&body, "ETag").unwrap_or_default().replace("&quot;", "").trim_matches('"').to_string())
    }

    /// Abandons a multipart upload (best effort).
    pub async fn abort_multipart(&self, key: &str, upload_id: &str) {
        let path = object_path(&self.config.bucket, key);
        let query = query_string(&[("uploadId", upload_id.to_string())]);
        let _ = self.signed(Method::DELETE, &path, &query, &[]).send().await;
    }
}

/// `Code: Message` of an S3 XML error body.
fn s3_error_text(body: &str) -> Option<String> {
    let message = xml_tag(body, "Message")?;
    Some(match xml_tag(body, "Code") {
        Some(code) => format!("{code}: {message}"),
        None => message,
    })
}

/// The text of the first `<tag>` in `xml`.
fn xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{tag}>"))? + start;
    Some(xml[start..end].to_string())
}

/// The stamp an event is applied with: its own, or for changes logged
/// before stamps existed, `(last_modified_ms, 0, peer site)`.
pub fn event_stamp(event: &WireEvent, peer_site: &str) -> crate::storage::stamp::Stamp {
    event
        .stamp
        .clone()
        .unwrap_or_else(|| crate::storage::stamp::Stamp::fallback(event.last_modified_ms, peer_site))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_are_aws_encoded_and_never_normalised() {
        assert_eq!(object_path("b", "a b/c+d"), "/b/a%20b/c%2Bd");
        assert_eq!(object_path("b", "x/../y/./z"), "/b/x/%2E%2E/y/%2E/z");
        assert_eq!(object_path("b", "100%"), "/b/100%25");
    }
}
