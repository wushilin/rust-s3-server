//! The signed S3 client a replication link uses to read its peer: the
//! changelog (`?x-rs3-changelog`), the snapshot listings, and object data
//! pinned to an exact version (`x-rs3-if-version`). Requests are SigV4-signed
//! with the link's access key, exactly as the peer verifies them.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use reqwest::{Method, StatusCode};

use crate::server::notify::read::{ReadResponse, SnapshotResponse};
use crate::server::notify::model::WireEvent;

/// Where a link reads from.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// `http(s)://host[:port]`, no path.
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
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
    /// Anything else: retried later.
    Other(String),
}

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerError::Gone(m) | PeerError::LogMismatch(m) | PeerError::Other(m) => f.write_str(m),
            PeerError::Superseded => f.write_str("the object changed on the peer since the event"),
            PeerError::NotFound => f.write_str("the object is not on the peer"),
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
        Ok(Self { config, host, http })
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

    async fn json<T: serde::de::DeserializeOwned>(&self, query: &str, timeout: Duration) -> Result<T, PeerError> {
        let path = format!("/{}", encode_segment(&self.config.bucket));
        let response = self
            .signed(Method::GET, &path, query, &[])
            .timeout(timeout)
            .send()
            .await
            .map_err(|err| PeerError::Other(format!("peer unreachable: {err}")))?;
        let status = response.status();
        let body = response.bytes().await.map_err(|err| PeerError::Other(err.to_string()))?;
        let message = || {
            serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v["error"].as_str().map(str::to_string))
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
