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
use crate::storage::stamp::{Rank, Stamp};

/// Request header naming the replication link that reads a changelog — a
/// [`Replicator`] in the [`encode_fields`] form.
pub const HEADER_REPLICATOR: &str = "x-rs3-replicator";
/// Query selecting the access report (`GET /{bucket}?x-rs3-access`): what
/// the caller's key may do on the bucket, as [`AccessReport`].
pub const QUERY_ACCESS: &str = "x-rs3-access";

/// The actions a link needs on the peer bucket: reading it (both modes),
/// and for a merge link also writing it with the changes' own stamps.
pub const READ_ACTIONS: [&str; 2] = ["s3:GetObject", "s3:GetBucketChangelog"];
pub const WRITE_ACTIONS: [&str; 3] = ["s3:PutObject", "s3:DeleteObject", "s3:ReplicateWrite"];

/// The answer to [`QUERY_ACCESS`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccessReport {
    pub site_id: String,
    /// The answering site's readable name (a label).
    #[serde(default)]
    pub site_name: String,
    pub bucket: String,
    pub read_only: bool,
    /// Each of [`READ_ACTIONS`] and [`WRITE_ACTIONS`]: allowed on the bucket
    /// (and on every key in it)?
    pub actions: BTreeMap<String, bool>,
    /// A merge link on the answering site already pairs this bucket with the
    /// caller's: a [`MergeBack`] in the [`encode_fields`] form.
    #[serde(default)]
    pub merge_back: Option<String>,
    /// The bucket's versioning state (`off`, `enabled`, `suspended`). A
    /// link requires both buckets to have the same one.
    #[serde(default = "default_versioning")]
    pub versioning: String,
}

fn default_versioning() -> String {
    "off".to_string()
}

/// Response header: the answering site already has a merge link pairing
/// this bucket with the reader's site — a [`MergeBack`] in the
/// [`encode_fields`] form.
pub const HEADER_MERGE_BACK: &str = "x-rs3-merge-back";

/// A header value with named fields: `key=value;key=value`, keys and
/// values URL-encoded (so a value may contain anything, `;` and `=`
/// included). Extensible: readers look fields up by name, ignore ones they
/// do not know, and do not depend on the order.
pub fn encode_fields(fields: &[(&str, &str)]) -> String {
    fields
        .iter()
        .map(|(key, value)| format!("{}={}", urlencoding::encode(key), urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join(";")
}

/// Parses [`encode_fields`]. Parts without `=` or that do not decode are
/// skipped; for a repeated key the first wins.
pub fn parse_fields(value: &str) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for part in value.split(';') {
        let Some((key, value)) = part.trim().split_once('=') else {
            continue;
        };
        let (Ok(key), Ok(value)) = (urlencoding::decode(key), urlencoding::decode(value)) else {
            continue;
        };
        fields.entry(key.into_owned()).or_insert_with(|| value.into_owned());
    }
    fields
}

/// Who reads a changelog ([`HEADER_REPLICATOR`]): `site`, `mode`
/// (`mirror` | `merge`), `bucket` (the link's bucket on its site), `link`
/// (its id) and `name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replicator {
    pub site: String,
    /// The reading site's readable name (optional field).
    pub site_name: String,
    pub mode: String,
    pub bucket: String,
    pub link: String,
    pub name: String,
}

impl Replicator {
    pub fn to_header(&self) -> String {
        encode_fields(&[
            ("site", &self.site),
            ("site_name", &self.site_name),
            ("mode", &self.mode),
            ("bucket", &self.bucket),
            ("link", &self.link),
            ("name", &self.name),
        ])
    }

    /// `None` unless `site`, `mode`, `bucket` and `link` are present and
    /// sane (`name` may be missing).
    pub fn parse(value: &str) -> Option<Self> {
        let mut fields = parse_fields(value);
        let mut take = |key: &str| fields.remove(key);
        let replicator = Self {
            site: take("site")?,
            site_name: take("site_name").unwrap_or_default(),
            mode: take("mode")?,
            bucket: take("bucket")?,
            link: take("link")?,
            name: take("name").unwrap_or_default(),
        };
        let sane = !replicator.site.is_empty()
            && replicator.site.len() <= 64
            && replicator.mode.len() <= 16
            && !replicator.bucket.is_empty()
            && replicator.bucket.len() <= 63
            && !replicator.link.is_empty()
            && replicator.link.len() <= 64
            && replicator.name.len() <= 256
            && replicator.site_name.len() <= 256;
        sane.then_some(replicator)
    }
}

/// The answering site's own merge link for the pair ([`HEADER_MERGE_BACK`],
/// and [`AccessReport::merge_back`]): `link` (its id) and `name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBack {
    pub link: String,
    pub name: String,
}

impl MergeBack {
    pub fn to_header(&self) -> String {
        encode_fields(&[("link", &self.link), ("name", &self.name)])
    }

    pub fn parse(value: &str) -> Option<Self> {
        let mut fields = parse_fields(value);
        let link = fields.remove("link").filter(|l| !l.is_empty())?;
        Some(Self { link, name: fields.remove("name").unwrap_or_default() })
    }
}

/// Where a link reads from.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// `http(s)://host[:port]`, no path.
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    /// Sent as [`HEADER_REPLICATOR`] on changelog reads — built per
    /// request, so a renamed site is known by its new name at once.
    pub replicator: Option<ReplicatorHeader>,
}

/// Builds the [`HEADER_REPLICATOR`] value.
#[derive(Clone)]
pub struct ReplicatorHeader(pub std::sync::Arc<dyn Fn() -> String + Send + Sync>);

impl std::fmt::Debug for ReplicatorHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicatorHeader")
    }
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
    /// `x-amz-version-id`: a real version (`Some(true)`), the `null`
    /// version (`Some(false)`), or a bucket without versioning (`None`).
    pub versioned: Option<bool>,
    /// The object's tags (`x-rs3-tags`, named fields).
    pub tags: BTreeMap<String, String>,
}

pub struct PeerClient {
    config: PeerConfig,
    host: String,
    http: reqwest::Client,
    /// The last changelog read's [`HEADER_MERGE_BACK`] answer.
    merge_back: std::sync::Mutex<Option<String>>,
    /// The site id the peer last answered an object HEAD with (`x-rs3-site`).
    last_site: std::sync::Mutex<Option<String>>,
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
        Ok(Self {
            config,
            host,
            http,
            merge_back: std::sync::Mutex::new(None),
            last_site: std::sync::Mutex::new(None),
        })
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

    /// The site id the peer reported on the last [`Self::state`].
    pub fn last_site(&self) -> Option<String> {
        self.last_site.lock().unwrap().clone()
    }

    async fn json<T: serde::de::DeserializeOwned>(&self, query: &str, timeout: Duration) -> Result<T, PeerError> {
        let path = format!("/{}", encode_segment(&self.config.bucket));
        // Signed with the request (the peer requires every x-rs3-* header to
        // be).
        let replicator = self.config.replicator.as_ref().map(|r| (r.0)());
        let extra: Vec<(&str, &str)> = replicator.iter().map(|r| (HEADER_REPLICATOR, r.as_str())).collect();
        let request = self.signed(Method::GET, &path, query, &extra).timeout(timeout);
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

    /// What the link's key may do on the peer bucket.
    pub async fn access(&self) -> Result<AccessReport, PeerError> {
        self.json(&format!("{QUERY_ACCESS}="), Duration::from_secs(15)).await
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
    pub async fn snapshot(&self, after: Option<&str>, limit: usize, prefix: &str, deleted: bool, versions: bool) -> Result<SnapshotResponse, PeerError> {
        let mut params = vec![
            ("limit", limit.to_string()),
            (crate::server::notify::read::QUERY_SNAPSHOT, String::new()),
        ];
        // A versions cursor is `<key>\0<version dir>` (see the worker).
        if let Some(after) = after {
            match after.split_once('\0') {
                Some((key, version)) => {
                    params.push(("after", key.to_string()));
                    params.push(("after_version", version.to_string()));
                }
                None => params.push(("after", after.to_string())),
            }
        }
        if versions {
            params.push(("versions", "1".to_string()));
        }
        if !prefix.is_empty() {
            params.push(("prefix", prefix.to_string()));
        }
        if deleted {
            params.push(("deleted", "1".to_string()));
        }
        self.json(&query_string(&params), Duration::from_secs(60)).await
    }

    /// GET (optionally a byte range) of exactly the state `version` /
    /// `stamp` of `key`: pinned by both, since a storage layout upgrade on
    /// the peer renames versions but never changes a stamp (§5.1).
    async fn get(&self, key: &str, version: &str, stamp: &Stamp, range: Option<(u64, u64)>, method: Method) -> Result<reqwest::Response, PeerError> {
        let path = object_path(&self.config.bucket, key);
        let range_value = range.map(|(start, end)| format!("bytes={start}-{end}"));
        let stamp_value = stamp.to_header();
        let mut extra: Vec<(&str, &str)> = vec![("x-rs3-if-version", version), ("x-rs3-if-stamp", stamp_value.as_str())];
        if let Some(range) = &range_value {
            extra.push(("range", range.as_str()));
        }
        // Asked for by version id (the stamp): a versioned peer still has
        // the state after the key moved on; an unversioned one answers 404
        // for a superseded state, which the caller skips.
        let query = format!("versionId={}", crate::storage::layouts::v3::stamp_dir(stamp));
        let response = self
            .signed(method, &path, &query, &extra)
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
            versioned: text("x-amz-version-id").map(|v| v != "null"),
            tags: text("x-rs3-tags").map(|v| parse_fields(&v)).unwrap_or_default(),
        }
    }

    /// Metadata of exactly `version` / `stamp` of `key`.
    pub async fn head(&self, key: &str, version: &str, stamp: &Stamp) -> Result<ObjectHeaders, PeerError> {
        let response = self.get(key, version, stamp, None, Method::HEAD).await?;
        Ok(Self::headers_of(&response))
    }

    /// The bytes (all, or `range`) of exactly `version` / `stamp` of `key`.
    pub async fn body(
        &self,
        key: &str,
        version: &str,
        stamp: &Stamp,
        range: Option<(u64, u64)>,
    ) -> Result<(ObjectHeaders, impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin), PeerError> {
        let response = self.get(key, version, stamp, range, Method::GET).await?;
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
        if let Some(site) = response.headers().get("x-rs3-site").and_then(|v| v.to_str().ok()) {
            *self.last_site.lock().unwrap() = Some(site.to_string());
        }
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
        if let Some(versioned) = meta.versioned {
            headers.push(("x-rs3-versioned".to_string(), versioned.to_string()));
        }
        headers
    }

    fn meta_headers(meta: &ObjectHeaders) -> Vec<(String, String)> {
        let fold = |v: &str| v.split_whitespace().collect::<Vec<_>>().join(" ");
        let mut headers = Vec::new();
        if !meta.tags.is_empty() {
            let tagging = meta
                .tags
                .iter()
                .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            headers.push(("x-amz-tagging".to_string(), tagging));
        }
        for (name, value) in &meta.user_meta {
            headers.push((format!("x-amz-meta-{name}"), fold(value)));
        }
        if let Some(class) = meta.storage_class.as_deref().filter(|c| *c != "STANDARD") {
            headers.push(("x-amz-storage-class".to_string(), class.to_string()));
        }
        headers
    }

    /// The raw user-metadata values. [`meta_headers`] folds whitespace for
    /// the signature (the canonical form); the headers sent carry the
    /// values as stored, so the peer stores exactly them.
    fn raw_meta(meta: &ObjectHeaders) -> reqwest::header::HeaderMap {
        let mut map = reqwest::header::HeaderMap::new();
        for (name, value) in &meta.user_meta {
            if let (Ok(name), Ok(value)) = (
                reqwest::header::HeaderName::from_bytes(format!("x-amz-meta-{name}").as_bytes()),
                reqwest::header::HeaderValue::from_str(value),
            ) {
                map.insert(name, value);
            }
        }
        map
    }

    fn with_content_headers(mut builder: reqwest::RequestBuilder, meta: &ObjectHeaders) -> reqwest::RequestBuilder {
        builder = builder.headers(Self::raw_meta(meta));
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
            StatusCode::NOT_FOUND => Err(PeerError::NotFound),
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
    pub async fn delete(&self, key: &str, stamp: &Stamp, removed: &Stamp, versioned: Option<bool>) -> Result<(), PeerError> {
        let path = object_path(&self.config.bucket, key);
        let stamp = stamp.to_header();
        let removed = removed.to_header();
        let versioned = versioned.map(|v| v.to_string());
        let mut extra = vec![("x-rs3-stamp", stamp.as_str()), ("x-rs3-removed-stamp", removed.as_str())];
        if let Some(v) = &versioned {
            extra.push(("x-rs3-versioned", v.as_str()));
        }
        let builder = self.signed(Method::DELETE, &path, "", &extra);
        self.send_write(builder, &format!("DELETE {key}")).await.map(|_| ())
    }

    /// Replaces the tags of version `stamp` of `key` on the peer
    /// (`PUT ?tagging&versionId`, a replicated write).
    pub async fn put_tags(&self, key: &str, stamp: &Stamp, tags: &BTreeMap<String, String>, author: &str) -> Result<(), PeerError> {
        let path = object_path(&self.config.bucket, key);
        let header = stamp.to_header();
        let extra = [("x-rs3-stamp", header.as_str())];
        let query = format!("tagging&versionId={}&x-rs3-author={author}", crate::storage::layouts::v3::stamp_dir(stamp));
        let set = tags
            .iter()
            .map(|(k, v)| format!("<Tag><Key>{}</Key><Value>{}</Value></Tag>", crate::server::xml::escape_xml(k), crate::server::xml::escape_xml(v)))
            .collect::<String>();
        let body = format!(r#"<Tagging><TagSet>{set}</TagSet></Tagging>"#);
        let builder = self.signed(Method::PUT, &path, &query, &extra).header("content-length", body.len()).body(body);
        self.send_write(builder, &format!("PUT {key}?tagging")).await.map(|_| ())
    }

    /// Sends this bucket's settings to the peer (replicated writes of
    /// `?versioning` and `?cors`, stamped with each setting's change): the
    /// peer adopts what is newer than its own.
    pub async fn put_settings(&self, settings: &crate::storage::metadata::BucketSettings) -> Result<(), PeerError> {
        let path = format!("/{}", self.config.bucket);
        if let Some(stamp) = &settings.versioning_stamp {
            if settings.versioning != crate::storage::metadata::BucketVersioning::Off {
                let status = if settings.versioning == crate::storage::metadata::BucketVersioning::Enabled { "Enabled" } else { "Suspended" };
                let body = format!("<VersioningConfiguration><Status>{status}</Status></VersioningConfiguration>");
                let query = format!("versioning&x-rs3-stamp={}", urlencoding::encode(&stamp.to_header()));
                let builder = self.signed(Method::PUT, &path, &query, &[]).header("content-length", body.len()).body(body);
                self.send_write(builder, "PUT ?versioning").await?;
            }
        }
        if let Some(stamp) = &settings.cors_stamp {
            let rules = settings
                .cors
                .iter()
                .map(|rule| {
                    let tag = |name: &str, values: &[String]| {
                        values.iter().map(|v| format!("<{name}>{}</{name}>", crate::server::xml::escape_xml(v))).collect::<String>()
                    };
                    let max_age = rule.max_age_seconds.map(|v| format!("<MaxAgeSeconds>{v}</MaxAgeSeconds>")).unwrap_or_default();
                    format!(
                        "<CORSRule>{}{}{}{}{max_age}</CORSRule>",
                        tag("AllowedOrigin", &rule.allowed_origins),
                        tag("AllowedMethod", &rule.allowed_methods),
                        tag("AllowedHeader", &rule.allowed_headers),
                        tag("ExposeHeader", &rule.expose_headers)
                    )
                })
                .collect::<String>();
            let body = format!("<CORSConfiguration>{rules}</CORSConfiguration>");
            let query = format!("cors&x-rs3-stamp={}", urlencoding::encode(&stamp.to_header()));
            let builder = self.signed(Method::PUT, &path, &query, &[]).header("content-length", body.len()).body(body);
            self.send_write(builder, "PUT ?cors").await?;
        }
        Ok(())
    }

    /// Permanently removes version `stamp` of `key` on the peer
    /// (`DELETE ?versionId`, a replicated write).
    pub async fn delete_version(&self, key: &str, stamp: &Stamp, author: &str) -> Result<(), PeerError> {
        let path = object_path(&self.config.bucket, key);
        let header = stamp.to_header();
        let extra = [("x-rs3-stamp", header.as_str())];
        let query = format!("versionId={}&x-rs3-author={author}", crate::storage::layouts::v3::stamp_dir(stamp));
        let builder = self.signed(Method::DELETE, &path, &query, &extra);
        self.send_write(builder, &format!("DELETE {key}?versionId")).await.map(|_| ())
    }

    /// The peer bucket's versioning state, read from a one-entry snapshot
    /// page (which the link's key can always read).
    pub async fn versioning(&self) -> Result<String, PeerError> {
        let page = self.snapshot(None, 1, "", false, false).await?;
        Ok(page.versioning)
    }

    /// Starts a multipart upload of `key` with its metadata.
    pub async fn create_multipart(&self, key: &str, meta: &ObjectHeaders) -> Result<String, PeerError> {
        let path = object_path(&self.config.bucket, key);
        let mut headers = Self::meta_headers(meta);
        // Replication's upload (a read-only peer bucket accepts it).
        headers.push((crate::server::REPLICATE_HEADER.to_string(), "true".to_string()));
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_fields_are_named_encoded_and_extensible() {
        let r = Replicator {
            site: "site-b".into(),
            site_name: "Tokyo; DC=2".into(),
            mode: "merge".into(),
            bucket: "photos".into(),
            link: "rl_1".into(),
            name: "a;b=c %d ✓".into(),
        };
        let header = r.to_header();
        assert_eq!(header.split(';').count(), 6, "separators in values are encoded: {header}");
        assert!(header.is_ascii(), "a valid header value: {header}");
        assert_eq!(Replicator::parse(&header), Some(r.clone()));
        // Any order, unknown fields ignored, a missing name tolerated.
        assert_eq!(
            Replicator::parse("future=1;link=rl_1;bucket=photos;mode=merge;site=site-b;name=x"),
            Some(Replicator { name: "x".into(), site_name: String::new(), ..r.clone() })
        );
        assert_eq!(Replicator::parse("site=s;mode=m;bucket=b;link=l").unwrap().name, "");
        // Required fields missing, or junk: ignored.
        assert_eq!(Replicator::parse("site=s;mode=m;bucket=b"), None);
        assert_eq!(Replicator::parse("site-b;merge;photos;rl_1;name"), None);
        assert_eq!(Replicator::parse(""), None);
        let back = MergeBack { link: "rl_2".into(), name: "x;y".into() };
        assert_eq!(MergeBack::parse(&back.to_header()), Some(back));
        assert_eq!(MergeBack::parse("name=x"), None);
    }

    #[test]
    fn paths_are_aws_encoded_and_never_normalised() {
        assert_eq!(object_path("b", "a b/c+d"), "/b/a%20b/c%2Bd");
        assert_eq!(object_path("b", "x/../y/./z"), "/b/x/%2E%2E/y/%2E/z");
        assert_eq!(object_path("b", "100%"), "/b/100%25");
    }
}
