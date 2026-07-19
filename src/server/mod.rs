//! Axum HTTP layer: routing, middleware, and request/response handling.
//!
//! Entry point: [`serve`] starts the HTTP server.  [`router`] builds the
//! Axum [`Router`] for use in integration tests.

pub mod auth;
pub mod config;
pub mod event_hub;
pub(crate) mod handlers;
pub mod iam;
pub(crate) mod jobs;
pub mod identity;
pub mod logging;
pub(crate) mod pipeline;
pub mod policy;
pub mod range;
pub mod registry;
pub mod ui;
pub mod xml;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::Path as FsPath;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Extension, Router};
use futures::{StreamExt, TryStreamExt};
use regex::Regex;

use self::auth::{auth_middleware, AuthState};
use self::identity::Identity;
use self::config::AppConfig;
use self::iam::IamStore;
use self::xml::{error_xml, list_buckets_xml, BucketListEntry, S3ErrorXml};
use crate::storage::errors::StorageError;
use crate::storage::metadata::is_valid_storage_class;
use crate::storage::metadata::quote_etag;
use crate::storage::store::{CompletePartRequest, LocalObjectStore};
use crate::storage::time::parse_http_date_ms;

const MAX_USER_META_BYTES: usize = 2 * 1024;

#[derive(Debug, Clone, Default)]
pub(crate) struct OperationActor {
    pub username: Option<String>,
    pub access_key: Option<String>,
}

impl OperationActor {
    fn label(&self) -> String {
        match (&self.username, &self.access_key) {
            (Some(username), Some(access_key)) => format!("{username}/{access_key}"),
            (Some(username), None) => username.clone(),
            (None, Some(access_key)) => format!("anonymous/{access_key}"),
            (None, None) => "anonymous".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum OperationMeasure {
    Bytes(u64),
    Objects(usize),
    Buckets(usize),
    Uploads(usize),
    Parts(usize),
}

#[derive(Debug, Clone, Copy)]
enum OperationDisposition {
    Partial,
}

#[derive(Debug, Clone)]
pub struct S3HttpConfig {
    pub address: SocketAddr,
    pub root: String,
    pub app_config: Arc<AppConfig>,
}

#[derive(Debug, Default)]
pub(crate) struct TrafficMetrics {
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    active_gets: AtomicU64,
    active_puts: AtomicU64,
    get_requests: AtomicU64,
    put_requests: AtomicU64,
    head_requests: AtomicU64,
    delete_requests: AtomicU64,
    post_requests: AtomicU64,
    other_requests: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
struct TrafficSnapshot {
    bytes_in: u64,
    bytes_out: u64,
    get_requests: u64,
    put_requests: u64,
    head_requests: u64,
    delete_requests: u64,
    post_requests: u64,
    other_requests: u64,
}

impl TrafficSnapshot {
    fn request_total(self) -> u64 {
        self.get_requests
            + self.put_requests
            + self.head_requests
            + self.delete_requests
            + self.post_requests
            + self.other_requests
    }

    fn saturating_sub(self, previous: Self) -> Self {
        Self {
            bytes_in: self.bytes_in.saturating_sub(previous.bytes_in),
            bytes_out: self.bytes_out.saturating_sub(previous.bytes_out),
            get_requests: self.get_requests.saturating_sub(previous.get_requests),
            put_requests: self.put_requests.saturating_sub(previous.put_requests),
            head_requests: self.head_requests.saturating_sub(previous.head_requests),
            delete_requests: self
                .delete_requests
                .saturating_sub(previous.delete_requests),
            post_requests: self.post_requests.saturating_sub(previous.post_requests),
            other_requests: self.other_requests.saturating_sub(previous.other_requests),
        }
    }
}

impl TrafficMetrics {
    fn add_request(&self, method: &Method) {
        let counter = match *method {
            Method::GET => &self.get_requests,
            Method::PUT => &self.put_requests,
            Method::HEAD => &self.head_requests,
            Method::DELETE => &self.delete_requests,
            Method::POST => &self.post_requests,
            _ => &self.other_requests,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn add_in(&self, bytes: u64) {
        self.bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }

    fn add_out(&self, bytes: u64) {
        self.bytes_out.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Marks an in-flight download; decremented when the guard drops
    /// (i.e. when the response body stream finishes or is abandoned).
    pub(crate) fn begin_get(self: &Arc<Self>) -> ActiveTransferGuard {
        self.active_gets.fetch_add(1, Ordering::Relaxed);
        ActiveTransferGuard { metrics: self.clone(), is_get: true }
    }

    /// Marks an in-flight upload; decremented when the guard drops.
    pub(crate) fn begin_put(self: &Arc<Self>) -> ActiveTransferGuard {
        self.active_puts.fetch_add(1, Ordering::Relaxed);
        ActiveTransferGuard { metrics: self.clone(), is_get: false }
    }

    /// `(total_bytes_in, total_bytes_out)` since start — the task endpoint lets
    /// the client derive up/down rates from successive polls.
    pub(crate) fn byte_totals(&self) -> (u64, u64) {
        (
            self.bytes_in.load(Ordering::Relaxed),
            self.bytes_out.load(Ordering::Relaxed),
        )
    }

    fn snapshot(&self) -> TrafficSnapshot {
        TrafficSnapshot {
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            get_requests: self.get_requests.load(Ordering::Relaxed),
            put_requests: self.put_requests.load(Ordering::Relaxed),
            head_requests: self.head_requests.load(Ordering::Relaxed),
            delete_requests: self.delete_requests.load(Ordering::Relaxed),
            post_requests: self.post_requests.load(Ordering::Relaxed),
            other_requests: self.other_requests.load(Ordering::Relaxed),
        }
    }
}

/// Builds the Axum router.  Exported so integration tests can call it directly.
pub fn router(store: LocalObjectStore, app_config: Arc<AppConfig>) -> Router {
    router_with_metrics(
        store,
        AuthState {
            config: app_config,
            iam: None,
        },
        Arc::new(TrafficMetrics::default()),
        registry::TaskRegistry::new(),
    )
}

fn router_with_metrics(
    store: LocalObjectStore,
    auth_state: AuthState,
    metrics: Arc<TrafficMetrics>,
    tasks: Arc<registry::TaskRegistry>,
) -> Router {
    Router::new()
        .route("/minio/health/live", get(health_live))
        .route("/minio/health/ready", get(health_live))
        .route("/minio/v2/metrics/cluster", get(metrics_endpoint))
        .route("/minio/v2/metrics/node", get(metrics_endpoint))
        .route("/minio/v2/metrics/bucket", get(metrics_endpoint))
        .route("/minio/v2/metrics/resource", get(metrics_endpoint))
        .route("/minio/prometheus/metrics", get(metrics_endpoint))
        .route("/", get(list_buckets))
        .route("/:bucket", any(bucket_route))
        .route("/:bucket/", any(bucket_route))
        .route("/:bucket/*key", any(object_route))
        // Innermost: register the in-flight task around the handler only (auth
        // must pass first). Request id is already set by the outer log layer.
        .layer(middleware::from_fn_with_state(
            tasks,
            task_registry_middleware,
        ))
        .layer(middleware::from_fn_with_state(auth_state, auth_middleware))
        .layer(middleware::from_fn_with_state(
            metrics,
            traffic_metrics_middleware,
        ))
        .layer(middleware::from_fn(log_middleware))
        .layer(DefaultBodyLimit::max(5 * 1024 * 1024 * 1024))
        .with_state(store)
}

/// Registers each request as an active task while its handler runs, so the
/// server always has an accurate view of in-flight work. The [`TaskGuard`]
/// deregisters on completion — success, error, or panic.
async fn task_registry_middleware(
    State(tasks): State<Arc<registry::TaskRegistry>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map(|id| id.0.clone())
        .unwrap_or_default();
    let method = request.method().clone();
    let is_copy = request.headers().contains_key("x-amz-copy-source");
    let (op, target) = operation_and_target(&method, request.uri(), is_copy);
    // Make the registry reachable by handlers that spawn their own jobs
    // (e.g. the rebuild-index admin verb).
    request.extensions_mut().insert(tasks.clone());
    let guard = tasks.register(request_id, registry::TaskKind::S3, op, target);
    let progress = guard.progress();
    let cancel = guard.cancel_token();

    // Uploads: total from the declared length, bytes counted as the request body
    // streams into the handler (the guard is held across `next.run`). A cancel
    // errors the stream mid-transfer — the handler's staging write fails and is
    // reclaimed by the sweeper, exactly like a client disconnect. Once the body
    // is fully read the stream is exhausted, so the atomic publish + SQLite
    // commit that follows can no longer be interrupted.
    if matches!(method, Method::PUT | Method::POST) {
        if let Some(total) = header_value(request.headers(), "x-amz-decoded-content-length")
            .or_else(|| header_value(request.headers(), header::CONTENT_LENGTH.as_str()))
            .and_then(|v| v.parse::<u64>().ok())
        {
            progress.set_total(total);
        }
        let counting = progress.clone();
        let cancel = cancel.clone();
        let (parts, body) = request.into_parts();
        let counted = body.into_data_stream().map(move |result| {
            if cancel.is_cancelled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "upload cancelled by operator",
                ));
            }
            match result {
                Ok(chunk) => {
                    counting.add_done(chunk.len() as u64);
                    Ok(chunk)
                }
                Err(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, err.to_string())),
            }
        });
        request = Request::from_parts(parts, Body::from_stream(counted));
    }

    let response = next.run(request).await;

    // Downloads: keep the task registered while the body streams to the client
    // (the guard rides inside the response stream), count bytes out, and stop
    // sending on cancel.
    if method == Method::GET && response.status().is_success() {
        if let Some(total) = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
        {
            progress.set_total(total);
        }
        let (parts, body) = response.into_parts();
        let counted = body.into_data_stream().map(move |result| {
            // `guard` is moved in here, so the task stays active until the last
            // byte is sent (or the client disconnects and the stream drops).
            let _hold = &guard;
            if cancel.is_cancelled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "download cancelled by operator",
                ));
            }
            match result {
                Ok(chunk) => {
                    progress.add_done(chunk.len() as u64);
                    Ok(chunk)
                }
                Err(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, err.to_string())),
            }
        });
        return Response::from_parts(parts, Body::from_stream(counted));
    }
    drop(guard);
    response
}

async fn health_live() -> Response {
    empty_response(StatusCode::OK)
}

async fn metrics_endpoint() -> Response {
    let body = "# HELP rusts3_up RustS3 compatibility metrics endpoint\n# TYPE rusts3_up gauge\nrusts3_up 1\n";
    let mut response = (StatusCode::OK, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    response
}

// ─── Request tracing / logging middleware ─────────────────────────────────────

/// A per-request correlation id, injected at the very top of the pipeline so
/// every downstream layer (auth, handlers) and every log line can be traced
/// back to a single request. Surfaced to clients as `x-amz-request-id`.
#[derive(Clone, Debug)]
pub(crate) struct RequestId(pub String);

fn new_request_id() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn log_middleware(mut request: Request<Body>, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let is_copy = request.headers().contains_key("x-amz-copy-source");
    // Inject the request id before anything else runs, so auth decisions and
    // handler logs can all reference it.
    let request_id = new_request_id();
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));
    log::debug!("[{request_id}] -> {method} {uri} dispatching");
    let put_decoded_content_length = if method == Method::PUT {
        header_value(request.headers(), "x-amz-decoded-content-length")
            .or_else(|| header_value(request.headers(), header::CONTENT_LENGTH.as_str()))
    } else {
        None
    };
    let put_request_bytes = Arc::new(AtomicU64::new(0));
    let request = if method == Method::PUT {
        let counted_bytes = put_request_bytes.clone();
        let (parts, body) = request.into_parts();
        let counted_body = body.into_data_stream().inspect_ok(move |bytes| {
            counted_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        });
        Request::from_parts(parts, Body::from_stream(counted_body))
    } else {
        request
    };
    let start = std::time::Instant::now();
    let mut response = next.run(request).await;
    let elapsed_ms = start.elapsed().as_millis();
    let status = response.status();
    // Echo the correlation id back to the client and ensure it's on the
    // response for anything downstream that inspects headers.
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-amz-request-id", value);
    }
    let content_length = log_content_length(
        &method,
        put_decoded_content_length.as_deref(),
        put_request_bytes.load(Ordering::Relaxed),
        response.headers(),
    );
    let actor = response
        .extensions()
        .get::<OperationActor>()
        .cloned()
        .unwrap_or_default()
        .label();
    let (operation, target) = operation_and_target(&method, &uri, is_copy);
    let result = if matches!(
        response.extensions().get::<OperationDisposition>(),
        Some(OperationDisposition::Partial)
    ) {
        "PARTIAL".to_string()
    } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        "Denied".to_string()
    } else if status.is_success() || status.is_redirection() {
        "OK".to_string()
    } else {
        format!("ERROR({})", status.as_u16())
    };
    let measure = response
        .extensions()
        .get::<OperationMeasure>()
        .map(format_operation_measure)
        .or_else(|| {
            matches!(operation, "UPLOAD" | "UPLOAD_PART" | "COPY")
                .then(|| format!("{content_length} bytes"))
        })
        .or_else(|| {
            (operation == "DOWNLOAD" && content_length != "-")
                .then(|| format!("{content_length} bytes"))
        })
        .unwrap_or_default();
    let suffix = if measure.is_empty() {
        format!("in {elapsed_ms}ms")
    } else {
        format!("{measure} in {elapsed_ms}ms")
    };
    if status.is_client_error() || status.is_server_error() {
        log::warn!(target: logging::TARGET_AUDIT, "[{request_id}] {actor} {operation} {target} {result} {suffix}");
    } else {
        log::info!(target: logging::TARGET_AUDIT, "[{request_id}] {actor} {operation} {target} {result} {suffix}");
    }
    response
}

fn operation_and_target(
    method: &Method,
    uri: &axum::http::Uri,
    is_copy: bool,
) -> (&'static str, String) {
    let path = percent_decode(uri.path());
    let trimmed = path.trim_start_matches('/');
    let (bucket, key) = trimmed
        .split_once('/')
        .map(|(bucket, key)| (bucket, Some(key)))
        .unwrap_or((trimmed, None));
    let query = parse_s3_query(uri.query().unwrap_or(""));
    if bucket.is_empty() {
        return ("LIST_BUCKETS", "/".to_string());
    }
    if key.is_none() || key == Some("") {
        if *method == Method::PUT {
            return ("CREATE_BUCKET", format!("/{bucket}"));
        }
        if *method == Method::DELETE {
            return ("DELETE_BUCKET", format!("/{bucket}"));
        }
        if *method == Method::POST && query.contains_key("delete") {
            return ("DELETE_OBJECTS", format!("/{bucket}"));
        }
        if *method == Method::POST && query.contains_key("rebuildIndex") {
            return ("REBUILD_INDEX", format!("/{bucket}"));
        }
        if *method == Method::GET {
            let prefix = query.get("prefix").map(String::as_str).unwrap_or("");
            let target = if prefix.is_empty() {
                format!("/{bucket}/")
            } else {
                format!("/{bucket}/{prefix}")
            };
            if query.contains_key("uploads") {
                return ("LIST_MULTIPART_UPLOADS", target);
            }
            if query.contains_key("versions") {
                return ("LIST_VERSIONS", target);
            }
            if query.contains_key("location") {
                return ("GET_BUCKET_LOCATION", format!("/{bucket}"));
            }
            return ("LIST", target);
        }
        if *method == Method::HEAD {
            return ("HEAD_BUCKET", format!("/{bucket}"));
        }
    }
    let target = path;
    match *method {
        Method::PUT if is_copy && query.contains_key("partNumber") => ("COPY_PART", target),
        Method::PUT if is_copy => ("COPY", target),
        Method::PUT if query.contains_key("partNumber") => ("UPLOAD_PART", target),
        Method::PUT => ("UPLOAD", target),
        Method::GET if query.contains_key("uploadId") => ("LIST_PARTS", target),
        Method::GET => ("DOWNLOAD", target),
        Method::HEAD => ("HEAD", target),
        Method::DELETE if query.contains_key("uploadId") => ("ABORT_MULTIPART", target),
        Method::DELETE => ("DELETE", target),
        Method::POST if query.contains_key("uploads") => ("CREATE_MULTIPART", target),
        Method::POST if query.contains_key("uploadId") => ("COMPLETE_MULTIPART", target),
        Method::POST => ("POST", target),
        _ => ("S3", target),
    }
}

fn format_operation_measure(measure: &OperationMeasure) -> String {
    match measure {
        OperationMeasure::Bytes(bytes) => format!("{bytes} bytes"),
        OperationMeasure::Objects(objects) => format!("{objects} objects"),
        OperationMeasure::Buckets(buckets) => format!("{buckets} buckets"),
        OperationMeasure::Uploads(uploads) => format!("{uploads} uploads"),
        OperationMeasure::Parts(parts) => format!("{parts} parts"),
    }
}

fn log_content_length(
    method: &Method,
    put_decoded_content_length: Option<&str>,
    put_request_bytes: u64,
    response_headers: &HeaderMap,
) -> String {
    if method == Method::PUT {
        return put_decoded_content_length
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| put_request_bytes.to_string());
    }
    header_value(response_headers, header::CONTENT_LENGTH.as_str()).unwrap_or_else(|| "-".into())
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(ToOwned::to_owned)
}

/// RAII marker for one in-flight transfer; dropping it (stream finished,
/// client disconnected, handler done) decrements the gauge.
pub(crate) struct ActiveTransferGuard {
    metrics: Arc<TrafficMetrics>,
    is_get: bool,
}

impl Drop for ActiveTransferGuard {
    fn drop(&mut self) {
        let counter = if self.is_get {
            &self.metrics.active_gets
        } else {
            &self.metrics.active_puts
        };
        counter.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn traffic_metrics_middleware(
    State(metrics): State<Arc<TrafficMetrics>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let (parts, body) = request.into_parts();
    metrics.add_request(&parts.method);
    // An upload is "active" while its request body streams in.
    let put_guard = (parts.method == Method::PUT).then(|| metrics.begin_put());
    let counted_metrics = metrics.clone();
    let counted_body = body.into_data_stream().inspect_ok(move |bytes| {
        let _hold = &put_guard;
        counted_metrics.add_in(bytes.len() as u64);
    });
    let request = Request::from_parts(parts, Body::from_stream(counted_body));

    let is_get = request.method() == Method::GET;
    let response = next.run(request).await;
    let (parts, body) = response.into_parts();
    // A download is "active" while its response body streams out.
    let get_guard = is_get.then(|| metrics.begin_get());
    let counted_metrics = metrics.clone();
    let counted_body = body.into_data_stream().inspect_ok(move |bytes| {
        let _hold = &get_guard;
        counted_metrics.add_out(bytes.len() as u64);
    });
    Response::from_parts(parts, Body::from_stream(counted_body))
}

// ─── Server entry point ───────────────────────────────────────────────────────

/// Takes an exclusive advisory lock on `<root>/.rusts3.lock` and refuses to
/// start if another process holds it. The returned handle must stay alive
/// for the process lifetime — dropping it releases the lock.
fn acquire_process_lock(root: &FsPath) -> Result<std::fs::File, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(root)?;
    let lock_path = root.join(".rusts3.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&lock_path)?;
    let rc = unsafe { libc::flock(std::os::unix::io::AsRawFd::as_raw_fd(&file), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        return Err(format!(
            "data directory {} is locked by another rusts3 process (flock on {} failed); \
             exactly one process may own a data directory",
            root.display(),
            lock_path.display()
        )
        .into());
    }
    log::info!("acquired process lock on {}", lock_path.display());
    Ok(file)
}

/// Starts the S3 server and blocks until shutdown.
///
/// Spawns a bounded background maintenance task for targeted visibility repair,
/// staging cleanup, and trash cleanup. Responds to SIGINT (Ctrl-C) by
/// cancelling all background tasks and draining in-flight HTTP requests before
/// returning.
pub async fn serve(config: S3HttpConfig) -> Result<(), Box<dyn std::error::Error>> {
    config.app_config.validate().map_err(|message| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
    })?;
    // Exactly one process may own a data directory: the per-key in-memory
    // locks are only meaningful under that assumption.
    let _process_lock = acquire_process_lock(FsPath::new(&config.root))?;

    let store = LocalObjectStore::from_storage_config(&config.root, &config.app_config.storage);
    let shutdown = store.shutdown_token();
    let metrics = Arc::new(TrafficMetrics::default());
    // One registry of in-flight work, shared by the HTTP handlers and the
    // background jobs, so "what is the server doing right now" has one answer.
    let tasks = registry::TaskRegistry::new();

    // Buckets with a missing or legacy-schema index rebuild in the
    // background; they serve 503 SlowDown until their rebuild completes.
    // Launch a registry-tracked rebuild job for every bucket whose index is
    // missing or outdated, so a long startup rebuild is visible like any other
    // in-flight work.
    match store.buckets_needing_rebuild().await {
        Ok(buckets) => {
            for bucket in buckets {
                log::warn!("startup rebuild bucket={bucket} reason=missing_or_outdated_index");
                jobs::rebuild_index::spawn(store.clone(), bucket, tasks.clone());
            }
        }
        Err(err) => log::warn!("startup rebuild scan failed error={err}"),
    }

    // Startup intent drain: resolve every crash-leftover intent before
    // serving. O(operations in flight at crash time) — milliseconds.
    match store.list_buckets().await {
        Ok(buckets) => {
            for (bucket, _) in &buckets {
                match store.drain_intents(bucket).await {
                    Ok(0) => {}
                    Ok(n) => log::info!("startup intent drain bucket={bucket} resolved={n}"),
                    Err(StorageError::BucketRebuilding(_)) => {
                        // Rebuild produces a fresh (empty) intent table.
                    }
                    Err(err) => log::warn!("startup intent drain failed bucket={bucket} error={err}"),
                }
            }
        }
        Err(err) => log::warn!("startup intent drain failed to list buckets error={err}"),
    }

    // Cancel all background tasks on SIGINT / Ctrl-C.
    let shutdown_sig = shutdown.clone();
    tokio::spawn(async move {
        log::info!("shutdown signal watcher task started");
        tokio::signal::ctrl_c().await.ok();
        log::info!("SIGINT received — shutting down");
        shutdown_sig.cancel();
        log::info!("shutdown signal watcher task completed");
    });

    if config.app_config.logging.enable_bandwidth_report {
        let metrics_shutdown = shutdown.clone();
        let metrics_for_task = metrics.clone();
        tokio::spawn(async move {
            log::info!("traffic metrics task started interval_secs=10");
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            let mut previous = TrafficSnapshot::default();
            interval.tick().await; // discard immediate first tick
            loop {
                tokio::select! {
                    _ = metrics_shutdown.cancelled() => {
                        log::info!("traffic metrics task stopping");
                        break;
                    }
                    _ = interval.tick() => {
                        let snapshot = metrics_for_task.snapshot();
                        let delta = snapshot.saturating_sub(previous);
                        previous = snapshot;
                        log::info!(
                            "traffic bytes_in={} bytes_out={} bytes_in_rate={}/s bytes_out_rate={}/s",
                            human_bytes(snapshot.bytes_in),
                            human_bytes(snapshot.bytes_out),
                            human_bytes(delta.bytes_in / 10),
                            human_bytes(delta.bytes_out / 10),
                        );
                        log::info!(
                            "traffic requests total={} get={} put={} head={} delete={} post={} other={} total_qps={}",
                            snapshot.request_total(),
                            snapshot.get_requests,
                            snapshot.put_requests,
                            snapshot.head_requests,
                            snapshot.delete_requests,
                            snapshot.post_requests,
                            snapshot.other_requests,
                            qps(delta.request_total()),
                        );
                    }
                }
            }
        });
    } else {
        log::info!("traffic metrics task disabled by config");
    }

    // Maintenance jobs: each single-purpose job runs on its own interval in its
    // own loop, so they never overlap (a loop awaits its run before the next
    // tick) and can be tuned to different frequencies. Each gets a cancellation
    // token that is a child of the process shutdown token, so a run can be
    // cancelled centrally (or all at once at shutdown), and each registers in
    // the task registry while it runs.
    spawn_maintenance_jobs(
        store.clone(),
        config.app_config.sweeper.clone(),
        shutdown.clone(),
        tasks.clone(),
    );

    // IAM store (admin.sqlite at the data root, outside any bucket) backs
    // runtime-managed users/keys/policies; built-in admin users come from
    // the config and are immutable at runtime.
    let iam = IamStore::open(FsPath::new(&config.root)).await?;
    // Pre-mint the hidden console signing key (`RSWEB_…`) for every built-in
    // admin at startup, via the same idempotent get-or-create path IAM users
    // hit lazily. Guarantees root always has a stable key for share links,
    // rather than creating it on the first presign. The secret is never exposed.
    for user in &config.app_config.auth.users {
        if let Err(err) = iam.web_key_for(&user.user, true).await {
            log::warn!(
                "could not pre-mint console signing key for built-in user {}: {err}",
                user.user
            );
        }
    }
    let auth_state = AuthState {
        config: config.app_config.clone(),
        iam: Some(iam.clone()),
    };

    // Management UI on its own port: web logins (user/password) only —
    // completely separate from the access-key-authenticated S3 API.
    if config.app_config.ui.enabled {
        let ui_state = ui::UiState {
            store: store.clone(),
            iam,
            config: config.app_config.clone(),
            metrics: metrics.clone(),
            tasks: tasks.clone(),
        };
        let ui_bind = format!(
            "{}:{}",
            config
                .app_config
                .ui
                .bind_address
                .clone()
                .unwrap_or_else(|| config.address.ip().to_string()),
            config.app_config.ui.bind_port
        );
        let ui_listener = tokio::net::TcpListener::bind(&ui_bind).await?;
        let ui_shutdown = shutdown.clone();
        log::info!("management UI listening on {ui_bind}");
        tokio::spawn(async move {
            if let Err(err) = axum::serve(ui_listener, ui::router(ui_state))
                .with_graceful_shutdown(ui_shutdown.cancelled_owned())
                .await
            {
                log::error!("management UI server error: {err}");
            }
        });
    }

    let app = router_with_metrics(store, auth_state, metrics, tasks);
    let listener = tokio::net::TcpListener::bind(config.address).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await?;
    log::info!("rusts3-v2 shutdown complete");
    Ok(())
}

/// Spawns one scheduler loop per maintenance job. Each loop runs its job on an
/// interval, never overlapping (it awaits each run before the next tick), under
/// a cancellation token that is a child of the process shutdown token — so the
/// whole set stops at shutdown and each job can later be cancelled on its own.
fn spawn_maintenance_jobs(
    store: LocalObjectStore,
    cfg: self::config::SweeperConfig,
    shutdown: tokio_util::sync::CancellationToken,
    tasks: Arc<registry::TaskRegistry>,
) {
    macro_rules! spawn_job {
        ($name:expr, $run:path) => {{
            let store = store.clone();
            let cfg = cfg.clone();
            let cancel = shutdown.child_token();
            let tasks = tasks.clone();
            tokio::spawn(async move {
                log::info!("job scheduler started job={} interval_secs={}", $name, cfg.interval_secs);
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
                    cfg.interval_secs.max(1),
                ));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            log::info!("job scheduler stopping job={}", $name);
                            break;
                        }
                        _ = interval.tick() => {
                            let run_id = new_request_id();
                            $run(&store, &cfg, &cancel, &tasks, &run_id).await;
                        }
                    }
                }
            });
        }};
    }
    spawn_job!("resolve_intents", jobs::resolve_intents::run_once);
    spawn_job!("delete_staging", jobs::delete_staging::run_once);
    spawn_job!("delete_trash", jobs::delete_trash::run_once);

    // Layout migration: relocate legacy 4-level objects to the single-level
    // layout. Its own loop (starts immediately); each run drains all remaining
    // legacy objects, then idles. Once nothing is left it finds no work.
    {
        let store = store.clone();
        let cancel = shutdown.child_token();
        let tasks = tasks.clone();
        let idle_secs = cfg.interval_secs.max(1);
        tokio::spawn(async move {
            log::info!(
                "job scheduler started job=migrate_layout interval_secs={idle_secs} (starts immediately)"
            );
            loop {
                if cancel.is_cancelled() {
                    log::info!("job scheduler stopping job=migrate_layout");
                    break;
                }
                let run_id = new_request_id();
                jobs::migrate_layout::run_once(&store, &cancel, &tasks, &run_id).await;
                tokio::select! {
                    _ = cancel.cancelled() => {
                        log::info!("job scheduler stopping job=migrate_layout");
                        break;
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(idle_secs)) => {}
                }
            }
        });
    }

    // Empty-directory reclaimer: its own self-paced loop rather than the fixed
    // scheduler above. It starts immediately, runs a full drain (which itself
    // re-runs passes until quiescent), then idles for `reclaim_interval_secs`.
    {
        let store = store.clone();
        let cancel = shutdown.child_token();
        let tasks = tasks.clone();
        let idle_secs = cfg.reclaim_interval_secs.max(1);
        tokio::spawn(async move {
            log::info!(
                "job scheduler started job=reclaim_dirs interval_secs={idle_secs} (starts immediately)"
            );
            loop {
                if cancel.is_cancelled() {
                    log::info!("job scheduler stopping job=reclaim_dirs");
                    break;
                }
                let run_id = new_request_id();
                jobs::reclaim::run_once(&store, &cancel, &tasks, &run_id).await;
                tokio::select! {
                    _ = cancel.cancelled() => {
                        log::info!("job scheduler stopping job=reclaim_dirs");
                        break;
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(idle_secs)) => {}
                }
            }
        });
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn qps(requests_in_window: u64) -> String {
    format!("{:.2}", requests_in_window as f64 / 10.0)
}

async fn list_buckets(State(store): State<LocalObjectStore>) -> Response {
    match store.list_buckets().await {
        Ok(buckets) => {
            let bucket_count = buckets.len();
            let buckets = buckets
                .into_iter()
                .map(|(name, meta)| BucketListEntry {
                    name,
                    created_at_ms: meta.created_at_ms,
                })
                .collect::<Vec<_>>();
            with_measure(
                xml_response(StatusCode::OK, list_buckets_xml(&buckets)),
                OperationMeasure::Buckets(bucket_count),
            )
        }
        Err(err) => s3_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            err.to_string(),
            "/",
        ),
    }
}

async fn bucket_route(
    State(store): State<LocalObjectStore>,
    Path(bucket): Path<String>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    method: Method,
    request_id: Option<Extension<RequestId>>,
    identity: Option<Extension<Identity>>,
    auth_state: Option<Extension<AuthState>>,
    tasks: Option<Extension<Arc<registry::TaskRegistry>>>,
    body: Body,
) -> Response {
    let query = parse_s3_query(raw_query.as_deref().unwrap_or(""));
    let ctx = handlers::BucketCtx {
        request_id: request_id.map(|Extension(id)| id.0).unwrap_or_default(),
        identity: identity.map(|Extension(id)| id),
        auth_state: auth_state.map(|Extension(state)| state),
        tasks: tasks.map(|Extension(tasks)| tasks),
        bucket,
        query,
        headers,
        method: method.clone(),
    };
    log::debug!(
        "[{}] handler bucket method={} {}",
        ctx.request_id,
        ctx.method,
        ctx.resource()
    );
    match method {
        Method::HEAD => handlers::head_bucket::handle(store, ctx, body).await,
        Method::PUT => handlers::create_bucket::handle(store, ctx, body).await,
        Method::DELETE => handlers::delete_bucket::handle(store, ctx, body).await,
        Method::GET if ctx.query.contains_key("location") => {
            handlers::get_bucket_location::handle(store, ctx, body).await
        }
        Method::GET if ctx.query.contains_key("uploads") => {
            handlers::list_uploads::handle(store, ctx, body).await
        }
        Method::GET if ctx.query.contains_key("versions") => {
            handlers::list_versions::handle(store, ctx, body).await
        }
        Method::GET if known_unimplemented_bucket_query(&ctx.query) => s3_error(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "This bucket operation is not implemented",
            &ctx.bucket,
        ),
        Method::GET => handlers::list_objects::handle(store, ctx, body).await,
        Method::POST if ctx.query.contains_key("delete") => {
            handlers::delete_objects::handle(store, ctx, body).await
        }
        Method::POST if ctx.query.contains_key("rebuildIndex") => {
            handlers::rebuild_index::handle(store, ctx, body).await
        }
        Method::POST if handlers::browser_post::is_form(&ctx.headers) => {
            handlers::browser_post::handle(store, ctx, body).await
        }
        Method::POST => s3_error(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "This bucket operation is not implemented",
            &ctx.resource(),
        ),
        _ => s3_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "The specified method is not allowed",
            &ctx.bucket,
        ),
    }
}

async fn object_route(
    State(store): State<LocalObjectStore>,
    Path((bucket, key)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    method: Method,
    request_id: Option<Extension<RequestId>>,
    identity: Option<Extension<Identity>>,
    body: Body,
) -> Response {
    let query = parse_s3_query(raw_query.as_deref().unwrap_or(""));
    let ctx = handlers::ObjectCtx {
        request_id: request_id.map(|Extension(id)| id.0).unwrap_or_default(),
        identity: identity.map(|Extension(id)| id),
        bucket,
        key,
        query,
        headers,
        method: method.clone(),
    };
    log::debug!(
        "[{}] handler object method={} {}",
        ctx.request_id,
        ctx.method,
        ctx.resource()
    );
    // Resolve the verb once, then hand the whole context to exactly one
    // self-contained handler module.
    let has_upload_id = ctx.query.contains_key("uploadId");
    let has_part_number = ctx.query.contains_key("partNumber");
    let has_uploads = ctx.query.contains_key("uploads");
    let is_copy = ctx.headers.contains_key("x-amz-copy-source");
    match method {
        Method::PUT if has_upload_id && has_part_number && is_copy => {
            handlers::copy_part::handle(store, ctx, body).await
        }
        Method::PUT if has_upload_id && has_part_number => {
            handlers::put_part::handle(store, ctx, body).await
        }
        Method::PUT if is_copy => handlers::copy_object::handle(store, ctx, body).await,
        Method::PUT => handlers::put_object::handle(store, ctx, body).await,
        Method::GET if has_upload_id => handlers::list_parts::handle(store, ctx, body).await,
        Method::GET | Method::HEAD => handlers::get_object::handle(store, ctx, body).await,
        Method::DELETE if has_upload_id => {
            handlers::abort_multipart::handle(store, ctx, body).await
        }
        Method::DELETE => handlers::delete_object::handle(store, ctx, body).await,
        Method::POST if has_uploads => handlers::create_multipart::handle(store, ctx, body).await,
        Method::POST if has_upload_id => {
            handlers::complete_multipart::handle(store, ctx, body).await
        }
        Method::POST => s3_error(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "This object operation is not implemented",
            &ctx.resource(),
        ),
        _ => s3_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "The specified method is not allowed",
            &ctx.resource(),
        ),
    }
}

fn parse_complete_parts_xml(xml: &str) -> Result<Vec<CompletePartRequest>, &'static str> {
    let part_re = Regex::new(r#"(?s)<Part>\s*(.*?)\s*</Part>"#).unwrap();
    let number_re = Regex::new(r#"(?s)<PartNumber>\s*(\d+)\s*</PartNumber>"#).unwrap();
    let etag_re = Regex::new(r#"(?s)<ETag>\s*"?([^"<]+)"?\s*</ETag>"#).unwrap();
    let mut parts = Vec::new();
    for capture in part_re.captures_iter(xml) {
        let block = capture.get(1).ok_or("Invalid Part")?.as_str();
        let number = number_re
            .captures(block)
            .and_then(|v| v.get(1))
            .and_then(|v| v.as_str().parse::<u16>().ok())
            .ok_or("Invalid PartNumber")?;
        let etag = normalize_complete_etag(
            etag_re
                .captures(block)
                .and_then(|v| v.get(1))
                .ok_or("Invalid ETag")?
                .as_str(),
        );
        parts.push(CompletePartRequest { number, etag });
    }
    if parts.is_empty() {
        let flat_number_re = Regex::new(r#"(?s)<PartNumber>\s*(\d+)\s*</PartNumber>"#).unwrap();
        let flat_etag_re = Regex::new(r#"(?s)<ETag>\s*"?([^"<]+)"?\s*</ETag>"#).unwrap();
        let numbers = flat_number_re
            .captures_iter(xml)
            .filter_map(|capture| capture.get(1).and_then(|v| v.as_str().parse::<u16>().ok()))
            .collect::<Vec<_>>();
        let etags = flat_etag_re
            .captures_iter(xml)
            .filter_map(|capture| capture.get(1).map(|v| normalize_complete_etag(v.as_str())))
            .collect::<Vec<_>>();
        if numbers.len() == etags.len() && !numbers.is_empty() {
            parts = numbers
                .into_iter()
                .zip(etags)
                .map(|(number, etag)| CompletePartRequest { number, etag })
                .collect();
        }
    }
    if parts.is_empty() {
        Err("No multipart parts found")
    } else {
        Ok(parts)
    }
}

fn parse_delete_objects_xml(xml: &str) -> (Vec<String>, bool) {
    let object_re = Regex::new(r#"(?s)<Object>\s*(.*?)\s*</Object>"#).unwrap();
    let key_re = Regex::new(r#"(?s)<Key>\s*(.*?)\s*</Key>"#).unwrap();
    let quiet_re = Regex::new(r#"(?si)<Quiet>\s*(true|false)\s*</Quiet>"#).unwrap();
    let quiet = quiet_re
        .captures(xml)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let keys = object_re
        .captures_iter(xml)
        .filter_map(|c| {
            let block = c.get(1)?.as_str();
            let key = key_re.captures(block)?.get(1)?.as_str();
            Some(unescape_xml(key))
        })
        .collect();
    (keys, quiet)
}

fn unescape_xml(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn parse_copy_source(raw: &str) -> Option<(String, String)> {
    let path = percent_decode(raw.trim_start_matches('/'));
    let slash = path.find('/')?;
    let bucket = path[..slash].to_string();
    let key = path[slash + 1..].to_string();
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((bucket, key))
}

fn parse_copy_source_range(headers: &HeaderMap) -> Result<Option<(u64, u64)>, &'static str> {
    let Some(value) = headers
        .get("x-amz-copy-source-range")
        .and_then(|v| v.to_str().ok())
    else {
        return Ok(None);
    };
    let Some(range) = value.strip_prefix("bytes=") else {
        return Err("Invalid x-amz-copy-source-range");
    };
    let Some((start, end)) = range.split_once('-') else {
        return Err("Invalid x-amz-copy-source-range");
    };
    let start = start
        .parse::<u64>()
        .map_err(|_| "Invalid x-amz-copy-source-range")?;
    let end = end
        .parse::<u64>()
        .map_err(|_| "Invalid x-amz-copy-source-range")?;
    if start > end {
        return Err("Invalid x-amz-copy-source-range");
    }
    Ok(Some((start, end)))
}

fn copy_source_preconditions_match(
    headers: &HeaderMap,
    source_etag: &str,
    source_last_modified_ms: i64,
) -> bool {
    if let Some(value) = headers
        .get("x-amz-copy-source-if-match")
        .and_then(|v| v.to_str().ok())
    {
        if !etag_condition_contains(value, source_etag) {
            return false;
        }
    }

    if let Some(value) = headers
        .get("x-amz-copy-source-if-none-match")
        .and_then(|v| v.to_str().ok())
    {
        if etag_condition_contains(value, source_etag) {
            return false;
        }
    }

    if let Some(value) = headers
        .get("x-amz-copy-source-if-unmodified-since")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(since_ms) = parse_http_date_ms(value) {
            if source_last_modified_ms > since_ms {
                return false;
            }
        }
    }

    if let Some(value) = headers
        .get("x-amz-copy-source-if-modified-since")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(since_ms) = parse_http_date_ms(value) {
            if source_last_modified_ms <= since_ms {
                return false;
            }
        }
    }

    true
}

fn etag_condition_contains(value: &str, etag: &str) -> bool {
    let etag = etag.trim().trim_matches('"');
    value.split(',').any(|candidate| {
        let candidate = candidate.trim().trim_matches('"');
        candidate == "*" || candidate == etag
    })
}

fn percent_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b as char);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn parse_s3_query(raw: &str) -> HashMap<String, String> {
    raw.split('&')
        .filter(|part| !part.is_empty())
        .filter_map(|part| {
            let mut it = part.splitn(2, '=');
            let key = it.next()?;
            let value = it.next().unwrap_or("");
            Some((percent_decode(key), percent_decode(value)))
        })
        .collect()
}

fn normalize_complete_etag(value: &str) -> String {
    value
        .trim()
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .trim_matches('"')
        .to_string()
}

fn is_aws_chunked(headers: &HeaderMap) -> bool {
    headers
        .get("x-amz-decoded-content-length")
        .and_then(|v| v.to_str().ok())
        .is_some()
        || headers
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_ascii_lowercase().contains("aws-chunked"))
            .unwrap_or(false)
}

fn object_content_encoding(headers: &HeaderMap) -> Option<String> {
    let value = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())?;
    let encodings = value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .filter(|v| !v.eq_ignore_ascii_case("aws-chunked"))
        .collect::<Vec<_>>();
    if encodings.is_empty() {
        None
    } else {
        Some(encodings.join(", "))
    }
}

fn storage_class_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-amz-storage-class")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

/// Returns an `InvalidStorageClass` error response when the client supplied a
/// storage class that is not one of the known S3 values; otherwise `None`.
fn reject_invalid_storage_class(value: Option<&str>, resource: &str) -> Option<Response> {
    if is_valid_storage_class(value) {
        None
    } else {
        Some(s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidStorageClass",
            "The storage class you specified is not valid",
            resource,
        ))
    }
}

fn extract_user_meta(headers: &HeaderMap) -> Result<BTreeMap<String, String>, String> {
    let mut meta = BTreeMap::new();
    for (name, value) in headers {
        let Some(key) = name.as_str().strip_prefix("x-amz-meta-") else {
            continue;
        };
        if key.is_empty() {
            return Err("User metadata key must not be empty".to_string());
        }
        let value = value
            .to_str()
            .map_err(|_| "User metadata values must be valid UTF-8".to_string())?;
        meta.insert(key.to_ascii_lowercase(), value.to_string());
    }

    let total = meta
        .iter()
        .map(|(key, value)| key.as_bytes().len() + value.as_bytes().len())
        .sum::<usize>();
    if total > MAX_USER_META_BYTES {
        return Err(format!(
            "User metadata size exceeds {MAX_USER_META_BYTES} bytes"
        ));
    }
    Ok(meta)
}

fn expected_payload_sha256(headers: &HeaderMap, aws_chunked: bool) -> Option<String> {
    if aws_chunked {
        return None;
    }
    let value = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())?;
    if value.eq_ignore_ascii_case("UNSIGNED-PAYLOAD") || value.starts_with("STREAMING-") {
        return None;
    }
    Some(value.to_ascii_lowercase())
}

fn known_unimplemented_bucket_query(query: &HashMap<String, String>) -> bool {
    [
        "versioning",
        "cors",
        "website",
        "lifecycle",
        "policy",
        "acl",
        "tagging",
    ]
    .iter()
    .any(|k| query.contains_key(*k))
}

fn storage_error_response(err: StorageError, resource: &str) -> Response {
    match err {
        StorageError::BucketNotFound(_) => s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "The specified bucket does not exist",
            resource,
        ),
        StorageError::ObjectNotFound { .. } => s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "The specified key does not exist",
            resource,
        ),
        StorageError::NoSuchUpload(_) => s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchUpload",
            "The specified multipart upload does not exist",
            resource,
        ),
        StorageError::InvalidBucketName(_)
        | StorageError::InvalidObjectKey(_)
        | StorageError::InvalidStagingId(_)
        | StorageError::InvalidMultipartUpload(_) => s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            err.to_string(),
            resource,
        ),
        StorageError::EntityTooSmall(_) => s3_error(
            StatusCode::BAD_REQUEST,
            "EntityTooSmall",
            err.to_string(),
            resource,
        ),
        StorageError::PayloadHashMismatch { .. } => s3_error(
            StatusCode::BAD_REQUEST,
            "XAmzContentSHA256Mismatch",
            err.to_string(),
            resource,
        ),
        StorageError::CorruptObject(_) => s3_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            err.to_string(),
            resource,
        ),
        StorageError::BucketRebuilding(_) => {
            let mut response = s3_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "SlowDown",
                "Bucket index rebuild in progress; retry shortly",
                resource,
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
            response
        }
        _ => s3_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            err.to_string(),
            resource,
        ),
    }
}

fn s3_error(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
    resource: &str,
) -> Response {
    let body = error_xml(&S3ErrorXml {
        code: code.to_string(),
        message: message.into(),
        resource: resource.to_string(),
        request_id: "rust-s3-server".to_string(),
    });
    xml_response(status, body)
}

fn xml_response(status: StatusCode, body: String) -> Response {
    let content_length = body.len();
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string()).unwrap(),
    );
    response.headers_mut().insert(
        "x-amz-request-id",
        HeaderValue::from_static("rust-s3-server"),
    );
    response
}

fn with_measure(mut response: Response, measure: OperationMeasure) -> Response {
    response.extensions_mut().insert(measure);
    response
}

fn empty_response(status: StatusCode) -> Response {
    let mut response = status.into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    response.headers_mut().insert(
        "x-amz-request-id",
        HeaderValue::from_static("rust-s3-server"),
    );
    response
}

fn empty_response_with_etag(status: StatusCode, etag: &str) -> Response {
    let mut response = empty_response(status);
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&quote_etag(etag)).unwrap(),
    );
    response
}

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod tests;
