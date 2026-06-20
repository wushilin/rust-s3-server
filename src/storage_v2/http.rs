//! Axum HTTP layer: routing, middleware, and request/response handling.
//!
//! Entry point: [`serve`] starts the HTTP server.  [`router`] builds the
//! Axum [`Router`] for use in integration tests.

use std::collections::HashMap;
use std::io::SeekFrom;
use std::net::SocketAddr;
use std::path::Path as FsPath;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use axum::body::{to_bytes, Body};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;
use futures::TryStreamExt;
use regex::Regex;

use super::auth::auth_middleware;
use super::config::AppConfig;
use super::errors::StorageError;
use super::metadata::quote_etag;
use super::range::{parse_range_header, RangeSelection};
use super::store::{CompletePartRequest, LocalObjectStore};
use super::sweeper::{sweep_bucket, SweepConfig};
use super::time::http_date_ms;
use super::xml::{
    complete_multipart_xml, error_xml, initiate_multipart_xml, list_buckets_xml,
    list_objects_v1_xml, list_objects_v2_xml, BucketListEntry, S3ErrorXml,
};

const STREAM_CHUNK_SIZE: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub struct S3HttpConfig {
    pub address: SocketAddr,
    pub root: String,
    pub app_config: Arc<AppConfig>,
}

#[derive(Debug, Default)]
struct TrafficMetrics {
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
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
    router_with_metrics(store, app_config, Arc::new(TrafficMetrics::default()))
}

fn router_with_metrics(
    store: LocalObjectStore,
    app_config: Arc<AppConfig>,
    metrics: Arc<TrafficMetrics>,
) -> Router {
    Router::new()
        .route("/", get(list_buckets))
        .route("/:bucket", any(bucket_route))
        .route("/:bucket/*key", any(object_route))
        .layer(middleware::from_fn_with_state(
            app_config.clone(),
            auth_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            metrics,
            traffic_metrics_middleware,
        ))
        .layer(middleware::from_fn(log_middleware))
        .layer(DefaultBodyLimit::max(5 * 1024 * 1024 * 1024))
        .with_state(store)
}

// ─── Logging middleware ───────────────────────────────────────────────────────

async fn log_middleware(request: Request<Body>, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let start = std::time::Instant::now();
    let response = next.run(request).await;
    let elapsed_ms = start.elapsed().as_millis();
    let status = response.status();
    let content_length = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");
    if status.is_client_error() || status.is_server_error() {
        log::warn!("{method} {uri} {status} size={content_length} {elapsed_ms}ms");
    } else {
        log::info!("{method} {uri} {status} size={content_length} {elapsed_ms}ms");
    }
    response
}

async fn traffic_metrics_middleware(
    State(metrics): State<Arc<TrafficMetrics>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let (parts, body) = request.into_parts();
    metrics.add_request(&parts.method);
    let counted_metrics = metrics.clone();
    let counted_body = body.into_data_stream().inspect_ok(move |bytes| {
        counted_metrics.add_in(bytes.len() as u64);
    });
    let request = Request::from_parts(parts, Body::from_stream(counted_body));

    let response = next.run(request).await;
    let (parts, body) = response.into_parts();
    let counted_metrics = metrics.clone();
    let counted_body = body.into_data_stream().inspect_ok(move |bytes| {
        counted_metrics.add_out(bytes.len() as u64);
    });
    Response::from_parts(parts, Body::from_stream(counted_body))
}

// ─── Server entry point ───────────────────────────────────────────────────────

/// Starts the S3 server and blocks until shutdown.
///
/// Spawns a background sweeper task that cleans up abandoned staging
/// directories every 5 minutes.  Responds to SIGINT (Ctrl-C) by cancelling
/// all background tasks and draining in-flight HTTP requests before returning.
pub async fn serve(config: S3HttpConfig) -> Result<(), Box<dyn std::error::Error>> {
    let store = LocalObjectStore::from_storage_config(&config.root, &config.app_config.storage);
    let shutdown = store.shutdown_token();
    let metrics = Arc::new(TrafficMetrics::default());

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

    // Sweeper task: runs on a configurable interval, exits cooperatively on shutdown.
    // Per-bucket cursors ensure we resume from where we left off across ticks so
    // large buckets are swept incrementally rather than only at the front of the list.
    let sweeper_store = store.clone();
    let sweeper_shutdown = shutdown.clone();
    let sweeper_cfg = config.app_config.sweeper.clone();
    tokio::spawn(async move {
        log::info!(
            "sweeper task started interval_secs={} max_objects_per_pass={} orphan_grace_period_secs={} staging_expiry_secs={}",
            sweeper_cfg.interval_secs,
            sweeper_cfg.max_objects_per_pass,
            sweeper_cfg.orphan_grace_period_secs,
            sweeper_cfg.staging_expiry_secs,
        );
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(sweeper_cfg.interval_secs));
        interval.tick().await; // discard the immediate first tick
        let mut cursors: std::collections::HashMap<String, Option<String>> =
            std::collections::HashMap::new();
        loop {
            tokio::select! {
                _ = sweeper_shutdown.cancelled() => {
                    log::info!("sweeper task stopping");
                    break;
                }
                _ = interval.tick() => {
                    log::info!("sweeper tick triggered");
                    match sweeper_store.list_buckets().await {
                        Ok(buckets) => {
                        log::info!("sweeper pass started buckets={}", buckets.len());
                        if buckets.is_empty() {
                            log::info!("sweeper pass complete buckets=0");
                        }
                        // Drop cursors for buckets that no longer exist.
                        let names: std::collections::HashSet<&str> =
                            buckets.iter().map(|(b, _)| b.as_str()).collect();
                        cursors.retain(|k, _| names.contains(k.as_str()));

                        for (bucket, _) in &buckets {
                            if sweeper_shutdown.is_cancelled() { break; }
                            let after = cursors
                                .get(bucket.as_str())
                                .and_then(|v| v.as_deref());
                            log::info!(
                                "sweeper bucket started bucket={} after={}",
                                bucket,
                                after.unwrap_or("-"),
                            );
                            let cfg = SweepConfig {
                                max_objects: sweeper_cfg.max_objects_per_pass,
                                orphan_grace_period_ms: sweeper_cfg.orphan_grace_period_secs as i64 * 1000,
                                staging_expiry_ms: sweeper_cfg.staging_expiry_secs as i64 * 1000,
                                now_ms: super::time::now_ms(),
                            };
                            match sweep_bucket(&sweeper_store, bucket, &cfg, after).await {
                                Ok(stats) => {
                                    let removed = stats.sqlite_orphans_removed
                                        + stats.physical_orphans_removed
                                        + stats.staging_dirs_removed;
                                    if removed > 0 {
                                        log::info!(
                                            "sweeper {bucket}: sqlite_orphans={} physical_orphans={} staging={}",
                                            stats.sqlite_orphans_removed,
                                            stats.physical_orphans_removed,
                                            stats.staging_dirs_removed,
                                        );
                                    }
                                    if stats.pass_complete {
                                        log::info!(
                                            "sweeper bucket complete bucket={} sqlite_orphans={} physical_orphans={} staging={} pass_complete=true",
                                            bucket,
                                            stats.sqlite_orphans_removed,
                                            stats.physical_orphans_removed,
                                            stats.staging_dirs_removed,
                                        );
                                        cursors.remove(bucket.as_str());
                                    } else if let Some(key) = stats.last_sqlite_key {
                                        log::info!(
                                            "sweeper bucket yielded bucket={} sqlite_orphans={} physical_orphans={} staging={} next_after={}",
                                            bucket,
                                            stats.sqlite_orphans_removed,
                                            stats.physical_orphans_removed,
                                            stats.staging_dirs_removed,
                                            key,
                                        );
                                        cursors.insert(bucket.clone(), Some(key));
                                    }
                                }
                                Err(err) => log::warn!("sweeper {bucket}: {err}"),
                            }
                        }
                        log::info!(
                            "sweeper pass rescheduled interval_secs={}",
                            sweeper_cfg.interval_secs,
                        );
                        }
                        Err(err) => {
                            log::warn!("sweeper pass failed to list buckets error={err}");
                            log::info!(
                                "sweeper pass rescheduled interval_secs={}",
                                sweeper_cfg.interval_secs,
                            );
                        }
                    }
                }
            }
        }
    });

    let app = router_with_metrics(store, config.app_config, metrics);
    let listener = tokio::net::TcpListener::bind(config.address).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await?;
    log::info!("rusts3-v2 shutdown complete");
    Ok(())
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
            let buckets = buckets
                .into_iter()
                .map(|(name, meta)| BucketListEntry {
                    name,
                    created_at_ms: meta.created_at_ms,
                })
                .collect::<Vec<_>>();
            xml_response(StatusCode::OK, list_buckets_xml(&buckets))
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
    Query(query): Query<HashMap<String, String>>,
    method: Method,
) -> Response {
    match method {
        Method::HEAD => {
            if store.bucket_exists(&bucket).await {
                empty_response(StatusCode::OK)
            } else {
                s3_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchBucket",
                    "The specified bucket does not exist",
                    &format!("/{bucket}"),
                )
            }
        }
        Method::PUT => match store.create_bucket(&bucket).await {
            Ok(()) => empty_response(StatusCode::OK),
            Err(err) => storage_error_response(err, &format!("/{bucket}")),
        },
        Method::DELETE => match store.delete_bucket(&bucket).await {
            Ok(()) => empty_response(StatusCode::NO_CONTENT),
            Err(StorageError::InvalidMultipartUpload(_)) => s3_error(
                StatusCode::CONFLICT,
                "BucketNotEmpty",
                "Bucket is not empty",
                &bucket,
            ),
            Err(err) => storage_error_response(err, &format!("/{bucket}")),
        },
        Method::GET => {
            if query.contains_key("location") {
                if !store.bucket_exists(&bucket).await {
                    return storage_error_response(
                        StorageError::BucketNotFound(bucket.clone()),
                        &format!("/{bucket}"),
                    );
                }
                // Return an empty LocationConstraint (meaning "default region / us-east-1").
                // Returning a real AWS region name (e.g. "ap-southeast-1") causes some S3
                // clients (mc) to redirect subsequent requests to that AWS regional endpoint,
                // where the bucket obviously doesn't exist.
                return xml_response(
                    StatusCode::OK,
                    r#"<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>"#.to_string(),
                );
            }
            if known_unimplemented_bucket_query(&query) {
                return s3_error(
                    StatusCode::NOT_IMPLEMENTED,
                    "NotImplemented",
                    "This bucket operation is not implemented",
                    &bucket,
                );
            }
            list_objects_response(store, bucket, query).await
        }
        Method::POST => {
            if query.contains_key("rebuildIndex") {
                if store.start_rebuild_background(&bucket) {
                    log::info!("rebuild_sqlite started bucket={bucket}");
                    empty_response(StatusCode::ACCEPTED)
                } else {
                    s3_error(
                        StatusCode::CONFLICT,
                        "RebuildInProgress",
                        "A rebuild is already running for this bucket",
                        &format!("/{bucket}"),
                    )
                }
            } else {
                s3_error(
                    StatusCode::NOT_IMPLEMENTED,
                    "NotImplemented",
                    "This bucket operation is not implemented",
                    &format!("/{bucket}"),
                )
            }
        }
        _ => s3_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "The specified method is not allowed",
            &bucket,
        ),
    }
}

async fn object_route(
    State(store): State<LocalObjectStore>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    method: Method,
    body: Body,
) -> Response {
    match method {
        Method::PUT => {
            if let (Some(upload_id), Some(part_number)) =
                (query.get("uploadId"), query.get("partNumber"))
            {
                let part_number = match part_number.parse::<u16>() {
                    Ok(v) => v,
                    Err(_) => {
                        return s3_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidArgument",
                            "Invalid partNumber",
                            &key,
                        )
                    }
                };
                let aws_chunked = is_aws_chunked(&headers);
                let expected_sha256 = expected_payload_sha256(&headers, aws_chunked);
                match store
                    .put_multipart_part_stream(
                        &bucket,
                        &key,
                        upload_id,
                        part_number,
                        body.into_data_stream(),
                        aws_chunked,
                        expected_sha256.as_deref(),
                    )
                    .await
                {
                    Ok(result) => empty_response_with_etag(StatusCode::OK, &result.etag),
                    Err(err) => storage_error_response(err, &format!("/{bucket}/{key}")),
                }
            } else {
                let aws_chunked = is_aws_chunked(&headers);
                let content_type = headers
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok());
                let expected_sha256 = expected_payload_sha256(&headers, aws_chunked);
                match store
                    .put_object_stream(
                        &bucket,
                        &key,
                        body.into_data_stream(),
                        content_type,
                        aws_chunked,
                        expected_sha256.as_deref(),
                    )
                    .await
                {
                    Ok(result) => empty_response_with_etag(StatusCode::OK, &result.etag),
                    Err(err) => storage_error_response(err, &format!("/{bucket}/{key}")),
                }
            }
        }
        Method::GET | Method::HEAD => get_or_head_object(store, bucket, key, headers, method).await,
        Method::DELETE => {
            if let Some(upload_id) = query.get("uploadId") {
                match store.abort_multipart(&bucket, &key, upload_id).await {
                    Ok(()) => empty_response(StatusCode::NO_CONTENT),
                    Err(err) => storage_error_response(err, &format!("/{bucket}/{key}")),
                }
            } else {
                match store.delete_object(&bucket, &key).await {
                    Ok(()) => empty_response(StatusCode::NO_CONTENT),
                    Err(err) => storage_error_response(err, &format!("/{bucket}/{key}")),
                }
            }
        }
        Method::POST => {
            if query.contains_key("uploads") {
                let content_type = headers
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok());
                match store.initiate_multipart(&bucket, &key, content_type).await {
                    Ok(upload_id) => xml_response(
                        StatusCode::OK,
                        initiate_multipart_xml(&bucket, &key, &upload_id),
                    ),
                    Err(err) => storage_error_response(err, &format!("/{bucket}/{key}")),
                }
            } else if let Some(upload_id) = query.get("uploadId") {
                let body = match to_bytes(body, 1024 * 1024).await {
                    Ok(body) => body,
                    Err(err) => {
                        return s3_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidRequest",
                            err.to_string(),
                            &format!("/{bucket}/{key}"),
                        )
                    }
                };
                let parts = match parse_complete_parts_xml(&String::from_utf8_lossy(&body)) {
                    Ok(parts) => parts,
                    Err(message) => {
                        return s3_error(
                            StatusCode::BAD_REQUEST,
                            "MalformedXML",
                            message,
                            &format!("/{bucket}/{key}"),
                        )
                    }
                };
                match store
                    .complete_multipart(&bucket, &key, upload_id, &parts)
                    .await
                {
                    Ok(result) => {
                        let host = headers
                            .get(header::HOST)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("127.0.0.1");
                        let location = format!("http://{host}/{bucket}/{key}");
                        xml_response(
                            StatusCode::OK,
                            complete_multipart_xml(&location, &bucket, &key, &result.etag),
                        )
                    }
                    Err(err) => storage_error_response(err, &format!("/{bucket}/{key}")),
                }
            } else {
                s3_error(
                    StatusCode::NOT_IMPLEMENTED,
                    "NotImplemented",
                    "This object operation is not implemented",
                    &format!("/{bucket}/{key}"),
                )
            }
        }
        _ => s3_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "The specified method is not allowed",
            &format!("/{bucket}/{key}"),
        ),
    }
}

async fn list_objects_response(
    store: LocalObjectStore,
    bucket: String,
    query: HashMap<String, String>,
) -> Response {
    let prefix = query.get("prefix").map(String::as_str).unwrap_or("");
    let delimiter = query.get("delimiter").map(String::as_str);
    let max_keys = query
        .get("max-keys")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000)
        .min(1000);

    // ListObjectsV2 is identified by list-type=2, or by the use of v2-only params.
    // ListObjectsV1 uses `marker`; without any of these the SDK is calling v1.
    let is_v2 = query.get("list-type").map(|v| v == "2").unwrap_or(false)
        || query.contains_key("continuation-token")
        || query.contains_key("start-after");

    let after = if is_v2 {
        query
            .get("continuation-token")
            .or_else(|| query.get("start-after"))
            .map(String::as_str)
    } else {
        query.get("marker").map(String::as_str)
    };

    match store
        .list_objects(&bucket, prefix, delimiter, after, max_keys)
        .await
    {
        Ok(page) => {
            let object_count = page.entries.len() + page.common_prefixes.len();
            log::info!(
                "LIST /{bucket} v={} prefix={prefix:?} after={after:?} returned={object_count} truncated={}",
                if is_v2 { 2 } else { 1 },
                page.is_truncated
            );
            let xml = if is_v2 {
                list_objects_v2_xml(
                    &bucket,
                    prefix,
                    delimiter,
                    query.get("continuation-token").map(String::as_str),
                    query.get("start-after").map(String::as_str),
                    max_keys,
                    &page,
                )
            } else {
                list_objects_v1_xml(
                    &bucket,
                    prefix,
                    delimiter,
                    query.get("marker").map(String::as_str),
                    max_keys,
                    &page,
                )
            };
            xml_response(StatusCode::OK, xml)
        }
        Err(err) => storage_error_response(err, &format!("/{bucket}")),
    }
}

async fn get_or_head_object(
    store: LocalObjectStore,
    bucket: String,
    key: String,
    headers: HeaderMap,
    method: Method,
) -> Response {
    let object = match store.read_object(&bucket, &key).await {
        Ok(object) => object,
        Err(err) => return storage_error_response(err, &format!("/{bucket}/{key}")),
    };
    let total_size = object.meta.size;
    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let selection = parse_range_header(range_header, total_size);

    // Unsatisfiable range: return 416 immediately.
    if let RangeSelection::Unsatisfiable { total_size } = selection {
        let mut response = empty_response(StatusCode::RANGE_NOT_SATISFIABLE);
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes */{total_size}")).unwrap(),
        );
        return response;
    }

    let (status, range_start, range_len, content_range) = match &selection {
        RangeSelection::Full => (StatusCode::OK, 0u64, total_size, None),
        RangeSelection::Single {
            start,
            end_inclusive,
        } => (
            StatusCode::PARTIAL_CONTENT,
            *start,
            end_inclusive - start + 1,
            Some(format!("bytes {start}-{end_inclusive}/{total_size}")),
        ),
        RangeSelection::Unsatisfiable { .. } => unreachable!(),
    };

    // Extract header values before consuming object fields.
    let etag = quote_etag(&object.meta.etag);
    let content_type = object.meta.content_type.clone();
    let last_modified = http_date_ms(object.meta.last_modified_ms);

    let mut builder = Response::builder()
        .status(status)
        .header(header::ETAG, etag)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, range_len.to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::LAST_MODIFIED, last_modified)
        .header("x-amz-request-id", "rust-s3-server");
    if let Some(cr) = content_range {
        builder = builder.header(header::CONTENT_RANGE, cr);
    }

    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        match stream_object_range(
            &object.meta,
            &object.part_offsets,
            &object.object_dir,
            range_start,
            range_len,
        )
        .await
        {
            Ok(body) => body,
            Err(err) => return storage_error_response(err, &format!("/{bucket}/{key}")),
        }
    };
    builder.body(body).unwrap()
}

/// Streams `range_len` bytes starting at `range_start` from the object's part
/// files without loading the full content into memory.
///
/// For single-part objects the file is seeked to the start offset and wrapped
/// in a `ReaderStream`.  For multi-part objects the relevant file segments are
/// piped through an in-process duplex channel so the caller gets a single
/// contiguous byte stream.
async fn stream_object_range(
    meta: &super::metadata::ObjectMeta,
    part_offsets: &[u64],
    object_dir: &FsPath,
    range_start: u64,
    range_len: u64,
) -> Result<Body, StorageError> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    use tokio_util::io::ReaderStream;

    if range_len == 0 {
        return Ok(Body::empty());
    }

    // Fast path: single-part object.
    if meta.parts.len() == 1 {
        let path = object_dir.join(&meta.parts[0].file);
        match tokio::fs::File::open(&path).await {
            Ok(mut file) => {
                if range_start > 0 {
                    if file.seek(SeekFrom::Start(range_start)).await.is_err() {
                        return Err(StorageError::CorruptObject(format!(
                            "failed to seek {}",
                            path.display()
                        )));
                    }
                }
                return Ok(Body::from_stream(ReaderStream::with_capacity(
                    file.take(range_len),
                    STREAM_CHUNK_SIZE,
                )));
            }
            Err(err) => {
                return Err(StorageError::CorruptObject(format!(
                    "failed to open {}: {err}",
                    path.display()
                )))
            }
        }
    }

    // Multi-part: calculate which segments fall inside [range_start, range_end).
    let range_end = range_start + range_len;
    let mut segments: Vec<(std::path::PathBuf, u64, u64)> = Vec::new();
    for (index, part) in meta.parts.iter().enumerate() {
        let part_start = part_offsets.get(index).copied().unwrap_or(0);
        let part_end = part_start + part.size;
        if part_start >= range_end {
            break;
        }
        if part_end > range_start {
            let read_from = range_start.max(part_start);
            let read_to = range_end.min(part_end);
            let skip = read_from - part_start;
            let take = read_to - read_from;
            segments.push((object_dir.join(&part.file), skip, take));
        }
    }

    if segments.len() == 1 {
        let (path, skip, take) = segments.pop().unwrap();
        match tokio::fs::File::open(&path).await {
            Ok(mut file) => {
                if skip > 0 && file.seek(SeekFrom::Start(skip)).await.is_err() {
                    return Err(StorageError::CorruptObject(format!(
                        "failed to seek {}",
                        path.display()
                    )));
                }
                return Ok(Body::from_stream(ReaderStream::with_capacity(
                    file.take(take),
                    STREAM_CHUNK_SIZE,
                )));
            }
            Err(err) => {
                return Err(StorageError::CorruptObject(format!(
                    "failed to open {}: {err}",
                    path.display()
                )))
            }
        }
    }

    // Pipe all segments through a duplex channel so axum sees one Body stream.
    let (mut writer, reader) = tokio::io::duplex(STREAM_CHUNK_SIZE);
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        for (path, skip, take) in segments {
            match tokio::fs::File::open(&path).await {
                Ok(mut file) => {
                    if skip > 0 && file.seek(SeekFrom::Start(skip)).await.is_err() {
                        return;
                    }
                    let mut limited = file.take(take);
                    if tokio::io::copy(&mut limited, &mut writer).await.is_err() {
                        return; // reader dropped (client disconnected)
                    }
                }
                Err(_) => return,
            }
        }
    });

    Ok(Body::from_stream(ReaderStream::with_capacity(
        reader,
        STREAM_CHUNK_SIZE,
    )))
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

fn expected_payload_sha256(headers: &HeaderMap, aws_chunked: bool) -> Option<String> {
    if aws_chunked {
        return None;
    }
    let value = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())?;
    if value.eq_ignore_ascii_case("UNSIGNED-PAYLOAD")
        || value.starts_with("STREAMING-")
        || value.len() != 64
    {
        return None;
    }
    Some(value.to_ascii_lowercase())
}

fn known_unimplemented_bucket_query(query: &HashMap<String, String>) -> bool {
    [
        "uploads",
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
        | StorageError::InvalidMultipartUpload(_)
        | StorageError::PhysicalIdTooLong { .. } => s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
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
mod integration_tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::storage_v2::store::LocalObjectStore;

    use super::{router, router_with_metrics, TrafficMetrics};

    fn make_app(tmp: &tempfile::TempDir) -> axum::Router {
        router(
            LocalObjectStore::new(tmp.path()),
            std::sync::Arc::new(super::super::config::AppConfig::default()),
        )
    }

    fn make_app_with_store(tmp: &tempfile::TempDir) -> (axum::Router, LocalObjectStore) {
        let store = LocalObjectStore::new(tmp.path());
        (
            router(
                store.clone(),
                std::sync::Arc::new(super::super::config::AppConfig::default()),
            ),
            store,
        )
    }

    fn make_app_with_metrics(
        tmp: &tempfile::TempDir,
    ) -> (axum::Router, std::sync::Arc<TrafficMetrics>) {
        let metrics = std::sync::Arc::new(TrafficMetrics::default());
        (
            router_with_metrics(
                LocalObjectStore::new(tmp.path()),
                std::sync::Arc::new(super::super::config::AppConfig::default()),
                metrics.clone(),
            ),
            metrics,
        )
    }

    async fn body_text(response: axum::response::Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8_lossy(&bytes).to_string()
    }

    fn extract_all_xml_tags(xml: &str, tag: &str) -> Vec<String> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let mut results = Vec::new();
        let mut pos = 0;
        while let Some(rel_start) = xml[pos..].find(&open) {
            let abs_start = pos + rel_start + open.len();
            let Some(rel_end) = xml[abs_start..].find(&close) else {
                break;
            };
            results.push(xml[abs_start..abs_start + rel_end].to_string());
            pos = abs_start + rel_end + close.len();
        }
        results
    }

    fn extract_xml_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let start = xml.find(&open)? + open.len();
        let end = xml[start..].find(&close)? + start;
        Some(&xml[start..end])
    }

    #[tokio::test]
    async fn put_get_delete_object_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        // Create bucket
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // PUT object
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket/hello.txt")
                    .header("content-type", "text/plain")
                    .body(Body::from("hello world"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(res.headers().contains_key("etag"));

        // GET object
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/test-bucket/hello.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_text(res).await, "hello world");

        // DELETE object
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/test-bucket/hello.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        // GET after delete → 404
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/test-bucket/hello.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_text(res).await;
        assert!(body.contains("<Code>NoSuchKey</Code>"));
    }

    #[tokio::test]
    async fn missing_bucket_returns_no_such_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/no-such-bucket/key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_text(res).await;
        assert!(body.contains("<Code>NoSuchBucket</Code>"));
    }

    #[tokio::test]
    async fn range_request_returns_partial_content() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rng-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rng-bucket/data")
                    .body(Body::from("0123456789"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/rng-bucket/data")
                    .header("range", "bytes=2-5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            res.headers()
                .get("content-range")
                .unwrap()
                .to_str()
                .unwrap(),
            "bytes 2-5/10"
        );
        assert_eq!(body_text(res).await, "2345");
    }

    #[tokio::test]
    async fn traffic_metrics_count_streamed_body_bytes_not_content_length_header() {
        let tmp = tempfile::tempdir().unwrap();
        let (app, metrics) = make_app_with_metrics(&tmp);

        let before_bucket = metrics.snapshot();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/metric-bucket")
                    .header("content-length", "999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let after_bucket = metrics.snapshot();
        assert_eq!(after_bucket.bytes_in, before_bucket.bytes_in);
        assert_eq!(after_bucket.put_requests - before_bucket.put_requests, 1);

        let before_put = metrics.snapshot();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/metric-bucket/object")
                    .body(Body::from("hello world"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let _ = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let after_put = metrics.snapshot();
        assert_eq!(after_put.bytes_in - before_put.bytes_in, 11);
        assert_eq!(after_put.put_requests - before_put.put_requests, 1);

        let before_head = metrics.snapshot();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/metric-bucket/object")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let _ = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let after_head = metrics.snapshot();
        assert_eq!(after_head.bytes_out, before_head.bytes_out);
        assert_eq!(after_head.head_requests - before_head.head_requests, 1);

        let before_get = metrics.snapshot();
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/metric-bucket/object")
                    .header("range", "bytes=1-4")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(body_text(res).await, "ello");
        let after_get = metrics.snapshot();
        assert_eq!(after_get.bytes_out - before_get.bytes_out, 4);
        assert_eq!(after_get.get_requests - before_get.get_requests, 1);
        assert_eq!(after_get.request_total(), 4);
    }

    #[tokio::test]
    async fn range_unsatisfiable_returns_416() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rng-bucket-2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rng-bucket-2/small")
                    .body(Body::from("hi"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/rng-bucket-2/small")
                    .header("range", "bytes=100-200")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert!(res
            .headers()
            .get("content-range")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("bytes */"));
    }

    #[tokio::test]
    async fn list_objects_with_prefix_and_delimiter() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/list-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        for key in [
            "/list-bucket/a/1",
            "/list-bucket/a/2",
            "/list-bucket/b/1",
            "/list-bucket/c",
        ] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(key)
                        .body(Body::from("x"))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/list-bucket?prefix=&delimiter=/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<Prefix>a/</Prefix>"));
        assert!(body.contains("<Prefix>b/</Prefix>"));
        assert!(body.contains("<Key>c</Key>"));
    }

    #[tokio::test]
    async fn multipart_upload_full_flow() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/mpu-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Initiate
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mpu-bucket/large.bin?uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let xml = body_text(res).await;
        let upload_id = extract_xml_tag(&xml, "UploadId").unwrap().to_string();

        // Upload parts
        let res1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/mpu-bucket/large.bin?uploadId={upload_id}&partNumber=1"
                    ))
                    .body(Body::from("hello"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res1.status(), StatusCode::OK);
        let etag1 = res1
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let res2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/mpu-bucket/large.bin?uploadId={upload_id}&partNumber=2"
                    ))
                    .body(Body::from(" world"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res2.status(), StatusCode::OK);
        let etag2 = res2
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Complete
        let complete_xml = format!(
            r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"#
        );
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/mpu-bucket/large.bin?uploadId={upload_id}"))
                    .body(Body::from(complete_xml))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<ETag>"));

        // Verify content
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/mpu-bucket/large.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_text(res).await, "hello world");
    }

    #[tokio::test]
    async fn delete_nonempty_bucket_returns_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/full-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/full-bucket/obj")
                    .body(Body::from("data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/full-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);
        let body = body_text(res).await;
        assert!(body.contains("<Code>BucketNotEmpty</Code>"));
    }

    #[tokio::test]
    async fn pagination_iterates_all_35_objects_exactly_once() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/pg-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Put 35 zero-padded objects so lexicographic order matches numeric order.
        for i in 1..=35u32 {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("/pg-bucket/obj-{i:02}"))
                        .body(Body::from(format!("data-{i}")))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let mut all_keys: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        let mut pages = 0usize;

        loop {
            let uri = match &token {
                None => "/pg-bucket?list-type=2&max-keys=10".to_string(),
                Some(t) => format!("/pg-bucket?list-type=2&max-keys=10&continuation-token={t}"),
            };
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(&uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let body = body_text(res).await;
            pages += 1;

            let keys = extract_all_xml_tags(&body, "Key");
            assert!(!keys.is_empty(), "page {pages} returned no keys");
            all_keys.extend(keys);

            let is_truncated = extract_xml_tag(&body, "IsTruncated") == Some("true");
            token = extract_xml_tag(&body, "NextContinuationToken").map(str::to_string);

            if !is_truncated {
                break;
            }
            assert!(
                token.is_some(),
                "truncated page must have NextContinuationToken"
            );
            assert!(pages < 20, "pagination did not terminate");
        }

        assert_eq!(
            all_keys.len(),
            35,
            "must return all 35 objects across pages"
        );
        // Keys must arrive in ascending order
        let mut sorted = all_keys.clone();
        sorted.sort();
        assert_eq!(all_keys, sorted, "keys must be in ascending order");
        // No duplicates
        let unique: std::collections::HashSet<_> = all_keys.iter().collect();
        assert_eq!(unique.len(), 35, "no duplicate keys");
    }

    #[tokio::test]
    async fn aborted_multipart_not_visible_on_get() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/ab-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Initiate
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ab-bucket/upload.bin?uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let xml = body_text(res).await;
        let upload_id = extract_xml_tag(&xml, "UploadId").unwrap().to_string();

        // Upload a part
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/ab-bucket/upload.bin?uploadId={upload_id}&partNumber=1"
                    ))
                    .body(Body::from("some data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Abort
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/ab-bucket/upload.bin?uploadId={upload_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        // GET must return 404 — upload was aborted, nothing committed
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/ab-bucket/upload.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_text(res).await;
        assert!(body.contains("<Code>NoSuchKey</Code>"));

        // List must show empty bucket — no object was committed
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/ab-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let list_body = body_text(res).await;
        assert!(
            !list_body.contains("<Key>"),
            "aborted upload must not appear in listing"
        );
    }

    #[tokio::test]
    async fn rebuild_index_endpoint_returns_202_and_index_can_be_queried() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rebuild-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rebuild-bucket/alpha")
                    .body(Body::from("a"))
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rebuild-bucket/beta")
                    .body(Body::from("b"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Trigger rebuild via HTTP endpoint
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rebuild-bucket?rebuildIndex")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);

        // Give the background task a moment to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Objects must still be listable
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/rebuild-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<Key>alpha</Key>"));
        assert!(body.contains("<Key>beta</Key>"));
    }

    #[tokio::test]
    async fn head_object_returns_metadata_without_body() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/head-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/head-bucket/file.txt")
                    .header("content-type", "text/plain")
                    .body(Body::from("abc"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/head-bucket/file.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get("content-length")
                .unwrap()
                .to_str()
                .unwrap(),
            "3"
        );
        assert_eq!(
            res.headers().get("content-type").unwrap().to_str().unwrap(),
            "text/plain"
        );
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn put_rejects_payload_hash_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/hash-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/hash-bucket/file.txt")
                    .header(
                        "x-amz-content-sha256",
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    )
                    .body(Body::from("abc"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = body_text(res).await;
        assert!(body.contains("<Code>XAmzContentSHA256Mismatch</Code>"));
    }

    #[tokio::test]
    async fn get_with_missing_physical_part_returns_error_not_success() {
        let tmp = tempfile::tempdir().unwrap();
        let (app, store) = make_app_with_store(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/corrupt-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/corrupt-bucket/file.txt")
                    .body(Body::from("abc"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let object_path = store
            .layout()
            .object_path("corrupt-bucket", "file.txt")
            .unwrap();
        tokio::fs::remove_file(object_path.object_dir.join("part.1"))
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/corrupt-bucket/file.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = body_text(res).await;
        assert!(body.contains("<Code>InternalError</Code>"));
    }

    #[tokio::test]
    async fn aws_chunked_put_stream_decodes_body() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/chunked-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let chunks = futures::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"5;chunk-signature=abc\r\nhe")),
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(
                b"llo\r\n0;chunk-signature=def\r\n\r\n",
            )),
        ]);
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/chunked-bucket/file.txt")
                    .header("content-encoding", "aws-chunked")
                    .body(Body::from_stream(chunks))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/chunked-bucket/file.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_text(res).await, "hello");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_parts_xml_accepts_quoted_etags() {
        let parts = parse_complete_parts_xml(
            r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"abc"</ETag></Part><Part><PartNumber>3</PartNumber><ETag>def</ETag></Part></CompleteMultipartUpload>"#,
        )
        .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].number, 1);
        assert_eq!(parts[0].etag, "abc");
        assert_eq!(parts[1].number, 3);
    }

    #[test]
    fn complete_parts_xml_accepts_aws_cli_order() {
        let parts = parse_complete_parts_xml(
            r#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><ETag>"abc"</ETag><PartNumber>1</PartNumber></Part><Part><ETag>"def"</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>"#,
        )
        .unwrap();
        assert_eq!(parts[0].number, 1);
        assert_eq!(parts[0].etag, "abc");
        assert_eq!(parts[1].number, 2);
    }

    #[test]
    fn complete_parts_xml_accepts_xml_escaped_etags() {
        let parts = parse_complete_parts_xml(
            r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;900150983cd24fb0d6963f7d28e17f72&quot;</ETag></Part></CompleteMultipartUpload>"#,
        )
        .unwrap();
        assert_eq!(parts[0].number, 1);
        assert_eq!(parts[0].etag, "900150983cd24fb0d6963f7d28e17f72");
    }

    #[test]
    fn human_bytes_formats_units() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.50 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.00 MiB");
    }

    #[test]
    fn qps_formats_window_rate() {
        assert_eq!(qps(0), "0.00");
        assert_eq!(qps(25), "2.50");
    }
}
