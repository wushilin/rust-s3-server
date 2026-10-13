//! Stateless changelog reads: the consumer owns its position.
//!
//! The server keeps nothing per reader. A reader asks for the changes after
//! the `seq` it has applied and stores the returned `next` itself — next to
//! the data it applied, ideally, so a rollback of that data rolls the
//! position back with it. Re-reading is always safe: consumers skip what
//! they already hold.
//!
//! Two front doors share this module:
//! - S3 bucket subresources, authenticated by SigV4 and authorized by IAM
//!   (`s3:GetBucketChangelog`): `GET /{bucket}?x-rs3-changelog` and
//!   `GET /{bucket}?x-rs3-changelog-snapshot`.
//! - Replication tokens (`/_rs3/changelog`), whose subscription supplies the
//!   bucket and filter.

use std::collections::HashMap;
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use super::model::{Filter, WireEvent};
use crate::storage::changelog::ChangeOp;
use crate::storage::errors::StorageError;
use crate::storage::store::LocalObjectStore;

pub const QUERY_CHANGELOG: &str = "x-rs3-changelog";
pub const QUERY_SNAPSHOT: &str = "x-rs3-changelog-snapshot";
pub const DEFAULT_LIMIT: usize = 500;
pub const MAX_LIMIT: usize = 10_000;
pub const MAX_WAIT_SECS: u64 = 60;
pub const DEFAULT_WAIT_SECS: u64 = 20;
/// Pages scanned per request when a filter rejects most changes, so a
/// sparse filter returns a (possibly empty) progress response instead of
/// walking the whole log in one request.
const MAX_SCAN_PAGES: usize = 20;

#[derive(Debug, Clone, Default)]
pub struct ReadParams {
    /// Return changes with `seq > after`.
    pub after: u64,
    /// Instead of `after`: start at the first change committed at or after
    /// this time (epoch ms). The response's `next` is a seq as usual.
    pub since_ms: Option<i64>,
    pub limit: usize,
    /// Long-poll: hold the request up to this long until at least `min`
    /// matching changes are available (Kafka's `fetch.max.wait.ms`).
    pub wait: Duration,
    /// Matching changes to wait for (Kafka's `fetch.min.bytes`, counted in
    /// changes); at least 1, at most `limit`.
    pub min: usize,
    pub filter: Filter,
    /// Refuse (409) if the bucket's log is not this one — the reader's
    /// position would be meaningless.
    pub expect_log_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotParams {
    /// Resume after this key (exclusive).
    pub after: Option<String>,
    pub limit: usize,
    pub filter: Filter,
    /// List the tombstones (deleted keys still remembered) instead of the
    /// objects: `op: "deleted"` entries with the delete's stamp.
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResponse {
    pub bucket: String,
    /// This server's site id: a replicator refuses to copy between two
    /// servers reporting the same one (a cloned data directory).
    pub site_id: String,
    pub log_id: String,
    /// The newest readable seq right now.
    pub head: u64,
    /// Changes `<=` this were removed by retention.
    pub trimmed_through: u64,
    /// The position to ask for next (`after=`). It moves past filtered-out
    /// changes too, so it can jump.
    pub next: u64,
    /// More changes are already available after `next`.
    pub more: bool,
    pub events: Vec<WireEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub bucket: String,
    /// This server's site id (see `storage::stamp`).
    pub site_id: String,
    pub log_id: String,
    /// The changelog head when this page was read. Save the *first* page's
    /// value and continue live (`after=`) from it once the listing is done:
    /// replaying from there repairs anything that changed during the walk.
    pub head: u64,
    pub entries: Vec<WireEvent>,
    /// Pass as `after=` for the next page; `None` when the listing is done.
    pub next_after: Option<String>,
}

#[derive(Debug)]
pub enum ReadError {
    BadRequest(String),
    /// The requested position is older than what retention kept.
    Gone { message: String, trimmed_through: u64, head: u64 },
    /// The log is not the one the reader followed, or the position is
    /// beyond its head.
    LogMismatch { message: String, log_id: String, head: u64 },
    Storage(StorageError),
}

impl From<StorageError> for ReadError {
    fn from(err: StorageError) -> Self {
        ReadError::Storage(err)
    }
}

impl IntoResponse for ReadError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            ReadError::BadRequest(message) => {
                (StatusCode::BAD_REQUEST, serde_json::json!({ "error": message }))
            }
            ReadError::Gone { message, trimmed_through, head } => (
                StatusCode::GONE,
                serde_json::json!({
                    "error": message,
                    "trimmed_through": trimmed_through,
                    "head": head,
                }),
            ),
            ReadError::LogMismatch { message, log_id, head } => (
                StatusCode::CONFLICT,
                serde_json::json!({ "error": message, "log_id": log_id, "head": head }),
            ),
            ReadError::Storage(err) => {
                let status = match err {
                    StorageError::BucketNotFound(_) => StatusCode::NOT_FOUND,
                    StorageError::BucketRebuilding(_) => StatusCode::SERVICE_UNAVAILABLE,
                    StorageError::InvalidBucketName(_) => StatusCode::BAD_REQUEST,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (status, serde_json::json!({ "error": err.to_string() }))
            }
        };
        (status, Json(body)).into_response()
    }
}

fn parse_u64(query: &HashMap<String, String>, name: &str) -> Result<Option<u64>, ReadError> {
    match query.get(name).map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => Ok(None),
        Some(v) => v
            .parse::<u64>()
            .map(Some)
            .map_err(|_| ReadError::BadRequest(format!("{name} must be a non-negative integer"))),
    }
}

fn parse_filter(query: &HashMap<String, String>) -> Result<Filter, ReadError> {
    let ops = match query.get("ops").map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => Vec::new(),
        Some(list) => list
            .split(',')
            .map(|op| match op.trim() {
                "put" => Ok(ChangeOp::Put),
                "delete" => Ok(ChangeOp::Delete),
                other => Err(ReadError::BadRequest(format!("unknown op {other:?} (put, delete)"))),
            })
            .collect::<Result<_, _>>()?,
    };
    Ok(Filter {
        prefix: query.get("prefix").cloned().unwrap_or_default(),
        suffix: query.get("suffix").cloned().unwrap_or_default(),
        ops,
    })
}

fn parse_limit(query: &HashMap<String, String>) -> Result<usize, ReadError> {
    Ok(parse_u64(query, "limit")?
        .map(|n| n as usize)
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, MAX_LIMIT))
}

/// `after` | `since`, `limit`, `min`, `wait` (secs) | `wait_ms`, `prefix`,
/// `suffix`, `ops`, `log_id`.
pub fn read_params(query: &HashMap<String, String>) -> Result<ReadParams, ReadError> {
    let limit = parse_limit(query)?;
    let wait = match (parse_u64(query, "wait_ms")?, parse_u64(query, "wait")?) {
        (Some(_), Some(_)) => return Err(ReadError::BadRequest("pass wait or wait_ms, not both".into())),
        (Some(ms), None) => Duration::from_millis(ms),
        (None, Some(secs)) => Duration::from_secs(secs),
        // Kafka-like by default: a caught-up reader is held until a change
        // arrives (or 20s pass). `wait=0` returns at once.
        (None, None) => Duration::from_secs(DEFAULT_WAIT_SECS),
    }
    .min(Duration::from_secs(MAX_WAIT_SECS));
    Ok(ReadParams {
        min: parse_u64(query, "min")?.map(|n| n as usize).unwrap_or(1).clamp(1, limit),
        wait,
        after: parse_u64(query, "after")?.unwrap_or(0),
        since_ms: match parse_u64(query, "since")? {
            Some(_) if query.get("after").is_some_and(|v| !v.is_empty()) => {
                return Err(ReadError::BadRequest("pass after or since, not both".into()))
            }
            since => since.map(|v| v as i64),
        },
        limit,
        filter: parse_filter(query)?,
        expect_log_id: query.get("log_id").cloned().filter(|v| !v.is_empty()),
    })
}

/// `after` (key), `limit`, `prefix`, `suffix`.
pub fn snapshot_params(query: &HashMap<String, String>) -> Result<SnapshotParams, ReadError> {
    Ok(SnapshotParams {
        after: query.get("after").cloned().filter(|v| !v.is_empty()),
        limit: parse_limit(query)?,
        filter: parse_filter(query)?,
        deleted: query
            .get("deleted")
            .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true")),
    })
}

/// The changes after `params.after`, long-polling when there are none.
pub async fn read(store: &LocalObjectStore, bucket: &str, params: &ReadParams) -> Result<ReadResponse, ReadError> {
    let deadline = tokio::time::Instant::now() + params.wait;
    let signal = store.change_signal(bucket);
    loop {
        // Arm the wake-up before reading so a commit landing between the
        // read and the wait is not missed.
        let changed = signal.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();

        let response = read_once(store, bucket, params).await?;
        // Enough to return: `min` matching changes, or a full page (`more`
        // means the limit or the scan budget was reached).
        let enough = response.events.len() >= params.min.max(1) || response.more;
        if enough || params.wait.is_zero() || tokio::time::Instant::now() >= deadline {
            return Ok(response);
        }
        tokio::select! {
            _ = &mut changed => {}
            _ = tokio::time::sleep_until(deadline) => return Ok(response),
        }
    }
}

async fn read_once(store: &LocalObjectStore, bucket: &str, params: &ReadParams) -> Result<ReadResponse, ReadError> {
    let index = store.index(bucket).await?;
    let status = index.changelog_status().await?;
    if let Some(expected) = &params.expect_log_id {
        if expected != &status.log_id {
            return Err(ReadError::LogMismatch {
                message: format!(
                    "bucket {bucket}'s changelog is {}, not {expected}: the log was replaced (an index rebuild \
                     could not carry it over), so positions in the old log mean nothing here. Re-snapshot.",
                    status.log_id
                ),
                log_id: status.log_id,
                head: status.head,
            });
        }
    }
    let mut params = params.clone();
    if let Some(since) = params.since_ms {
        match index.position_at_time(since).await? {
            Some(after) => params.after = after,
            None => {
                return Err(ReadError::Gone {
                    message: format!(
                        "changes of bucket {bucket} from {since} may have been removed by retention; the oldest \
                         available position is after={}. Re-snapshot, or accept the loss and continue from there.",
                        status.trimmed_through
                    ),
                    trimmed_through: status.trimmed_through,
                    head: status.head,
                })
            }
        }
    }
    let params = &params;
    if params.after < status.trimmed_through {
        return Err(ReadError::Gone {
            message: format!(
                "changes {}–{} of bucket {bucket} were removed by retention; the oldest available position \
                 is after={}. Re-snapshot, or accept the loss and continue from there.",
                params.after + 1,
                status.trimmed_through,
                status.trimmed_through
            ),
            trimmed_through: status.trimmed_through,
            head: status.head,
        });
    }
    if params.after > status.head {
        return Err(ReadError::LogMismatch {
            message: format!(
                "after={} is beyond bucket {bucket}'s changelog head ({}): this position does not belong to \
                 this log.",
                params.after, status.head
            ),
            log_id: status.log_id,
            head: status.head,
        });
    }

    let mut next = params.after;
    let mut events = Vec::new();
    let mut more = false;
    for _ in 0..MAX_SCAN_PAGES {
        let page = index.read_changelog(next, params.limit, usize::MAX).await?;
        next = next.max(page.covered_through);
        events.extend(
            page.events
                .iter()
                .filter(|event| params.filter.matches(event))
                .map(WireEvent::from),
        );
        more = page.more;
        if !events.is_empty() || !page.more {
            break;
        }
    }
    Ok(ReadResponse {
        bucket: bucket.to_string(),
        site_id: store.site_id(),
        log_id: status.log_id,
        head: status.head,
        trimmed_through: status.trimmed_through,
        next,
        more,
        events,
    })
}

/// One page of the current objects, in key order, with what replication
/// needs (`version`, `etag`, `size`).
pub async fn snapshot(
    store: &LocalObjectStore,
    bucket: &str,
    params: &SnapshotParams,
) -> Result<SnapshotResponse, ReadError> {
    let index = store.index(bucket).await?;
    if params.deleted {
        let head = index.changelog_head();
        let (tombstones, more) = index
            .list_tombstones(&params.filter.prefix, params.after.as_deref(), params.limit)
            .await?;
        let next_after = if more { tombstones.last().map(|t| t.object_key.clone()) } else { None };
        return Ok(SnapshotResponse {
            bucket: bucket.to_string(),
            site_id: store.site_id(),
            log_id: index.changelog_id().to_string(),
            head,
            entries: tombstones
                .into_iter()
                .filter(|t| params.filter.matches_key(&t.object_key))
                .map(|t| WireEvent {
                    seq: None,
                    op: "deleted".to_string(),
                    key: t.object_key,
                    etag: t.etag,
                    size: t.size,
                    last_modified_ms: t.deleted_at_ms,
                    version: t.version,
                    origin: None,
                    ts_ms: None,
                    parts: None,
                    removed_stamp: t.removed.clone(),
                    stamp: Some(t.stamp),
                })
                .collect(),
            next_after,
        });
    }
    if !params.filter.wants_puts() {
        // A deletes-only reader has nothing to list.
        return Ok(SnapshotResponse {
            bucket: bucket.to_string(),
            site_id: store.site_id(),
            log_id: index.changelog_id().to_string(),
            head: index.changelog_head(),
            entries: Vec::new(),
            next_after: None,
        });
    }
    // Head first: everything committed after it is replayed live.
    let head = index.changelog_head();
    let log_id = index.changelog_id().to_string();
    let site = store.site_id();
    let page = index
        .list(&params.filter.prefix, None, params.after.as_deref(), params.limit)
        .await?;
    let entries = page
            .entries
            .iter()
            .filter(|record| params.filter.matches_key(&record.object_key))
            .map(|record| WireEvent::exists(record, &site))
            .collect();
    Ok(SnapshotResponse {
        bucket: bucket.to_string(),
        site_id: site,
        log_id,
        head,
        entries,
        next_after: if page.is_truncated { page.next_after } else { None },
    })
}
