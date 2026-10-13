//! Subscription records and the batch wire format.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::storage::changelog::{ChangeEvent, ChangeOp};

/// Highest record version this build reads.
pub(super) const RECORD_VERSION: u32 = 1;

fn default_version() -> u32 {
    RECORD_VERSION
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionKind {
    /// The server pushes batches to `url`.
    Webhook,
    /// A replicator pulls batches with a replication token.
    Pull,
}

/// Which changes a subscription receives. Empty fields match everything.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub suffix: String,
    /// `put` / `delete`; empty = both. Snapshot entries count as `put`.
    #[serde(default)]
    pub ops: Vec<ChangeOp>,
}

impl Filter {
    pub fn matches_key(&self, key: &str) -> bool {
        key.starts_with(&self.prefix) && key.ends_with(&self.suffix)
    }

    pub fn matches(&self, event: &ChangeEvent) -> bool {
        self.matches_key(&event.key) && (self.ops.is_empty() || self.ops.contains(&event.op))
    }

    pub fn wants_puts(&self) -> bool {
        self.ops.is_empty() || self.ops.contains(&ChangeOp::Put)
    }
}

/// A webhook or a pull (replication-token) subscription. Configuration only —
/// progress lives in [`SubState`], which changes on every delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    #[serde(default = "default_version")]
    pub v: u32,
    pub id: String,
    pub name: String,
    pub bucket: String,
    pub kind: SubscriptionKind,
    #[serde(default)]
    pub filter: Filter,
    /// Webhook target.
    #[serde(default)]
    pub url: String,
    /// Extra request headers (webhook). Values are secrets: never logged,
    /// masked in API responses.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// HMAC-SHA256 signing secret (webhook).
    #[serde(default)]
    pub secret: String,
    /// SHA-256 (hex) of the replication token (pull). The token itself is
    /// shown once, at creation or rotation.
    #[serde(default)]
    pub token_hash: String,
    #[serde(default)]
    pub batch_max_events: Option<usize>,
    #[serde(default)]
    pub batch_max_bytes: Option<usize>,
    #[serde(default)]
    pub mute_secs: Option<u64>,
    #[serde(default)]
    pub created_at_ms: i64,
    #[serde(default)]
    pub created_by: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    /// An admin paused it.
    Operator,
    /// Its offset fell behind the retention trim point: events were lost.
    Expired,
    /// The bucket's changelog was replaced (an index rebuild could not carry
    /// it over); offsets in the old log mean nothing in the new one.
    LogReplaced,
    /// The bucket no longer exists.
    BucketGone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pause {
    pub reason: PauseReason,
    pub message: String,
    pub at_ms: i64,
}

/// An initial (or re-requested) snapshot in progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotState {
    /// Changelog head when the snapshot started; live delivery resumes from
    /// here once the walk finishes.
    pub through: u64,
    /// Last key delivered (exclusive resume point).
    #[serde(default)]
    pub cursor: Option<String>,
    /// Whether the `snapshot_begin` batch was delivered.
    #[serde(default)]
    pub begun: bool,
}

/// Delivery progress of one subscription.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubState {
    #[serde(default = "default_version")]
    pub v: u32,
    /// The changelog this offset refers to.
    #[serde(default)]
    pub log_id: String,
    /// Every change with `seq <= offset` was delivered (or filtered out).
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub snapshot: Option<SnapshotState>,
    #[serde(default)]
    pub paused: Option<Pause>,
    /// Webhook only: no delivery before this time.
    #[serde(default)]
    pub muted_until_ms: i64,
    #[serde(default)]
    pub consecutive_failures: u32,
    #[serde(default)]
    pub last_attempt_ms: i64,
    #[serde(default)]
    pub last_success_ms: i64,
    #[serde(default)]
    pub last_status: Option<u16>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub delivered_events: u64,
    #[serde(default)]
    pub delivered_batches: u64,
}

impl SubState {
    /// The delivery position only — what an ack moves.
    pub fn position(&self) -> Position {
        Position {
            log_id: self.log_id.clone(),
            offset: self.offset,
            snapshot: self.snapshot.clone(),
        }
    }

    pub fn set_position(&mut self, position: Position) {
        self.log_id = position.log_id;
        self.offset = position.offset;
        self.snapshot = position.snapshot;
    }
}

/// A delivery position; pull consumers receive it (encoded) as the `ack`
/// value of each batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub log_id: String,
    pub offset: u64,
    #[serde(default)]
    pub snapshot: Option<SnapshotState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Snapshot,
    Live,
}

/// One event on the wire. Live events carry their `seq`; snapshot entries
/// (`op: "exists"`) have none.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireEvent {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub seq: Option<u64>,
    pub op: String,
    pub key: String,
    pub etag: String,
    pub size: u64,
    pub last_modified_ms: i64,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub origin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ts_ms: Option<i64>,
    /// Upload layout to reproduce for an identical ETag; absent for objects
    /// indexed before layouts were recorded (ask GetObjectAttributes).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub parts: Option<crate::storage::changelog::PartLayout>,
    /// Version stamp: the state's (put / exists) or the delete's own.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub stamp: Option<crate::storage::stamp::Stamp>,
    /// Deletes: the stamp of the version removed. A delete ranks by it, so
    /// it never defeats a newer version than the one its author saw.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub removed_stamp: Option<crate::storage::stamp::Stamp>,
}

impl WireEvent {
    /// A snapshot entry for a stored object. Objects written before stamps
    /// existed get the fallback stamp `(last_modified_ms, 0, site)`.
    pub fn exists(record: &crate::storage::index::ObjectRecord, site: &str) -> Self {
        Self {
            seq: None,
            op: "exists".to_string(),
            key: record.object_key.clone(),
            etag: record.etag.clone(),
            size: record.size,
            last_modified_ms: record.last_modified_ms,
            version: crate::storage::changelog::version_of(&record.blob_dir),
            origin: None,
            ts_ms: None,
            parts: record.parts.clone(),
            stamp: Some(
                record
                    .stamp
                    .clone()
                    .unwrap_or_else(|| crate::storage::stamp::Stamp::fallback(record.last_modified_ms, site)),
            ),
            removed_stamp: None,
        }
    }
}

impl From<&ChangeEvent> for WireEvent {
    fn from(event: &ChangeEvent) -> Self {
        Self {
            seq: Some(event.seq),
            op: event.op.as_str().to_string(),
            key: event.key.clone(),
            etag: event.etag.clone(),
            size: event.size,
            last_modified_ms: event.last_modified_ms,
            version: event.version.clone(),
            origin: Some(
                match event.origin {
                    crate::storage::changelog::ChangeOrigin::Client => "client",
                    crate::storage::changelog::ChangeOrigin::Repair => "repair",
                }
                .to_string(),
            ),
            ts_ms: Some(event.ts_ms),
            parts: event.parts.clone(),
            stamp: event.stamp.clone(),
            removed_stamp: event.removed_stamp.clone(),
        }
    }
}

/// One delivery: the webhook POST body, and (plus `ack`) the pull response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Batch {
    pub subscription: String,
    pub bucket: String,
    pub phase: Phase,
    /// Live: the seq range this batch covers, inclusive. Events inside it may
    /// be sparse (filtered out or never committed). Snapshot: both equal the
    /// snapshot's `snapshot_through`.
    pub from_seq: u64,
    pub to_seq: u64,
    #[serde(default)]
    pub snapshot_begin: bool,
    #[serde(default)]
    pub snapshot_end: bool,
    /// Snapshot: live delivery continues after this seq.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub snapshot_through: Option<u64>,
    pub events: Vec<WireEvent>,
}
