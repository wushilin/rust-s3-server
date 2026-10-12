//! Per-bucket changelog: an ordered, durable record of every committed object
//! change, kept in the `changelog` column family of the bucket's index
//! database and written in the *same* write batch as the change itself, so the
//! log can never disagree with the index.
//!
//! Each entry gets a sequence number (`seq`) from a per-bucket allocator.
//! Sequence numbers are strictly increasing but **not contiguous**: a commit
//! whose write fails leaves a hole, and trimming removes the oldest range.
//! Consumers order and de-duplicate by `seq` and must never treat a hole as
//! data loss — loss is reported explicitly (see [`ChangelogStatus`]).
//!
//! Commits on different keys run concurrently, so seq `n+1` can be durable
//! before seq `n`. Readers therefore only ever see entries up to the
//! *watermark* — the highest seq below which every allocated seq has finished
//! committing (see [`SeqTracker`]) — so a reader can never skip past an entry
//! that lands later.
//!
//! The `meta` family carries two changelog keys:
//! - `changelog_id`: a random id minted when the log is first created. It
//!   survives index rebuilds (the log is copied across); a consumer that sees
//!   it change knows its offset refers to a different log.
//! - `changelog_trimmed_through`: the highest seq ever removed by retention.
//!   Entries `<=` it are gone; a consumer whose offset is below it has lost
//!   events. It also seeds the allocator when every entry has been trimmed.

use std::collections::BTreeSet;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::errors::{Result, StorageError};

pub const CF_CHANGELOG: &str = "changelog";
pub const KEY_CHANGELOG_ID: &[u8] = b"changelog_id";
pub const KEY_TRIMMED_THROUGH: &[u8] = b"changelog_trimmed_through";

/// Highest changelog value version this build can read.
const CHANGE_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeOp {
    /// An object was created or overwritten (PUT, copy, multipart complete,
    /// or a health-scan resync of its attributes).
    Put,
    /// An object was removed.
    Delete,
}

impl ChangeOp {
    pub fn as_str(self) -> &'static str {
        match self {
            ChangeOp::Put => "put",
            ChangeOp::Delete => "delete",
        }
    }
}

/// Who caused a change. Replicators should treat `Repair` deletes with care:
/// they mean "this server lost the object", not "a client deleted it".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChangeOrigin {
    #[default]
    Client,
    Repair,
}

/// One changelog entry as stored and as delivered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub seq: u64,
    pub op: ChangeOp,
    pub key: String,
    /// For `put`: the new object's ETag. For `delete`: the removed object's.
    pub etag: String,
    pub size: u64,
    pub last_modified_ms: i64,
    /// Identifies this exact object state (changes on every publish, even
    /// when the ETag does not). For `delete`, the version that was removed.
    pub version: String,
    pub origin: ChangeOrigin,
    /// When the change committed (server clock; informational only — order
    /// by `seq`).
    pub ts_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChangeValueV1 {
    #[serde(default = "default_version")]
    v: u32,
    op: ChangeOp,
    #[serde(default)]
    key: String,
    #[serde(default)]
    etag: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    last_modified_ms: i64,
    #[serde(default)]
    version: String,
    #[serde(default)]
    origin: ChangeOrigin,
    #[serde(default)]
    ts_ms: i64,
}

fn default_version() -> u32 {
    CHANGE_VERSION
}

/// The stable version id of a blob dir: its leaf name (`V1AB3F7C_9F00`),
/// which is unique per publish and does not leak the on-disk fanout.
pub fn version_of(blob_dir: &str) -> String {
    blob_dir
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(blob_dir)
        .to_string()
}

pub fn encode_change(event: &ChangeEvent) -> Vec<u8> {
    serde_json::to_vec(&ChangeValueV1 {
        v: CHANGE_VERSION,
        op: event.op,
        key: event.key.clone(),
        etag: event.etag.clone(),
        size: event.size,
        last_modified_ms: event.last_modified_ms,
        version: event.version.clone(),
        origin: event.origin,
        ts_ms: event.ts_ms,
    })
    .expect("ChangeValueV1 serializes")
}

pub fn decode_change(seq: u64, value: &[u8]) -> Result<ChangeEvent> {
    let v: ChangeValueV1 = serde_json::from_slice(value)?;
    if v.v > CHANGE_VERSION {
        return Err(StorageError::Db(format!(
            "changelog value is v{}, newer than this build understands (v{CHANGE_VERSION})",
            v.v
        )));
    }
    Ok(ChangeEvent {
        seq,
        op: v.op,
        key: v.key,
        etag: v.etag,
        size: v.size,
        last_modified_ms: v.last_modified_ms,
        version: v.version,
        origin: v.origin,
        ts_ms: v.ts_ms,
    })
}

pub fn seq_key(seq: u64) -> [u8; 8] {
    seq.to_be_bytes()
}

pub fn seq_from_key(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let n = bytes.len().min(8);
    buf[8 - n..].copy_from_slice(&bytes[..n]);
    u64::from_be_bytes(buf)
}

pub fn decode_u64(bytes: &[u8]) -> u64 {
    if bytes.len() == 8 {
        u64::from_le_bytes(bytes.try_into().unwrap())
    } else {
        0
    }
}

/// Allocates sequence numbers and tracks which are still being written, so
/// readers can stop at the commit-order watermark.
#[derive(Debug)]
pub struct SeqTracker {
    state: Mutex<SeqState>,
}

#[derive(Debug)]
struct SeqState {
    /// Last allocated seq.
    last: u64,
    in_flight: BTreeSet<u64>,
}

impl SeqTracker {
    pub fn new(last: u64) -> Self {
        Self {
            state: Mutex::new(SeqState {
                last,
                in_flight: BTreeSet::new(),
            }),
        }
    }

    /// Allocates the next seq and marks it in flight until the returned guard
    /// drops (after the write batch carrying it returned, success or not).
    pub fn begin(&self) -> SeqGuard<'_> {
        let mut state = self.state.lock().unwrap();
        state.last += 1;
        let seq = state.last;
        state.in_flight.insert(seq);
        SeqGuard { tracker: self, seq }
    }

    /// Every seq `<=` this has finished committing (or failed and will never
    /// appear). Readers must not read past it.
    pub fn watermark(&self) -> u64 {
        let state = self.state.lock().unwrap();
        match state.in_flight.first() {
            Some(first) => first - 1,
            None => state.last,
        }
    }
}

pub struct SeqGuard<'a> {
    tracker: &'a SeqTracker,
    pub seq: u64,
}

impl Drop for SeqGuard<'_> {
    fn drop(&mut self) {
        self.tracker.state.lock().unwrap().in_flight.remove(&self.seq);
    }
}

/// A page of changelog entries returned by a read.
#[derive(Debug, Clone, Default)]
pub struct ChangelogPage {
    pub events: Vec<ChangeEvent>,
    /// The highest seq this read covered. Equals the last event's seq, or —
    /// when the read stopped at the watermark — the watermark, so a consumer
    /// can advance past holes without receiving anything.
    pub covered_through: u64,
    /// Whether more entries exist beyond `covered_through` right now.
    pub more: bool,
}

/// Where a bucket's log stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChangelogStatus {
    pub log_id: String,
    /// Commit-order watermark: the newest readable seq.
    pub head: u64,
    /// Entries `<=` this were removed by retention.
    pub trimmed_through: u64,
    /// Commit time of the oldest retained entry, if any.
    pub oldest_ts_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_waits_for_out_of_order_commits() {
        let tracker = SeqTracker::new(10);
        let a = tracker.begin();
        let b = tracker.begin();
        assert_eq!((a.seq, b.seq), (11, 12));
        assert_eq!(tracker.watermark(), 10);
        drop(b); // 12 committed first
        assert_eq!(tracker.watermark(), 10, "11 still in flight");
        drop(a);
        assert_eq!(tracker.watermark(), 12);
    }

    #[test]
    fn version_is_the_blob_leaf() {
        assert_eq!(version_of("objects/ab/V1AB_9F00"), "V1AB_9F00");
        assert_eq!(version_of("objects\\ab\\V1AB_9F00"), "V1AB_9F00");
        assert_eq!(version_of("V1AB"), "V1AB");
    }

    #[test]
    fn change_round_trips() {
        let event = ChangeEvent {
            seq: 7,
            op: ChangeOp::Delete,
            key: "a/b".into(),
            etag: "abc".into(),
            size: 3,
            last_modified_ms: 5,
            version: "V1".into(),
            origin: ChangeOrigin::Repair,
            ts_ms: 9,
        };
        assert_eq!(decode_change(7, &encode_change(&event)).unwrap(), event);
    }
}
