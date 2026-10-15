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
/// Commit time of the newest trimmed entry: changes at or before it may be
/// gone, changes after it are all retained.
pub const KEY_TRIMMED_TS: &[u8] = b"changelog_trimmed_ts";

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
    /// One version of a key (named by `stamp`) was permanently removed —
    /// `DeleteObject?versionId`: a noncurrent version, the current one, or
    /// a delete marker. The key's current state is whatever remains.
    #[serde(rename = "remove_version")]
    RemoveVersion,
    /// The tags of one version (named by `stamp`) were replaced; `tags`
    /// holds the new set. Neither the ETag nor the version changed.
    Tags,
    /// A bucket setting changed (`key` is empty): `settings` carries the
    /// bucket's replicated settings, each with the stamp of its last change.
    Bucket,
}

impl ChangeOp {
    pub fn as_str(self) -> &'static str {
        match self {
            ChangeOp::Put => "put",
            ChangeOp::Delete => "delete",
            ChangeOp::RemoveVersion => "remove_version",
            ChangeOp::Tags => "tags",
            ChangeOp::Bucket => "bucket",
        }
    }
}

/// Who caused a change. Replicators should treat `Repair` deletes with care:
/// they mean "this server lost the object", not "a client deleted it".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChangeOrigin {
    /// A client of this site wrote it.
    #[default]
    Client,
    /// Replication applied it here: a change made on another site (its
    /// stamp names that site), or a mirror's own sweep.
    Replication,
    /// The health scan changed local state (a lost object's row removed, a
    /// row resynced from its blob, a lost object restored).
    Repair,
}

impl ChangeOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            ChangeOrigin::Client => "client",
            ChangeOrigin::Replication => "replication",
            ChangeOrigin::Repair => "repair",
        }
    }
}

/// How an object's bytes were uploaded — what a replicator must reproduce
/// to get the identical ETag on the other side. A single PUT is
/// `{"count": 1}` (ETag = MD5 of the content). A multipart upload carries
/// `multipart: true` (its ETag is the MD5 of the part MD5s plus `-N`, so even
/// one part differs from a single PUT) and either a uniform layout
/// (`size` for every part but the `last`) or every part's `sizes`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartLayout {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub multipart: bool,
    pub count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sizes: Option<Vec<u64>>,
}

impl PartLayout {
    pub fn single() -> Self {
        Self {
            multipart: false,
            count: 1,
            size: None,
            last: None,
            sizes: None,
        }
    }

    /// A multipart layout from its part sizes, compact when uniform.
    pub fn multipart(sizes: &[u64]) -> Self {
        let count = sizes.len() as u32;
        let uniform = sizes.len() >= 1 && sizes[..sizes.len() - 1].iter().all(|s| *s == sizes[0]);
        if uniform {
            Self {
                multipart: true,
                count,
                size: Some(sizes[0]),
                last: sizes.last().copied(),
                sizes: None,
            }
        } else {
            Self {
                multipart: true,
                count,
                size: None,
                last: None,
                sizes: Some(sizes.to_vec()),
            }
        }
    }

    /// The layout of a stored object.
    pub fn of(meta: &super::metadata::ObjectMeta) -> Self {
        match meta.storage {
            super::metadata::ObjectStorageKind::Single => Self::single(),
            super::metadata::ObjectStorageKind::Multipart => {
                Self::multipart(&meta.parts.iter().map(|p| p.size).collect::<Vec<_>>())
            }
        }
    }

    /// Every part's size (a single PUT is one part of the whole object).
    pub fn part_sizes(&self, total: u64) -> Vec<u64> {
        if let Some(sizes) = &self.sizes {
            return sizes.clone();
        }
        if !self.multipart {
            return vec![total];
        }
        let size = self.size.unwrap_or(total);
        let mut out = vec![size; self.count.saturating_sub(1) as usize];
        out.push(self.last.unwrap_or(size));
        out
    }
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
    /// The object's upload layout (for `delete`, the removed version's).
    /// `None` for objects indexed before layouts were recorded.
    pub parts: Option<PartLayout>,
    /// The change's version stamp: the new state's for `put`, the delete's
    /// own for `delete`. `None` only for changes logged before stamps.
    pub stamp: Option<super::stamp::Stamp>,
    /// `delete` only: the stamp of the version it removed — what the delete
    /// ranks by (see `stamp::Rank`).
    pub removed_stamp: Option<super::stamp::Stamp>,
    /// The state is a real version of a versioning-enabled bucket (its
    /// stamp is its version id); `false` is the key's `null` version.
    pub versioned: bool,
    /// `tags` only: the version's new tag set.
    pub tags: Option<std::collections::BTreeMap<String, String>>,
    /// `bucket` only: the bucket's settings after the change.
    pub settings: Option<super::metadata::BucketSettings>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parts: Option<PartLayout>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stamp: Option<super::stamp::Stamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    removed_stamp: Option<super::stamp::Stamp>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    versioned: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tags: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    settings: Option<super::metadata::BucketSettings>,
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
        parts: event.parts.clone(),
        stamp: event.stamp.clone(),
        removed_stamp: event.removed_stamp.clone(),
        versioned: event.versioned,
        tags: event.tags.clone(),
        settings: event.settings.clone(),
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
        parts: v.parts,
        stamp: v.stamp,
        removed_stamp: v.removed_stamp,
        versioned: v.versioned,
        tags: v.tags,
        settings: v.settings,
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
    /// Commit time given to the last allocated seq.
    last_ts: i64,
    in_flight: BTreeSet<u64>,
}

impl SeqTracker {
    pub fn new(last: u64, last_ts: i64) -> Self {
        Self {
            state: Mutex::new(SeqState {
                last,
                last_ts,
                in_flight: BTreeSet::new(),
            }),
        }
    }

    /// Allocates the next seq and marks it in flight until the returned guard
    /// drops (after the write batch carrying it returned, success or not).
    /// Its commit time is `now_ms`, clamped so commit times never decrease
    /// in seq order — which makes "the first change at or after time T" a
    /// binary search over seqs.
    pub fn begin(&self, now_ms: i64) -> SeqGuard<'_> {
        let mut state = self.state.lock().unwrap();
        state.last += 1;
        state.last_ts = state.last_ts.max(now_ms);
        let seq = state.last;
        let ts = state.last_ts;
        state.in_flight.insert(seq);
        SeqGuard { tracker: self, seq, ts }
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
    /// The change's commit time (non-decreasing in seq order).
    pub ts: i64,
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
        let tracker = SeqTracker::new(10, 0);
        let a = tracker.begin(5);
        let b = tracker.begin(3);
        assert_eq!((a.seq, b.seq), (11, 12));
        assert_eq!((a.ts, b.ts), (5, 5), "commit times never decrease in seq order");
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
    fn part_layouts_are_compact_and_reversible() {
        let uniform = PartLayout::multipart(&[8, 8, 8, 3]);
        assert_eq!((uniform.size, uniform.last, uniform.sizes.is_none()), (Some(8), Some(3), true));
        assert_eq!(uniform.part_sizes(27), [8, 8, 8, 3]);
        let one = PartLayout::multipart(&[7]);
        assert!(one.multipart, "one-part multipart is not a single PUT (its ETag ends in -1)");
        assert_eq!(one.part_sizes(7), [7]);
        let irregular = PartLayout::multipart(&[8, 5, 8]);
        assert_eq!(irregular.sizes.as_deref(), Some(&[8, 5, 8][..]));
        assert_eq!(irregular.part_sizes(21), [8, 5, 8]);
        assert_eq!(PartLayout::single().part_sizes(42), [42]);
        assert_eq!(serde_json::to_string(&PartLayout::single()).unwrap(), r#"{"count":1}"#);
        assert_eq!(
            serde_json::to_string(&uniform).unwrap(),
            r#"{"multipart":true,"count":4,"size":8,"last":3}"#
        );
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
            parts: Some(PartLayout::multipart(&[5, 5, 2])),
            stamp: Some(super::super::stamp::Stamp { ms: 9, c: 1, site: "s".into() }),
            removed_stamp: Some(super::super::stamp::Stamp { ms: 5, c: 0, site: "s".into() }),
            versioned: true,
            tags: None,
            settings: None,
        };
        assert_eq!(decode_change(7, &encode_change(&event)).unwrap(), event);
        let removal = ChangeEvent { op: ChangeOp::RemoveVersion, versioned: false, ..event };
        let encoded = encode_change(&removal);
        assert!(std::str::from_utf8(&encoded).unwrap().contains(r#""op":"remove_version""#));
        assert_eq!(decode_change(7, &encoded).unwrap(), removal);
    }
}
