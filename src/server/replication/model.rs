//! Replication link records.

use serde::{Deserialize, Serialize};

pub(super) const RECORD_VERSION: u32 = 1;

fn default_version() -> u32 {
    RECORD_VERSION
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// One-way: the local bucket becomes a copy of the peer's — its initial
    /// snapshot deletes local objects the peer does not have.
    #[default]
    Mirror,
    /// Two-way: the link pulls the peer's changes into the local bucket and
    /// pushes the local bucket's changes to the peer, so both buckets end up
    /// as the union, every key at its highest-stamped state. One link (on
    /// either site) serves the pair.
    Merge,
}

/// "Replicate `peer_bucket` at `peer_endpoint` with `local_bucket`" — the
/// whole bucket. A mirror pulls only; a merge pulls and pushes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Link {
    #[serde(default = "default_version")]
    pub v: u32,
    pub id: String,
    pub name: String,
    pub local_bucket: String,
    pub peer_endpoint: String,
    pub peer_bucket: String,
    /// The peer key: what the link may do on the peer is that key's policy
    /// there.
    pub access_key: String,
    /// Secret: never returned by the API.
    pub secret_key: String,
    /// An access key on this site: what the link may do here (write the
    /// local bucket, and for a merge link read it and its changelog) is that
    /// key's policy. Only named — the server acts as it, so no secret.
    #[serde(default)]
    pub local_access_key: String,
    /// The two site ids this link was made between: this site's, and the
    /// peer's (from the creation check, or its first contact when created
    /// unchecked). A site id is how a site's changelog and stamps are
    /// known, so if either side reports another one the link stops until an
    /// admin accepts the new ids.
    #[serde(default)]
    pub local_site_id: String,
    #[serde(default)]
    pub peer_site_id: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub mode: Mode,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub created_at_ms: i64,
    #[serde(default)]
    pub created_by: String,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_true() -> bool {
    true
}

/// Where a link is in the peer's changelog. Kept in the local bucket's
/// index, beside the data it describes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    #[serde(default = "default_version")]
    pub v: u32,
    pub peer_log_id: String,
    pub peer_site: String,
    pub phase: Phase,
    /// When the checkpoint was written (ms) — a site whose checkpoint is
    /// older than the tombstone retention must re-snapshot.
    #[serde(default)]
    pub saved_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum Phase {
    /// Copying the peer's objects (then, in merge mode, its tombstones).
    Snapshot {
        /// The peer's changelog head when the snapshot started; live
        /// replication continues after it.
        through: u64,
        /// Resume after this key.
        #[serde(default)]
        cursor: Option<String>,
        #[serde(default)]
        tombstones: bool,
    },
    /// Following the peer's changelog.
    Live { after: u64 },
}

/// Where a merge link's push is in the local changelog. Kept in the local
/// bucket's index under `<link id>:push`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushCheckpoint {
    #[serde(default = "default_version")]
    pub v: u32,
    /// The local changelog this position belongs to.
    pub local_log_id: String,
    /// The peer's changelog when the push started: a peer whose log was
    /// replaced (rebuilt, restored from backup) gets everything again.
    pub peer_log_id: String,
    pub peer_site: String,
    pub phase: Phase,
    #[serde(default)]
    pub saved_at_ms: i64,
}

/// The push half of a merge link (the local bucket's changes to the peer).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PushStatus {
    /// `idle` | `snapshot` | `live` | `error`
    pub state: String,
    /// Position in the local changelog.
    pub position: Option<u64>,
    pub local_head: Option<u64>,
    pub snapshot_cursor: Option<String>,
    pub last_error: Option<String>,
    pub last_error_ms: i64,
    pub last_progress_ms: i64,
    pub copied: u64,
    pub copied_bytes: u64,
    pub metadata_only: u64,
    pub deleted: u64,
    pub skipped: u64,
    pub superseded: u64,
    pub collapsed: u64,
}

/// What the console shows for a link.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LinkStatus {
    /// `idle` | `snapshot` | `live` | `error` | `paused` | `stopped`
    pub state: String,
    pub peer_site: Option<String>,
    /// The peer's readable name, as it last reported it (a label).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_site_name: Option<String>,
    pub position: Option<u64>,
    pub peer_head: Option<u64>,
    pub snapshot_cursor: Option<String>,
    pub last_error: Option<String>,
    pub last_error_ms: i64,
    pub last_progress_ms: i64,
    pub running_since_ms: i64,
    pub copied: u64,
    pub copied_bytes: u64,
    pub metadata_only: u64,
    pub deleted: u64,
    pub skipped: u64,
    /// Changes refused because the local state was newer (last-writer-wins).
    pub superseded: u64,
    /// Changes skipped because a later change to the same key was in the
    /// same batch.
    pub collapsed: u64,
    pub restarts: u64,
    /// A site id changed since the link was made (what changed): the link
    /// is stopped until the new ids are accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site_changed: Option<String>,
    /// Merge links: the push direction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push: Option<PushStatus>,
}
