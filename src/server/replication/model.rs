//! Replication link records.

use serde::{Deserialize, Serialize};

use crate::server::notify::model::Filter;

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
    /// Two-way member: the local bucket takes the peer's changes but keeps
    /// what only it has; with a link in each direction both buckets end up
    /// as the union, every key at its highest-stamped state.
    Merge,
}

/// "Pull `peer_bucket` from the peer at `peer_endpoint` into
/// `local_bucket`." Configured on the receiving site.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Link {
    #[serde(default = "default_version")]
    pub v: u32,
    pub id: String,
    pub name: String,
    pub local_bucket: String,
    pub peer_endpoint: String,
    pub peer_bucket: String,
    pub access_key: String,
    /// Secret: never returned by the API.
    pub secret_key: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub mode: Mode,
    /// Only keys under `filter.prefix` are replicated (suffix / ops are not
    /// used: a replica must see every change of the keys it holds).
    #[serde(default)]
    pub filter: Filter,
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

/// What the console shows for a link.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LinkStatus {
    /// `idle` | `snapshot` | `live` | `error` | `paused` | `stopped`
    pub state: String,
    pub peer_site: Option<String>,
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
}
