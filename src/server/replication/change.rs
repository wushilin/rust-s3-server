//! One replicated change — the model both workers apply. A changelog event
//! (or a snapshot entry) from either side is turned into a [`Change`] once,
//! here, so the rules that read an event are in one place: which stamp it
//! is applied with, what it ranks by, and whether it is a client's change
//! at all.

use crate::server::notify::model::{WireEvent, WireOp, WireOrigin};
use crate::storage::changelog::PartLayout;
use crate::storage::stamp::{Rank, Stamp};

/// What the change does to the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// The key holds the state `version` (a put, or a snapshot object).
    Put,
    /// The key's version stamped `removed` was deleted (a delete, or a
    /// snapshot tombstone). The delete ranks by `removed`.
    Delete { removed: Stamp },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub key: String,
    /// The exact state (a put's), to fetch pinned with `x-rs3-if-version`.
    pub version: String,
    pub etag: String,
    pub size: u64,
    pub parts: Option<PartLayout>,
    pub kind: Kind,
    /// The change's own stamp: the state's for a put, the delete's for a
    /// delete.
    pub stamp: Stamp,
    pub origin: WireOrigin,
}

impl Change {
    /// Reads an event of `site`'s changelog (or snapshot). Events logged
    /// before stamps existed get the fallback stamp `(last_modified_ms, 0,
    /// site)`; a delete without a removed stamp ranks by its own. An event
    /// whose operation this build does not know is an error: it must not be
    /// guessed at.
    pub fn from_event(event: &WireEvent, site: &str) -> Result<Self, String> {
        let stamp = event
            .stamp
            .clone()
            .unwrap_or_else(|| Stamp::fallback(event.last_modified_ms, site));
        let kind = match event.op {
            WireOp::Put | WireOp::Exists => Kind::Put,
            WireOp::Delete | WireOp::Deleted => Kind::Delete {
                removed: event.removed_stamp.clone().unwrap_or_else(|| stamp.clone()),
            },
            WireOp::Unknown => return Err(format!("event for {} has an unknown op (a newer peer?)", event.key)),
        };
        Ok(Self {
            key: event.key.clone(),
            version: event.version.clone(),
            etag: event.etag.clone(),
            size: event.size,
            parts: event.parts.clone(),
            kind,
            stamp,
            origin: event.origin.unwrap_or(WireOrigin::Client),
        })
    }

    /// What the change ranks by (see [`Rank`]): a put by its stamp, a
    /// delete by the version it removed.
    pub fn rank(&self) -> Rank {
        match &self.kind {
            Kind::Put => Rank::live(self.stamp.clone()),
            Kind::Delete { removed } => Rank::deleted(removed.clone()),
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(self.kind, Kind::Delete { .. })
    }

    /// A delete only: the stamp of the version it removed.
    pub fn removed(&self) -> Option<&Stamp> {
        match &self.kind {
            Kind::Delete { removed } => Some(removed),
            Kind::Put => None,
        }
    }

    /// A health scan removed a row whose data was lost: the site lost its
    /// copy, no client deleted the object. Never replicated — the other
    /// side keeps its copy (and can restore from it).
    pub fn is_repair_delete(&self) -> bool {
        self.is_delete() && self.origin == WireOrigin::Repair
    }

    /// Uploaded as a multipart object: its layout, to reproduce for an
    /// identical ETag.
    pub fn multipart_layout(&self) -> Option<&PartLayout> {
        self.parts.as_ref().filter(|layout| layout.multipart)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(op: WireOp, stamp: Option<Stamp>, removed: Option<Stamp>) -> WireEvent {
        WireEvent {
            seq: Some(1),
            op,
            key: "k".into(),
            etag: "e".into(),
            size: 1,
            last_modified_ms: 500,
            version: "V1".into(),
            origin: None,
            ts_ms: None,
            parts: None,
            stamp,
            removed_stamp: removed,
        }
    }

    fn st(ms: i64) -> Stamp {
        Stamp { ms, c: 0, site: "a".into() }
    }

    #[test]
    fn puts_rank_by_stamp_and_deletes_by_the_removed_version() {
        let put = Change::from_event(&event(WireOp::Put, Some(st(7)), None), "a").unwrap();
        assert_eq!(put.rank(), Rank::live(st(7)));
        let delete = Change::from_event(&event(WireOp::Delete, Some(st(9)), Some(st(7))), "a").unwrap();
        assert_eq!(delete.rank(), Rank::deleted(st(7)));
        assert_eq!(delete.stamp, st(9));
        let tombstone = Change::from_event(&event(WireOp::Deleted, Some(st(9)), Some(st(7))), "a").unwrap();
        assert_eq!(tombstone.rank(), Rank::deleted(st(7)));
    }

    #[test]
    fn missing_stamps_fall_back() {
        let old = Change::from_event(&event(WireOp::Exists, None, None), "peer").unwrap();
        assert_eq!(old.stamp, Stamp::fallback(500, "peer"));
        let delete = Change::from_event(&event(WireOp::Delete, Some(st(9)), None), "a").unwrap();
        assert_eq!(delete.rank(), Rank::deleted(st(9)), "a delete without a removed stamp ranks by its own");
    }

    #[test]
    fn unknown_ops_are_refused_and_repair_deletes_recognised() {
        assert!(Change::from_event(&event(WireOp::Unknown, None, None), "a").is_err());
        let mut repair = event(WireOp::Delete, Some(st(9)), None);
        repair.origin = Some(WireOrigin::Repair);
        assert!(Change::from_event(&repair, "a").unwrap().is_repair_delete());
        let mut replicated = event(WireOp::Delete, Some(st(9)), None);
        replicated.origin = Some(WireOrigin::Replication);
        assert!(!Change::from_event(&replicated, "a").unwrap().is_repair_delete());
    }
}
