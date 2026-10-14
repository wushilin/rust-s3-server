//! The write model. Every change to an object's state — a client's S3
//! request, a change replicated from another site, a mirror's sweep, a
//! repair — is a write with a [`WriteOrigin`], and the store treats them all
//! the same way: under the key's lock it decides whether the write applies
//! (last-writer-wins, see [`Rank`]), gives the new state its stamp, and
//! commits the index row, the tombstone and the changelog entry in one
//! batch. The changelog entry is a side effect of the commit, never written
//! by a caller.
//!
//! What differs between origins is only:
//! - whether the write is refused on a read-only bucket (only a client's is),
//! - whether the new state keeps a stamp it was given (a replicated state
//!   keeps the stamp of the site that made it) or gets a fresh local one,
//! - whether the write is adjudicated against the current state (a restore
//!   is not: it overwrites a broken state on purpose, guarded by a
//!   precondition),
//! - and how the changelog records who caused it ([`ChangeOrigin`]).

use super::changelog::ChangeOrigin;
use super::stamp::{Rank, Stamp};
use super::store::Precondition;

/// Who makes a write, and with what authority.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum WriteOrigin {
    /// A client's own request to this site. The new state gets a fresh local
    /// stamp, which outranks every state the site has seen; a read-only
    /// bucket refuses it.
    #[default]
    Client,
    /// A state made on another site, applied here with that site's stamp
    /// (`x-rs3-stamp`, or a changelog event's `stamp`). Applies only if it
    /// outranks the key's current state; a read-only bucket accepts it.
    Replicated(Stamp),
    /// A mirror link's own write on this site (the sweep deleting what the
    /// peer does not have): a fresh local stamp, adjudicated like a
    /// replicated change, accepted on a read-only bucket.
    Mirror,
    /// A health-scan repair of local state: a fresh local stamp, accepted
    /// anywhere, logged as `repair` so replicas know it is not a client's
    /// change.
    Repair,
    /// Restoring a lost object to exactly this stamp over the broken current
    /// state: not adjudicated (the state it replaces is unusable). Guard it
    /// with a precondition on what is being replaced.
    Restore(Stamp),
}

impl WriteOrigin {
    /// A request's origin from its `x-rs3-stamp` header: with one it is a
    /// replicated write, without it a client's.
    pub fn stamped(stamp: Option<Stamp>) -> Self {
        match stamp {
            Some(stamp) => WriteOrigin::Replicated(stamp),
            None => WriteOrigin::Client,
        }
    }

    /// Accepted on a read-only bucket? Only a client's write is refused.
    pub fn allowed_on_read_only(&self) -> bool {
        !matches!(self, WriteOrigin::Client)
    }

    /// The stamp the new state keeps, if the write brings one; otherwise the
    /// site mints a fresh one.
    pub fn given_stamp(&self) -> Option<&Stamp> {
        match self {
            WriteOrigin::Replicated(stamp) | WriteOrigin::Restore(stamp) => Some(stamp),
            WriteOrigin::Client | WriteOrigin::Mirror | WriteOrigin::Repair => None,
        }
    }

    /// Compared against the key's current state before it applies? A restore
    /// is the one write that replaces a state regardless of rank.
    pub fn adjudicated(&self) -> bool {
        !matches!(self, WriteOrigin::Restore(_))
    }

    /// How the changelog records the change.
    pub fn change_origin(&self) -> ChangeOrigin {
        match self {
            WriteOrigin::Client => ChangeOrigin::Client,
            WriteOrigin::Replicated(_) | WriteOrigin::Mirror => ChangeOrigin::Replication,
            WriteOrigin::Repair | WriteOrigin::Restore(_) => ChangeOrigin::Repair,
        }
    }

    /// The incoming rank of a put of this origin, when it has one to compare
    /// (a given stamp); a locally stamped put outranks by construction.
    pub fn put_rank(&self) -> Option<Rank> {
        self.given_stamp().cloned().map(Rank::live)
    }
}

/// How a write is performed: its origin and an optional S3 precondition
/// (`If-None-Match: *` / `If-Match`), evaluated under the key lock.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteOptions {
    pub origin: WriteOrigin,
    pub precondition: Option<Precondition>,
}

impl WriteOptions {
    pub fn client() -> Self {
        Self::default()
    }

    /// A write of `origin`, unconditional.
    pub fn from_origin(origin: WriteOrigin) -> Self {
        Self { origin, precondition: None }
    }

    /// A replicated write keeping `stamp`.
    pub fn replicated(stamp: Stamp) -> Self {
        Self::from_origin(WriteOrigin::Replicated(stamp))
    }

    /// From a request's optional `x-rs3-stamp` (see [`WriteOrigin::stamped`]).
    pub fn stamped(stamp: Option<Stamp>) -> Self {
        Self::from_origin(WriteOrigin::stamped(stamp))
    }

    pub fn precondition(precondition: Option<Precondition>) -> Self {
        Self { origin: WriteOrigin::Client, precondition }
    }

    pub fn with_precondition(mut self, precondition: Option<Precondition>) -> Self {
        self.precondition = precondition;
        self
    }
}

/// The last-writer-wins rule, in one place: does a write ranking `incoming`
/// apply over a key whose current state ranks `current`? Equal ranks do not
/// apply (a replay or an echo is a no-op).
pub fn outranks(incoming: &Rank, current: Option<&Rank>) -> bool {
    current.is_none_or(|current| incoming > current)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(ms: i64, site: &str) -> Stamp {
        Stamp { ms, c: 0, site: site.into() }
    }

    #[test]
    fn origins_differ_only_in_the_documented_ways() {
        let replicated = WriteOrigin::Replicated(st(5, "a"));
        let restore = WriteOrigin::Restore(st(5, "a"));
        assert!(!WriteOrigin::Client.allowed_on_read_only());
        for origin in [&replicated, &WriteOrigin::Mirror, &WriteOrigin::Repair, &restore] {
            assert!(origin.allowed_on_read_only(), "{origin:?}");
        }
        assert_eq!(replicated.given_stamp(), Some(&st(5, "a")));
        assert_eq!(restore.given_stamp(), Some(&st(5, "a")));
        assert_eq!(WriteOrigin::Client.given_stamp(), None);
        assert_eq!(WriteOrigin::Mirror.given_stamp(), None);
        assert!(replicated.adjudicated() && WriteOrigin::Client.adjudicated() && WriteOrigin::Mirror.adjudicated());
        assert!(!restore.adjudicated());
        assert_eq!(WriteOrigin::Client.change_origin(), ChangeOrigin::Client);
        assert_eq!(replicated.change_origin(), ChangeOrigin::Replication);
        assert_eq!(WriteOrigin::Mirror.change_origin(), ChangeOrigin::Replication);
        assert_eq!(WriteOrigin::Repair.change_origin(), ChangeOrigin::Repair);
        assert_eq!(restore.change_origin(), ChangeOrigin::Repair);
        assert_eq!(WriteOrigin::stamped(None), WriteOrigin::Client);
        assert_eq!(WriteOrigin::stamped(Some(st(1, "a"))), WriteOrigin::Replicated(st(1, "a")));
    }

    #[test]
    fn equal_ranks_never_apply() {
        let live = Rank::live(st(5, "a"));
        assert!(outranks(&live, None));
        assert!(!outranks(&live, Some(&live)));
        assert!(outranks(&Rank::deleted(st(5, "a")), Some(&live)));
        assert!(!outranks(&Rank::live(st(5, "a")), Some(&Rank::deleted(st(5, "a")))));
        assert!(outranks(&Rank::live(st(6, "a")), Some(&Rank::deleted(st(5, "a")))));
    }
}
