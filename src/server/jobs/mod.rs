//! Background jobs — verbs the server runs on its own initiative.
//!
//! A job is a verb like any HTTP handler, with one difference: it is triggered
//! by the scheduler (an interval timer, startup), never by the UI or an HTTP
//! route, and it returns a plain outcome rather than an HTTP `Response`. The
//! same rules apply: one self-contained module per job, no cross-job code
//! sharing, shared logic in the `server`/storage libraries. Every run carries a
//! correlation id and registers in the task registry, so a stuck job is
//! visible (with its type and start time) and can be cancelled centrally.
//!
//! Principle: if the server is doing something, it is a verb — whether a client
//! asked for it or the scheduler did. Each maintenance purpose is its own job
//! so they can run at independent frequencies without overlapping.

#[path = "delete_staging/lib.rs"]
pub(crate) mod delete_staging;
#[path = "delete_trash/lib.rs"]
pub(crate) mod delete_trash;
#[path = "migrate_layout/lib.rs"]
pub(crate) mod migrate_layout;
#[path = "reclaim/lib.rs"]
pub(crate) mod reclaim;
#[path = "rebuild_index/lib.rs"]
pub(crate) mod rebuild_index;
#[path = "resolve_intents/lib.rs"]
pub(crate) mod resolve_intents;
