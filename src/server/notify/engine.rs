//! Turns a subscription's position into its next batch. Pure with respect to
//! the subscription: reads the bucket's index, never writes anything — the
//! caller persists the returned state.

use super::model::{
    Batch, Pause, PauseReason, Phase, Position, SnapshotState, SubState, Subscription, WireEvent,
};
use crate::storage::errors::Result;
use crate::storage::index::ObjectIndex;
use crate::storage::time::now_ms;

/// Batch size limits after per-subscription overrides.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_events: usize,
    pub max_bytes: usize,
}

#[derive(Debug)]
pub enum Next {
    /// Nothing to deliver. `position` may have moved past filtered-out
    /// changes; persist it if it differs.
    UpToDate { position: Position },
    /// Deliver `batch`; on success the position becomes `after`.
    Deliver { batch: Batch, after: Position },
    /// The subscription cannot continue.
    Pause(Pause),
}

/// The message shown when an offset fell behind retention — on the pause
/// itself and on every refused resume.
pub fn expired_message(sub: &Subscription, offset: u64, trimmed_through: u64, head: u64) -> String {
    format!(
        "Cannot resume {} \"{}\" (bucket \"{}\"): its offset is seq {offset}, but the oldest retained \
         changelog entry is after seq {trimmed_through}. Events {}–{trimmed_through} have been deleted by \
         retention and can't be delivered. Choose one of: resume from oldest retained (seq {}), \
         re-snapshot, skip to head (seq {head}), or set offset.",
        kind_label(sub),
        sub.name,
        sub.bucket,
        offset + 1,
        trimmed_through + 1,
    )
}

pub fn log_replaced_message(sub: &Subscription) -> String {
    format!(
        "Cannot resume {} \"{}\" (bucket \"{}\"): the bucket's changelog was replaced (an index \
         rebuild could not carry it over), so its offset no longer refers to anything. Choose one of: \
         re-snapshot, skip to head, or set offset.",
        kind_label(sub),
        sub.name,
        sub.bucket
    )
}

fn kind_label(sub: &Subscription) -> &'static str {
    match sub.kind {
        super::model::SubscriptionKind::Webhook => "webhook",
        super::model::SubscriptionKind::Pull => "replication token",
    }
}

/// Checks whether the position can continue in this log; `Some(pause)` when
/// it cannot.
pub async fn check_continuity(
    index: &ObjectIndex,
    sub: &Subscription,
    position: &Position,
) -> Result<Option<Pause>> {
    if position.log_id != index.changelog_id() {
        return Ok(Some(Pause {
            reason: PauseReason::LogReplaced,
            message: log_replaced_message(sub),
            at_ms: now_ms(),
        }));
    }
    let status = index.changelog_status().await?;
    // A snapshot in progress resumes live from `through`; that is the offset
    // that must still be retained.
    let offset = position.snapshot.as_ref().map(|s| s.through).unwrap_or(position.offset);
    if offset < status.trimmed_through {
        return Ok(Some(Pause {
            reason: PauseReason::Expired,
            message: expired_message(sub, offset, status.trimmed_through, status.head),
            at_ms: now_ms(),
        }));
    }
    if offset > status.head {
        // Only possible if the log went backwards without its id changing.
        return Ok(Some(Pause {
            reason: PauseReason::LogReplaced,
            message: log_replaced_message(sub),
            at_ms: now_ms(),
        }));
    }
    Ok(None)
}

/// The next batch for `state`.
pub async fn next(index: &ObjectIndex, sub: &Subscription, state: &SubState, limits: Limits) -> Result<Next> {
    let position = state.position();
    if let Some(pause) = check_continuity(index, sub, &position).await? {
        return Ok(Next::Pause(pause));
    }
    match &position.snapshot {
        Some(snapshot) => next_snapshot(index, sub, &position, snapshot, limits).await,
        None => next_live(index, sub, position, limits).await,
    }
}

async fn next_live(index: &ObjectIndex, sub: &Subscription, mut position: Position, limits: Limits) -> Result<Next> {
    loop {
        let page = index
            .read_changelog(position.offset, limits.max_events, limits.max_bytes)
            .await?;
        let events: Vec<WireEvent> = page
            .events
            .iter()
            .filter(|event| sub.filter.matches(event))
            .map(WireEvent::from)
            .collect();
        if !events.is_empty() {
            let batch = Batch {
                subscription: sub.id.clone(),
                bucket: sub.bucket.clone(),
                phase: Phase::Live,
                from_seq: position.offset + 1,
                to_seq: page.covered_through,
                snapshot_begin: false,
                snapshot_end: false,
                snapshot_through: None,
                events,
            };
            let after = Position {
                offset: page.covered_through,
                ..position
            };
            return Ok(Next::Deliver { batch, after });
        }
        // Everything read was filtered out (or there was nothing): skip it.
        let advanced = page.covered_through > position.offset;
        position.offset = position.offset.max(page.covered_through);
        if !(advanced && page.more) {
            return Ok(Next::UpToDate { position });
        }
    }
}

async fn next_snapshot(
    index: &ObjectIndex,
    sub: &Subscription,
    position: &Position,
    snapshot: &SnapshotState,
    limits: Limits,
) -> Result<Next> {
    // Walk the live index (no RocksDB snapshot is held across batches:
    // replaying the log from `through` repairs anything that changed during
    // the walk).
    let mut entries = Vec::new();
    let mut cursor = snapshot.cursor.clone();
    let mut truncated = true;
    let mut bytes = 0usize;
    // Bounds the walk when a suffix filter rejects most keys: an empty,
    // non-final batch is fine, an unbounded walk is not.
    let mut scan_budget = limits.max_events.saturating_mul(10).max(1000);
    if sub.filter.wants_puts() {
        while truncated && entries.len() < limits.max_events && bytes < limits.max_bytes && scan_budget > 0 {
            let want = limits.max_events - entries.len();
            let page = index
                .list(&sub.filter.prefix, None, cursor.as_deref(), want)
                .await?;
            truncated = page.is_truncated;
            if page.entries.is_empty() {
                truncated = false;
                break;
            }
            scan_budget = scan_budget.saturating_sub(page.entries.len());
            for record in page.entries {
                cursor = Some(record.object_key.clone());
                if !sub.filter.matches_key(&record.object_key) {
                    continue;
                }
                bytes += record.object_key.len() + 128;
                entries.push(WireEvent {
                    seq: None,
                    op: "exists".to_string(),
                    key: record.object_key.clone(),
                    etag: record.etag.clone(),
                    size: record.size,
                    last_modified_ms: record.last_modified_ms,
                    version: crate::storage::changelog::version_of(&record.blob_dir),
                    origin: None,
                    ts_ms: None,
                });
                if bytes >= limits.max_bytes {
                    break;
                }
            }
            if bytes >= limits.max_bytes {
                // Stopped mid-page: there may be more.
                truncated = true;
            }
        }
    } else {
        truncated = false;
    }
    let end = !truncated;
    let batch = Batch {
        subscription: sub.id.clone(),
        bucket: sub.bucket.clone(),
        phase: Phase::Snapshot,
        from_seq: snapshot.through,
        to_seq: snapshot.through,
        snapshot_begin: !snapshot.begun,
        snapshot_end: end,
        snapshot_through: Some(snapshot.through),
        events: entries,
    };
    let after = if end {
        Position {
            log_id: position.log_id.clone(),
            offset: snapshot.through,
            snapshot: None,
        }
    } else {
        Position {
            log_id: position.log_id.clone(),
            offset: position.offset,
            snapshot: Some(SnapshotState {
                through: snapshot.through,
                cursor,
                begun: true,
            }),
        }
    };
    Ok(Next::Deliver { batch, after })
}
