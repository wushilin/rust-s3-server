//! One replication link's worker: snapshot the peer's bucket, then follow its
//! changelog, applying every change locally with the change's own stamp.
//!
//! Applying is idempotent and order-independent (last-writer-wins on
//! stamps), so the worker can always resume from an older checkpoint:
//! - a change whose stamp is not newer than the local state is skipped
//!   without a request (that is also how echoes of our own changes die);
//! - the same bytes are never copied twice: identical content with a newer
//!   stamp is a metadata-only update (hardlinks);
//! - object data is read pinned to the event's version, so a change that the
//!   peer has since superseded is skipped (a later event carries it);
//! - multipart objects are re-uploaded with the peer's part layout, so the
//!   local ETag is identical.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::model::{Checkpoint, Link, LinkStatus, Mode, Phase, RECORD_VERSION};
use super::peer::{event_stamp, PeerClient, PeerError};
use crate::server::notify::model::WireEvent;
use crate::storage::errors::StorageError;
use crate::storage::store::{CompletePartRequest, LocalObjectStore, PublishOptions};
use crate::storage::time::now_ms;

const SNAPSHOT_PAGE: usize = 200;
const CHANGES_PAGE: usize = 500;

pub(super) struct Worker {
    pub store: LocalObjectStore,
    pub link: Link,
    pub peer: PeerClient,
    pub status: Arc<Mutex<LinkStatus>>,
    pub heartbeat: Arc<AtomicI64>,
    pub cancel: CancellationToken,
    /// A checkpoint older than this (ms) must re-snapshot: tombstones that
    /// would refuse older changes may have expired.
    pub max_checkpoint_age_ms: i64,
    /// Long-poll wait while following the changelog.
    pub long_poll: Duration,
    /// Changes applied concurrently within a batch.
    pub parallelism: usize,
    /// The link's local key still exists and may do what the link does
    /// here (checked every step: revoking it stops the link).
    pub local_check: LocalCheck,
    /// Records the peer's site id on a link that has none yet (created
    /// without the check): its first contact binds it.
    pub bind_peer: BindPeer,
    /// How often a transfer in progress renews the lease (the heartbeat):
    /// every chunk counts as progress, the renewal is throttled to this.
    pub lease_renew: Duration,
}

/// Renews `heartbeat` (the worker's lease) if `renew` has passed since it
/// was last renewed. Called per chunk of a transfer, so a worker busy
/// moving one large object is never mistaken for a hung one, while a
/// worker that stalls (no chunk for `stale_after`) still is.
pub(super) fn renew_lease(heartbeat: &AtomicI64, renew: Duration) {
    let now = now_ms();
    if now - heartbeat.load(Ordering::Relaxed) >= renew.as_millis() as i64 {
        heartbeat.store(now, Ordering::SeqCst);
    }
}

/// Awaits a request to the peer, renewing `heartbeat` every `renew` while it
/// is pending. Such a wait is bounded by the client's timeouts (connect,
/// read), so it is not a hang — and there may be nothing else to observe:
/// a pushed body can sit in socket buffers, consumed from our side in an
/// instant, while the peer takes a while to receive and answer it. The
/// stale guard is for a worker stuck where no timeout applies.
pub(super) async fn alive<T>(heartbeat: &AtomicI64, renew: Duration, fut: impl std::future::Future<Output = T>) -> T {
    tokio::pin!(fut);
    let mut tick = tokio::time::interval(renew.max(Duration::from_millis(50)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            out = &mut fut => return out,
            _ = tick.tick() => heartbeat.store(now_ms(), Ordering::SeqCst),
        }
    }
}

/// A caught-up link refreshes its checkpoint this often, so an idle link
/// does not age past the tombstone retention and re-snapshot for nothing
/// (the age guard is for a reader that is behind).
pub(super) fn refresh_after_ms(max_checkpoint_age_ms: i64) -> i64 {
    (max_checkpoint_age_ms / 4).min(3_600_000).max(1)
}

pub(super) type LocalCheck = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;
pub(super) type BindPeer = Arc<dyn Fn(String) + Send + Sync>;

/// The link's own site id no longer being this site's (site ids never
/// change, so only if site.json was replaced or deleted by hand): what
/// changed.
pub(super) fn local_site_changed(link: &Link, current: &str) -> Option<String> {
    (!link.local_site_id.is_empty() && link.local_site_id != current).then(|| {
        format!(
            "this site's id changed from {} to {current} since the link was made; it stays stopped until the new \
             site ids are accepted",
            link.local_site_id
        )
    })
}

/// The peer reporting another site id than the link was made with.
pub(super) fn peer_site_changed(link: &Link, reported: &str) -> Option<String> {
    (!link.peer_site_id.is_empty() && link.peer_site_id != reported).then(|| {
        format!(
            "the peer's site id changed from {} to {reported} since the link was made (renamed, or the endpoint \
             now reaches another site); it stays stopped until the new site ids are accepted",
            link.peer_site_id
        )
    })
}

/// Why a step stopped.
#[derive(Debug)]
pub(super) enum StepError {
    /// Start over with a fresh snapshot.
    Resnapshot(String),
    /// Will not work until reconfigured (e.g. both sides report one site id).
    Fatal(String),
    /// Retry the same step after a pause.
    Retry(String),
}

impl std::fmt::Display for StepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StepError::Resnapshot(m) | StepError::Fatal(m) | StepError::Retry(m) => f.write_str(m),
        }
    }
}

impl From<StorageError> for StepError {
    fn from(err: StorageError) -> Self {
        StepError::Retry(format!("local: {err}"))
    }
}

impl From<PeerError> for StepError {
    fn from(err: PeerError) -> Self {
        match err {
            PeerError::Gone(m) => StepError::Resnapshot(format!("behind the peer's retention: {m}")),
            PeerError::LogMismatch(m) => StepError::Resnapshot(format!("the peer's changelog changed: {m}")),
            other => StepError::Retry(other.to_string()),
        }
    }
}

impl Worker {
    fn beat(&self) {
        self.heartbeat.store(now_ms(), Ordering::SeqCst);
    }

    fn update(&self, f: impl FnOnce(&mut LinkStatus)) {
        f(&mut self.status.lock().unwrap());
    }

    /// A request to the peer, the lease renewed while it is pending.
    async fn alive<T>(&self, fut: impl std::future::Future<Output = T>) -> T {
        alive(&self.heartbeat, self.lease_renew, fut).await
    }

    /// Runs until cancelled or a fatal error.
    pub async fn run(self) {
        self.update(|s| {
            s.running_since_ms = now_ms();
            s.state = "idle".to_string();
        });
        let mut backoff = Duration::from_secs(1);
        loop {
            if self.cancel.is_cancelled() {
                return;
            }
            self.beat();
            match self.step().await {
                Ok(()) => {
                    backoff = Duration::from_secs(1);
                    self.peer_answered();
                }
                Err(StepError::Resnapshot(reason)) => {
                    log::warn!("replication {} re-snapshots: {reason}", self.link.id);
                    self.record_error(&reason);
                    if let Err(err) = self.save(None).await {
                        self.record_error(&format!("could not reset checkpoint: {err}"));
                    }
                }
                Err(StepError::Fatal(reason)) => {
                    log::error!("replication {} stopped: {reason}", self.link.id);
                    self.record_error(&reason);
                    self.update(|s| s.state = "stopped".to_string());
                    // Stay stopped a while before the supervisor tries again
                    // (the configuration may get fixed meanwhile).
                    tokio::select! {
                        _ = self.cancel.cancelled() => {}
                        _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                    }
                    return;
                }
                Err(StepError::Retry(reason)) => {
                    log::warn!("replication {} retrying in {}s: {reason}", self.link.id, backoff.as_secs());
                    self.record_error(&reason);
                    self.update(|s| s.state = "error".to_string());
                    tokio::select! {
                        _ = self.cancel.cancelled() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
    }

    /// The peer answered (or a step completed): an error from before is no
    /// longer news. Cleared as soon as the answer arrives, not when a long
    /// poll eventually ends; a failure applying it sets a new one.
    fn peer_answered(&self) {
        self.update(|s| {
            if s.last_error.is_some() {
                s.last_error = None;
                s.last_error_ms = 0;
            }
        });
    }

    fn record_error(&self, reason: &str) {
        self.update(|s| {
            s.last_error = Some(reason.to_string());
            s.last_error_ms = now_ms();
        });
    }

    async fn load(&self) -> Result<Option<Checkpoint>, StepError> {
        let index = self.store.index(&self.link.local_bucket).await?;
        let Some(bytes) = index.get_checkpoint(&self.link.id).await? else {
            return Ok(None);
        };
        match serde_json::from_slice::<Checkpoint>(&bytes) {
            Ok(checkpoint) => Ok(Some(checkpoint)),
            Err(err) => Err(StepError::Resnapshot(format!("unreadable checkpoint: {err}"))),
        }
    }

    async fn save(&self, checkpoint: Option<&Checkpoint>) -> Result<(), StepError> {
        let index = self.store.index(&self.link.local_bucket).await?;
        let value = match checkpoint {
            Some(checkpoint) => {
                let mut checkpoint = checkpoint.clone();
                checkpoint.saved_at_ms = now_ms();
                Some(serde_json::to_vec(&checkpoint).expect("checkpoint serializes"))
            }
            None => None,
        };
        index.put_checkpoint(&self.link.id, value).await?;
        Ok(())
    }

    /// The peer's current name (a label it may change any time).
    fn peer_named(&self, name: &str) {
        if !name.is_empty() {
            self.update(|s| {
                if s.peer_site_name.as_deref() != Some(name) {
                    s.peer_site_name = Some(name.to_string());
                }
            });
        }
    }

    fn site_changed(&self, what: String) -> StepError {
        self.update(|s| s.site_changed = Some(what.clone()));
        StepError::Fatal(what)
    }

    fn check_site(&self, peer_site: &str) -> Result<(), StepError> {
        let local = self.store.site_id();
        if let Some(changed) = peer_site_changed(&self.link, peer_site) {
            return Err(self.site_changed(changed));
        }
        if self.link.peer_site_id.is_empty() && peer_site != local {
            (self.bind_peer)(peer_site.to_string());
        }
        if peer_site == local {
            return Err(StepError::Fatal(format!(
                "the peer reports this server's own site id ({local}): a copy of this data directory? A copy \
                 is the same site; start the second site from an empty data directory."
            )));
        }
        // Both sites configured a merge link for the same pair. One link
        // syncs both ways, so one of them stands down — the one on the
        // higher site id, so exactly one keeps running.
        if self.link.mode == Mode::Merge && local.as_str() > peer_site {
            if let Some(super::peer::MergeBack { link: id, name }) =
                self.peer.merge_back().as_deref().and_then(super::peer::MergeBack::parse)
            {
                return Err(StepError::Fatal(format!(
                    "site {peer_site} already merges this bucket pair with link \"{name}\" ({id}), which syncs both \
                     ways; this link stands down. Delete one of the two."
                )));
            }
        }
        Ok(())
    }

    /// One unit of progress: a snapshot page or a changelog batch, applied
    /// and checkpointed.
    async fn step(&self) -> Result<(), StepError> {
        if let Some(changed) = local_site_changed(&self.link, &self.store.site_id()) {
            return Err(self.site_changed(changed));
        }
        (self.local_check)().map_err(StepError::Retry)?;
        let checkpoint = match self.load().await? {
            Some(checkpoint) if now_ms() - checkpoint.saved_at_ms > self.max_checkpoint_age_ms => {
                return Err(StepError::Resnapshot(
                    "the checkpoint is older than the tombstone retention".to_string(),
                ))
            }
            other => other,
        };
        let Some(mut checkpoint) = checkpoint else {
            // Start: the first snapshot page fixes where live replay begins.
            let page = self.peer.snapshot(None, SNAPSHOT_PAGE, "", false).await?;
            self.peer_answered();
            self.check_site(&page.site_id)?;
            self.peer_named(&page.site_name);
            let mut checkpoint = Checkpoint {
                v: RECORD_VERSION,
                peer_log_id: page.log_id.clone(),
                peer_site: page.site_id.clone(),
                phase: Phase::Snapshot { through: page.head, cursor: None, tombstones: false },
                saved_at_ms: now_ms(),
            };
            self.update(|s| {
                s.state = "snapshot".to_string();
                s.peer_site = Some(page.site_id.clone());
                s.peer_head = Some(page.head);
            });
            self.apply_snapshot_page(&mut checkpoint, None, page).await?;
            return self.save(Some(&checkpoint)).await;
        };
        // Resumed from a checkpoint (e.g. after a restart): the peer is
        // known already. The console shows it, and a merge reader of ours
        // is told about this link by it (`x-rs3-merge-back`).
        self.update(|s| {
            if s.peer_site.as_deref() != Some(checkpoint.peer_site.as_str()) {
                s.peer_site = Some(checkpoint.peer_site.clone());
            }
        });
        match checkpoint.phase.clone() {
            Phase::Snapshot { cursor, tombstones: false, .. } => {
                self.update(|s| s.state = "snapshot".to_string());
                let page = self.peer.snapshot(cursor.as_deref(), SNAPSHOT_PAGE, "", false).await?;
                self.peer_answered();
                if page.log_id != checkpoint.peer_log_id {
                    return Err(StepError::Resnapshot("the peer's changelog changed mid-snapshot".to_string()));
                }
                self.apply_snapshot_page(&mut checkpoint, cursor, page).await?;
                self.save(Some(&checkpoint)).await
            }
            Phase::Snapshot { through, cursor, tombstones: true } => {
                // Merge mode: the peer's remembered deletes, so a key deleted
                // there does not survive here.
                let page = self.peer.snapshot(cursor.as_deref(), SNAPSHOT_PAGE, "", true).await?;
                self.peer_answered();
                self.apply_all(&page.entries, &checkpoint.peer_site).await?;
                checkpoint.phase = match page.next_after {
                    Some(next) => Phase::Snapshot { through, cursor: Some(next), tombstones: true },
                    None => Phase::Live { after: through },
                };
                self.save(Some(&checkpoint)).await
            }
            Phase::Live { after } => {
                self.update(|s| {
                    s.state = "live".to_string();
                    s.position = Some(after);
                    s.snapshot_cursor = None;
                });
                // Right after an error, ask without waiting: a quick answer
                // confirms the peer is back (and clears the error) instead of
                // a long poll holding it for up to `long_poll`.
                let recovering = self.status.lock().unwrap().last_error.is_some();
                let wait = if recovering { Duration::ZERO } else { self.long_poll };
                let page = tokio::select! {
                    _ = self.cancel.cancelled() => return Ok(()),
                    page = self.peer.changes(after, CHANGES_PAGE, wait, "", Some(&checkpoint.peer_log_id)) => page?,
                };
                self.peer_answered();
                self.check_site(&page.site_id)?;
                self.peer_named(&page.site_name);
                self.apply_all(&page.events, &checkpoint.peer_site).await?;
                checkpoint.phase = Phase::Live { after: page.next };
                self.update(|s| {
                    s.position = Some(page.next);
                    s.peer_head = Some(page.head);
                    s.last_progress_ms = now_ms();
                });
                let idle_too_long = now_ms() - checkpoint.saved_at_ms > refresh_after_ms(self.max_checkpoint_age_ms);
                if page.next != after || idle_too_long {
                    self.save(Some(&checkpoint)).await?;
                }
                Ok(())
            }
        }
    }

    /// Applies one page of the peer's objects; in mirror mode also removes
    /// local keys in the page's key range that the peer does not have.
    async fn apply_snapshot_page(
        &self,
        checkpoint: &mut Checkpoint,
        from: Option<String>,
        page: crate::server::notify::read::SnapshotResponse,
    ) -> Result<(), StepError> {
        let Phase::Snapshot { through, .. } = checkpoint.phase else {
            return Ok(());
        };
        self.apply_all(&page.entries, &checkpoint.peer_site).await?;
        if self.link.mode == Mode::Mirror {
            let upto = page.next_after.clone();
            let present: std::collections::HashSet<&str> = page.entries.iter().map(|e| e.key.as_str()).collect();
            self.sweep(from.as_deref(), upto.as_deref(), &present).await?;
        }
        self.update(|s| {
            s.snapshot_cursor = page.next_after.clone();
            s.last_progress_ms = now_ms();
        });
        checkpoint.phase = match (page.next_after, self.link.mode) {
            (Some(next), _) => Phase::Snapshot { through, cursor: Some(next), tombstones: false },
            (None, Mode::Merge) => Phase::Snapshot { through, cursor: None, tombstones: true },
            (None, Mode::Mirror) => Phase::Live { after: through },
        };
        Ok(())
    }

    /// Mirror mode: deletes local keys in `(from, upto]` (to the end when
    /// `upto` is `None`) that are not in `present`.
    async fn sweep(
        &self,
        from: Option<&str>,
        upto: Option<&str>,
        present: &std::collections::HashSet<&str>,
    ) -> Result<(), StepError> {
        let bucket = &self.link.local_bucket;
        let mut after = from.map(str::to_string);
        loop {
            let page = self
                .store
                .list_objects(bucket, "", None, after.as_deref(), 1000)
                .await?;
            for entry in &page.entries {
                if upto.is_some_and(|upto| entry.object_key.as_str() > upto) {
                    return Ok(());
                }
                if !present.contains(entry.object_key.as_str()) {
                    // A replication write (stamped), so a read-only replica
                    // accepts it; it removes exactly the version seen here.
                    let removed = self
                        .store
                        .effective_stamp(entry.stamp.as_ref(), entry.last_modified_ms);
                    match self
                        .store
                        .delete_object_stamped(bucket, &entry.object_key, Some(self.store.next_stamp()), Some(removed))
                        .await
                    {
                        Ok(_) => self.update(|s| s.deleted += 1),
                        // Changed under us: the next snapshot or event decides.
                        Err(StorageError::StaleStamp { .. }) => {}
                        Err(err) => return Err(err.into()),
                    }
                }
            }
            if !page.is_truncated {
                return Ok(());
            }
            after = page.next_after;
        }
    }

    /// Applies a batch: only the latest change per key (the earlier ones are
    /// superseded — applying them would fetch data only to discard it), up
    /// to `parallelism` at a time. Safe in any order: the last-writer-wins
    /// check and the commit are atomic under each key's lock, so concurrent
    /// applies of one key still end at the highest stamp. Returns only once
    /// every change is applied, so the caller's checkpoint never runs ahead.
    pub(super) async fn apply_all(&self, events: &[WireEvent], peer_site: &str) -> Result<(), StepError> {
        use futures::StreamExt;
        let (batch, collapsed) = latest_per_key(events);
        if collapsed > 0 {
            self.update(|s| s.collapsed += collapsed as u64);
        }
        let mut results = futures::stream::iter(batch)
            .map(|event: WireEvent| async move { self.apply(&event, peer_site).await })
            .buffer_unordered(self.parallelism.max(1));
        let mut first_error = None;
        while let Some(result) = results.next().await {
            if let Err(err) = result {
                first_error.get_or_insert(err);
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Applies one change (or snapshot entry) with its own stamp.
    async fn apply(&self, event: &WireEvent, peer_site: &str) -> Result<(), StepError> {
        self.beat();
        let bucket = &self.link.local_bucket;
        let stamp = event_stamp(event, peer_site);
        let is_delete = matches!(event.op.as_str(), "delete" | "deleted");
        // A delete ranks by the version it removed (falling back to its own
        // stamp for changes logged before that was recorded).
        let removed = event.removed_stamp.clone().unwrap_or_else(|| stamp.clone());
        let incoming = if is_delete {
            crate::storage::stamp::Rank::deleted(removed.clone())
        } else {
            crate::storage::stamp::Rank::live(stamp.clone())
        };
        // Not ranked above what is here: nothing to fetch (an echo, a replay,
        // or a change that lost to another).
        if let Some(current) = self.store.key_rank(bucket, &event.key).await? {
            if current >= incoming {
                self.update(|s| s.skipped += 1);
                return Ok(());
            }
        }
        let result = match event.op.as_str() {
            "delete" | "deleted" => {
                if event.origin.as_deref() == Some("repair") {
                    // The peer lost its copy; that is not a client's delete.
                    // Keep ours (the peer can restore from it).
                    self.update(|s| s.skipped += 1);
                    return Ok(());
                }
                self.store
                    .delete_object_stamped(bucket, &event.key, Some(stamp), Some(removed))
                    .await
                    .map(|_| self.update(|s| s.deleted += 1))
            }
            _ => match self.apply_put(event, stamp, true).await {
                Ok(()) => Ok(()),
                Err(PutError::Peer(PeerError::Superseded | PeerError::NotFound)) => {
                    // Changed or deleted on the peer since: a later event
                    // carries its current state.
                    self.update(|s| s.skipped += 1);
                    Ok(())
                }
                Err(PutError::Peer(err)) => return Err(err.into()),
                Err(PutError::Local(err)) => Err(err),
            },
        };
        match result {
            Ok(()) => Ok(()),
            Err(StorageError::StaleStamp { .. }) => {
                // A newer state landed meanwhile.
                self.update(|s| s.superseded += 1);
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Restores a lost object from this link's peer: the bytes of the peer's
    /// state `event`, published with `options` (a restore stamp, guarded).
    pub async fn restore(&self, event: &WireEvent, options: PublishOptions) -> Result<(), String> {
        let stamp = options.stamp.clone().ok_or("a restore needs the stamp")?;
        match self.copy(event, stamp, options).await {
            Ok(()) => Ok(()),
            Err(PutError::Peer(err)) => Err(format!("peer: {err}")),
            Err(PutError::Local(err)) => Err(format!("local: {err}")),
        }
    }

    async fn apply_put(&self, event: &WireEvent, stamp: crate::storage::stamp::Stamp, metadata_only_ok: bool) -> Result<(), PutError> {
        let bucket = &self.link.local_bucket;
        let key = &event.key;
        let local = self.store.index(bucket).await.map_err(PutError::Local)?.get(key).await.map_err(PutError::Local)?;
        let same_bytes = local.filter(|row| metadata_only_ok && row.etag == event.etag && row.size == event.size);
        if let Some(local) = same_bytes {
            // Same bytes here already: adopt the peer's metadata and stamp
            // without moving data.
            let _ = local;
            let headers = self.alive(self.peer.head(key, &event.version)).await.map_err(PutError::Peer)?;
            self.store
                .copy_object_with_metadata(
                    bucket,
                    key,
                    bucket,
                    key,
                    headers.storage_class.as_deref(),
                    Some(&headers.user_meta),
                    headers.content_type.as_deref(),
                    headers.content_language.as_deref(),
                    PublishOptions { precondition: None, stamp: Some(stamp), ..Default::default() },
                )
                .await
                .map_err(PutError::Local)?;
            self.update(|s| s.metadata_only += 1);
            return Ok(());
        }
        let options = PublishOptions { precondition: None, stamp: Some(stamp.clone()), ..Default::default() };
        self.copy(event, stamp, options).await
    }

    /// Copies the bytes of the peer's state `event` (with its layout).
    async fn copy(&self, event: &WireEvent, stamp: crate::storage::stamp::Stamp, options: PublishOptions) -> Result<(), PutError> {
        let bucket = &self.link.local_bucket;
        let key = &event.key;
        let _ = &stamp;
        match event.parts.as_ref().filter(|layout| layout.multipart) {
            Some(layout) => self.copy_multipart(event, layout, options).await,
            None => {
                use futures::StreamExt;
                let (headers, stream) = self.alive(self.peer.body(key, &event.version, None)).await.map_err(PutError::Peer)?;
                let stream = stream.inspect(|_| renew_lease(&self.heartbeat, self.lease_renew));
                self.store
                    .put_object_stream_with_metadata(
                        bucket,
                        key,
                        stream,
                        headers.content_type.as_deref(),
                        headers.content_encoding.as_deref(),
                        headers.storage_class.as_deref(),
                        headers.content_language.as_deref(),
                        &headers.user_meta,
                        false,
                        None,
                        None,
                        options,
                    )
                    .await
                    .map_err(PutError::Local)?;
                self.update(|s| {
                    s.copied += 1;
                    s.copied_bytes += event.size;
                });
                Ok(())
            }
        }
    }

    /// Re-uploads a multipart object with the peer's part layout, so the
    /// local ETag is identical.
    async fn copy_multipart(
        &self,
        event: &WireEvent,
        layout: &crate::storage::changelog::PartLayout,
        options: PublishOptions,
    ) -> Result<(), PutError> {
        let bucket = &self.link.local_bucket;
        let key = &event.key;
        let headers = self.alive(self.peer.head(key, &event.version)).await.map_err(PutError::Peer)?;
        let upload_id = self
            .store
            .initiate_multipart_replicated(
                bucket,
                key,
                headers.content_type.as_deref(),
                headers.content_encoding.as_deref(),
                headers.storage_class.as_deref(),
                headers.content_language.as_deref(),
                &headers.user_meta,
            )
            .await
            .map_err(PutError::Local)?;
        let result: Result<(), PutError> = async {
            let mut parts = Vec::new();
            let mut offset = 0u64;
            for (i, size) in layout.part_sizes(event.size).into_iter().enumerate() {
                self.beat();
                let number = (i + 1) as u16;
                let range = if size == 0 { None } else { Some((offset, offset + size - 1)) };
                use futures::StreamExt;
                let (_, stream) = self.alive(self.peer.body(key, &event.version, range)).await.map_err(PutError::Peer)?;
                let stream = stream.inspect(|_| renew_lease(&self.heartbeat, self.lease_renew));
                let put = self
                    .store
                    .put_multipart_part_stream(bucket, key, &upload_id, number, stream, false, None, None)
                    .await
                    .map_err(PutError::Local)?;
                parts.push(CompletePartRequest { number, etag: put.etag });
                offset += size;
            }
            let done = self
                .store
                .complete_multipart_with(bucket, key, &upload_id, &parts, options)
                .await
                .map_err(PutError::Local)?;
            if done.etag != event.etag {
                log::warn!(
                    "replication {}: {key} copied with ETag {} but the peer's is {} (layout mismatch)",
                    self.link.id,
                    done.etag,
                    event.etag
                );
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = self.store.abort_multipart(bucket, key, &upload_id).await;
        } else {
            self.update(|s| {
                s.copied += 1;
                s.copied_bytes += event.size;
            });
        }
        result
    }
}

/// The latest change per key of a batch, in log order (the earlier ones are
/// superseded — applying them would fetch data only to discard it), and how
/// many were dropped.
pub(super) fn latest_per_key(events: &[WireEvent]) -> (Vec<WireEvent>, usize) {
    let mut latest: std::collections::HashMap<&str, &WireEvent> = std::collections::HashMap::new();
    for event in events {
        // Later in the batch = later in the log (snapshot pages hold each
        // key once).
        latest.insert(event.key.as_str(), event);
    }
    let collapsed = events.len() - latest.len();
    let mut batch: Vec<WireEvent> = latest.into_values().cloned().collect();
    batch.sort_by_key(|e| e.seq.unwrap_or(0));
    (batch, collapsed)
}

enum PutError {
    Peer(PeerError),
    Local(StorageError),
}
