//! The push half of a merge link: the local bucket's changes, applied to the
//! peer with their own stamps — the pull worker's mirror image. Snapshot the
//! local bucket (objects, then tombstones) to the peer, then follow the local
//! changelog.
//!
//! Safe to repeat, like the pull side: before sending anything the peer's
//! state of the key is asked for (a HEAD), and a change not ranked above it
//! is skipped; the peer refuses a stale stamped write anyway (412). Changes
//! that came from the peer (their stamp carries the peer's site) are not
//! sent back while following the log.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::model::{Checkpoint, Link, LinkStatus, Phase, PushCheckpoint, PushStatus, RECORD_VERSION};
use super::change::{Change, Kind};
use super::peer::{ObjectHeaders, PeerClient, PeerError};
use super::worker::{latest_per_key, StepError};
use crate::server::notify::model::{Filter, WireEvent};
use crate::server::notify::read::{self, ReadError, ReadParams, SnapshotParams};
use crate::storage::errors::StorageError;
use crate::storage::write::outranks;
use crate::storage::time::now_ms;

const SNAPSHOT_PAGE: usize = 200;
const CHANGES_PAGE: usize = 500;

/// The push checkpoint's key in the local bucket's index.
pub(super) fn checkpoint_key(link_id: &str) -> String {
    format!("{link_id}:push")
}

pub(super) struct Pusher {
    pub store: crate::storage::store::LocalObjectStore,
    pub link: Link,
    pub peer: PeerClient,
    pub status: Arc<Mutex<LinkStatus>>,
    pub heartbeat: Arc<AtomicI64>,
    pub cancel: CancellationToken,
    pub max_checkpoint_age_ms: i64,
    pub long_poll: Duration,
    pub parallelism: usize,
    pub local_check: super::worker::LocalCheck,
    pub lease_renew: Duration,
}

impl From<ReadError> for StepError {
    fn from(err: ReadError) -> Self {
        match err {
            ReadError::Gone { message, .. } => StepError::Resnapshot(format!("behind the local retention: {message}")),
            ReadError::LogMismatch { message, .. } => StepError::Resnapshot(format!("the local changelog changed: {message}")),
            ReadError::BadRequest(message) => StepError::Retry(message),
            ReadError::Storage(err) => err.into(),
        }
    }
}

impl Pusher {
    fn beat(&self) {
        self.heartbeat.store(now_ms(), Ordering::SeqCst);
    }

    fn update(&self, f: impl FnOnce(&mut PushStatus)) {
        f(self.status.lock().unwrap().push.get_or_insert_with(PushStatus::default));
    }

    /// A request to the peer, the lease renewed while it is pending.
    async fn alive<T>(&self, fut: impl std::future::Future<Output = T>) -> T {
        super::worker::alive(&self.heartbeat, self.lease_renew, fut).await
    }

    fn record_error(&self, reason: &str) {
        self.update(|s| {
            s.last_error = Some(reason.to_string());
            s.last_error_ms = now_ms();
        });
    }

    fn peer_answered(&self) {
        self.update(|s| {
            s.last_error = None;
            s.last_error_ms = 0;
        });
    }

    /// The link is stopped (by the pull side, or a site id change): push
    /// nothing.
    fn held(&self) -> bool {
        let status = self.status.lock().unwrap();
        status.state == "stopped" || status.site_changed.is_some()
    }

    pub async fn run(self) {
        self.update(|s| s.state = "idle".to_string());
        let mut backoff = Duration::from_secs(1);
        loop {
            if self.cancel.is_cancelled() {
                return;
            }
            self.beat();
            // The pull side found this pair served by a link on the peer:
            // stand down with it.
            if self.held() {
                self.update(|s| s.state = "stopped".to_string());
                tokio::select! {
                    _ = self.cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
                }
            }
            match self.step().await {
                Ok(()) => backoff = Duration::from_secs(1),
                Err(StepError::Resnapshot(reason)) => {
                    log::warn!("replication {} push re-snapshots: {reason}", self.link.id);
                    self.record_error(&reason);
                    if let Err(err) = self.save(None).await {
                        self.record_error(&format!("could not reset the push checkpoint: {err}"));
                    }
                }
                Err(StepError::Fatal(reason) | StepError::Retry(reason)) => {
                    log::warn!("replication {} push retrying in {}s: {reason}", self.link.id, backoff.as_secs());
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

    async fn load(&self) -> Result<Option<PushCheckpoint>, StepError> {
        let index = self.store.index(&self.link.local_bucket).await?;
        let Some(bytes) = index.get_checkpoint(&checkpoint_key(&self.link.id)).await? else {
            return Ok(None);
        };
        match serde_json::from_slice::<PushCheckpoint>(&bytes) {
            Ok(checkpoint) => Ok(Some(checkpoint)),
            Err(err) => Err(StepError::Resnapshot(format!("unreadable push checkpoint: {err}"))),
        }
    }

    async fn save(&self, checkpoint: Option<&PushCheckpoint>) -> Result<(), StepError> {
        let index = self.store.index(&self.link.local_bucket).await?;
        let value = checkpoint.map(|checkpoint| {
            let mut checkpoint = checkpoint.clone();
            checkpoint.saved_at_ms = now_ms();
            serde_json::to_vec(&checkpoint).expect("checkpoint serializes")
        });
        index.put_checkpoint(&checkpoint_key(&self.link.id), value).await?;
        Ok(())
    }

    /// The pull side's view of the peer's changelog, once it has one.
    async fn pull_log_id(&self) -> Result<Option<String>, StepError> {
        let index = self.store.index(&self.link.local_bucket).await?;
        Ok(index
            .get_checkpoint(&self.link.id)
            .await?
            .and_then(|bytes| serde_json::from_slice::<Checkpoint>(&bytes).ok())
            .map(|checkpoint| checkpoint.peer_log_id))
    }

    async fn step(&self) -> Result<(), StepError> {
        if let Some(changed) = super::worker::local_site_changed(&self.link, &self.store.site_id()) {
            self.status.lock().unwrap().site_changed = Some(changed.clone());
            return Err(StepError::Fatal(changed));
        }
        (self.local_check)().map_err(StepError::Retry)?;
        let bucket = self.link.local_bucket.clone();
        // Held only to read the log's identity, never across a long poll or
        // the peer's writes: an index rebuild waits for in-flight handles.
        let (log_id, head) = {
            let index = self.store.index(&bucket).await?;
            (index.changelog_id().to_string(), index.changelog_head())
        };
        let checkpoint = match self.load().await? {
            Some(checkpoint) if now_ms() - checkpoint.saved_at_ms > self.max_checkpoint_age_ms => {
                return Err(StepError::Resnapshot("the push checkpoint is older than the tombstone retention".into()))
            }
            Some(checkpoint) if checkpoint.local_log_id != log_id => {
                return Err(StepError::Resnapshot("the local changelog was replaced".into()))
            }
            other => other,
        };
        let Some(mut checkpoint) = checkpoint else {
            // Start: learn who the peer is, then list from the local head.
            let probe = self.alive(self.peer.snapshot(None, 1, "", false)).await?;
            self.peer_answered();
            if let Some(changed) = super::worker::peer_site_changed(&self.link, &probe.site_id) {
                return Err(StepError::Fatal(changed));
            }
            if probe.site_id == self.store.site_id() {
                return Err(StepError::Retry(format!(
                    "the peer reports this server's own site id ({}): a copied data directory?",
                    probe.site_id
                )));
            }
            let checkpoint = PushCheckpoint {
                v: RECORD_VERSION,
                local_log_id: log_id,
                peer_log_id: probe.log_id,
                peer_site: probe.site_id,
                phase: Phase::Snapshot { through: head, cursor: None, tombstones: false },
                saved_at_ms: now_ms(),
            };
            self.update(|s| s.state = "snapshot".to_string());
            return self.save(Some(&checkpoint)).await;
        };
        // The peer's log was replaced (the pull side re-snapshotted onto a
        // new one): it may have lost what was pushed, so push it all again.
        if let Some(pulled) = self.pull_log_id().await? {
            if pulled != checkpoint.peer_log_id {
                return Err(StepError::Resnapshot("the peer's changelog was replaced".into()));
            }
        }
        match checkpoint.phase.clone() {
            Phase::Snapshot { through, cursor, tombstones } => {
                self.update(|s| s.state = "snapshot".to_string());
                let params = SnapshotParams { after: cursor, limit: SNAPSHOT_PAGE, filter: Filter::default(), deleted: tombstones };
                let page = read::snapshot(&self.store, &bucket, &params).await?;
                self.apply_all(&page.entries, &checkpoint.peer_site, false).await?;
                self.peer_answered();
                checkpoint.phase = match (page.next_after.clone(), tombstones) {
                    (Some(next), _) => Phase::Snapshot { through, cursor: Some(next), tombstones },
                    (None, false) => Phase::Snapshot { through, cursor: None, tombstones: true },
                    (None, true) => Phase::Live { after: through },
                };
                self.update(|s| {
                    s.snapshot_cursor = page.next_after;
                    s.last_progress_ms = now_ms();
                });
                self.save(Some(&checkpoint)).await
            }
            Phase::Live { after } => {
                self.update(|s| {
                    s.state = "live".to_string();
                    s.position = Some(after);
                    s.snapshot_cursor = None;
                });
                // After an error, confirm the peer is back before the long
                // poll on local changes: a quiet bucket would otherwise show
                // the error until its next write.
                let recovering = self.status.lock().unwrap().push.as_ref().is_some_and(|p| p.last_error.is_some());
                if recovering {
                    self.peer.access().await?;
                    self.peer_answered();
                }
                let params = ReadParams {
                    after,
                    limit: CHANGES_PAGE,
                    wait: self.long_poll,
                    min: 1,
                    expect_log_id: Some(checkpoint.local_log_id.clone()),
                    ..ReadParams::default()
                };
                let page = tokio::select! {
                    _ = self.cancel.cancelled() => return Ok(()),
                    page = read::read(&self.store, &bucket, &params) => page?,
                };
                // Stopped while this waited for changes: keep them for later.
                if self.held() {
                    return Ok(());
                }
                self.apply_all(&page.events, &checkpoint.peer_site, true).await?;
                if !page.events.is_empty() {
                    self.peer_answered();
                }
                checkpoint.phase = Phase::Live { after: page.next };
                self.update(|s| {
                    s.position = Some(page.next);
                    s.local_head = Some(page.head);
                    s.last_progress_ms = now_ms();
                });
                let idle_too_long =
                    now_ms() - checkpoint.saved_at_ms > super::worker::refresh_after_ms(self.max_checkpoint_age_ms);
                if page.next != after || idle_too_long {
                    self.save(Some(&checkpoint)).await?;
                }
                Ok(())
            }
        }
    }

    /// Applies a batch to the peer, latest change per key, `parallelism` at a
    /// time; returns only once all are applied (the checkpoint never runs
    /// ahead).
    async fn apply_all(&self, events: &[WireEvent], peer_site: &str, live: bool) -> Result<(), StepError> {
        use futures::StreamExt;
        let (batch, collapsed) = latest_per_key(events);
        if collapsed > 0 {
            self.update(|s| s.collapsed += collapsed as u64);
        }
        let mut results = futures::stream::iter(batch)
            .map(|event: WireEvent| async move { self.apply(&event, peer_site, live).await })
            .buffer_unordered(self.parallelism.max(1));
        let mut first_error = None;
        while let Some(result) = results.next().await {
            if let Err(err) = result {
                first_error.get_or_insert(err);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn apply(&self, event: &WireEvent, peer_site: &str, live: bool) -> Result<(), StepError> {
        self.beat();
        let change = Change::from_event(event, &self.store.site_id()).map_err(StepError::Fatal)?;
        // Came from the peer (pulled by this link): it has it, or newer.
        if live && change.stamp.site == peer_site {
            self.update(|s| s.skipped += 1);
            return Ok(());
        }
        if change.is_repair_delete() {
            // A lost local copy is not a client's delete.
            self.update(|s| s.skipped += 1);
            return Ok(());
        }
        let remote = self.alive(self.peer.state(&change.key)).await.map_err(StepError::from)?;
        // Still the site the link was made with? Checked before every write.
        if let Some(changed) = self.peer.last_site().and_then(|site| super::worker::peer_site_changed(&self.link, &site)) {
            self.status.lock().unwrap().site_changed = Some(changed.clone());
            return Err(StepError::Fatal(changed));
        }
        if !outranks(&change.rank(), remote.as_ref().map(|(rank, _)| rank)) {
            self.update(|s| s.skipped += 1);
            return Ok(());
        }
        let result = match &change.kind {
            Kind::Delete { removed } => self
                .alive(self.peer.delete(&change.key, &change.stamp, removed))
                .await
                .map(|()| self.update(|s| s.deleted += 1)),
            Kind::Put => {
                let remote_headers = remote.and_then(|(_, headers)| headers);
                match self.push_put(&change, remote_headers).await {
                    Ok(()) => Ok(()),
                    Err(PushError::Gone) => {
                        // Changed or deleted here since: a later event carries it.
                        self.update(|s| s.skipped += 1);
                        return Ok(());
                    }
                    Err(PushError::Local(err)) => return Err(err.into()),
                    Err(PushError::Peer(err)) => Err(err),
                }
            }
        };
        match result {
            Ok(()) => Ok(()),
            Err(PeerError::Stale) => {
                self.update(|s| s.superseded += 1);
                Ok(())
            }
            Err(err) => Err(StepError::Retry(format!("peer: {err}"))),
        }
    }

    /// Sends the local object of `change` (exactly its version) to the peer:
    /// metadata only when the peer holds the same bytes, else the data, with
    /// the original part layout for multipart objects (same ETag).
    async fn push_put(&self, change: &Change, remote: Option<ObjectHeaders>) -> Result<(), PushError> {
        let bucket = &self.link.local_bucket;
        let key = &change.key;
        let stamp = &change.stamp;
        let object = match self.store.read_object(bucket, key).await {
            Ok(object) => object,
            Err(StorageError::ObjectNotFound { .. }) => return Err(PushError::Gone),
            Err(err) => return Err(PushError::Local(err)),
        };
        if crate::storage::changelog::version_of(&object.object_dir.to_string_lossy()) != change.version {
            return Err(PushError::Gone);
        }
        let meta = ObjectHeaders {
            content_type: Some(object.meta.content_type.clone()),
            content_encoding: object.meta.content_encoding.clone(),
            content_language: object.meta.content_language.clone(),
            storage_class: Some(object.meta.storage_class.clone()),
            user_meta: object.meta.user_meta.clone(),
            etag: object.meta.etag.clone(),
            size: object.meta.size,
        };
        if remote.is_some_and(|r| r.etag == object.meta.etag && r.size == object.meta.size) {
            self.alive(self.peer.copy_metadata(key, &meta, stamp)).await.map_err(PushError::Peer)?;
            self.update(|s| s.metadata_only += 1);
            return Ok(());
        }
        let size = object.meta.size;
        let body = |start: u64, len: u64| {
            let object = &object;
            let heartbeat = self.heartbeat.clone();
            let renew = self.lease_renew;
            async move {
                use futures::StreamExt;
                let body = crate::server::handlers::get_object::stream_object_range(
                    &object.meta,
                    &object.part_offsets,
                    &object.object_dir,
                    start,
                    len,
                )
                .await
                .map_err(PushError::Local)?;
                let stream = body.into_data_stream().inspect(move |_| super::worker::renew_lease(&heartbeat, renew));
                Ok::<_, PushError>(reqwest::Body::wrap_stream(stream))
            }
        };
        match change.multipart_layout() {
            None => {
                self.alive(self.peer.put(key, &meta, stamp, size, body(0, size).await?)).await.map_err(PushError::Peer)?;
            }
            Some(layout) => {
                let upload_id = self.alive(self.peer.create_multipart(key, &meta)).await.map_err(PushError::Peer)?;
                let result: Result<(), PushError> = async {
                    let mut etags = Vec::new();
                    let mut offset = 0u64;
                    for (i, len) in layout.part_sizes(size).into_iter().enumerate() {
                        self.beat();
                        let etag = self
                            .alive(self.peer.upload_part(key, &upload_id, (i + 1) as u16, len, body(offset, len).await?))
                            .await
                            .map_err(PushError::Peer)?;
                        etags.push(etag);
                        offset += len;
                    }
                    let etag = self
                        .alive(self.peer.complete_multipart(key, &upload_id, &etags, stamp))
                        .await
                        .map_err(PushError::Peer)?;
                    if etag != object.meta.etag {
                        log::warn!(
                            "replication {}: pushed {key} with ETag {etag} but the local one is {} (layout mismatch)",
                            self.link.id,
                            object.meta.etag
                        );
                    }
                    Ok(())
                }
                .await;
                if result.is_err() {
                    self.alive(self.peer.abort_multipart(key, &upload_id)).await;
                }
                result?;
            }
        }
        self.update(|s| {
            s.copied += 1;
            s.copied_bytes += size;
        });
        Ok(())
    }
}

enum PushError {
    /// The local object moved on since the event.
    Gone,
    Local(StorageError),
    Peer(PeerError),
}
