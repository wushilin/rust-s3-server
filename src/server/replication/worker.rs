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
}

/// Why a step stopped.
enum StepError {
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
                Ok(()) => backoff = Duration::from_secs(1),
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

    fn check_site(&self, peer_site: &str) -> Result<(), StepError> {
        let local = self.store.site_id();
        if peer_site == local {
            return Err(StepError::Fatal(format!(
                "the peer reports this server's own site id ({local}): a copied data directory? Give one of \
                 them its own server.site_id."
            )));
        }
        Ok(())
    }

    /// One unit of progress: a snapshot page or a changelog batch, applied
    /// and checkpointed.
    async fn step(&self) -> Result<(), StepError> {
        let prefix = self.link.filter.prefix.clone();
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
            let page = self.peer.snapshot(None, SNAPSHOT_PAGE, &prefix, false).await?;
            self.check_site(&page.site_id)?;
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
        match checkpoint.phase.clone() {
            Phase::Snapshot { cursor, tombstones: false, .. } => {
                self.update(|s| s.state = "snapshot".to_string());
                let page = self.peer.snapshot(cursor.as_deref(), SNAPSHOT_PAGE, &prefix, false).await?;
                if page.log_id != checkpoint.peer_log_id {
                    return Err(StepError::Resnapshot("the peer's changelog changed mid-snapshot".to_string()));
                }
                self.apply_snapshot_page(&mut checkpoint, cursor, page).await?;
                self.save(Some(&checkpoint)).await
            }
            Phase::Snapshot { through, cursor, tombstones: true } => {
                // Merge mode: the peer's remembered deletes, so a key deleted
                // there does not survive here.
                let page = self.peer.snapshot(cursor.as_deref(), SNAPSHOT_PAGE, &prefix, true).await?;
                for entry in &page.entries {
                    self.apply(entry, &checkpoint.peer_site).await?;
                }
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
                let page = tokio::select! {
                    _ = self.cancel.cancelled() => return Ok(()),
                    page = self.peer.changes(after, CHANGES_PAGE, self.long_poll, &prefix, Some(&checkpoint.peer_log_id)) => page?,
                };
                self.check_site(&page.site_id)?;
                for event in &page.events {
                    self.apply(event, &checkpoint.peer_site).await?;
                }
                checkpoint.phase = Phase::Live { after: page.next };
                self.update(|s| {
                    s.position = Some(page.next);
                    s.peer_head = Some(page.head);
                    s.last_progress_ms = now_ms();
                });
                if page.next != after {
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
        for entry in &page.entries {
            self.apply(entry, &checkpoint.peer_site).await?;
        }
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
    /// `upto` is `None`) under the link's prefix that are not in `present`.
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
                .list_objects(bucket, &self.link.filter.prefix, None, after.as_deref(), 1000)
                .await?;
            for entry in &page.entries {
                if upto.is_some_and(|upto| entry.object_key.as_str() > upto) {
                    return Ok(());
                }
                if !present.contains(entry.object_key.as_str()) {
                    self.store.delete_object(bucket, &entry.object_key).await?;
                    self.update(|s| s.deleted += 1);
                }
            }
            if !page.is_truncated {
                return Ok(());
            }
            after = page.next_after;
        }
    }

    /// Applies one change (or snapshot entry) with its own stamp.
    async fn apply(&self, event: &WireEvent, peer_site: &str) -> Result<(), StepError> {
        self.beat();
        let bucket = &self.link.local_bucket;
        let stamp = event_stamp(event, peer_site);
        // Not newer than what is here: nothing to fetch (an echo, a replay,
        // or a change that lost to a newer one).
        if let Some((current, _)) = self.store.key_stamp(bucket, &event.key).await? {
            if current >= stamp {
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
                    .delete_object_stamped(bucket, &event.key, Some(stamp))
                    .await
                    .map(|_| self.update(|s| s.deleted += 1))
            }
            _ => match self.apply_put(event, stamp).await {
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

    async fn apply_put(&self, event: &WireEvent, stamp: crate::storage::stamp::Stamp) -> Result<(), PutError> {
        let bucket = &self.link.local_bucket;
        let key = &event.key;
        let local = self.store.index(bucket).await.map_err(PutError::Local)?.get(key).await.map_err(PutError::Local)?;
        if let Some(local) = local.filter(|row| row.etag == event.etag && row.size == event.size) {
            // Same bytes here already: adopt the peer's metadata and stamp
            // without moving data.
            let _ = local;
            let headers = self.peer.head(key, &event.version).await.map_err(PutError::Peer)?;
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
                    PublishOptions { precondition: None, stamp: Some(stamp) },
                )
                .await
                .map_err(PutError::Local)?;
            self.update(|s| s.metadata_only += 1);
            return Ok(());
        }
        match event.parts.as_ref().filter(|layout| layout.multipart) {
            Some(layout) => self.copy_multipart(event, layout, stamp).await,
            None => {
                let (headers, stream) = self.peer.body(key, &event.version, None).await.map_err(PutError::Peer)?;
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
                        PublishOptions { precondition: None, stamp: Some(stamp) },
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
        stamp: crate::storage::stamp::Stamp,
    ) -> Result<(), PutError> {
        let bucket = &self.link.local_bucket;
        let key = &event.key;
        let headers = self.peer.head(key, &event.version).await.map_err(PutError::Peer)?;
        let upload_id = self
            .store
            .initiate_multipart_with_metadata(
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
                let (_, stream) = self.peer.body(key, &event.version, range).await.map_err(PutError::Peer)?;
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
                .complete_multipart_with(bucket, key, &upload_id, &parts, PublishOptions { precondition: None, stamp: Some(stamp) })
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

enum PutError {
    Peer(PeerError),
    Local(StorageError),
}
