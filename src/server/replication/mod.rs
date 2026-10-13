//! Server-side replication: links that pull a peer's bucket into a local
//! one, driven by the peer's changelog.
//!
//! A link is configured on the **receiving** site ("pull bucket X from site
//! A"). For two-way replication each site configures a link from the other
//! (mode `merge`). Positions belong to the receiver and live in the local
//! bucket's index, beside the data they describe.
//!
//! One worker per enabled link. A supervisor ticks every
//! `replication.check_interval_secs` (5s): it starts a worker for any enabled
//! link without one — after a crash, a panic, or a restart — and replaces a
//! worker whose heartbeat (its lease) went stale, so a hung worker cannot
//! hold a link forever. At most one worker runs per link.

pub mod db;
pub mod model;
pub mod peer;
#[cfg(test)]
mod tests;
mod worker;

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use self::db::LinkDb;
use self::model::{Link, LinkStatus, Mode, RECORD_VERSION};
use self::peer::{PeerClient, PeerConfig};
use super::notify::model::Filter;
use crate::storage::store::{DeleteGuard, LocalObjectStore};
use crate::storage::time::now_ms;

/// Families this module keeps in `admin.rocksdb`.
pub const ADMIN_DB_FAMILIES: [&str; 1] = [db::CF_LINKS];

/// Supervisor timing.
#[derive(Debug, Clone)]
pub struct Timing {
    pub check_interval: Duration,
    /// A worker silent for longer than this is replaced.
    pub stale_after: Duration,
    /// Checkpoints older than this re-snapshot (the tombstone retention).
    pub max_checkpoint_age: Duration,
    pub long_poll: Duration,
    /// Changes a link applies concurrently within a batch.
    pub parallelism: usize,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            stale_after: Duration::from_secs(300),
            max_checkpoint_age: Duration::from_secs(14 * 86_400),
            long_poll: Duration::from_secs(20),
            parallelism: 8,
        }
    }
}

#[derive(Debug)]
pub enum LinkError {
    NotFound(String),
    BadRequest(String),
    Internal(String),
}

impl std::fmt::Display for LinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkError::NotFound(m) | LinkError::BadRequest(m) | LinkError::Internal(m) => f.write_str(m),
        }
    }
}

impl From<crate::storage::errors::StorageError> for LinkError {
    fn from(err: crate::storage::errors::StorageError) -> Self {
        match err {
            crate::storage::errors::StorageError::BucketNotFound(b) => LinkError::NotFound(format!("bucket {b} not found")),
            other => LinkError::Internal(other.to_string()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LinkRequest {
    pub name: String,
    pub local_bucket: String,
    pub peer_endpoint: String,
    pub peer_bucket: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub prefix: String,
    #[serde(default = "yes")]
    pub enabled: bool,
    /// Mirror links need a read-only target: with this, a read-write target
    /// is made read-only as part of creating the link (otherwise refused).
    #[serde(default)]
    pub make_read_only: bool,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct LinkUpdate {
    pub name: Option<String>,
    pub peer_endpoint: Option<String>,
    pub access_key: Option<String>,
    /// Omitted or empty: keep the stored secret.
    pub secret_key: Option<String>,
    pub region: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum LinkAction {
    Pause,
    Resume,
    /// Discard the checkpoint and start again from a fresh snapshot.
    Resnapshot,
}

/// What the console shows: the configuration (without the secret) and the
/// live status.
#[derive(Debug, Clone, Serialize)]
pub struct LinkView {
    pub id: String,
    pub name: String,
    pub local_bucket: String,
    pub peer_endpoint: String,
    pub peer_bucket: String,
    pub access_key: String,
    pub region: String,
    pub mode: Mode,
    pub prefix: String,
    pub enabled: bool,
    pub created_at_ms: i64,
    pub created_by: String,
    pub running: bool,
    pub heartbeat_ms: i64,
    pub status: LinkStatus,
}

struct Runtime {
    link: Link,
    status: Arc<Mutex<LinkStatus>>,
    heartbeat: Arc<AtomicI64>,
    worker: Option<(CancellationToken, tokio::task::JoinHandle<()>)>,
}

struct Inner {
    store: LocalObjectStore,
    db: LinkDb,
    timing: Timing,
    shutdown: CancellationToken,
    links: RwLock<HashMap<String, Arc<Mutex<Runtime>>>>,
    /// Serialises link creation, so the per-bucket rules (a mirror is the
    /// bucket's only link) cannot be raced.
    create_lock: tokio::sync::Mutex<()>,
}

#[derive(Clone)]
pub struct ReplicationService {
    inner: Arc<Inner>,
}

fn random_hex(bytes: usize) -> String {
    use rand::Rng;
    let mut buf = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// Names of the mirror links writing into `bucket`.
fn mirror_links_into(inner: &Inner, bucket: &str) -> Vec<String> {
    inner
        .links
        .read()
        .unwrap()
        .values()
        .map(|rt| rt.lock().unwrap().link.clone())
        .filter(|l| l.local_bucket == bucket && l.mode == Mode::Mirror)
        .map(|l| format!("{} ({})", l.name, l.id))
        .collect()
}

fn peer_config(link: &Link) -> PeerConfig {
    PeerConfig {
        endpoint: link.peer_endpoint.clone(),
        bucket: link.peer_bucket.clone(),
        access_key: link.access_key.clone(),
        secret_key: link.secret_key.clone(),
        region: link.region.clone(),
    }
}

impl ReplicationService {
    pub async fn start(
        store: LocalObjectStore,
        db: LinkDb,
        timing: Timing,
        shutdown: CancellationToken,
    ) -> Result<Self, LinkError> {
        let service = Self {
            inner: Arc::new(Inner {
                store,
                db,
                timing,
                shutdown,
                links: RwLock::new(HashMap::new()),
                create_lock: tokio::sync::Mutex::new(()),
            }),
        };
        for link in service.inner.db.load_all().await? {
            service.install(link);
        }
        let weak = Arc::downgrade(&service.inner);
        service.inner.store.add_delete_guard(DeleteGuard(Arc::new(move |bucket| {
            let inner = weak.upgrade()?;
            let names: Vec<String> = inner
                .links
                .read()
                .unwrap()
                .values()
                .filter_map(|rt| {
                    let rt = rt.lock().unwrap();
                    (rt.link.local_bucket == bucket).then(|| format!("{} ({})", rt.link.name, rt.link.id))
                })
                .collect();
            (!names.is_empty()).then(|| {
                format!(
                    "bucket {bucket} is the target of replication links; delete them first: {}",
                    names.join(", ")
                )
            })
        })));
        let weak = Arc::downgrade(&service.inner);
        service.inner.store.set_peer_restorer(crate::storage::store::PeerRestorer(Arc::new(move |request| {
            let inner = weak.upgrade();
            Box::pin(async move {
                match inner {
                    Some(inner) => ReplicationService { inner }.restore(request).await,
                    None => Err("replication is shutting down".to_string()),
                }
            })
        })));
        // A mirror's target stays read-only while the mirror exists.
        let weak = Arc::downgrade(&service.inner);
        service.inner.store.add_read_write_guard(DeleteGuard(Arc::new(move |bucket| {
            let inner = weak.upgrade()?;
            let mirrors = mirror_links_into(&inner, bucket);
            (!mirrors.is_empty()).then(|| {
                format!(
                    "bucket {bucket} is a mirror replica and must stay read-only; delete its mirror link first: {}",
                    mirrors.join(", ")
                )
            })
        })));
        service.spawn_supervisor();
        Ok(service)
    }

    fn install(&self, link: Link) {
        let id = link.id.clone();
        let runtime = Runtime {
            link,
            status: Arc::new(Mutex::new(LinkStatus::default())),
            heartbeat: Arc::new(AtomicI64::new(0)),
            worker: None,
        };
        self.inner.links.write().unwrap().insert(id, Arc::new(Mutex::new(runtime)));
    }

    fn spawn_supervisor(&self) {
        let service = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(service.inner.timing.check_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = service.inner.shutdown.cancelled() => {
                        service.stop_all();
                        break;
                    }
                    _ = tick.tick() => service.supervise(),
                }
            }
        });
    }

    fn stop_all(&self) {
        for rt in self.inner.links.read().unwrap().values() {
            if let Some((cancel, _)) = rt.lock().unwrap().worker.take() {
                cancel.cancel();
            }
        }
    }

    /// One supervisor pass: every enabled link gets exactly one live worker.
    pub fn supervise(&self) {
        let now = now_ms();
        let stale_ms = self.inner.timing.stale_after.as_millis() as i64;
        let runtimes: Vec<Arc<Mutex<Runtime>>> = self.inner.links.read().unwrap().values().cloned().collect();
        for rt in runtimes {
            let mut rt = rt.lock().unwrap();
            if !rt.link.enabled {
                if let Some((cancel, handle)) = rt.worker.take() {
                    cancel.cancel();
                    handle.abort();
                }
                rt.status.lock().unwrap().state = "paused".to_string();
                continue;
            }
            let restart = match &rt.worker {
                None => true,
                Some((_, handle)) if handle.is_finished() => {
                    // Exited (fatal error or panic): try again.
                    true
                }
                Some(_) => {
                    let silent = now - rt.heartbeat.load(Ordering::SeqCst);
                    if silent > stale_ms {
                        log::warn!(
                            "replication {} worker silent for {}s: replacing it",
                            rt.link.id,
                            silent / 1000
                        );
                        true
                    } else {
                        false
                    }
                }
            };
            if !restart {
                continue;
            }
            if let Some((cancel, handle)) = rt.worker.take() {
                cancel.cancel();
                handle.abort();
                rt.status.lock().unwrap().restarts += 1;
            }
            let peer = match PeerClient::new(peer_config(&rt.link)) {
                Ok(peer) => peer,
                Err(err) => {
                    let mut status = rt.status.lock().unwrap();
                    status.state = "error".to_string();
                    status.last_error = Some(err);
                    status.last_error_ms = now;
                    continue;
                }
            };
            let cancel = self.inner.shutdown.child_token();
            rt.heartbeat.store(now, Ordering::SeqCst);
            let worker = worker::Worker {
                store: self.inner.store.clone(),
                link: rt.link.clone(),
                peer,
                status: rt.status.clone(),
                heartbeat: rt.heartbeat.clone(),
                cancel: cancel.clone(),
                max_checkpoint_age_ms: self.inner.timing.max_checkpoint_age.as_millis() as i64,
                long_poll: self.inner.timing.long_poll,
                parallelism: self.inner.timing.parallelism,
            };
            rt.worker = Some((cancel, tokio::spawn(worker.run())));
        }
    }

    fn runtime(&self, id: &str) -> Result<Arc<Mutex<Runtime>>, LinkError> {
        self.inner
            .links
            .read()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| LinkError::NotFound(format!("replication link {id} not found")))
    }

    fn validate(&self, link: &Link) -> Result<(), LinkError> {
        if link.name.trim().is_empty() || link.name.len() > 128 {
            return Err(LinkError::BadRequest("name must be 1-128 characters".into()));
        }
        if link.access_key.is_empty() || link.secret_key.is_empty() {
            return Err(LinkError::BadRequest("the peer access key and secret are required".into()));
        }
        crate::storage::encoding::validate_bucket_name(&link.peer_bucket)
            .map_err(|_| LinkError::BadRequest(format!("invalid peer bucket {:?}", link.peer_bucket)))?;
        PeerClient::new(peer_config(link)).map_err(LinkError::BadRequest)?;
        Ok(())
    }

    pub async fn create(&self, request: LinkRequest, created_by: &str) -> Result<LinkView, LinkError> {
        if !self.inner.store.bucket_exists(&request.local_bucket).await {
            return Err(LinkError::NotFound(format!("bucket {} not found", request.local_bucket)));
        }
        let _creating = self.inner.create_lock.lock().await;
        let bucket = request.local_bucket.clone();
        // A mirror owns its bucket: no other link may write into it.
        let existing: Vec<(String, Mode)> = self
            .inner
            .links
            .read()
            .unwrap()
            .values()
            .map(|rt| rt.lock().unwrap().link.clone())
            .filter(|l| l.local_bucket == bucket)
            .map(|l| (format!("{} ({})", l.name, l.id), l.mode))
            .collect();
        if let Some((name, _)) = existing.iter().find(|(_, mode)| *mode == Mode::Mirror) {
            return Err(LinkError::BadRequest(format!(
                "bucket {bucket} is already a mirror of {name}; a mirror is the bucket's only link"
            )));
        }
        if request.mode == Mode::Mirror && !existing.is_empty() {
            let names: Vec<&str> = existing.iter().map(|(n, _)| n.as_str()).collect();
            return Err(LinkError::BadRequest(format!(
                "bucket {bucket} already has replication links ({}); a mirror must be the bucket's only link",
                names.join(", ")
            )));
        }
        if request.mode == Mode::Mirror && !request.prefix.is_empty() {
            // The whole bucket must be read-only for a mirror, so mirroring
            // part of it would freeze the rest: nobody could write it.
            return Err(LinkError::BadRequest(
                "a mirror replicates the whole bucket; a prefix is only supported for merge links".into(),
            ));
        }
        if request.mode == Mode::Mirror {
            let read_only = self.inner.store.bucket_meta(&bucket).await?.read_only;
            if !read_only && !request.make_read_only {
                return Err(LinkError::BadRequest(format!(
                    "a mirror needs a read-only target: make bucket {bucket} read-only first (or create the link \
                     with make_read_only)"
                )));
            }
        }
        let link = Link {
            v: RECORD_VERSION,
            id: format!("rl_{}", random_hex(8)),
            name: request.name.trim().to_string(),
            local_bucket: request.local_bucket,
            peer_endpoint: request.peer_endpoint.trim().trim_end_matches('/').to_string(),
            peer_bucket: request.peer_bucket,
            access_key: request.access_key,
            secret_key: request.secret_key,
            region: request.region.filter(|r| !r.is_empty()).unwrap_or_else(|| "us-east-1".to_string()),
            mode: request.mode,
            filter: Filter { prefix: request.prefix, ..Filter::default() },
            enabled: request.enabled,
            created_at_ms: now_ms(),
            created_by: created_by.to_string(),
        };
        self.validate(&link)?;
        if link.mode == Mode::Mirror {
            self.inner.store.set_bucket_read_only(&link.local_bucket, true).await?;
        }
        self.inner.db.put(&link).await?;
        let id = link.id.clone();
        self.install(link);
        log::info!("replication link created id={id} by={created_by}");
        self.supervise();
        self.view(&id)
    }

    pub async fn update(&self, id: &str, update: LinkUpdate) -> Result<LinkView, LinkError> {
        let rt = self.runtime(id)?;
        let mut link = rt.lock().unwrap().link.clone();
        if let Some(name) = update.name {
            link.name = name.trim().to_string();
        }
        if let Some(endpoint) = update.peer_endpoint {
            link.peer_endpoint = endpoint.trim().trim_end_matches('/').to_string();
        }
        if let Some(access_key) = update.access_key {
            link.access_key = access_key;
        }
        if let Some(secret) = update.secret_key.filter(|s| !s.is_empty()) {
            link.secret_key = secret;
        }
        if let Some(region) = update.region.filter(|r| !r.is_empty()) {
            link.region = region;
        }
        if let Some(enabled) = update.enabled {
            link.enabled = enabled;
        }
        self.validate(&link)?;
        self.inner.db.put(&link).await?;
        {
            let mut rt = rt.lock().unwrap();
            rt.link = link;
            // Restart with the new settings.
            if let Some((cancel, handle)) = rt.worker.take() {
                cancel.cancel();
                handle.abort();
            }
        }
        self.supervise();
        self.view(id)
    }

    pub async fn delete(&self, id: &str) -> Result<(), LinkError> {
        let rt = self.runtime(id)?;
        let bucket = {
            let mut rt = rt.lock().unwrap();
            if let Some((cancel, handle)) = rt.worker.take() {
                cancel.cancel();
                handle.abort();
            }
            rt.link.local_bucket.clone()
        };
        self.inner.db.delete(id).await?;
        self.inner.links.write().unwrap().remove(id);
        if let Ok(index) = self.inner.store.index(&bucket).await {
            let _ = index.put_checkpoint(id, None).await;
        }
        log::info!("replication link deleted id={id}");
        Ok(())
    }

    pub async fn act(&self, id: &str, action: LinkAction) -> Result<LinkView, LinkError> {
        match action {
            LinkAction::Pause => {
                self.update(id, LinkUpdate { enabled: Some(false), ..LinkUpdate::default() }).await?;
            }
            LinkAction::Resume => {
                self.update(id, LinkUpdate { enabled: Some(true), ..LinkUpdate::default() }).await?;
            }
            LinkAction::Resnapshot => {
                let rt = self.runtime(id)?;
                let bucket = {
                    let mut rt = rt.lock().unwrap();
                    if let Some((cancel, handle)) = rt.worker.take() {
                        cancel.cancel();
                        handle.abort();
                    }
                    rt.link.local_bucket.clone()
                };
                self.inner.store.index(&bucket).await?.put_checkpoint(id, None).await?;
                self.supervise();
            }
        }
        self.view(id)
    }

    pub fn view(&self, id: &str) -> Result<LinkView, LinkError> {
        let runtime = self.runtime(id)?;
        let rt = runtime.lock().unwrap();
        let link = &rt.link;
        let status = rt.status.lock().unwrap().clone();
        let view = LinkView {
            id: link.id.clone(),
            name: link.name.clone(),
            local_bucket: link.local_bucket.clone(),
            peer_endpoint: link.peer_endpoint.clone(),
            peer_bucket: link.peer_bucket.clone(),
            access_key: link.access_key.clone(),
            region: link.region.clone(),
            mode: link.mode,
            prefix: link.filter.prefix.clone(),
            enabled: link.enabled,
            created_at_ms: link.created_at_ms,
            created_by: link.created_by.clone(),
            running: rt.worker.as_ref().is_some_and(|(_, h)| !h.is_finished()),
            heartbeat_ms: rt.heartbeat.load(Ordering::SeqCst),
            status,
        };
        Ok(view)
    }

    pub fn list(&self) -> Vec<LinkView> {
        let mut ids: Vec<(String, String)> = self
            .inner
            .links
            .read()
            .unwrap()
            .values()
            .map(|rt| {
                let rt = rt.lock().unwrap();
                (rt.link.local_bucket.clone(), rt.link.id.clone())
            })
            .collect();
        ids.sort();
        ids.into_iter().filter_map(|(_, id)| self.view(&id).ok()).collect()
    }

    /// Restores a lost object from the first peer (a link pulling into its
    /// bucket) that holds the lost version or a newer one — or, with
    /// `allow_older`, any version. The peer's state keeps its stamp; the
    /// publish is guarded by If-Match on the lost version's ETag, so a write
    /// that landed meanwhile is never clobbered.
    pub async fn restore(&self, request: crate::storage::store::RestoreRequest) -> Result<String, String> {
        let links = self.peers_for(&request.bucket);
        if links.is_empty() {
            return Err(format!(
                "no enabled replication link pulls into bucket {}: nothing to restore from",
                request.bucket
            ));
        }
        let mut notes = Vec::new();
        for link in links {
            let peer = match PeerClient::new(peer_config(&link)) {
                Ok(peer) => peer,
                Err(err) => {
                    notes.push(format!("{}: {err}", link.name));
                    continue;
                }
            };
            // The peer's current state of the key: the first entry of a
            // listing under the key itself.
            let page = match peer.snapshot(None, 1, &request.key, false).await {
                Ok(page) => page,
                Err(err) => {
                    notes.push(format!("{}: {err}", link.name));
                    continue;
                }
            };
            let Some(entry) = page.entries.into_iter().find(|e| e.key == request.key) else {
                notes.push(format!("{}: the peer does not have it", link.name));
                continue;
            };
            let stamp = peer::event_stamp(&entry, &page.site_id);
            if stamp < request.lost_stamp && !request.allow_older {
                notes.push(format!(
                    "{}: the peer only has an older version (stamp {}, lost {}); restoring it would roll the \
                     key back — use \"restore older from peer\" to accept that",
                    link.name,
                    stamp.to_header(),
                    request.lost_stamp.to_header()
                ));
                continue;
            }
            let worker = worker::Worker {
                store: self.inner.store.clone(),
                link: link.clone(),
                peer,
                status: Arc::new(Mutex::new(LinkStatus::default())),
                heartbeat: Arc::new(AtomicI64::new(now_ms())),
                cancel: CancellationToken::new(),
                max_checkpoint_age_ms: i64::MAX,
                long_poll: self.inner.timing.long_poll,
                parallelism: 1,
            };
            let options = crate::storage::store::PublishOptions {
                precondition: Some(crate::storage::store::Precondition::IfMatch(request.lost_etag.clone())),
                stamp: Some(stamp.clone()),
                restore: true,
            };
            match worker.restore(&entry, options).await {
                Ok(()) => {
                    return Ok(format!(
                        "restored from {} (site {}, stamp {})",
                        link.name,
                        page.site_id,
                        stamp.to_header()
                    ))
                }
                Err(err) => notes.push(format!("{}: {err}", link.name)),
            }
        }
        Err(format!("not restored: {}", notes.join("; ")))
    }

    /// The enabled links whose local bucket is `bucket` — for restoring a
    /// lost object from a peer.
    pub(crate) fn peers_for(&self, bucket: &str) -> Vec<Link> {
        self.inner
            .links
            .read()
            .unwrap()
            .values()
            .map(|rt| rt.lock().unwrap().link.clone())
            .filter(|link| link.local_bucket == bucket && link.enabled)
            .collect()
    }
}
