//! Server-side replication: links that replicate a whole bucket with a
//! peer's, driven by changelogs.
//!
//! - **mirror**: the link pulls the peer's bucket into a read-only local
//!   one (configured on the receiving site);
//! - **merge**: the link pulls the peer's changes in *and* pushes the local
//!   bucket's changes out, so one link (on either site) keeps both buckets
//!   the union. If both sites configure one for the same pair, the link on
//!   the higher site id stands down (the peer reports its own link back on
//!   changelog reads, `x-rs3-merge-back`).
//!
//! Positions belong to the site running the link and live in the local
//! bucket's index: the pull position in the peer's log, and (merge) the
//! push position in the local log.
//!
//! One pull worker per enabled link, plus a push worker for a merge link. A
//! supervisor ticks every `replication.check_interval_secs` (5s): it starts
//! a worker that is missing — after a crash, a panic, or a restart — and
//! replaces one whose heartbeat (its lease) went stale, so a hung worker
//! cannot hold a link forever. At most one of each runs per link.

pub mod db;
pub mod model;
pub mod peer;
mod push;
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
use crate::storage::store::{DeleteGuard, LocalObjectStore};
use crate::storage::time::now_ms;

/// Resolves an access key on this site to the identity it acts as (`None`:
/// no such key). A link's local writes are authorized as its local key.
#[derive(Clone)]
pub struct LocalAuth(pub Arc<dyn Fn(&str) -> Option<crate::server::identity::Identity> + Send + Sync>);

/// Actions a link needs on its local bucket: writing it with stamps (both
/// modes); a merge link's push also reads it and its changelog.
pub const LOCAL_WRITE_ACTIONS: [&str; 3] = ["s3:PutObject", "s3:DeleteObject", "s3:ReplicateWrite"];

/// Whether `identity` may do `action` on bucket `bucket` — on the bucket
/// itself for the changelog, otherwise on every key (links replicate the
/// whole bucket).
pub fn allowed_on_bucket(identity: &crate::server::identity::Identity, action: &'static str, bucket: &str) -> bool {
    let requirement = if action == "s3:GetBucketChangelog" {
        crate::server::policy::Requirement::bucket(action, bucket)
    } else {
        crate::server::policy::Requirement::object(action, bucket, "*")
    };
    identity.authorize(&[requirement])
}

/// The actions `link` needs on its local bucket.
fn local_actions(mode: Mode) -> Vec<&'static str> {
    let mut needed = LOCAL_WRITE_ACTIONS.to_vec();
    if mode == Mode::Merge {
        needed.extend(peer::READ_ACTIONS);
    }
    needed
}

/// Checks `link`'s local key: it exists and may do everything the mode
/// needs on the local bucket. `Err` says what is missing.
fn check_local(auth: &LocalAuth, link: &Link) -> Result<(), String> {
    if link.local_access_key.is_empty() {
        return Err("the link has no local access key: edit it and choose the key whose permissions it uses here".into());
    }
    let Some(identity) = auth.0(&link.local_access_key) else {
        return Err(format!(
            "local access key {} does not exist on this site (deleted?)",
            link.local_access_key
        ));
    };
    let missing: Vec<&str> = local_actions(link.mode)
        .into_iter()
        .filter(|action| !allowed_on_bucket(&identity, action, &link.local_bucket))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(format!(
        "local access key {} lacks {} on bucket {} of this site",
        link.local_access_key,
        missing.join(", "),
        link.local_bucket
    ))
}

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
    /// An access key on this site whose policy governs the link here.
    #[serde(default)]
    pub local_access_key: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub mode: Mode,
    /// Not supported: links replicate the whole bucket. Refused when set.
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default = "yes")]
    pub enabled: bool,
    /// Mirror links need a read-only target: with this, a read-write target
    /// is made read-only as part of creating the link (otherwise refused).
    #[serde(default)]
    pub make_read_only: bool,
    /// Check the peer first: reachable, the bucket exists, and the key has
    /// every permission the mode needs (default). Off only to configure a
    /// link to a peer that is down right now.
    #[serde(default = "yes")]
    pub verify: bool,
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
    pub local_access_key: Option<String>,
    pub region: Option<String>,
    pub enabled: Option<bool>,
    /// Check new peer settings first (default true).
    pub verify: Option<bool>,
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
    pub local_access_key: String,
    pub region: String,
    pub mode: Mode,
    pub enabled: bool,
    pub created_at_ms: i64,
    pub created_by: String,
    pub running: bool,
    pub heartbeat_ms: i64,
    pub status: LinkStatus,
}

type Task = (CancellationToken, tokio::task::JoinHandle<()>);

struct Runtime {
    link: Link,
    status: Arc<Mutex<LinkStatus>>,
    /// The pull worker.
    heartbeat: Arc<AtomicI64>,
    worker: Option<Task>,
    /// The push worker (merge links).
    push_heartbeat: Arc<AtomicI64>,
    pusher: Option<Task>,
}

impl Runtime {
    /// Stops both workers; true if any was running.
    fn stop(&mut self) -> bool {
        let mut stopped = false;
        for task in [self.worker.take(), self.pusher.take()].into_iter().flatten() {
            task.0.cancel();
            task.1.abort();
            stopped = true;
        }
        stopped
    }
}

/// A link on another site that reads one of this site's buckets — seen on
/// its changelog reads (`x-rs3-replicator`).
#[derive(Debug, Clone, Serialize)]
pub struct IncomingLink {
    pub bucket: String,
    pub site: String,
    pub mode: String,
    /// The bucket on the reader's site.
    pub peer_bucket: String,
    pub link_id: String,
    pub link_name: String,
    pub last_seen_ms: i64,
}

/// Incoming links not seen for this long are dropped from the list.
const INCOMING_TTL_MS: i64 = 5 * 60 * 1000;

struct Inner {
    store: LocalObjectStore,
    db: LinkDb,
    local_auth: LocalAuth,
    timing: Timing,
    shutdown: CancellationToken,
    links: RwLock<HashMap<String, Arc<Mutex<Runtime>>>>,
    /// Serialises link creation, so the per-bucket rules (a mirror is the
    /// bucket's only link) cannot be raced.
    create_lock: tokio::sync::Mutex<()>,
    /// Keyed by `(bucket, reader site, reader link id)`.
    incoming: Mutex<HashMap<(String, String, String), IncomingLink>>,
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

fn peer_config(link: &Link, site: &str) -> PeerConfig {
    let mode = match link.mode {
        Mode::Mirror => "mirror",
        Mode::Merge => "merge",
    };
    PeerConfig {
        endpoint: link.peer_endpoint.clone(),
        bucket: link.peer_bucket.clone(),
        access_key: link.access_key.clone(),
        secret_key: link.secret_key.clone(),
        region: link.region.clone(),
        replicator: Some(
            peer::Replicator {
                site: site.to_string(),
                mode: mode.to_string(),
                bucket: link.local_bucket.clone(),
                link: link.id.clone(),
                name: link.name.clone(),
            }
            .to_header(),
        ),
    }
}

/// Whether a worker slot needs a (new) worker: none yet, exited (fatal
/// error or panic), or silent past its lease.
fn needs_start(task: &Option<Task>, heartbeat: &AtomicI64, now: i64, stale_ms: i64, id: &str, what: &str) -> bool {
    match task {
        None => true,
        Some((_, handle)) if handle.is_finished() => true,
        Some(_) => {
            let silent = now - heartbeat.load(Ordering::SeqCst);
            if silent > stale_ms {
                log::warn!("replication {id} {what} worker silent for {}s: replacing it", silent / 1000);
                true
            } else {
                false
            }
        }
    }
}

impl ReplicationService {
    pub async fn start(
        store: LocalObjectStore,
        db: LinkDb,
        local_auth: LocalAuth,
        timing: Timing,
        shutdown: CancellationToken,
    ) -> Result<Self, LinkError> {
        let service = Self {
            inner: Arc::new(Inner {
                store,
                db,
                local_auth,
                timing,
                shutdown,
                links: RwLock::new(HashMap::new()),
                create_lock: tokio::sync::Mutex::new(()),
                incoming: Mutex::new(HashMap::new()),
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
        // Links on other sites reading our buckets: remember them, and tell a
        // merge reader when a merge link here already pairs the buckets.
        let weak = Arc::downgrade(&service.inner);
        service.inner.store.set_replica_reader(crate::storage::store::ReplicaReader(Arc::new(move |bucket, header, record| {
            let inner = weak.upgrade()?;
            let peer::Replicator { site, mode, bucket: peer_bucket, link: link_id, name: link_name } =
                peer::Replicator::parse(header)?;
            let now = now_ms();
            if record {
                let mut incoming = inner.incoming.lock().unwrap();
                incoming.retain(|_, seen| now - seen.last_seen_ms < INCOMING_TTL_MS);
                incoming.insert(
                    (bucket.to_string(), site.clone(), link_id.clone()),
                    IncomingLink {
                        bucket: bucket.to_string(),
                        site: site.clone(),
                        mode: mode.clone(),
                        peer_bucket: peer_bucket.clone(),
                        link_id,
                        link_name,
                        last_seen_ms: now,
                    },
                );
            }
            if mode != "merge" {
                return None;
            }
            let links = inner.links.read().unwrap();
            links.values().find_map(|rt| {
                let rt = rt.lock().unwrap();
                let link = &rt.link;
                let paired = link.mode == Mode::Merge
                    && link.local_bucket == bucket
                    && link.peer_bucket == peer_bucket
                    && rt.status.lock().unwrap().peer_site.as_deref() == Some(site.as_str());
                paired.then(|| peer::MergeBack { link: link.id.clone(), name: link.name.clone() }.to_header())
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
            push_heartbeat: Arc::new(AtomicI64::new(0)),
            pusher: None,
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
            let mut rt = rt.lock().unwrap();
            for (cancel, _) in [rt.worker.take(), rt.pusher.take()].into_iter().flatten() {
                cancel.cancel();
            }
        }
    }

    /// One supervisor pass: every enabled link gets exactly one live pull
    /// worker, and a merge link one push worker.
    pub fn supervise(&self) {
        let now = now_ms();
        let stale_ms = self.inner.timing.stale_after.as_millis() as i64;
        let runtimes: Vec<Arc<Mutex<Runtime>>> = self.inner.links.read().unwrap().values().cloned().collect();
        for rt in runtimes {
            let mut rt = rt.lock().unwrap();
            if !rt.link.enabled {
                rt.stop();
                let mut status = rt.status.lock().unwrap();
                status.state = "paused".to_string();
                if let Some(push) = status.push.as_mut() {
                    push.state = "paused".to_string();
                }
                continue;
            }
            let id = rt.link.id.clone();
            let pull_heartbeat = rt.heartbeat.clone();
            if needs_start(&rt.worker, &pull_heartbeat, now, stale_ms, &id, "pull") {
                if let Some((cancel, handle)) = rt.worker.take() {
                    cancel.cancel();
                    handle.abort();
                    rt.status.lock().unwrap().restarts += 1;
                }
                let Some(peer) = self.peer_client(&rt, now) else { continue };
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
                    local_check: self.local_check(&rt.link),
                };
                rt.worker = Some((cancel, tokio::spawn(worker.run())));
            }
            if rt.link.mode != Mode::Merge {
                continue;
            }
            let push_heartbeat = rt.push_heartbeat.clone();
            if needs_start(&rt.pusher, &push_heartbeat, now, stale_ms, &id, "push") {
                if let Some((cancel, handle)) = rt.pusher.take() {
                    cancel.cancel();
                    handle.abort();
                    rt.status.lock().unwrap().restarts += 1;
                }
                let Some(peer) = self.peer_client(&rt, now) else { continue };
                let cancel = self.inner.shutdown.child_token();
                rt.push_heartbeat.store(now, Ordering::SeqCst);
                let pusher = push::Pusher {
                    store: self.inner.store.clone(),
                    link: rt.link.clone(),
                    peer,
                    status: rt.status.clone(),
                    heartbeat: rt.push_heartbeat.clone(),
                    cancel: cancel.clone(),
                    max_checkpoint_age_ms: self.inner.timing.max_checkpoint_age.as_millis() as i64,
                    long_poll: self.inner.timing.long_poll,
                    parallelism: self.inner.timing.parallelism,
                    local_check: self.local_check(&rt.link),
                };
                rt.pusher = Some((cancel, tokio::spawn(pusher.run())));
            }
        }
    }

    fn local_check(&self, link: &Link) -> worker::LocalCheck {
        let auth = self.inner.local_auth.clone();
        let link = link.clone();
        Arc::new(move || check_local(&auth, &link))
    }

    fn peer_client(&self, rt: &Runtime, now: i64) -> Option<PeerClient> {
        match PeerClient::new(peer_config(&rt.link, &self.inner.store.site_id())) {
            Ok(peer) => Some(peer),
            Err(err) => {
                let mut status = rt.status.lock().unwrap();
                status.state = "error".to_string();
                status.last_error = Some(err);
                status.last_error_ms = now;
                None
            }
        }
    }

    /// Links on other sites that read this site's buckets (seen within the
    /// last few minutes).
    pub fn incoming(&self) -> Vec<IncomingLink> {
        let now = now_ms();
        let mut list: Vec<IncomingLink> = self
            .inner
            .incoming
            .lock()
            .unwrap()
            .values()
            .filter(|seen| now - seen.last_seen_ms < INCOMING_TTL_MS)
            .cloned()
            .collect();
        list.sort_by(|a, b| (&a.bucket, &a.site, &a.link_name).cmp(&(&b.bucket, &b.site, &b.link_name)));
        list
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
        PeerClient::new(peer_config(link, &self.inner.store.site_id())).map_err(LinkError::BadRequest)?;
        Ok(())
    }

    /// Checks a link against its peer before it is saved: the peer answers,
    /// is not this site, has the bucket, and the key may do everything the
    /// mode needs — reading it, and for a merge link also writing it with
    /// the changes' own stamps. A merge link the peer already runs for this
    /// pair makes a second one pointless: refused.
    async fn verify(&self, link: &Link) -> Result<(), LinkError> {
        let peer = PeerClient::new(peer_config(link, &self.inner.store.site_id())).map_err(LinkError::BadRequest)?;
        let at = format!("bucket {} at {}", link.peer_bucket, link.peer_endpoint);
        let report = match peer.access().await {
            Ok(report) => report,
            Err(peer::PeerError::Other(message)) if message.starts_with("bad peer response") => {
                return Err(LinkError::BadRequest(format!(
                    "{at} did not answer the access check: is it a rusts3 site of this version? ({message})"
                )))
            }
            Err(err) => return Err(LinkError::BadRequest(format!("checking {at}: {err}"))),
        };
        let local = self.inner.store.site_id();
        if report.site_id == local {
            return Err(LinkError::BadRequest(format!(
                "the peer reports this server's own site id ({local}): it is this server, or a copy of its data \
                 directory (give the copy its own server.site_id)"
            )));
        }
        let mut needed: Vec<&str> = peer::READ_ACTIONS.to_vec();
        if link.mode == Mode::Merge {
            needed.extend(peer::WRITE_ACTIONS);
        }
        let missing: Vec<&str> = needed
            .into_iter()
            .filter(|action| !report.actions.get(*action).copied().unwrap_or(false))
            .collect();
        if !missing.is_empty() {
            let why = match link.mode {
                Mode::Mirror => "a mirror link reads the peer bucket and its changelog",
                Mode::Merge => {
                    "a merge link reads the peer bucket and its changelog, and writes this bucket's changes into \
                     it with their original stamps"
                }
            };
            return Err(LinkError::BadRequest(format!(
                "access key {} lacks {} on {at} ({why})",
                link.access_key,
                missing.join(", ")
            )));
        }
        if link.mode == Mode::Merge && report.read_only {
            return Err(LinkError::BadRequest(format!(
                "{at} is read-only (a mirror replica?): a merge link would write into it"
            )));
        }
        if let (Mode::Merge, Some(peer::MergeBack { link: id, name })) =
            (link.mode, report.merge_back.as_deref().and_then(peer::MergeBack::parse))
        {
            return Err(LinkError::BadRequest(format!(
                "site {} already merges {} with this bucket through its link \"{name}\" ({id}), which syncs both \
                 ways: no link is needed here",
                report.site_id, link.peer_bucket
            )));
        }
        Ok(())
    }

    pub async fn create(&self, request: LinkRequest, created_by: &str) -> Result<LinkView, LinkError> {
        if !self.inner.store.bucket_exists(&request.local_bucket).await {
            return Err(LinkError::NotFound(format!("bucket {} not found", request.local_bucket)));
        }
        if request.prefix.as_deref().is_some_and(|p| !p.is_empty()) {
            return Err(LinkError::BadRequest(
                "replication links replicate the whole bucket; prefixes are not supported (webhooks can filter by \
                 prefix)"
                    .into(),
            ));
        }
        let _creating = self.inner.create_lock.lock().await;
        let bucket = request.local_bucket.clone();
        let endpoint = request.peer_endpoint.trim().trim_end_matches('/').to_string();
        // A mirror owns its bucket: no other link may write into it.
        let existing_links: Vec<Link> = self
            .inner
            .links
            .read()
            .unwrap()
            .values()
            .map(|rt| rt.lock().unwrap().link.clone())
            .filter(|l| l.local_bucket == bucket)
            .collect();
        let existing: Vec<(String, Mode)> =
            existing_links.iter().map(|l| (format!("{} ({})", l.name, l.id), l.mode)).collect();
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
        if let Some(same) = existing_links
            .iter()
            .find(|l| l.peer_endpoint == endpoint && l.peer_bucket == request.peer_bucket)
        {
            return Err(LinkError::BadRequest(format!(
                "bucket {bucket} is already linked with {}/{} by {} ({})",
                endpoint, request.peer_bucket, same.name, same.id
            )));
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
            peer_endpoint: endpoint,
            peer_bucket: request.peer_bucket,
            access_key: request.access_key,
            secret_key: request.secret_key,
            local_access_key: request.local_access_key.trim().to_string(),
            region: request.region.filter(|r| !r.is_empty()).unwrap_or_else(|| "us-east-1".to_string()),
            mode: request.mode,
            enabled: request.enabled,
            created_at_ms: now_ms(),
            created_by: created_by.to_string(),
        };
        self.validate(&link)?;
        check_local(&self.inner.local_auth, &link).map_err(LinkError::BadRequest)?;
        if request.verify {
            self.verify(&link).await?;
        }
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
        let before = (link.peer_endpoint.clone(), link.access_key.clone(), link.secret_key.clone(), link.region.clone());
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
        if let Some(local) = update.local_access_key {
            link.local_access_key = local.trim().to_string();
            check_local(&self.inner.local_auth, &link).map_err(LinkError::BadRequest)?;
        }
        self.validate(&link)?;
        // New peer or credentials: check them as on create.
        let after = (link.peer_endpoint.clone(), link.access_key.clone(), link.secret_key.clone(), link.region.clone());
        if after != before && update.verify.unwrap_or(true) {
            self.verify(&link).await?;
        }
        self.inner.db.put(&link).await?;
        {
            let mut rt = rt.lock().unwrap();
            rt.link = link;
            // Restart with the new settings.
            rt.stop();
        }
        self.supervise();
        self.view(id)
    }

    pub async fn delete(&self, id: &str) -> Result<(), LinkError> {
        let rt = self.runtime(id)?;
        let bucket = {
            let mut rt = rt.lock().unwrap();
            rt.stop();
            rt.link.local_bucket.clone()
        };
        self.inner.db.delete(id).await?;
        self.inner.links.write().unwrap().remove(id);
        if let Ok(index) = self.inner.store.index(&bucket).await {
            let _ = index.put_checkpoint(id, None).await;
            let _ = index.put_checkpoint(&push::checkpoint_key(id), None).await;
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
                    rt.stop();
                    rt.link.local_bucket.clone()
                };
                let index = self.inner.store.index(&bucket).await?;
                index.put_checkpoint(id, None).await?;
                index.put_checkpoint(&push::checkpoint_key(id), None).await?;
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
            local_access_key: link.local_access_key.clone(),
            region: link.region.clone(),
            mode: link.mode,
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
            let peer = match PeerClient::new(peer_config(&link, &self.inner.store.site_id())) {
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
                local_check: self.local_check(&link),
            };
            if let Err(err) = check_local(&self.inner.local_auth, &link) {
                notes.push(format!("{}: {err}", link.name));
                continue;
            }
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
