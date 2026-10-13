//! Changelog subscriptions: webhooks (pushed) and replication tokens (pulled).
//!
//! Every subscription belongs to one bucket and tracks its own position in
//! that bucket's changelog (`storage::changelog`). One global service runs
//! them all: a shared HTTP client, one worker task per webhook, a global cap
//! on concurrent deliveries, a checker tick that resumes muted webhooks, and
//! the retention trim of every bucket's log.
//!
//! Delivery rules:
//! - Batches only, one in flight per webhook, in `seq` order. At-least-once:
//!   a batch is re-sent until a 2xx arrives, and consumers de-duplicate by
//!   `seq`.
//! - A commit wakes its bucket's webhooks. A webhook that is muted ignores the
//!   wake; a webhook that is busy or behind picks the change up in its
//!   backlog drain. So only an up-to-date webhook delivers "as it happens".
//! - Any unsuccessful delivery (non-2xx, timeout, connection error) mutes
//!   the webhook for its mute time (or the receiver's `Retry-After`, capped).
//!   The checker tick unmutes it once that passes, and it drains its backlog.
//! - A position that can no longer continue (offset behind retention, or a
//!   replaced log) pauses the subscription until an admin chooses how to
//!   handle the loss. Plain resume of such a subscription fails with a clear
//!   message.

pub mod db;
pub mod delivery;
pub mod engine;
pub mod model;
pub mod pull;
pub mod read;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use self::db::NotifyDb;
use self::engine::{Limits, Next};
use self::model::{
    Batch, Filter, Pause, PauseReason, Phase, SnapshotState, SubState, Subscription,
    SubscriptionKind,
};
use super::config::{ChangelogConfig, WebhookConfig};
use crate::storage::errors::StorageError;
use crate::storage::store::{ChangeListener, DeleteGuard, LocalObjectStore};
use crate::storage::time::now_ms;

/// Families this module keeps in `admin.rocksdb`.
pub const ADMIN_DB_FAMILIES: [&str; 2] = [db::CF_SUBSCRIPTIONS, db::CF_STATE];

/// Header values are never echoed back.
pub const MASKED: &str = "********";

const TOKEN_PREFIX: &str = "rs3cl_";
const SECRET_PREFIX: &str = "whsec_";
const TRIM_BATCH: usize = 50_000;

#[derive(Debug)]
pub enum NotifyError {
    NotFound(String),
    BadRequest(String),
    /// The action conflicts with the subscription's state (e.g. resuming a
    /// subscription whose events expired).
    Conflict(String),
    /// Webhook position no longer retained.
    Gone(String),
    /// A paused replication token.
    Forbidden(String),
    Unauthorized,
    Internal(String),
}

impl std::fmt::Display for NotifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NotifyError::NotFound(m)
            | NotifyError::BadRequest(m)
            | NotifyError::Conflict(m)
            | NotifyError::Gone(m)
            | NotifyError::Forbidden(m)
            | NotifyError::Internal(m) => f.write_str(m),
            NotifyError::Unauthorized => f.write_str("invalid replication token"),
        }
    }
}

impl From<StorageError> for NotifyError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::BucketNotFound(b) => NotifyError::NotFound(format!("bucket {b} not found")),
            other => NotifyError::Internal(other.to_string()),
        }
    }
}

pub type NResult<T> = std::result::Result<T, NotifyError>;

/// What an admin submits to create a subscription.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateRequest {
    pub name: String,
    pub bucket: String,
    pub kind: SubscriptionKind,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub filter: Filter,
    /// Start with a snapshot of every existing object, then go live.
    #[serde(default)]
    pub initial_snapshot: bool,
    /// Webhook signing secret; generated when absent.
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub batch_max_events: Option<usize>,
    #[serde(default)]
    pub batch_max_bytes: Option<usize>,
    #[serde(default)]
    pub mute_secs: Option<u64>,
}

/// Changes to an existing subscription. Absent fields stay as they are; a
/// header whose value is `null` (or the mask) keeps its stored value.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct UpdateRequest {
    pub name: Option<String>,
    pub url: Option<String>,
    pub headers: Option<BTreeMap<String, Option<String>>>,
    pub filter: Option<Filter>,
    pub batch_max_events: Option<Option<usize>>,
    pub batch_max_bytes: Option<Option<usize>>,
    pub mute_secs: Option<Option<u64>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    Pause,
    Resume,
    ResumeFromOldest,
    Resnapshot,
    SkipToHead,
    SetOffset { offset: u64 },
    Unmute,
    RotateSecret,
    RotateToken,
}

/// Admin-facing view of a subscription. Secrets are masked.
#[derive(Debug, Clone, Serialize)]
pub struct SubView {
    pub id: String,
    pub name: String,
    pub bucket: String,
    pub kind: SubscriptionKind,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub filter: Filter,
    pub batch_max_events: Option<usize>,
    pub batch_max_bytes: Option<usize>,
    pub mute_secs: Option<u64>,
    pub created_at_ms: i64,
    pub created_by: String,
    /// `active` | `muted` | `paused` | `snapshot`
    pub status: &'static str,
    pub paused: Option<Pause>,
    pub muted_until_ms: i64,
    pub phase: Phase,
    pub offset: u64,
    pub snapshot_cursor: Option<String>,
    pub head: Option<u64>,
    /// Changes not yet delivered (`head - offset`; includes filtered-out ones).
    pub lag: Option<u64>,
    pub trimmed_through: Option<u64>,
    /// When the oldest undelivered change is trimmed by retention — after
    /// that the subscription pauses as expired.
    pub expires_at_ms: Option<i64>,
    pub consecutive_failures: u32,
    pub last_attempt_ms: i64,
    pub last_success_ms: i64,
    pub last_status: Option<u16>,
    pub last_error: Option<String>,
    pub delivered_events: u64,
    pub delivered_batches: u64,
}

/// Returned by create and by secret/token rotation: the only time a secret
/// is shown.
#[derive(Debug, Clone, Serialize)]
pub struct Created {
    pub subscription: SubView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
}

struct Runtime {
    sub: RwLock<Subscription>,
    state: Mutex<SubState>,
    /// Bumped by every admin action, so a delivery that started before it
    /// does not overwrite the admin's change when it finishes.
    generation: AtomicU64,
    wake: Notify,
    cancel: CancellationToken,
}

impl Runtime {
    fn sub(&self) -> Subscription {
        self.sub.read().unwrap().clone()
    }
}

struct Inner {
    store: LocalObjectStore,
    db: NotifyDb,
    cfg: WebhookConfig,
    changelog: ChangelogConfig,
    client: reqwest::Client,
    permits: Arc<Semaphore>,
    shutdown: CancellationToken,
    subs: RwLock<HashMap<String, Arc<Runtime>>>,
}

/// The global subscription service. Cheap to clone.
#[derive(Clone)]
pub struct NotifyService {
    inner: Arc<Inner>,
}

fn random_hex(bytes: usize) -> String {
    use rand::Rng;
    let mut buf = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn hash_token(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

impl NotifyService {
    /// Loads every subscription, hooks into the store's commit path and
    /// starts the workers, the checker tick and the trim loop.
    pub async fn start(
        store: LocalObjectStore,
        db: NotifyDb,
        cfg: WebhookConfig,
        changelog: ChangelogConfig,
        shutdown: CancellationToken,
    ) -> NResult<Self> {
        let service = Self {
            inner: Arc::new(Inner {
                client: delivery::build_client(&cfg),
                permits: Arc::new(Semaphore::new(cfg.max_concurrent_deliveries.max(1))),
                store,
                db,
                cfg,
                changelog,
                shutdown,
                subs: RwLock::new(HashMap::new()),
            }),
        };
        for (sub, state) in service.inner.db.load_all().await? {
            service.install(sub, state);
        }
        let weak = Arc::downgrade(&service.inner);
        service
            .inner
            .store
            .set_change_listener(ChangeListener(Arc::new(move |bucket, _watermark| {
                if let Some(inner) = weak.upgrade() {
                    for rt in inner.subs.read().unwrap().values() {
                        if rt.sub.read().unwrap().bucket == bucket {
                            rt.wake.notify_one();
                        }
                    }
                }
            })));
        let weak = Arc::downgrade(&service.inner);
        service.inner.store.set_delete_guard(DeleteGuard(Arc::new(move |bucket| {
            let inner = weak.upgrade()?;
            let names: Vec<String> = inner
                .subs
                .read()
                .unwrap()
                .values()
                .filter_map(|rt| {
                    let sub = rt.sub.read().unwrap();
                    (sub.bucket == bucket).then(|| format!("{} ({})", sub.name, sub.id))
                })
                .collect();
            (!names.is_empty()).then(|| {
                format!(
                    "bucket {bucket} still has changelog subscriptions; delete them first: {}",
                    names.join(", ")
                )
            })
        })));
        service.spawn_checker();
        service.spawn_trimmer();
        log::info!(
            "changelog subscriptions loaded count={}",
            service.inner.subs.read().unwrap().len()
        );
        Ok(service)
    }

    fn install(&self, sub: Subscription, state: SubState) -> Arc<Runtime> {
        let kind = sub.kind;
        let rt = Arc::new(Runtime {
            sub: RwLock::new(sub.clone()),
            state: Mutex::new(state),
            generation: AtomicU64::new(0),
            wake: Notify::new(),
            cancel: self.inner.shutdown.child_token(),
        });
        self.inner.subs.write().unwrap().insert(sub.id.clone(), rt.clone());
        if kind == SubscriptionKind::Webhook {
            let service = self.clone();
            let worker = rt.clone();
            tokio::spawn(async move { service.run_webhook(worker).await });
        }
        rt
    }

    fn runtime(&self, id: &str) -> NResult<Arc<Runtime>> {
        self.inner
            .subs
            .read()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| NotifyError::NotFound(format!("subscription {id} not found")))
    }

    fn limits(&self, sub: &Subscription) -> Limits {
        Limits {
            max_events: sub
                .batch_max_events
                .unwrap_or(self.inner.cfg.batch_max_events)
                .clamp(1, 10_000),
            max_bytes: sub
                .batch_max_bytes
                .unwrap_or(self.inner.cfg.batch_max_bytes)
                .clamp(1024, 64 * 1024 * 1024),
        }
    }

    // ── webhook worker ──────────────────────────────────────────────────────

    async fn run_webhook(&self, rt: Arc<Runtime>) {
        // Drain whatever accumulated while the server was down.
        rt.wake.notify_one();
        loop {
            tokio::select! {
                _ = rt.cancel.cancelled() => break,
                _ = rt.wake.notified() => {}
            }
            self.drain(&rt).await;
        }
    }

    /// Delivers batches until the webhook is up to date, muted or paused.
    async fn drain(&self, rt: &Arc<Runtime>) {
        loop {
            if rt.cancel.is_cancelled() {
                return;
            }
            let generation = rt.generation.load(Ordering::SeqCst);
            let state = rt.state.lock().await.clone();
            if state.paused.is_some() || state.muted_until_ms > now_ms() {
                return;
            }
            let sub = rt.sub();
            let index = match self.inner.store.index(&sub.bucket).await {
                Ok(index) => index,
                // The checker tick retries once the rebuild finishes.
                Err(StorageError::BucketRebuilding(_)) => return,
                Err(StorageError::BucketNotFound(_)) => {
                    self.pause_from_worker(rt, generation, bucket_gone(&sub)).await;
                    return;
                }
                Err(err) => {
                    log::warn!("webhook {} cannot open bucket {}: {err}", sub.id, sub.bucket);
                    return;
                }
            };
            let site = self.inner.store.site_id();
            let next = engine::next(&index, &sub, &state, self.limits(&sub), &site).await;
            drop(index);
            match next {
                Err(err) => {
                    log::warn!("webhook {} cannot read changelog: {err}", sub.id);
                    return;
                }
                Ok(Next::Pause(pause)) => {
                    log::warn!("webhook {} paused: {}", sub.id, pause.message);
                    self.pause_from_worker(rt, generation, pause).await;
                    return;
                }
                Ok(Next::UpToDate { position }) => {
                    if position != state.position() {
                        self.apply_from_worker(rt, generation, |st| st.set_position(position)).await;
                    }
                    return;
                }
                Ok(Next::Deliver { batch, after }) => {
                    let delivered = self.deliver(&sub, &batch).await;
                    let ok = delivered.is_ok();
                    let events = batch.events.len() as u64;
                    let mute = self.mute_duration(&sub, delivered.as_ref().err());
                    self.apply_from_worker(rt, generation, |st| {
                        st.last_attempt_ms = now_ms();
                        match &delivered {
                            Ok(status) => {
                                st.set_position(after);
                                st.last_status = Some(*status);
                                st.last_error = None;
                                st.last_success_ms = now_ms();
                                st.consecutive_failures = 0;
                                st.delivered_events += events;
                                st.delivered_batches += 1;
                            }
                            Err(failure) => {
                                st.last_status = failure.status;
                                st.last_error = Some(failure.message.clone());
                                st.consecutive_failures += 1;
                                st.muted_until_ms = now_ms() + mute.as_millis() as i64;
                            }
                        }
                    })
                    .await;
                    if !ok {
                        if let Err(failure) = &delivered {
                            log::info!(
                                "webhook {} muted for {}s: {}",
                                sub.id,
                                mute.as_secs(),
                                failure.message
                            );
                        }
                        return;
                    }
                }
            }
        }
    }

    async fn deliver(&self, sub: &Subscription, batch: &Batch) -> Result<u16, delivery::Failure> {
        let _permit = self
            .inner
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("delivery semaphore is never closed");
        let body = serde_json::to_vec(batch).expect("batch serializes");
        delivery::send(
            &self.inner.client,
            &self.inner.cfg,
            delivery::Request {
                url: &sub.url,
                subscription: &sub.id,
                delivery_id: &random_hex(16),
                secret: &sub.secret,
                headers: &sub.headers,
                body,
            },
        )
        .await
    }

    fn mute_duration(&self, sub: &Subscription, failure: Option<&delivery::Failure>) -> Duration {
        let base = sub.mute_secs.unwrap_or(self.inner.cfg.mute_secs);
        let wanted = failure
            .and_then(|f| f.retry_after_secs)
            .map(|retry| retry.max(base))
            .unwrap_or(base);
        Duration::from_secs(wanted.min(self.inner.cfg.max_mute_secs.max(base)))
    }

    /// Applies a worker's state change unless an admin action intervened.
    async fn apply_from_worker(&self, rt: &Runtime, generation: u64, change: impl FnOnce(&mut SubState)) {
        let mut state = rt.state.lock().await;
        if rt.generation.load(Ordering::SeqCst) != generation {
            return;
        }
        change(&mut state);
        let id = rt.sub.read().unwrap().id.clone();
        if let Err(err) = self.inner.db.put_state(&id, &state, false).await {
            log::warn!("could not persist subscription {id} state: {err}");
        }
    }

    async fn pause_from_worker(&self, rt: &Runtime, generation: u64, pause: Pause) {
        self.apply_from_worker(rt, generation, |st| st.paused = Some(pause)).await;
    }

    // ── checker tick and trimming ───────────────────────────────────────────

    fn spawn_checker(&self) {
        let service = self.clone();
        let every = Duration::from_secs(self.inner.cfg.check_interval_secs.max(1));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = service.inner.shutdown.cancelled() => break,
                    _ = tick.tick() => service.check().await,
                }
            }
        });
    }

    /// One checker pass: unmute webhooks whose mute expired and wake every
    /// active webhook (a safety net for anything a wake-up missed, e.g. a
    /// rebuild that just finished).
    pub async fn check(&self) {
        let now = now_ms();
        let runtimes: Vec<Arc<Runtime>> = self.inner.subs.read().unwrap().values().cloned().collect();
        for rt in runtimes {
            if rt.sub.read().unwrap().kind != SubscriptionKind::Webhook {
                continue;
            }
            let mut state = rt.state.lock().await;
            if state.paused.is_some() {
                continue;
            }
            if state.muted_until_ms != 0 && state.muted_until_ms <= now {
                state.muted_until_ms = 0;
                let id = rt.sub.read().unwrap().id.clone();
                let _ = self.inner.db.put_state(&id, &state, false).await;
            }
            if state.muted_until_ms == 0 {
                rt.wake.notify_one();
            }
        }
    }

    fn spawn_trimmer(&self) {
        let service = self.clone();
        let every = Duration::from_secs(self.inner.changelog.trim_interval_secs.max(1));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = service.inner.shutdown.cancelled() => break,
                    _ = tick.tick() => {
                        let cutoff = now_ms()
                            - (service.inner.changelog.retention_days as i64) * 86_400_000;
                        service.trim_all(cutoff).await;
                    }
                }
            }
        });
    }

    /// Trims every bucket's changelog of entries committed before `cutoff_ms`.
    /// Subscriptions left behind pause as expired on their next attempt.
    pub async fn trim_all(&self, cutoff_ms: i64) -> usize {
        let buckets = match self.inner.store.list_buckets().await {
            Ok(buckets) => buckets,
            Err(err) => {
                log::warn!("changelog trim cannot list buckets: {err}");
                return 0;
            }
        };
        let mut total = 0;
        for (bucket, _) in buckets {
            let Ok(index) = self.inner.store.index(&bucket).await else {
                continue;
            };
            loop {
                match index.trim_changelog(cutoff_ms, TRIM_BATCH).await {
                    Ok(0) => break,
                    Ok(n) => total += n,
                    Err(err) => {
                        log::warn!("changelog trim failed bucket={bucket} error={err}");
                        break;
                    }
                }
            }
        }
        if total > 0 {
            log::info!("changelog trim removed {total} entries");
        }
        total
    }

    // ── admin operations ────────────────────────────────────────────────────

    pub async fn create(&self, request: CreateRequest, created_by: &str) -> NResult<Created> {
        let name = request.name.trim().to_string();
        if name.is_empty() || name.len() > 128 {
            return Err(NotifyError::BadRequest("name must be 1-128 characters".into()));
        }
        let index = self.inner.store.index(&request.bucket).await?;
        let mut sub = Subscription {
            v: model::RECORD_VERSION,
            id: format!(
                "{}{}",
                match request.kind {
                    SubscriptionKind::Webhook => "wh_",
                    SubscriptionKind::Pull => "rt_",
                },
                random_hex(8)
            ),
            name,
            bucket: request.bucket.clone(),
            kind: request.kind,
            filter: request.filter,
            url: String::new(),
            headers: BTreeMap::new(),
            secret: String::new(),
            token_hash: String::new(),
            batch_max_events: request.batch_max_events,
            batch_max_bytes: request.batch_max_bytes,
            mute_secs: request.mute_secs,
            created_at_ms: now_ms(),
            created_by: created_by.to_string(),
        };
        let mut token = None;
        let mut secret = None;
        match request.kind {
            SubscriptionKind::Webhook => {
                delivery::validate_url(&request.url, &self.inner.cfg).map_err(NotifyError::BadRequest)?;
                delivery::validate_headers(&request.headers).map_err(NotifyError::BadRequest)?;
                sub.url = request.url;
                sub.headers = request.headers;
                let value = request
                    .secret
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| format!("{SECRET_PREFIX}{}", random_hex(24)));
                sub.secret = value.clone();
                secret = Some(value);
            }
            SubscriptionKind::Pull => {
                let value = format!("{TOKEN_PREFIX}{}", random_hex(32));
                sub.token_hash = hash_token(&value);
                token = Some(value);
            }
        }
        let head = index.changelog_head();
        let state = SubState {
            v: model::RECORD_VERSION,
            log_id: index.changelog_id().to_string(),
            offset: head,
            // Readers of a pull token run their own snapshot (they own the
            // position), so only a webhook starts with one server-side.
            snapshot: (request.initial_snapshot && request.kind == SubscriptionKind::Webhook).then_some(SnapshotState {
                through: head,
                cursor: None,
                begun: false,
            }),
            ..SubState::default()
        };
        drop(index);
        self.inner.db.put(&sub, &state).await?;
        let id = sub.id.clone();
        self.install(sub, state);
        log::info!("changelog subscription created id={id} by={created_by}");
        Ok(Created {
            subscription: self.view(&id).await?,
            token,
            secret,
        })
    }

    pub async fn update(&self, id: &str, request: UpdateRequest) -> NResult<SubView> {
        let rt = self.runtime(id)?;
        let mut sub = rt.sub();
        if let Some(name) = request.name {
            let name = name.trim().to_string();
            if name.is_empty() || name.len() > 128 {
                return Err(NotifyError::BadRequest("name must be 1-128 characters".into()));
            }
            sub.name = name;
        }
        if sub.kind == SubscriptionKind::Webhook {
            if let Some(url) = request.url {
                delivery::validate_url(&url, &self.inner.cfg).map_err(NotifyError::BadRequest)?;
                sub.url = url;
            }
            if let Some(headers) = request.headers {
                let mut merged = BTreeMap::new();
                for (name, value) in headers {
                    let value = match value.filter(|v| v != MASKED) {
                        Some(value) => value,
                        None => match sub.headers.get(&name) {
                            Some(existing) => existing.clone(),
                            None => {
                                return Err(NotifyError::BadRequest(format!(
                                    "header {name:?} has no value"
                                )))
                            }
                        },
                    };
                    merged.insert(name, value);
                }
                delivery::validate_headers(&merged).map_err(NotifyError::BadRequest)?;
                sub.headers = merged;
            }
        }
        if let Some(filter) = request.filter {
            sub.filter = filter;
        }
        if let Some(v) = request.batch_max_events {
            sub.batch_max_events = v;
        }
        if let Some(v) = request.batch_max_bytes {
            sub.batch_max_bytes = v;
        }
        if let Some(v) = request.mute_secs {
            sub.mute_secs = v;
        }
        let state = rt.state.lock().await.clone();
        self.inner.db.put(&sub, &state).await?;
        *rt.sub.write().unwrap() = sub;
        rt.wake.notify_one();
        self.view(id).await
    }

    pub async fn delete(&self, id: &str) -> NResult<()> {
        let rt = self.runtime(id)?;
        self.inner.db.delete(id).await?;
        rt.cancel.cancel();
        self.inner.subs.write().unwrap().remove(id);
        log::info!("changelog subscription deleted id={id}");
        Ok(())
    }

    /// Subscription ids on `bucket` — a bucket cannot be deleted while any
    /// exist.
    pub fn subscriptions_for_bucket(&self, bucket: &str) -> Vec<String> {
        self.inner
            .subs
            .read()
            .unwrap()
            .values()
            .filter(|rt| rt.sub.read().unwrap().bucket == bucket)
            .map(|rt| rt.sub.read().unwrap().id.clone())
            .collect()
    }

    pub async fn list(&self, bucket: Option<&str>) -> NResult<Vec<SubView>> {
        let mut ids: Vec<(String, String, i64)> = self
            .inner
            .subs
            .read()
            .unwrap()
            .values()
            .map(|rt| {
                let sub = rt.sub.read().unwrap();
                (sub.id.clone(), sub.bucket.clone(), sub.created_at_ms)
            })
            .filter(|(_, b, _)| bucket.is_none_or(|want| want == b))
            .collect();
        ids.sort_by(|a, b| (&a.1, a.2).cmp(&(&b.1, b.2)));
        let mut out = Vec::with_capacity(ids.len());
        for (id, _, _) in ids {
            out.push(self.view(&id).await?);
        }
        Ok(out)
    }

    pub async fn view(&self, id: &str) -> NResult<SubView> {
        let rt = self.runtime(id)?;
        let sub = rt.sub();
        let state = rt.state.lock().await.clone();
        let mut head = None;
        let mut trimmed = None;
        let mut expires_at_ms = None;
        if let Ok(index) = self.inner.store.index(&sub.bucket).await {
            if let Ok(status) = index.changelog_status().await {
                head = Some(status.head);
                trimmed = Some(status.trimmed_through);
                let offset = state.snapshot.as_ref().map(|s| s.through).unwrap_or(state.offset);
                if offset < status.head && state.log_id == status.log_id {
                    if let Ok(page) = index.read_changelog(offset, 1, usize::MAX).await {
                        expires_at_ms = page.events.first().map(|e| {
                            e.ts_ms + (self.inner.changelog.retention_days as i64) * 86_400_000
                        });
                    }
                }
            }
        }
        let status = if state.paused.is_some() {
            "paused"
        } else if state.muted_until_ms > now_ms() {
            "muted"
        } else if state.snapshot.is_some() {
            "snapshot"
        } else {
            "active"
        };
        Ok(SubView {
            id: sub.id.clone(),
            name: sub.name.clone(),
            bucket: sub.bucket.clone(),
            kind: sub.kind,
            url: sub.url.clone(),
            headers: sub.headers.keys().map(|k| (k.clone(), MASKED.to_string())).collect(),
            filter: sub.filter.clone(),
            batch_max_events: sub.batch_max_events,
            batch_max_bytes: sub.batch_max_bytes,
            mute_secs: sub.mute_secs,
            created_at_ms: sub.created_at_ms,
            created_by: sub.created_by.clone(),
            status,
            paused: state.paused.clone(),
            muted_until_ms: state.muted_until_ms,
            phase: if state.snapshot.is_some() { Phase::Snapshot } else { Phase::Live },
            offset: state.offset,
            snapshot_cursor: state.snapshot.as_ref().and_then(|s| s.cursor.clone()),
            head,
            lag: head.map(|h| h.saturating_sub(state.offset)),
            trimmed_through: trimmed,
            expires_at_ms,
            consecutive_failures: state.consecutive_failures,
            last_attempt_ms: state.last_attempt_ms,
            last_success_ms: state.last_success_ms,
            last_status: state.last_status,
            last_error: state.last_error.clone(),
            delivered_events: state.delivered_events,
            delivered_batches: state.delivered_batches,
        })
    }

    /// Runs an admin action. Rotations return the new secret / token.
    pub async fn act(&self, id: &str, action: Action) -> NResult<Created> {
        let rt = self.runtime(id)?;
        let sub = rt.sub();
        if sub.kind == SubscriptionKind::Pull
            && !matches!(action, Action::Pause | Action::Resume | Action::RotateToken)
        {
            return Err(NotifyError::BadRequest(
                "replication tokens have no server-side position: readers choose it themselves (after=)".into(),
            ));
        }
        let mut shown_secret = None;
        let mut shown_token = None;
        {
            let mut state = rt.state.lock().await;
            let index = self.inner.store.index(&sub.bucket).await?;
            let mut next = state.clone();
            match action {
                Action::Pause => {
                    if next.paused.is_none() {
                        next.paused = Some(Pause {
                            reason: PauseReason::Operator,
                            message: "paused by an administrator".to_string(),
                            at_ms: now_ms(),
                        });
                    }
                }
                Action::Resume if sub.kind == SubscriptionKind::Pull => {
                    next.paused = None;
                }
                Action::Resume => {
                    // Resume continues exactly where it stopped — or fails,
                    // changing nothing, when that position cannot continue.
                    if let Some(pause) = engine::check_continuity(&index, &sub, &next.position()).await? {
                        let refusal = pause.message.clone();
                        if next.paused.as_ref().is_none_or(|p| p.reason == PauseReason::Operator) {
                            next.paused = Some(pause);
                            self.inner.db.put_state(id, &next, true).await?;
                            rt.generation.fetch_add(1, Ordering::SeqCst);
                            *state = next;
                        }
                        return Err(NotifyError::Conflict(refusal));
                    }
                    next.paused = None;
                    next.muted_until_ms = 0;
                }
                Action::ResumeFromOldest => {
                    let status = index.changelog_status().await?;
                    next.log_id = status.log_id;
                    next.offset = status.trimmed_through;
                    next.snapshot = None;
                    next.paused = None;
                    next.muted_until_ms = 0;
                }
                Action::Resnapshot => {
                    let head = index.changelog_head();
                    next.log_id = index.changelog_id().to_string();
                    next.offset = head;
                    next.snapshot = Some(SnapshotState {
                        through: head,
                        cursor: None,
                        begun: false,
                    });
                    next.paused = None;
                    next.muted_until_ms = 0;
                }
                Action::SkipToHead => {
                    next.log_id = index.changelog_id().to_string();
                    next.offset = index.changelog_head();
                    next.snapshot = None;
                    next.paused = None;
                    next.muted_until_ms = 0;
                }
                Action::SetOffset { offset } => {
                    let status = index.changelog_status().await?;
                    if offset < status.trimmed_through || offset > status.head {
                        return Err(NotifyError::BadRequest(format!(
                            "offset must be between {} (oldest retained - 1) and {} (head)",
                            status.trimmed_through, status.head
                        )));
                    }
                    next.log_id = status.log_id;
                    next.offset = offset;
                    next.snapshot = None;
                    next.paused = None;
                    next.muted_until_ms = 0;
                }
                Action::Unmute => {
                    next.muted_until_ms = 0;
                }
                Action::RotateSecret => {
                    if sub.kind != SubscriptionKind::Webhook {
                        return Err(NotifyError::BadRequest("only webhooks have a secret".into()));
                    }
                    let value = format!("{SECRET_PREFIX}{}", random_hex(24));
                    let mut updated = sub.clone();
                    updated.secret = value.clone();
                    self.inner.db.put(&updated, &next).await?;
                    *rt.sub.write().unwrap() = updated;
                    shown_secret = Some(value);
                }
                Action::RotateToken => {
                    if sub.kind != SubscriptionKind::Pull {
                        return Err(NotifyError::BadRequest("only replication subscriptions have a token".into()));
                    }
                    let value = format!("{TOKEN_PREFIX}{}", random_hex(32));
                    let mut updated = sub.clone();
                    updated.token_hash = hash_token(&value);
                    self.inner.db.put(&updated, &next).await?;
                    *rt.sub.write().unwrap() = updated;
                    shown_token = Some(value);
                }
            }
            self.inner.db.put_state(id, &next, true).await?;
            rt.generation.fetch_add(1, Ordering::SeqCst);
            *state = next;
        }
        rt.wake.notify_one();
        Ok(Created {
            subscription: self.view(id).await?,
            token: shown_token,
            secret: shown_secret,
        })
    }

    // ── pull ────────────────────────────────────────────────────────────────

    fn runtime_for_token(&self, token: &str) -> NResult<Arc<Runtime>> {
        if !token.starts_with(TOKEN_PREFIX) {
            return Err(NotifyError::Unauthorized);
        }
        let hash = hash_token(token);
        self.inner
            .subs
            .read()
            .unwrap()
            .values()
            .find(|rt| {
                let sub = rt.sub.read().unwrap();
                sub.kind == SubscriptionKind::Pull && sub.token_hash == hash
            })
            .cloned()
            .ok_or(NotifyError::Unauthorized)
    }

    /// Resolves a replication token to what it may read: its bucket and
    /// filter. Tokens carry no position — readers keep their own and pass it
    /// as `after=`.
    pub async fn token_scope(&self, token: &str) -> NResult<(String, Filter)> {
        let rt = self.runtime_for_token(token)?;
        let sub = rt.sub();
        let state = rt.state.lock().await;
        if state.paused.is_some() {
            return Err(NotifyError::Forbidden(format!(
                "replication token \"{}\" is paused by an administrator",
                sub.name
            )));
        }
        Ok((sub.bucket, sub.filter))
    }

    /// Records a served read for the console ("last read", "served through").
    /// Informational only: it never decides what a reader gets next.
    pub async fn record_read(&self, token: &str, served_through: u64) {
        let Ok(rt) = self.runtime_for_token(token) else { return };
        let id = rt.sub.read().unwrap().id.clone();
        let mut state = rt.state.lock().await;
        state.offset = served_through;
        state.last_attempt_ms = now_ms();
        state.last_success_ms = state.last_attempt_ms;
        state.delivered_batches += 1;
        if let Err(err) = self.inner.db.put_state(&id, &state, false).await {
            log::debug!("could not record read for {id}: {err}");
        }
    }
}

fn bucket_gone(sub: &Subscription) -> Pause {
    Pause {
        reason: PauseReason::BucketGone,
        message: format!("bucket \"{}\" no longer exists", sub.bucket),
        at_ms: now_ms(),
    }
}
