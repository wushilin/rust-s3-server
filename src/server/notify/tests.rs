//! End-to-end tests for changelog subscriptions against real, in-process
//! webhook receivers (axum on 127.0.0.1:0) whose replies are scripted per
//! request: 2xx, 429 + Retry-After, 5xx, or a stall past the client timeout.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use super::model::{Batch, Filter, PauseReason, Phase, SubscriptionKind};
use super::*;
use crate::server::config::{ChangelogConfig, WebhookConfig};
use crate::server::iam::IamStore;
use crate::storage::changelog::ChangeOrigin;
use crate::storage::store::LocalObjectStore;

// ── scripted receiver ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Reply {
    Ok,
    Status(u16),
    /// 429 with `Retry-After: <secs>`.
    SlowDown(u64),
    /// Never answer within the client's timeout.
    Stall,
}

#[derive(Debug, Clone)]
struct Attempt {
    at: Instant,
    headers: HeaderMap,
    body: Vec<u8>,
    batch: Batch,
    /// What the receiver answered.
    accepted: bool,
}

#[derive(Default)]
struct ReceiverState {
    script: Mutex<VecDeque<Reply>>,
    /// Reply once the script runs out.
    fallback: Mutex<Option<Reply>>,
    attempts: Mutex<Vec<Attempt>>,
}

#[derive(Clone)]
struct Receiver {
    url: String,
    state: Arc<ReceiverState>,
}

impl Receiver {
    async fn start() -> Self {
        let state = Arc::new(ReceiverState::default());
        let app = Router::new()
            .route("/hook", post(receive))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            url: format!("http://{addr}/hook"),
            state,
        }
    }

    fn script(&self, replies: impl IntoIterator<Item = Reply>) {
        self.state.script.lock().unwrap().extend(replies);
    }

    fn always(&self, reply: Reply) {
        *self.state.fallback.lock().unwrap() = Some(reply);
    }

    fn attempts(&self) -> Vec<Attempt> {
        self.state.attempts.lock().unwrap().clone()
    }

    /// Batches the receiver accepted (2xx), in arrival order.
    fn accepted(&self) -> Vec<Batch> {
        self.attempts()
            .into_iter()
            .filter(|a| a.accepted)
            .map(|a| a.batch)
            .collect()
    }

    /// Every accepted live event key, in delivery order.
    fn live_keys(&self) -> Vec<String> {
        self.accepted()
            .iter()
            .filter(|b| b.phase == Phase::Live)
            .flat_map(|b| b.events.iter().map(|e| e.key.clone()))
            .collect()
    }
}

async fn receive(State(state): State<Arc<ReceiverState>>, request: Request<Body>) -> Response {
    let headers = request.headers().clone();
    let body = to_bytes(request.into_body(), usize::MAX).await.unwrap().to_vec();
    let batch: Batch = serde_json::from_slice(&body).expect("receiver got a valid batch");
    let reply = state
        .script
        .lock()
        .unwrap()
        .pop_front()
        .or_else(|| state.fallback.lock().unwrap().clone())
        .unwrap_or(Reply::Ok);
    let accepted = matches!(reply, Reply::Ok);
    state.attempts.lock().unwrap().push(Attempt {
        at: Instant::now(),
        headers,
        body,
        batch,
        accepted,
    });
    match reply {
        Reply::Ok => StatusCode::OK.into_response(),
        Reply::Status(code) => StatusCode::from_u16(code).unwrap().into_response(),
        Reply::SlowDown(secs) => (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", secs.to_string())],
        )
            .into_response(),
        Reply::Stall => {
            tokio::time::sleep(Duration::from_secs(30)).await;
            StatusCode::OK.into_response()
        }
    }
}

// ── harness ─────────────────────────────────────────────────────────────────

struct Harness {
    tmp: tempfile::TempDir,
    store: LocalObjectStore,
    iam: IamStore,
    service: NotifyService,
    shutdown: CancellationToken,
}

fn test_config() -> WebhookConfig {
    WebhookConfig {
        check_interval_secs: 1,
        mute_secs: 1,
        max_mute_secs: 5,
        // Generous: a client-side timeout on a request the receiver did
        // process is a legitimate re-delivery, which would upset the exact
        // counts these tests assert when the machine is busy.
        request_timeout_secs: 5,
        max_concurrent_deliveries: 4,
        batch_max_events: 100,
        batch_max_bytes: 1024 * 1024,
        allow_http: true,
        allow_private_addresses: true,
    }
}

impl Harness {
    async fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        let iam = IamStore::open(tmp.path()).await.unwrap();
        let shutdown = CancellationToken::new();
        let service = start(&store, &iam, &shutdown).await;
        Self {
            tmp,
            store,
            iam,
            service,
            shutdown,
        }
    }

    /// Simulates a server restart: stops every task and starts a new service
    /// over the same data directory.
    async fn restart(&mut self) {
        self.shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.shutdown = CancellationToken::new();
        self.service = start(&self.store, &self.iam, &self.shutdown).await;
    }

    async fn put(&self, bucket: &str, key: &str, body: &[u8]) {
        self.store
            .put_object(bucket, key, body, None, None, false)
            .await
            .unwrap();
    }

    async fn webhook(&self, name: &str, bucket: &str, url: &str, filter: Filter, snapshot: bool) -> Created {
        self.service
            .create(
                CreateRequest {
                    name: name.to_string(),
                    bucket: bucket.to_string(),
                    kind: SubscriptionKind::Webhook,
                    url: url.to_string(),
                    headers: BTreeMap::from([("Authorization".to_string(), format!("Bearer {name}"))]),
                    filter,
                    initial_snapshot: snapshot,
                    secret: None,
                    batch_max_events: None,
                    batch_max_bytes: None,
                    mute_secs: None,
                },
                "tester",
            )
            .await
            .unwrap()
    }

    async fn view(&self, id: &str) -> SubView {
        self.service.view(id).await.unwrap()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn start(store: &LocalObjectStore, iam: &IamStore, shutdown: &CancellationToken) -> NotifyService {
    NotifyService::start(
        store.clone(),
        db::NotifyDb::new(iam.admin_db()),
        test_config(),
        ChangelogConfig::default(),
        shutdown.clone(),
    )
    .await
    .unwrap()
}

/// Polls `check` until it holds, or panics with `what` after `secs`.
async fn wait_until(secs: u64, what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_view(h: &Harness, id: &str, secs: u64, what: &str, check: impl Fn(&SubView) -> bool) -> SubView {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let view = h.view(id).await;
        if check(&view) {
            return view;
        }
        assert!(Instant::now() < deadline, "timed out waiting for: {what}; last view {view:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn prefix(p: &str) -> Filter {
    Filter {
        prefix: p.to_string(),
        ..Filter::default()
    }
}

/// Live batches of an unfiltered subscription tile the seq space: each
/// starts right after the previous one ended. Event seqs are strictly
/// increasing and inside their batch's range.
fn assert_contiguous(batches: &[Batch]) {
    assert_ordered(batches, true);
}

/// For a filtered subscription the offset also moves past filtered-out
/// changes without a delivery, so batches may leave gaps — never overlap.
fn assert_ordered(batches: &[Batch], tiled: bool) {
    let live: Vec<&Batch> = batches.iter().filter(|b| b.phase == Phase::Live).collect();
    for pair in live.windows(2) {
        if tiled {
            assert_eq!(pair[1].from_seq, pair[0].to_seq + 1, "batches must tile: {pair:?}");
        } else {
            assert!(pair[1].from_seq > pair[0].to_seq, "batches must not overlap: {pair:?}");
        }
    }
    let seqs: Vec<u64> = live.iter().flat_map(|b| b.events.iter().map(|e| e.seq.unwrap())).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "seqs not increasing: {seqs:?}");
    for b in &live {
        for e in &b.events {
            let seq = e.seq.unwrap();
            assert!(seq >= b.from_seq && seq <= b.to_seq, "event {seq} outside its batch");
        }
    }
}

// ── tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn webhook_delivers_signed_batches_with_custom_headers() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let created = h.webhook("hook-a", "bkt", &rx.url, Filter::default(), false).await;
    let secret = created.secret.clone().expect("secret shown at creation");
    assert_eq!(created.subscription.headers["Authorization"], MASKED, "header values are masked");

    h.put("bkt", "one.txt", b"1").await;
    h.put("bkt", "two.txt", b"22").await;
    h.put("bkt", "one.txt", b"111").await; // overwrite: new version, new etag
    h.store.delete_object("bkt", "two.txt").await.unwrap();

    wait_until(10, "4 events", || rx.live_keys().len() == 4).await;
    assert_eq!(rx.live_keys(), ["one.txt", "two.txt", "one.txt", "two.txt"]);
    let batches = rx.accepted();
    assert_contiguous(&batches);
    let events: Vec<_> = batches.iter().flat_map(|b| b.events.clone()).collect();
    assert_eq!(events.iter().map(|e| e.op.as_str()).collect::<Vec<_>>(), ["put", "put", "put", "delete"]);
    // Every state is identified: the overwrite changed both etag and version,
    // and the delete names the version (and etag) it removed.
    assert_ne!(events[0].etag, events[2].etag);
    assert_ne!(events[0].version, events[2].version);
    assert_eq!(events[3].version, events[1].version);
    assert_eq!(events[3].etag, events[1].etag);
    assert_eq!(events[1].size, 2);
    assert!(events.iter().all(|e| e.origin.as_deref() == Some("client")));

    for attempt in rx.attempts() {
        assert_eq!(attempt.headers["authorization"], "Bearer hook-a");
        assert_eq!(attempt.headers["x-rs3-subscription"], created.subscription.id.as_str());
        assert!(attempt.headers.contains_key("x-rs3-delivery"));
        let sig = attempt.headers["x-rs3-signature"].to_str().unwrap();
        let t: i64 = sig.split(',').next().unwrap().trim_start_matches("t=").parse().unwrap();
        assert_eq!(sig, delivery::signature(&secret, t, &attempt.body), "signature verifies");
    }
    let view = wait_view(&h, &created.subscription.id, 5, "caught up", |v| v.lag == Some(0)).await;
    assert_eq!(view.status, "active");
    assert_eq!(view.delivered_events, 4);
}

#[tokio::test]
async fn filters_skip_events_and_still_advance_the_offset() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let filter = Filter {
        prefix: "logs/".into(),
        suffix: ".txt".into(),
        ops: vec![],
    };
    let created = h.webhook("logs", "bkt", &rx.url, filter, false).await;
    let id = created.subscription.id;

    h.put("bkt", "img/a.png", b"x").await;
    h.put("bkt", "logs/a.txt", b"x").await;
    h.put("bkt", "logs/b.bin", b"x").await;
    wait_until(10, "the one matching event", || rx.live_keys() == ["logs/a.txt"]).await;

    // A long run of non-matching changes: nothing is sent, but the offset
    // keeps up with the head (non-contiguous offsets), so the subscription
    // never looks behind or pins retention.
    for i in 0..250 {
        h.put("bkt", &format!("img/{i}.png"), b"x").await;
    }
    let view = wait_view(&h, &id, 10, "offset advanced past filtered events", |v| v.lag == Some(0)).await;
    assert_eq!(rx.live_keys(), ["logs/a.txt"], "no deliveries for filtered events");
    assert!(view.offset >= 253);

    // Ops filter: deletes only.
    let rx2 = Receiver::start().await;
    let deletes = h
        .webhook("deletes", "bkt", &rx2.url, Filter { ops: vec![crate::storage::changelog::ChangeOp::Delete], ..Filter::default() }, false)
        .await;
    h.put("bkt", "z", b"x").await;
    h.store.delete_object("bkt", "z").await.unwrap();
    wait_until(10, "delete event", || !rx2.accepted().is_empty()).await;
    let events: Vec<_> = rx2.accepted().iter().flat_map(|b| b.events.clone()).collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].op, "delete");
    let _ = deletes;
}

#[tokio::test]
async fn several_receivers_get_their_streams_concurrently() {
    let h = Harness::new().await;
    for bucket in ["bkt1", "bkt2"] {
        h.store.create_bucket(bucket).await.unwrap();
    }
    let receivers = [
        Receiver::start().await,
        Receiver::start().await,
        Receiver::start().await,
        Receiver::start().await,
    ];
    // Two hooks on b1 (one filtered), one on b2, and one on b1 that keeps
    // failing — it must not hold up the others.
    h.webhook("all-b1", "bkt1", &receivers[0].url, Filter::default(), false).await;
    h.webhook("a-only-b1", "bkt1", &receivers[1].url, prefix("a/"), false).await;
    h.webhook("all-b2", "bkt2", &receivers[2].url, Filter::default(), false).await;
    receivers[3].always(Reply::Status(500));
    h.webhook("broken-b1", "bkt1", &receivers[3].url, Filter::default(), false).await;

    // Concurrent writers across both buckets.
    let mut writers = Vec::new();
    for w in 0..6 {
        let store = h.store.clone();
        writers.push(tokio::spawn(async move {
            for i in 0..20 {
                let bucket = if w % 2 == 0 { "bkt1" } else { "bkt2" };
                let dir = if i % 2 == 0 { "a" } else { "bkt" };
                store
                    .put_object(bucket, &format!("{dir}/{w}-{i}"), b"payload", None, None, false)
                    .await
                    .unwrap();
            }
        }));
    }
    for w in writers {
        w.await.unwrap();
    }

    let expect_b1: HashSet<String> = (0..6)
        .filter(|w| w % 2 == 0)
        .flat_map(|w| (0..20).map(move |i| format!("{}/{w}-{i}", if i % 2 == 0 { "a" } else { "bkt" })))
        .collect();
    let expect_b2: HashSet<String> = (0..6)
        .filter(|w| w % 2 == 1)
        .flat_map(|w| (0..20).map(move |i| format!("{}/{w}-{i}", if i % 2 == 0 { "a" } else { "bkt" })))
        .collect();
    let expect_a: HashSet<String> = expect_b1.iter().filter(|k| k.starts_with("a/")).cloned().collect();

    wait_until(15, "all three healthy streams complete", || {
        receivers[0].live_keys().len() == expect_b1.len()
            && receivers[1].live_keys().len() == expect_a.len()
            && receivers[2].live_keys().len() == expect_b2.len()
    })
    .await;
    for (rx, expected) in [(&receivers[0], &expect_b1), (&receivers[1], &expect_a), (&receivers[2], &expect_b2)] {
        let keys = rx.live_keys();
        let unique: HashSet<String> = keys.iter().cloned().collect();
        assert_eq!(&unique, expected);
        assert_eq!(unique.len(), keys.len(), "each event delivered exactly once when all succeed");
    }
    assert_contiguous(&receivers[0].accepted());
    assert_ordered(&receivers[1].accepted(), false);
    assert_contiguous(&receivers[2].accepted());
    {
    }
    assert!(receivers[3].accepted().is_empty());
}

#[tokio::test]
async fn slow_down_mutes_until_the_ticker_resumes_with_the_backlog() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    // First attempt: 429 asking for 3s. mute_secs is 1, so Retry-After wins.
    rx.script([Reply::SlowDown(3)]);
    let id = h.webhook("slow", "bkt", &rx.url, Filter::default(), false).await.subscription.id;

    h.put("bkt", "k1", b"x").await;
    wait_until(5, "first attempt", || rx.attempts().len() == 1).await;
    let refused_at = rx.attempts()[0].at;
    let muted = wait_view(&h, &id, 5, "muted", |v| v.status == "muted").await;
    assert_eq!(muted.last_status, Some(429));
    assert_eq!(muted.consecutive_failures, 1);
    let mute_ms = muted.muted_until_ms - now_ms();
    assert!((1500..=3100).contains(&mute_ms), "Retry-After honoured: {mute_ms}ms left");

    // Changes while muted are skipped (no request), not lost.
    h.put("bkt", "k2", b"x").await;
    h.put("bkt", "k3", b"x").await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(rx.attempts().len(), 1, "a muted webhook sends nothing");

    // The checker tick (1s) unmutes it after ~3s; it then drains everything
    // in one batch starting where the refused one started.
    wait_until(6, "resumed delivery", || rx.accepted().len() == 1).await;
    let retry = &rx.attempts()[1];
    assert!(retry.at - refused_at >= Duration::from_millis(2900), "stayed muted for Retry-After");
    assert_eq!(retry.batch.from_seq, rx.attempts()[0].batch.from_seq, "same start as the refused batch");
    assert_eq!(rx.live_keys(), ["k1", "k2", "k3"]);
    let view = wait_view(&h, &id, 5, "healthy again", |v| v.status == "active" && v.lag == Some(0)).await;
    assert_eq!(view.consecutive_failures, 0);
    assert_eq!(view.last_status, Some(200));
}

#[tokio::test]
async fn any_failure_mutes_errors_timeouts_and_refused_connections() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    rx.script([Reply::Status(503), Reply::Status(400), Reply::Stall]);
    let id = h.webhook("flaky", "bkt", &rx.url, Filter::default(), false).await.subscription.id;
    h.put("bkt", "k", b"x").await;

    wait_until(30, "delivered after three failures", || rx.accepted().len() == 1).await;
    let attempts = rx.attempts();
    assert_eq!(attempts.len(), 4);
    // Each failure muted the hook for ~mute_secs (1s) before the next try,
    // resumed by the 1s ticker.
    for pair in attempts.windows(2) {
        assert!(pair[1].at - pair[0].at >= Duration::from_millis(900), "muted between attempts");
    }
    assert_eq!(rx.live_keys(), ["k"]);
    let view = wait_view(&h, &id, 5, "recovered", |v| v.consecutive_failures == 0).await;
    assert_eq!(view.status, "active");

    // A receiver that is gone entirely (connection refused) mutes too.
    let dead = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}/hook", listener.local_addr().unwrap())
    };
    let dead_id = h.webhook("dead", "bkt", &dead, Filter::default(), false).await.subscription.id;
    h.put("bkt", "k2", b"x").await;
    let view = wait_view(&h, &dead_id, 5, "dead receiver muted", |v| v.status == "muted").await;
    assert!(view.last_error.unwrap().contains("connection failed"));
    assert_eq!(view.last_status, None);
}

#[tokio::test]
async fn initial_snapshot_then_live_changes() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    for key in ["data/1", "data/2", "data/3", "data/4", "data/5", "other/x"] {
        h.put("bkt", key, b"x").await;
    }
    let rx = Receiver::start().await;
    let created = h
        .service
        .create(
            CreateRequest {
                name: "snap".into(),
                bucket: "bkt".into(),
                kind: SubscriptionKind::Webhook,
                url: rx.url.clone(),
                headers: BTreeMap::new(),
                filter: prefix("data/"),
                initial_snapshot: true,
                secret: None,
                batch_max_events: Some(2),
                batch_max_bytes: None,
                mute_secs: None,
            },
            "tester",
        )
        .await
        .unwrap();
    assert_eq!(created.subscription.status, "snapshot");

    wait_until(10, "snapshot end", || rx.accepted().iter().any(|b| b.snapshot_end)).await;
    let snapshot: Vec<Batch> = rx.accepted().into_iter().filter(|b| b.phase == Phase::Snapshot).collect();
    assert!(snapshot.len() >= 3, "batch_max_events=2 splits 5 entries");
    assert!(snapshot[0].snapshot_begin);
    assert!(snapshot.iter().skip(1).all(|b| !b.snapshot_begin));
    assert!(snapshot.last().unwrap().snapshot_end);
    let keys: Vec<String> = snapshot.iter().flat_map(|b| b.events.iter().map(|e| e.key.clone())).collect();
    assert_eq!(keys, ["data/1", "data/2", "data/3", "data/4", "data/5"]);
    assert!(snapshot.iter().flat_map(|b| &b.events).all(|e| e.op == "exists" && e.seq.is_none() && !e.version.is_empty()));
    let through = snapshot[0].snapshot_through.unwrap();

    h.put("bkt", "data/6", b"x").await;
    h.put("bkt", "other/y", b"x").await;
    wait_until(10, "live event after snapshot", || rx.live_keys() == ["data/6"]).await;
    let live = rx.accepted().into_iter().find(|b| b.phase == Phase::Live).unwrap();
    assert_eq!(live.from_seq, through + 1, "live continues right after the snapshot point");
}

#[tokio::test]
async fn snapshot_of_an_empty_bucket_still_sends_begin_and_end() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    h.webhook("empty", "bkt", &rx.url, Filter::default(), true).await;
    wait_until(10, "one begin+end batch", || !rx.accepted().is_empty()).await;
    let batch = &rx.accepted()[0];
    assert!(batch.snapshot_begin && batch.snapshot_end && batch.events.is_empty());
}

#[tokio::test]
async fn expired_offset_pauses_and_plain_resume_is_refused() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let id = h.webhook("expiring", "bkt", &rx.url, Filter::default(), false).await.subscription.id;

    // An admin pauses the hook; changes pile up behind it.
    h.service.act(&id, Action::Pause).await.unwrap();
    for i in 0..5 {
        h.put("bkt", &format!("old-{i}"), b"x").await;
    }
    // Retention removes them all before the hook resumes.
    assert_eq!(h.service.trim_all(now_ms() + 1_000).await, 5);

    let err = h.service.act(&id, Action::Resume).await.unwrap_err();
    let NotifyError::Conflict(message) = err else { panic!("expected a conflict, got {err:?}") };
    assert!(message.contains("Cannot resume webhook \"expiring\" (bucket \"bkt\")"), "{message}");
    assert!(message.contains("deleted by retention"), "{message}");
    assert!(message.contains("resume from oldest retained"), "{message}");
    let view = h.view(&id).await;
    assert_eq!(view.paused.as_ref().unwrap().reason, PauseReason::Expired);
    assert_eq!(view.paused.unwrap().message, message, "the pause carries the same explanation");
    // Asking again gives the same clear answer, and nothing was delivered.
    assert!(matches!(h.service.act(&id, Action::Resume).await, Err(NotifyError::Conflict(_))));
    assert!(rx.attempts().is_empty());

    // Set offset is range-checked.
    assert!(matches!(
        h.service.act(&id, Action::SetOffset { offset: 1 }).await,
        Err(NotifyError::BadRequest(_))
    ));
    // Accept the loss: continue from the oldest retained entry.
    h.service.act(&id, Action::ResumeFromOldest).await.unwrap();
    h.put("bkt", "new", b"x").await;
    wait_until(10, "delivery after resume-from-oldest", || rx.live_keys() == ["new"]).await;
    assert_eq!(h.view(&id).await.status, "active");
}

#[tokio::test]
async fn a_muted_webhook_left_behind_by_retention_pauses_itself() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    rx.script([Reply::Status(500)]);
    let id = h.webhook("left-behind", "bkt", &rx.url, Filter::default(), false).await.subscription.id;
    h.put("bkt", "lost", b"x").await;
    wait_view(&h, &id, 5, "muted", |v| v.status == "muted").await;
    // Its undelivered change expires while it is muted.
    h.service.trim_all(now_ms() + 1_000).await;

    // On its next attempt (after the ticker unmutes it) it detects the gap
    // and pauses instead of skipping the lost event silently.
    let view = wait_view(&h, &id, 6, "paused as expired", |v| v.status == "paused").await;
    assert_eq!(view.paused.unwrap().reason, PauseReason::Expired);
    assert_eq!(rx.attempts().len(), 1, "no delivery past the gap");

    // Re-snapshot recovers a consistent state for the receiver.
    h.service.act(&id, Action::Resnapshot).await.unwrap();
    wait_until(10, "snapshot delivered", || rx.accepted().iter().any(|b| b.snapshot_end)).await;
    let keys: Vec<String> = rx.accepted().iter().flat_map(|b| b.events.iter().map(|e| e.key.clone())).collect();
    assert_eq!(keys, ["lost"]);
}

#[tokio::test]
async fn skip_to_head_and_set_offset_replay() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let id = h.webhook("ops", "bkt", &rx.url, Filter::default(), false).await.subscription.id;
    h.put("bkt", "a", b"x").await;
    h.put("bkt", "b", b"x").await;
    wait_until(10, "two events", || rx.live_keys().len() == 2).await;

    // Rewind and replay from the start of the retained log.
    h.service.act(&id, Action::SetOffset { offset: 0 }).await.unwrap();
    wait_until(10, "replayed", || rx.live_keys().len() == 4).await;
    assert_eq!(rx.live_keys(), ["a", "b", "a", "b"]);

    // Paused + skip to head: the backlog is skipped.
    h.service.act(&id, Action::Pause).await.unwrap();
    h.put("bkt", "skipped", b"x").await;
    h.service.act(&id, Action::SkipToHead).await.unwrap();
    h.put("bkt", "after", b"x").await;
    wait_until(10, "post-skip event", || rx.live_keys().len() == 5).await;
    assert_eq!(rx.live_keys().last().unwrap(), "after");
}

#[tokio::test]
async fn backlog_is_split_by_the_batch_limit() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let id = h
        .service
        .create(
            CreateRequest {
                name: "small".into(),
                bucket: "bkt".into(),
                kind: SubscriptionKind::Webhook,
                url: rx.url.clone(),
                headers: BTreeMap::new(),
                filter: Filter::default(),
                initial_snapshot: false,
                secret: None,
                batch_max_events: Some(3),
                batch_max_bytes: None,
                mute_secs: None,
            },
            "tester",
        )
        .await
        .unwrap()
        .subscription
        .id;
    h.service.act(&id, Action::Pause).await.unwrap();
    for i in 0..10 {
        h.put("bkt", &format!("k{i:02}"), b"x").await;
    }
    h.service.act(&id, Action::Resume).await.unwrap();
    wait_until(10, "backlog drained", || rx.live_keys().len() == 10).await;
    let sizes: Vec<usize> = rx.accepted().iter().map(|b| b.events.len()).collect();
    assert_eq!(sizes, [3, 3, 3, 1], "drained back-to-back in limit-sized batches");
    assert_contiguous(&rx.accepted());
}

#[tokio::test]
async fn operator_pause_resumes_where_it_stopped() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let id = h.webhook("p", "bkt", &rx.url, Filter::default(), false).await.subscription.id;
    h.service.act(&id, Action::Pause).await.unwrap();
    h.put("bkt", "while-paused", b"x").await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(rx.attempts().is_empty(), "paused hooks send nothing, even on ticks");
    h.service.act(&id, Action::Resume).await.unwrap();
    wait_until(10, "backlog after resume", || rx.live_keys() == ["while-paused"]).await;
}

#[tokio::test]
async fn state_survives_a_restart_without_redelivery() {
    let mut h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let id = h.webhook("durable", "bkt", &rx.url, Filter::default(), false).await.subscription.id;
    h.put("bkt", "before", b"x").await;
    wait_until(10, "first event", || rx.live_keys() == ["before"]).await;
    wait_view(&h, &id, 5, "acked", |v| v.lag == Some(0)).await;

    h.restart().await;
    h.put("bkt", "after", b"x").await;
    wait_until(10, "event after restart", || rx.live_keys().len() == 2).await;
    assert_eq!(rx.live_keys(), ["before", "after"], "nothing re-sent after restart");

    // A muted state survives a restart too.
    rx.always(Reply::Status(500));
    h.put("bkt", "muted", b"x").await;
    wait_view(&h, &id, 5, "muted", |v| v.status == "muted").await;
    let attempts = rx.attempts().len();
    h.restart().await;
    assert_eq!(h.view(&id).await.status, "muted");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(rx.attempts().len(), attempts, "restart does not bypass the mute");
}

#[tokio::test]
async fn index_rebuild_carries_the_changelog() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let id = h.webhook("rebuild", "bkt", &rx.url, Filter::default(), false).await.subscription.id;
    h.put("bkt", "a", b"x").await;
    wait_until(10, "first", || rx.live_keys() == ["a"]).await;
    let log_id = h.store.index("bkt").await.unwrap().changelog_id().to_string();

    h.store.rebuild_index("bkt").await.unwrap();
    assert_eq!(h.store.index("bkt").await.unwrap().changelog_id(), log_id, "same log after rebuild");
    h.put("bkt", "b", b"x").await;
    wait_until(10, "continues after rebuild", || rx.live_keys() == ["a", "b"]).await;
    assert_contiguous(&rx.accepted());
    assert_eq!(h.view(&id).await.status, "active");
}

#[tokio::test]
async fn repair_changes_are_marked_as_such() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    h.webhook("repair", "bkt", &rx.url, Filter::default(), false).await;
    h.put("bkt", "k", b"x").await;
    let index = h.store.index("bkt").await.unwrap();
    let row = index.get("k").await.unwrap().unwrap();
    index
        .commit_delete_from("k", &row.blob_dir, now_ms(), ChangeOrigin::Repair)
        .await
        .unwrap();
    wait_until(10, "two events", || rx.live_keys().len() == 2).await;
    let events: Vec<_> = rx.accepted().iter().flat_map(|b| b.events.clone()).collect();
    assert_eq!(events[1].op, "delete");
    assert_eq!(events[1].origin.as_deref(), Some("repair"));
}

#[tokio::test]
async fn bucket_with_subscriptions_cannot_be_deleted() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let id = h.webhook("guard", "bkt", &rx.url, Filter::default(), false).await.subscription.id;
    let err = h.store.delete_bucket("bkt").await.unwrap_err();
    assert!(matches!(err, crate::storage::errors::StorageError::BucketInUse(_)), "{err:?}");
    assert!(err.to_string().contains("guard"));
    h.service.delete(&id).await.unwrap();
    h.store.delete_bucket("bkt").await.unwrap();
}

#[tokio::test]
async fn webhook_creation_is_validated() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let strict = NotifyService::start(
        h.store.clone(),
        db::NotifyDb::new(h.iam.admin_db()),
        WebhookConfig::default(),
        ChangelogConfig::default(),
        h.shutdown.clone(),
    )
    .await
    .unwrap();
    let base = CreateRequest {
        name: "x".into(),
        bucket: "bkt".into(),
        kind: SubscriptionKind::Webhook,
        url: "https://127.0.0.1/hook".into(),
        headers: BTreeMap::new(),
        filter: Filter::default(),
        initial_snapshot: false,
        secret: None,
        batch_max_events: None,
        batch_max_bytes: None,
        mute_secs: None,
    };
    let internal = strict.create(base.clone(), "t").await.unwrap_err();
    assert!(internal.to_string().contains("internal address"), "{internal}");
    let http = strict
        .create(CreateRequest { url: "http://example.com/".into(), ..base.clone() }, "t")
        .await
        .unwrap_err();
    assert!(http.to_string().contains("allow_http"), "{http}");
    let reserved = strict
        .create(
            CreateRequest {
                url: "https://example.com/".into(),
                headers: BTreeMap::from([("Host".to_string(), "evil".to_string())]),
                ..base.clone()
            },
            "t",
        )
        .await
        .unwrap_err();
    assert!(reserved.to_string().contains("reserved"), "{reserved}");
    let no_bucket = strict
        .create(CreateRequest { bucket: "nope".into(), url: "https://example.com/".into(), ..base }, "t")
        .await
        .unwrap_err();
    assert!(matches!(no_bucket, NotifyError::NotFound(_)));
}

#[tokio::test]
async fn header_update_keeps_masked_values() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let id = h.webhook("hdr", "bkt", &rx.url, Filter::default(), false).await.subscription.id;
    h.service
        .update(
            &id,
            UpdateRequest {
                headers: Some(BTreeMap::from([
                    ("Authorization".to_string(), Some(MASKED.to_string())),
                    ("X-Tenant".to_string(), Some("t1".to_string())),
                ])),
                ..UpdateRequest::default()
            },
        )
        .await
        .unwrap();
    h.put("bkt", "k", b"x").await;
    wait_until(10, "delivery", || rx.accepted().len() == 1).await;
    let headers = &rx.attempts()[0].headers;
    assert_eq!(headers["authorization"], "Bearer hdr", "masked value kept");
    assert_eq!(headers["x-tenant"], "t1");
}

// ── pull (replication token) ────────────────────────────────────────────────

async fn call(app: &Router, method: &str, uri: &str, token: Option<&str>, body: Option<serde_json::Value>) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let request = match body {
        Some(body) => request
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
        None => request.body(Body::empty()).unwrap(),
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
}

/// The S3 router with auth disabled, plus the token front door.
fn s3_app(h: &Harness) -> Router {
    crate::server::router_with_notify(
        h.store.clone(),
        crate::server::auth::AuthState {
            config: Arc::new(crate::server::config::AppConfig::default()),
            iam: None,
            usage: None,
        },
        Arc::new(crate::server::TrafficMetrics::default()),
        crate::server::registry::TaskRegistry::new(),
        Some(h.service.clone()),
    )
}

fn keys_of(page: &serde_json::Value, field: &str) -> Vec<String> {
    page[field]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["key"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn changelog_reads_are_stateless_and_reader_positioned() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    for key in ["a", "b", "c"] {
        h.put("bkt", key, b"x").await;
    }
    let app = s3_app(&h);

    let (status, page) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&limit=2", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(keys_of(&page, "events"), ["a", "b"]);
    assert_eq!((page["next"].as_u64(), page["more"].as_bool(), page["head"].as_u64()), (Some(2), Some(true), Some(3)));
    let log_id = page["log_id"].as_str().unwrap().to_string();
    // Nothing is stored server-side: the same request returns the same page,
    // and any position the reader chooses works — forward, or replay.
    let (_, again) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&limit=2", None, None).await;
    assert_eq!(again, page);
    let (_, rest) = call(&app, "GET", "/bkt?x-rs3-changelog&after=2", None, None).await;
    assert_eq!((keys_of(&rest, "events"), rest["next"].as_u64(), rest["more"].as_bool()), (vec!["c".to_string()], Some(3), Some(false)));
    let (_, replay) = call(&app, "GET", "/bkt?x-rs3-changelog&after=1", None, None).await;
    assert_eq!(keys_of(&replay, "events"), ["b", "c"]);
    // Caught up: empty, and next stays put.
    let (_, idle) = call(&app, "GET", "/bkt?x-rs3-changelog&after=3", None, None).await;
    assert_eq!((idle["events"].as_array().unwrap().len(), idle["next"].as_u64()), (0, Some(3)));

    // Filters move `next` past what they reject.
    h.put("bkt", "logs/1", b"x").await;
    h.put("bkt", "img/1", b"x").await;
    let (_, filtered) = call(&app, "GET", "/bkt?x-rs3-changelog&after=3&prefix=img/", None, None).await;
    assert_eq!((keys_of(&filtered, "events"), filtered["next"].as_u64()), (vec!["img/1".to_string()], Some(5)));
    let (_, none) = call(&app, "GET", "/bkt?x-rs3-changelog&after=3&prefix=zzz/", None, None).await;
    assert_eq!((none["events"].as_array().unwrap().len(), none["next"].as_u64()), (0, Some(5)), "skips filtered changes");
    let (status, _) = call(&app, "GET", "/bkt?x-rs3-changelog&ops=rename", None, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Positions that do not belong to this log are refused.
    let (status, body) = call(&app, "GET", "/bkt?x-rs3-changelog&after=999", None, None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (status, body) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&log_id=other", None, None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(body["error"].as_str().unwrap().contains("Re-snapshot"));
    let (status, _) = call(&app, "GET", &format!("/bkt?x-rs3-changelog&after=0&log_id={log_id}"), None, None).await;
    assert_eq!(status, StatusCode::OK);

    // Behind retention: 410 naming the oldest available position.
    h.service.trim_all(now_ms() + 1_000).await;
    let (status, body) = call(&app, "GET", "/bkt?x-rs3-changelog&after=1", None, None).await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(body["trimmed_through"], 5);
    assert!(body["error"].as_str().unwrap().contains("after=5"));
    let (status, _) = call(&app, "GET", "/bkt?x-rs3-changelog&after=5", None, None).await;
    assert_eq!(status, StatusCode::OK);

    // Read-only, and other verbs never fall through to bucket create/delete.
    let (status, _) = call(&app, "DELETE", "/bkt?x-rs3-changelog", None, None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert!(h.store.bucket_exists("bkt").await);
}

#[tokio::test]
async fn changelog_long_poll_wakes_on_commit() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let app = s3_app(&h);
    let writer = {
        let store = h.store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            store.put_object("bkt", "late", b"x", None, None, false).await.unwrap();
        })
    };
    let started = Instant::now();
    let (_, page) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&wait=10", None, None).await;
    writer.await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(5), "woke on the commit, not the timeout");
    assert_eq!(keys_of(&page, "events"), ["late"]);
    // An idle long poll returns empty after its wait.
    let started = Instant::now();
    let (_, idle) = call(&app, "GET", "/bkt?x-rs3-changelog&after=1&wait=1", None, None).await;
    assert!(started.elapsed() >= Duration::from_millis(900));
    assert!(idle["events"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn snapshot_listing_pages_with_the_head_to_continue_from() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    for key in ["d/1", "d/2", "d/3", "e/1"] {
        h.put("bkt", key, b"x").await;
    }
    let app = s3_app(&h);
    let (status, first) = call(&app, "GET", "/bkt?x-rs3-changelog-snapshot&prefix=d/&limit=2", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(keys_of(&first, "entries"), ["d/1", "d/2"]);
    assert_eq!(first["head"], 4);
    assert!(first["entries"].as_array().unwrap().iter().all(|e| e["op"] == "exists" && e["version"].as_str().is_some_and(|v| !v.is_empty())));
    let after = first["next_after"].as_str().unwrap().to_string();
    // A change during the walk: replaying live from the first page's head
    // picks it up.
    h.put("bkt", "d/0", b"x").await;
    let (_, second) = call(&app, "GET", &format!("/bkt?x-rs3-changelog-snapshot&prefix=d/&limit=2&after={after}"), None, None).await;
    assert_eq!(keys_of(&second, "entries"), ["d/3"]);
    assert!(second["next_after"].is_null(), "listing complete");
    let (_, live) = call(&app, "GET", "/bkt?x-rs3-changelog&after=4&prefix=d/", None, None).await;
    assert_eq!(keys_of(&live, "events"), ["d/0"]);
}

#[tokio::test]
async fn replication_token_reads_its_bucket_and_filter_only() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    h.store.create_bucket("other").await.unwrap();
    h.put("bkt", "in/a", b"x").await;
    h.put("bkt", "out/b", b"x").await;
    h.put("other", "in/c", b"x").await;
    let created = h
        .service
        .create(
            CreateRequest {
                name: "replica".into(),
                bucket: "bkt".into(),
                kind: SubscriptionKind::Pull,
                url: String::new(),
                headers: BTreeMap::new(),
                filter: prefix("in/"),
                initial_snapshot: true,
                secret: None,
                batch_max_events: None,
                batch_max_bytes: None,
                mute_secs: None,
            },
            "tester",
        )
        .await
        .unwrap();
    let token = created.token.expect("token shown once");
    let app = pull::router(h.service.clone());

    for bad in [None, Some("rs3cl_wrong"), Some("not-a-token")] {
        let (status, _) = call(&app, "GET", "/_rs3/changelog", bad, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    // The token's filter is the scope; a query cannot widen it.
    let (status, page) = call(&app, "GET", "/_rs3/changelog?after=0&prefix=", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["bucket"], "bkt");
    assert_eq!(keys_of(&page, "events"), ["in/a"]);
    let (_, snap) = call(&app, "GET", "/_rs3/changelog/snapshot", Some(&token), None).await;
    assert_eq!(keys_of(&snap, "entries"), ["in/a"]);
    // The console sees the last read; it does not steer the next one.
    let view = h.view(&created.subscription.id).await;
    assert_eq!(view.offset, 2);
    assert!(view.last_success_ms > 0);
    let (_, replay) = call(&app, "GET", "/_rs3/changelog?after=0", Some(&token), None).await;
    assert_eq!(keys_of(&replay, "events"), ["in/a"], "a reader may always re-read");

    // No server-side position: those actions do not apply to tokens.
    for action in [Action::SkipToHead, Action::Resnapshot, Action::SetOffset { offset: 0 }, Action::ResumeFromOldest] {
        assert!(matches!(h.service.act(&created.subscription.id, action).await, Err(NotifyError::BadRequest(_))));
    }
    // Pause disables the token; resume re-enables it.
    h.service.act(&created.subscription.id, Action::Pause).await.unwrap();
    let (status, body) = call(&app, "GET", "/_rs3/changelog", Some(&token), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    h.service.act(&created.subscription.id, Action::Resume).await.unwrap();
    let (status, _) = call(&app, "GET", "/_rs3/changelog", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    // Rotation invalidates the old token.
    let rotated = h.service.act(&created.subscription.id, Action::RotateToken).await.unwrap();
    let (status, _) = call(&app, "GET", "/_rs3/changelog", Some(&token), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&app, "GET", "/_rs3/changelog", Some(rotated.token.as_deref().unwrap()), None).await;
    assert_eq!(status, StatusCode::OK);
    // The ack endpoint is gone.
    let (status, _) = call(&app, "POST", "/_rs3/changelog/ack", Some(rotated.token.as_deref().unwrap()), Some(serde_json::json!({"ack": "x"}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn pull_paths_never_reach_s3_handlers() {
    // The pull router sits beside the S3 router: anything else falls
    // through to S3 (here: 404 for a missing route in the bare pull router).
    let h = Harness::new().await;
    let app = pull::router(h.service.clone());
    let (status, _) = call(&app, "GET", "/_rs3/changelog/other", Some("rs3cl_x"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let _ = h.tmp.path();
}

#[tokio::test]
async fn get_by_exact_version_for_replicators() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    h.webhook("ver", "bkt", &rx.url, Filter::default(), false).await;
    h.put("bkt", "k", b"v1").await;
    wait_until(10, "event", || rx.live_keys().len() == 1).await;
    let v1 = rx.accepted()[0].events[0].version.clone();

    let app = crate::server::router(h.store.clone(), Arc::new(crate::server::config::AppConfig::default()));
    let get = |version: Option<String>, method: &'static str| {
        let app = app.clone();
        async move {
            let mut request = Request::builder().method(method).uri("/bkt/k");
            if let Some(v) = version {
                request = request.header("x-rs3-if-version", v);
            }
            app.oneshot(request.body(Body::empty()).unwrap()).await.unwrap()
        }
    };
    let response = get(None, "GET").await;
    assert_eq!(response.headers()["x-rs3-version"], v1.as_str(), "GET names the version");
    assert_eq!(get(None, "HEAD").await.headers()["x-rs3-version"], v1.as_str());
    assert_eq!(get(Some(v1.clone()), "GET").await.status(), StatusCode::OK);

    // Overwritten with identical bytes: same ETag, different version.
    h.put("bkt", "k", b"v1").await;
    wait_until(10, "second event", || rx.live_keys().len() == 2).await;
    let v2 = rx.accepted().last().unwrap().events.last().unwrap().version.clone();
    assert_ne!(v1, v2);
    assert_eq!(get(Some(v1), "GET").await.status(), StatusCode::PRECONDITION_FAILED, "old version is gone");
    assert_eq!(get(Some(v2), "GET").await.status(), StatusCode::OK);
}

#[tokio::test]
async fn pull_prefix_never_shadows_object_keys() {
    // Path-style `/_rs3/changelog` is the pull API (no bucket can be named
    // `_rs3`). Virtual-hosted, the same path is an object key in the bucket
    // named by the Host header, and must reach S3 untouched.
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let mut config = crate::server::config::AppConfig::default();
    config.auth.public_hostname = Some("s3.example.com".to_string());
    let app = crate::server::router_with_notify(
        h.store.clone(),
        crate::server::auth::AuthState {
            config: Arc::new(config),
            iam: None,
            usage: None,
        },
        Arc::new(crate::server::TrafficMetrics::default()),
        crate::server::registry::TaskRegistry::new(),
        Some(h.service.clone()),
    );
    let send = |method: &str, host: &str, body: &'static str| {
        let request = Request::builder()
            .method(method)
            .uri("/_rs3/changelog")
            .header("host", host)
            .body(Body::from(body))
            .unwrap();
        app.clone().oneshot(request)
    };
    // Virtual-hosted: an ordinary object PUT and GET.
    let put = send("PUT", "bkt.s3.example.com", "an object, not the API").await.unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    assert!(h.store.index("bkt").await.unwrap().get("_rs3/changelog").await.unwrap().is_some());
    let get = send("GET", "bkt.s3.example.com", "").await.unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(to_bytes(get.into_body(), usize::MAX).await.unwrap(), "an object, not the API");
    // Path-style: the pull API (401 without a token).
    let api = send("GET", "s3.example.com", "").await.unwrap();
    assert_eq!(api.status(), StatusCode::UNAUTHORIZED);
}
