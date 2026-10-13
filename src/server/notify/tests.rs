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
        .commit_delete_from("k", &row.blob_dir, now_ms(), ChangeOrigin::Repair, None, None)
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

    let (status, page) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&limit=2&wait=0", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(keys_of(&page, "events"), ["a", "b"]);
    assert_eq!((page["next"].as_u64(), page["more"].as_bool(), page["head"].as_u64()), (Some(2), Some(true), Some(3)));
    let log_id = page["log_id"].as_str().unwrap().to_string();
    // Nothing is stored server-side: the same request returns the same page,
    // and any position the reader chooses works — forward, or replay.
    let (_, again) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&limit=2&wait=0", None, None).await;
    assert_eq!(again, page);
    let (_, rest) = call(&app, "GET", "/bkt?x-rs3-changelog&after=2&wait=0", None, None).await;
    assert_eq!((keys_of(&rest, "events"), rest["next"].as_u64(), rest["more"].as_bool()), (vec!["c".to_string()], Some(3), Some(false)));
    let (_, replay) = call(&app, "GET", "/bkt?x-rs3-changelog&after=1&wait=0", None, None).await;
    assert_eq!(keys_of(&replay, "events"), ["b", "c"]);
    // Caught up: empty, and next stays put.
    let (_, idle) = call(&app, "GET", "/bkt?x-rs3-changelog&after=3&wait=0", None, None).await;
    assert_eq!((idle["events"].as_array().unwrap().len(), idle["next"].as_u64()), (0, Some(3)));

    // Filters move `next` past what they reject.
    h.put("bkt", "logs/1", b"x").await;
    h.put("bkt", "img/1", b"x").await;
    let (_, filtered) = call(&app, "GET", "/bkt?x-rs3-changelog&after=3&prefix=img/&wait=0", None, None).await;
    assert_eq!((keys_of(&filtered, "events"), filtered["next"].as_u64()), (vec!["img/1".to_string()], Some(5)));
    let (_, none) = call(&app, "GET", "/bkt?x-rs3-changelog&after=3&prefix=zzz/&wait=0", None, None).await;
    assert_eq!((none["events"].as_array().unwrap().len(), none["next"].as_u64()), (0, Some(5)), "skips filtered changes");
    let (status, _) = call(&app, "GET", "/bkt?x-rs3-changelog&ops=rename&wait=0", None, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Positions that do not belong to this log are refused.
    let (status, body) = call(&app, "GET", "/bkt?x-rs3-changelog&after=999&wait=0", None, None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (status, body) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&log_id=other&wait=0", None, None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(body["error"].as_str().unwrap().contains("Re-snapshot"));
    let (status, _) = call(&app, "GET", &format!("/bkt?x-rs3-changelog&after=0&log_id={log_id}&wait=0"), None, None).await;
    assert_eq!(status, StatusCode::OK);

    // Behind retention: 410 naming the oldest available position.
    h.service.trim_all(now_ms() + 1_000).await;
    let (status, body) = call(&app, "GET", "/bkt?x-rs3-changelog&after=1&wait=0", None, None).await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(body["trimmed_through"], 5);
    assert!(body["error"].as_str().unwrap().contains("after=5"));
    let (status, _) = call(&app, "GET", "/bkt?x-rs3-changelog&after=5&wait=0", None, None).await;
    assert_eq!(status, StatusCode::OK);

    // Read-only, and other verbs never fall through to bucket create/delete.
    let (status, _) = call(&app, "DELETE", "/bkt?x-rs3-changelog&wait=0", None, None).await;
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
    let (_, live) = call(&app, "GET", "/bkt?x-rs3-changelog&after=4&prefix=d/&wait=0", None, None).await;
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
        let (status, _) = call(&app, "GET", "/_rs3/changelog?wait=0", bad, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    // The token's filter is the scope; a query cannot widen it.
    let (status, page) = call(&app, "GET", "/_rs3/changelog?after=0&prefix=&wait=0", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["bucket"], "bkt");
    assert_eq!(keys_of(&page, "events"), ["in/a"]);
    let (_, snap) = call(&app, "GET", "/_rs3/changelog/snapshot", Some(&token), None).await;
    assert_eq!(keys_of(&snap, "entries"), ["in/a"]);
    // The console sees the last read; it does not steer the next one.
    let view = h.view(&created.subscription.id).await;
    assert_eq!(view.offset, 2);
    assert!(view.last_success_ms > 0);
    let (_, replay) = call(&app, "GET", "/_rs3/changelog?after=0&wait=0", Some(&token), None).await;
    assert_eq!(keys_of(&replay, "events"), ["in/a"], "a reader may always re-read");

    // No server-side position: those actions do not apply to tokens.
    for action in [Action::SkipToHead, Action::Resnapshot, Action::SetOffset { offset: 0 }, Action::ResumeFromOldest] {
        assert!(matches!(h.service.act(&created.subscription.id, action).await, Err(NotifyError::BadRequest(_))));
    }
    // Pause disables the token; resume re-enables it.
    h.service.act(&created.subscription.id, Action::Pause).await.unwrap();
    let (status, body) = call(&app, "GET", "/_rs3/changelog?wait=0", Some(&token), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    h.service.act(&created.subscription.id, Action::Resume).await.unwrap();
    let (status, _) = call(&app, "GET", "/_rs3/changelog?wait=0", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    // Rotation invalidates the old token.
    let rotated = h.service.act(&created.subscription.id, Action::RotateToken).await.unwrap();
    let (status, _) = call(&app, "GET", "/_rs3/changelog?wait=0", Some(&token), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&app, "GET", "/_rs3/changelog?wait=0", Some(rotated.token.as_deref().unwrap()), None).await;
    assert_eq!(status, StatusCode::OK);
    // The ack endpoint is gone.
    let (status, _) = call(&app, "POST", "/_rs3/changelog/ack?wait=0", Some(rotated.token.as_deref().unwrap()), Some(serde_json::json!({"ack": "x"}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn pull_paths_never_reach_s3_handlers() {
    // The pull router sits beside the S3 router: anything else falls
    // through to S3 (here: 404 for a missing route in the bare pull router).
    let h = Harness::new().await;
    let app = pull::router(h.service.clone());
    let (status, _) = call(&app, "GET", "/_rs3/changelog/other?wait=0", Some("rs3cl_x"), None).await;
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
            .uri("/_rs3/changelog?wait=0")
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

// ── part layouts ────────────────────────────────────────────────────────────

async fn multipart_upload(store: &LocalObjectStore, bucket: &str, key: &str, parts: &[Vec<u8>]) -> String {
    let upload_id = store.initiate_multipart(bucket, key, None, None).await.unwrap();
    let mut done = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        let number = (i + 1) as u16;
        let put = store
            .put_multipart_part(bucket, key, &upload_id, number, part, false)
            .await
            .unwrap();
        done.push(crate::storage::store::CompletePartRequest { number, etag: put.etag });
    }
    store.complete_multipart(bucket, key, &upload_id, &done).await.unwrap().etag
}

/// What a replicator does with an event: split the bytes by the event's
/// layout and upload them the same way. Returns the destination ETag.
async fn replicate_by_layout(
    store: &LocalObjectStore,
    bucket: &str,
    key: &str,
    bytes: &[u8],
    layout: &crate::storage::changelog::PartLayout,
) -> String {
    if !layout.multipart {
        return store.put_object(bucket, key, bytes, None, None, false).await.unwrap().etag;
    }
    let mut chunks = Vec::new();
    let mut offset = 0usize;
    for size in layout.part_sizes(bytes.len() as u64) {
        chunks.push(bytes[offset..offset + size as usize].to_vec());
        offset += size as usize;
    }
    assert_eq!(offset, bytes.len(), "layout covers the whole object");
    multipart_upload(store, bucket, key, &chunks).await
}

#[tokio::test]
async fn part_layouts_let_a_replica_reproduce_every_etag() {
    const MIB: usize = 5 * 1024 * 1024;
    let h = Harness::new().await;
    h.store.create_bucket("src-bkt").await.unwrap();
    h.store.create_bucket("dst-bkt").await.unwrap();
    let objects: Vec<(&str, Vec<Vec<u8>>, bool)> = vec![
        ("uniform", vec![vec![1u8; MIB], vec![2u8; MIB], vec![3u8; 3]], true),
        ("irregular", vec![vec![4u8; MIB + 1024], vec![5u8; MIB], vec![6u8; 10]], true),
        ("one-part", vec![vec![7u8; 10]], true),
        ("single", vec![b"single put body".to_vec()], false),
    ];
    let mut source_etags = std::collections::HashMap::new();
    for (key, parts, multipart) in &objects {
        let etag = if *multipart {
            multipart_upload(&h.store, "src-bkt", key, parts).await
        } else {
            h.put("src-bkt", key, &parts[0]).await;
            h.store.index("src-bkt").await.unwrap().get(key).await.unwrap().unwrap().etag
        };
        source_etags.insert(key.to_string(), etag);
    }

    let page = read::read(&h.store, "src-bkt", &read::ReadParams { limit: 100, ..Default::default() })
        .await
        .unwrap();
    assert_eq!(page.events.len(), objects.len());
    for event in &page.events {
        let layout = event.parts.as_ref().expect("new objects carry a layout");
        let (_, parts, multipart) = objects.iter().find(|(k, _, _)| *k == event.key).unwrap();
        assert_eq!(layout.multipart, *multipart, "{}", event.key);
        assert_eq!(layout.count as usize, parts.len());
        let bytes: Vec<u8> = parts.concat();
        let replica = replicate_by_layout(&h.store, "dst-bkt", &event.key, &bytes, layout).await;
        assert_eq!(replica, event.etag, "{}: same layout + same bytes = same ETag", event.key);
        assert_eq!(replica, source_etags[&event.key]);
    }
    let uniform = page.events.iter().find(|e| e.key == "uniform").unwrap().parts.clone().unwrap();
    assert_eq!((uniform.size, uniform.last, uniform.sizes.clone()), (Some(MIB as u64), Some(3), None));
    let irregular = page.events.iter().find(|e| e.key == "irregular").unwrap().parts.clone().unwrap();
    assert_eq!(irregular.sizes, Some(vec![(MIB + 1024) as u64, MIB as u64, 10]));
    let one = page.events.iter().find(|e| e.key == "one-part").unwrap();
    assert!(one.etag.ends_with("-1"), "one-part multipart ETag: {}", one.etag);

    // Snapshot entries carry it too, and an index rebuild restores it from
    // meta.json.
    h.store.rebuild_index("src-bkt").await.unwrap();
    let snap = read::snapshot(&h.store, "src-bkt", &read::SnapshotParams { limit: 100, ..Default::default() })
        .await
        .unwrap();
    for entry in &snap.entries {
        let live = page.events.iter().find(|e| e.key == entry.key).unwrap();
        assert_eq!(entry.parts, live.parts, "{} after rebuild", entry.key);
    }
    // Deletes carry the removed version's layout.
    h.store.delete_object("src-bkt", "uniform").await.unwrap();
    let after = read::read(&h.store, "src-bkt", &read::ReadParams { after: page.next, limit: 10, ..Default::default() })
        .await
        .unwrap();
    assert_eq!(after.events[0].op, "delete");
    assert_eq!(after.events[0].parts.as_ref(), Some(&uniform));
}

#[tokio::test]
async fn rows_without_a_layout_omit_it() {
    // Rows indexed before layouts were recorded: the field is absent (the
    // replicator asks GetObjectAttributes), never a wrong guess.
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    h.put("bkt", "k", b"x").await;
    let index = h.store.index("bkt").await.unwrap();
    let row = index.get("k").await.unwrap().unwrap();
    let intent = index.insert_publish_intent("k", &row.blob_dir, now_ms()).await.unwrap();
    index
        .commit_publish(&crate::storage::index::ObjectRecord { parts: None, ..row }, intent, None, now_ms())
        .await
        .unwrap();
    let app = s3_app(&h);
    let (_, page) = call(&app, "GET", "/bkt?x-rs3-changelog&after=1&wait=0", None, None).await;
    assert!(page["events"][0].get("parts").is_none(), "{page}");
    let (_, snap) = call(&app, "GET", "/bkt?x-rs3-changelog-snapshot", None, None).await;
    assert!(snap["entries"][0].get("parts").is_none());
    let (_, first) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&limit=1&wait=0", None, None).await;
    assert_eq!(first["events"][0]["parts"], serde_json::json!({"count": 1}));
}

// ── version stamps ──────────────────────────────────────────────────────────

use crate::storage::stamp::Stamp;

fn stamp_of(value: &serde_json::Value) -> Stamp {
    serde_json::from_value(value.clone()).expect("a stamp")
}

async fn send_put(app: &Router, uri: &str, body: &'static str, headers: &[(&str, &str)]) -> axum::response::Response {
    let mut request = Request::builder().method("PUT").uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    app.clone().oneshot(request.body(Body::from(body)).unwrap()).await.unwrap()
}

async fn head_stamp(app: &Router, uri: &str) -> (Stamp, String) {
    let response = app
        .clone()
        .oneshot(Request::builder().method("HEAD").uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let stamp = Stamp::parse_header(response.headers()["x-rs3-stamp"].to_str().unwrap()).unwrap();
    let last_modified = response.headers()["last-modified"].to_str().unwrap().to_string();
    (stamp, last_modified)
}

#[tokio::test]
async fn every_state_gets_an_increasing_stamp_from_this_site() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let app = s3_app(&h);
    let site = h.store.site_id();
    assert_eq!(site.len(), 16, "generated on first start");

    h.put("bkt", "k", b"one").await;
    h.put("bkt", "k", b"two").await;
    h.store.delete_object("bkt", "k").await.unwrap();
    h.put("bkt", "k", b"three").await;
    let (_, page) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&wait=0", None, None).await;
    assert_eq!(page["site_id"], site.as_str());
    let stamps: Vec<Stamp> = page["events"].as_array().unwrap().iter().map(|e| stamp_of(&e["stamp"])).collect();
    assert_eq!(stamps.len(), 4);
    assert!(stamps.windows(2).all(|w| w[0] < w[1]), "puts and deletes stamped in order: {stamps:?}");
    assert!(stamps.iter().all(|s| s.site == site));

    // GET/HEAD report the current state's stamp; Last-Modified matches it.
    let (stamp, _) = head_stamp(&app, "/bkt/k").await;
    assert_eq!(&stamp, stamps.last().unwrap());
    // It lives in meta.json, so an index rebuild keeps it.
    h.store.rebuild_index("bkt").await.unwrap();
    let (_, snap) = call(&app, "GET", "/bkt?x-rs3-changelog-snapshot", None, None).await;
    assert_eq!(stamp_of(&snap["entries"][0]["stamp"]), stamp);
    assert_eq!(snap["site_id"], site.as_str());
}

#[tokio::test]
async fn a_replicated_write_keeps_its_origin_stamp() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let app = s3_app(&h);
    // Written on site "site-a" a while ago.
    let origin = Stamp { ms: 1_700_000_000_000, c: 3, site: "site-a".into() };
    let response = send_put(&app, "/bkt/k", "replicated", &[("x-rs3-stamp", &origin.to_header())]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let (stamp, last_modified) = head_stamp(&app, "/bkt/k").await;
    assert_eq!(stamp, origin, "kept, not re-stamped");
    assert_eq!(last_modified, crate::storage::time::http_date_ms(origin.ms), "shows the origin's time");
    let (_, page) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&wait=0", None, None).await;
    assert_eq!(stamp_of(&page["events"][0]["stamp"]), origin);

    // A site whose clock runs ahead: the next local write still outranks it.
    let ahead = Stamp { ms: now_ms() + 120_000, c: 0, site: "site-b".into() };
    send_put(&app, "/bkt/k", "from b", &[("x-rs3-stamp", &ahead.to_header())]).await;
    h.put("bkt", "k", b"local").await;
    let (local, _) = head_stamp(&app, "/bkt/k").await;
    assert!(local > ahead, "{local:?} must outrank {ahead:?}");
    assert_eq!(local.site, h.store.site_id());

    // Validation.
    for bad in ["nonsense", "1.2", "1.x.site", "1.0.bad.site"] {
        assert_eq!(send_put(&app, "/bkt/x", "b", &[("x-rs3-stamp", bad)]).await.status(), StatusCode::BAD_REQUEST, "{bad}");
    }
    let far = Stamp { ms: now_ms() + 3_600_000, c: 0, site: "site-c".into() };
    let response = send_put(&app, "/bkt/x", "b", &[("x-rs3-stamp", &far.to_header())]).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST, "a peer clock an hour ahead is refused");
}

#[tokio::test]
async fn a_replicated_multipart_upload_is_stamped_at_completion() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let upload_id = h.store.initiate_multipart("bkt", "big", None, None).await.unwrap();
    let part = h.store.put_multipart_part("bkt", "big", &upload_id, 1, b"tail", false).await.unwrap();
    let origin = Stamp { ms: 1_700_000_000_000, c: 0, site: "site-a".into() };
    let result = h
        .store
        .complete_multipart_with(
            "bkt",
            "big",
            &upload_id,
            &[crate::storage::store::CompletePartRequest { number: 1, etag: part.etag }],
            crate::storage::store::PublishOptions { precondition: None, stamp: Some(origin.clone()), ..Default::default() },
        )
        .await
        .unwrap();
    assert_eq!(result.stamp, Some(origin.clone()));
    assert_eq!(result.last_modified_ms, origin.ms);
    let app = s3_app(&h);
    assert_eq!(head_stamp(&app, "/bkt/big").await.0, origin);
}

#[tokio::test]
async fn objects_from_before_stamps_report_a_fallback_stamp() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    h.put("bkt", "old", b"x").await;
    // Strip the stamp from both the row and meta.json, as an object written
    // by an older server would be.
    let index = h.store.index("bkt").await.unwrap();
    let row = index.get("old").await.unwrap().unwrap();
    let meta_path = h.tmp.path().join("buckets").join("bkt").join(&row.blob_dir).join("meta.json");
    let mut meta: serde_json::Value = serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    meta.as_object_mut().unwrap().remove("stamp");
    std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    let intent = index.insert_publish_intent("old", &row.blob_dir, now_ms()).await.unwrap();
    index
        .commit_publish(&crate::storage::index::ObjectRecord { stamp: None, ..row.clone() }, intent, None, now_ms())
        .await
        .unwrap();
    h.store.forget_cached_meta("bkt", "old");

    let expected = Stamp::fallback(row.last_modified_ms, &h.store.site_id());
    let app = s3_app(&h);
    assert_eq!(head_stamp(&app, "/bkt/old").await.0, expected);
    let (_, snap) = call(&app, "GET", "/bkt?x-rs3-changelog-snapshot", None, None).await;
    assert_eq!(stamp_of(&snap["entries"][0]["stamp"]), expected);
    // Its next write is stamped normally and outranks the fallback.
    h.put("bkt", "old", b"y").await;
    assert!(head_stamp(&app, "/bkt/old").await.0 > expected);
}

#[tokio::test]
async fn the_site_id_and_clock_survive_a_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path());
    store.init_site(None).unwrap();
    store.create_bucket("bkt").await.unwrap();
    store.put_object("bkt", "k", b"x", None, None, false).await.unwrap();
    let site = store.site_id();
    let before = store.index("bkt").await.unwrap().get("k").await.unwrap().unwrap().stamp.unwrap();
    drop(store);
    let reopened = LocalObjectStore::new(tmp.path());
    reopened.init_site(None).unwrap();
    assert_eq!(reopened.site_id(), site);
    assert!(reopened.next_stamp() > before);
    // A configured id overrides the generated one.
    let named = LocalObjectStore::new(tmp.path());
    named.init_site(Some("sg-1")).unwrap();
    assert_eq!(named.site_id(), "sg-1");
}

#[tokio::test]
async fn reading_from_a_point_in_time() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let app = s3_app(&h);
    for key in ["a", "b"] {
        h.put("bkt", key, b"x").await;
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    let t = now_ms();
    tokio::time::sleep(Duration::from_millis(5)).await;
    for key in ["c", "d", "e"] {
        h.put("bkt", key, b"x").await;
    }
    let (status, page) = call(&app, "GET", &format!("/bkt?x-rs3-changelog&since={t}&wait=0"), None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(keys_of(&page, "events"), ["c", "d", "e"]);
    let (_, all) = call(&app, "GET", "/bkt?x-rs3-changelog&since=0&wait=0", None, None).await;
    assert_eq!(keys_of(&all, "events"), ["a", "b", "c", "d", "e"]);
    let (_, none) = call(&app, "GET", &format!("/bkt?x-rs3-changelog&since={}&wait=0", now_ms() + 60_000), None, None).await;
    assert_eq!((none["events"].as_array().unwrap().len(), none["next"].as_u64()), (0, Some(5)));
    // Commit times never decrease in seq order.
    let ts: Vec<i64> = all["events"].as_array().unwrap().iter().map(|e| e["ts_ms"].as_i64().unwrap()).collect();
    assert!(ts.windows(2).all(|w| w[0] <= w[1]));
    let (status, _) = call(&app, "GET", "/bkt?x-rs3-changelog&since=0&after=1&wait=0", None, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Once retention removed changes from that time, the answer is 410.
    h.service.trim_all(t).await;
    let (status, _) = call(&app, "GET", "/bkt?x-rs3-changelog&since=0&wait=0", None, None).await;
    assert_eq!(status, StatusCode::GONE);
    let (status, page) = call(&app, "GET", &format!("/bkt?x-rs3-changelog&since={t}&wait=0"), None, None).await;
    assert_eq!(status, StatusCode::OK, "everything from t is still retained");
    assert_eq!(keys_of(&page, "events"), ["c", "d", "e"]);
}

// ── metadata-only updates ───────────────────────────────────────────────────

async fn object_bytes(store: &LocalObjectStore, bucket: &str, key: &str) -> Vec<u8> {
    let object = store.read_object(bucket, key).await.unwrap();
    let mut out = Vec::new();
    for part in &object.meta.parts {
        out.extend(std::fs::read(object.object_dir.join(&part.file)).unwrap());
    }
    out
}

#[tokio::test]
async fn copying_onto_itself_updates_metadata_without_touching_the_bytes() {
    const MIB: usize = 5 * 1024 * 1024;
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let parts = vec![vec![1u8; MIB], vec![2u8; MIB], vec![3u8; 7]];
    let etag = multipart_upload(&h.store, "bkt", "big", &parts).await;
    assert!(etag.ends_with("-3"));
    let before = h.store.index("bkt").await.unwrap().get("big").await.unwrap().unwrap();

    let meta = BTreeMap::from([("owner".to_string(), "team-a".to_string())]);
    let result = h
        .store
        .copy_object_with_metadata(
            "bkt",
            "big",
            "bkt",
            "big",
            None,
            Some(&meta),
            Some("application/x-test"),
            None,
            crate::storage::store::PublishOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.etag, etag, "the multipart ETag is kept");
    let after = h.store.index("bkt").await.unwrap().get("big").await.unwrap().unwrap();
    assert_eq!(after.parts, before.parts, "the part layout is kept");
    assert_ne!(after.blob_dir, before.blob_dir, "a new version");
    assert!(after.stamp > before.stamp, "with a new stamp");
    let object = h.store.read_object("bkt", "big").await.unwrap();
    assert_eq!(object.meta.user_meta, meta);
    assert_eq!(object.meta.content_type, "application/x-test");
    assert_eq!(object_bytes(&h.store, "bkt", "big").await, parts.concat());

    // The part files are hardlinks: purging the retired dir from trash leaves
    // the object intact.
    let trash = h.tmp.path().join("buckets").join("bkt").join("trash");
    assert!(trash.exists(), "the old version was retired to trash");
    std::fs::remove_dir_all(&trash).unwrap();
    assert_eq!(object_bytes(&h.store, "bkt", "big").await, parts.concat());

    // It is a normal change: logged as a put with the same ETag.
    let page = read::read(&h.store, "bkt", &read::ReadParams { limit: 10, ..Default::default() }).await.unwrap();
    let last = page.events.last().unwrap();
    assert_eq!((last.op.as_str(), last.etag.as_str()), ("put", etag.as_str()));
    assert_eq!(last.parts, before.parts);

    // Copying to a different key is still a byte copy (one part, plain MD5).
    let copy = h.store.copy_object("bkt", "big", "bkt", "other").await.unwrap();
    assert!(!copy.etag.contains('-'), "{}", copy.etag);
}

#[tokio::test]
async fn self_copy_over_http_and_stamp_only_updates() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let upload_id = h.store.initiate_multipart("bkt", "k", None, None).await.unwrap();
    let part = h.store.put_multipart_part("bkt", "k", &upload_id, 1, b"only part", false).await.unwrap();
    let etag = h
        .store
        .complete_multipart("bkt", "k", &upload_id, &[crate::storage::store::CompletePartRequest { number: 1, etag: part.etag }])
        .await
        .unwrap()
        .etag;
    let app = s3_app(&h);

    // An S3 client's metadata update.
    let response = send_put(
        &app,
        "/bkt/k",
        "",
        &[
            ("x-amz-copy-source", "/bkt/k"),
            ("x-amz-metadata-directive", "REPLACE"),
            ("x-amz-meta-color", "blue"),
        ],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains(&etag), "CopyObjectResult keeps the -1 ETag");

    // A replicator adopting the origin's stamp for identical content and
    // metadata: no bytes move, the stamp changes.
    let origin = Stamp { ms: now_ms() + 1_000, c: 0, site: "site-a".into() };
    let response = send_put(
        &app,
        "/bkt/k",
        "",
        &[
            ("x-amz-copy-source", "/bkt/k"),
            ("x-amz-metadata-directive", "REPLACE"),
            ("x-amz-meta-color", "blue"),
            ("x-rs3-stamp", &origin.to_header()),
        ],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(head_stamp(&app, "/bkt/k").await.0, origin);
    let row = h.store.index("bkt").await.unwrap().get("k").await.unwrap().unwrap();
    assert_eq!(row.etag, etag);
    assert_eq!(object_bytes(&h.store, "bkt", "k").await, b"only part");
}

// ── tombstones and conditional (last-writer-wins) apply ─────────────────────

use crate::storage::store::PublishOptions;

async fn put_stamped(store: &LocalObjectStore, bucket: &str, key: &str, body: &[u8], stamp: Option<Stamp>) -> crate::storage::errors::Result<crate::storage::store::PutResult> {
    let stream = futures::stream::once(futures::future::ready(Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(body))));
    store
        .put_object_stream_with_metadata(
            bucket,
            key,
            stream,
            None,
            None,
            None,
            None,
            &BTreeMap::new(),
            false,
            None,
            None,
            PublishOptions { precondition: None, stamp, ..Default::default() },
        )
        .await
}

fn st(ms: i64, site: &str) -> Stamp {
    Stamp { ms, c: 0, site: site.into() }
}

#[tokio::test]
async fn replicated_changes_apply_only_over_older_states() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let base = now_ms();
    put_stamped(&h.store, "bkt", "k", b"v5", Some(st(base + 5, "a"))).await.unwrap();

    // Older or equal: refused, nothing changes, nothing is logged.
    let head_before = h.store.index("bkt").await.unwrap().changelog_head();
    for stale in [st(base + 4, "z"), st(base + 5, "a")] {
        let err = put_stamped(&h.store, "bkt", "k", b"old", Some(stale)).await.unwrap_err();
        assert!(matches!(err, crate::storage::errors::StorageError::StaleStamp { .. }), "{err:?}");
    }
    assert_eq!(object_bytes(&h.store, "bkt", "k").await, b"v5");
    assert_eq!(h.store.index("bkt").await.unwrap().changelog_head(), head_before, "no event for a refused change");
    // The same (ms, c) from a higher site id wins the tie.
    put_stamped(&h.store, "bkt", "k", b"tie", Some(st(base + 5, "b"))).await.unwrap();
    assert_eq!(object_bytes(&h.store, "bkt", "k").await, b"tie");

    // Over HTTP: 412 naming the current stamp.
    let app = s3_app(&h);
    let response = send_put(&app, "/bkt/k", "older", &[("x-rs3-stamp", &st(base, "a").to_header())]).await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(response.headers()["x-rs3-stamp-current"], st(base + 5, "b").to_header().as_str());

    // Local writes are never conditional, and outrank everything seen.
    h.put("bkt", "k", b"local").await;
    let local = h.store.key_stamp("bkt", "k").await.unwrap().unwrap().0;
    assert!(local > st(base + 5, "b"));
}

#[tokio::test]
async fn deletes_are_version_scoped() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let app = s3_app(&h);
    let base = now_ms();
    let v1 = st(base + 1, "a");
    put_stamped(&h.store, "bkt", "k", b"v1", Some(v1.clone())).await.unwrap();
    // Site a deletes v1 (the version it sees).
    h.store.delete_object_stamped("bkt", "k", Some(st(base + 3, "a")), Some(v1.clone())).await.unwrap();
    let head = app
        .clone()
        .oneshot(Request::builder().method("HEAD").uri("/bkt/k").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::NOT_FOUND);
    assert_eq!(head.headers()["x-rs3-stamp"], st(base + 3, "a").to_header().as_str());
    assert_eq!(head.headers()["x-rs3-deleted"], "true");

    // Meanwhile site b had written v2 — before the delete in time, but the
    // deleter never saw it. It arrives late and survives: the delete only
    // removed v1.
    put_stamped(&h.store, "bkt", "k", b"v2", Some(st(base + 2, "b"))).await.unwrap();
    assert_eq!(object_bytes(&h.store, "bkt", "k").await, b"v2");
    // Replaying the delete of v1 does not touch v2.
    assert!(h.store.delete_object_stamped("bkt", "k", Some(st(base + 3, "a")), Some(v1.clone())).await.is_err());
    assert_eq!(object_bytes(&h.store, "bkt", "k").await, b"v2");
    // A delete that saw v2 removes it; v1 (or v2) replayed later stays dead.
    h.store.delete_object_stamped("bkt", "k", Some(st(base + 4, "b")), Some(st(base + 2, "b"))).await.unwrap();
    assert!(h.store.read_object("bkt", "k").await.is_err());
    for old in [v1.clone(), st(base + 2, "b")] {
        let err = put_stamped(&h.store, "bkt", "k", b"old", Some(old)).await.unwrap_err();
        assert!(matches!(err, crate::storage::errors::StorageError::StaleStamp { .. }), "{err:?}");
    }
    // Anything newer than the deleted version applies and clears the tombstone.
    put_stamped(&h.store, "bkt", "k", b"v5", Some(st(base + 5, "a"))).await.unwrap();
    assert_eq!(object_bytes(&h.store, "bkt", "k").await, b"v5");
    assert!(h.store.index("bkt").await.unwrap().get_tombstone("k").await.unwrap().is_none());

    // A replicated delete for a key never seen here still records the
    // tombstone: the version it removed (or older) cannot arrive afterwards,
    // a newer one can.
    h.store.delete_object_stamped("bkt", "ghost", Some(st(base + 10, "a")), Some(st(base + 8, "a"))).await.unwrap();
    assert!(put_stamped(&h.store, "bkt", "ghost", b"removed one", Some(st(base + 8, "a"))).await.is_err());
    put_stamped(&h.store, "bkt", "ghost", b"newer", Some(st(base + 9, "b"))).await.unwrap();
    h.store.delete_object_stamped("bkt", "ghost", Some(st(base + 11, "b")), Some(st(base + 9, "b"))).await.unwrap();
    // Local deletes remove exactly the version they see.
    h.put("bkt", "local", b"x").await;
    let seen = h.store.index("bkt").await.unwrap().get("local").await.unwrap().unwrap().stamp.unwrap();
    h.store.delete_object("bkt", "local").await.unwrap();
    let tomb = h.store.index("bkt").await.unwrap().get_tombstone("local").await.unwrap().unwrap();
    assert_eq!(tomb.removed, Some(seen));
    // ...and say so in the changelog.
    let (_, log) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&wait=0", None, None).await;
    let last = log["events"].as_array().unwrap().last().unwrap().clone();
    assert_eq!(last["op"], "delete");
    assert_eq!(stamp_of(&last["removed_stamp"]), tomb.removed.clone().unwrap());

    // Snapshot listing of tombstones.
    let (_, page) = call(&app, "GET", "/bkt?x-rs3-changelog-snapshot&deleted=1", None, None).await;
    let keys = keys_of(&page, "entries");
    assert_eq!(keys, ["ghost", "local"]);
    assert!(page["entries"].as_array().unwrap().iter().all(|e| e["op"] == "deleted" && e["stamp"].is_object()));

    // Tombstones survive a rebuild and expire after their retention.
    h.store.rebuild_index("bkt").await.unwrap();
    assert!(put_stamped(&h.store, "bkt", "ghost", b"late", Some(st(base + 9, "b"))).await.is_err());
    assert_eq!(h.service.purge_tombstones(now_ms() + 1_000).await, 2);
    put_stamped(&h.store, "bkt", "ghost", b"late", Some(st(base + 9, "b"))).await.unwrap();
}

#[tokio::test]
async fn a_stale_replicated_multipart_upload_is_refused_before_assembly() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    h.put("bkt", "k", b"newer").await;
    let upload_id = h.store.initiate_multipart("bkt", "k", None, None).await.unwrap();
    let part = h.store.put_multipart_part("bkt", "k", &upload_id, 1, b"older", false).await.unwrap();
    let err = h
        .store
        .complete_multipart_with(
            "bkt",
            "k",
            &upload_id,
            &[crate::storage::store::CompletePartRequest { number: 1, etag: part.etag }],
            PublishOptions { precondition: None, stamp: Some(st(1, "a")), ..Default::default() },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, crate::storage::errors::StorageError::StaleStamp { .. }));
    assert_eq!(object_bytes(&h.store, "bkt", "k").await, b"newer");
    h.store.abort_multipart("bkt", "k", &upload_id).await.unwrap();
}

/// Two sites receive the same set of changes in different orders and must
/// end in the same state.
#[tokio::test]
async fn two_sites_converge_whatever_the_delivery_order() {
    #[derive(Clone)]
    enum Change {
        Put(&'static str, &'static [u8], Stamp),
        /// (key, the delete's stamp, the stamp of the version it removed)
        Delete(&'static str, Stamp, Stamp),
    }
    async fn apply(store: &LocalObjectStore, change: &Change) {
        // Changes that do not outrank the current state are refused; that
        // is the point.
        let _ = match change {
            Change::Put(key, body, stamp) => put_stamped(store, "bkt", key, body, Some(stamp.clone())).await.map(|_| ()),
            Change::Delete(key, stamp, removed) => store
                .delete_object_stamped("bkt", key, Some(stamp.clone()), Some(removed.clone()))
                .await
                .map(|_| ()),
        };
    }
    async fn state(store: &LocalObjectStore) -> Vec<(String, Option<Vec<u8>>, Stamp)> {
        let mut out = Vec::new();
        for key in ["k1", "k2", "k3", "k4"] {
            let Some((stamp, deleted)) = store.key_stamp("bkt", key).await.unwrap() else { continue };
            let body = if deleted { None } else { Some(object_bytes(store, "bkt", key).await) };
            out.push((key.to_string(), body, stamp));
        }
        out
    }
    let base = now_ms();
    let changes = vec![
        // k1: a deletes its a1 while b concurrently wrote b1 — b1 survives.
        Change::Put("k1", b"a1", st(base + 1, "site-a")),
        Change::Put("k1", b"b1", st(base + 2, "site-b")),
        Change::Delete("k1", st(base + 3, "site-a"), st(base + 1, "site-a")),
        // k2: same millisecond on both sites: the site id decides.
        Change::Put("k2", b"a2", st(base + 5, "site-a")),
        Change::Put("k2", b"b2", st(base + 5, "site-b")),
        // k3: a delete of an older version loses to the newer write.
        Change::Put("k3", b"a3", st(base + 7, "site-a")),
        Change::Delete("k3", st(base + 8, "site-b"), st(base + 6, "site-b")),
        // k4: a delete of the latest version wins.
        Change::Put("k4", b"a4", st(base + 1, "site-a")),
        Change::Delete("k4", st(base + 4, "site-b"), st(base + 1, "site-a")),
    ];
    let mut orders: Vec<Vec<Change>> = vec![changes.clone(), changes.iter().rev().cloned().collect()];
    // A few interleavings.
    for rot in 1..changes.len() {
        let mut order = changes.clone();
        order.rotate_left(rot);
        orders.push(order);
    }
    let mut results = Vec::new();
    for order in orders {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("bkt").await.unwrap();
        for change in &order {
            apply(&store, change).await;
        }
        // Re-delivering everything (replays, echoes) changes nothing.
        for change in &order {
            apply(&store, change).await;
        }
        results.push(state(&store).await);
    }
    let expected = vec![
        ("k1".to_string(), Some(b"b1".to_vec()), st(base + 2, "site-b")),
        ("k2".to_string(), Some(b"b2".to_vec()), st(base + 5, "site-b")),
        ("k3".to_string(), Some(b"a3".to_vec()), st(base + 7, "site-a")),
        ("k4".to_string(), None, st(base + 4, "site-b")),
    ];
    for result in results {
        assert_eq!(result, expected);
    }
}

#[tokio::test]
async fn reads_wait_for_min_changes_or_the_max_wait() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let app = s3_app(&h);
    // Three changes arrive 150ms apart; a reader wants at least 3.
    let writer = {
        let store = h.store.clone();
        tokio::spawn(async move {
            for key in ["a", "b", "c"] {
                tokio::time::sleep(Duration::from_millis(150)).await;
                store.put_object("bkt", key, b"x", None, None, false).await.unwrap();
            }
        })
    };
    let started = Instant::now();
    let (_, page) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&min=3&wait_ms=5000", None, None).await;
    writer.await.unwrap();
    assert_eq!(keys_of(&page, "events"), ["a", "b", "c"], "held until min was reached");
    assert!(started.elapsed() < Duration::from_secs(3), "returned on min, not the wait");

    // Not enough within the wait: whatever is there comes back after it.
    let started = Instant::now();
    let (_, page) = call(&app, "GET", "/bkt?x-rs3-changelog&after=1&min=10&wait_ms=400", None, None).await;
    assert_eq!(keys_of(&page, "events"), ["b", "c"]);
    assert!(started.elapsed() >= Duration::from_millis(380), "{:?}", started.elapsed());
    // A full page returns at once even below min (min is capped at limit).
    let started = Instant::now();
    let (_, page) = call(&app, "GET", "/bkt?x-rs3-changelog&after=0&limit=2&min=50&wait_ms=5000", None, None).await;
    assert_eq!(keys_of(&page, "events"), ["a", "b"]);
    assert!(started.elapsed() < Duration::from_secs(2));
    // Defaults: min=1 and a 20s wait — a reader with changes waiting gets
    // them at once; wait=0 answers a caught-up reader at once.
    let started = Instant::now();
    let (_, page) = call(&app, "GET", "/bkt?x-rs3-changelog&after=2", None, None).await;
    assert_eq!(keys_of(&page, "events"), ["c"]);
    let (_, idle) = call(&app, "GET", "/bkt?x-rs3-changelog&after=3&wait=0", None, None).await;
    assert!(idle["events"].as_array().unwrap().is_empty());
    assert!(started.elapsed() < Duration::from_secs(2));
    let (status, _) = call(&app, "GET", "/bkt?x-rs3-changelog&wait=1&wait_ms=10", None, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let params = read::read_params(&std::collections::HashMap::from([("min".to_string(), "5".to_string())])).unwrap();
    assert_eq!((params.min, params.wait), (5, Duration::from_secs(20)));
}

#[test]
fn changelog_reads_are_labelled_in_the_task_monitor() {
    let label = |uri: &str| crate::server::operation_and_target(&axum::http::Method::GET, &uri.parse().unwrap(), false);
    assert_eq!(label("/photos?x-rs3-changelog&after=5&wait=20"), ("CHANGELOG", "/photos/ changelog".to_string()));
    assert_eq!(label("/photos?x-rs3-changelog-snapshot"), ("CHANGELOG", "/photos/ changelog snapshot".to_string()));
    assert_eq!(label("/photos?list-type=2").0, "LIST");
}

// ── the console API (what the browser calls) ───────────────────────────────

async fn console_app(h: &Harness) -> (Router, String) {
    let state = crate::server::ui::UiState {
        store: h.store.clone(),
        iam: h.iam.clone(),
        key_usage: crate::server::key_usage::KeyUsageStore::open(h.tmp.path()).await.unwrap(),
        config: Arc::new(crate::server::config::AppConfig::default()),
        metrics: Arc::new(crate::server::TrafficMetrics::default()),
        tasks: crate::server::registry::TaskRegistry::new(),
        scans: crate::server::jobs::perf_scan::ScanService::new(
            crate::server::scan_store::ScanStore::open(h.tmp.path()).await.unwrap(),
        ),
        stats: None,
        notify: Some(h.service.clone()),
        replication: None,
    };
    // A built-in (config) admin session, as after a console login.
    let session = h.iam.create_session("admin", true);
    (crate::server::ui::router(state), session)
}

async fn console(app: &Router, session: &str, method: &str, path: &str, body: Option<serde_json::Value>) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("cookie", format!("rusts3_ui_session={session}"));
    let body = match body {
        Some(value) => {
            request = request.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    let response = app.clone().oneshot(request.body(body).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
}

#[tokio::test]
async fn console_shows_an_expired_webhook_and_recovers_it_by_re_snapshot() {
    let h = Harness::new().await;
    h.store.create_bucket("bkt").await.unwrap();
    let rx = Receiver::start().await;
    let (app, session) = console_app(&h).await;

    // Created from the console, like the "New subscription" dialog does.
    let (status, created) = console(
        &app,
        &session,
        "POST",
        "/api/subscriptions",
        Some(serde_json::json!({
            "name": "console-hook", "bucket": "bkt", "kind": "webhook", "url": rx.url,
            "headers": {"Authorization": "Bearer x"}, "filter": {"prefix": ""}, "initial_snapshot": false
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert!(created["secret"].as_str().unwrap().starts_with("whsec_"), "shown once");
    let id = created["subscription"]["id"].as_str().unwrap().to_string();
    let path = format!("/api/subscriptions/{id}/action");

    // The receiver is down while changes pile up and expire.
    rx.always(Reply::Status(503));
    h.put("bkt", "lost", b"x").await;
    wait_view(&h, &id, 5, "muted", |v| v.status == "muted").await;
    h.service.trim_all(now_ms() + 1_000).await;
    rx.always(Reply::Ok);
    h.service.act(&id, Action::Unmute).await.unwrap();
    wait_view(&h, &id, 6, "paused as expired", |v| v.status == "paused").await;

    // The list the console renders shows why.
    let (_, list) = console(&app, &session, "GET", "/api/subscriptions", None).await;
    let row = &list["subscriptions"][0];
    assert_eq!(row["status"], "paused");
    assert_eq!(row["paused"]["reason"], "expired");
    assert!(row["paused"]["message"].as_str().unwrap().contains("deleted by retention"));
    assert_eq!(row["headers"]["Authorization"], MASKED, "secrets stay masked");

    // "Resume" is refused with the explanation the dialog displays.
    let (status, body) = console(&app, &session, "POST", &path, Some(serde_json::json!({"action": "resume"}))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let message = body["error"].as_str().unwrap();
    assert!(message.contains("Cannot resume webhook \"console-hook\""), "{message}");
    assert!(message.contains("re-snapshot"), "{message}");
    // "Set offset" outside the retained range is refused too.
    let (status, _) = console(&app, &session, "POST", &path, Some(serde_json::json!({"action": "set_offset", "offset": 0}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // "Re-snapshot" recovers: a snapshot of the current objects, then live.
    let (status, body) = console(&app, &session, "POST", &path, Some(serde_json::json!({"action": "resnapshot"}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["subscription"]["status"], "snapshot");
    wait_until(10, "snapshot delivered", || rx.accepted().iter().any(|b| b.snapshot_end)).await;
    let keys: Vec<String> = rx.accepted().iter().flat_map(|b| b.events.iter().map(|e| e.key.clone())).collect();
    assert_eq!(keys, ["lost"]);
    h.put("bkt", "after", b"x").await;
    wait_until(10, "live again", || rx.live_keys() == ["after"]).await;
    let (_, view) = console(&app, &session, "GET", &format!("/api/subscriptions/{id}"), None).await;
    assert_eq!(view["status"], "active");

    // Only admins: an IAM user's session is refused.
    h.iam.create_user("alice", "password123").await.unwrap();
    let alice = h.iam.create_session("alice", false);
    let (status, _) = console(&app, &alice, "GET", "/api/subscriptions", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

// ── read-only buckets ──────────────────────────────────────────────────────

#[tokio::test]
async fn a_read_only_bucket_accepts_only_replicated_writes() {
    use crate::storage::errors::StorageError;
    let h = Harness::new().await;
    h.store.create_bucket("replica").await.unwrap();
    h.put("replica", "existing", b"x").await;
    h.store.set_bucket_read_only("replica", true).await.unwrap();
    assert!(h.store.bucket_meta("replica").await.unwrap().read_only);
    let ro = |r: crate::storage::errors::Result<()>| matches!(r, Err(StorageError::BucketReadOnly(_)));

    // Every client write path is refused.
    assert!(ro(h.store.put_object("replica", "k", b"x", None, None, false).await.map(|_| ())));
    assert!(ro(put_stamped(&h.store, "replica", "k", b"x", None).await.map(|_| ())));
    assert!(ro(h.store.copy_object("replica", "existing", "replica", "copy").await.map(|_| ())));
    assert!(ro(h
        .store
        .copy_object_with_metadata("replica", "existing", "replica", "existing", None, Some(&BTreeMap::new()), Some("text/x"), None, Default::default())
        .await
        .map(|_| ())));
    assert!(ro(h.store.initiate_multipart("replica", "big", None, None).await.map(|_| ())));
    assert!(ro(h.store.delete_object("replica", "existing").await));
    assert_eq!(object_bytes(&h.store, "replica", "existing").await, b"x", "nothing changed");
    let head = h.store.index("replica").await.unwrap().changelog_head();

    // Replicated (stamped) writes go through: put, metadata update,
    // multipart, delete.
    let base = now_ms() + 1_000;
    put_stamped(&h.store, "replica", "k", b"replicated", Some(st(base, "peer"))).await.unwrap();
    h.store
        .copy_object_with_metadata(
            "replica",
            "k",
            "replica",
            "k",
            None,
            Some(&BTreeMap::from([("m".to_string(), "1".to_string())])),
            None,
            None,
            PublishOptions { stamp: Some(st(base + 1, "peer")), ..Default::default() },
        )
        .await
        .unwrap();
    let upload = h.store.initiate_multipart_replicated("replica", "big", None, None, None, None, &BTreeMap::new()).await.unwrap();
    let part = h.store.put_multipart_part("replica", "big", &upload, 1, b"part", false).await.unwrap();
    h.store
        .complete_multipart_with(
            "replica",
            "big",
            &upload,
            &[crate::storage::store::CompletePartRequest { number: 1, etag: part.etag }],
            PublishOptions { stamp: Some(st(base + 2, "peer")), ..Default::default() },
        )
        .await
        .unwrap();
    h.store.delete_object_stamped("replica", "existing", Some(st(base + 3, "peer")), None).await.unwrap();
    assert!(h.store.index("replica").await.unwrap().changelog_head() > head);

    // Read-write again: client writes work.
    h.store.set_bucket_read_only("replica", false).await.unwrap();
    h.put("replica", "client", b"x").await;
}

#[tokio::test]
async fn read_only_over_http_and_the_console() {
    let h = Harness::new().await;
    h.store.create_bucket("replica").await.unwrap();
    h.put("replica", "doc", b"x").await;
    let (console_app, session) = console_app(&h).await;
    let (status, _) = console(&console_app, &session, "PUT", "/api/buckets/replica/read-only", Some(serde_json::json!({"read_only": true}))).await;
    assert_eq!(status, StatusCode::OK);
    let (_, settings) = console(&console_app, &session, "GET", "/api/buckets/replica/cors", None).await;
    assert_eq!(settings["read_only"], true);
    let (_, buckets) = console(&console_app, &session, "GET", "/api/buckets", None).await;
    assert_eq!(buckets["buckets"][0]["read_only"], true);
    // Only admins may change it.
    h.iam.create_user("bob", "password123").await.unwrap();
    let bob = h.iam.create_session("bob", false);
    let (status, _) = console(&console_app, &bob, "PUT", "/api/buckets/replica/read-only", Some(serde_json::json!({"read_only": false}))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // A console delete is refused like any client write.
    let (status, body) = console(&console_app, &session, "DELETE", "/api/object?bucket=replica&key=doc", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let app = s3_app(&h);
    // A client PUT is refused up front, with a clear reason.
    let response = send_put(&app, "/replica/new", "data", &[]).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains("read-only"), "{}", String::from_utf8_lossy(&body));
    let response = send_put(&app, "/replica/copy", "", &[("x-amz-copy-source", "/replica/doc")]).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let delete = app
        .clone()
        .oneshot(Request::builder().method("DELETE").uri("/replica/doc").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::FORBIDDEN);
    // A replicated (stamped) PUT is accepted.
    let stamp = st(now_ms() + 1_000, "peer").to_header();
    let response = send_put(&app, "/replica/new", "data", &[("x-rs3-stamp", &stamp)]).await;
    assert_eq!(response.status(), StatusCode::OK);
}
