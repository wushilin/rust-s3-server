//! Conformance tests for `docs/spec-replication.md` and
//! `docs/spec-webhooks.md`: every request, header, parameter, status code
//! and JSON field the specifications promise, exercised over HTTP exactly as
//! a foreign implementation would use them (SigV4-signed requests through
//! the real router, a real webhook receiver). Test names cite the section
//! they cover. A behaviour change that breaks one of these is a protocol
//! change and needs the spec updated with it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::{to_bytes, Body};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::routing::post;
use axum::Router;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use super::auth::AuthState;
use super::config::{AppConfig, ChangelogConfig, Credential, WebhookConfig};
use super::iam::IamStore;
use super::notify::model::{Filter, SubscriptionKind};
use super::notify::{db::NotifyDb, CreateRequest, NotifyService};
use crate::storage::stamp::Stamp;
use crate::storage::store::LocalObjectStore;

const HOST: &str = "s3.spec.test";
const REGION: &str = "us-east-1";
const ADMIN: (&str, &str) = ("spec-admin", "spec-admin-secret");
const BUCKET: &str = "bkt";
const SITE: &str = "0123456789abcdef";

/// An S3 key: an IAM user with a policy on `b`.
#[derive(Clone)]
struct Key {
    ak: String,
    sk: String,
}

struct Server {
    _tmp: tempfile::TempDir,
    store: LocalObjectStore,
    notify: NotifyService,
    app: Router,
    shutdown: CancellationToken,
    /// Has every replication action on `b` (the "Replicate one bucket" preset).
    replicator: Key,
    /// May read and write `b`, but not replicate (no `s3:ReplicateWrite`,
    /// no `s3:GetBucketChangelog`).
    writer: Key,
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    /// The S3 error code of an XML error body.
    fn s3_code(&self) -> String {
        let text = self.text();
        let start = text.find("<Code>").map(|i| i + 6).unwrap_or(0);
        let end = text[start..].find("</Code>").map(|i| i + start).unwrap_or(start);
        text[start..end].to_string()
    }
}

impl Server {
    async fn start() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("site.json"), format!(r#"{{"site_id":"{SITE}","site_name":"Spec site"}}"#)).unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.init_site(None).unwrap();
        let iam = IamStore::open(tmp.path()).await.unwrap();
        let shutdown = CancellationToken::new();
        let notify = NotifyService::start(
            store.clone(),
            NotifyDb::new(iam.admin_db()),
            WebhookConfig {
                check_interval_secs: 1,
                mute_secs: 1,
                max_mute_secs: 5,
                request_timeout_secs: 5,
                max_concurrent_deliveries: 4,
                batch_max_events: 100,
                batch_max_bytes: 1024 * 1024,
                allow_http: true,
                allow_private_addresses: true,
            },
            ChangelogConfig::default(),
            shutdown.clone(),
        )
        .await
        .unwrap();
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        config.auth.credentials.push(Credential { access_key: ADMIN.0.into(), secret_key: ADMIN.1.into() });
        let app = super::router_with_notify(
            store.clone(),
            AuthState { config: Arc::new(config), iam: Some(iam.clone()), usage: None },
            Arc::new(super::TrafficMetrics::default()),
            super::registry::TaskRegistry::new(),
            Some(notify.clone()),
        );
        let replicator = key_with(
            &iam,
            "replicator",
            &["s3:GetBucketChangelog", "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ReplicateWrite", "s3:AbortMultipartUpload"],
        )
        .await;
        let writer = key_with(&iam, "writer", &["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]).await;
        store.create_bucket(BUCKET).await.unwrap();
        Self { _tmp: tmp, store, notify, app, shutdown, replicator, writer }
    }

    fn admin(&self) -> Key {
        Key { ak: ADMIN.0.into(), sk: ADMIN.1.into() }
    }

    /// A SigV4-signed request. Every header in `headers` is signed (listed
    /// in `SignedHeaders`), as the spec requires for `x-rs3-*`.
    async fn send(&self, key: &Key, method: &str, path: &str, query: &str, headers: &[(&str, &str)], body: &[u8]) -> Reply {
        self.send_with(key, method, path, query, headers, &[], body).await
    }

    /// Like [`Self::send`], plus `unsigned` headers sent but left out of the
    /// signature.
    #[allow(clippy::too_many_arguments)]
    async fn send_with(
        &self,
        key: &Key,
        method: &str,
        path: &str,
        query: &str,
        headers: &[(&str, &str)],
        unsigned: &[(&str, &str)],
        body: &[u8],
    ) -> Reply {
        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let (authorization, _) = super::auth::compute_auth_header_payload(
            method,
            path,
            query,
            HOST,
            &key.ak,
            &key.sk,
            REGION,
            &datetime,
            "UNSIGNED-PAYLOAD",
            headers,
        );
        let uri = if query.is_empty() { path.to_string() } else { format!("{path}?{query}") };
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("host", HOST)
            .header("x-amz-date", &datetime)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .header("authorization", authorization);
        for (name, value) in headers.iter().chain(unsigned) {
            request = request.header(*name, *value);
        }
        let response = self.app.clone().oneshot(request.body(Body::from(body.to_vec())).unwrap()).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap().to_vec();
        Reply { status, headers, body }
    }

    async fn put(&self, key: &str, body: &[u8]) -> Reply {
        self.send(&self.admin(), "PUT", &format!("/{BUCKET}/{key}"), "", &[], body).await
    }

    async fn changes(&self, query: &str) -> Reply {
        let query = if query.is_empty() { "x-rs3-changelog".to_string() } else { format!("x-rs3-changelog&{query}") };
        self.send(&self.replicator, "GET", &format!("/{BUCKET}"), &query, &[], b"").await
    }

    async fn snapshot(&self, query: &str) -> Reply {
        let query = if query.is_empty() { "x-rs3-changelog-snapshot".to_string() } else { format!("x-rs3-changelog-snapshot&{query}") };
        self.send(&self.replicator, "GET", &format!("/{BUCKET}"), &query, &[], b"").await
    }

    async fn head(&self, key: &str) -> Reply {
        self.send(&self.admin(), "HEAD", &format!("/{BUCKET}/{key}"), "", &[], b"").await
    }

    async fn events(&self, after: u64) -> Vec<Value> {
        let page = self.changes(&format!("after={after}&wait=0")).await;
        assert_eq!(page.status, StatusCode::OK, "{}", page.text());
        page.json()["events"].as_array().unwrap().clone()
    }

    async fn last_event(&self) -> Value {
        self.events(0).await.last().cloned().expect("an event")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.store.shutdown_token().cancel();
    }
}

async fn key_with(iam: &IamStore, user: &str, actions: &[&str]) -> Key {
    iam.create_user(user, "password123").await.unwrap();
    let key = iam.create_access_key(user, &["spec".to_string()]).await.unwrap();
    let policy: super::policy::PolicyDocument = serde_json::from_value(json!({
        "Statement": [{"Effect": "Allow", "Action": actions,
                       "Resource": [format!("arn:aws:s3:::{BUCKET}"), format!("arn:aws:s3:::{BUCKET}/*")]}]
    }))
    .unwrap();
    iam.set_policy(user, Some(&policy)).await.unwrap();
    Key { ak: key.access_key, sk: key.secret_key }
}

fn keys_of(value: &Value) -> BTreeSet<String> {
    value.as_object().expect("an object").keys().cloned().collect()
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

fn stamp_of(value: &Value) -> Stamp {
    serde_json::from_value(value.clone()).expect("a stamp")
}

fn st(ms: i64, c: u32, site: &str) -> String {
    Stamp { ms, c, site: site.into() }.to_header()
}

fn now_ms() -> i64 {
    crate::storage::time::now_ms()
}

// ── §1 transport and signing ─────────────────────────────────────────────

#[tokio::test]
async fn s1_an_unsigned_x_rs3_header_is_refused_and_writes_nothing() {
    let s = Server::start().await;
    let stamp = st(now_ms(), 0, "ffffffffffffffff");
    let reply = s
        .send_with(&s.replicator, "PUT", "/bkt/k", "", &[], &[("x-rs3-stamp", &stamp)], b"x")
        .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN, "{}", reply.text());
    assert_eq!(reply.s3_code(), "SignatureDoesNotMatch");
    assert_eq!(s.head("k").await.status, StatusCode::NOT_FOUND);
    assert!(s.events(0).await.is_empty(), "nothing was logged");
    // Signed, the same request is accepted.
    let reply = s.send(&s.replicator, "PUT", "/bkt/k", "", &[("x-rs3-stamp", &stamp)], b"x").await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
}

// ── §3 the event, §4.1 reading ──────────────────────────────────────────

#[tokio::test]
async fn s3_a_put_is_logged_with_every_field_and_matches_the_object_headers() {
    let s = Server::start().await;
    let put = s
        .send(&s.admin(), "PUT", "/bkt/photos/a.jpg", "", &[("content-type", "image/jpeg"), ("x-amz-meta-camera", "x100")], b"jpeg bytes")
        .await;
    assert_eq!(put.status, StatusCode::OK);
    let etag = put.header("etag").unwrap().trim_matches('"').to_string();

    let page = s.changes("after=0&wait=0").await;
    assert_eq!(page.status, StatusCode::OK);
    assert_eq!(page.header("content-type"), Some("application/json"));
    let page = page.json();
    assert_eq!(
        keys_of(&page),
        set(&["bucket", "site_id", "site_name", "log_id", "head", "trimmed_through", "next", "more", "events"])
    );
    assert_eq!(page["bucket"], BUCKET);
    assert_eq!(page["site_id"], SITE);
    assert_eq!(page["site_name"], "Spec site");
    assert!(page["log_id"].as_str().is_some_and(|id| !id.is_empty()));
    assert_eq!(page["trimmed_through"], 0);
    assert_eq!(page["more"], false);
    let events = page["events"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(
        keys_of(event),
        set(&["seq", "op", "key", "etag", "size", "last_modified_ms", "version", "origin", "ts_ms", "parts", "stamp"]),
        "a live put carries exactly these fields: {event}"
    );
    assert_eq!(event["op"], "put");
    assert_eq!(event["key"], "photos/a.jpg");
    assert_eq!(event["etag"], etag, "bare ETag, no quotes");
    assert_eq!(event["size"], 10);
    assert_eq!(event["origin"], "client");
    assert_eq!(event["parts"], json!({"count": 1}));
    assert_eq!(event["stamp"]["site"], SITE);
    assert_eq!(keys_of(&event["stamp"]), set(&["ms", "c", "site"]));
    let seq = event["seq"].as_u64().unwrap();
    assert_eq!(page["head"], seq);
    assert_eq!(page["next"], seq, "next is the position to read after");
    assert!(event["ts_ms"].as_i64().unwrap() >= event["last_modified_ms"].as_i64().unwrap() - 1000);

    // §5.1: GET/HEAD report the same identity.
    let head = s.head("photos/a.jpg").await;
    assert_eq!(head.status, StatusCode::OK);
    assert_eq!(head.header("x-rs3-version"), event["version"].as_str());
    assert_eq!(head.header("x-rs3-stamp"), Some(stamp_of(&event["stamp"]).to_header().as_str()));
    assert_eq!(head.header("x-rs3-site"), Some(SITE));
    let get = s.send(&s.admin(), "GET", "/bkt/photos/a.jpg", "", &[], b"").await;
    assert_eq!(get.header("x-rs3-version"), head.header("x-rs3-version"));
    assert_eq!(get.header("x-rs3-stamp"), head.header("x-rs3-stamp"));
    assert_eq!(get.header("x-rs3-site"), Some(SITE));

    // Reading after the head is a normal empty answer, not an error.
    let empty = s.changes(&format!("after={seq}&wait=0")).await;
    assert_eq!(empty.status, StatusCode::OK);
    assert_eq!(empty.json()["events"], json!([]));
    assert_eq!(empty.json()["next"], seq);
}

#[tokio::test]
async fn s3_a_delete_names_the_removed_version_and_the_key_keeps_a_tombstone() {
    let s = Server::start().await;
    s.put("k", b"first").await;
    let put = s.last_event().await;
    let del = s.send(&s.admin(), "DELETE", "/bkt/k", "", &[], b"").await;
    assert_eq!(del.status, StatusCode::NO_CONTENT);
    let event = s.last_event().await;
    assert_eq!(
        keys_of(&event),
        set(&["seq", "op", "key", "etag", "size", "last_modified_ms", "version", "origin", "ts_ms", "parts", "stamp", "removed_stamp"]),
        "{event}"
    );
    assert_eq!(event["op"], "delete");
    assert_eq!(event["etag"], put["etag"], "the removed version's ETag");
    assert_eq!(event["version"], put["version"]);
    assert_eq!(event["size"], put["size"]);
    assert_eq!(event["parts"], put["parts"]);
    assert_eq!(event["removed_stamp"], put["stamp"], "a delete ranks by the version it removed");
    assert!(stamp_of(&event["stamp"]) > stamp_of(&put["stamp"]), "the delete's own stamp is newer");
    assert_eq!(event["origin"], "client");

    // §5.1: a deleted key answers 404 with its tombstone.
    let head = s.head("k").await;
    assert_eq!(head.status, StatusCode::NOT_FOUND);
    assert_eq!(head.header("x-rs3-site"), Some(SITE));
    assert_eq!(head.header("x-rs3-deleted"), Some("true"));
    assert_eq!(head.header("x-rs3-stamp"), Some(stamp_of(&event["stamp"]).to_header().as_str()));
    assert_eq!(head.header("x-rs3-removed-stamp"), Some(stamp_of(&put["stamp"]).to_header().as_str()));
    // A key that never existed: 404 with the site only.
    let never = s.head("never").await;
    assert_eq!(never.status, StatusCode::NOT_FOUND);
    assert_eq!(never.header("x-rs3-site"), Some(SITE));
    assert!(never.header("x-rs3-stamp").is_none() && never.header("x-rs3-deleted").is_none());
}

#[tokio::test]
async fn s3_2_multipart_layouts_and_metadata_copies_are_described() {
    let s = Server::start().await;
    const MIB5: usize = 5 * 1024 * 1024;
    let create = s.send(&s.admin(), "POST", "/bkt/big", "uploads", &[], b"").await;
    assert_eq!(create.status, StatusCode::OK);
    let text = create.text();
    let upload = text.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let mut etags = Vec::new();
    for (number, part) in [vec![1u8; MIB5], vec![2u8; 1]].iter().enumerate() {
        let reply = s
            .send(&s.admin(), "PUT", "/bkt/big", &format!("partNumber={}&uploadId={upload}", number + 1), &[], part)
            .await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
        etags.push(reply.header("etag").unwrap().to_string());
    }
    let manifest = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etags[0], etags[1]
    );
    let done = s
        .send(&s.admin(), "POST", "/bkt/big", &format!("uploadId={upload}"), &[], manifest.as_bytes())
        .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.text());
    let event = s.last_event().await;
    assert_eq!(event["op"], "put");
    assert_eq!(event["parts"], json!({"multipart": true, "count": 2, "size": MIB5, "last": 1}));
    assert!(event["etag"].as_str().unwrap().ends_with("-2"));
    assert_eq!(event["size"], MIB5 + 1);
    let before = event.clone();

    // A metadata-only copy keeps bytes, layout and ETag; new version, new stamp.
    let copy = s
        .send(
            &s.admin(),
            "PUT",
            "/bkt/big",
            "",
            &[("x-amz-copy-source", "/bkt/big"), ("x-amz-metadata-directive", "REPLACE"), ("x-amz-meta-note", "v2")],
            b"",
        )
        .await;
    assert_eq!(copy.status, StatusCode::OK, "{}", copy.text());
    let event = s.last_event().await;
    assert_eq!(event["op"], "put");
    assert_eq!(event["etag"], before["etag"]);
    assert_eq!(event["parts"], before["parts"]);
    assert_ne!(event["version"], before["version"], "every publish is a new version");
    assert!(stamp_of(&event["stamp"]) > stamp_of(&before["stamp"]));
    assert_eq!(s.head("big").await.header("x-amz-meta-note"), Some("v2"));
}

#[tokio::test]
async fn s4_1_read_parameters_filters_and_errors() {
    let s = Server::start().await;
    s.put("logs/a.txt", b"1").await;
    s.put("logs/b.log", b"2").await;
    s.put("img/c.txt", b"3").await;
    s.send(&s.admin(), "DELETE", "/bkt/logs/a.txt", "", &[], b"").await;
    let all = s.events(0).await;
    assert_eq!(all.len(), 4);
    let head = all[3]["seq"].as_u64().unwrap();
    let keys = |events: &[Value]| events.iter().map(|e| e["key"].as_str().unwrap().to_string()).collect::<Vec<_>>();

    // Filters.
    let page = s.changes("after=0&wait=0&prefix=logs/").await.json();
    assert_eq!(keys(page["events"].as_array().unwrap()), ["logs/a.txt", "logs/b.log", "logs/a.txt"]);
    let page = s.changes("after=0&wait=0&suffix=.txt&ops=put").await.json();
    assert_eq!(keys(page["events"].as_array().unwrap()), ["logs/a.txt", "img/c.txt"]);
    let page = s.changes("after=0&wait=0&ops=delete").await.json();
    assert_eq!(keys(page["events"].as_array().unwrap()), ["logs/a.txt"]);
    assert_eq!(page["next"], head, "next moves past filtered-out changes");
    // Paging.
    let page = s.changes("after=0&wait=0&limit=2").await.json();
    assert_eq!(page["events"].as_array().unwrap().len(), 2);
    assert_eq!(page["more"], true);
    let next = page["next"].as_u64().unwrap();
    let rest = s.changes(&format!("after={next}&wait=0")).await.json();
    assert_eq!(rest["events"].as_array().unwrap().len(), 2);
    assert_eq!(rest["more"], false);
    // since.
    let since = all[2]["ts_ms"].as_i64().unwrap();
    let page = s.changes(&format!("since={since}&wait=0")).await.json();
    assert!(keys(page["events"].as_array().unwrap()).starts_with(&["img/c.txt".to_string()]), "{page}");
    // log_id.
    let log_id = page["log_id"].as_str().unwrap().to_string();
    assert_eq!(s.changes(&format!("after=0&wait=0&log_id={log_id}")).await.status, StatusCode::OK);

    // 400s.
    for bad in ["ops=bogus&wait=0", "after=1&since=1&wait=0", "wait=1&wait_ms=1", "after=x&wait=0", "limit=-1&wait=0"] {
        let reply = s.changes(bad).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{bad}: {}", reply.text());
        assert!(reply.json()["error"].as_str().is_some(), "{bad}: {}", reply.text());
    }
    // 409: beyond the head, or another log.
    let reply = s.changes(&format!("after={}&wait=0", head + 10)).await;
    assert_eq!(reply.status, StatusCode::CONFLICT);
    assert_eq!(keys_of(&reply.json()), set(&["error", "log_id", "head"]));
    assert_eq!(reply.json()["head"], head);
    assert_eq!(reply.json()["log_id"], log_id);
    let reply = s.changes("after=0&wait=0&log_id=not-this-log").await;
    assert_eq!(reply.status, StatusCode::CONFLICT);
    // 404 and 403.
    let reply = s.send(&s.admin(), "GET", "/nope", "x-rs3-changelog&wait=0", &[], b"").await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    let reply = s.send(&s.replicator, "GET", "/nope", "x-rs3-changelog&wait=0", &[], b"").await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN, "a key without a policy on the bucket learns nothing about it");
    let reply = s.send(&s.writer, "GET", "/bkt", "x-rs3-changelog&wait=0", &[], b"").await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN, "needs s3:GetBucketChangelog");

    // 410: behind retention.
    let index = s.store.index(BUCKET).await.unwrap();
    assert_eq!(index.trim_changelog(now_ms() + 1_000, 2).await.unwrap(), 2);
    let reply = s.changes("after=0&wait=0").await;
    assert_eq!(reply.status, StatusCode::GONE, "{}", reply.text());
    assert_eq!(keys_of(&reply.json()), set(&["error", "trimmed_through", "head"]));
    let trimmed = reply.json()["trimmed_through"].as_u64().unwrap();
    assert_eq!(reply.json()["head"], head);
    let page = s.changes(&format!("after={trimmed}&wait=0")).await;
    assert_eq!(page.status, StatusCode::OK, "the oldest available position reads");
    assert_eq!(page.json()["trimmed_through"], trimmed);
    assert_eq!(page.json()["events"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn s4_1_long_poll_holds_until_a_change_or_the_wait() {
    let s = Server::start().await;
    s.put("k", b"x").await;
    let head = s.last_event().await["seq"].as_u64().unwrap();
    let started = Instant::now();
    let page = s.changes(&format!("after={head}&wait=1")).await;
    assert!(started.elapsed() >= Duration::from_millis(900), "held for the wait");
    assert_eq!(page.json()["events"], json!([]));
    assert_eq!(page.json()["next"], head);
    let started = Instant::now();
    let app = s.app.clone();
    let store = s.store.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = app;
        store.put_object(BUCKET, "k2", b"y", None, None, false).await.unwrap();
    });
    let page = s.changes(&format!("after={head}&wait=10&wait_ms=")).await;
    writer.await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(5), "woke on the commit");
    assert_eq!(page.json()["events"][0]["key"], "k2");
}

// ── §4.2 snapshot ───────────────────────────────────────────────────────

#[tokio::test]
async fn s4_2_snapshot_pages_objects_then_tombstones() {
    let s = Server::start().await;
    for key in ["a", "b", "c"] {
        s.put(key, key.as_bytes()).await;
    }
    let first = s.snapshot("limit=2").await;
    assert_eq!(first.status, StatusCode::OK);
    let first = first.json();
    assert_eq!(keys_of(&first), set(&["bucket", "site_id", "site_name", "log_id", "head", "entries", "next_after"]));
    assert_eq!(first["next_after"], "b");
    let head = first["head"].as_u64().unwrap();
    let entry = &first["entries"][0];
    assert_eq!(
        keys_of(entry),
        set(&["op", "key", "etag", "size", "last_modified_ms", "version", "parts", "stamp"]),
        "a snapshot entry has no seq, origin or ts_ms: {entry}"
    );
    assert_eq!(entry["op"], "exists");
    assert_eq!(entry["stamp"]["site"], SITE);
    let second = s.snapshot("limit=2&after=b").await.json();
    assert_eq!(second["entries"].as_array().unwrap().len(), 1);
    assert_eq!(second["entries"][0]["key"], "c");
    assert_eq!(second["next_after"], Value::Null);
    // Changes during the walk replay from the first page's head.
    s.put("d", b"d").await;
    let live = s.changes(&format!("after={head}&wait=0")).await.json();
    assert_eq!(live["events"][0]["key"], "d");

    // Tombstones.
    s.send(&s.admin(), "DELETE", "/bkt/b", "", &[], b"").await;
    let delete = s.last_event().await;
    let tombs = s.snapshot("deleted=1").await.json();
    assert_eq!(tombs["entries"].as_array().unwrap().len(), 1);
    let tomb = &tombs["entries"][0];
    assert_eq!(tomb["op"], "deleted");
    assert_eq!(tomb["key"], "b");
    assert_eq!(tomb["stamp"], delete["stamp"]);
    assert_eq!(tomb["removed_stamp"], delete["removed_stamp"]);
    assert_eq!(tomb["etag"], delete["etag"]);
    assert_eq!(tomb["version"], delete["version"]);
    let objects = s.snapshot("").await.json();
    assert_eq!(objects["entries"].as_array().unwrap().len(), 3, "a, c, d");
    assert_eq!(s.snapshot("ops=delete").await.json()["entries"], json!([]));
}

// ── §4.3 replication tokens ─────────────────────────────────────────────

#[tokio::test]
async fn s4_3_a_replication_token_reads_the_same_shapes() {
    let s = Server::start().await;
    s.put("in/a", b"1").await;
    s.put("out/b", b"2").await;
    let created = s
        .notify
        .create(
            CreateRequest {
                name: "pull".into(),
                bucket: BUCKET.into(),
                kind: SubscriptionKind::Pull,
                url: String::new(),
                headers: BTreeMap::new(),
                filter: Filter { prefix: "in/".into(), ..Filter::default() },
                initial_snapshot: false,
                secret: None,
                batch_max_events: None,
                batch_max_bytes: None,
                mute_secs: None,
            },
            "spec",
        )
        .await
        .unwrap();
    let token = created.token.unwrap();
    assert!(token.starts_with("rs3cl_"));
    let bearer = |token: &str| {
        Request::builder()
            .method("GET")
            .uri("/_rs3/changelog?after=0&wait=0")
            .header("host", HOST)
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };
    let response = s.app.clone().oneshot(bearer(&token)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let page: Value = serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        keys_of(&page),
        set(&["bucket", "site_id", "site_name", "log_id", "head", "trimmed_through", "next", "more", "events"])
    );
    assert_eq!(page["events"].as_array().unwrap().len(), 1, "the token's filter is its scope");
    assert_eq!(page["events"][0]["key"], "in/a");
    assert_eq!(page["site_id"], SITE);
    let snapshot = s
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/_rs3/changelog/snapshot")
                .header("host", HOST)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(snapshot.status(), StatusCode::OK);
    let snapshot: Value = serde_json::from_slice(&to_bytes(snapshot.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(keys_of(&snapshot), set(&["bucket", "site_id", "site_name", "log_id", "head", "entries", "next_after"]));
    assert_eq!(snapshot["entries"][0]["op"], "exists");
    for bad in ["", "rs3cl_wrong"] {
        let response = s.app.clone().oneshot(bearer(bad)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "token {bad:?}");
    }
}

// ── §5.2–§5.4 replicated writes ─────────────────────────────────────────

#[tokio::test]
async fn s5_2_a_replicated_put_keeps_its_stamp_and_needs_replicate_write() {
    let s = Server::start().await;
    let peer = "feedfacefeedface";
    let ms = now_ms() - 5_000;
    let stamp = st(ms, 3, peer);
    let reply = s
        .send(&s.replicator, "PUT", "/bkt/k", "", &[("x-rs3-stamp", &stamp), ("x-amz-meta-from", "peer")], b"peer bytes")
        .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
    let head = s.head("k").await;
    assert_eq!(head.header("x-rs3-stamp"), Some(stamp.as_str()), "the state keeps the given stamp");
    assert_eq!(head.header("last-modified"), Some(crate::storage::time::http_date_ms(ms).as_str()));
    assert_eq!(head.header("x-amz-meta-from"), Some("peer"));
    let event = s.last_event().await;
    assert_eq!(event["origin"], "replication");
    assert_eq!(event["stamp"], json!({"ms": ms, "c": 3, "site": peer}));
    assert_eq!(event["last_modified_ms"], ms);

    // Without s3:ReplicateWrite the stamp is refused, even with PutObject.
    let later = st(ms + 1, 0, peer);
    let reply = s.send(&s.writer, "PUT", "/bkt/k", "", &[("x-rs3-stamp", &later)], b"x").await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert_eq!(reply.s3_code(), "AccessDenied");
    assert_eq!(s.head("k").await.header("x-rs3-stamp"), Some(stamp.as_str()));
    // Malformed, or too far in the future.
    for bad in ["nonsense", "1.2", &st(now_ms() + 11 * 60 * 1000, 0, peer)] {
        let reply = s.send(&s.replicator, "PUT", "/bkt/k", "", &[("x-rs3-stamp", bad)], b"x").await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{bad}: {}", reply.text());
        assert_eq!(reply.s3_code(), "InvalidArgument");
    }
    // Multipart: the stamp goes on the completion.
    let create = s.send(&s.replicator, "POST", "/bkt/mp", "uploads", &[], b"").await;
    let text = create.text();
    let upload = text.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let part = s.send(&s.replicator, "PUT", "/bkt/mp", &format!("partNumber=1&uploadId={upload}"), &[], b"part").await;
    let manifest = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        part.header("etag").unwrap()
    );
    let mp_stamp = st(ms + 2, 0, peer);
    let done = s
        .send(&s.replicator, "POST", "/bkt/mp", &format!("uploadId={upload}"), &[("x-rs3-stamp", &mp_stamp)], manifest.as_bytes())
        .await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.text());
    assert_eq!(s.head("mp").await.header("x-rs3-stamp"), Some(mp_stamp.as_str()));
    assert_eq!(s.last_event().await["parts"], json!({"multipart": true, "count": 1, "size": 4, "last": 4}));
    // A metadata-only copy with a stamp.
    let copy_stamp = st(ms + 3, 0, peer);
    let copy = s
        .send(
            &s.replicator,
            "PUT",
            "/bkt/k",
            "",
            &[("x-amz-copy-source", "/bkt/k"), ("x-amz-metadata-directive", "REPLACE"), ("x-rs3-stamp", &copy_stamp), ("x-amz-meta-from", "peer2")],
            b"",
        )
        .await;
    assert_eq!(copy.status, StatusCode::OK, "{}", copy.text());
    let head = s.head("k").await;
    assert_eq!(head.header("x-rs3-stamp"), Some(copy_stamp.as_str()));
    assert_eq!(head.header("x-amz-meta-from"), Some("peer2"));
    assert_eq!(head.header("etag"), reply_etag(&s, "k").await.as_deref());
}

async fn reply_etag(s: &Server, key: &str) -> Option<String> {
    s.head(key).await.header("etag").map(str::to_string)
}

#[tokio::test]
async fn s5_4_a_stale_replicated_write_is_412_with_the_current_stamp() {
    let s = Server::start().await;
    let peer = "feedfacefeedface";
    let ms = now_ms() - 5_000;
    let newer = st(ms + 10, 0, peer);
    let older = st(ms, 0, peer);
    assert_eq!(s.send(&s.replicator, "PUT", "/bkt/k", "", &[("x-rs3-stamp", &newer)], b"new").await.status, StatusCode::OK);
    let head_before = s.last_event().await["seq"].as_u64().unwrap();
    for stale in [&older, &newer] {
        let reply = s.send(&s.replicator, "PUT", "/bkt/k", "", &[("x-rs3-stamp", stale)], b"old").await;
        assert_eq!(reply.status, StatusCode::PRECONDITION_FAILED, "{}", reply.text());
        assert_eq!(reply.s3_code(), "PreconditionFailed");
        assert_eq!(reply.header("x-rs3-stamp-current"), Some(newer.as_str()));
    }
    assert_eq!(s.send(&s.admin(), "GET", "/bkt/k", "", &[], b"").await.text(), "new");
    assert_eq!(s.last_event().await["seq"], head_before, "nothing was logged");
    // A stale delete, too; and the current stamp of a tombstone is marked.
    let reply = s
        .send(&s.replicator, "DELETE", "/bkt/k", "", &[("x-rs3-stamp", &st(ms + 20, 0, peer)), ("x-rs3-removed-stamp", &older)], b"")
        .await;
    assert_eq!(reply.status, StatusCode::PRECONDITION_FAILED, "a delete of an older version loses");
    assert_eq!(reply.header("x-rs3-stamp-current"), Some(newer.as_str()));
    let reply = s
        .send(&s.replicator, "DELETE", "/bkt/k", "", &[("x-rs3-stamp", &st(ms + 20, 0, peer)), ("x-rs3-removed-stamp", &newer)], b"")
        .await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    let reply = s.send(&s.replicator, "PUT", "/bkt/k", "", &[("x-rs3-stamp", &newer)], b"back").await;
    assert_eq!(reply.status, StatusCode::PRECONDITION_FAILED, "the tombstone outranks the removed version");
    assert_eq!(reply.header("x-rs3-stamp-current"), Some(format!("{newer} (deleted)").as_str()));
    // A client's write is never stale.
    assert_eq!(s.put("k", b"client").await.status, StatusCode::OK);
    assert_eq!(s.last_event().await["origin"], "client");
}

#[tokio::test]
async fn s5_2_a_replicated_delete_is_version_scoped_and_tombstones_absent_keys() {
    let s = Server::start().await;
    let peer = "feedfacefeedface";
    let ms = now_ms() - 5_000;
    let v1 = st(ms, 0, peer);
    let v2 = st(ms + 5, 0, peer);
    assert_eq!(s.send(&s.replicator, "PUT", "/bkt/k", "", &[("x-rs3-stamp", &v1)], b"v1").await.status, StatusCode::OK);
    assert_eq!(s.send(&s.replicator, "PUT", "/bkt/k", "", &[("x-rs3-stamp", &v2)], b"v2").await.status, StatusCode::OK);
    // Deleting v1 (seen elsewhere before v2 arrived) does not remove v2.
    let del = s
        .send(&s.replicator, "DELETE", "/bkt/k", "", &[("x-rs3-stamp", &st(ms + 9, 0, peer)), ("x-rs3-removed-stamp", &v1)], b"")
        .await;
    assert_eq!(del.status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(s.send(&s.admin(), "GET", "/bkt/k", "", &[], b"").await.text(), "v2");
    // Deleting v2 does.
    let del_stamp = st(ms + 9, 0, peer);
    let del = s
        .send(&s.replicator, "DELETE", "/bkt/k", "", &[("x-rs3-stamp", &del_stamp), ("x-rs3-removed-stamp", &v2)], b"")
        .await;
    assert_eq!(del.status, StatusCode::NO_CONTENT);
    let event = s.last_event().await;
    assert_eq!(event["op"], "delete");
    assert_eq!(event["origin"], "replication");
    assert_eq!(event["stamp"], json!({"ms": ms + 9, "c": 0, "site": peer}));
    assert_eq!(event["removed_stamp"], json!({"ms": ms + 5, "c": 0, "site": peer}));
    let head = s.head("k").await;
    assert_eq!(head.header("x-rs3-removed-stamp"), Some(v2.as_str()));
    assert_eq!(head.header("x-rs3-stamp"), Some(del_stamp.as_str()));
    // A delete without a removed stamp ranks by its own stamp.
    let v3 = st(ms + 20, 0, peer);
    assert_eq!(s.send(&s.replicator, "PUT", "/bkt/k", "", &[("x-rs3-stamp", &v3)], b"v3").await.status, StatusCode::OK);
    let del = s.send(&s.replicator, "DELETE", "/bkt/k", "", &[("x-rs3-stamp", &st(ms + 19, 0, peer))], b"").await;
    assert_eq!(del.status, StatusCode::PRECONDITION_FAILED, "ranks by its own stamp, below v3");
    let del = s.send(&s.replicator, "DELETE", "/bkt/k", "", &[("x-rs3-stamp", &st(ms + 21, 0, peer))], b"").await;
    assert_eq!(del.status, StatusCode::NO_CONTENT);
    assert_eq!(s.head("k").await.header("x-rs3-removed-stamp"), Some(st(ms + 21, 0, peer).as_str()));
    // A stamped delete of a key never seen here still leaves a tombstone
    // and a delete event that removed nothing (§3.1).
    let ghost = st(ms + 30, 0, peer);
    let del = s
        .send(&s.replicator, "DELETE", "/bkt/ghost", "", &[("x-rs3-stamp", &ghost), ("x-rs3-removed-stamp", &st(ms + 29, 0, peer))], b"")
        .await;
    assert_eq!(del.status, StatusCode::NO_CONTENT);
    let event = s.last_event().await;
    assert_eq!(event["op"], "delete");
    assert_eq!(event["key"], "ghost");
    assert_eq!((event["etag"].as_str(), event["size"].as_u64(), event["version"].as_str()), (Some(""), Some(0), Some("")));
    assert_eq!(event["removed_stamp"], json!({"ms": ms + 29, "c": 0, "site": peer}));
    assert_eq!(s.head("ghost").await.header("x-rs3-deleted"), Some("true"));
    let tombs = s.snapshot("deleted=1").await.json();
    assert!(tombs["entries"].as_array().unwrap().iter().any(|t| t["key"] == "ghost"));
    // The older version arriving later is refused.
    let late = s.send(&s.replicator, "PUT", "/bkt/ghost", "", &[("x-rs3-stamp", &st(ms + 29, 0, peer))], b"late").await;
    assert_eq!(late.status, StatusCode::PRECONDITION_FAILED);
    // x-rs3-removed-stamp needs x-rs3-stamp.
    let reply = s.send(&s.replicator, "DELETE", "/bkt/k", "", &[("x-rs3-removed-stamp", &v3)], b"").await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(reply.s3_code(), "InvalidArgument");
    // A client's delete of an absent key is an idempotent no-op.
    let before = s.last_event().await["seq"].as_u64().unwrap();
    assert_eq!(s.send(&s.admin(), "DELETE", "/bkt/nothing", "", &[], b"").await.status, StatusCode::NO_CONTENT);
    assert_eq!(s.last_event().await["seq"], before);
}

#[tokio::test]
async fn s5_3_a_read_only_bucket_takes_only_replicated_writes() {
    let s = Server::start().await;
    s.put("k", b"x").await;
    s.store.set_bucket_read_only(BUCKET, true).await.unwrap();
    let refused = [
        ("PUT", "/bkt/k", "", vec![], b"y".as_slice()),
        ("DELETE", "/bkt/k", "", vec![], b"".as_slice()),
        ("POST", "/bkt/k", "uploads", vec![], b"".as_slice()),
        ("PUT", "/bkt/k2", "", vec![("x-amz-copy-source", "/bkt/k")], b"".as_slice()),
        ("POST", "/bkt", "delete", vec![], b"<Delete><Object><Key>k</Key></Object></Delete>".as_slice()),
    ];
    for (method, path, query, headers, body) in refused {
        let reply = s.send(&s.admin(), method, path, query, &headers, body).await;
        assert_eq!(reply.status, StatusCode::FORBIDDEN, "{method} {path}?{query}: {}", reply.text());
        assert_eq!(reply.s3_code(), "AccessDenied", "{method} {path}");
    }
    assert_eq!(s.send(&s.admin(), "GET", "/bkt/k", "", &[], b"").await.text(), "x", "reads are unaffected");
    let stamp = st(now_ms(), 0, "feedfacefeedface");
    assert_eq!(s.send(&s.replicator, "PUT", "/bkt/k", "", &[("x-rs3-stamp", &stamp)], b"replicated").await.status, StatusCode::OK);
    assert_eq!(s.send(&s.admin(), "GET", "/bkt/k", "", &[], b"").await.text(), "replicated");
    let create = s.send(&s.replicator, "POST", "/bkt/mp", "uploads", &[], b"").await;
    assert_eq!(create.status, StatusCode::FORBIDDEN, "an unmarked multipart create is a client's write");
    let create = s.send(&s.writer, "POST", "/bkt/mp", "uploads", &[("x-rs3-replicate", "true")], b"").await;
    assert_eq!(create.status, StatusCode::FORBIDDEN, "x-rs3-replicate needs s3:ReplicateWrite");
    assert_eq!(create.s3_code(), "AccessDenied");
    let create = s.send(&s.replicator, "POST", "/bkt/mp", "uploads", &[("x-rs3-replicate", "maybe")], b"").await;
    assert_eq!(create.status, StatusCode::BAD_REQUEST);
    let create = s.send(&s.replicator, "POST", "/bkt/mp", "uploads", &[("x-rs3-replicate", "true")], b"").await;
    assert_eq!(create.status, StatusCode::OK, "x-rs3-replicate marks a replicated upload: {}", create.text());
    let text = create.text();
    let upload = text.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap().to_string();
    let part = s.send(&s.replicator, "PUT", "/bkt/mp", &format!("partNumber=1&uploadId={upload}"), &[], b"part").await;
    assert_eq!(part.status, StatusCode::OK, "parts of a replicated upload need no stamp: {}", part.text());
    let manifest = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        part.header("etag").unwrap()
    );
    let done = s.send(&s.replicator, "POST", "/bkt/mp", &format!("uploadId={upload}"), &[("x-rs3-stamp", &stamp)], manifest.as_bytes()).await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.text());
    assert_eq!(s.head("mp").await.header("x-rs3-stamp"), Some(stamp.as_str()));
    assert_eq!(s.send(&s.replicator, "GET", "/bkt", "x-rs3-access", &[], b"").await.json()["read_only"], true);
}

// ── §6 checking a peer ──────────────────────────────────────────────────

#[tokio::test]
async fn s6_1_the_access_report_names_the_five_actions() {
    let s = Server::start().await;
    let report = s.send(&s.replicator, "GET", "/bkt", "x-rs3-access", &[], b"").await;
    assert_eq!(report.status, StatusCode::OK);
    let report = report.json();
    assert_eq!(keys_of(&report), set(&["site_id", "site_name", "bucket", "read_only", "actions", "merge_back"]));
    assert_eq!(report["site_id"], SITE);
    assert_eq!(report["site_name"], "Spec site");
    assert_eq!(report["bucket"], BUCKET);
    assert_eq!(report["read_only"], false);
    assert_eq!(report["merge_back"], Value::Null);
    assert_eq!(
        report["actions"],
        json!({"s3:GetObject": true, "s3:GetBucketChangelog": true, "s3:PutObject": true, "s3:DeleteObject": true, "s3:ReplicateWrite": true})
    );
    let report = s.send(&s.writer, "GET", "/bkt", "x-rs3-access", &[], b"").await.json();
    assert_eq!(
        report["actions"],
        json!({"s3:GetObject": true, "s3:GetBucketChangelog": false, "s3:PutObject": true, "s3:DeleteObject": true, "s3:ReplicateWrite": false})
    );
    // Naming the reader is accepted; without a merge link there is no merge-back.
    let replicator = super::replication::peer::Replicator {
        site: "feedfacefeedface".into(),
        site_name: "Peer".into(),
        mode: "merge".into(),
        bucket: BUCKET.into(),
        link: "rl_1".into(),
        name: "spec link".into(),
    }
    .to_header();
    let report = s.send(&s.replicator, "GET", "/bkt", "x-rs3-access", &[("x-rs3-replicator", &replicator)], b"").await;
    assert_eq!(report.status, StatusCode::OK);
    assert_eq!(report.json()["merge_back"], Value::Null);
    let page = s.send(&s.replicator, "GET", "/bkt", "x-rs3-changelog&wait=0", &[("x-rs3-replicator", &replicator)], b"").await;
    assert_eq!(page.status, StatusCode::OK);
    assert!(page.header("x-rs3-merge-back").is_none());
    assert_eq!(s.send(&s.admin(), "GET", "/nope", "x-rs3-access", &[], b"").await.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn s7_named_fields_are_percent_encoded_and_extensible() {
    use super::replication::peer::{encode_fields, parse_fields, MergeBack, Replicator};
    let value = encode_fields(&[("link", "rl_1"), ("name", "a;b=c d"), ("future", "x")]);
    assert_eq!(value, "link=rl_1;name=a%3Bb%3Dc%20d;future=x");
    let parsed = parse_fields(&value);
    assert_eq!(parsed["name"], "a;b=c d");
    assert_eq!(MergeBack::parse(&value), Some(MergeBack { link: "rl_1".into(), name: "a;b=c d".into() }));
    assert_eq!(MergeBack::parse("name=only"), None, "link is required");
    let replicator = Replicator { site: "s".into(), site_name: String::new(), mode: "mirror".into(), bucket: "b".into(), link: "l".into(), name: String::new() };
    assert_eq!(Replicator::parse(&replicator.to_header()), Some(replicator));
    assert_eq!(Replicator::parse("site=s;mode=mirror;bucket=b"), None, "link is required");
    assert_eq!(Replicator::parse("link=l;bucket=b;mode=merge;site=s;extra=1").map(|r| r.mode), Some("merge".into()), "order and unknown fields do not matter");
}

#[tokio::test]
async fn s5_1_reads_pinned_to_a_version_fail_once_the_state_moves_on() {
    let s = Server::start().await;
    s.put("k", b"one").await;
    let version = s.head("k").await.header("x-rs3-version").unwrap().to_string();
    let pinned = s.send(&s.admin(), "GET", "/bkt/k", "", &[("x-rs3-if-version", &version)], b"").await;
    assert_eq!(pinned.status, StatusCode::OK);
    assert_eq!(pinned.text(), "one");
    // Same bytes, new version: the pin fails.
    s.send(&s.admin(), "PUT", "/bkt/k", "", &[("x-amz-copy-source", "/bkt/k"), ("x-amz-metadata-directive", "REPLACE"), ("x-amz-meta-n", "2")], b"")
        .await;
    let moved = s.send(&s.admin(), "GET", "/bkt/k", "", &[("x-rs3-if-version", &version)], b"").await;
    assert_eq!(moved.status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(moved.s3_code(), "PreconditionFailed");
    assert!(moved.text().contains("x-rs3-if-version"), "{}", moved.text());
    assert_eq!(s.head("k").await.header("etag"), pinned.header("etag"), "the ETag did not change");
}

// ── webhooks ─────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct Received {
    headers: HeaderMap,
    body: Vec<u8>,
}

#[derive(Clone)]
struct Receiver {
    url: String,
    got: Arc<Mutex<Vec<Received>>>,
    fail_first: Arc<Mutex<u32>>,
}

impl Receiver {
    async fn start(fail_first: u32) -> Self {
        let got: Arc<Mutex<Vec<Received>>> = Arc::new(Mutex::new(Vec::new()));
        let fails = Arc::new(Mutex::new(fail_first));
        let state = (got.clone(), fails.clone());
        let app = Router::new().route(
            "/hook",
            post(move |request: Request<Body>| {
                let (got, fails) = state.clone();
                async move {
                    let headers = request.headers().clone();
                    let body = to_bytes(request.into_body(), usize::MAX).await.unwrap().to_vec();
                    got.lock().unwrap().push(Received { headers, body });
                    let mut fails = fails.lock().unwrap();
                    if *fails > 0 {
                        *fails -= 1;
                        StatusCode::INTERNAL_SERVER_ERROR
                    } else {
                        StatusCode::OK
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { url: format!("http://{addr}/hook"), got, fail_first: fails }
    }

    fn received(&self) -> Vec<Received> {
        self.got.lock().unwrap().clone()
    }

    async fn wait_for(&self, n: usize) -> Vec<Received> {
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.received().len() < n {
            assert!(Instant::now() < deadline, "waited for {n} deliveries, got {}", self.received().len());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.received()
    }
}

async fn webhook(s: &Server, rx: &Receiver, snapshot: bool, batch_max_events: Option<usize>) -> (String, String) {
    let created = s
        .notify
        .create(
            CreateRequest {
                name: "spec-hook".into(),
                bucket: BUCKET.into(),
                kind: SubscriptionKind::Webhook,
                url: rx.url.clone(),
                headers: BTreeMap::from([("X-Tenant".to_string(), "t1".to_string())]),
                filter: Filter::default(),
                initial_snapshot: snapshot,
                secret: Some("spec-secret".into()),
                batch_max_events,
                batch_max_bytes: None,
                mute_secs: None,
            },
            "spec",
        )
        .await
        .unwrap();
    (created.subscription.id, created.secret.unwrap())
}

fn batch_of(received: &Received) -> Value {
    serde_json::from_slice(&received.body).expect("a JSON batch")
}

#[tokio::test]
async fn w1_w3_the_request_and_the_live_batch_have_the_specified_shape() {
    let s = Server::start().await;
    let rx = Receiver::start(0).await;
    let (id, secret) = webhook(&s, &rx, false, None).await;
    assert_eq!(secret, "spec-secret");
    s.put("a", b"1").await;
    s.send(&s.admin(), "DELETE", "/bkt/a", "", &[], b"").await;
    let got = rx.wait_for(1).await;
    // Give a second batch a chance to arrive if the two changes were split.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let got = if rx.received().len() > got.len() { rx.received() } else { got };
    let mut events = Vec::new();
    let mut previous_to: Option<u64> = None;
    for received in &got {
        let header = |name: &str| received.headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string);
        assert_eq!(header("content-type").as_deref(), Some("application/json"));
        assert_eq!(header("x-rs3-subscription").as_deref(), Some(id.as_str()));
        assert_eq!(header("x-tenant").as_deref(), Some("t1"), "custom headers are sent");
        let delivery = header("x-rs3-delivery").unwrap();
        assert!(delivery.len() == 32 && delivery.chars().all(|c| c.is_ascii_hexdigit()), "{delivery}");
        let signature = header("x-rs3-signature").unwrap();
        let (t, v1) = signature.split_once(",v1=").unwrap();
        let t: i64 = t.strip_prefix("t=").unwrap().parse().unwrap();
        assert!(v1.len() == 64 && v1.chars().all(|c| c.is_ascii_hexdigit()));
        assert!((now_ms() / 1000 - t).abs() < 60, "t is the unix time of the attempt");
        assert_eq!(signature, super::notify::delivery::signature(&secret, t, &received.body), "HMAC over <t>.<body>");

        let batch = batch_of(received);
        assert_eq!(
            keys_of(&batch),
            set(&["subscription", "bucket", "phase", "from_seq", "to_seq", "snapshot_begin", "snapshot_end", "events"]),
            "{batch}"
        );
        assert_eq!(batch["subscription"], id);
        assert_eq!(batch["bucket"], BUCKET);
        assert_eq!(batch["phase"], "live");
        assert_eq!((batch["snapshot_begin"].as_bool(), batch["snapshot_end"].as_bool()), (Some(false), Some(false)));
        let (from, to) = (batch["from_seq"].as_u64().unwrap(), batch["to_seq"].as_u64().unwrap());
        assert!(from <= to);
        if let Some(previous) = previous_to {
            assert_eq!(from, previous + 1, "consecutive live batches tile the log");
        }
        previous_to = Some(to);
        for event in batch["events"].as_array().unwrap() {
            let seq = event["seq"].as_u64().unwrap();
            assert!(from <= seq && seq <= to, "events lie in the batch's range");
            events.push(event.clone());
        }
    }
    assert_eq!(events.iter().map(|e| e["op"].as_str().unwrap()).collect::<Vec<_>>(), ["put", "delete"]);
    assert_eq!(
        keys_of(&events[0]),
        set(&["seq", "op", "key", "etag", "size", "last_modified_ms", "version", "origin", "ts_ms", "parts", "stamp"])
    );
    assert_eq!(events[1]["removed_stamp"], events[0]["stamp"]);
    // The same events the changelog read returns.
    assert_eq!(s.events(0).await, events);
}

#[tokio::test]
async fn w3_snapshot_batches_bracket_the_walk_and_live_continues_after_it() {
    let s = Server::start().await;
    for key in ["a", "b", "c"] {
        s.put(key, b"x").await;
    }
    let rx = Receiver::start(0).await;
    let _ = webhook(&s, &rx, true, Some(2)).await;
    let deadline = Instant::now() + Duration::from_secs(15);
    while !rx.received().iter().any(|r| batch_of(r)["snapshot_end"] == true) {
        assert!(Instant::now() < deadline, "snapshot end");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let batches: Vec<Value> = rx.received().iter().map(batch_of).collect();
    assert!(batches.len() >= 2, "2 entries per batch split 3 objects");
    assert!(batches.iter().all(|b| b["phase"] == "snapshot"));
    assert_eq!(batches[0]["snapshot_begin"], true);
    assert!(batches[1..].iter().all(|b| b["snapshot_begin"] == false));
    assert_eq!(batches.last().unwrap()["snapshot_end"], true);
    let through = batches[0]["snapshot_through"].as_u64().unwrap();
    for batch in &batches {
        assert_eq!(
            keys_of(batch),
            set(&["subscription", "bucket", "phase", "from_seq", "to_seq", "snapshot_begin", "snapshot_end", "snapshot_through", "events"])
        );
        assert_eq!((batch["from_seq"].as_u64(), batch["to_seq"].as_u64()), (Some(through), Some(through)));
        assert_eq!(batch["snapshot_through"], through);
        for event in batch["events"].as_array().unwrap() {
            assert_eq!(event["op"], "exists");
            assert!(event.get("seq").is_none() && event.get("origin").is_none() && event.get("ts_ms").is_none(), "{event}");
            assert!(event["stamp"].is_object() && event["version"].as_str().is_some_and(|v| !v.is_empty()));
        }
    }
    let keys: Vec<&str> = batches.iter().flat_map(|b| b["events"].as_array().unwrap()).map(|e| e["key"].as_str().unwrap()).collect();
    assert_eq!(keys, ["a", "b", "c"]);
    s.put("d", b"x").await;
    let n = batches.len();
    let got = rx.wait_for(n + 1).await;
    let live = batch_of(&got[n]);
    assert_eq!(live["phase"], "live");
    assert_eq!(live["from_seq"], through + 1, "live continues right after the snapshot point");
    assert_eq!(live["events"][0]["key"], "d");
}

#[tokio::test]
async fn w4_a_failed_delivery_is_sent_again_with_a_new_delivery_id() {
    let s = Server::start().await;
    let rx = Receiver::start(1).await;
    let _ = webhook(&s, &rx, false, None).await;
    s.put("a", b"1").await;
    let got = rx.wait_for(2).await;
    assert_eq!(batch_of(&got[0])["events"], batch_of(&got[1])["events"], "the same batch again");
    assert_ne!(got[0].headers["x-rs3-delivery"], got[1].headers["x-rs3-delivery"]);
    assert_eq!(*rx.fail_first.lock().unwrap(), 0);
}

// ── object versioning (S3 semantics over the wire) ──────────────────────────

#[tokio::test]
async fn s9_versioning_enabled_suspended_and_version_scoped_requests() {
    let s = Server::start().await;
    let admin = s.admin();
    async fn versioning(s: &Server, body: &str) -> Reply {
        s.send(&s.admin(), "PUT", &format!("/{BUCKET}"), "versioning", &[], body.as_bytes()).await
    }
    async fn get_versioning(s: &Server) -> String {
        s.send(&s.admin(), "GET", &format!("/{BUCKET}"), "versioning", &[], b"").await.text()
    }
    async fn get(s: &Server, query: &str) -> Reply {
        s.send(&s.admin(), "GET", &format!("/{BUCKET}/v/k"), query, &[], b"").await
    }

    // Never enabled: no version ids anywhere, no Status in the config.
    let r = s.put("v/k", b"null").await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r.header("x-amz-version-id").is_none(), "no version id before versioning");
    assert!(!get_versioning(&s).await.contains("<Status>"));
    assert!(get(&s, "").await.header("x-amz-version-id").is_none());
    let r = versioning(&s, "<VersioningConfiguration><Status>Bogus</Status></VersioningConfiguration>").await;
    assert_eq!((r.status, r.s3_code()), (StatusCode::BAD_REQUEST, "MalformedXML".to_string()));

    // Enabled: every write is a version, named in the response.
    let r = versioning(&s, "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(get_versioning(&s).await.contains("<Status>Enabled</Status>"));
    let v1 = s.put("v/k", b"one").await.header("x-amz-version-id").expect("a version id").to_string();
    let v2 = s.put("v/k", b"two").await.header("x-amz-version-id").expect("a version id").to_string();
    assert!(v1 < v2 && v1 != "null");
    let r = get(&s, "").await;
    assert_eq!((r.status, r.body.as_slice(), r.header("x-amz-version-id")), (StatusCode::OK, &b"two"[..], Some(v2.as_str())));
    let r = get(&s, &format!("versionId={v1}")).await;
    assert_eq!((r.status, r.body.as_slice(), r.header("x-amz-version-id")), (StatusCode::OK, &b"one"[..], Some(v1.as_str())));
    let r = get(&s, "versionId=null").await;
    assert_eq!((r.status, r.body.as_slice(), r.header("x-amz-version-id")), (StatusCode::OK, &b"null"[..], Some("null")));
    let r = get(&s, "versionId=0000000000000000001.0000000000.0123456789abcdef").await;
    assert_eq!((r.status, r.s3_code()), (StatusCode::NOT_FOUND, "NoSuchVersion".to_string()));
    let r = get(&s, "versionId=bogus").await;
    assert_eq!((r.status, r.s3_code()), (StatusCode::NOT_FOUND, "NoSuchVersion".to_string()));

    // A delete leaves a marker; the versions stay readable by id.
    let r = s.send(&admin, "DELETE", &format!("/{BUCKET}/v/k"), "", &[], b"").await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    assert_eq!(r.header("x-amz-delete-marker"), Some("true"));
    let marker = r.header("x-amz-version-id").expect("the marker's id").to_string();
    assert!(marker > v2);
    let r = get(&s, "").await;
    assert_eq!((r.status, r.header("x-amz-delete-marker"), r.header("x-amz-version-id")), (StatusCode::NOT_FOUND, Some("true"), Some(marker.as_str())));
    let r = get(&s, &format!("versionId={marker}")).await;
    assert_eq!((r.status, r.header("x-amz-delete-marker")), (StatusCode::METHOD_NOT_ALLOWED, Some("true")));
    let r = get(&s, &format!("versionId={v2}")).await;
    assert_eq!((r.status, r.body.as_slice()), (StatusCode::OK, &b"two"[..]));
    let listing = s.send(&admin, "GET", &format!("/{BUCKET}"), "versions&prefix=v/", &[], b"").await.text();
    assert!(listing.contains(&format!("<DeleteMarker><Key>v/k</Key><VersionId>{marker}</VersionId><IsLatest>true</IsLatest>")), "{listing}");
    assert_eq!(listing.matches("<Version>").count(), 3, "{listing}");
    assert!(listing.contains(&format!("<VersionId>{v2}</VersionId><IsLatest>false</IsLatest>")), "{listing}");
    assert!(listing.contains("<VersionId>null</VersionId><IsLatest>false</IsLatest>"), "{listing}");
    assert!(listing.contains("<IsTruncated>false</IsTruncated>"));

    // Removing the marker by its id undeletes the key.
    let r = s.send(&admin, "DELETE", &format!("/{BUCKET}/v/k"), &format!("versionId={marker}"), &[], b"").await;
    assert_eq!((r.status, r.header("x-amz-version-id")), (StatusCode::NO_CONTENT, Some(marker.as_str())));
    let r = get(&s, "").await;
    assert_eq!((r.status, r.body.as_slice(), r.header("x-amz-version-id")), (StatusCode::OK, &b"two"[..], Some(v2.as_str())));
    // Pages of one: every version is reachable through the markers.
    let mut seen = Vec::new();
    let mut query = String::from("versions&prefix=v/&max-keys=1");
    for _ in 0..5 {
        let page = s.send(&admin, "GET", &format!("/{BUCKET}"), &query, &[], b"").await.text();
        let id = page.split("<VersionId>").nth(1).map(|rest| rest.split('<').next().unwrap().to_string()).expect("one version");
        seen.push(id.clone());
        if page.contains("<IsTruncated>false</IsTruncated>") {
            break;
        }
        let key_marker = page.split("<NextKeyMarker>").nth(1).unwrap().split('<').next().unwrap().to_string();
        let version_marker = page.split("<NextVersionIdMarker>").nth(1).unwrap().split('<').next().unwrap().to_string();
        query = format!("versions&prefix=v/&max-keys=1&key-marker={key_marker}&version-id-marker={version_marker}");
    }
    assert_eq!(seen, vec![v2.clone(), v1.clone(), "null".to_string()]);

    // Suspended: a write is the null version and replaces only the null one.
    let r = versioning(&s, "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>").await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(get_versioning(&s).await.contains("<Status>Suspended</Status>"));
    assert_eq!(s.put("v/k", b"three").await.header("x-amz-version-id"), Some("null"));
    let listing = s.send(&admin, "GET", &format!("/{BUCKET}"), "versions&prefix=v/", &[], b"").await.text();
    assert!(listing.contains("<VersionId>null</VersionId><IsLatest>true</IsLatest>"), "{listing}");
    assert_eq!(listing.matches("<Version>").count(), 3, "null, v2, v1: {listing}");
    assert_eq!(get(&s, "").await.body, b"three");
    assert_eq!(get(&s, &format!("versionId={v1}")).await.body, b"one");
    // Off is not a state a bucket can return to.
    let r = versioning(&s, "<VersioningConfiguration></VersioningConfiguration>").await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}
