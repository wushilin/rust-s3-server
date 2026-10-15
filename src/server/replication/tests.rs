//! Replication between real servers: each `Site` is a full rusts3 (store,
//! IAM with SigV4 auth on, changelog service, replication service) listening
//! on 127.0.0.1, and links talk to each other over HTTP exactly as two
//! deployments would.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use super::model::Mode;
use super::*;
use crate::server::config::{AppConfig, ChangelogConfig, Credential, WebhookConfig};
use crate::server::iam::IamStore;
use crate::server::notify::{db::NotifyDb, NotifyService};
use crate::storage::store::{CompletePartRequest, LocalObjectStore};

const AK: &str = "site-root";
const SK: &str = "site-root-secret";

struct Site {
    tmp: tempfile::TempDir,
    name: String,
    store: LocalObjectStore,
    iam: IamStore,
    notify: NotifyService,
    replication: ReplicationService,
    shutdown: CancellationToken,
    endpoint: String,
}

fn fast_timing() -> Timing {
    Timing {
        check_interval: Duration::from_millis(200),
        stale_after: Duration::from_secs(30),
        max_checkpoint_age: Duration::from_secs(14 * 86_400),
        long_poll: Duration::from_secs(1),
        parallelism: 8,
        lease_renew: Duration::from_millis(200),
        fatal_pause: Duration::from_millis(500),
    }
}

impl Site {
    async fn start(name: &str) -> Self {
        Self::start_with(name, fast_timing()).await
    }

    async fn start_with(name: &str, timing: Timing) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        Self::open(tmp, name, timing, "127.0.0.1:0").await
    }

    /// Opens a site on an existing data directory (also: a restart), on
    /// `bind` (a restart keeps its address, as a real deployment does).
    async fn open(tmp: tempfile::TempDir, name: &str, timing: Timing, bind: &str) -> Self {
        // Readable ids in tests: a site id is generated at first start, but
        // whatever site.json records is the id, so seed it.
        let site_file = tmp.path().join("site.json");
        if !site_file.exists() {
            std::fs::write(&site_file, format!(r#"{{"site_id":"{name}","site_name":"{name} (name)"}}"#)).unwrap();
        }
        let store = LocalObjectStore::new(tmp.path());
        store.init_site(None).unwrap();
        let iam = IamStore::open(tmp.path()).await.unwrap();
        let shutdown = CancellationToken::new();
        let notify = NotifyService::start(
            store.clone(),
            NotifyDb::new(iam.admin_db()),
            WebhookConfig::default(),
            ChangelogConfig::default(),
            shutdown.clone(),
        )
        .await
        .unwrap();
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        config.auth.credentials.push(Credential {
            access_key: AK.to_string(),
            secret_key: SK.to_string(),
        });
        let local_auth = {
            let auth = crate::server::auth::AuthState { config: Arc::new(config.clone()), iam: Some(iam.clone()), usage: None };
            LocalAuth(Arc::new(move |key| auth.identity_of_key(key)))
        };
        let replication =
            ReplicationService::start(store.clone(), db::LinkDb::new(iam.admin_db()), local_auth, timing, shutdown.clone())
                .await
                .unwrap();
        let app = crate::server::router_with_notify(
            store.clone(),
            crate::server::auth::AuthState {
                config: Arc::new(config),
                iam: Some(iam.clone()),
                usage: None,
            },
            Arc::new(crate::server::TrafficMetrics::default()),
            crate::server::registry::TaskRegistry::new(),
            Some(notify.clone()),
        );
        let listener = {
            let mut tries = 0;
            loop {
                match tokio::net::TcpListener::bind(bind).await {
                    Ok(listener) => break listener,
                    // The old listener's socket may linger briefly.
                    Err(_) if tries < 50 => {
                        tries += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(err) => panic!("cannot bind {bind}: {err}"),
                }
            }
        };
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let stop = shutdown.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move { stop.cancelled().await })
                .await;
        });
        Self {
            tmp,
            name: name.to_string(),
            store,
            iam,
            notify,
            replication,
            shutdown,
            endpoint,
        }
    }

    /// Stops this site and starts it again on the same data.
    async fn restart(self) -> Self {
        self.stop().await.start().await
    }

    /// Stops this site (its data and address are kept for [`Stopped::start`]).
    async fn stop(self) -> Stopped {
        self.shutdown.cancel();
        // As the real server does: the store's token is the process one, and
        // it ends long polls so the shutdown is not held up.
        self.store.shutdown_token().cancel();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let Site { tmp, name, store, iam, notify, replication, endpoint, .. } = self;
        drop((store, iam, notify, replication));
        tokio::time::sleep(Duration::from_millis(300)).await;
        Stopped { tmp, name, endpoint }
    }

    /// A link on this site pulling `bucket` from `peer`.
    async fn pull_from(&self, peer: &Site, bucket: &str, mode: Mode) -> String {
        self.pull_with(peer, bucket, mode, AK, SK).await
    }

    async fn pull_with(&self, peer: &Site, bucket: &str, mode: Mode, ak: &str, sk: &str) -> String {
        self.link_with(peer, bucket, mode, ak, sk, true).await.unwrap()
    }

    /// Stops this site and starts a different, empty one (site id `name`)
    /// at the same address: the endpoint now reaches another site.
    async fn replace_with_fresh(self, name: &str) -> Self {
        self.shutdown.cancel();
        self.store.shutdown_token().cancel();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let Site { store, iam, notify, replication, endpoint, .. } = self;
        drop((store, iam, notify, replication));
        tokio::time::sleep(Duration::from_millis(300)).await;
        let bind = endpoint.trim_start_matches("http://").to_string();
        Self::open(tempfile::tempdir().unwrap(), name, fast_timing(), &bind).await
    }

    /// A link here with `bucket` on `peer`, with the peer key `ak`/`sk`;
    /// `verify: false` skips the creation-time permission check.
    async fn link_with(&self, peer: &Site, bucket: &str, mode: Mode, ak: &str, sk: &str, verify: bool) -> Result<String, LinkError> {
        self.replication
            .create(
                LinkRequest {
                    name: format!("{}-from-{}", self.name, peer.name),
                    local_bucket: bucket.to_string(),
                    peer_endpoint: peer.endpoint.clone(),
                    peer_bucket: bucket.to_string(),
                    access_key: ak.to_string(),
                    secret_key: sk.to_string(),
                    local_access_key: AK.to_string(),
                    region: None,
                    mode,
                    prefix: None,
                    enabled: true,
                    make_read_only: mode == Mode::Mirror,
                    verify,
                },
                "tester",
            )
            .await
            .map(|view| view.id)
    }

    async fn put(&self, bucket: &str, key: &str, body: &[u8]) {
        self.store.put_object(bucket, key, body, None, None, false).await.unwrap();
    }

    async fn put_meta(&self, bucket: &str, key: &str, body: &[u8], content_type: &str, meta: &[(&str, &str)]) {
        let meta: BTreeMap<String, String> = meta.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let stream = futures::stream::once(futures::future::ready(Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(body))));
        self.store
            .put_object_stream_with_metadata(
                bucket,
                key,
                stream,
                Some(content_type),
                None,
                None,
                None,
                &meta,
                false,
                None,
                None,
                crate::storage::store::WriteOptions::default(),
            )
            .await
            .unwrap();
    }

    async fn multipart(&self, bucket: &str, key: &str, parts: &[Vec<u8>]) -> String {
        let upload = self.store.initiate_multipart(bucket, key, Some("application/x-big"), None).await.unwrap();
        let mut done = Vec::new();
        for (i, part) in parts.iter().enumerate() {
            let number = (i + 1) as u16;
            let put = self.store.put_multipart_part(bucket, key, &upload, number, part, false).await.unwrap();
            done.push(CompletePartRequest { number, etag: put.etag });
        }
        self.store.complete_multipart(bucket, key, &upload, &done).await.unwrap().etag
    }

    /// key → (etag, size, stamp, content type, user metadata, bytes).
    async fn contents(&self, bucket: &str) -> BTreeMap<String, (String, u64, String, String, BTreeMap<String, String>, Vec<u8>)> {
        let mut out = BTreeMap::new();
        let mut after: Option<String> = None;
        loop {
            let page = self.store.list_objects(bucket, "", None, after.as_deref(), 1000).await.unwrap();
            for entry in &page.entries {
                // Replication may delete a key between the listing and the
                // read; it is simply not part of this snapshot.
                let Ok(object) = self.store.read_object(bucket, &entry.object_key).await else {
                    continue;
                };
                let mut bytes = Vec::new();
                for part in &object.meta.parts {
                    bytes.extend(std::fs::read(object.object_dir.join(&part.file)).unwrap());
                }
                let stamp = self
                    .store
                    .effective_stamp(object.meta.stamp.as_ref(), object.meta.last_modified_ms)
                    .to_header();
                out.insert(
                    entry.object_key.clone(),
                    (object.meta.etag.clone(), object.meta.size, stamp, object.meta.content_type.clone(), object.meta.user_meta.clone(), bytes),
                );
            }
            if !page.is_truncated {
                return out;
            }
            after = page.next_after;
        }
    }

    fn link(&self, id: &str) -> LinkView {
        self.replication.view(id).unwrap()
    }
}


/// A stopped site: start it again on the same data and address.
struct Stopped {
    tmp: tempfile::TempDir,
    name: String,
    endpoint: String,
}

impl Stopped {
    async fn start(self) -> Site {
        let bind = self.endpoint.trim_start_matches("http://").to_string();
        Site::open(self.tmp, &self.name, fast_timing(), &bind).await
    }
}

/// A TCP proxy to `target` (`host:port`) that limits both directions to
/// about `bytes_per_sec`: a slow network between two sites.
async fn throttled_proxy(target: String, bytes_per_sec: usize) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn pump(mut from: tokio::net::tcp::OwnedReadHalf, mut to: tokio::net::tcp::OwnedWriteHalf, bytes_per_sec: usize) {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = match from.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            if to.write_all(&buf[..n]).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis((n as u64 * 1000 / bytes_per_sec as u64).max(1))).await;
        }
        let _ = to.shutdown().await;
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let target = target.clone();
            tokio::spawn(async move {
                let Ok(upstream) = tokio::net::TcpStream::connect(&target).await else { return };
                let (cr, cw) = client.into_split();
                let (ur, uw) = upstream.into_split();
                let _ = tokio::join!(tokio::spawn(pump(cr, uw, bytes_per_sec)), tokio::spawn(pump(ur, cw, bytes_per_sec)));
            });
        }
    });
    endpoint
}

async fn eventually<F, Fut>(secs: u64, what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn same(a: &Site, b: &Site, bucket: &str) -> bool {
    a.contents(bucket).await == b.contents(bucket).await
}

const MIB: usize = 5 * 1024 * 1024;

#[tokio::test]
async fn one_way_mirror_copies_everything_exactly_and_follows_changes() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    a.put("data", "single.txt", b"hello").await;
    a.put_meta("data", "meta.json", b"{}", "application/json", &[("owner", "team-a")]).await;
    let etag = a.multipart("data", "big.bin", &[vec![1u8; MIB], vec![2u8; MIB], vec![3u8; 11]]).await;
    // Something only the mirror has: mirror mode removes it.
    b.put("data", "stray.txt", b"not on a").await;

    let id = b.pull_from(&a, "data", Mode::Mirror).await;
    eventually(30, "initial mirror", || same(&a, &b, "data")).await;
    let copy = b.contents("data").await;
    assert_eq!(copy["big.bin"].0, etag, "multipart ETag reproduced");
    assert_eq!(copy["meta.json"].3, "application/json");
    assert_eq!(copy["meta.json"].4["owner"], "team-a");
    assert!(!copy.contains_key("stray.txt"));
    assert_eq!(copy["single.txt"].2, a.contents("data").await["single.txt"].2, "stamps carried, not re-minted");

    // Live changes: create, overwrite, metadata-only update, delete.
    a.put("data", "new.txt", b"new").await;
    a.put("data", "single.txt", b"hello again").await;
    a.store
        .copy_object_with_metadata(
            "data",
            "big.bin",
            "data",
            "big.bin",
            None,
            Some(&BTreeMap::from([("tier".to_string(), "cold".to_string())])),
            None,
            None,
            crate::storage::store::WriteOptions::default(),
        )
        .await
        .unwrap();
    a.store.delete_object("data", "meta.json").await.unwrap();
    eventually(30, "live changes", || same(&a, &b, "data")).await;
    // Counters are bumped just after each commit; give the last one a beat.
    eventually(5, "the tier change counted as metadata-only (no data moved)", || {
        let view = b.link(&id);
        async move { view.status.metadata_only >= 1 }
    })
    .await;
    assert_eq!(b.link(&id).status.state, "live");
    assert_eq!(b.contents("data").await["big.bin"].0, etag);
}

#[tokio::test]
async fn two_way_merge_converges_to_the_union_and_stops() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("shared").await.unwrap();
    }
    a.put("shared", "only-a", b"a").await;
    b.put("shared", "only-b", b"b").await;
    a.put("shared", "both", b"from a").await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    b.put("shared", "both", b"from b, later").await;
    // A delete on a before replication starts, of a key b also has.
    a.put("shared", "gone", b"x").await;
    b.put("shared", "gone", b"x").await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    a.store.delete_object("shared", "gone").await.unwrap();

    // One merge link (on b) syncs both ways.
    let id = b.pull_from(&a, "shared", Mode::Merge).await;
    eventually(30, "initial merge", || same(&a, &b, "shared")).await;
    let merged = a.contents("shared").await;
    assert!(merged.contains_key("only-a") && merged.contains_key("only-b"), "union");
    assert_eq!(merged["both"].5, b"from b, later", "the later write wins on both sides");
    // a deleted its own copy of "gone"; b's copy was a different, newer
    // version a never saw — deletes are version-scoped, so it survives.
    assert_eq!(merged["gone"].5, b"x", "b's unseen version survives a's delete");
    assert_eq!(merged["gone"].2, b.contents("shared").await["gone"].2);
    // Each side logs what replication applied as such, stamped by the site
    // that made the change (the spec's `origin: "replication"`).
    let origin_of = |site: &Site, key: &str| {
        let store = site.store.clone();
        let key = key.to_string();
        async move {
            let index = store.index("shared").await.unwrap();
            let page = index.read_changelog(0, 10_000, usize::MAX).await.unwrap();
            let event = page.events.iter().rev().find(|e| e.key == key).unwrap().clone();
            (event.origin, event.stamp.unwrap().site)
        }
    };
    assert_eq!(origin_of(&a, "only-b").await, (crate::storage::changelog::ChangeOrigin::Replication, "site-b".to_string()));
    assert_eq!(origin_of(&b, "only-a").await, (crate::storage::changelog::ChangeOrigin::Replication, "site-a".to_string()));
    assert_eq!(origin_of(&a, "only-a").await, (crate::storage::changelog::ChangeOrigin::Client, "site-a".to_string()));

    // Writes on both sides, including the same key.
    a.put("shared", "k1", b"a1").await;
    b.put("shared", "k2", b"b2").await;
    a.put("shared", "both", b"a again").await;
    b.store.delete_object("shared", "only-a").await.unwrap();
    // Pushed from b: metadata, and a multipart object with its layout (so
    // the same ETag); then a metadata-only change (no bytes resent).
    b.put_meta("shared", "meta", b"m", "text/x-b", &[("owner", "b")]).await;
    let etag = b.multipart("shared", "big", &[vec![1u8; MIB], vec![2u8; 1000]]).await;
    eventually(30, "two-way changes", || same(&a, &b, "shared")).await;
    let merged = b.contents("shared").await;
    assert_eq!(merged["both"].5, b"a again");
    assert!(!merged.contains_key("only-a"));
    assert_eq!(a.contents("shared").await["big"].0, etag);
    // Counters move just after the peer applied a change: wait until the
    // multipart copy is counted before taking the baseline.
    eventually(10, "the multipart copy is counted", || {
        let view = b.link(&id);
        async move { view.status.push.is_some_and(|p| p.copied_bytes >= (MIB + 1000) as u64) }
    })
    .await;
    let copied = b.link(&id).status.push.unwrap().copied_bytes;
    b.store
        .copy_object_with_metadata("shared", "big", "shared", "big", None, None, Some("text/v2"), None, Default::default())
        .await
        .unwrap();
    eventually(30, "metadata-only push", || same(&a, &b, "shared")).await;
    // The counter moves just after the peer applied it.
    eventually(10, "counted", || {
        let view = b.link(&id);
        async move { view.status.push.is_some_and(|p| p.metadata_only >= 1) }
    })
    .await;
    let push = b.link(&id).status.push.unwrap();
    assert_eq!(push.copied_bytes, copied, "no bytes resent for a metadata change");

    // No echo loop: once converged, the changelogs stop growing.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let heads = |site: &Site| {
        let store = site.store.clone();
        async move { store.index("shared").await.unwrap().changelog_head() }
    };
    let (ha, hb) = (heads(&a).await, heads(&b).await);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!((heads(&a).await, heads(&b).await), (ha, hb), "changes stop bouncing");
}

/// An access key on `site` for a user allowed only `actions` on bucket
/// `data`.
async fn key_with(site: &Site, user: &str, actions: &[&str]) -> (String, String) {
    site.iam.create_user(user, "password123").await.unwrap();
    let key = site.iam.create_access_key(user, &["k".to_string()]).await.unwrap();
    let policy: crate::server::policy::PolicyDocument = serde_json::from_value(serde_json::json!({
        "Statement": [{"Effect": "Allow", "Action": actions, "Resource": ["arn:aws:s3:::data", "arn:aws:s3:::data/*"]}]
    }))
    .unwrap();
    site.iam.set_policy(user, Some(&policy)).await.unwrap();
    (key.access_key, key.secret_key)
}

#[tokio::test]
async fn a_merge_link_needs_write_and_replicate_write_on_the_peer() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    a.put("data", "from-a", b"a").await;
    b.put("data", "from-b", b"b").await;
    // Without s3:ReplicateWrite the link is refused up front, saying so...
    let (ak, sk) = key_with(&a, "no-stamps", &["s3:GetObject", "s3:GetBucketChangelog", "s3:PutObject", "s3:DeleteObject"]).await;
    let refused = b.link_with(&a, "data", Mode::Merge, &ak, &sk, true).await.unwrap_err().to_string();
    assert!(refused.contains("lacks s3:ReplicateWrite"), "{refused}");
    // ...and if permissions shrink later, the pull works and the push
    // reports why not.
    let id = b.link_with(&a, "data", Mode::Merge, &ak, &sk, false).await.unwrap();
    eventually(30, "the push error shows", || {
        let view = b.link(&id);
        async move { view.status.push.and_then(|p| p.last_error).is_some_and(|e| e.contains("ReplicateWrite")) }
    })
    .await;
    eventually(30, "the pull still works", || async { b.contents("data").await.contains_key("from-a") }).await;
    assert!(!a.contents("data").await.contains_key("from-b"));
    // With it, both ways.
    b.replication.delete(&id).await.unwrap();
    let (ak, sk) = key_with(
        &a,
        "merger",
        &["s3:GetObject", "s3:GetBucketChangelog", "s3:PutObject", "s3:DeleteObject", "s3:ReplicateWrite"],
    )
    .await;
    let id = b.pull_with(&a, "data", Mode::Merge, &ak, &sk).await;
    b.store.delete_object("data", "from-a").await.unwrap();
    eventually(30, "merged both ways", || same(&a, &b, "data")).await;
    assert!(!a.contents("data").await.contains_key("from-a"), "the delete was pushed");
    eventually(10, "no error left", || {
        let view = b.link(&id);
        async move { view.status.last_error.is_none() && view.status.push.is_some_and(|p| p.last_error.is_none()) }
    })
    .await;
}

#[tokio::test]
async fn a_delete_of_a_key_absent_here_still_reaches_the_peer() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let id = b.pull_from(&a, "data", Mode::Merge).await;
    eventually(30, "live", || {
        let view = b.link(&id);
        async move { view.status.state == "live" && view.status.push.is_some_and(|p| p.state == "live") }
    })
    .await;
    // Pause, so b does not have a's object yet when a stamped delete (say
    // from an s3:ReplicateWrite client) removes it here.
    b.replication.act(&id, LinkAction::Pause).await.unwrap();
    put_aged(&a, "data", "k", b"v1", 60_000).await;
    let later = crate::storage::stamp::Stamp { ms: crate::storage::time::now_ms(), c: 0, site: "site-b".into() };
    b.store.delete_object_stamped("data", "k", Some(later), None).await.unwrap();
    b.replication.act(&id, LinkAction::Resume).await.unwrap();
    // The delete removes everything up to its stamp, so v1 goes on a too —
    // it must not stay on a while b refuses it forever.
    eventually(30, "deleted on both", || async { !a.contents("data").await.contains_key("k") }).await;
    assert!(!b.contents("data").await.contains_key("k"));
}

#[tokio::test]
async fn a_link_is_bound_to_the_peer_site_and_stops_if_another_answers() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    a.put("data", "from-a", b"a").await;
    b.put("data", "from-b", b"b").await;
    let id = b.pull_from(&a, "data", Mode::Merge).await;
    let view = b.link(&id);
    assert_eq!((view.local_site_id.as_str(), view.peer_site_id.as_str()), ("site-b", "site-a"), "bound at creation");
    eventually(30, "merged", || same(&a, &b, "data")).await;
    let peer_name = b.link(&id).status.peer_site_name;
    assert_eq!(peer_name.as_deref(), Some("site-a (name)"), "the peer's name is shown");

    // The endpoint now reaches a different site (a fresh server): the link
    // stops and says so; nothing moves either way.
    let a = a.replace_with_fresh("site-c").await;
    a.store.create_bucket("data").await.unwrap();
    eventually(30, "stopped on another site answering", || {
        let view = b.link(&id);
        async move {
            view.status.state == "stopped"
                && view.status.site_changed.as_deref().is_some_and(|c| c.contains("from site-a to site-c"))
        }
    })
    .await;
    b.put("data", "while-stopped", b"b2").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(a.contents("data").await.is_empty(), "nothing pushed to the stranger");
    // Accepted: bound to the new site, which gets everything.
    let view = b.replication.act(&id, LinkAction::AcceptSiteIds).await.unwrap();
    assert_eq!(view.peer_site_id, "site-c");
    eventually(30, "converged with the new site", || same(&a, &b, "data")).await;
    assert!(a.contents("data").await.contains_key("from-b"));
    eventually(10, "no error", || {
        let view = b.link(&id);
        async move { view.status.site_changed.is_none() && view.status.last_error.is_none() }
    })
    .await;
}

#[tokio::test]
async fn a_renamed_site_is_known_by_its_new_name_at_once() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let id = b.pull_from(&a, "data", Mode::Merge).await;
    let incoming_name = |site: &Site| site.replication.incoming().first().map(|i| i.site_name.clone());
    eventually(30, "a sees b's link", || async { incoming_name(&a).as_deref() == Some("site-b (name)") }).await;
    // Both rename while the link runs: no restart, no effect on anything
    // but the labels.
    b.store.set_site_name("Tokyo").unwrap();
    a.store.set_site_name("Singapore").unwrap();
    a.put("data", "k", b"v").await;
    eventually(30, "a lists b's link under its new name", || async { incoming_name(&a).as_deref() == Some("Tokyo") }).await;
    eventually(30, "b shows a's new name", || {
        let view = b.link(&id);
        async move { view.status.peer_site_name.as_deref() == Some("Singapore") }
    })
    .await;
    eventually(30, "still replicating", || same(&a, &b, "data")).await;
    let view = b.link(&id);
    assert_eq!((view.local_site_id.as_str(), view.peer_site_id.as_str()), ("site-b", "site-a"), "ids untouched");
    assert!(view.status.site_changed.is_none() && view.status.last_error.is_none());
}

#[tokio::test]
async fn an_unchecked_link_binds_the_peer_site_id_on_first_contact() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let id = b.link_with(&a, "data", Mode::Merge, AK, SK, false).await.unwrap();
    assert_eq!(b.link(&id).peer_site_id, "");
    eventually(30, "bound", || {
        let view = b.link(&id);
        async move { view.peer_site_id == "site-a" }
    })
    .await;
    // The running link holds the bound id (not the unbound copy it started
    // with): another site answering at the endpoint stops it.
    let a = a.replace_with_fresh("site-c").await;
    a.store.create_bucket("data").await.unwrap();
    eventually(30, "stopped on another site", || {
        let view = b.link(&id);
        async move { view.status.site_changed.as_deref().is_some_and(|c| c.contains("from site-a to site-c")) }
    })
    .await;
    // Persisted: still bound after a restart.
    let b = b.restart().await;
    assert_eq!(b.link(&id).peer_site_id, "site-a");
}

#[tokio::test]
async fn a_slow_transfer_of_a_large_object_is_not_mistaken_for_a_hung_worker() {
    // A 2 MB/s link, an 8 MB object each way (4 s per transfer), and a lease
    // of 2 s: the worker renews it while the bytes move, so it is never
    // replaced mid-copy (which would restart the copy forever).
    let a = Site::start("site-a").await;
    let b = Site::start_with("site-b", Timing { stale_after: Duration::from_secs(2), ..fast_timing() }).await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let slow = throttled_proxy(a.endpoint.trim_start_matches("http://").to_string(), 2 * 1024 * 1024).await;
    let id = b
        .replication
        .create(LinkRequest { peer_endpoint: slow, ..request("data", &a, Mode::Merge, "", false) }, "t")
        .await
        .unwrap()
        .id;
    let big = vec![7u8; 8 * 1024 * 1024];
    a.put("data", "big-from-a", &big).await;
    b.put("data", "big-from-b", &big).await;
    // Also a multipart one: the renewal must cover every part.
    a.multipart("data", "parts-from-a", &[vec![1u8; MIB], vec![2u8; MIB]]).await;
    eventually(90, "both large objects crossed the slow link", || same(&a, &b, "data")).await;
    let view = b.link(&id);
    assert_eq!(view.status.restarts, 0, "never replaced while transferring: {:?}", view.status);
    assert!(view.status.last_error.is_none(), "{:?}", view.status);
}

#[tokio::test]
async fn an_idle_link_refreshes_its_checkpoint_instead_of_re_snapshotting() {
    let a = Site::start("site-a").await;
    let b = Site::start_with("site-b", Timing { max_checkpoint_age: Duration::from_secs(3), ..fast_timing() }).await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    a.put("data", "k", b"v").await;
    let id = b.pull_from(&a, "data", Mode::Merge).await;
    eventually(30, "live both ways", || {
        let view = b.link(&id);
        async move { view.status.state == "live" && view.status.push.as_ref().is_some_and(|p| p.state == "live") }
    })
    .await;
    let before = b.link(&id).status;
    let saved = checkpoint_of(&b, "data", &id).await.unwrap().saved_at_ms;
    // Quiet for longer than the retention: nothing to re-walk.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let after = b.link(&id).status;
    assert_eq!((after.skipped, after.push.as_ref().unwrap().skipped), (before.skipped, before.push.as_ref().unwrap().skipped), "no re-snapshot: {after:?}");
    assert_eq!((after.state.as_str(), after.last_error.as_deref()), ("live", None), "{after:?}");
    assert!(checkpoint_of(&b, "data", &id).await.unwrap().saved_at_ms > saved, "the checkpoint was refreshed");
}

#[tokio::test]
async fn a_push_error_clears_once_the_peer_is_back_even_on_a_quiet_bucket() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let id = b.pull_from(&a, "data", Mode::Merge).await;
    eventually(30, "live", || {
        let view = b.link(&id);
        async move { view.status.push.as_ref().is_some_and(|p| p.state == "live") }
    })
    .await;
    let stopped = a.stop().await;
    b.put("data", "during-the-outage", b"x").await;
    eventually(30, "the push reports the outage", || {
        let view = b.link(&id);
        async move { view.status.push.as_ref().is_some_and(|p| p.last_error.is_some()) }
    })
    .await;
    let a = stopped.start().await;
    // No further local writes: the error must still clear.
    eventually(30, "cleared", || {
        let view = b.link(&id);
        async move { view.status.push.as_ref().is_some_and(|p| p.last_error.is_none()) }
    })
    .await;
    eventually(30, "delivered", || same(&a, &b, "data")).await;
}

#[tokio::test]
async fn merge_links_on_both_sites_one_stands_down() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let on_a = a.pull_from(&b, "data", Mode::Merge).await;
    eventually(30, "a's link is live", || {
        let view = a.link(&on_a);
        async move { view.status.state == "live" }
    })
    .await;
    // Creating the second one is refused: a's link already serves the pair.
    let refused = b.link_with(&a, "data", Mode::Merge, AK, SK, true).await.unwrap_err().to_string();
    assert!(refused.contains("already merges") && refused.contains(&on_a), "{refused}");
    // One configured anyway (say, while a was down) stands down at runtime.
    let on_b = b.link_with(&a, "data", Mode::Merge, AK, SK, false).await.unwrap();
    // b has the higher site id: its link stands down, a's keeps both ways.
    eventually(30, "b's link stands down", || {
        let view = b.link(&on_b);
        async move { view.status.state == "stopped" && view.status.last_error.is_some_and(|e| e.contains("stands down")) }
    })
    .await;
    a.put("data", "x", b"from a").await;
    b.put("data", "y", b"from b").await;
    eventually(30, "converged through a's link", || same(&a, &b, "data")).await;
    let view = a.link(&on_a);
    assert_ne!(view.status.state, "stopped", "{:?}", view.status);
    // Each side lists the other's link as incoming.
    let incoming = b.replication.incoming();
    assert!(incoming.iter().any(|i| i.site == "site-a" && i.link_id == on_a && i.mode == "merge"), "{incoming:?}");

    // Still so after both restart (the links resume from checkpoints).
    let a = a.restart().await;
    let b = b.restart().await;
    eventually(30, "b's link stands down again", || {
        let view = b.link(&on_b);
        async move { view.status.state == "stopped" }
    })
    .await;
    a.put("data", "z", b"after restart").await;
    eventually(30, "converged", || same(&a, &b, "data")).await;
    assert_eq!(a.link(&on_a).status.peer_site.as_deref(), Some("site-b"));
}

#[tokio::test]
async fn a_link_is_checked_against_the_peer_before_it_is_created() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    a.store.create_bucket("data").await.unwrap();
    b.store.create_bucket("data").await.unwrap();
    let refused = |r: Result<String, LinkError>| r.unwrap_err().to_string();
    // A wrong secret.
    let msg = refused(b.link_with(&a, "data", Mode::Merge, AK, "wrong", true).await);
    assert!(msg.contains("SignatureDoesNotMatch"), "{msg}");
    // No such bucket on the peer.
    let msg = refused(
        b.replication
            .create(LinkRequest { peer_bucket: "nope".into(), ..request("data", &a, Mode::Merge, "", false) }, "t")
            .await
            .map(|v| v.id),
    );
    assert!(msg.contains("nope"), "{msg}");
    // Nobody there.
    let msg = refused(
        b.replication
            .create(LinkRequest { peer_endpoint: "http://127.0.0.1:9".into(), ..request("data", &a, Mode::Merge, "", false) }, "t")
            .await
            .map(|v| v.id),
    );
    assert!(msg.contains("unreachable"), "{msg}");
    // A mirror only needs to read.
    let (ak, sk) = key_with(&a, "reader", &["s3:GetObject", "s3:GetBucketChangelog"]).await;
    b.store.create_bucket("copy").await.unwrap();
    let mirror = LinkRequest { access_key: ak.clone(), secret_key: sk.clone(), ..request("copy", &a, Mode::Mirror, "", true) };
    b.replication.create(mirror, "t").await.unwrap();
    // A merge also writes: refused with what is missing.
    let msg = refused(b.link_with(&a, "data", Mode::Merge, &ak, &sk, true).await);
    assert!(msg.contains("lacks s3:PutObject, s3:DeleteObject, s3:ReplicateWrite"), "{msg}");
    // The peer being this very server.
    let msg = refused(b.link_with(&b, "data", Mode::Merge, AK, SK, true).await);
    assert!(msg.contains("own site id"), "{msg}");
}

#[tokio::test]
async fn the_local_side_acts_as_the_links_local_key() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    a.put("data", "k", b"v").await;
    let with_local = |key: &str| LinkRequest { local_access_key: key.to_string(), ..request("data", &a, Mode::Merge, "", false) };
    // The local key must exist...
    let msg = b.replication.create(with_local("no-such-key"), "t").await.unwrap_err().to_string();
    assert!(msg.contains("does not exist"), "{msg}");
    // ...and may do here what the link does: write (and for a merge, read).
    let (writer, _) = key_with(&b, "writer", &["s3:PutObject", "s3:DeleteObject", "s3:ReplicateWrite"]).await;
    let msg = b.replication.create(with_local(&writer), "t").await.unwrap_err().to_string();
    assert!(msg.contains("lacks s3:GetObject, s3:GetBucketChangelog") && msg.contains("this site"), "{msg}");
    let (ak, _) = key_with(
        &b,
        "local-replicator",
        &["s3:GetObject", "s3:GetBucketChangelog", "s3:PutObject", "s3:DeleteObject", "s3:ReplicateWrite"],
    )
    .await;
    let id = b.replication.create(with_local(&ak), "t").await.unwrap().id;
    eventually(30, "replicating as the local key", || same(&a, &b, "data")).await;
    // Revoking the key's permission stops the link, with the reason.
    b.iam.set_policy("local-replicator", None).await.unwrap();
    eventually(10, "stopped by the local policy", || {
        let view = b.link(&id);
        async move { view.status.last_error.is_some_and(|e| e.contains("local access key") && e.contains("lacks")) }
    })
    .await;
    a.put("data", "k2", b"v").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!b.contents("data").await.contains_key("k2"));
}

#[tokio::test]
async fn least_privilege_credentials_are_enough_to_pull() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    a.put("data", "k", b"v").await;
    // On a: a user that may only read the bucket and its changelog.
    a.iam.create_user("replicator", "password123").await.unwrap();
    let key = a.iam.create_access_key("replicator", &["b".to_string()]).await.unwrap();
    let policy: crate::server::policy::PolicyDocument = serde_json::from_str(
        r#"{"Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:GetBucketChangelog"],
            "Resource":["arn:aws:s3:::data","arn:aws:s3:::data/*"]}]}"#,
    )
    .unwrap();
    a.iam.set_policy("replicator", Some(&policy)).await.unwrap();
    b.pull_with(&a, "data", Mode::Mirror, &key.access_key, &key.secret_key).await;
    eventually(30, "mirror with a read-only key", || same(&a, &b, "data")).await;

    // A wrong secret: the link reports the error and keeps retrying.
    b.store.create_bucket("data2").await.unwrap();
    let bad = b
        .replication
        .create(
            LinkRequest {
                name: "bad-secret".into(),
                local_bucket: "data2".into(),
                peer_endpoint: a.endpoint.clone(),
                peer_bucket: "data".into(),
                access_key: key.access_key.clone(),
                secret_key: "wrong".into(),
                local_access_key: AK.into(),
                region: None,
                mode: Mode::Mirror,
                prefix: None,
                enabled: true,
                make_read_only: true,
                // A runtime error is the point here.
                verify: false,
            },
            "tester",
        )
        .await
        .unwrap()
        .id;
    eventually(10, "the error is visible", || {
        let view = b.link(&bad);
        async move { view.status.last_error.is_some_and(|e| e.contains("403")) }
    })
    .await;
}

#[tokio::test]
async fn a_crashed_or_hung_worker_is_replaced_by_the_supervisor() {
    let a = Site::start("site-a").await;
    let b = Site::start_with(
        "site-b",
        Timing { stale_after: Duration::from_secs(2), ..fast_timing() },
    )
    .await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let id = b.pull_from(&a, "data", Mode::Mirror).await;
    a.put("data", "k1", b"v").await;
    eventually(30, "first copy", || same(&a, &b, "data")).await;

    // Crash: the worker task dies.
    {
        let rt = b.replication.runtime(&id).unwrap();
        let rt = rt.lock().unwrap();
        rt.worker.as_ref().unwrap().1.abort();
    }
    a.put("data", "k2", b"v").await;
    eventually(10, "restarted after a crash", || same(&a, &b, "data")).await;
    assert!(b.link(&id).status.restarts >= 1);

    // Hang: a peer that accepts connections and never answers.
    let black_hole = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hole = format!("http://{}", black_hole.local_addr().unwrap());
    let _keep = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            if let Ok((socket, _)) = black_hole.accept().await {
                held.push(socket);
            }
        }
    });
    b.store.create_bucket("hung").await.unwrap();
    let hung = b
        .replication
        .create(
            LinkRequest {
                name: "hung".into(),
                // Its own bucket: "data" is a mirror, which allows no other link.
                local_bucket: "hung".into(),
                peer_endpoint: hole,
                peer_bucket: "data".into(),
                access_key: AK.into(),
                secret_key: SK.into(),
                local_access_key: AK.into(),
                region: None,
                mode: Mode::Merge,
                prefix: None,
                enabled: true,
                make_read_only: false,
                // The peer never answers.
                verify: false,
            },
            "tester",
        )
        .await
        .unwrap()
        .id;
    eventually(15, "the silent worker is replaced", || {
        let view = b.link(&hung);
        async move { view.status.restarts >= 1 }
    })
    .await;
}

#[tokio::test]
async fn the_position_lives_with_the_data_and_survives_a_restart() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    for i in 0..20 {
        a.put("data", &format!("k{i}"), b"v").await;
    }
    let id = b.pull_from(&a, "data", Mode::Mirror).await;
    eventually(30, "mirrored", || same(&a, &b, "data")).await;
    // The checkpoint is saved just after a batch applies; wait for it to be
    // live at the peer's head. It is kept in the target bucket's index.
    let head = a.store.index("data").await.unwrap().changelog_head();
    eventually(30, "checkpoint saved at the head", || {
        let store = b.store.clone();
        let id = id.clone();
        async move {
            let Some(bytes) = store.index("data").await.unwrap().get_checkpoint(&id).await.unwrap() else {
                return false;
            };
            let checkpoint: model::Checkpoint = serde_json::from_slice(&bytes).unwrap();
            checkpoint.phase == model::Phase::Live { after: head }
        }
    })
    .await;

    let b = b.restart().await;
    a.put("data", "after-restart", b"v").await;
    eventually(30, "resumed", || same(&a, &b, "data")).await;
    eventually(5, "the copy counted", || {
        let view = b.link(&id);
        async move { view.status.copied >= 1 }
    })
    .await;
    let status = b.link(&id).status;
    // A new snapshot would have walked (and skipped) all 20 objects again.
    assert_eq!((status.copied, status.skipped), (1, 0), "resumed from the checkpoint: {status:?}");

    // An index rebuild carries the checkpoint over.
    b.store.rebuild_index("data").await.unwrap();
    assert!(b.store.index("data").await.unwrap().get_checkpoint(&id).await.unwrap().is_some());
}

#[tokio::test]
async fn falling_behind_the_peers_retention_re_snapshots() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    a.put("data", "k1", b"v").await;
    let id = b.pull_from(&a, "data", Mode::Mirror).await;
    eventually(30, "mirrored", || same(&a, &b, "data")).await;
    b.replication.act(&id, LinkAction::Pause).await.unwrap();
    a.put("data", "k2", b"v").await;
    a.store.delete_object("data", "k1").await.unwrap();
    // a's retention removes those changes before b comes back.
    a.notify.trim_all(crate::storage::time::now_ms() + 1_000).await;
    b.replication.act(&id, LinkAction::Resume).await.unwrap();
    eventually(30, "re-snapshotted to the same state", || same(&a, &b, "data")).await;
}

#[tokio::test]
async fn a_cloned_site_is_refused() {
    let a = Site::start("same-id").await;
    let b = Site::start("same-id").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    // Refused up front...
    let msg = b.link_with(&a, "data", Mode::Mirror, AK, SK, true).await.unwrap_err().to_string();
    assert!(msg.contains("own site id"), "{msg}");
    // ...and at runtime (the copy happened after the link was made).
    let id = b.link_with(&a, "data", Mode::Mirror, AK, SK, false).await.unwrap();
    eventually(10, "refused", || {
        let view = b.link(&id);
        async move { view.status.last_error.is_some_and(|e| e.contains("own site id")) }
    })
    .await;
}

#[tokio::test]
async fn a_target_bucket_cannot_be_deleted_under_a_link() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let id = b.pull_from(&a, "data", Mode::Mirror).await;
    let err = b.store.delete_bucket("data").await.unwrap_err();
    assert!(err.to_string().contains("replication links"), "{err}");
    b.replication.delete(&id).await.unwrap();
    b.store.delete_bucket("data").await.unwrap();
}

// ── restoring lost objects from a peer (health-scan repair) ────────────────

use crate::storage::scan::{Finding, FindingKind, FindingState, RepairAction, ScanProgress};

async fn findings(store: &LocalObjectStore, bucket: &str) -> Vec<Finding> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let collector = tokio::spawn(async move {
        let mut out = Vec::new();
        while let Some(finding) = rx.recv().await {
            out.push(finding);
        }
        out
    });
    store
        .scan_bucket(bucket, &CancellationToken::new(), &ScanProgress::default(), &tx)
        .await
        .unwrap();
    drop(tx);
    collector.await.unwrap()
}

/// Writes `key` as if written `age_ms` ago on this site (the health scan
/// does not judge objects younger than its grace window).
async fn put_aged(site: &Site, bucket: &str, key: &str, body: &[u8], age_ms: i64) {
    let stamp = crate::storage::stamp::Stamp { ms: crate::storage::time::now_ms() - age_ms, c: 0, site: site.name.clone() };
    let stream = futures::stream::once(futures::future::ready(Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(body))));
    site.store
        .put_object_stream_with_metadata(
            bucket,
            key,
            stream,
            Some("text/plain"),
            None,
            None,
            None,
            &BTreeMap::from([("owner".to_string(), "x".to_string())]),
            false,
            None,
            None,
            crate::storage::store::WriteOptions::replicated(stamp),
        )
        .await
        .unwrap();
}

/// Destroys `key`'s bytes on disk, as a failed disk would.
async fn lose(site: &Site, bucket: &str, key: &str) {
    let row = site.store.index(bucket).await.unwrap().get(key).await.unwrap().unwrap();
    std::fs::remove_dir_all(site.tmp.path().join("buckets").join(bucket).join(&row.blob_dir)).unwrap();
    site.store.forget_cached_meta(bucket, key);
}

#[tokio::test]
async fn a_lost_object_is_restored_from_the_peer() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    put_aged(&a, "data", "doc.txt", b"precious", 60_000).await;
    // A multipart object, also old enough to be judged.
    let upload = a.store.initiate_multipart("data", "big.bin", None, None).await.unwrap();
    let mut done = Vec::new();
    for (i, part) in [vec![7u8; MIB], vec![8u8; 9]].iter().enumerate() {
        let number = (i + 1) as u16;
        let put = a.store.put_multipart_part("data", "big.bin", &upload, number, part, false).await.unwrap();
        done.push(CompletePartRequest { number, etag: put.etag });
    }
    let old = crate::storage::stamp::Stamp { ms: crate::storage::time::now_ms() - 60_000, c: 1, site: "site-a".into() };
    let etag = a
        .store
        .complete_multipart_with("data", "big.bin", &upload, &done, crate::storage::store::WriteOptions::replicated(old))
        .await
        .unwrap()
        .etag;
    b.pull_from(&a, "data", Mode::Mirror).await;
    eventually(30, "mirrored", || same(&a, &b, "data")).await;
    let before = b.contents("data").await;

    // b's disk loses both objects.
    lose(&b, "data", "doc.txt").await;
    lose(&b, "data", "big.bin").await;
    let found = findings(&b.store, "data").await;
    let missing: Vec<&Finding> = found.iter().filter(|f| f.kind == FindingKind::MissingBlob).collect();
    assert_eq!(missing.len(), 2, "{found:?}");
    assert_eq!(FindingKind::MissingBlob.actions()[0], RepairAction::RestoreFromPeer, "restore is recommended");
    for finding in missing {
        let (state, message) = b.store.repair_finding(finding, RepairAction::RestoreFromPeer).await;
        assert_eq!(state, FindingState::Repaired, "{message}");
        assert!(message.contains("restored from"), "{message}");
    }
    let after = b.contents("data").await;
    assert_eq!(after, before, "bytes, ETag (incl. multipart), metadata and stamp all back");
    assert_eq!(after["big.bin"].0, etag);
    assert!(findings(&b.store, "data").await.iter().all(|f| f.kind != FindingKind::MissingBlob));
}

#[tokio::test]
async fn a_lost_blob_holds_the_link_with_an_error_never_skipped() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    put_aged(&b, "data", "lost", b"bytes the disk lost", 60_000).await;
    b.put("data", "fine", b"ok").await;
    lose(&b, "data", "lost").await;
    // The index still names the object but its bytes are gone: not a change
    // that moved on (that would be skipped — a later event carries it), but
    // data loss. The push stops at it and says so.
    let id = b.pull_from(&a, "data", Mode::Merge).await;
    eventually(30, "the push reports the lost object", || {
        let view = b.link(&id);
        async move { view.status.push.and_then(|p| p.last_error).is_some_and(|e| e.contains("data/lost")) }
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!a.contents("data").await.contains_key("lost"));
    let push = b.link(&id).status.push.unwrap();
    assert_eq!(push.state, "error", "{push:?}");
    assert!(push.last_error.is_some_and(|e| e.contains("data/lost")), "still held at it");
    // Resolved (here: the key is deleted; a restore from a peer that has it
    // works too): the push carries on.
    b.store.delete_object("data", "lost").await.unwrap();
    eventually(30, "moving again", || same(&a, &b, "data")).await;
}

#[tokio::test]
async fn restoring_never_rolls_back_silently() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    put_aged(&a, "data", "k", b"v1", 60_000).await;
    // A key that may read a but not write it: b's changes never reach a.
    let (ak, sk) = key_with(&a, "reader", &["s3:GetObject", "s3:GetBucketChangelog"]).await;
    b.link_with(&a, "data", Mode::Merge, &ak, &sk, false).await.unwrap();
    eventually(30, "merged", || same(&a, &b, "data")).await;
    // b writes a newer version that a never gets.
    put_aged(&b, "data", "k", b"v2 only on b", 30_000).await;
    lose(&b, "data", "k").await;
    let finding = findings(&b.store, "data")
        .await
        .into_iter()
        .find(|f| f.kind == FindingKind::MissingBlob)
        .unwrap();

    // a only has the older v1: refused, with the reason.
    let (state, message) = b.store.repair_finding(&finding, RepairAction::RestoreFromPeer).await;
    assert_eq!(state, FindingState::Failed);
    assert!(message.contains("older version"), "{message}");
    // Explicitly accepting the rollback restores v1.
    let (state, message) = b.store.repair_finding(&finding, RepairAction::RestoreOlderFromPeer).await;
    assert_eq!(state, FindingState::Repaired, "{message}");
    assert_eq!(b.contents("data").await["k"].5, b"v1");

    // A bucket nothing replicates into has no peer to restore from.
    b.store.create_bucket("local-only").await.unwrap();
    put_aged(&b, "local-only", "x", b"x", 60_000).await;
    lose(&b, "local-only", "x").await;
    let finding = findings(&b.store, "local-only")
        .await
        .into_iter()
        .find(|f| f.kind == FindingKind::MissingBlob)
        .unwrap();
    let (state, message) = b.store.repair_finding(&finding, RepairAction::RestoreFromPeer).await;
    assert_eq!(state, FindingState::Failed);
    assert!(message.contains("no enabled replication link"), "{message}");
}

// ── chaos: everything at once ───────────────────────────────────────────────

/// A tiny deterministic generator (xorshift) so failures reproduce.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Random puts, deletes, metadata-only updates and the odd multipart upload
/// on a shared key space. `skew_ms` stamps writes as a site whose clock is
/// off by that much would.
async fn writer(store: LocalObjectStore, site: String, seed: u64, ops: usize, skew_ms: i64) {
    let mut rng = Rng(seed);
    for i in 0..ops {
        let key = format!("k{}", rng.below(12));
        let stamp = (skew_ms != 0).then(|| crate::storage::stamp::Stamp {
            ms: crate::storage::time::now_ms() + skew_ms,
            c: 0,
            site: site.clone(),
        });
        match rng.below(10) {
            0..=4 => {
                let body = format!("{site}-{i}-{}", rng.next()).into_bytes();
                let stream = futures::stream::once(futures::future::ready(Ok::<_, std::io::Error>(bytes::Bytes::from(body))));
                let _ = store
                    .put_object_stream_with_metadata(
                        "chaos",
                        &key,
                        stream,
                        None,
                        None,
                        None,
                        None,
                        &BTreeMap::new(),
                        false,
                        None,
                        None,
                        crate::storage::store::WriteOptions::stamped(stamp),
                    )
                    .await;
            }
            5..=6 => {
                // A skewed writer's delete removes what it currently sees.
                let removed = store.key_rank("chaos", &key).await.ok().flatten().filter(|r| !r.deleted).map(|r| r.stamp);
                let _ = store.delete_object_stamped("chaos", &key, stamp, removed).await;
            }
            7..=8 => {
                let meta = BTreeMap::from([("rev".to_string(), format!("{site}-{i}"))]);
                let _ = store
                    .copy_object_with_metadata(
                        "chaos",
                        &key,
                        "chaos",
                        &key,
                        None,
                        Some(&meta),
                        None,
                        None,
                        crate::storage::store::WriteOptions::stamped(stamp),
                    )
                    .await;
            }
            _ if i % 40 == 9 => {
                // Occasionally a real multipart object.
                if let Ok(upload) = store.initiate_multipart("chaos", &key, None, None).await {
                    let mut done = Vec::new();
                    for (n, part) in [vec![i as u8; MIB], vec![1u8; 33]].iter().enumerate() {
                        let number = (n + 1) as u16;
                        if let Ok(put) = store.put_multipart_part("chaos", &key, &upload, number, part, false).await {
                            done.push(CompletePartRequest { number, etag: put.etag });
                        }
                    }
                    let options = crate::storage::store::WriteOptions::stamped(stamp);
                    if store.complete_multipart_with("chaos", &key, &upload, &done, options).await.is_err() {
                        let _ = store.abort_multipart("chaos", &key, &upload).await;
                    }
                }
            }
            _ => {}
        }
        if i % 7 == 0 {
            tokio::time::sleep(Duration::from_millis(rng.below(15))).await;
        }
    }
}

#[tokio::test]
async fn two_way_chaos_converges_exactly() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("chaos").await.unwrap();
    }
    let id_b = b.pull_from(&a, "chaos", Mode::Merge).await;

    // Two writers on each site, one of b's with a clock 1.5s fast, all on
    // the same twelve keys, while replication runs both ways.
    let writers = vec![
        tokio::spawn(writer(a.store.clone(), "site-a".into(), 1, 150, 0)),
        tokio::spawn(writer(a.store.clone(), "site-a".into(), 2, 150, 0)),
        tokio::spawn(writer(b.store.clone(), "site-b".into(), 3, 150, 0)),
        tokio::spawn(writer(b.store.clone(), "site-b".into(), 4, 150, 1_500)),
    ];
    // Mid-run: b's index is rebuilt (its changelog and checkpoints carry over).
    tokio::time::sleep(Duration::from_millis(800)).await;
    b.store.rebuild_index("chaos").await.unwrap();
    for w in writers {
        w.await.unwrap();
    }
    // And b restarts.
    let b = b.restart().await;

    let deadline = Instant::now() + Duration::from_secs(60);
    while !same(&a, &b, "chaos").await {
        if Instant::now() > deadline {
            let (ca, cb) = (a.contents("chaos").await, b.contents("chaos").await);
            let keys: std::collections::BTreeSet<&String> = ca.keys().chain(cb.keys()).collect();
            for key in keys {
                let (x, y) = (ca.get(key), cb.get(key));
                if x != y {
                    let fmt = |v: Option<&(String, u64, String, String, BTreeMap<String, String>, Vec<u8>)>| {
                        v.map(|v| format!("etag={} size={} stamp={} meta={:?}", v.0, v.1, v.2, v.4)).unwrap_or("absent".into())
                    };
                    let ta = a.store.index("chaos").await.unwrap().get_tombstone(key).await.unwrap().map(|t| t.stamp.to_header());
                    let tb = b.store.index("chaos").await.unwrap().get_tombstone(key).await.unwrap().map(|t| t.stamp.to_header());
                    eprintln!("DIFF {key}\n  a: {} tomb={ta:?}\n  b: {} tomb={tb:?}", fmt(x), fmt(y));
                    for site in [&a, &b] {
                        let index = site.store.index("chaos").await.unwrap();
                        let page = index.read_changelog(0, 100_000, usize::MAX).await.unwrap();
                        for e in page.events.iter().filter(|e| &e.key == key) {
                            eprintln!(
                                "  {} seq={} {:?} origin={:?} stamp={:?} removed={:?} ts={}",
                                site.name,
                                e.seq,
                                e.op,
                                e.origin,
                                e.stamp.as_ref().map(|s| s.to_header()),
                                e.removed_stamp.as_ref().map(|s| s.to_header()),
                                e.ts_ms
                            );
                        }
                    }
                }
            }
            for view in a.replication.list().into_iter().chain(b.replication.list()) {
                eprintln!("LINK {} {:?}", view.name, view.status);
            }
            panic!("sites did not converge");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let final_state = a.contents("chaos").await;
    assert!(!final_state.is_empty());
    // Every surviving key on both sides carries exactly the same stamp, ETag,
    // metadata and bytes: that is what `same` compares.
    // Quiet afterwards: no change bounces between the sites.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let head = |site: &Site| {
        let store = site.store.clone();
        async move { store.index("chaos").await.unwrap().changelog_head() }
    };
    let before = (head(&a).await, head(&b).await);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!((head(&a).await, head(&b).await), before, "no ping-pong");
    let status = b.link(&id_b).status;
    assert_eq!(status.state, "live", "{status:?}");
    assert_eq!(status.push.as_ref().unwrap().state, "live", "{status:?}");
}

// ── parallel batches ───────────────────────────────────────────────────────

async fn checkpoint_of(site: &Site, bucket: &str, id: &str) -> Option<model::Checkpoint> {
    let bytes = site.store.index(bucket).await.unwrap().get_checkpoint(id).await.unwrap()?;
    Some(serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn a_batch_applies_only_the_latest_change_per_key() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let id = b.pull_from(&a, "data", Mode::Mirror).await;
    eventually(30, "live", || {
        let view = b.link(&id);
        async move { view.status.state == "live" }
    })
    .await;
    b.replication.act(&id, LinkAction::Pause).await.unwrap();
    for i in 0..5 {
        a.put("data", "hot", format!("version {i}").as_bytes()).await;
    }
    for i in 0..30 {
        a.put("data", &format!("cold-{i:02}"), b"x").await;
    }
    b.replication.act(&id, LinkAction::Resume).await.unwrap();
    eventually(30, "caught up", || same(&a, &b, "data")).await;
    eventually(5, "all copies counted", || {
        let view = b.link(&id);
        async move { view.status.copied >= 31 }
    })
    .await;
    let status = b.link(&id).status;
    assert_eq!(b.contents("data").await["hot"].5, b"version 4");
    assert_eq!(status.copied, 31, "the hot key fetched once, not five times: {status:?}");
    assert_eq!(status.collapsed, 4, "{status:?}");
}

#[tokio::test]
async fn a_failed_change_holds_the_checkpoint_until_it_succeeds() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let id = b.pull_from(&a, "data", Mode::Mirror).await;
    eventually(30, "live", || {
        let view = b.link(&id);
        async move { view.status.state == "live" }
    })
    .await;
    let start = a.store.index("data").await.unwrap().changelog_head();
    eventually(10, "checkpoint at the head", || {
        let (b, id) = (&b, id.clone());
        async move { checkpoint_of(b, "data", &id).await.map(|c| c.phase) == Some(model::Phase::Live { after: start }) }
    })
    .await;

    // One batch: nine good objects and one whose bytes the peer has lost
    // (its reads fail with 500 until it is rewritten).
    b.replication.act(&id, LinkAction::Pause).await.unwrap();
    for i in 0..9 {
        a.put("data", &format!("good-{i}"), b"x").await;
    }
    a.put("data", "broken", b"x").await;
    let row = a.store.index("data").await.unwrap().get("broken").await.unwrap().unwrap();
    std::fs::remove_dir_all(a.tmp.path().join("buckets").join("data").join(&row.blob_dir)).unwrap();
    a.store.forget_cached_meta("data", "broken");
    b.replication.act(&id, LinkAction::Resume).await.unwrap();

    eventually(30, "the good ones applied", || {
        let b = &b;
        async move { b.contents("data").await.len() == 9 }
    })
    .await;
    // The batch failed as a whole: the checkpoint did not move.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let held = checkpoint_of(&b, "data", &id).await.unwrap();
    assert_eq!(held.phase, model::Phase::Live { after: start }, "watermark held at the batch start");
    assert!(b.link(&id).status.last_error.is_some());

    // The peer repairs the object: the batch replays (the good ones are now
    // no-ops) and the checkpoint advances past it.
    a.put("data", "broken", b"fixed").await;
    eventually(30, "converged", || same(&a, &b, "data")).await;
    let head = a.store.index("data").await.unwrap().changelog_head();
    eventually(10, "checkpoint advanced", || {
        let (b, id) = (&b, id.clone());
        async move { checkpoint_of(b, "data", &id).await.map(|c| c.phase) == Some(model::Phase::Live { after: head }) }
    })
    .await;
}

#[tokio::test]
async fn a_change_superseded_before_the_fetch_is_skipped_never_mixed() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    // The event for v1 (a multipart object with metadata)...
    let upload = a.store.initiate_multipart_with_metadata("data", "k", Some("text/v1"), None, None, None, &BTreeMap::new()).await.unwrap();
    let mut parts = Vec::new();
    for (i, part) in [vec![1u8; MIB], vec![2u8; 5]].iter().enumerate() {
        let n = (i + 1) as u16;
        parts.push(CompletePartRequest { number: n, etag: a.store.put_multipart_part("data", "k", &upload, n, part, false).await.unwrap().etag });
    }
    a.store.complete_multipart("data", "k", &upload, &parts).await.unwrap();
    let v1 = crate::server::notify::read::read(
        &a.store,
        "data",
        &crate::server::notify::read::ReadParams { limit: 10, ..Default::default() },
    )
    .await
    .unwrap();
    // ...then, before anyone fetches it, k changes: first its metadata only,
    // then its bytes and layout entirely.
    a.store
        .copy_object_with_metadata("data", "k", "data", "k", None, None, Some("text/v2"), None, Default::default())
        .await
        .unwrap();

    let worker = |b: &Site| worker::Worker {
        store: b.store.clone(),
        link: model::Link {
            v: 1,
            id: "manual".into(),
            name: "manual".into(),
            local_bucket: "data".into(),
            peer_endpoint: a.endpoint.clone(),
            peer_bucket: "data".into(),
            access_key: AK.into(),
            secret_key: SK.into(),
            local_access_key: AK.into(),
            local_site_id: String::new(),
            peer_site_id: String::new(),
            region: "us-east-1".into(),
            mode: Mode::Mirror,
            enabled: true,
            created_at_ms: 0,
            created_by: String::new(),
        },
        peer: peer::PeerClient::new(peer::PeerConfig {
            endpoint: a.endpoint.clone(),
            bucket: "data".into(),
            access_key: AK.into(),
            secret_key: SK.into(),
            region: "us-east-1".into(),
            replicator: None,
        })
        .unwrap(),
        status: Arc::new(std::sync::Mutex::new(model::LinkStatus::default())),
        heartbeat: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        cancel: CancellationToken::new(),
        max_checkpoint_age_ms: i64::MAX,
        long_poll: Duration::from_secs(1),
        parallelism: 4,
        local_check: Arc::new(|| Ok(())),
        bind_peer: Arc::new(|_| {}),
        lease_renew: Duration::from_secs(5),
        fatal_pause: Duration::from_secs(30),
    };
    let w = worker(&b);
    // Applying the stale v1 event: the peer answers 412 to the pinned reads,
    // so nothing at all is written — no v1 bytes with v2 metadata, no
    // partial multipart.
    assert!(w.apply_all(&v1.events, &v1.site_id).await.is_ok());
    assert!(b.contents("data").await.is_empty(), "nothing applied from a superseded event");
    assert_eq!(w.status.lock().unwrap().skipped, 1);
    assert!(
        b.store.list_multipart_uploads("data").await.unwrap().is_empty(),
        "no half-copied multipart upload left behind"
    );
    // The later event carries the current state, applied whole.
    let v2 = crate::server::notify::read::read(
        &a.store,
        "data",
        &crate::server::notify::read::ReadParams { after: v1.next, limit: 10, ..Default::default() },
    )
    .await
    .unwrap();
    w.apply_all(&v2.events, &v2.site_id).await.unwrap();
    assert!(same(&a, &b, "data").await);
    assert_eq!(b.contents("data").await["k"].3, "text/v2");
}

#[tokio::test]
async fn a_mirror_into_a_read_only_bucket() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    a.put("data", "one", b"1").await;
    b.put("data", "stray", b"only on b").await;
    b.store.set_bucket_read_only("data", true).await.unwrap();
    b.pull_from(&a, "data", Mode::Mirror).await;
    // Replication writes, including the sweep of b's stray object.
    eventually(30, "mirrored", || same(&a, &b, "data")).await;
    assert!(!b.contents("data").await.contains_key("stray"));
    // Clients cannot write the replica...
    assert!(b.store.put_object("data", "local", b"x", None, None, false).await.is_err());
    assert!(b.store.delete_object("data", "one").await.is_err());
    // ...so it stays exactly the remote: remote wins.
    a.put("data", "two", b"2").await;
    a.store.delete_object("data", "one").await.unwrap();
    eventually(30, "following", || same(&a, &b, "data")).await;
}

// ── link integrity rules ───────────────────────────────────────────────────

fn request(bucket: &str, peer: &Site, mode: Mode, prefix: &str, make_read_only: bool) -> LinkRequest {
    LinkRequest {
        name: format!("{bucket}-{mode:?}"),
        local_bucket: bucket.into(),
        peer_endpoint: peer.endpoint.clone(),
        peer_bucket: "data".into(),
        access_key: AK.into(),
        secret_key: SK.into(),
        local_access_key: AK.into(),
        region: None,
        mode,
        prefix: (!prefix.is_empty()).then(|| prefix.to_string()),
        enabled: true,
        make_read_only,
        verify: true,
    }
}

#[tokio::test]
async fn mirror_targets_are_read_only_exclusive_and_whole_bucket() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    a.store.create_bucket("data").await.unwrap();
    for bucket in ["mirror-one", "mirror-two", "shared"] {
        b.store.create_bucket(bucket).await.unwrap();
    }
    let err = |r: Result<LinkView, LinkError>| r.err().map(|e| e.to_string()).unwrap_or_default();

    // A mirror needs a read-only target...
    let refused = err(b.replication.create(request("mirror-one", &a, Mode::Mirror, "", false), "t").await);
    assert!(refused.contains("read-only target"), "{refused}");
    assert!(!b.store.bucket_meta("mirror-one").await.unwrap().read_only);
    // ...which creating it can arrange.
    let mirror = b.replication.create(request("mirror-one", &a, Mode::Mirror, "", true), "t").await.unwrap().id;
    assert!(b.store.bucket_meta("mirror-one").await.unwrap().read_only);
    // A mirror replicates the whole bucket.
    let refused = err(b.replication.create(request("mirror-two", &a, Mode::Mirror, "photos/", true), "t").await);
    assert!(refused.contains("whole bucket"), "{refused}");
    // A mirror is its bucket's only link, either way round.
    let refused = err(b.replication.create(request("mirror-one", &a, Mode::Merge, "", false), "t").await);
    assert!(refused.contains("only link"), "{refused}");
    b.replication.create(request("shared", &a, Mode::Merge, "", false), "t").await.unwrap();
    let refused = err(b.replication.create(request("shared", &a, Mode::Mirror, "", true), "t").await);
    assert!(refused.contains("only link"), "{refused}");
    assert!(!b.store.bucket_meta("shared").await.unwrap().read_only, "a refused mirror changes nothing");

    // The replica cannot be made writable while mirrored — paused or not.
    b.replication.act(&mirror, LinkAction::Pause).await.unwrap();
    let refused = b.store.set_bucket_read_only("mirror-one", false).await.unwrap_err().to_string();
    assert!(refused.contains("mirror replica"), "{refused}");
    // Deleting the link leaves it read-only; making it writable is then an
    // explicit step.
    b.replication.delete(&mirror).await.unwrap();
    assert!(b.store.bucket_meta("mirror-one").await.unwrap().read_only);
    b.store.set_bucket_read_only("mirror-one", false).await.unwrap();
}

#[tokio::test]
async fn links_replicate_whole_buckets_only() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    a.store.create_bucket("data").await.unwrap();
    b.store.create_bucket("data").await.unwrap();
    let refused = b.replication.create(request("data", &a, Mode::Merge, "shared/", false), "t").await.unwrap_err();
    assert!(refused.to_string().contains("whole bucket"), "{refused}");
    // One link per bucket pair.
    b.replication.create(request("data", &a, Mode::Merge, "", false), "t").await.unwrap();
    let refused = b.replication.create(request("data", &a, Mode::Merge, "", false), "t").await.unwrap_err();
    assert!(refused.to_string().contains("already linked"), "{refused}");
}

#[tokio::test]
async fn the_last_error_clears_once_the_link_works_again() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    a.store.create_bucket("data").await.unwrap();
    b.store.create_bucket("data").await.unwrap();
    a.put("data", "k", b"v").await;
    let id = b
        .replication
        .create(LinkRequest { secret_key: "wrong".into(), verify: false, ..request("data", &a, Mode::Merge, "", false) }, "t")
        .await
        .unwrap()
        .id;
    eventually(10, "the error shows", || {
        let view = b.link(&id);
        async move { view.status.last_error.is_some_and(|e| e.contains("403")) }
    })
    .await;
    // Fixed: the error goes away with the first successful step.
    b.replication
        .update(&id, LinkUpdate { secret_key: Some(SK.into()), ..LinkUpdate::default() })
        .await
        .unwrap();
    eventually(30, "replicating", || same(&a, &b, "data")).await;
    eventually(10, "the error is cleared", || {
        let view = b.link(&id);
        async move { view.status.last_error.is_none() && view.status.last_error_ms == 0 }
    })
    .await;
}

#[tokio::test]
async fn the_error_clears_promptly_even_with_a_long_poll() {
    let a = Site::start("site-a").await;
    // Production-like long poll: 20s.
    let b = Site::start_with("site-b", Timing { long_poll: Duration::from_secs(20), ..fast_timing() }).await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    let id = b.pull_from(&a, "data", Mode::Merge).await;
    eventually(30, "live", || {
        let view = b.link(&id);
        async move { view.status.state == "live" }
    })
    .await;
    // The peer goes away (its long poll is cut) and comes back.
    let a = a.restart().await;
    eventually(30, "the outage is reported", || {
        let view = b.link(&id);
        async move { view.status.last_error.is_some() }
    })
    .await;
    let back = Instant::now();
    eventually(15, "cleared once the peer answers", || {
        let view = b.link(&id);
        async move { view.status.last_error.is_none() }
    })
    .await;
    assert!(back.elapsed() < Duration::from_secs(10), "not held for a full long poll: {:?}", back.elapsed());
    a.put("data", "after", b"v").await;
    eventually(30, "replicating", || same(&a, &b, "data")).await;
}


// ── version-aware replication ───────────────────────────────────────────────
// A version is immutable and named by its stamp on every site, so two
// versioning-enabled sites hold the same set of versions: history is a
// union, last-writer-wins only decides which one is current.

/// `(version id, is current, is delete marker, etag)` of every version of
/// `key`, newest first.
async fn versions_of(site: &Site, bucket: &str, key: &str) -> Vec<(String, bool, bool, String)> {
    site.store
        .list_versions(bucket, key)
        .await
        .unwrap()
        .into_iter()
        .map(|v| (v.version_id(), v.is_current, v.record.delete_marker, v.record.etag.clone()))
        .collect()
}

async fn same_versions(a: &Site, b: &Site, bucket: &str, key: &str) -> bool {
    versions_of(a, bucket, key).await == versions_of(b, bucket, key).await
}

#[tokio::test]
async fn versioned_sites_hold_the_same_versions_through_snapshot_bursts_and_removals() {
    use crate::storage::metadata::BucketVersioning;
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    // History before the link exists: a null version, then versioning, a
    // burst of versions, a delete marker, and a write on top.
    a.put("data", "k", b"null").await;
    a.store.set_bucket_versioning("data", BucketVersioning::Enabled).await.unwrap();
    for n in 0..5 {
        a.put("data", "k", format!("v{n}").as_bytes()).await;
    }
    a.store.delete_object("data", "k").await.unwrap();
    a.put("data", "k", b"after").await;
    let on_a = versions_of(&a, "data", "k").await;
    assert_eq!(on_a.len(), 8);
    assert!(on_a.iter().any(|v| v.0 == "null"), "the null version is history");
    assert_eq!(on_a.iter().filter(|v| v.2).count(), 1, "one marker");

    // The snapshot carries every version, the null one as null.
    b.store.set_bucket_versioning("data", BucketVersioning::Enabled).await.unwrap();
    let link = b.pull_from(&a, "data", Mode::Mirror).await;
    eventually(30, "every version on b", || same_versions(&a, &b, "data", "k")).await;
    assert_eq!(versions_of(&b, "data", "k").await, on_a);
    assert!(same(&a, &b, "data").await);
    for v in &on_a {
        if !v.2 {
            let (ra, rb) = (
                a.store.read_object_version("data", "k", &v.0).await.unwrap(),
                b.store.read_object_version("data", "k", &v.0).await.unwrap(),
            );
            assert_eq!((ra.meta.etag, ra.meta.size, ra.meta.stamp), (rb.meta.etag, rb.meta.size, rb.meta.stamp), "{}", v.0);
        }
    }

    // A live burst: nothing is collapsed on the way.
    for n in 0..5 {
        a.put("data", "k", format!("w{n}").as_bytes()).await;
    }
    eventually(30, "the burst on b", || async { versions_of(&b, "data", "k").await.len() == 13 }).await;
    assert!(same_versions(&a, &b, "data", "k").await);

    // Removing versions on a removes them on b: a noncurrent one, the
    // current one (the next newest takes over on both), and a marker on
    // top of it all (undelete).
    let victim = versions_of(&a, "data", "k").await[6].0.clone();
    a.store.delete_object_version("data", "k", &victim, crate::storage::write::WriteOrigin::Client).await.unwrap();
    let current = versions_of(&a, "data", "k").await[0].0.clone();
    a.store.delete_object_version("data", "k", &current, crate::storage::write::WriteOrigin::Client).await.unwrap();
    eventually(30, "removals on b", || same_versions(&a, &b, "data", "k")).await;
    let on_b = versions_of(&b, "data", "k").await;
    assert_eq!(on_b.len(), 11);
    assert!(on_b.iter().all(|v| v.0 != victim && v.0 != current));
    assert_eq!(b.store.read_object("data", "k").await.unwrap().meta.etag, a.store.read_object("data", "k").await.unwrap().meta.etag);
    // A copy of a removed version arriving later is refused: b remembers it.
    let victim_stamp = crate::storage::layouts::v3::parse_stamp_dir(&victim).unwrap();
    assert!(b.store.version_known("data", "k", &victim_stamp).await.unwrap(), "a version tombstone");
    a.store.delete_object("data", "k").await.unwrap();
    eventually(30, "the marker on b", || async { b.store.read_object("data", "k").await.is_err() && same_versions(&a, &b, "data", "k").await }).await;
    let marker = versions_of(&a, "data", "k").await[0].0.clone();
    a.store.delete_object_version("data", "k", &marker, crate::storage::write::WriteOrigin::Client).await.unwrap();
    eventually(30, "undeleted on b", || async { b.store.read_object("data", "k").await.is_ok() && same_versions(&a, &b, "data", "k").await }).await;
    assert!(b.link(&link).status.last_error.is_none());
}

#[tokio::test]
async fn a_merge_link_pushes_every_version_and_removal_to_a_versioned_peer() {
    use crate::storage::metadata::BucketVersioning;
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
        site.store.set_bucket_versioning("data", BucketVersioning::Enabled).await.unwrap();
    }
    // Versions on b before the link, and a burst after it: all pushed.
    for n in 0..3 {
        b.put("data", "k", format!("b{n}").as_bytes()).await;
    }
    let link = b.pull_from(&a, "data", Mode::Merge).await;
    for n in 3..6 {
        b.put("data", "k", format!("b{n}").as_bytes()).await;
    }
    eventually(30, "every version on a", || async { versions_of(&a, "data", "k").await.len() == 6 && same_versions(&a, &b, "data", "k").await }).await;
    // Both sides write: the union, with the same current everywhere.
    a.put("data", "k", b"a0").await;
    b.put("data", "k", b"b6").await;
    eventually(30, "the union", || async { versions_of(&a, "data", "k").await.len() == 8 && same_versions(&a, &b, "data", "k").await }).await;
    // A removal on b reaches a.
    let victim = versions_of(&b, "data", "k").await[4].0.clone();
    b.store.delete_object_version("data", "k", &victim, crate::storage::write::WriteOrigin::Client).await.unwrap();
    eventually(30, "the removal on a", || same_versions(&a, &b, "data", "k")).await;
    assert_eq!(versions_of(&a, "data", "k").await.len(), 7);
    assert!(b.link(&link).status.last_error.is_none());
}

#[tokio::test]
async fn a_link_needs_the_same_versioning_on_both_buckets() {
    use crate::storage::metadata::BucketVersioning;
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    a.store.set_bucket_versioning("data", BucketVersioning::Enabled).await.unwrap();
    // Refused at creation, with the reason.
    let err = b.link_with(&a, "data", Mode::Mirror, AK, SK, true).await.unwrap_err();
    assert!(err.to_string().contains("versioning differs"), "{err}");
    // An unchecked link (made while the peer was unreachable, say) stops at
    // its first page instead of replicating.
    let link = b.link_with(&a, "data", Mode::Mirror, AK, SK, false).await.unwrap();
    a.put("data", "k", b"x").await;
    eventually(30, "stopped", || async { b.link(&link).status.state == "stopped" }).await;
    assert!(b.link(&link).status.last_error.as_deref().is_some_and(|e| e.contains("versioning differs")), "{:?}", b.link(&link).status.last_error);
    assert!(b.store.read_object("data", "k").await.is_err(), "nothing replicated");
    // The same setting on both sides: the link resumes on its own.
    b.store.set_bucket_versioning("data", BucketVersioning::Enabled).await.unwrap();
    eventually(30, "mirrored", || same(&a, &b, "data")).await;
    eventually(30, "live", || async { b.link(&link).status.state == "live" }).await;
}

#[tokio::test]
async fn a_versioning_change_stops_a_merge_link_both_ways_until_both_match_then_it_continues() {
    use crate::storage::metadata::BucketVersioning;
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    a.put("data", "from-a", b"a").await;
    b.put("data", "from-b", b"b").await;
    let link = b.pull_from(&a, "data", Mode::Merge).await;
    eventually(30, "merged", || same(&a, &b, "data")).await;

    // Versioning turned on at a only: pull and push both stop at once,
    // and nothing crosses meanwhile.
    a.store.set_bucket_versioning("data", BucketVersioning::Enabled).await.unwrap();
    a.put("data", "while-apart-a", b"a2").await;
    b.put("data", "while-apart-b", b"b2").await;
    // The pull side sees the peer's setting on its next page and stops the
    // link; the push side checks on its next batch and backs off. Nothing
    // crosses either way meanwhile.
    eventually(30, "the link stopped", || async { b.link(&link).status.state == "stopped" }).await;
    assert!(b.link(&link).status.last_error.as_deref().is_some_and(|e| e.contains("versioning differs")), "{:?}", b.link(&link).status.last_error);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(b.store.read_object("data", "while-apart-a").await.is_err(), "nothing pulled while apart");
    assert!(a.store.read_object("data", "while-apart-b").await.is_err(), "nothing pushed while apart");

    // Both enabled: nothing was checkpointed while apart, so both
    // directions continue from their positions (no re-snapshot) and
    // everything written apart crosses, as versions on both sides.
    let position_before = b.link(&link).status.position;
    b.store.set_bucket_versioning("data", BucketVersioning::Enabled).await.unwrap();
    eventually(60, "converged", || same(&a, &b, "data")).await;
    assert!(b.link(&link).status.position >= position_before, "continued from the position, not from a snapshot");
    assert!(b.link(&link).status.snapshot_cursor.is_none());
    for key in ["from-a", "from-b", "while-apart-a", "while-apart-b"] {
        assert!(same_versions(&a, &b, "data", key).await, "{key}");
    }
    assert_eq!(b.link(&link).status.state, "live");
    a.put("data", "from-a", b"a3").await;
    b.put("data", "from-b", b"b3").await;
    eventually(30, "versions on both", || async {
        same_versions(&a, &b, "data", "from-a").await && same_versions(&a, &b, "data", "from-b").await && versions_of(&a, "data", "from-a").await.len() == 2
    })
    .await;
}
