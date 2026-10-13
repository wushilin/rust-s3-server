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
        let store = LocalObjectStore::new(tmp.path());
        store.init_site(Some(name)).unwrap();
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
        let replication = ReplicationService::start(store.clone(), db::LinkDb::new(iam.admin_db()), timing, shutdown.clone())
            .await
            .unwrap();
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        config.auth.credentials.push(Credential {
            access_key: AK.to_string(),
            secret_key: SK.to_string(),
        });
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
        self.shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let Site { tmp, name, store, iam, notify, replication, endpoint, .. } = self;
        drop((store, iam, notify, replication));
        tokio::time::sleep(Duration::from_millis(300)).await;
        let bind = endpoint.trim_start_matches("http://").to_string();
        Self::open(tmp, &name, fast_timing(), &bind).await
    }

    /// A link on this site pulling `bucket` from `peer`.
    async fn pull_from(&self, peer: &Site, bucket: &str, mode: Mode) -> String {
        self.pull_with(peer, bucket, mode, AK, SK).await
    }

    async fn pull_with(&self, peer: &Site, bucket: &str, mode: Mode, ak: &str, sk: &str) -> String {
        self.replication
            .create(
                LinkRequest {
                    name: format!("{}-from-{}", self.name, peer.name),
                    local_bucket: bucket.to_string(),
                    peer_endpoint: peer.endpoint.clone(),
                    peer_bucket: bucket.to_string(),
                    access_key: ak.to_string(),
                    secret_key: sk.to_string(),
                    region: None,
                    mode,
                    prefix: String::new(),
                    enabled: true,
                },
                "tester",
            )
            .await
            .unwrap()
            .id
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
                crate::storage::store::PublishOptions::default(),
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
            crate::storage::store::PublishOptions::default(),
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

    b.pull_from(&a, "shared", Mode::Merge).await;
    a.pull_from(&b, "shared", Mode::Merge).await;
    eventually(30, "initial merge", || same(&a, &b, "shared")).await;
    let merged = a.contents("shared").await;
    assert!(merged.contains_key("only-a") && merged.contains_key("only-b"), "union");
    assert_eq!(merged["both"].5, b"from b, later", "the later write wins on both sides");
    // a deleted its own copy of "gone"; b's copy was a different, newer
    // version a never saw — deletes are version-scoped, so it survives.
    assert_eq!(merged["gone"].5, b"x", "b's unseen version survives a's delete");
    assert_eq!(merged["gone"].2, b.contents("shared").await["gone"].2);

    // Writes on both sides, including the same key.
    a.put("shared", "k1", b"a1").await;
    b.put("shared", "k2", b"b2").await;
    a.put("shared", "both", b"a again").await;
    b.store.delete_object("shared", "only-a").await.unwrap();
    eventually(30, "two-way changes", || same(&a, &b, "shared")).await;
    let merged = b.contents("shared").await;
    assert_eq!(merged["both"].5, b"a again");
    assert!(!merged.contains_key("only-a"));

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
    let bad = b.pull_with(&a, "data", Mode::Mirror, &key.access_key, "wrong").await;
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
    let hung = b
        .replication
        .create(
            LinkRequest {
                name: "hung".into(),
                local_bucket: "data".into(),
                peer_endpoint: hole,
                peer_bucket: "data".into(),
                access_key: AK.into(),
                secret_key: SK.into(),
                region: None,
                mode: Mode::Merge,
                prefix: String::new(),
                enabled: true,
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
    let id = b.pull_from(&a, "data", Mode::Mirror).await;
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
            crate::storage::store::PublishOptions { stamp: Some(stamp), ..Default::default() },
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
        .complete_multipart_with("data", "big.bin", &upload, &done, crate::storage::store::PublishOptions { stamp: Some(old), ..Default::default() })
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
async fn restoring_never_rolls_back_silently() {
    let a = Site::start("site-a").await;
    let b = Site::start("site-b").await;
    for site in [&a, &b] {
        site.store.create_bucket("data").await.unwrap();
    }
    put_aged(&a, "data", "k", b"v1", 60_000).await;
    let id = b.pull_from(&a, "data", Mode::Merge).await;
    eventually(30, "merged", || same(&a, &b, "data")).await;
    // Stop pulling, then b writes a newer version that a never gets.
    b.replication.act(&id, LinkAction::Pause).await.unwrap();
    put_aged(&b, "data", "k", b"v2 only on b", 30_000).await;
    b.replication.act(&id, LinkAction::Resume).await.unwrap();
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
                        crate::storage::store::PublishOptions { stamp, ..Default::default() },
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
                        crate::storage::store::PublishOptions { stamp, ..Default::default() },
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
                    let options = crate::storage::store::PublishOptions { stamp, ..Default::default() };
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
    a.pull_from(&b, "chaos", Mode::Merge).await;

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
            region: "us-east-1".into(),
            mode: Mode::Mirror,
            filter: Default::default(),
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
        })
        .unwrap(),
        status: Arc::new(std::sync::Mutex::new(model::LinkStatus::default())),
        heartbeat: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        cancel: CancellationToken::new(),
        max_checkpoint_age_ms: i64::MAX,
        long_poll: Duration::from_secs(1),
        parallelism: 4,
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
