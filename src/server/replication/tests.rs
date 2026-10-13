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
    }
}

impl Site {
    async fn start(name: &str) -> Self {
        Self::start_with(name, fast_timing()).await
    }

    async fn start_with(name: &str, timing: Timing) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        Self::open(tmp, name, timing).await
    }

    /// Opens a site on an existing data directory (also: a restart).
    async fn open(tmp: tempfile::TempDir, name: &str, timing: Timing) -> Self {
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
        let Site { tmp, name, store, iam, notify, replication, .. } = self;
        drop((store, iam, notify, replication));
        tokio::time::sleep(Duration::from_millis(300)).await;
        Self::open(tmp, &name, fast_timing()).await
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
    let view = b.link(&id);
    assert_eq!(view.status.state, "live");
    assert!(view.status.metadata_only >= 1, "the tier change moved no data: {:?}", view.status);
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
    assert!(!merged.contains_key("gone"), "a's newer delete (its tombstone) wins over b's copy");

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
