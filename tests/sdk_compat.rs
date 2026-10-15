//! What an AWS SDK user sees of versioning, object tagging and replication:
//! the official AWS SDK for Rust against freshly started `rusts3` servers,
//! over real sockets. The unit and spec tests assert the wire format; this
//! asserts that a real client's model of it (version ids, delete markers,
//! tag sets, listings) comes out right, and that two linked sites agree.
//!
//! Needs a built `rusts3` (`target/release` or `target/debug`, or
//! `RUSTS3_BIN`).

use std::collections::BTreeSet;
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, Delete, ObjectIdentifier, Tag, Tagging, TaggingDirective, VersioningConfiguration};
use aws_sdk_s3::Client;

const AK: &str = "sdkkey";
const SK: &str = "sdksecret";

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").expect("bind").local_addr().expect("addr").port()
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rusts3_binary() -> PathBuf {
    if let Ok(path) = std::env::var("RUSTS3_BIN") {
        return PathBuf::from(path);
    }
    for profile in ["release", "debug"] {
        let candidate = repo_root().join("target").join(profile).join("rusts3");
        if candidate.exists() {
            return candidate;
        }
    }
    panic!("no rusts3 binary: build one (cargo build --release) or set RUSTS3_BIN");
}

struct Server {
    child: Child,
    port: u16,
    ui_port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    /// A server with the S3 API and the console (for replication links)
    /// on two ports, one user with one key.
    fn start(binary: &Path, site_name: &str) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let (port, ui_port) = (free_port(), free_port());
        let config = format!(
            "server:\n  bind_address: 127.0.0.1\n  bind_port: {port}\n  base_dir: {data}\n  site_name: {site_name}\n\
             ui:\n  enabled: true\n  bind_address: 127.0.0.1\n  bind_port: {ui_port}\n  public_hostname: 127.0.0.1:{ui_port}\n  public_scheme: http\n\
             auth:\n  enabled: true\n  public_hostname: 127.0.0.1:{port}\n  public_scheme: http\n  users:\n    - user: admin\n      password: sdk-admin\n      api_keys:\n        - ak: {AK}\n          secret: {SK}\n\
             replication:\n  check_interval_secs: 1\n\
             logging:\n  level: warn\n  enable_bandwidth_report: false\n",
            data = dir.path().join("data").display()
        );
        let config_path = dir.path().join("config.yaml");
        std::fs::File::create(&config_path).and_then(|mut f| f.write_all(config.as_bytes())).expect("write config");
        let child = Command::new(binary)
            .args(["run", "-c"])
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn rusts3");
        let server = Server { child, port, ui_port, _dir: dir };
        for port in [server.port, server.ui_port] {
            let deadline = Instant::now() + Duration::from_secs(30);
            while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
                assert!(Instant::now() < deadline, "rusts3 did not listen on {port}");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        server
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn console(&self) -> String {
        format!("http://127.0.0.1:{}", self.ui_port)
    }

    async fn client(&self) -> Client {
        let credentials = aws_credential_types::Credentials::from_keys(AK, SK, None);
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(credentials)
            .endpoint_url(self.endpoint())
            .load()
            .await;
        let s3 = aws_sdk_s3::config::Builder::from(&config).force_path_style(true).build();
        Client::from_conf(s3)
    }

    /// A console session (cookie) for the replication API.
    async fn login(&self) -> (reqwest::Client, String) {
        let http = reqwest::Client::new();
        let response = http
            .post(format!("{}/api/login", self.console()))
            .header("content-type", "application/json")
            .body(r#"{"username":"admin","password":"sdk-admin"}"#)
            .send()
            .await
            .expect("login");
        assert!(response.status().is_success(), "login: {}", response.status());
        let cookie = response
            .headers()
            .get_all("set-cookie")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .map(|c| c.split(';').next().unwrap_or("").to_string())
            .collect::<Vec<_>>()
            .join("; ");
        (http, cookie)
    }

    /// Creates a replication link from this site to `peer`'s bucket.
    async fn link(&self, peer: &Server, bucket: &str, mode: &str) -> Result<String, String> {
        let (http, cookie) = self.login().await;
        let body = serde_json::json!({
            "name": format!("{mode}-{bucket}"),
            "local_bucket": bucket,
            "peer_endpoint": peer.endpoint(),
            "peer_bucket": bucket,
            "access_key": AK,
            "secret_key": SK,
            "local_access_key": AK,
            "mode": mode,
        });
        let response = http
            .post(format!("{}/api/replication", self.console()))
            .header("content-type", "application/json")
            .header("cookie", &cookie)
            .body(body.to_string())
            .send()
            .await
            .expect("create link");
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(text);
        }
        let view: serde_json::Value = serde_json::from_str(&text).expect("link json");
        Ok(view["id"].as_str().expect("link id").to_string())
    }

    async fn link_status(&self, id: &str) -> serde_json::Value {
        let (http, cookie) = self.login().await;
        let text = http
            .get(format!("{}/api/replication/{id}", self.console()))
            .header("cookie", &cookie)
            .send()
            .await
            .expect("link view")
            .text()
            .await
            .unwrap_or_default();
        serde_json::from_str::<serde_json::Value>(&text).expect("link view json")["status"].clone()
    }
}

/// The versions and markers of a bucket as the SDK reports them:
/// `(key, version id, is latest, is delete marker)`.
async fn versions(s3: &Client, bucket: &str) -> BTreeSet<(String, String, bool, bool)> {
    let mut out = BTreeSet::new();
    let mut key_marker = None;
    let mut version_marker = None;
    loop {
        let page = s3
            .list_object_versions()
            .bucket(bucket)
            .set_key_marker(key_marker.clone())
            .set_version_id_marker(version_marker.clone())
            .send()
            .await
            .expect("ListObjectVersions");
        for v in page.versions() {
            out.insert((v.key().unwrap_or("").to_string(), v.version_id().unwrap_or("").to_string(), v.is_latest().unwrap_or(false), false));
        }
        for m in page.delete_markers() {
            out.insert((m.key().unwrap_or("").to_string(), m.version_id().unwrap_or("").to_string(), m.is_latest().unwrap_or(false), true));
        }
        if !page.is_truncated().unwrap_or(false) {
            return out;
        }
        key_marker = page.next_key_marker().map(str::to_string);
        version_marker = page.next_version_id_marker().map(str::to_string);
    }
}

async fn tags_of(s3: &Client, bucket: &str, key: &str, version_id: Option<&str>) -> Vec<(String, String)> {
    let mut tags: Vec<(String, String)> = s3
        .get_object_tagging()
        .bucket(bucket)
        .key(key)
        .set_version_id(version_id.map(str::to_string))
        .send()
        .await
        .expect("GetObjectTagging")
        .tag_set()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    tags.sort();
    tags
}

fn tagging(pairs: &[(&str, &str)]) -> Tagging {
    let tags = pairs.iter().map(|(k, v)| Tag::builder().key(*k).value(*v).build().expect("tag")).collect();
    Tagging::builder().set_tag_set(Some(tags)).build().expect("tagging")
}

async fn body_of(s3: &Client, bucket: &str, key: &str, version_id: Option<&str>) -> String {
    let out = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .set_version_id(version_id.map(str::to_string))
        .send()
        .await
        .expect("GetObject");
    String::from_utf8(out.body.collect().await.expect("body").into_bytes().to_vec()).expect("utf8")
}

async fn eventually<F, Fut>(secs: u64, what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn versions_and_tags_through_the_aws_sdk() {
    let server = Server::start(&rusts3_binary(), "sdk-a");
    let s3 = server.client().await;
    let bucket = "sdk-versions";
    s3.create_bucket().bucket(bucket).send().await.expect("CreateBucket");

    // Before versioning: no version ids, one null version per key.
    let put = s3.put_object().bucket(bucket).key("k").body(ByteStream::from_static(b"null")).tagging("env=dev").send().await.expect("PutObject");
    assert!(put.version_id().is_none());
    assert_eq!(s3.get_bucket_versioning().bucket(bucket).send().await.expect("GetBucketVersioning").status(), None);
    assert_eq!(versions(&s3, bucket).await, BTreeSet::from([("k".to_string(), "null".to_string(), true, false)]));
    assert_eq!(tags_of(&s3, bucket, "k", None).await, vec![("env".to_string(), "dev".to_string())]);

    // Enable: every write is a version with an id, tags are per version.
    s3.put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(VersioningConfiguration::builder().status(BucketVersioningStatus::Enabled).build())
        .send()
        .await
        .expect("PutBucketVersioning");
    assert_eq!(s3.get_bucket_versioning().bucket(bucket).send().await.unwrap().status(), Some(&BucketVersioningStatus::Enabled));
    let mut ids = Vec::new();
    for n in 1..=3 {
        let put = s3.put_object().bucket(bucket).key("k").body(ByteStream::from(format!("v{n}").into_bytes())).tagging(format!("n={n}")).send().await.expect("PutObject");
        ids.push(put.version_id().expect("a version id").to_string());
    }
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "ids sort in write order: {ids:?}");
    assert_eq!(body_of(&s3, bucket, "k", None).await, "v3");
    assert_eq!(body_of(&s3, bucket, "k", Some(&ids[0])).await, "v1");
    assert_eq!(body_of(&s3, bucket, "k", Some("null")).await, "null");
    let head = s3.head_object().bucket(bucket).key("k").version_id(&ids[1]).send().await.expect("HeadObject");
    assert_eq!((head.version_id(), head.tag_count()), (Some(ids[1].as_str()), Some(1)));
    assert_eq!(tags_of(&s3, bucket, "k", Some(&ids[1])).await, vec![("n".to_string(), "2".to_string())]);
    assert_eq!(tags_of(&s3, bucket, "k", Some("null")).await, vec![("env".to_string(), "dev".to_string())]);
    let listed = versions(&s3, bucket).await;
    assert_eq!(listed.len(), 4);
    assert!(listed.contains(&("k".to_string(), ids[2].clone(), true, false)));
    assert!(listed.contains(&("k".to_string(), "null".to_string(), false, false)));

    // Tags are mutable per version without a new version.
    let etag_before = s3.head_object().bucket(bucket).key("k").send().await.unwrap().e_tag().map(str::to_string);
    let put_tags = s3.put_object_tagging().bucket(bucket).key("k").version_id(&ids[0]).tagging(tagging(&[("n", "one"), ("extra", "yes")])).send().await.expect("PutObjectTagging");
    assert_eq!(put_tags.version_id(), Some(ids[0].as_str()));
    assert_eq!(tags_of(&s3, bucket, "k", Some(&ids[0])).await, vec![("extra".to_string(), "yes".to_string()), ("n".to_string(), "one".to_string())]);
    assert_eq!(tags_of(&s3, bucket, "k", None).await, vec![("n".to_string(), "3".to_string())], "the current version is untouched");
    s3.delete_object_tagging().bucket(bucket).key("k").send().await.expect("DeleteObjectTagging");
    assert!(tags_of(&s3, bucket, "k", None).await.is_empty());
    assert_eq!(versions(&s3, bucket).await.len(), 4, "no new version");
    assert_eq!(s3.head_object().bucket(bucket).key("k").send().await.unwrap().e_tag().map(str::to_string), etag_before);
    let bad = s3.put_object_tagging().bucket(bucket).key("k").tagging(tagging(&[("bad*key", "x")])).send().await;
    assert!(bad.is_err(), "an invalid tag key is refused");

    // Copies keep or replace tags.
    s3.copy_object().bucket(bucket).key("copy").copy_source(format!("{bucket}/k?versionId={}", ids[0])).send().await.expect("CopyObject");
    assert_eq!(tags_of(&s3, bucket, "copy", None).await, vec![("extra".to_string(), "yes".to_string()), ("n".to_string(), "one".to_string())]);
    s3.copy_object().bucket(bucket).key("copy2").copy_source(format!("{bucket}/k")).tagging_directive(TaggingDirective::Replace).tagging("only=this").send().await.expect("CopyObject");
    assert_eq!(tags_of(&s3, bucket, "copy2", None).await, vec![("only".to_string(), "this".to_string())]);

    // A delete leaves a marker; by id it removes exactly that version.
    let del = s3.delete_object().bucket(bucket).key("k").send().await.expect("DeleteObject");
    assert_eq!(del.delete_marker(), Some(true));
    let marker = del.version_id().expect("the marker's id").to_string();
    assert!(s3.get_object().bucket(bucket).key("k").send().await.is_err());
    assert_eq!(body_of(&s3, bucket, "k", Some(&ids[2])).await, "v3");
    let listed = versions(&s3, bucket).await;
    assert!(listed.contains(&("k".to_string(), marker.clone(), true, true)));
    assert_eq!(listed.len(), 7, "{listed:?}");
    let undelete = s3.delete_object().bucket(bucket).key("k").version_id(&marker).send().await.expect("DeleteObject versionId");
    assert_eq!(undelete.version_id(), Some(marker.as_str()));
    assert_eq!(body_of(&s3, bucket, "k", None).await, "v3");
    // Bulk delete by version id, with a delimiter listing in between.
    let listed = s3.list_object_versions().bucket(bucket).delimiter("/").prefix("c").send().await.unwrap();
    assert_eq!(listed.versions().len(), 2, "copy and copy2");
    let gone = s3
        .delete_objects()
        .bucket(bucket)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key("k").version_id(&ids[0]).build().unwrap())
                .objects(ObjectIdentifier::builder().key("k").version_id("null").build().unwrap())
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("DeleteObjects");
    assert_eq!(gone.deleted().len(), 2);
    assert!(gone.errors().is_empty());
    let listed = versions(&s3, bucket).await;
    assert!(!listed.iter().any(|v| v.0 == "k" && (v.1 == ids[0] || v.1 == "null")));
    assert!(s3.get_object().bucket(bucket).key("k").version_id(&ids[0]).send().await.is_err(), "NoSuchVersion");

    // Suspended: writes are the null version again, history stays.
    s3.put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(VersioningConfiguration::builder().status(BucketVersioningStatus::Suspended).build())
        .send()
        .await
        .unwrap();
    let put = s3.put_object().bucket(bucket).key("k").body(ByteStream::from_static(b"susp")).send().await.unwrap();
    assert_eq!(put.version_id(), Some("null"));
    let listed = versions(&s3, bucket).await;
    assert!(listed.contains(&("k".to_string(), "null".to_string(), true, false)));
    assert!(listed.contains(&("k".to_string(), ids[2].clone(), false, false)));
}

#[tokio::test]
async fn replication_keeps_versions_and_tags_in_sync_through_the_aws_sdk() {
    let binary = rusts3_binary();
    let a = Server::start(&binary, "sdk-a");
    let b = Server::start(&binary, "sdk-b");
    let (sa, sb) = (a.client().await, b.client().await);
    let bucket = "sdk-repl";
    for s3 in [&sa, &sb] {
        s3.create_bucket().bucket(bucket).send().await.expect("CreateBucket");
        s3.put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(VersioningConfiguration::builder().status(BucketVersioningStatus::Enabled).build())
            .send()
            .await
            .expect("PutBucketVersioning");
    }
    // History before the link, including a burst and a tagged object.
    for n in 1..=3 {
        sa.put_object().bucket(bucket).key("k").body(ByteStream::from(format!("a{n}").into_bytes())).send().await.unwrap();
    }
    sa.put_object().bucket(bucket).key("tagged").body(ByteStream::from_static(b"t")).tagging("owner=a").send().await.unwrap();
    let link = b.link(&a, bucket, "merge").await.expect("link");
    eventually(60, "the history on b", || async { versions(&sa, bucket).await == versions(&sb, bucket).await && !versions(&sb, bucket).await.is_empty() }).await;
    assert_eq!(tags_of(&sb, bucket, "tagged", None).await, vec![("owner".to_string(), "a".to_string())]);

    // Live: writes on both sides, a burst, tag changes, removals.
    for n in 4..=6 {
        sa.put_object().bucket(bucket).key("k").body(ByteStream::from(format!("a{n}").into_bytes())).send().await.unwrap();
    }
    sb.put_object().bucket(bucket).key("from-b").body(ByteStream::from_static(b"b")).tagging("owner=b").send().await.unwrap();
    eventually(60, "the burst and b's write everywhere", || async {
        let (va, vb) = (versions(&sa, bucket).await, versions(&sb, bucket).await);
        va == vb && va.iter().filter(|v| v.0 == "k").count() == 6 && va.iter().any(|v| v.0 == "from-b")
    })
    .await;
    assert_eq!(tags_of(&sa, bucket, "from-b", None).await, vec![("owner".to_string(), "b".to_string())]);
    assert_eq!(body_of(&sb, bucket, "k", None).await, "a6");
    let k_versions: Vec<String> = {
        let mut v: Vec<_> = versions(&sa, bucket).await.into_iter().filter(|v| v.0 == "k").map(|v| v.1).collect();
        v.sort();
        v
    };
    assert_eq!(body_of(&sb, bucket, "k", Some(&k_versions[0])).await, "a1", "the oldest version crossed");

    // A tag change on a reaches b on the same version; one on b reaches a.
    sa.put_object_tagging().bucket(bucket).key("k").version_id(&k_versions[1]).tagging(tagging(&[("note", "second")])).send().await.unwrap();
    eventually(30, "tags on b", || async { tags_of(&sb, bucket, "k", Some(&k_versions[1])).await == vec![("note".to_string(), "second".to_string())] }).await;
    sb.delete_object_tagging().bucket(bucket).key("tagged").send().await.unwrap();
    eventually(30, "tags cleared on a", || async { tags_of(&sa, bucket, "tagged", None).await.is_empty() }).await;
    assert_eq!(versions(&sa, bucket).await, versions(&sb, bucket).await, "tag changes made no versions");

    // Removing a version on b removes it on a; a delete marker on a shows
    // on b; removing the marker on b undeletes on a.
    sb.delete_object().bucket(bucket).key("k").version_id(&k_versions[2]).send().await.unwrap();
    eventually(30, "the removal on a", || async { !versions(&sa, bucket).await.iter().any(|v| v.1 == k_versions[2]) }).await;
    let marker = sa.delete_object().bucket(bucket).key("k").send().await.unwrap().version_id().unwrap().to_string();
    eventually(30, "the marker on b", || async { versions(&sb, bucket).await.contains(&("k".to_string(), marker.clone(), true, true)) }).await;
    assert!(sb.get_object().bucket(bucket).key("k").send().await.is_err());
    sb.delete_object().bucket(bucket).key("k").version_id(&marker).send().await.unwrap();
    eventually(30, "undeleted on a", || async { sa.get_object().bucket(bucket).key("k").send().await.is_ok() && versions(&sa, bucket).await == versions(&sb, bucket).await }).await;
    assert_eq!(body_of(&sa, bucket, "k", None).await, "a6");
    let status = b.link_status(&link).await;
    assert_eq!(status["state"], "live", "{status}");
    assert!(status["last_error"].is_null(), "{status}");

    // Bucket settings travel: a merge link to an unversioned bucket turns
    // versioning on there, as the SDK sees it.
    for s3 in [&sa, &sb] {
        s3.create_bucket().bucket("sdk-settings").send().await.unwrap();
    }
    sa.put_bucket_versioning()
        .bucket("sdk-settings")
        .versioning_configuration(VersioningConfiguration::builder().status(BucketVersioningStatus::Enabled).build())
        .send()
        .await
        .unwrap();
    b.link(&a, "sdk-settings", "merge").await.expect("link");
    eventually(60, "versioning adopted on b", || async {
        sb.get_bucket_versioning().bucket("sdk-settings").send().await.unwrap().status() == Some(&BucketVersioningStatus::Enabled)
    })
    .await;
}
