//! End-to-end IAM tests: users, groups, memberships, and policy conflict
//! resolution, exercised through the two real routers rather than against the
//! evaluator in isolation.
//!
//! * The **S3 API** router with auth enabled and a live [`IamStore`]: every
//!   request is SigV4-signed with an IAM access key, so a test passes only if
//!   key lookup, signature verification, the merged user+group policy, the
//!   request→action mapping, and the handler all agree.
//! * The **console API** router over the *same* store, driven with real session
//!   cookies — the path an administrator actually uses to create groups, attach
//!   policies, and assign memberships.
//!
//! The recurring shape is "change something in the console, then watch what the
//! same access key may do on the S3 API", because the property that matters is
//! that a grant or a revocation takes effect on the very next request.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::auth::AuthState;
use super::config::{ApiKeyPair, AppConfig, BuiltinUser};
use super::iam::{AccessKey, Group, IamStore};
use super::key_usage::KeyUsageStore;
use super::policy::{is_authorized, PolicyDocument, Requirement};
use super::{registry, router_with_metrics, scan_store, ui, TrafficMetrics};
use crate::storage::store::LocalObjectStore;

const HOST: &str = "localhost";
const REGION: &str = "us-east-1";
const ROOT_USER: &str = "root";
const ROOT_AK: &str = "ROOTKEY";
const ROOT_SK: &str = "ROOTSECRET";

const OK: StatusCode = StatusCode::OK;
const DENIED: StatusCode = StatusCode::FORBIDDEN;

struct Harness {
    _tmp: tempfile::TempDir,
    store: LocalObjectStore,
    iam: IamStore,
    usage: KeyUsageStore,
    s3: axum::Router,
    console: axum::Router,
}

/// A credential to sign S3 requests with.
#[derive(Clone)]
struct Cred {
    ak: String,
    sk: String,
}

impl From<&AccessKey> for Cred {
    fn from(key: &AccessKey) -> Self {
        Self {
            ak: key.access_key.clone(),
            sk: key.secret_key.clone(),
        }
    }
}

fn root_cred() -> Cred {
    Cred {
        ak: ROOT_AK.to_string(),
        sk: ROOT_SK.to_string(),
    }
}

fn policy(json: &str) -> PolicyDocument {
    serde_json::from_str(json).unwrap()
}

/// `Allow` of `actions` on a whole bucket and everything in it.
fn allow(actions: &[&str], bucket: &str) -> PolicyDocument {
    statement("Allow", actions, &[format!("arn:aws:s3:::{bucket}"), format!("arn:aws:s3:::{bucket}/*")])
}

fn deny(actions: &[&str], resources: &[&str]) -> PolicyDocument {
    statement("Deny", actions, &resources.iter().map(|r| r.to_string()).collect::<Vec<_>>())
}

fn statement(effect: &str, actions: &[&str], resources: &[String]) -> PolicyDocument {
    serde_json::from_value(json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": effect, "Action": actions, "Resource": resources}],
    }))
    .unwrap()
}

const READ: &[&str] = &["s3:GetObject", "s3:ListBucket"];
const WRITE: &[&str] = &["s3:PutObject", "s3:DeleteObject"];

impl Harness {
    async fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = AppConfig::default();
        config.auth.enabled = true;
        config.auth.public_hostname = Some(HOST.to_string());
        config.auth.users.push(BuiltinUser {
            user: ROOT_USER.to_string(),
            password: Some("root-password".to_string()),
            api_keys: vec![ApiKeyPair {
                ak: ROOT_AK.to_string(),
                secret: ROOT_SK.to_string(),
            }],
        });
        let config = Arc::new(config);
        let store = LocalObjectStore::new(tmp.path());
        let iam = IamStore::open(tmp.path()).await.unwrap();
        let usage = KeyUsageStore::open(tmp.path()).await.unwrap();
        let metrics = Arc::new(TrafficMetrics::default());
        let tasks = registry::TaskRegistry::new();
        let s3 = router_with_metrics(
            store.clone(),
            AuthState {
                config: config.clone(),
                iam: Some(iam.clone()),
                usage: Some(usage.clone()),
            },
            metrics.clone(),
            tasks.clone(),
        );
        let console = ui::router(ui::UiState {
            store: store.clone(),
            iam: iam.clone(),
            key_usage: usage.clone(),
            config,
            metrics,
            tasks,
            scans: super::jobs::perf_scan::ScanService::new(
                scan_store::ScanStore::open(tmp.path()).await.unwrap(),
            ),
            stats: None,
        });
        Self {
            _tmp: tmp,
            store,
            iam,
            usage,
            s3,
            console,
        }
    }

    /// Seeds buckets and objects directly in the store (no auth involved).
    async fn seed(&self, bucket: &str, keys: &[&str]) {
        self.store.create_bucket(bucket).await.unwrap();
        for key in keys {
            self.store
                .put_object(bucket, key, b"data", None, None, false)
                .await
                .unwrap();
        }
    }

    async fn user(&self, name: &str) -> Cred {
        self.iam.create_user(name, "password123").await.unwrap();
        Cred::from(&self.iam.create_access_key(name, &["test".to_string()]).await.unwrap())
    }

    async fn group(&self, name: &str, policy: Option<&PolicyDocument>) {
        self.iam.create_group(name, policy).await.unwrap();
    }

    async fn join(&self, user: &str, groups: &[&str]) {
        let names: Vec<String> = groups.iter().map(|g| g.to_string()).collect();
        let resolved = self.iam.resolve_groups(&names).await.unwrap();
        self.iam.set_user_groups(user, &resolved).await.unwrap();
    }

    // ── S3 API ──────────────────────────────────────────────────────────────

    async fn s3_full(
        &self,
        cred: &Cred,
        method: &str,
        path: &str,
        query: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> (StatusCode, String) {
        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let auth = super::auth::compute_auth_header(
            method, path, query, HOST, &cred.ak, &cred.sk, REGION, &datetime,
        );
        let uri = match query.is_empty() {
            true => path.to_string(),
            false => format!("{path}?{query}"),
        };
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("host", HOST)
            .header("x-amz-date", &datetime)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .header("authorization", auth);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let response = self
            .s3
            .clone()
            .oneshot(request.body(Body::from(body.to_vec())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn s3(&self, cred: &Cred, method: &str, path: &str, query: &str) -> StatusCode {
        let body: &[u8] = if method == "PUT" { b"payload" } else { b"" };
        self.s3_full(cred, method, path, query, &[], body).await.0
    }

    async fn get(&self, cred: &Cred, path: &str) -> StatusCode {
        self.s3(cred, "GET", path, "").await
    }

    async fn put(&self, cred: &Cred, path: &str) -> StatusCode {
        self.s3(cred, "PUT", path, "").await
    }

    async fn delete(&self, cred: &Cred, path: &str) -> StatusCode {
        self.s3(cred, "DELETE", path, "").await
    }

    async fn list(&self, cred: &Cred, bucket: &str, prefix: &str) -> StatusCode {
        let query = match prefix.is_empty() {
            true => "list-type=2".to_string(),
            false => format!("list-type=2&prefix={}", urlencoding::encode(prefix)),
        };
        self.s3(cred, "GET", &format!("/{bucket}"), &query).await
    }

    // ── console API ─────────────────────────────────────────────────────────

    /// A logged-in console session for `username` (`builtin` = config admin).
    fn session(&self, username: &str, builtin: bool) -> String {
        self.iam.create_session(username, builtin)
    }

    async fn console(
        &self,
        session: Option<&str>,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder().method(method).uri(path);
        if let Some(token) = session {
            request = request.header("cookie", format!("rusts3_ui_session={token}"));
        }
        let body = match body {
            Some(value) => {
                request = request.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&value).unwrap())
            }
            None => Body::empty(),
        };
        let response = self.console.clone().oneshot(request.body(body).unwrap()).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }
}

// ═══ baseline: what an IAM key is without any grant ═══════════════════════════

#[tokio::test]
async fn a_key_with_no_policy_and_no_groups_authenticates_but_may_do_nothing() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    let alice = h.user("alice").await;

    assert_eq!(h.get(&alice, "/").await, DENIED);
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
    assert_eq!(h.list(&alice, "docs", "").await, DENIED);
    assert_eq!(h.put(&alice, "/docs/new.txt").await, DENIED);
    assert_eq!(h.delete(&alice, "/docs/a.txt").await, DENIED);
    assert_eq!(h.put(&alice, "/newbucket").await, DENIED);
    // Denied by policy, not rejected as a stranger: the body says which.
    let (_, body) = h.s3_full(&alice, "GET", "/docs/a.txt", "", &[], b"").await;
    assert!(body.contains("AccessDenied"), "{body}");

    // A group that carries no policy grants nothing either.
    h.group("empty", None).await;
    h.join("alice", &["empty"]).await;
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);

    // Nothing above was destroyed or created.
    assert_eq!(h.get(&root_cred(), "/docs/a.txt").await, OK);
    assert_eq!(h.get(&root_cred(), "/docs/new.txt").await, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_bad_secret_or_unknown_key_is_rejected_whatever_the_policy_says() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    let alice = h.user("alice").await;
    h.group("everything", Some(&policy(
        r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}"#,
    )))
    .await;
    h.join("alice", &["everything"]).await;
    assert_eq!(h.get(&alice, "/docs/a.txt").await, OK);

    let wrong_secret = Cred { ak: alice.ak.clone(), sk: "not-the-secret".to_string() };
    let (status, body) = h.s3_full(&wrong_secret, "GET", "/docs/a.txt", "", &[], b"").await;
    assert_eq!(status, DENIED);
    assert!(body.contains("SignatureDoesNotMatch"), "{body}");

    let stranger = Cred { ak: "RSAKNOSUCHKEY".to_string(), sk: alice.sk.clone() };
    assert_eq!(h.get(&stranger, "/docs/a.txt").await, DENIED);

    // Another user's secret does not open this user's key.
    let bob = h.user("bob").await;
    let crossed = Cred { ak: alice.ak.clone(), sk: bob.sk.clone() };
    assert_eq!(h.get(&crossed, "/docs/a.txt").await, DENIED);
}

// ═══ one group ═══════════════════════════════════════════════════════════════

#[tokio::test]
async fn a_single_group_grants_exactly_its_policy() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    h.seed("other", &["b.txt"]).await;
    let alice = h.user("alice").await;
    h.group("docs-readers", Some(&allow(READ, "docs"))).await;
    h.join("alice", &["docs-readers"]).await;

    assert_eq!(h.get(&alice, "/docs/a.txt").await, OK);
    assert_eq!(h.s3(&alice, "HEAD", "/docs/a.txt", "").await, OK);
    assert_eq!(h.list(&alice, "docs", "").await, OK);
    // Not write, not another bucket, not account-level listing.
    assert_eq!(h.put(&alice, "/docs/new.txt").await, DENIED);
    assert_eq!(h.delete(&alice, "/docs/a.txt").await, DENIED);
    assert_eq!(h.get(&alice, "/other/b.txt").await, DENIED);
    assert_eq!(h.list(&alice, "other", "").await, DENIED);
    // A missing object is still a 404 for someone allowed to read — and a 403,
    // not a 404, for someone who is not: existence must not leak.
    assert_eq!(h.get(&alice, "/docs/missing.txt").await, StatusCode::NOT_FOUND);
    assert_eq!(h.get(&alice, "/other/missing.txt").await, DENIED);
    assert_eq!(h.get(&alice, "/nosuchbucket/x").await, DENIED);
}

// ═══ multiple memberships ═════════════════════════════════════════════════════

#[tokio::test]
async fn multiple_memberships_union_their_grants() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    h.seed("logs", &["old.log"]).await;
    h.seed("vault", &["secret"]).await;
    let alice = h.user("alice").await;
    h.group("docs-readers", Some(&allow(READ, "docs"))).await;
    h.group("logs-writers", Some(&allow(WRITE, "logs"))).await;
    h.join("alice", &["docs-readers", "logs-writers"]).await;
    assert_eq!(h.iam.groups_for("alice").len(), 2);

    // From docs-readers.
    assert_eq!(h.get(&alice, "/docs/a.txt").await, OK);
    assert_eq!(h.put(&alice, "/docs/new.txt").await, DENIED);
    // From logs-writers: write, but that group never granted read.
    assert_eq!(h.put(&alice, "/logs/new.log").await, OK);
    assert_eq!(h.delete(&alice, "/logs/old.log").await, StatusCode::NO_CONTENT);
    assert_eq!(h.get(&alice, "/logs/new.log").await, DENIED);
    assert_eq!(h.list(&alice, "logs", "").await, DENIED);
    // From neither.
    assert_eq!(h.get(&alice, "/vault/secret").await, DENIED);

    // Grants are not pooled across groups into something neither gave: reading
    // `docs` and writing `logs` does not add up to writing `docs`.
    assert_eq!(h.delete(&alice, "/docs/a.txt").await, DENIED);
    assert_eq!(h.get(&root_cred(), "/docs/a.txt").await, OK);
}

#[tokio::test]
async fn a_direct_user_policy_and_group_policies_add_up() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    h.seed("home", &[]).await;
    let alice = h.user("alice").await;
    h.iam.set_policy("alice", Some(&allow(&["s3:GetObject", "s3:PutObject", "s3:ListBucket"], "home"))).await.unwrap();
    h.group("docs-readers", Some(&allow(READ, "docs"))).await;
    h.join("alice", &["docs-readers"]).await;

    assert_eq!(h.put(&alice, "/home/mine.txt").await, OK);
    assert_eq!(h.get(&alice, "/home/mine.txt").await, OK);
    assert_eq!(h.get(&alice, "/docs/a.txt").await, OK);
    assert_eq!(h.put(&alice, "/docs/mine.txt").await, DENIED);

    // Detaching the direct policy leaves the group's grant standing, and the
    // other way round.
    h.iam.set_policy("alice", None).await.unwrap();
    assert_eq!(h.get(&alice, "/home/mine.txt").await, DENIED);
    assert_eq!(h.get(&alice, "/docs/a.txt").await, OK);
}

#[tokio::test]
async fn copy_needs_read_on_the_source_and_write_on_the_destination_from_any_group() {
    let h = Harness::new().await;
    h.seed("src", &["a.txt"]).await;
    h.seed("dst", &[]).await;
    let alice = h.user("alice").await;
    h.group("src-readers", Some(&allow(READ, "src"))).await;
    h.group("dst-writers", Some(&allow(WRITE, "dst"))).await;
    let copy = |cred: Cred, to: &'static str| {
        let h = &h;
        async move {
            h.s3_full(&cred, "PUT", to, "", &[("x-amz-copy-source", "/src/a.txt")], b"").await.0
        }
    };

    h.join("alice", &["src-readers"]).await;
    assert_eq!(copy(alice.clone(), "/dst/copy.txt").await, DENIED);
    h.join("alice", &["dst-writers"]).await;
    assert_eq!(copy(alice.clone(), "/dst/copy.txt").await, DENIED);
    // Only with both halves, each from a different group.
    h.join("alice", &["src-readers", "dst-writers"]).await;
    assert_eq!(copy(alice.clone(), "/dst/copy.txt").await, OK);
    assert_eq!(h.get(&root_cred(), "/dst/copy.txt").await, OK);
    // And not back the other way.
    assert_eq!(copy(alice.clone(), "/src/copy.txt").await, DENIED);
}

// ═══ conflict resolution ══════════════════════════════════════════════════════

#[tokio::test]
async fn an_explicit_deny_in_any_group_beats_every_allow() {
    let h = Harness::new().await;
    h.seed("docs", &["public/a.txt", "secret/b.txt"]).await;
    let alice = h.user("alice").await;
    h.group("docs-full", Some(&allow(&["s3:*"], "docs"))).await;
    h.group("no-secrets", Some(&deny(&["s3:*"], &["arn:aws:s3:::docs/secret/*"]))).await;

    h.join("alice", &["docs-full"]).await;
    assert_eq!(h.get(&alice, "/docs/secret/b.txt").await, OK);

    // Deny wins whichever order the memberships are listed in…
    for order in [["docs-full", "no-secrets"], ["no-secrets", "docs-full"]] {
        h.join("alice", &order).await;
        assert_eq!(h.get(&alice, "/docs/public/a.txt").await, OK, "{order:?}");
        assert_eq!(h.put(&alice, "/docs/public/new.txt").await, OK, "{order:?}");
        assert_eq!(h.get(&alice, "/docs/secret/b.txt").await, DENIED, "{order:?}");
        assert_eq!(h.put(&alice, "/docs/secret/new.txt").await, DENIED, "{order:?}");
        assert_eq!(h.delete(&alice, "/docs/secret/b.txt").await, DENIED, "{order:?}");
    }
    // …and over a broad allow attached directly to the user.
    h.iam.set_policy("alice", Some(&policy(
        r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::*"}]}"#,
    )))
    .await
    .unwrap();
    assert_eq!(h.get(&alice, "/docs/secret/b.txt").await, DENIED);
    // A deny attached directly to the user likewise beats a group's allow.
    h.iam.set_policy("alice", Some(&deny(&["s3:DeleteObject"], &["arn:aws:s3:::*"]))).await.unwrap();
    h.join("alice", &["docs-full"]).await;
    assert_eq!(h.get(&alice, "/docs/secret/b.txt").await, OK);
    assert_eq!(h.delete(&alice, "/docs/public/a.txt").await, DENIED);

    // Nothing was written or deleted where it was denied.
    assert_eq!(h.get(&root_cred(), "/docs/secret/b.txt").await, OK);
    assert_eq!(h.get(&root_cred(), "/docs/secret/new.txt").await, StatusCode::NOT_FOUND);
    assert_eq!(h.get(&root_cred(), "/docs/public/a.txt").await, OK);
}

#[tokio::test]
async fn a_deny_is_only_as_wide_as_its_action_and_resource() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    h.seed("logs", &["x.log"]).await;
    let alice = h.user("alice").await;
    h.group("both", Some(&policy(r#"{"Statement":[
        {"Effect":"Allow","Action":"s3:*","Resource":["arn:aws:s3:::docs","arn:aws:s3:::docs/*","arn:aws:s3:::logs","arn:aws:s3:::logs/*"]}
    ]}"#))).await;
    h.group("logs-readonly", Some(&deny(&["s3:PutObject", "s3:DeleteObject"], &["arn:aws:s3:::logs/*"]))).await;
    h.join("alice", &["both", "logs-readonly"]).await;

    assert_eq!(h.get(&alice, "/logs/x.log").await, OK);
    assert_eq!(h.put(&alice, "/logs/new.log").await, DENIED);
    assert_eq!(h.delete(&alice, "/logs/x.log").await, DENIED);
    // The deny names `logs`; `docs` is untouched by it.
    assert_eq!(h.put(&alice, "/docs/new.txt").await, OK);
    assert_eq!(h.delete(&alice, "/docs/a.txt").await, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn a_deny_alone_grants_nothing() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    let alice = h.user("alice").await;
    h.group("no-deletes", Some(&deny(&["s3:DeleteObject"], &["arn:aws:s3:::*"]))).await;
    h.join("alice", &["no-deletes"]).await;
    // "Everything except delete" is not what a deny means: default is deny.
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
    assert_eq!(h.put(&alice, "/docs/b.txt").await, DENIED);
}

#[tokio::test]
async fn console_rules_with_a_prefix_scope_objects_and_listings_and_deny_rules_carve_out() {
    let h = Harness::new().await;
    h.seed("team", &["shared/a.txt", "shared/private/p.txt", "other/o.txt"]).await;
    let alice = h.user("alice").await;
    let admin = h.session(ROOT_USER, true);

    // Exactly what the console's rule editor sends.
    let (status, _) = h.console(Some(&admin), "POST", "/api/groups", Some(json!({"name": "team-shared"}))).await;
    assert_eq!(status, OK);
    let (status, body) = h.console(Some(&admin), "PUT", "/api/groups/team-shared/policy/rules", Some(json!({"rules": [
        {"effect": "Allow", "access": "readwrite", "bucket": "team", "prefix": "shared/"},
        {"effect": "Deny", "access": "readwrite", "bucket": "team", "prefix": "shared/private/"},
    ]}))).await;
    assert_eq!(status, OK, "{body}");
    let (status, _) = h.console(Some(&admin), "PUT", "/api/users/alice/groups", Some(json!({"groups": ["team-shared"]}))).await;
    assert_eq!(status, OK);

    assert_eq!(h.get(&alice, "/team/shared/a.txt").await, OK);
    assert_eq!(h.put(&alice, "/team/shared/new.txt").await, OK);
    assert_eq!(h.get(&alice, "/team/other/o.txt").await, DENIED);
    assert_eq!(h.put(&alice, "/team/other/new.txt").await, DENIED);
    assert_eq!(h.get(&alice, "/team/shared/private/p.txt").await, DENIED);
    assert_eq!(h.put(&alice, "/team/shared/private/new.txt").await, DENIED);
    // Listings are scoped by the same prefixes.
    assert_eq!(h.list(&alice, "team", "shared/").await, OK);
    assert_eq!(h.list(&alice, "team", "shared/sub/").await, OK);
    assert_eq!(h.list(&alice, "team", "").await, DENIED);
    assert_eq!(h.list(&alice, "team", "other/").await, DENIED);
    assert_eq!(h.list(&alice, "team", "shared/private/").await, DENIED);
    // A prefix is a string prefix of the key, never a sibling that merely
    // starts the same way.
    h.store.put_object("team", "shared-not/x.txt", b"data", None, None, false).await.unwrap();
    assert_eq!(h.get(&alice, "/team/shared-not/x.txt").await, DENIED);
}

#[tokio::test]
async fn batch_delete_authorizes_every_key_against_the_merged_policy() {
    let h = Harness::new().await;
    h.seed("docs", &["public/a.txt", "secret/b.txt"]).await;
    let alice = h.user("alice").await;
    h.group("docs-full", Some(&allow(&["s3:*"], "docs"))).await;
    h.group("no-secrets", Some(&deny(&["s3:*"], &["arn:aws:s3:::docs/secret/*"]))).await;
    h.join("alice", &["docs-full", "no-secrets"]).await;

    let xml = br#"<Delete><Object><Key>public/a.txt</Key></Object><Object><Key>secret/b.txt</Key></Object></Delete>"#;
    let (status, body) = h.s3_full(&alice, "POST", "/docs", "delete", &[], xml).await;
    assert_eq!(status, OK, "{body}");
    assert!(body.contains("<Deleted><Key>public/a.txt</Key>"), "{body}");
    assert!(body.contains("<Error><Key>secret/b.txt</Key><Code>AccessDenied</Code>"), "{body}");
    assert_eq!(h.get(&root_cred(), "/docs/public/a.txt").await, StatusCode::NOT_FOUND);
    assert_eq!(h.get(&root_cred(), "/docs/secret/b.txt").await, OK);
}

// ═══ changes take effect on the next request ══════════════════════════════════

#[tokio::test]
async fn membership_and_policy_changes_apply_to_the_very_next_request_of_the_same_key() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    let alice = h.user("alice").await;
    h.group("docs-readers", Some(&allow(READ, "docs"))).await;
    h.group("docs-writers", Some(&allow(WRITE, "docs"))).await;

    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
    h.join("alice", &["docs-readers"]).await;
    assert_eq!(h.get(&alice, "/docs/a.txt").await, OK);

    // Setting memberships replaces them; it does not add to them.
    h.join("alice", &["docs-writers"]).await;
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
    assert_eq!(h.put(&alice, "/docs/b.txt").await, OK);
    h.join("alice", &[]).await;
    assert_eq!(h.put(&alice, "/docs/c.txt").await, DENIED);
    assert!(h.iam.policy_for("alice").is_none());

    // Editing a group's policy reaches every member at once.
    let bob = h.user("bob").await;
    h.join("alice", &["docs-readers"]).await;
    h.join("bob", &["docs-readers"]).await;
    assert_eq!(h.put(&bob, "/docs/bob.txt").await, DENIED);
    h.iam.set_group_policy("docs-readers", Some(&allow(&["s3:GetObject", "s3:PutObject"], "docs"))).await.unwrap();
    assert_eq!(h.put(&alice, "/docs/alice.txt").await, OK);
    assert_eq!(h.put(&bob, "/docs/bob.txt").await, OK);
    // Detaching it leaves members with nothing from that group.
    h.iam.set_group_policy("docs-readers", None).await.unwrap();
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
    assert_eq!(h.get(&bob, "/docs/a.txt").await, DENIED);
}

#[tokio::test]
async fn deleting_a_group_revokes_only_what_that_group_granted() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    h.seed("logs", &["x.log"]).await;
    let alice = h.user("alice").await;
    h.group("docs-readers", Some(&allow(READ, "docs"))).await;
    h.group("logs-readers", Some(&allow(READ, "logs"))).await;
    h.join("alice", &["docs-readers", "logs-readers"]).await;

    h.iam.delete_group("docs-readers").await.unwrap();
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
    assert_eq!(h.get(&alice, "/logs/x.log").await, OK);
    assert_eq!(h.iam.groups_for("alice"), ["logs-readers"]);

    // Deleting a *deny* group gives back what it was withholding — so this is
    // an action to take deliberately, and it must be exact.
    h.group("logs-full", Some(&allow(&["s3:*"], "logs"))).await;
    h.group("logs-locked", Some(&deny(&["s3:DeleteObject"], &["arn:aws:s3:::logs/*"]))).await;
    h.join("alice", &["logs-full", "logs-locked"]).await;
    assert_eq!(h.delete(&alice, "/logs/x.log").await, DENIED);
    h.iam.delete_group("logs-locked").await.unwrap();
    assert_eq!(h.delete(&alice, "/logs/x.log").await, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn a_recreated_group_or_user_inherits_nothing_from_its_namesake() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    let alice = h.user("alice").await;
    h.group("docs-readers", Some(&allow(READ, "docs"))).await;
    h.join("alice", &["docs-readers"]).await;

    // Group: delete, recreate under the same name (any casing) with a broader
    // policy. Former members must not silently be members again.
    h.iam.delete_group("docs-readers").await.unwrap();
    h.group("Docs-Readers", Some(&allow(&["s3:*"], "docs"))).await;
    assert!(h.iam.groups_for("alice").is_empty());
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);

    // User: delete, recreate. The old key is dead, and the namesake has no
    // memberships, no policy, and no keys.
    h.join("alice", &["Docs-Readers"]).await;
    h.iam.set_policy("alice", Some(&allow(&["s3:*"], "docs"))).await.unwrap();
    assert_eq!(h.get(&alice, "/docs/a.txt").await, OK);
    h.iam.delete_user("alice").await.unwrap();
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
    let reborn = h.user("alice").await;
    assert!(h.iam.groups_for("alice").is_empty());
    assert!(h.iam.policy_for("alice").is_none());
    assert_eq!(h.iam.list_access_keys("alice").await.unwrap().len(), 1);
    assert_eq!(h.get(&reborn, "/docs/a.txt").await, DENIED);
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
}

#[tokio::test]
async fn a_membership_in_a_group_that_does_not_exist_can_never_come_alive() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    let alice = h.user("alice").await;

    // Naming an unknown group is refused outright…
    assert!(h.iam.resolve_groups(&["ghost".to_string()]).await.is_err());
    // …including straight at the store, below the name resolution the console
    // does first — otherwise the row would lie dormant until someone creates a
    // group by that name, and alice would be in it without anyone adding her.
    let attempt = h.iam.set_user_groups("alice", &[Group::named("ghost").unwrap()]).await;
    assert!(attempt.is_err(), "membership in a nonexistent group was accepted");
    h.group("ghost", Some(&allow(&["s3:*"], "docs"))).await;
    assert!(h.iam.groups_for("alice").is_empty());
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
}

#[tokio::test]
async fn one_users_grants_never_leak_to_another() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    let alice = h.user("alice").await;
    let bob = h.user("bob").await;
    let alicia = h.user("alicia").await; // shares a prefix with `alice`
    h.group("docs-readers", Some(&allow(READ, "docs"))).await;
    h.join("alice", &["docs-readers"]).await;

    assert_eq!(h.get(&alice, "/docs/a.txt").await, OK);
    assert_eq!(h.get(&bob, "/docs/a.txt").await, DENIED);
    assert_eq!(h.get(&alicia, "/docs/a.txt").await, DENIED);
    // Clearing `alice`'s memberships must not touch a user whose name merely
    // starts the same way (memberships are stored under a `user\0group` key).
    h.join("alicia", &["docs-readers"]).await;
    h.join("alice", &[]).await;
    assert_eq!(h.get(&alicia, "/docs/a.txt").await, OK);
    h.iam.delete_user("alice").await.unwrap();
    assert_eq!(h.get(&alicia, "/docs/a.txt").await, OK);
    assert_eq!(h.iam.groups_for("alicia"), ["docs-readers"]);
}

#[tokio::test]
async fn deleting_a_key_kills_it_at_once_and_leaves_the_users_other_keys_alone() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    let first = h.user("alice").await;
    let second = Cred::from(&h.iam.create_access_key("alice", &["second".to_string()]).await.unwrap());
    h.group("docs-readers", Some(&allow(READ, "docs"))).await;
    h.join("alice", &["docs-readers"]).await;
    // Every key of a user carries that user's policy.
    assert_eq!(h.get(&first, "/docs/a.txt").await, OK);
    assert_eq!(h.get(&second, "/docs/a.txt").await, OK);

    h.iam.delete_access_key(&first.ak).await.unwrap();
    assert_eq!(h.get(&first, "/docs/a.txt").await, DENIED);
    assert_eq!(h.get(&second, "/docs/a.txt").await, OK);
}

#[tokio::test]
async fn memberships_policies_and_keys_survive_a_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let (ak, secret) = {
        let iam = IamStore::open(tmp.path()).await.unwrap();
        iam.create_user("alice", "password123").await.unwrap();
        iam.create_group("docs-full", Some(&allow(&["s3:*"], "docs"))).await.unwrap();
        iam.create_group("no-secrets", Some(&deny(&["s3:*"], &["arn:aws:s3:::docs/secret/*"]))).await.unwrap();
        let groups = iam.resolve_groups(&["docs-full".to_string(), "NO-SECRETS".to_string()]).await.unwrap();
        iam.set_user_groups("alice", &groups).await.unwrap();
        let key = iam.create_access_key("alice", &["test".to_string()]).await.unwrap();
        (key.access_key, key.secret_key)
    };
    let iam = IamStore::open(tmp.path()).await.unwrap();
    assert_eq!(iam.find_key(&ak).unwrap(), (secret, "alice".to_string()));
    let mut groups = iam.groups_for("alice");
    groups.sort();
    assert_eq!(groups, ["docs-full", "no-secrets"]);
    let effective = iam.policy_for("alice").unwrap();
    assert!(is_authorized(&effective, &[Requirement::object("s3:GetObject", "docs", "public/a")]));
    assert!(!is_authorized(&effective, &[Requirement::object("s3:GetObject", "docs", "secret/b")]));
}

// ═══ the admin group and root ═════════════════════════════════════════════════

#[tokio::test]
async fn a_member_of_the_admin_group_is_root_and_no_deny_can_bind_them() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt", "secret/b.txt"]).await;
    let alice = h.user("alice").await;
    h.join("alice", &["admin"]).await;
    assert!(h.iam.is_admin("alice"));

    assert_eq!(h.get(&alice, "/").await, OK);
    assert_eq!(h.put(&alice, "/brand-new-bucket").await, OK);
    assert_eq!(h.put(&alice, "/brand-new-bucket/x").await, OK);
    assert_eq!(h.delete(&alice, "/brand-new-bucket/x").await, StatusCode::NO_CONTENT);
    assert_eq!(h.delete(&alice, "/brand-new-bucket").await, StatusCode::NO_CONTENT);

    // Every way a Deny could reach an administrator: another group, a policy
    // attached directly, a blanket deny of everything. None of them binds.
    h.group("no-secrets", Some(&deny(&["s3:*"], &["arn:aws:s3:::docs/secret/*"]))).await;
    h.group("nothing-at-all", Some(&deny(&["s3:*"], &["arn:aws:s3:::*"]))).await;
    h.iam.set_policy("alice", Some(&deny(&["s3:*"], &["arn:aws:s3:::*"]))).await.unwrap();
    for order in [["admin", "no-secrets", "nothing-at-all"], ["nothing-at-all", "no-secrets", "admin"]] {
        h.join("alice", &order).await;
        assert_eq!(h.get(&alice, "/docs/secret/b.txt").await, OK, "{order:?}");
        assert_eq!(h.put(&alice, "/docs/secret/new.txt").await, OK, "{order:?}");
        assert_eq!(h.get(&alice, "/").await, OK, "{order:?}");
        assert_eq!(h.put(&alice, "/admin-made").await, OK, "{order:?}");
        assert_eq!(h.delete(&alice, "/admin-made").await, StatusCode::NO_CONTENT, "{order:?}");
    }
    let (status, body) = h.s3_full(&alice, "POST", "/docs", "delete", &[], br#"<Delete><Object><Key>secret/new.txt</Key></Object></Delete>"#).await;
    assert_eq!(status, OK);
    assert!(body.contains("<Deleted><Key>secret/new.txt</Key>"), "{body}");
    // The effective policy reported for them says the same: the admin grant,
    // and nothing that could contradict it.
    let effective = h.iam.policy_for("alice").unwrap();
    assert!(effective.statement.iter().all(|s| s.effect == super::policy::Effect::Allow));
    assert!(h.iam.identity_for("alice").is_unrestricted());
    // The console sees them the same way: every bucket, denied prefix included.
    let session = h.session("alice", false);
    let (status, _) = h.console(Some(&session), "GET", "/api/users", None).await;
    assert_eq!(status, OK);

    // It is the membership that makes them root. Out of the group, the denies
    // that were waiting apply on the very next request.
    h.join("alice", &["no-secrets", "nothing-at-all"]).await;
    assert!(!h.iam.is_admin("alice"));
    assert!(!h.iam.identity_for("alice").is_unrestricted());
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
    assert_eq!(h.get(&alice, "/docs/secret/b.txt").await, DENIED);
}

#[tokio::test]
async fn root_config_credentials_are_never_bound_by_iam_policy() {
    let h = Harness::new().await;
    h.seed("docs", &["secret/b.txt"]).await;
    // A user who happens to be called like the built-in admin cannot exist, so
    // no deny can ever be attached to root by name.
    assert_eq!(h.get(&root_cred(), "/docs/secret/b.txt").await, OK);
    assert_eq!(h.put(&root_cred(), "/another").await, OK);
    assert_eq!(h.get(&root_cred(), "/").await, OK);
    let admin = h.session(ROOT_USER, true);
    let (status, _) = h.console(Some(&admin), "PUT", &format!("/api/users/{ROOT_USER}/policy"), Some(json!({
        "Statement": [{"Effect": "Deny", "Action": "s3:*", "Resource": "arn:aws:s3:::*"}]
    }))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(h.get(&root_cred(), "/docs/secret/b.txt").await, OK);
}

// ═══ the console API that manages all of this ═════════════════════════════════

#[tokio::test]
async fn only_administrators_may_manage_users_groups_memberships_and_policies() {
    let h = Harness::new().await;
    h.user("alice").await;
    h.user("bob").await;
    h.group("ops", None).await;
    let alice = h.session("alice", false);
    let everything = json!({"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::*"}]});

    let attempts: Vec<(&str, String, Option<Value>)> = vec![
        ("GET", "/api/users".into(), None),
        ("POST", "/api/users".into(), Some(json!({"username": "mallory", "password": "password123"}))),
        ("DELETE", "/api/users/bob".into(), None),
        ("PUT", "/api/users/bob/password".into(), Some(json!({"password": "newpassword123"}))),
        ("PUT", "/api/users/alice/policy".into(), Some(everything.clone())),
        ("PUT", "/api/users/alice/policy/rules".into(), Some(json!({"rules": []}))),
        ("PUT", "/api/users/alice/groups".into(), Some(json!({"groups": ["admin"]}))),
        ("GET", "/api/groups".into(), None),
        ("POST", "/api/groups".into(), Some(json!({"name": "mine", "policy": everything}))),
        ("DELETE", "/api/groups/ops".into(), None),
        ("PUT", "/api/groups/ops/policy".into(), Some(everything.clone())),
        ("PUT", "/api/groups/ops/policy/rules".into(), Some(json!({"rules": []}))),
        ("GET", "/api/admin/export".into(), None),
    ];
    for (method, path, body) in &attempts {
        let (status, _) = h.console(Some(&alice), method, path, body.clone()).await;
        assert_eq!(status, DENIED, "non-admin {method} {path}");
        let (status, _) = h.console(None, method, path, body.clone()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "anonymous {method} {path}");
    }
    // Nothing above took effect.
    assert!(!h.iam.user_exists("mallory"));
    assert!(h.iam.user_exists("bob"));
    assert!(h.iam.policy_for("alice").is_none());
    assert!(!h.iam.is_admin("alice"));
    assert!(h.iam.resolve_groups(&["ops".to_string()]).await.is_ok());
    assert!(h.iam.resolve_groups(&["mine".to_string()]).await.is_err());

    // A user may still manage their own keys — and only their own.
    let (status, _) = h.console(Some(&alice), "GET", "/api/users/alice/keys", None).await;
    assert_eq!(status, OK);
    let (status, _) = h.console(Some(&alice), "GET", "/api/users/bob/keys", None).await;
    assert_eq!(status, DENIED);
    let (status, _) = h.console(Some(&alice), "POST", "/api/users/bob/keys", Some(json!({"tags": ["x"]}))).await;
    assert_eq!(status, DENIED);
    let bobs_key = h.iam.list_access_keys("bob").await.unwrap().remove(0).access_key;
    let (status, _) = h.console(Some(&alice), "DELETE", &format!("/api/keys/{bobs_key}"), None).await;
    assert_eq!(status, DENIED);
    let (status, _) = h.console(Some(&alice), "PUT", &format!("/api/keys/{bobs_key}"), Some(json!({"tags": ["pwned"]}))).await;
    assert_eq!(status, DENIED);
    assert!(h.iam.find_key(&bobs_key).is_some());
}

#[tokio::test]
async fn promotion_to_admin_and_demotion_take_effect_on_the_live_session() {
    let h = Harness::new().await;
    h.user("alice").await;
    let root = h.session(ROOT_USER, true);
    let alice = h.session("alice", false);

    let (status, _) = h.console(Some(&alice), "GET", "/api/users", None).await;
    assert_eq!(status, DENIED);
    let (status, _) = h.console(Some(&root), "PUT", "/api/users/alice/groups", Some(json!({"groups": ["admin"]}))).await;
    assert_eq!(status, OK);
    // Same cookie, no re-login.
    let (status, _) = h.console(Some(&alice), "GET", "/api/users", None).await;
    assert_eq!(status, OK);
    let (_, me) = h.console(Some(&alice), "GET", "/api/me", None).await;
    assert_eq!(me["is_admin"], true, "{me}");

    // An admin cannot lock themselves out by dropping their own membership…
    let (status, _) = h.console(Some(&alice), "PUT", "/api/users/alice/groups", Some(json!({"groups": []}))).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(h.iam.is_admin("alice"));
    // …nor delete themselves; another admin can do both.
    let (status, _) = h.console(Some(&alice), "DELETE", "/api/users/alice", None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = h.console(Some(&root), "PUT", "/api/users/alice/groups", Some(json!({"groups": []}))).await;
    assert_eq!(status, OK);
    let (status, _) = h.console(Some(&alice), "GET", "/api/users", None).await;
    assert_eq!(status, DENIED);
}

#[tokio::test]
async fn the_console_validates_groups_memberships_and_policies() {
    let h = Harness::new().await;
    h.user("alice").await;
    let root = h.session(ROOT_USER, true);
    let put = |path: &'static str, body: Value| {
        let (h, root) = (&h, root.clone());
        async move { h.console(Some(&root), "PUT", path, Some(body)).await.0 }
    };

    // Groups: reserved and duplicate names.
    for name in ["admin", "ADMIN", "", "has space", "a/b"] {
        let (status, _) = h.console(Some(&root), "POST", "/api/groups", Some(json!({"name": name}))).await;
        assert!(status.is_client_error(), "group name {name:?} -> {status}");
    }
    let (status, _) = h.console(Some(&root), "POST", "/api/groups", Some(json!({"name": "Ops"}))).await;
    assert_eq!(status, OK);
    let (status, _) = h.console(Some(&root), "POST", "/api/groups", Some(json!({"name": "ops"}))).await;
    assert!(status.is_client_error(), "case-insensitive duplicate -> {status}");
    // The admin group is fixed: no policy edits, no deletion.
    assert!(put("/api/groups/admin/policy", json!(null)).await.is_client_error());
    assert!(put("/api/groups/admin/policy/rules", json!({"rules": []})).await.is_client_error());
    let (status, _) = h.console(Some(&root), "DELETE", "/api/groups/admin", None).await;
    assert!(status.is_client_error());

    // Memberships: unknown group / unknown user; names are case-insensitive
    // and duplicates collapse.
    assert!(put("/api/users/alice/groups", json!({"groups": ["ghost"]})).await.is_client_error());
    assert!(put("/api/users/nobody/groups", json!({"groups": ["Ops"]})).await.is_client_error());
    assert!(h.iam.groups_for("alice").is_empty());
    assert_eq!(put("/api/users/alice/groups", json!({"groups": ["OPS", "ops", "Ops"]})).await, OK);
    assert_eq!(h.iam.groups_for("alice"), ["Ops"]);
    let (_, listed) = h.console(Some(&root), "GET", "/api/users/alice/groups", None).await;
    assert_eq!(listed["groups"], json!(["Ops"]));
    let (_, groups) = h.console(Some(&root), "GET", "/api/groups", None).await;
    let ops = groups["groups"].as_array().unwrap().iter().find(|g| g["name"] == "Ops").unwrap().clone();
    assert_eq!(ops["members"], 1, "{ops}");

    // Policies: malformed documents are refused rather than stored as "deny all"
    // or, worse, as something broader than written.
    for bad in [
        json!({"Statement": [{"Effect": "Permit", "Action": "s3:*", "Resource": "*"}]}),
        json!({"Statement": [{"Effect": "Allow", "Action": "s3:*"}]}),
        json!({"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*", "Principal": "*"}]}),
        json!({"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*",
                              "Condition": {"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}}}]}),
    ] {
        let status = put("/api/groups/Ops/policy", bad.clone()).await;
        assert!(status.is_client_error(), "{bad} -> {status}");
        let status = put("/api/users/alice/policy", bad.clone()).await;
        assert!(status.is_client_error(), "{bad} -> {status}");
    }
    assert!(h.iam.policy_for("alice").is_none());
    // Rules: a prefix needs a specific bucket.
    assert!(put("/api/groups/Ops/policy/rules", json!({"rules": [
        {"effect": "Allow", "access": "read", "bucket": "*", "prefix": "x/"}
    ]})).await.is_client_error());
}

#[tokio::test]
async fn deleting_a_user_or_group_in_the_console_cascades() {
    let h = Harness::new().await;
    h.seed("docs", &["a.txt"]).await;
    let alice = h.user("alice").await;
    let root = h.session(ROOT_USER, true);
    h.group("docs-readers", Some(&allow(READ, "docs"))).await;
    h.join("alice", &["docs-readers"]).await;
    let alice_session = h.session("alice", false);
    assert_eq!(h.get(&alice, "/docs/a.txt").await, OK);
    assert!(h.usage.get(&alice.ak).is_some());

    let (status, _) = h.console(Some(&root), "DELETE", "/api/users/alice", None).await;
    assert_eq!(status, OK);
    // Key dead, its usage stamp gone, console session ended, group emptied.
    assert_eq!(h.get(&alice, "/docs/a.txt").await, DENIED);
    assert!(h.usage.get(&alice.ak).is_none());
    let (status, _) = h.console(Some(&alice_session), "GET", "/api/me", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (_, groups) = h.console(Some(&root), "GET", "/api/groups", None).await;
    let readers = groups["groups"].as_array().unwrap().iter().find(|g| g["name"] == "docs-readers").unwrap().clone();
    assert_eq!(readers["members"], 0, "{readers}");
}

// ═══ listing buckets ══════════════════════════════════════════════════════════

/// The bucket names in a ListBuckets response, sorted.
fn bucket_names(xml: &str) -> Vec<String> {
    let mut names: Vec<String> = xml
        .split("<Name>")
        .skip(1)
        .filter_map(|rest| rest.split("</Name>").next())
        .map(str::to_string)
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn list_buckets_shows_each_caller_the_buckets_they_may_have_access_to() {
    let h = Harness::new().await;
    for bucket in ["docs", "team", "team-archive", "logs-2026", "vault"] {
        h.seed(bucket, &["shared/a.txt"]).await;
    }
    let listed = |cred: Cred| {
        let h = &h;
        async move {
            let (status, body) = h.s3_full(&cred, "GET", "/", "", &[], b"").await;
            (status, bucket_names(&body))
        }
    };
    let all = ["docs", "logs-2026", "team", "team-archive", "vault"];
    assert_eq!(listed(root_cred()).await, (OK, all.map(String::from).to_vec()));

    // No policy at all: nothing to show, and no listing either.
    let alice = h.user("alice").await;
    assert_eq!(listed(alice.clone()).await.0, DENIED);

    // One bucket granted → that bucket, not a 403 and not the whole account.
    h.group("docs-readers", Some(&allow(READ, "docs"))).await;
    h.join("alice", &["docs-readers"]).await;
    assert_eq!(listed(alice.clone()).await, (OK, vec!["docs".to_string()]));

    // Scoped to a prefix (the console's rule editor): the bucket the prefix is
    // in is listed — and its similarly named sibling is not.
    h.group("team-shared", Some(&super::policy::compile_rules(&[super::policy::PolicyRule {
        effect: super::policy::Effect::Allow,
        access: super::policy::RuleAccess::ReadWrite,
        bucket: "team".to_string(),
        prefix: "shared/".to_string(),
    }]).unwrap())).await;
    h.join("alice", &["docs-readers", "team-shared"]).await;
    assert_eq!(listed(alice.clone()).await, (OK, vec!["docs".to_string(), "team".to_string()]));
    // Listed is not granted: outside her prefix she is still refused.
    assert_eq!(h.get(&alice, "/team/shared/a.txt").await, OK);
    assert_eq!(h.list(&alice, "team", "").await, DENIED);

    // Several memberships union; a wildcard bucket grant counts.
    h.group("all-logs", Some(&policy(
        r#"{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::logs-*/*"}]}"#,
    ))).await;
    h.join("alice", &["docs-readers", "team-shared", "all-logs"]).await;
    assert_eq!(listed(alice.clone()).await.1, ["docs", "logs-2026", "team"]);

    // The console lists exactly the same buckets for the same user.
    let session = h.session("alice", false);
    let (status, body) = h.console(Some(&session), "GET", "/api/buckets", None).await;
    assert_eq!(status, OK);
    let mut console_names: Vec<&str> = body["buckets"].as_array().unwrap().iter().map(|b| b["name"].as_str().unwrap()).collect();
    console_names.sort();
    assert_eq!(console_names, ["docs", "logs-2026", "team"]);

    // An AWS-style account-wide grant lists everything…
    let bob = h.user("bob").await;
    h.iam.set_policy("bob", Some(&policy(
        r#"{"Statement":[{"Effect":"Allow","Action":"s3:ListAllMyBuckets","Resource":"arn:aws:s3:::*"}]}"#,
    ))).await.unwrap();
    assert_eq!(listed(bob.clone()).await, (OK, all.map(String::from).to_vec()));
    // …seeing them all still opens none of them…
    assert_eq!(h.get(&bob, "/vault/shared/a.txt").await, DENIED);
    // …a Deny on a whole bucket takes it off the list…
    h.group("no-vault", Some(&deny(&["s3:*"], &["arn:aws:s3:::vault", "arn:aws:s3:::vault/*"]))).await;
    h.join("bob", &["no-vault"]).await;
    assert_eq!(listed(bob.clone()).await.1, ["docs", "logs-2026", "team", "team-archive"]);
    // …and a policy that forbids listing in so many words is obeyed.
    h.group("no-listing", Some(&deny(&["s3:ListAllMyBuckets"], &["arn:aws:s3:::*"]))).await;
    h.join("bob", &["no-listing"]).await;
    assert_eq!(listed(bob.clone()).await.0, DENIED);

    // An administrator sees everything, whatever else they are a member of.
    h.join("bob", &["admin", "no-vault", "no-listing"]).await;
    assert_eq!(listed(bob).await, (OK, all.map(String::from).to_vec()));
}
