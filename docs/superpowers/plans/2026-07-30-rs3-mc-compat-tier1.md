# rs3 mc-Compatibility Tier 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make rs3's destructive commands (`rm -r`, `rb --force`) real, make `mirror` an incremental sync with `--remove`/`--overwrite`/honest `--dry-run` and cross-object parallelism, add server-side copy for same-endpoint S3→S3, and add parallel multipart downloads — while fixing the multipart-threshold bug and making unimplemented flags refuse instead of silently no-op.

**Architecture:** rs3 is a single-binary CLI (`client/` crate, currently one 1400-line `src/main.rs`) built on `aws-sdk-s3`, reading `mc`-style config. This plan first adds an end-to-end test harness that boots the sibling `rusts3` server (repo root crate) per test, then splits `main.rs` into focused modules, then lands features task-by-task. Pure logic (URL parsing, mirror diff planning) gets unit tests; command behavior gets e2e tests against a live rusts3.

**Tech Stack:** Rust edition 2024, tokio, aws-sdk-s3 1.x, clap 4, chrono, futures. New deps: `percent-encoding = "2"` (runtime), `tempfile = "3"` (dev).

## Global Constraints

- All work happens in the `client/` crate (`/home/code/workspace/rust-s3-server/client`). The server crate at repo root is only built/run by tests, never modified.
- CLI syntax must remain a superset-compatible subset of MinIO `mc` (reference source in `client/mc-reference/`): same command names, same flag names/shorthands. New flags are allowed only where mc has the same flag.
- Flags that are accepted but not implemented must return a hard error (`anyhow!("... is not implemented yet")`), never silently no-op. Exception: global `--json`/`--quiet`/`--no-color` stay as accepted no-ops (next plan).
- Default part size / multipart threshold: 256 MiB (`DEFAULT_PART_SIZE`). Minimum part size 5 MiB. `--part-size` sets **both** the part size and the multipart threshold.
- Destructive recursive operations require `--force` (matching mc): `rm --recursive` without `--force` and without `--dry-run` is an error; `rb` on a non-empty bucket requires `--force`; `rb` on an alias root requires `--dangerous`.
- Batch deletes use `DeleteObjects` with at most 1000 keys per request.
- Multi-target commands continue after per-target failures and exit non-zero at the end if any failed.
- Every task ends with `cargo fmt`, `cargo build`, the task's tests passing, and a commit.
- e2e tests must be runnable with plain `cargo test` from `client/` (they build the server on first run; set `RUSTS3_BIN` to skip).

---

### Task 1: End-to-end test harness + smoke test

**Files:**
- Modify: `client/Cargo.toml` (add `[dev-dependencies] tempfile = "3"`)
- Create: `client/tests/common/mod.rs`
- Create: `client/tests/e2e_smoke.rs`

**Interfaces:**
- Produces: `common::TestServer` with:
  - `TestServer::start() -> TestServer` — boots a fresh rusts3 on a free port with a temp data dir; panics with a clear message on failure.
  - `server.rs3(args: &[&str]) -> std::process::Output` — runs the rs3 binary with `MC_HOST_TEST` set so alias `test` points at this server, and `MC_CONFIG_DIR` pointing into the temp dir (so no real `~/.mc` is touched).
  - `server.rs3_ok(args: &[&str]) -> String` — like `rs3` but asserts exit success and returns stdout.
  - `server.alias() -> &'static str` — returns `"test"`.
  - Credentials are always `testkey` / `testsecret`.
- Every later task's e2e tests consume this module via `mod common;`.

- [ ] **Step 1: Add dev-dependency**

In `client/Cargo.toml` append:

```toml
[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 2: Write the harness**

Create `client/tests/common/mod.rs`:

```rust
#![allow(dead_code)]
use std::io::Write;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

pub struct TestServer {
    child: Child,
    pub port: u16,
    pub dir: tempfile::TempDir,
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

fn rusts3_binary() -> PathBuf {
    if let Ok(bin) = std::env::var("RUSTS3_BIN") {
        return PathBuf::from(bin);
    }
    // client/ crate root -> repo root
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("client has parent")
        .to_path_buf();
    let bin = repo_root.join("target/release/rusts3");
    if !bin.exists() {
        let status = Command::new("cargo")
            .args(["build", "--release"])
            .current_dir(&repo_root)
            .status()
            .expect("run cargo build for rusts3");
        assert!(status.success(), "building rusts3 failed");
    }
    bin
}

impl TestServer {
    pub fn start() -> Self {
        let dir = tempfile::tempdir().expect("create temp dir");
        let port = free_port();
        let config = format!(
            "server:\n  bind_address: 127.0.0.1\n  bind_port: {port}\n  base_dir: {data}\nui:\n  enabled: false\nauth:\n  enabled: true\n  credentials:\n    - access_key: testkey\n      secret_key: testsecret\nlogging:\n  level: warn\n  enable_bandwidth_report: false\n",
            data = dir.path().join("data").display()
        );
        let config_path = dir.path().join("config.yaml");
        std::fs::File::create(&config_path)
            .and_then(|mut f| f.write_all(config.as_bytes()))
            .expect("write server config");
        let child = Command::new(rusts3_binary())
            .args(["run", "-c"])
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn rusts3");
        let server = TestServer { child, port, dir };
        server.wait_ready();
        server
    }

    pub fn alias(&self) -> &'static str {
        "test"
    }

    fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let out = self.rs3(&["ls", "test/"]);
            if out.status.success() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "rusts3 did not become ready on port {}: {}",
                self.port,
                String::from_utf8_lossy(&out.stderr)
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    pub fn rs3(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_rs3"))
            .args(args)
            .env(
                "MC_HOST_TEST",
                format!("http://testkey:testsecret@127.0.0.1:{}", self.port),
            )
            .env("MC_CONFIG_DIR", self.dir.path().join("mc-config"))
            .output()
            .expect("run rs3")
    }

    pub fn rs3_ok(&self, args: &[&str]) -> String {
        let out = self.rs3(args);
        assert!(
            out.status.success(),
            "rs3 {:?} failed:\nstdout: {}\nstderr: {}",
            args,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
```

- [ ] **Step 3: Write the smoke test**

Create `client/tests/e2e_smoke.rs`:

```rust
mod common;
use common::TestServer;

#[test]
fn put_ls_get_roundtrip() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/smoke"]);

    let src = server.dir.path().join("hello.txt");
    std::fs::write(&src, b"hello rs3 e2e").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/smoke/hello.txt"]);

    let listing = server.rs3_ok(&["ls", "test/smoke"]);
    assert!(listing.contains("hello.txt"), "listing was: {listing}");

    let dst = server.dir.path().join("hello.out");
    server.rs3_ok(&["get", "test/smoke/hello.txt", dst.to_str().unwrap()]);
    assert_eq!(std::fs::read(&dst).unwrap(), b"hello rs3 e2e");
}
```

- [ ] **Step 4: Run it — expect PASS (this is harness bring-up, not a red/green cycle)**

Run from `client/`: `cargo test --test e2e_smoke`
Expected: PASS. First run may take minutes while it release-builds rusts3; verify a second run is fast.
If `wait_ready` times out, debug by removing `Stdio::null()` temporarily — do not commit that.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock tests/
git commit -m "test: add e2e harness that boots rusts3 per test"
```

---

### Task 2: Split main.rs into modules (no behavior change)

**Files:**
- Modify: `client/src/main.rs`
- Create: `client/src/config.rs`
- Create: `client/src/urls.rs`
- Create: `client/src/transfer.rs`
- Create: `client/src/list.rs`

**Interfaces:**
- Produces (all `pub(crate)`, signatures unchanged from today unless noted):
  - `config.rs`: `McConfig`, `Alias`, `default_api()`, `default_path()`, `load_config()`, `save_config()`, `config_path()`, `env_alias()`, `client_for_alias(alias_name: &str) -> Result<(Client, Alias)>`
  - `urls.rs`: `S3Url { alias, bucket, key }`, `parse_s3_url()`, `is_s3_url()`, `join_s3_target()`, `join_key()`, `parse_size()`, `format_time()`
  - `transfer.rs`: `DEFAULT_PART_SIZE`, `UploadedPart`, `upload_file()`, `multipart_upload()`, `multipart_copy_s3_to_s3()`, `transfer_object_between_s3()`, `download_object()`, `download_key_to_path()`
  - `list.rs`: `ListedObject { key, size }`, `list_s3_objects()`
- `main.rs` keeps: clap structs, `main()`, and the per-command `async fn`s (`alias`, `ls`, `mb`, `rb`, `put`, `cp`, `get`, `cat`, `rm`, `stat`, `mirror`, `copy_s3_object_to_s3`, `copy_local_path`, `mirror_local_to_s3`, `mirror_s3_to_local`, `mirror_s3_to_s3`).

- [ ] **Step 1: Move code**

Cut the listed items from `main.rs` into the four new files verbatim, add `mod config; mod urls; mod transfer; mod list;` at the top of `main.rs`, mark moved items `pub(crate)`, and fix `use` paths (each new module needs its own imports; `main.rs` gains `use config::*; use urls::*; use transfer::*; use list::*;` or explicit paths — explicit preferred).

- [ ] **Step 2: Verify no behavior change**

Run: `cargo build && cargo test --test e2e_smoke`
Expected: builds clean, smoke test passes.

- [ ] **Step 3: Commit**

```bash
git add src/
git commit -m "refactor: split main.rs into config/urls/transfer/list modules"
```

---

### Task 3: Correctness fixes — multipart threshold, trailing-slash URLs, multipart storage-class, mb error filter

**Files:**
- Modify: `client/src/transfer.rs` (threshold, storage-class)
- Modify: `client/src/urls.rs` (trailing slash) + unit tests in-file
- Modify: `client/src/main.rs` (`mb` error filter; pass storage-class through)

**Interfaces:**
- Consumes: Task 2 module layout.
- Produces: `multipart_upload` gains a trailing parameter `storage_class: Option<&str>`. `parse_s3_url` now preserves a trailing `/` on the key (`"a/b/p/"` → key `Some("p/")`; `"a/b/"` → key `None`).

- [ ] **Step 1: Write failing unit tests for `parse_s3_url`**

Append to `client/src/urls.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_preserves_trailing_slash_on_key() {
        let u = parse_s3_url("alias/bucket/prefix/").unwrap();
        assert_eq!(u.alias, "alias");
        assert_eq!(u.bucket.as_deref(), Some("bucket"));
        assert_eq!(u.key.as_deref(), Some("prefix/"));
    }

    #[test]
    fn parse_bucket_root_with_trailing_slash_has_no_key() {
        let u = parse_s3_url("alias/bucket/").unwrap();
        assert_eq!(u.bucket.as_deref(), Some("bucket"));
        assert_eq!(u.key, None);
    }

    #[test]
    fn parse_plain_object_key() {
        let u = parse_s3_url("alias/bucket/dir/obj.bin").unwrap();
        assert_eq!(u.key.as_deref(), Some("dir/obj.bin"));
    }

    #[test]
    fn parse_alias_only() {
        let u = parse_s3_url("alias").unwrap();
        assert_eq!(u.bucket, None);
        assert_eq!(u.key, None);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test parse_`
Expected: `parse_preserves_trailing_slash_on_key` FAILS (today `trim_matches('/')` strips the trailing slash).

- [ ] **Step 3: Fix `parse_s3_url`**

```rust
pub(crate) fn parse_s3_url(input: &str) -> Result<S3Url> {
    let normalized = input.trim_start_matches('/');
    let mut parts = normalized.splitn(3, '/');
    let alias = parts.next().unwrap_or_default().to_string();
    if alias.is_empty() {
        return Err(anyhow!("target must be ALIAS[/BUCKET[/OBJECT]]"));
    }
    let bucket = parts
        .next()
        .filter(|b| !b.is_empty())
        .map(str::to_string);
    let key = parts
        .next()
        .filter(|k| !k.is_empty())
        .map(str::to_string);
    Ok(S3Url { alias, bucket, key })
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test parse_` — Expected: PASS (all four).

- [ ] **Step 5: Fix the multipart threshold and storage-class in `transfer.rs`**

In `upload_file`, change the decision line to use the caller's part size:

```rust
    if disable_multipart || metadata.len() <= part_size {
```

and pass storage-class into the multipart path:

```rust
        multipart_upload(
            &client,
            source,
            &bucket,
            &key,
            metadata.len(),
            part_size,
            parallel.max(1),
            storage_class,
        )
        .await?;
```

In `multipart_upload`, add the parameter and apply it:

```rust
pub(crate) async fn multipart_upload(
    client: &Client,
    source: &Path,
    bucket: &str,
    key: &str,
    total_size: u64,
    part_size: u64,
    parallel: usize,
    storage_class: Option<&str>,
) -> Result<()> {
    ...
    let mut create = client.create_multipart_upload().bucket(bucket).key(key);
    if let Some(sc) = storage_class {
        create = create.storage_class(aws_sdk_s3::types::StorageClass::from(sc));
    }
    let created = create.send().await?;
```

Fix the other caller in `main.rs` (`mirror_local_to_s3` passes `None`).

- [ ] **Step 6: Fix `mb --ignore-existing` to only swallow already-exists errors**

In `main.rs` `mb()`:

```rust
        match result {
            Ok(_) => println!("Bucket created successfully `{target}`."),
            Err(err) => {
                let svc = err.as_service_error();
                let already_exists = svc.is_some_and(|e| {
                    e.is_bucket_already_owned_by_you() || e.is_bucket_already_exists()
                });
                if args.ignore_existing && already_exists {
                    println!("Bucket `{target}` already exists.");
                } else {
                    return Err(err.into());
                }
            }
        }
```

(`err` here is `SdkError<CreateBucketError>`; `as_service_error()` gives `Option<&CreateBucketError>` which has both `is_` predicates.)

- [ ] **Step 7: e2e check for the threshold fix**

Append to `client/tests/e2e_smoke.rs`:

```rust
#[test]
fn part_size_flag_lowers_multipart_threshold() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mp"]);
    // 12 MiB of patterned data with 5 MiB parts -> must take the multipart path.
    let src = server.dir.path().join("big.bin");
    let data: Vec<u8> = (0..12 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&[
        "put", "--part-size", "5MiB", src.to_str().unwrap(), "test/mp/big.bin",
    ]);
    let stat = server.rs3_ok(&["stat", "test/mp/big.bin"]);
    // Multipart ETags contain "-<parts>"; 12MiB / 5MiB = 3 parts.
    assert!(stat.contains("-3"), "expected multipart etag, stat: {stat}");
    let dst = server.dir.path().join("big.out");
    server.rs3_ok(&["get", "test/mp/big.bin", dst.to_str().unwrap()]);
    assert_eq!(std::fs::read(&dst).unwrap(), data);
}

#[test]
fn mb_ignore_existing_only_ignores_existing() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dup"]);
    server.rs3_ok(&["mb", "--ignore-existing", "test/dup"]);
    // A genuinely invalid bucket name must still fail even with -p.
    let out = server.rs3(&["mb", "--ignore-existing", "test/Invalid_Bucket_NAME"]);
    assert!(!out.status.success());
}
```

Run: `cargo test` — Expected: all unit + e2e tests PASS.

- [ ] **Step 8: Commit**

```bash
git add src/ tests/
git commit -m "fix: honor --part-size as multipart threshold, keep trailing-slash keys, apply storage-class to multipart, narrow mb -p"
```

---

### Task 4: Streaming paginator with rich entries

**Files:**
- Modify: `client/src/list.rs`
- Modify: `client/src/main.rs` (adapt `mirror_s3_to_local` / `mirror_s3_to_s3` call sites)

**Interfaces:**
- Consumes: Task 2 layout.
- Produces:
  - `ListedObject { pub key: String, pub size: u64, pub modified: Option<DateTime<Utc>> }`
  - `ObjectPaginator::new(client: Client, bucket: String, prefix: String) -> ObjectPaginator`
  - `ObjectPaginator::next_page(&mut self) -> Result<Option<Vec<ListedObject>>>` — pages of up to 1000; `Ok(None)` when exhausted; skips zero-byte keys ending in `/` (folder markers).
  - `list_s3_objects()` is deleted; the convenience replacement is `collect_objects(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<ListedObject>>` built on the paginator.
- Tasks 5, 6, and 9 consume `ObjectPaginator`; Task 9 consumes `collect_objects` and `modified`.

- [ ] **Step 1: Implement**

Replace the body of `client/src/list.rs` with:

```rust
use anyhow::Result;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub(crate) struct ListedObject {
    pub key: String,
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
}

pub(crate) struct ObjectPaginator {
    client: Client,
    bucket: String,
    prefix: String,
    token: Option<String>,
    done: bool,
}

impl ObjectPaginator {
    pub(crate) fn new(client: Client, bucket: String, prefix: String) -> Self {
        Self { client, bucket, prefix, token: None, done: false }
    }

    pub(crate) async fn next_page(&mut self) -> Result<Option<Vec<ListedObject>>> {
        if self.done {
            return Ok(None);
        }
        let resp = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .set_continuation_token(self.token.take())
            .send()
            .await?;
        let mut page = Vec::new();
        for obj in resp.contents() {
            let Some(key) = obj.key() else { continue };
            let size = obj.size().unwrap_or_default() as u64;
            if key.ends_with('/') && size == 0 {
                continue; // folder marker
            }
            page.push(ListedObject {
                key: key.to_string(),
                size,
                modified: obj.last_modified().and_then(|t| {
                    DateTime::<Utc>::from_timestamp(t.secs(), t.subsec_nanos())
                }),
            });
        }
        if resp.is_truncated().unwrap_or(false) {
            self.token = resp.next_continuation_token().map(String::from);
            if self.token.is_none() {
                self.done = true;
            }
        } else {
            self.done = true;
        }
        Ok(Some(page))
    }
}

pub(crate) async fn collect_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<ListedObject>> {
    let mut pager = ObjectPaginator::new(client.clone(), bucket.to_string(), prefix.to_string());
    let mut all = Vec::new();
    while let Some(page) = pager.next_page().await? {
        all.extend(page);
    }
    Ok(all)
}
```

- [ ] **Step 2: Adapt call sites**

`mirror_s3_to_local` and `mirror_s3_to_s3` in `main.rs` currently call `list_s3_objects(...)`. Change both to `collect_objects(...)` (Task 9 replaces these loops entirely; this keeps the build green meanwhile).

- [ ] **Step 3: Verify**

Run: `cargo build && cargo test`
Expected: clean build, all tests pass (paginator behavior is exercised by existing mirror e2e usage in later tasks; for now the smoke suite must stay green).

- [ ] **Step 4: Commit**

```bash
git add src/
git commit -m "refactor: page object listings through ObjectPaginator with modified times"
```

---

### Task 5: rm --recursive / --force / --dry-run with batch DeleteObjects

**Files:**
- Modify: `client/src/main.rs` (RmArgs + `rm`)
- Create: `client/tests/e2e_rm.rs`

**Interfaces:**
- Consumes: `ObjectPaginator` (Task 4), `parse_s3_url` (Task 3 semantics).
- Produces: `async fn remove_prefix(client: &Client, alias: &str, bucket: &str, prefix: &str, dry_run: bool) -> Result<u64>` in `main.rs` — deletes (or prints, for dry-run) every object under `prefix`, returns count removed. Task 6 (`rb --force`) consumes this exact function.
- mc semantics implemented: `--recursive` requires `--force` unless `--dry-run`; non-recursive `rm` on a missing key is an error; multiple targets continue on error and exit non-zero at the end.

- [ ] **Step 1: Write failing e2e tests**

Create `client/tests/e2e_rm.rs`:

```rust
mod common;
use common::TestServer;

fn seed(server: &TestServer, bucket: &str, keys: &[&str]) {
    server.rs3_ok(&["mb", &format!("test/{bucket}")]);
    let src = server.dir.path().join("seed.txt");
    std::fs::write(&src, b"x").unwrap();
    for key in keys {
        server.rs3_ok(&["put", src.to_str().unwrap(), &format!("test/{bucket}/{key}")]);
    }
}

#[test]
fn rm_recursive_requires_force() {
    let server = TestServer::start();
    seed(&server, "b1", &["a/1.txt"]);
    let out = server.rs3(&["rm", "--recursive", "test/b1/a/"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--force"));
    // object must still exist
    server.rs3_ok(&["stat", "test/b1/a/1.txt"]);
}

#[test]
fn rm_recursive_force_deletes_prefix_only() {
    let server = TestServer::start();
    seed(&server, "b2", &["keep.txt", "p/1.txt", "p/2.txt", "p/deep/3.txt"]);
    server.rs3_ok(&["rm", "--recursive", "--force", "test/b2/p/"]);
    let listing = server.rs3_ok(&["ls", "--recursive", "test/b2"]);
    assert!(listing.contains("keep.txt"), "listing: {listing}");
    assert!(!listing.contains("p/"), "listing: {listing}");
}

#[test]
fn rm_dry_run_deletes_nothing() {
    let server = TestServer::start();
    seed(&server, "b3", &["p/1.txt"]);
    let out = server.rs3_ok(&["rm", "--recursive", "--dry-run", "test/b3/p/"]);
    assert!(out.contains("p/1.txt"));
    server.rs3_ok(&["stat", "test/b3/p/1.txt"]);
}

#[test]
fn rm_missing_key_fails_but_later_targets_run() {
    let server = TestServer::start();
    seed(&server, "b4", &["real.txt"]);
    let out = server.rs3(&["rm", "test/b4/ghost.txt", "test/b4/real.txt"]);
    assert!(!out.status.success());
    // second target was still processed
    let stat = server.rs3(&["stat", "test/b4/real.txt"]);
    assert!(!stat.status.success());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test e2e_rm`
Expected: FAIL — today `rm --recursive` "succeeds" without deleting the prefix (flags ignored) and missing-key delete succeeds (S3 DeleteObject is idempotent), so at least `rm_recursive_requires_force`, `rm_recursive_force_deletes_prefix_only`, and `rm_missing_key_fails_but_later_targets_run` fail.

- [ ] **Step 3: Implement**

Add `--dry-run` to `RmArgs`:

```rust
    #[arg(long)]
    dry_run: bool,
```

Replace `rm()` in `main.rs`:

```rust
async fn rm(args: RmArgs) -> Result<()> {
    if args.versions || args.version_id.is_some() {
        return Err(anyhow!("rm --versions/--version-id is not implemented yet"));
    }
    if args.recursive && !args.force && !args.dry_run {
        return Err(anyhow!(
            "removal with --recursive requires --force (or use --dry-run)"
        ));
    }
    let mut failures = 0u64;
    for target in &args.targets {
        if let Err(err) = rm_one_target(target, &args).await {
            eprintln!("rm: {target}: {err:#}");
            failures += 1;
        }
    }
    if failures > 0 {
        return Err(anyhow!("{failures} target(s) failed"));
    }
    Ok(())
}

async fn rm_one_target(target: &str, args: &RmArgs) -> Result<()> {
    let parsed = parse_s3_url(target)?;
    let bucket = parsed
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in target `{target}`"))?;
    let (client, _) = client_for_alias(&parsed.alias).await?;
    if args.recursive {
        let prefix = parsed.key.unwrap_or_default();
        let removed =
            remove_prefix(&client, &parsed.alias, &bucket, &prefix, args.dry_run).await?;
        if removed == 0 {
            println!("Nothing to remove under `{target}`.");
        }
        Ok(())
    } else {
        let key = parsed
            .key
            .ok_or_else(|| anyhow!("object key is required in target `{target}`"))?;
        // DeleteObject succeeds for missing keys; stat first for an mc-like error.
        client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|_| anyhow!("object does not exist"))?;
        if args.dry_run {
            println!("DRY-RUN rm `{target}`.");
            return Ok(());
        }
        client.delete_object().bucket(&bucket).key(&key).send().await?;
        println!("Removed `{target}`.");
        Ok(())
    }
}

async fn remove_prefix(
    client: &Client,
    alias: &str,
    bucket: &str,
    prefix: &str,
    dry_run: bool,
) -> Result<u64> {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    let mut pager =
        ObjectPaginator::new(client.clone(), bucket.to_string(), prefix.to_string());
    let mut removed = 0u64;
    while let Some(page) = pager.next_page().await? {
        if page.is_empty() {
            continue;
        }
        if dry_run {
            for obj in &page {
                println!("DRY-RUN rm `{alias}/{bucket}/{}`.", obj.key);
            }
            removed += page.len() as u64;
            continue;
        }
        for chunk in page.chunks(1000) {
            let ids = chunk
                .iter()
                .map(|o| ObjectIdentifier::builder().key(&o.key).build())
                .collect::<Result<Vec<_>, _>>()?;
            let delete = Delete::builder().set_objects(Some(ids)).build()?;
            let resp = client
                .delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
                .await?;
            for err in resp.errors() {
                return Err(anyhow!(
                    "delete failed for `{}`: {}",
                    err.key().unwrap_or("?"),
                    err.message().unwrap_or("unknown error")
                ));
            }
            for obj in chunk {
                println!("Removed `{alias}/{bucket}/{}`.", obj.key);
            }
            removed += chunk.len() as u64;
        }
    }
    Ok(removed)
}
```

Note: deleting while paginating the same prefix is safe — `ListObjectsV2` continuation tokens tolerate deleted keys, and each page is fully deleted before the next fetch.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test e2e_rm` — Expected: PASS (all four). Then `cargo test` for the full suite.

- [ ] **Step 5: Commit**

```bash
git add src/ tests/
git commit -m "feat: implement rm --recursive/--force/--dry-run with batched DeleteObjects"
```

---

### Task 6: rb --force and --dangerous

**Files:**
- Modify: `client/src/main.rs` (RbArgs unchanged; rewrite `rb`)
- Create: `client/tests/e2e_rb.rs`

**Interfaces:**
- Consumes: `remove_prefix` (Task 5), `ObjectPaginator` (Task 4).
- Produces: `async fn remove_bucket(client: &Client, alias: &str, bucket: &str, force: bool) -> Result<()>` — with `force`, aborts all incomplete multipart uploads, empties the bucket via `remove_prefix`, then deletes it; without `force`, deletes only if already empty, otherwise errors mentioning `--force`.
- mc semantics: `rb alias` (no bucket) requires `--dangerous` and removes every bucket.

- [ ] **Step 1: Write failing e2e tests**

Create `client/tests/e2e_rb.rs`:

```rust
mod common;
use common::TestServer;

#[test]
fn rb_nonempty_without_force_fails() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/full"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/full/f.txt"]);
    let out = server.rs3(&["rb", "test/full"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--force"));
}

#[test]
fn rb_force_empties_then_removes() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/full2"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/full2/a/f.txt"]);
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/full2/b/f.txt"]);
    server.rs3_ok(&["rb", "--force", "test/full2"]);
    let listing = server.rs3_ok(&["ls", "test/"]);
    assert!(!listing.contains("full2"), "listing: {listing}");
}

#[test]
fn rb_alias_root_requires_dangerous() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/one"]);
    let out = server.rs3(&["rb", "--force", "test"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--dangerous"));
    server.rs3_ok(&["rb", "--force", "--dangerous", "test"]);
    let listing = server.rs3_ok(&["ls", "test/"]);
    assert!(listing.trim().is_empty(), "listing: {listing}");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test e2e_rb`
Expected: `rb_force_empties_then_removes` and `rb_alias_root_requires_dangerous` FAIL (`--force` currently ignored; alias root currently errors differently). `rb_nonempty_without_force_fails` may pass if the error mentions `--force` — it won't yet; confirm it fails on the assertion.

- [ ] **Step 3: Implement**

Rewrite `rb()` in `main.rs`:

```rust
async fn rb(args: RbArgs) -> Result<()> {
    let mut failures = 0u64;
    for target in &args.targets {
        let parsed = match parse_s3_url(target) {
            Ok(p) => p,
            Err(err) => {
                eprintln!("rb: {target}: {err:#}");
                failures += 1;
                continue;
            }
        };
        let result: Result<()> = async {
            let (client, _) = client_for_alias(&parsed.alias).await?;
            match &parsed.bucket {
                Some(bucket) => {
                    remove_bucket(&client, &parsed.alias, bucket, args.force).await
                }
                None => {
                    if !args.dangerous {
                        return Err(anyhow!(
                            "removing all buckets on `{}` requires --dangerous",
                            parsed.alias
                        ));
                    }
                    let resp = client.list_buckets().send().await?;
                    for bucket in resp.buckets() {
                        let name = bucket.name().unwrap_or_default();
                        remove_bucket(&client, &parsed.alias, name, args.force).await?;
                    }
                    Ok(())
                }
            }
        }
        .await;
        if let Err(err) = result {
            eprintln!("rb: {target}: {err:#}");
            failures += 1;
        }
    }
    if failures > 0 {
        return Err(anyhow!("{failures} target(s) failed"));
    }
    Ok(())
}

async fn remove_bucket(
    client: &Client,
    alias: &str,
    bucket: &str,
    force: bool,
) -> Result<()> {
    if force {
        abort_incomplete_uploads(client, bucket).await?;
        remove_prefix(client, alias, bucket, "", false).await?;
    }
    client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .map_err(|err| {
            let not_empty = err
                .as_service_error()
                .map(|e| format!("{e:?}").contains("BucketNotEmpty"))
                .unwrap_or(false);
            if not_empty {
                anyhow!("`{alias}/{bucket}` is not empty; use --force to remove its contents")
            } else {
                anyhow!(err)
            }
        })?;
    println!("Removed `{alias}/{bucket}` successfully.");
    Ok(())
}

async fn abort_incomplete_uploads(client: &Client, bucket: &str) -> Result<()> {
    let mut key_marker: Option<String> = None;
    let mut id_marker: Option<String> = None;
    loop {
        let resp = client
            .list_multipart_uploads()
            .bucket(bucket)
            .set_key_marker(key_marker.take())
            .set_upload_id_marker(id_marker.take())
            .send()
            .await?;
        for upload in resp.uploads() {
            let (Some(key), Some(id)) = (upload.key(), upload.upload_id()) else {
                continue;
            };
            client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(id)
                .send()
                .await?;
        }
        if resp.is_truncated().unwrap_or(false) {
            key_marker = resp.next_key_marker().map(String::from);
            id_marker = resp.next_upload_id_marker().map(String::from);
        } else {
            return Ok(());
        }
    }
}
```

Note on the `BucketNotEmpty` check: `DeleteBucketError` has no typed variant for it in aws-sdk-s3, so matching the debug representation of the unmodeled service error is the pragmatic detection; the fallback path still returns the original error.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test e2e_rb` then `cargo test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/ tests/
git commit -m "feat: rb --force empties bucket (objects + multipart), --dangerous for alias root"
```

---

### Task 7: Server-side copy for same-endpoint S3→S3

**Files:**
- Modify: `client/Cargo.toml` (add `percent-encoding = "2"`)
- Modify: `client/src/transfer.rs`
- Modify: `client/src/main.rs` (thread `Alias` values into transfer calls)
- Create: `client/tests/e2e_copy.rs`

**Interfaces:**
- Consumes: `Alias` (has `url`, `access_key`), `transfer_object_between_s3`, `multipart_copy_s3_to_s3`.
- Produces:
  - `pub(crate) fn same_endpoint(a: &Alias, b: &Alias) -> bool` — true when `url` and `access_key` match.
  - `transfer_object_between_s3` gains two parameters: `source_alias: &Alias, target_alias: &Alias` (after the two clients), and internally picks: same endpoint + size ≤ min(part_size, 5 GiB) → `CopyObject`; same endpoint + larger → parallel `UploadPartCopy`; different endpoints → existing streaming paths.
  - `pub(crate) fn encode_copy_source(bucket: &str, key: &str) -> String` — percent-encodes the key (slashes preserved).
- Task 9 (mirror) consumes the new `transfer_object_between_s3` signature.

- [ ] **Step 1: Write failing e2e test**

Create `client/tests/e2e_copy.rs`:

```rust
mod common;
use common::TestServer;

#[test]
fn same_alias_copy_small_object() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/src"]);
    server.rs3_ok(&["mb", "test/dst"]);
    let src = server.dir.path().join("obj.bin");
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/src/dir/obj name.bin"]);
    server.rs3_ok(&["cp", "test/src/dir/obj name.bin", "test/dst/copy.bin"]);
    let out = server.dir.path().join("copy.bin");
    server.rs3_ok(&["get", "test/dst/copy.bin", out.to_str().unwrap()]);
    assert_eq!(std::fs::read(&out).unwrap(), data);
}

#[test]
fn same_alias_copy_large_object_uses_multipart() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/big"]);
    let src = server.dir.path().join("big.bin");
    let data: Vec<u8> = (0..12 * 1024 * 1024u32).map(|i| (i % 249) as u8).collect();
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&["put", "--part-size", "5MiB", src.to_str().unwrap(), "test/big/a.bin"]);
    server.rs3_ok(&["cp", "--part-size", "5MiB", "test/big/a.bin", "test/big/b.bin"]);
    let stat = server.rs3_ok(&["stat", "test/big/b.bin"]);
    assert!(stat.contains("-3"), "expected multipart etag on copy target: {stat}");
    let out = server.dir.path().join("b.out");
    server.rs3_ok(&["get", "test/big/b.bin", out.to_str().unwrap()]);
    assert_eq!(std::fs::read(&out).unwrap(), data);
}
```

- [ ] **Step 2: Run to verify current state**

Run: `cargo test --test e2e_copy`
Expected: the small-object test likely PASSES today via GET→PUT (the key with a space exercises `encode_copy_source` once implemented); the large-object test may pass too. These tests are regression guards — the observable change (no client-side data movement) is verified by code path, so ALSO add the temporary assertion below during development: in `transfer_object_between_s3`'s streaming branch, `eprintln!("rs3: falling back to streaming copy")` and assert in the test that stderr does NOT contain it for same-alias copies. Keep the `eprintln!` gated behind `std::env::var("RS3_DEBUG_COPY").is_ok()` in the final code, and have the tests set that env var.

Concretely: `TestServer::rs3` already returns `Output`; in both tests, after implementation, add:

```rust
    // verify server-side path was taken
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rs3"))
        .args(["cp", "test/src/dir/obj name.bin", "test/dst/copy2.bin"])
        .env("MC_HOST_TEST", format!("http://testkey:testsecret@127.0.0.1:{}", server.port))
        .env("MC_CONFIG_DIR", server.dir.path().join("mc-config"))
        .env("RS3_DEBUG_COPY", "1")
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains("falling back to streaming copy"),
        "same-endpoint copy took the streaming path"
    );
```

(Add the equivalent block to the large test with its own target key `test/big/c.bin`.)

- [ ] **Step 3: Implement**

Add to `client/Cargo.toml` dependencies: `percent-encoding = "2"`.

In `transfer.rs`:

```rust
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

const COPY_SOURCE_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'/')
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

pub(crate) fn encode_copy_source(bucket: &str, key: &str) -> String {
    format!("{bucket}/{}", utf8_percent_encode(key, COPY_SOURCE_ENCODE))
}

pub(crate) fn same_endpoint(a: &crate::config::Alias, b: &crate::config::Alias) -> bool {
    a.url == b.url && a.access_key == b.access_key
}
```

Change `transfer_object_between_s3` signature and body:

```rust
const MAX_SINGLE_COPY: u64 = 5 * 1024 * 1024 * 1024; // AWS CopyObject ceiling

pub(crate) async fn transfer_object_between_s3(
    source_client: &Client,
    source_alias: &crate::config::Alias,
    source_bucket: &str,
    source_key: &str,
    target_client: &Client,
    target_alias: &crate::config::Alias,
    target_bucket: &str,
    target_key: &str,
    size: u64,
    part_size: u64,
    disable_multipart: bool,
    parallel: usize,
) -> Result<()> {
    if same_endpoint(source_alias, target_alias) {
        let single_limit = part_size.min(MAX_SINGLE_COPY);
        if disable_multipart || size <= single_limit {
            target_client
                .copy_object()
                .bucket(target_bucket)
                .key(target_key)
                .copy_source(encode_copy_source(source_bucket, source_key))
                .send()
                .await?;
        } else {
            multipart_server_side_copy(
                target_client, source_bucket, source_key, target_bucket, target_key,
                size, part_size, parallel,
            )
            .await?;
        }
        return Ok(());
    }
    if std::env::var("RS3_DEBUG_COPY").is_ok() {
        eprintln!("rs3: falling back to streaming copy");
    }
    // ... existing GET->PUT / multipart_copy_s3_to_s3 branches unchanged ...
}
```

Add `multipart_server_side_copy` (mirrors `multipart_copy_s3_to_s3`'s structure — create upload, fan out parts with `buffer_unordered(parallel.max(1))`, abort on failure, complete on success — but each part is one call, no data through the client):

```rust
async fn multipart_server_side_copy(
    target_client: &Client,
    source_bucket: &str,
    source_key: &str,
    target_bucket: &str,
    target_key: &str,
    total_size: u64,
    part_size: u64,
    parallel: usize,
) -> Result<()> {
    if part_size < 5 * 1024 * 1024 {
        return Err(anyhow!("multipart part size must be at least 5MiB"));
    }
    let created = target_client
        .create_multipart_upload()
        .bucket(target_bucket)
        .key(target_key)
        .send()
        .await?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| anyhow!("server did not return upload id"))?
        .to_string();
    let copy_source = encode_copy_source(source_bucket, source_key);
    let part_count = total_size.div_ceil(part_size);
    let uploads = stream::iter((1..=part_count).map(|part_index| {
        let client = target_client.clone();
        let copy_source = copy_source.clone();
        let target_bucket = target_bucket.to_string();
        let target_key = target_key.to_string();
        let upload_id = upload_id.clone();
        async move {
            let start = (part_index - 1) * part_size;
            let end = (total_size - 1).min(start + part_size - 1);
            let part_number = part_index as i32;
            let resp = client
                .upload_part_copy()
                .bucket(target_bucket)
                .key(target_key)
                .upload_id(upload_id)
                .part_number(part_number)
                .copy_source(copy_source)
                .copy_source_range(format!("bytes={start}-{end}"))
                .send()
                .await?;
            Ok::<UploadedPart, anyhow::Error>(UploadedPart {
                part_number,
                etag: resp.copy_part_result().and_then(|r| r.e_tag()).map(String::from),
            })
        }
    }))
    .buffer_unordered(parallel.max(1));
    let mut results = uploads.collect::<Vec<_>>().await;
    if let Some(err) = results.iter().find_map(|r| r.as_ref().err()) {
        let _ = target_client
            .abort_multipart_upload()
            .bucket(target_bucket)
            .key(target_key)
            .upload_id(&upload_id)
            .send()
            .await;
        return Err(anyhow!("{err}"));
    }
    let mut parts = results.drain(..).collect::<Result<Vec<_>>>()?;
    parts.sort_by_key(|p| p.part_number);
    let completed = parts
        .into_iter()
        .map(|p| {
            CompletedPart::builder()
                .part_number(p.part_number)
                .set_e_tag(p.etag)
                .build()
        })
        .collect::<Vec<_>>();
    target_client
        .complete_multipart_upload()
        .bucket(target_bucket)
        .key(target_key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await?;
    Ok(())
}
```

Update callers in `main.rs` (`copy_s3_object_to_s3`, `mirror_s3_to_s3`): both already hold `(client, alias)` pairs from `client_for_alias` — currently they discard the alias with `_`; bind it and pass through.

Add a unit test in `transfer.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_source_encodes_specials_keeps_slashes() {
        assert_eq!(
            encode_copy_source("bkt", "dir/obj name+x.bin"),
            "bkt/dir/obj%20name%2Bx.bin"
        );
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test e2e_copy` and `cargo test`. Expected: PASS, including the no-streaming-fallback assertions.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock src/ tests/
git commit -m "feat: server-side CopyObject/UploadPartCopy for same-endpoint S3-to-S3 copies"
```

---

### Task 8: Parallel multipart downloads with atomic rename

**Files:**
- Modify: `client/src/transfer.rs` (`download_object`, `download_key_to_path`)
- Modify: `client/src/main.rs` (GetArgs, `get`, `cp` S3→local path, `mirror_s3_to_local`)
- Create: `client/tests/e2e_download.rs`

**Interfaces:**
- Consumes: `parse_size`, `DEFAULT_PART_SIZE`.
- Produces:
  - `download_key_to_path(client: &Client, bucket: &str, key: &str, output: &Path, part_size: u64, parallel: usize) -> Result<()>` — sizes the object via `HeadObject`; ≤ `part_size` → single streaming GET; larger → pre-allocated temp file + parallel ranged GETs writing at offsets; both paths write to `<output>.rs3.part` then rename.
  - `download_object(source: &str, target: Option<PathBuf>, part_size: u64, parallel: usize) -> Result<()>`.
- Task 9 consumes the new `download_key_to_path` signature.
- mc parity: `get` gains no new flags (defaults: 256 MiB / 4 workers, matching mc's silent internal parallelism); `cp` and `mirror` pass their existing `--part-size`/`--parallel` through.

- [ ] **Step 1: Write failing e2e test**

Create `client/tests/e2e_download.rs`:

```rust
mod common;
use common::TestServer;

#[test]
fn large_download_via_cp_is_correct() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dl"]);
    let src = server.dir.path().join("big.bin");
    // 17 MiB so 5 MiB parts give 4 ranges with a short tail.
    let data: Vec<u8> = (0..17 * 1024 * 1024u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
        .collect();
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&["put", "--part-size", "5MiB", src.to_str().unwrap(), "test/dl/big.bin"]);

    let dst = server.dir.path().join("big.out");
    server.rs3_ok(&[
        "cp", "--part-size", "5MiB", "--parallel", "4",
        "test/dl/big.bin", dst.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(&dst).unwrap(), data, "parallel download corrupted data");
    // no leftover temp file
    assert!(!server.dir.path().join("big.out.rs3.part").exists());
}

#[test]
fn failed_download_leaves_no_partial_output() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dl2"]);
    let dst = server.dir.path().join("ghost.out");
    let out = server.rs3(&["get", "test/dl2/ghost.bin", dst.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(!dst.exists());
    assert!(!server.dir.path().join("ghost.out.rs3.part").exists());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test e2e_download`
Expected: compile error is acceptable as "failing" only if signatures already changed — they haven't, so both tests run: the first passes byte-equality via the current single-stream path (it will fail after Step 3 only if the implementation is wrong — it is the correctness guard), the second FAILS today because the streaming path `fs::File::create(output)` creates the empty output file before the GET error surfaces. Confirm the second test fails.

- [ ] **Step 3: Implement**

Replace `download_key_to_path` and `download_object` in `transfer.rs`:

```rust
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

pub(crate) async fn download_key_to_path(
    client: &Client,
    bucket: &str,
    key: &str,
    output: &Path,
    part_size: u64,
    parallel: usize,
) -> Result<()> {
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|err| anyhow!("stat `{bucket}/{key}`: {err}"))?;
    let size = head.content_length().unwrap_or_default() as u64;
    if let Some(parent) = output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }
    let tmp = {
        let mut name = output.file_name().unwrap_or_default().to_os_string();
        name.push(".rs3.part");
        output.with_file_name(name)
    };
    let result = download_to_temp(client, bucket, key, &tmp, size, part_size, parallel).await;
    match result {
        Ok(()) => {
            fs::rename(&tmp, output).await?;
            Ok(())
        }
        Err(err) => {
            let _ = fs::remove_file(&tmp).await;
            Err(err)
        }
    }
}

async fn download_to_temp(
    client: &Client,
    bucket: &str,
    key: &str,
    tmp: &Path,
    size: u64,
    part_size: u64,
    parallel: usize,
) -> Result<()> {
    if size <= part_size {
        let resp = client.get_object().bucket(bucket).key(key).send().await?;
        let mut reader = resp.body.into_async_read();
        let file = fs::File::create(tmp).await?;
        let mut writer = BufWriter::new(file);
        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.flush().await?;
        return Ok(());
    }
    let file = fs::File::create(tmp).await?;
    file.set_len(size).await?;
    drop(file);
    let part_count = size.div_ceil(part_size);
    let downloads = stream::iter((0..part_count).map(|part_index| {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let tmp = tmp.to_path_buf();
        async move {
            let start = part_index * part_size;
            let end = (size - 1).min(start + part_size - 1);
            let resp = client
                .get_object()
                .bucket(bucket)
                .key(key)
                .range(format!("bytes={start}-{end}"))
                .send()
                .await?;
            let mut file = fs::OpenOptions::new().write(true).open(&tmp).await?;
            file.seek(SeekFrom::Start(start)).await?;
            let mut reader = resp.body.into_async_read();
            let copied = tokio::io::copy(&mut reader, &mut file).await?;
            let expected = end - start + 1;
            if copied != expected {
                return Err(anyhow!(
                    "short range read: got {copied} of {expected} bytes at offset {start}"
                ));
            }
            file.flush().await?;
            Ok::<(), anyhow::Error>(())
        }
    }))
    .buffer_unordered(parallel.max(1));
    let results = downloads.collect::<Vec<_>>().await;
    for result in results {
        result?;
    }
    Ok(())
}

pub(crate) async fn download_object(
    source: &str,
    target: Option<PathBuf>,
    part_size: u64,
    parallel: usize,
) -> Result<()> {
    let parsed = crate::urls::parse_s3_url(source)?;
    let bucket = parsed
        .bucket
        .ok_or_else(|| anyhow!("bucket is required in source `{source}`"))?;
    let key = parsed
        .key
        .ok_or_else(|| anyhow!("object key is required in source `{source}`"))?;
    let (client, _) = crate::config::client_for_alias(&parsed.alias).await?;
    let output = match target {
        Some(path) if path.is_dir() => path.join(key.rsplit('/').next().unwrap_or(&key)),
        Some(path) => path,
        None => PathBuf::from(key.rsplit('/').next().unwrap_or(&key)),
    };
    download_key_to_path(&client, &bucket, &key, &output, part_size, parallel).await?;
    println!("Downloaded `{source}` to `{}`.", output.display());
    Ok(())
}
```

Update callers in `main.rs`:
- `get()`: reject `--version-id` with a hard error (`anyhow!("get --version-id is not implemented yet")`), then `download_object(&args.source, args.target, DEFAULT_PART_SIZE, 4).await`.
- `cp` S3→local single object: `download_object(source, Some(PathBuf::from(&target)), part_size, args.parallel).await?`.
- `mirror_s3_to_local` gains `part_size: u64, parallel: usize` parameters and passes them to `download_key_to_path`; the `mirror()` and `cp --recursive` call sites pass their parsed values.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test e2e_download` then `cargo test`. Expected: PASS — byte-identical 17 MiB roundtrip through the ranged path, and no partial files after a failed get.

- [ ] **Step 5: Commit**

```bash
git add src/ tests/
git commit -m "feat: parallel ranged downloads with atomic .rs3.part rename for cp/get/mirror"
```

---

### Task 9: Incremental mirror — diff planner, --remove, --overwrite, --dry-run, cross-object parallelism

**Files:**
- Create: `client/src/mirror.rs` (planner + entry collection + executors)
- Modify: `client/src/main.rs` (`mirror()` rewritten to use the planner; old `mirror_local_to_s3`/`mirror_s3_to_local`/`mirror_s3_to_s3` deleted; `cp --recursive` call sites redirected)
- Create: `client/tests/e2e_mirror.rs`

**Interfaces:**
- Consumes: `collect_objects` + `ListedObject.modified` (Task 4), `transfer_object_between_s3` with alias params (Task 7), `download_key_to_path` (Task 8), `upload_file` (Task 3 signature), `remove_prefix`-style batching pattern (Task 5).
- Produces in `mirror.rs`:

```rust
pub(crate) struct Entry {
    pub rel: String,               // path relative to the mirrored root, '/'-separated
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
}
pub(crate) struct MirrorPlan {
    pub copies: Vec<Entry>,        // source entries to transfer
    pub deletes: Vec<String>,      // target rels to delete (only when remove=true)
}
pub(crate) fn plan_mirror(source: &[Entry], target: &[Entry], overwrite: bool, remove: bool) -> MirrorPlan;
pub(crate) async fn collect_local_entries(root: &Path) -> Result<Vec<Entry>>;
pub(crate) async fn collect_s3_entries(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<Entry>>;
pub(crate) async fn run_mirror(args: &MirrorArgs) -> Result<()>;   // full command body
```

- Copy rule: copy a source entry when target lacks its `rel`, sizes differ, or the source `modified` is strictly newer than the target's; `overwrite` copies everything. Missing timestamps on either side ⇒ copy (safe). Delete rule: with `remove`, delete every target `rel` absent from source.
- Concurrency: copies execute via `stream::iter(...).map(...).buffer_unordered(parallel)`; failures are counted and reported per object; the command errors at the end if any failed. Deletes run after copies (S3 target: 1000-key `DeleteObjects` batches; local target: `fs::remove_file`).
- `--dry-run`/`--fake` print `PUT <src> -> <dst>` / `DEL <dst>` lines plus a `Planned N put(s), M delete(s)` summary and execute nothing.
- `--watch` stays a hard error.

- [ ] **Step 1: Write failing unit tests for the planner**

Create `client/src/mirror.rs` starting with types, a stub, and tests:

```rust
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Entry {
    pub rel: String,
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
}

#[derive(Debug, Default)]
pub(crate) struct MirrorPlan {
    pub copies: Vec<Entry>,
    pub deletes: Vec<String>,
}

pub(crate) fn plan_mirror(
    source: &[Entry],
    target: &[Entry],
    overwrite: bool,
    remove: bool,
) -> MirrorPlan {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn entry(rel: &str, size: u64, ts: Option<i64>) -> Entry {
        Entry {
            rel: rel.into(),
            size,
            modified: ts.map(|t| Utc.timestamp_opt(t, 0).unwrap()),
        }
    }

    #[test]
    fn copies_missing_targets() {
        let plan = plan_mirror(&[entry("a", 1, Some(100))], &[], false, false);
        assert_eq!(plan.copies.len(), 1);
        assert!(plan.deletes.is_empty());
    }

    #[test]
    fn skips_same_size_older_or_equal_source() {
        let src = [entry("a", 5, Some(100))];
        let dst = [entry("a", 5, Some(100)), ];
        assert!(plan_mirror(&src, &dst, false, false).copies.is_empty());
        let dst_newer = [entry("a", 5, Some(200))];
        assert!(plan_mirror(&src, &dst_newer, false, false).copies.is_empty());
    }

    #[test]
    fn copies_when_size_differs_or_source_newer() {
        let dst = [entry("a", 5, Some(100)), entry("b", 9, Some(100))];
        let src = [entry("a", 6, Some(100)), entry("b", 9, Some(150))];
        let plan = plan_mirror(&src, &dst, false, false);
        let rels: Vec<_> = plan.copies.iter().map(|e| e.rel.as_str()).collect();
        assert_eq!(rels, vec!["a", "b"]);
    }

    #[test]
    fn copies_when_timestamps_missing() {
        let src = [entry("a", 5, None)];
        let dst = [entry("a", 5, Some(100))];
        assert_eq!(plan_mirror(&src, &dst, false, false).copies.len(), 1);
    }

    #[test]
    fn overwrite_copies_everything() {
        let src = [entry("a", 5, Some(100))];
        let dst = [entry("a", 5, Some(200))];
        assert_eq!(plan_mirror(&src, &dst, true, false).copies.len(), 1);
    }

    #[test]
    fn remove_deletes_extraneous_targets_only_when_asked() {
        let src = [entry("a", 1, Some(100))];
        let dst = [entry("a", 1, Some(100)), entry("stale", 2, Some(50))];
        assert!(plan_mirror(&src, &dst, false, false).deletes.is_empty());
        assert_eq!(
            plan_mirror(&src, &dst, false, true).deletes,
            vec!["stale".to_string()]
        );
    }
}
```

- [ ] **Step 2: Run to verify failure**

Add `mod mirror;` to `main.rs`. Run: `cargo test plan_ mirror::` → `cargo test --lib` isn't available (binary crate); run `cargo test mirror::tests`
Expected: panics with `not yet implemented` — all 6 FAIL.

- [ ] **Step 3: Implement the planner**

```rust
pub(crate) fn plan_mirror(
    source: &[Entry],
    target: &[Entry],
    overwrite: bool,
    remove: bool,
) -> MirrorPlan {
    let target_map: BTreeMap<&str, &Entry> =
        target.iter().map(|e| (e.rel.as_str(), e)).collect();
    let source_set: HashSet<&str> = source.iter().map(|e| e.rel.as_str()).collect();
    let copies = source
        .iter()
        .filter(|s| {
            if overwrite {
                return true;
            }
            match target_map.get(s.rel.as_str()) {
                None => true,
                Some(t) => {
                    t.size != s.size
                        || match (s.modified, t.modified) {
                            (Some(sm), Some(tm)) => sm > tm,
                            _ => true,
                        }
                }
            }
        })
        .cloned()
        .collect();
    let deletes = if remove {
        target
            .iter()
            .filter(|t| !source_set.contains(t.rel.as_str()))
            .map(|t| t.rel.clone())
            .collect()
    } else {
        Vec::new()
    };
    MirrorPlan { copies, deletes }
}
```

Run: `cargo test mirror::tests` — Expected: PASS (all 6).

- [ ] **Step 4: Implement entry collection**

Append to `mirror.rs`:

```rust
pub(crate) async fn collect_local_entries(root: &Path) -> Result<Vec<Entry>> {
    use std::collections::VecDeque;
    let mut entries = Vec::new();
    let mut dirs = VecDeque::from([root.to_path_buf()]);
    while let Some(dir) = dirs.pop_front() {
        let mut rd = tokio::fs::read_dir(&dir)
            .await
            .with_context(|| format!("read {}", dir.display()))?;
        while let Some(item) = rd.next_entry().await? {
            let path = item.path();
            let meta = item.metadata().await?;
            if meta.is_dir() {
                dirs.push_back(path);
            } else if meta.is_file() {
                let rel = path
                    .strip_prefix(root)?
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                entries.push(Entry {
                    rel,
                    size: meta.len(),
                    modified: meta.modified().ok().map(DateTime::<Utc>::from),
                });
            }
        }
    }
    entries.sort_by(|a, b| a.rel.cmp(&b.rel));
    Ok(entries)
}

pub(crate) async fn collect_s3_entries(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<Entry>> {
    let objects = crate::list::collect_objects(client, bucket, prefix).await?;
    let mut entries: Vec<Entry> = objects
        .into_iter()
        .filter_map(|o| {
            let rel = o
                .key
                .strip_prefix(prefix)
                .unwrap_or(&o.key)
                .trim_start_matches('/')
                .to_string();
            if rel.is_empty() {
                return None;
            }
            Some(Entry { rel, size: o.size, modified: o.modified })
        })
        .collect();
    entries.sort_by(|a, b| a.rel.cmp(&b.rel));
    Ok(entries)
}
```

- [ ] **Step 5: Implement `run_mirror` and rewire `main.rs`**

Append to `mirror.rs` (this consumes `MirrorArgs` — move nothing; `main.rs` passes `&args`):

```rust
enum Side {
    Local(std::path::PathBuf),
    S3 {
        client: Client,
        alias: crate::config::Alias,
        alias_name: String,
        bucket: String,
        prefix: String,
    },
}

async fn resolve_side(spec: &str) -> Result<Side> {
    let path = Path::new(spec);
    if path.exists() || !crate::urls::is_s3_url(spec) {
        return Ok(Side::Local(path.to_path_buf()));
    }
    let url = crate::urls::parse_s3_url(spec)?;
    let bucket = url
        .bucket
        .clone()
        .ok_or_else(|| anyhow!("bucket is required in `{spec}`"))?;
    let (client, alias) = crate::config::client_for_alias(&url.alias).await?;
    Ok(Side::S3 {
        client,
        alias,
        alias_name: url.alias,
        bucket,
        prefix: url.key.unwrap_or_default(),
    })
}

fn s3_key(prefix: &str, rel: &str) -> String {
    crate::urls::join_key(prefix, rel)
}

pub(crate) async fn run_mirror(args: &crate::MirrorArgs) -> Result<()> {
    use futures::stream::{self, StreamExt};

    if args.watch {
        return Err(anyhow!("mirror --watch is not implemented yet"));
    }
    let part_size = crate::urls::parse_size(&args.part_size)?;
    let parallel = args.parallel.max(1);
    let dry_run = args.dry_run || args.fake;

    let source = resolve_side(&args.source).await?;
    let target = resolve_side(&args.target).await?;

    let source_entries = match &source {
        Side::Local(root) => {
            if !root.is_dir() {
                return Err(anyhow!("mirror source `{}` is not a directory", root.display()));
            }
            collect_local_entries(root).await?
        }
        Side::S3 { client, bucket, prefix, .. } => {
            collect_s3_entries(client, bucket, prefix).await?
        }
    };
    let target_entries = match &target {
        Side::Local(root) => {
            if root.exists() {
                collect_local_entries(root).await?
            } else {
                Vec::new()
            }
        }
        Side::S3 { client, bucket, prefix, .. } => {
            collect_s3_entries(client, bucket, prefix).await?
        }
    };

    let plan = plan_mirror(&source_entries, &target_entries, args.overwrite, args.remove);

    if dry_run {
        for entry in &plan.copies {
            println!("PUT {}/{} -> {}/{}", args.source.trim_end_matches('/'), entry.rel,
                     args.target.trim_end_matches('/'), entry.rel);
        }
        for rel in &plan.deletes {
            println!("DEL {}/{}", args.target.trim_end_matches('/'), rel);
        }
        println!(
            "Planned {} put(s), {} delete(s).",
            plan.copies.len(),
            plan.deletes.len()
        );
        return Ok(());
    }

    // --- copies, cross-object parallel ---
    let failures = stream::iter(plan.copies.iter().map(|entry| {
        let source = &source;
        let target = &target;
        async move {
            let result = copy_entry(source, target, entry, part_size,
                                    args.disable_multipart, parallel).await;
            match result {
                Ok(()) => {
                    println!("Mirrored `{}`.", entry.rel);
                    0u64
                }
                Err(err) => {
                    eprintln!("mirror: `{}` failed: {err:#}", entry.rel);
                    1u64
                }
            }
        }
    }))
    .buffer_unordered(parallel)
    .fold(0u64, |acc, n| async move { acc + n })
    .await;

    // --- deletes ---
    let mut delete_failures = 0u64;
    if !plan.deletes.is_empty() {
        match &target {
            Side::Local(root) => {
                for rel in &plan.deletes {
                    let path = root.join(rel);
                    match tokio::fs::remove_file(&path).await {
                        Ok(()) => println!("Removed `{}`.", path.display()),
                        Err(err) => {
                            eprintln!("mirror: remove `{}` failed: {err}", path.display());
                            delete_failures += 1;
                        }
                    }
                }
            }
            Side::S3 { client, alias_name, bucket, prefix, .. } => {
                use aws_sdk_s3::types::{Delete, ObjectIdentifier};
                for chunk in plan.deletes.chunks(1000) {
                    let ids = chunk
                        .iter()
                        .map(|rel| {
                            ObjectIdentifier::builder().key(s3_key(prefix, rel)).build()
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let delete = Delete::builder().set_objects(Some(ids)).build()?;
                    let resp = client
                        .delete_objects()
                        .bucket(bucket)
                        .delete(delete)
                        .send()
                        .await?;
                    delete_failures += resp.errors().len() as u64;
                    for err in resp.errors() {
                        eprintln!(
                            "mirror: remove `{}` failed: {}",
                            err.key().unwrap_or("?"),
                            err.message().unwrap_or("unknown")
                        );
                    }
                    for rel in chunk {
                        println!("Removed `{alias_name}/{bucket}/{}`.", s3_key(prefix, rel));
                    }
                }
            }
        }
    }

    let total = failures + delete_failures;
    if total > 0 {
        return Err(anyhow!("{total} object(s) failed"));
    }
    Ok(())
}

async fn copy_entry(
    source: &Side,
    target: &Side,
    entry: &Entry,
    part_size: u64,
    disable_multipart: bool,
    parallel: usize,
) -> Result<()> {
    match (source, target) {
        (Side::Local(src_root), Side::S3 { alias_name, bucket, prefix, .. }) => {
            let src = src_root.join(&entry.rel);
            let target_url = format!(
                "{alias_name}/{bucket}/{}",
                s3_key(prefix, &entry.rel)
            );
            crate::transfer::upload_file(
                &src, &target_url, part_size, parallel, disable_multipart, None,
            )
            .await
        }
        (Side::S3 { client, bucket, prefix, .. }, Side::Local(dst_root)) => {
            let key = s3_key(prefix, &entry.rel);
            let output = dst_root.join(&entry.rel);
            crate::transfer::download_key_to_path(
                client, bucket, &key, &output, part_size, parallel,
            )
            .await
        }
        (
            Side::S3 { client: sc, alias: sa, bucket: sb, prefix: sp, .. },
            Side::S3 { client: tc, alias: ta, bucket: tb, prefix: tp, .. },
        ) => {
            crate::transfer::transfer_object_between_s3(
                sc, sa, sb, &s3_key(sp, &entry.rel),
                tc, ta, tb, &s3_key(tp, &entry.rel),
                entry.size, part_size, disable_multipart, parallel,
            )
            .await
        }
        (Side::Local(_), Side::Local(_)) => {
            Err(anyhow!("mirror between two local paths is not supported"))
        }
    }
}
```

In `main.rs`:
- `mirror()` becomes `mirror::run_mirror(&args).await`.
- Make `MirrorArgs` and its fields `pub(crate)` (mirror.rs references `crate::MirrorArgs`).
- Delete `mirror_local_to_s3`, `mirror_s3_to_local`, `mirror_s3_to_s3`.
- `cp --recursive` call sites: S3→S3 recursive and local-dir→S3 and S3→local recursive now build a `MirrorArgs`-equivalent call — simplest is to construct a `MirrorArgs { parallel: args.parallel, part_size: args.part_size.clone(), overwrite: true, remove: false, dry_run: false, fake: false, watch: false, disable_multipart: args.disable_multipart, source: source.clone(), target: target.clone() }` and call `mirror::run_mirror(&that).await` — **note `overwrite: true`**: `cp` always copies, it is not a sync.

- [ ] **Step 6: Write e2e tests**

Create `client/tests/e2e_mirror.rs`:

```rust
mod common;
use common::TestServer;
use std::fs;

fn write(dir: &std::path::Path, rel: &str, contents: &[u8]) {
    let path = dir.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

#[test]
fn mirror_is_incremental() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/m1"]);
    let src = server.dir.path().join("srcdir");
    write(&src, "a.txt", b"aaa");
    write(&src, "sub/b.txt", b"bbb");
    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/m1/pfx"]);
    let listing = server.rs3_ok(&["ls", "--recursive", "test/m1"]);
    assert!(listing.contains("pfx/a.txt") && listing.contains("pfx/sub/b.txt"));

    // second run with nothing changed must transfer nothing
    let out = server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/m1/pfx"]);
    assert!(!out.contains("Mirrored"), "second run re-copied: {out}");

    // touch one file with new content -> exactly one transfer
    std::thread::sleep(std::time::Duration::from_millis(1100)); // S3 mtime granularity
    write(&src, "a.txt", b"aaa2");
    let out = server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/m1/pfx"]);
    assert_eq!(out.matches("Mirrored").count(), 1, "out: {out}");
}

#[test]
fn mirror_remove_deletes_extraneous() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/m2"]);
    let src = server.dir.path().join("srcdir2");
    write(&src, "keep.txt", b"k");
    write(&src, "drop.txt", b"d");
    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/m2/p"]);
    fs::remove_file(src.join("drop.txt")).unwrap();
    server.rs3_ok(&["mirror", "--remove", src.to_str().unwrap(), "test/m2/p"]);
    let listing = server.rs3_ok(&["ls", "--recursive", "test/m2"]);
    assert!(listing.contains("keep.txt"));
    assert!(!listing.contains("drop.txt"), "listing: {listing}");
}

#[test]
fn mirror_dry_run_prints_plan_and_does_nothing() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/m3"]);
    let src = server.dir.path().join("srcdir3");
    write(&src, "x.txt", b"x");
    let out = server.rs3_ok(&["mirror", "--dry-run", src.to_str().unwrap(), "test/m3/p"]);
    assert!(out.contains("PUT"), "out: {out}");
    assert!(out.contains("Planned 1 put(s)"), "out: {out}");
    let listing = server.rs3_ok(&["ls", "--recursive", "test/m3"]);
    assert!(!listing.contains("x.txt"));
}

#[test]
fn mirror_s3_to_s3_and_back_to_local() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/m4a"]);
    server.rs3_ok(&["mb", "test/m4b"]);
    let src = server.dir.path().join("srcdir4");
    write(&src, "one.bin", b"11111");
    write(&src, "d/two.bin", b"22222");
    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/m4a/p"]);
    server.rs3_ok(&["mirror", "test/m4a/p", "test/m4b/q"]);
    let dst = server.dir.path().join("outdir4");
    server.rs3_ok(&["mirror", "test/m4b/q", dst.to_str().unwrap()]);
    assert_eq!(fs::read(dst.join("one.bin")).unwrap(), b"11111");
    assert_eq!(fs::read(dst.join("d/two.bin")).unwrap(), b"22222");
}
```

- [ ] **Step 7: Run to verify pass**

Run: `cargo test --test e2e_mirror` then the full `cargo test`.
Expected: PASS. Known timing caveat: `mirror_is_incremental` depends on local mtime being older than the uploaded object's `LastModified` on the second run — the upload happens after the file write, so `LastModified >= local mtime` holds; the 1.1 s sleep before rewriting guarantees strict newness for the third run.

- [ ] **Step 8: Commit**

```bash
git add src/ tests/
git commit -m "feat: incremental mirror with diff planner, --remove/--overwrite/--dry-run, parallel objects"
```

---

### Task 10: Refuse remaining unimplemented flags; update README

**Files:**
- Modify: `client/src/main.rs` (guards at the top of `ls`, `cat`, `cp`, `stat`, `mb`)
- Modify: `client/README.md`
- Create: `client/tests/e2e_refuse.rs`

**Interfaces:**
- Consumes: everything prior.
- Produces: every still-unimplemented flag errors with the exact string `<flag> is not implemented yet` on stderr and non-zero exit. Covered flags: `ls --rewind/--versions/--incomplete/--summarize/--storage-class/--zip`, `cat --offset/--tail`, `cp --older-than/--newer-than`, `stat --recursive`, `mb --with-lock/--with-versioning`. (`rm`/`get` version flags and `mirror --watch` were handled in Tasks 5/8/9.)

- [ ] **Step 1: Write failing e2e test**

Create `client/tests/e2e_refuse.rs`:

```rust
mod common;
use common::TestServer;

#[test]
fn unimplemented_flags_hard_error() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/r"]);
    let cases: &[&[&str]] = &[
        &["ls", "--rewind", "7d", "test/r"],
        &["ls", "--versions", "test/r"],
        &["ls", "--incomplete", "test/r"],
        &["ls", "--summarize", "test/r"],
        &["ls", "--zip", "test/r"],
        &["cat", "--offset", "5", "test/r/x"],
        &["cat", "--tail", "5", "test/r/x"],
        &["cp", "--older-than", "7d", "test/r/x", "test/r/y"],
        &["cp", "--newer-than", "7d", "test/r/x", "test/r/y"],
        &["stat", "--recursive", "test/r"],
        &["mb", "--with-lock", "test/r2"],
        &["mb", "--with-versioning", "test/r3"],
    ];
    for args in cases {
        let out = server.rs3(args);
        assert!(!out.status.success(), "expected failure for {args:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("not implemented"),
            "wrong error for {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    // buckets from refused mb calls must not exist
    let listing = server.rs3_ok(&["ls", "test/"]);
    assert!(!listing.contains("r2") && !listing.contains("r3"), "listing: {listing}");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test e2e_refuse` — Expected: FAIL (flags currently no-op).

- [ ] **Step 3: Implement guards**

At the top of each command function, e.g. in `ls()`:

```rust
    if args.rewind.is_some() {
        return Err(anyhow!("ls --rewind is not implemented yet"));
    }
    if args.versions {
        return Err(anyhow!("ls --versions is not implemented yet"));
    }
    if args.incomplete {
        return Err(anyhow!("ls --incomplete is not implemented yet"));
    }
    if args.summarize {
        return Err(anyhow!("ls --summarize is not implemented yet"));
    }
    if args.zip {
        return Err(anyhow!("ls --zip is not implemented yet"));
    }
    if args.storage_class.is_some() {
        return Err(anyhow!("ls --storage-class is not implemented yet"));
    }
```

Repeat the same pattern for `cat` (`offset`, `tail`), `cp` (`older_than`, `newer_than`), `stat` (`recursive`), `mb` (`with_lock`, `with_versioning`). Ensure `main()` prints errors to **stderr** and exits non-zero — anyhow's `Result` from `main` already does both.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test e2e_refuse` then full `cargo test`. Expected: PASS.

- [ ] **Step 5: Update README**

In `client/README.md`:
- Under **Implemented**, note: `rm` recursive with batched deletes; `rb --force/--dangerous`; incremental `mirror` with `--remove`, `--overwrite`, `--dry-run`; same-endpoint server-side copy; parallel ranged downloads.
- Add a **Not yet implemented** section listing the refused flags above plus `mirror --watch`, `alias import`, versioning-related flags, and note that unimplemented flags error instead of no-op.
- Update the Multipart section: `--part-size` sets both part size and the multipart threshold; downloads above the threshold use parallel ranged GETs.

- [ ] **Step 6: Final verification and commit**

Run: `cargo fmt && cargo build --release && cargo test`
Expected: everything green.

```bash
git add src/ tests/ README.md
git commit -m "feat: hard-error on unimplemented mc flags; document tier-1 behavior"
```

---

## Self-Review Notes

- **Spec coverage:** tier-1 destructive commands (Tasks 5–6), incremental mirror + remove/overwrite/dry-run + object parallelism (Task 9), server-side copy (Task 7), parallel multipart downloads (Task 8), threshold bug (Task 3), refuse-instead-of-ignore (Tasks 5, 8, 10). `--json` output and new commands (`mv`, `head`, `du`, `pipe`, `tree`, `diff`, `find`, `share`) are explicitly deferred to the next plan.
- **Type consistency:** `remove_prefix` defined in Task 5 and consumed in Task 6; `transfer_object_between_s3` alias-bearing signature defined in Task 7 and consumed in Task 9's `copy_entry`; `download_key_to_path(part_size, parallel)` defined in Task 8 and consumed in Task 9; `ListedObject.modified` defined in Task 4 and consumed in Task 9's `collect_s3_entries`.
- **Known judgment calls:** `cp --recursive` maps to `run_mirror` with `overwrite: true` (cp always copies); `BucketNotEmpty` detection string-matches the unmodeled service error; e2e incremental test sleeps 1.1 s for mtime granularity; mirror still collects full entry lists in memory (~100 B/object — fine to ~10 M objects; streaming merge-join deferred).
