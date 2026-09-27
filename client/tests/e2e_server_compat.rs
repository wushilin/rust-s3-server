//! rs3 against the hardened server: every path where a server-side rule
//! (signed headers, POST policy enforcement, copy-source preconditions,
//! Range/Content-Range checks, delimiter paging, DeleteObjects parsing,
//! duplicate query rejection, probe-path auth) meets the client code that
//! exercises it. Each test drives the real `rs3` binary, or `curl` on
//! commands rs3 generates, against a real `rusts3` process.
mod common;
use common::TestServer;
use std::fs;
use std::path::Path;
use std::process::Command;

/// Deterministic, incompressible-enough bytes without a `rand` dependency.
fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u8
        })
        .collect()
}

fn keys(server: &TestServer, bucket: &str, prefix: &str) -> Vec<String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = server.sdk_client().await;
        let mut out = Vec::new();
        let mut token = None;
        loop {
            let resp = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix)
                .set_continuation_token(token)
                .send()
                .await
                .unwrap();
            out.extend(
                resp.contents()
                    .iter()
                    .filter_map(|o| o.key().map(str::to_string)),
            );
            token = resp.next_continuation_token().map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        out
    })
}

fn content_type(server: &TestServer, bucket: &str, key: &str) -> Option<String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = server.sdk_client().await;
        client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap()
            .content_type()
            .map(str::to_string)
    })
}

#[cfg(unix)]
/// HTTP status of a `curl` invocation (`-w %{http_code}`), body discarded.
fn curl_status(args: &[&str]) -> String {
    let out = Command::new("curl")
        .args(["-s", "-o", "/dev/null", "-w", "%{http_code}"])
        .args(args)
        .output()
        .expect("run curl");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn assert_same_file(a: &Path, b: &Path) {
    let (x, y) = (fs::read(a).unwrap(), fs::read(b).unwrap());
    assert_eq!(x.len(), y.len(), "{} vs {}", a.display(), b.display());
    assert!(x == y, "{} and {} differ", a.display(), b.display());
}

/// Multipart through every transfer path rs3 has: upload, ranged verified
/// download, same-endpoint server-side copy (UploadPartCopy pinned with
/// copy-source-if-match), S3->S3 copy with `--disable-multipart` off, mirror
/// download, and a concurrent pipe. Every copy must be byte-identical and
/// pass rs3's own ETag verification.
#[test]
fn multipart_round_trip_through_every_transfer_path() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mpart"]);
    let base = server.dir.path().join("mp");
    fs::create_dir_all(&base).unwrap();
    let src = base.join("big.bin");
    // 23 MiB at 5 MiB parts: five parts, the last one short.
    fs::write(&src, pseudo_random(23 * 1024 * 1024 + 123, 7)).unwrap();

    server.rs3_ok(&["cp", "-s", "5MiB", src.to_str().unwrap(), "test/mpart/big.bin"]);
    let stat = server.rs3_ok(&["--json", "stat", "test/mpart/big.bin"]);
    assert!(stat.contains("-5"), "expected a 5-part multipart ETag: {stat}");

    let down = base.join("down.bin");
    server.rs3_ok(&["cp", "test/mpart/big.bin", down.to_str().unwrap()]);
    assert_same_file(&src, &down);

    server.rs3_ok(&["cp", "-s", "5MiB", "test/mpart/big.bin", "test/mpart/copy/big.bin"]);
    let copied = base.join("copied.bin");
    server.rs3_ok(&["cp", "test/mpart/copy/big.bin", copied.to_str().unwrap()]);
    assert_same_file(&src, &copied);

    let mirror_dst = base.join("mirror");
    server.rs3_ok(&["mirror", "test/mpart/copy", mirror_dst.to_str().unwrap()]);
    assert_same_file(&src, &mirror_dst.join("big.bin"));

    let piped = Command::new(env!("CARGO_BIN_EXE_rs3"))
        .args(["pipe", "-s", "5MiB", "--concurrent", "3", "test/mpart/piped.bin"])
        .env("MC_HOST_TEST", server.mc_host())
        .env("MC_CONFIG_DIR", server.dir.path().join("mc-config"))
        .stdin(fs::File::open(&src).unwrap())
        .output()
        .expect("run rs3 pipe");
    assert!(
        piped.status.success(),
        "pipe failed: {}",
        String::from_utf8_lossy(&piped.stderr)
    );
    let piped_down = base.join("piped.bin");
    server.rs3_ok(&["cp", "test/mpart/piped.bin", piped_down.to_str().unwrap()]);
    assert_same_file(&src, &piped_down);
}

/// Keys whose meaning lives in whitespace and XML-special characters. The
/// old DeleteObjects parser trimmed `<Key>` text, so a bulk delete of
/// `del/report ` removed nothing (the trimmed key did not exist) and the
/// object survived; `a&b<c>` exercises entity decoding on the way in and
/// escaping on the way out.
#[cfg(unix)]
#[test]
fn whitespace_and_xml_special_keys_survive_upload_list_and_bulk_delete() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/wspace"]);
    let names = [
        "report ",
        " leading",
        "a&b<c>'\".txt",
        "unicode-日本語.txt",
        "report",
    ];
    let tree = server.dir.path().join("ws-src");
    for dir in ["keep", "del"] {
        fs::create_dir_all(tree.join(dir)).unwrap();
        for name in names {
            fs::write(tree.join(dir).join(name), format!("{dir}:{name}")).unwrap();
        }
    }
    server.rs3_ok(&["cp", "--recursive", tree.to_str().unwrap(), "test/wspace/"]);

    let mut listed = keys(&server, "wspace", "");
    listed.sort();
    let mut expected: Vec<String> = ["keep", "del"]
        .iter()
        .flat_map(|d| names.iter().map(move |n| format!("{d}/{n}")))
        .collect();
    expected.sort();
    assert_eq!(listed, expected, "keys must round-trip byte-exact");

    for name in names {
        let got = server.rs3_ok(&["cat", &format!("test/wspace/keep/{name}")]);
        assert_eq!(got, format!("keep:{name}"), "content of {name:?}");
    }

    server.rs3_ok(&["rm", "--recursive", "--force", "test/wspace/del/"]);
    assert_eq!(
        keys(&server, "wspace", "del/"),
        Vec::<String>::new(),
        "bulk delete must remove every key exactly as named"
    );
    assert_eq!(keys(&server, "wspace", "keep/").len(), names.len());
}

/// More common prefixes than one listing page holds. Every prefix appears
/// exactly once across pages, and no page is an empty "IsTruncated" tail.
#[test]
fn non_recursive_listing_pages_across_more_than_a_thousand_prefixes() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/pages"]);
    let tree = server.dir.path().join("pg");
    const DIRS: usize = 1003;
    for i in 0..DIRS {
        let d = tree.join(format!("d{i:04}"));
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join("a"), b"a").unwrap();
        fs::write(d.join("b"), b"b").unwrap();
    }
    fs::write(tree.join("top.txt"), b"top").unwrap();
    server.rs3_ok(&["cp", "--recursive", &format!("{}/", tree.display()), "test/pages/"]);

    let out = server.rs3_ok(&["--json", "ls", "test/pages/"]);
    let mut prefixes = Vec::new();
    let mut files = Vec::new();
    for line in out.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        let key = v["key"].as_str().unwrap().to_string();
        if key.ends_with('/') {
            prefixes.push(key);
        } else {
            files.push(key);
        }
    }
    let unique: std::collections::BTreeSet<_> = prefixes.iter().cloned().collect();
    assert_eq!(prefixes.len(), DIRS, "one entry per prefix: {:?}", &prefixes[..5]);
    assert_eq!(unique.len(), DIRS, "no prefix listed twice");
    assert_eq!(files, vec!["top.txt".to_string()]);

    assert_eq!(keys(&server, "pages", "").len(), DIRS * 2 + 1);
}

/// `share upload` hand-rolls a POST policy. The server now enforces every
/// condition and refuses fields the policy does not cover, so the generated
/// command must still work as printed -- and must stop working the moment
/// anyone edits it.
#[cfg(unix)]
#[test]
fn share_upload_command_works_and_its_policy_is_enforced() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/pol"]);
    let out = server.rs3_ok(&[
        "--json",
        "share",
        "upload",
        "-T",
        "text/plain",
        "test/pol/note.txt",
    ]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    let cmd = v["share"].as_str().unwrap().to_string();
    let payload = server.dir.path().join("note.txt");
    fs::write(&payload, b"policy ok").unwrap();
    let cmd = cmd.replace("<FILE>", payload.to_str().unwrap());
    assert!(cmd.contains("Content-Type=text/plain"), "cmd: {cmd}");

    let run = |extra: &str, from: &str, to: &str| -> String {
        let edited = cmd.replacen("curl ", &format!("curl -s -o /dev/null -w %{{http_code}} {extra} "), 1);
        let edited = edited.replace(from, to);
        let out = Command::new("sh").arg("-c").arg(&edited).output().unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    };

    // An extra field the policy never mentions.
    let status = run("-F x-amz-meta-evil=1", "", "");
    assert!(status.starts_with('4'), "uncovered field accepted: {status}");
    // A Content-Type that violates the eq condition.
    let status = run("", "Content-Type=text/plain", "Content-Type=text/html");
    assert!(status.starts_with('4'), "wrong Content-Type accepted: {status}");
    assert!(keys(&server, "pol", "").is_empty(), "a rejected POST stored something");

    // The command exactly as printed.
    let status = run("", "", "");
    assert!(status.starts_with('2'), "generated command rejected: {status}");
    assert_eq!(server.rs3_ok(&["cat", "test/pol/note.txt"]), "policy ok");
    assert_eq!(
        content_type(&server, "pol", "note.txt").as_deref(),
        Some("text/plain")
    );
}

/// A presigned URL from `share download` signs only `host`. Adding an
/// unsigned `x-amz-*` header must invalidate it (it could otherwise turn
/// the request into something the signer never authorised), while the URL
/// as issued keeps working.
#[cfg(unix)]
#[test]
fn share_download_url_rejects_unsigned_amz_headers() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/sig"]);
    let src = server.dir.path().join("s.txt");
    fs::write(&src, b"signed").unwrap();
    server.rs3_ok(&["cp", src.to_str().unwrap(), "test/sig/s.txt"]);

    let out = server.rs3_ok(&["--json", "share", "download", "test/sig/s.txt"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    let url = v["share"].as_str().unwrap().to_string();

    assert_eq!(curl_status(&[&url]), "200");
    assert_eq!(
        curl_status(&["-H", "x-amz-meta-injected: 1", &url]),
        "403",
        "unsigned x-amz header must break the signature"
    );
    // Duplicate query parameters are refused before auth even runs.
    assert_eq!(curl_status(&[&format!("{url}&X-Amz-Expires=1")]), "400");
}

/// `/minio/v2/metrics/` used to authenticate as root by prefix. With a
/// bucket literally named `minio`, anonymous requests under that prefix
/// reached the object routes.
#[cfg(unix)]
#[test]
fn metrics_prefix_grants_nothing_on_a_bucket_named_minio() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/minio"]);
    let base = format!("http://127.0.0.1:{}", server.port);

    let put = curl_status(&["-X", "PUT", "--data-binary", "x", &format!("{base}/minio/v2/metrics/pwn")]);
    assert_eq!(put, "403", "anonymous PUT under the metrics prefix");
    let del = curl_status(&["-X", "DELETE", &format!("{base}/minio/v2/metrics/cluster")]);
    assert_ne!(&del[..1], "2", "anonymous DELETE on a probe path: {del}");
    assert!(keys(&server, "minio", "").is_empty());

    // The real probe stays open.
    assert_eq!(curl_status(&[&format!("{base}/minio/health/live")]), "200");
}
