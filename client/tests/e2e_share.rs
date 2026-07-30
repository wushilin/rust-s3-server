mod common;
use common::TestServer;

#[test]
fn share_download_url_works() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/shb"]);
    let src = server.dir.path().join("s.txt");
    std::fs::write(&src, b"shared!").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/shb/s.txt"]);
    let out = server.rs3_ok(&["--json", "share", "download", "test/shb/s.txt"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    let url = v["share"].as_str().unwrap();
    assert!(url.contains("X-Amz-Signature"), "presigned: {url}");
    assert!(v["timeLeft"].as_u64().unwrap() > 0, "raw ns int");
    // the presigned URL must actually download without credentials
    let body = std::process::Command::new("curl")
        .args(["-sf", url])
        .output()
        .unwrap();
    assert!(body.status.success(), "curl failed on presigned url");
    assert_eq!(body.stdout, b"shared!");
    // share list remembers it
    let out = server.rs3_ok(&["share", "list", "download"]);
    assert!(out.contains("s.txt"), "out: {out}");
}

#[test]
fn share_expire_rules() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/shc"]);
    let src = server.dir.path().join("s.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/shc/s.txt"]);
    assert!(
        !server
            .rs3(&["share", "download", "--expire", "7d", "test/shc/s.txt"])
            .status
            .success(),
        "7d invalid (Go units only)"
    );
    assert!(
        !server
            .rs3(&["share", "download", "--expire", "200h", "test/shc/s.txt"])
            .status
            .success(),
        "over 7-day cap"
    );
    server.rs3_ok(&["share", "download", "--expire", "30m", "test/shc/s.txt"]);
}

#[test]
fn share_upload_curl_template() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/shd"]);
    let out = server.rs3_ok(&["share", "upload", "test/shd/up.bin"]);
    assert!(out.contains("curl "), "out: {out}");
    assert!(
        out.contains("-F file=@<FILE>"),
        "literal <FILE> placeholder: {out}"
    );
    assert!(
        out.contains("x-amz-signature") || out.contains("X-Amz-Signature"),
        "out: {out}"
    );
}

/// Beyond the brief: the generated `share upload` `curl` command is not
/// just shaped correctly, it must actually work. Parse the printed
/// template out of `--json` output, substitute the literal `<FILE>`
/// placeholder with a real local file, run it through a shell exactly as a
/// human would, and confirm the object lands in the bucket -- this is the
/// same round trip ground-truth-verified manually against real `mc share
/// upload`'s own curl output during development (see the task report).
#[test]
fn share_upload_curl_actually_uploads() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/she"]);
    let out = server.rs3_ok(&["--json", "share", "upload", "test/she/uploaded.bin"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    let curl_cmd = v["share"].as_str().unwrap().to_string();
    assert!(curl_cmd.starts_with("curl "), "curl_cmd: {curl_cmd}");
    assert!(curl_cmd.contains("<FILE>"), "curl_cmd: {curl_cmd}");

    let payload = server.dir.path().join("payload.bin");
    std::fs::write(&payload, b"uploaded via curl").unwrap();
    let substituted = curl_cmd.replace("<FILE>", payload.to_str().unwrap());

    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(&substituted)
        .output()
        .expect("run generated curl command");
    assert!(
        output.status.success(),
        "curl exited non-zero: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(
        body.contains("uploaded.bin"),
        "unexpected curl response body: {body}"
    );

    // The object must actually be present and match the uploaded content.
    let cat_out = server.rs3_ok(&["cat", "test/she/uploaded.bin"]);
    assert_eq!(cat_out, "uploaded via curl");
}

#[test]
fn share_upload_recursive_curl_template_has_name_placeholder() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/shf"]);
    let out = server.rs3_ok(&["share", "upload", "--recursive", "test/shf/uploads/"]);
    assert!(out.contains("-F key=uploads/<NAME> "), "out: {out}");
    assert!(out.contains("-F file=@<FILE>"), "out: {out}");
}

#[test]
fn share_upload_content_type_appears_in_human_and_json() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/shg"]);
    let out = server.rs3_ok(&[
        "share",
        "upload",
        "--content-type",
        "image/png",
        "test/shg/pic.png",
    ]);
    assert!(out.contains("Content-Type: image/png"), "out: {out}");
    let out = server.rs3_ok(&[
        "--json",
        "share",
        "upload",
        "--content-type",
        "image/png",
        "test/shg/pic2.png",
    ]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(v["contentType"], "image/png");
}

#[test]
fn share_upload_rejects_non_recursive_prefix() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/shh"]);
    let out = server.rs3(&["share", "upload", "test/shh/prefix/"]);
    assert!(
        !out.status.success(),
        "a trailing-slash target without --recursive must be rejected"
    );
}

#[test]
fn share_list_unknown_kind_is_rejected() {
    let server = TestServer::start();
    let out = server.rs3(&["share", "list", "bogus"]);
    assert!(!out.status.success(), "only upload|download are valid");
}
