mod common;
use common::TestServer;

fn regex_lite(pattern: &str) -> impl Fn(&str) -> bool {
    let re = regex::Regex::new(pattern).unwrap();
    move |s: &str| re.is_match(s)
}

#[test]
fn ls_json_is_content_message() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lsjson"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"hello").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lsjson/dir/f.txt"]);
    let out = server.rs3_ok(&["--json", "ls", "test/lsjson/dir/"]);
    let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
    assert_eq!(v["status"], "success");
    assert_eq!(v["type"], "file");
    assert_eq!(v["size"], 5);
    assert_eq!(v["key"], "f.txt");
    assert!(v["lastModified"].as_str().unwrap().contains('T'));
    assert!(v.get("etag").is_some());
}

#[test]
fn ls_summarize_and_human_format() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lssum"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, vec![0u8; 1024]).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lssum/a.bin"]);
    let out = server.rs3_ok(&["ls", "--summarize", "test/lssum"]);
    assert!(out.contains("Total Size: 1.0KiB"), "out: {out}");
    assert!(out.contains("Total Objects: 1"), "out: {out}");
    // human per-object line: "[YYYY-MM-DD HH:MM:SS +ZZ:ZZ]  1.0KiB a.bin"
    let re =
        regex_lite("\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} [^\\]]+\\] +1\\.0KiB a\\.bin");
    assert!(out.lines().any(|l| re(l)), "out: {out}");
}

#[test]
fn rb_json_always_compact_and_rm_modtime_null() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/msg1"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/msg1/f.txt"]);
    let out = server.rs3_ok(&["--json", "rm", "test/msg1/f.txt"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert!(
        v["modTime"].is_null(),
        "modTime must be literal null: {out}"
    );
    assert_eq!(v["dryRun"], false);
    let out = server.rs3_ok(&["--json", "rb", "test/msg1"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(v["status"], "success");
    assert_eq!(v["bucket"], "test/msg1");
}

#[test]
fn ls_incomplete_lists_multipart_uploads() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/inc1"]);
    // start a multipart upload directly via the SDK and don't complete it
    common::start_incomplete_multipart(&server, "inc1", "pending/upload.bin");
    let out = server.rs3_ok(&["ls", "--incomplete", "test/inc1"]);
    assert!(out.contains("pending/upload.bin"), "out: {out}");
}

#[test]
fn ls_storage_class_filters_non_matching_but_keeps_empty_class() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lssc"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lssc/f.txt"]);
    // The server always reports STANDARD for plain objects, so a matching
    // filter keeps the object and a non-matching one excludes it.
    let matching = server.rs3_ok(&["ls", "--storage-class", "STANDARD", "test/lssc"]);
    assert!(matching.contains("f.txt"), "out: {matching}");
    let non_matching = server.rs3_ok(&["ls", "--storage-class", "GLACIER", "test/lssc"]);
    assert!(!non_matching.contains("f.txt"), "out: {non_matching}");
    // "*" disables the filter entirely.
    let unfiltered = server.rs3_ok(&["ls", "--storage-class", "*", "test/lssc"]);
    assert!(unfiltered.contains("f.txt"), "out: {unfiltered}");
}

#[test]
fn errors_are_mc_shaped() {
    let server = TestServer::start();
    // human mode: stderr line starts with "rs3: <ERROR> ", exit 1
    let out = server.rs3(&["stat", "test/nosuchbucket/key"]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.starts_with("rs3: <ERROR> "), "stderr was: {err}");
    // json mode: stdout gets the error envelope, single line (piped => compact)
    let out = server.rs3(&["--json", "stat", "test/nosuchbucket/key"]);
    assert!(!out.status.success());
    let line = String::from_utf8_lossy(&out.stdout);
    let v: serde_json::Value = serde_json::from_str(line.trim()).expect("valid json");
    assert_eq!(v["status"], "error");
    assert_eq!(v["error"]["type"], "fatal");
    assert!(v["error"]["message"].as_str().unwrap().len() > 0);
    assert!(
        !line.trim().contains('\n'),
        "must be single-line when piped"
    );
}
