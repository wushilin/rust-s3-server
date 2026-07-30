mod common;
use common::TestServer;

#[test]
fn head_prints_first_n_lines() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/headb"]);
    let src = server.dir.path().join("lines.txt");
    std::fs::write(
        &src,
        (1..=50).map(|i| format!("line{i}\n")).collect::<String>(),
    )
    .unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/headb/lines.txt"]);
    let out = server.rs3_ok(&["head", "test/headb/lines.txt"]);
    assert_eq!(out.lines().count(), 10, "default -n 10: {out}");
    assert_eq!(out.lines().next().unwrap(), "line1");
    let out = server.rs3_ok(&["head", "-n", "3", "test/headb/lines.txt"]);
    assert_eq!(
        out.lines().collect::<Vec<_>>(),
        vec!["line1", "line2", "line3"]
    );
}

#[test]
fn head_negative_lines_defaults_to_ten() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/headneg"]);
    let src = server.dir.path().join("lines.txt");
    std::fs::write(
        &src,
        (1..=50).map(|i| format!("line{i}\n")).collect::<String>(),
    )
    .unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/headneg/lines.txt"]);
    let out = server.rs3_ok(&["head", "-n", "-5", "test/headneg/lines.txt"]);
    assert_eq!(out.lines().count(), 10, "negative -n resets to 10: {out}");
}

#[test]
fn head_normalizes_crlf_to_unix_newlines() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/headcrlf"]);
    let src = server.dir.path().join("crlf.txt");
    std::fs::write(&src, b"line1\r\nline2\r\nline3\r\n").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/headcrlf/crlf.txt"]);
    let out = server.rs3_ok(&["head", "-n", "2", "test/headcrlf/crlf.txt"]);
    assert_eq!(out, "line1\nline2\n");
}

#[test]
fn head_decodes_gzip_by_content_type() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/headgz"]);

    // Build a gzip-compressed payload in-process (avoids depending on a
    // system `gzip` binary being present in the test environment).
    let plain: String = (1..=20).map(|i| format!("gzline{i}\n")).collect();
    let src = server.dir.path().join("lines.txt.gz");
    {
        use std::io::Write;
        let file = std::fs::File::create(&src).unwrap();
        let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        encoder.write_all(plain.as_bytes()).unwrap();
        encoder.finish().unwrap();
    }

    server.rs3_ok(&[
        "put",
        "--attr",
        "Content-Type=application/gzip",
        src.to_str().unwrap(),
        "test/headgz/lines.txt.gz",
    ]);

    let out = server.rs3_ok(&["head", "-n", "3", "test/headgz/lines.txt.gz"]);
    assert_eq!(
        out.lines().collect::<Vec<_>>(),
        vec!["gzline1", "gzline2", "gzline3"],
        "expected gzip-decoded lines: {out}"
    );
}

#[test]
fn head_multiple_targets_streams_each_in_sequence() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/headmulti"]);
    let a = server.dir.path().join("a.txt");
    let b = server.dir.path().join("b.txt");
    std::fs::write(&a, "a1\na2\n").unwrap();
    std::fs::write(&b, "b1\nb2\n").unwrap();
    server.rs3_ok(&["put", a.to_str().unwrap(), "test/headmulti/a.txt"]);
    server.rs3_ok(&["put", b.to_str().unwrap(), "test/headmulti/b.txt"]);
    let out = server.rs3_ok(&["head", "test/headmulti/a.txt", "test/headmulti/b.txt"]);
    assert_eq!(out, "a1\na2\nb1\nb2\n");
}

#[test]
fn head_bzip2_content_type_is_hard_error() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/headbz"]);
    let src = server.dir.path().join("f.bz2");
    std::fs::write(&src, b"not really bzip2 but content-type drives this").unwrap();
    server.rs3_ok(&[
        "put",
        "--attr",
        "Content-Type=application/bzip2",
        src.to_str().unwrap(),
        "test/headbz/f.bz2",
    ]);
    let out = server.rs3(&["head", "test/headbz/f.bz2"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("bzip2-compressed objects are not supported yet"),
        "stderr: {stderr}"
    );
}

#[test]
fn cat_offset_and_tail() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/catb"]);
    let src = server.dir.path().join("abc.txt");
    std::fs::write(&src, b"0123456789").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/catb/abc.txt"]);
    assert_eq!(
        server.rs3_ok(&["cat", "--offset", "7", "test/catb/abc.txt"]),
        "789"
    );
    assert_eq!(
        server.rs3_ok(&["cat", "--tail", "3", "test/catb/abc.txt"]),
        "789"
    );
    assert_eq!(
        server.rs3_ok(&["cat", "--tail", "99", "test/catb/abc.txt"]),
        "0123456789"
    );
    let out = server.rs3(&["cat", "--tail", "1", "--offset", "1", "test/catb/abc.txt"]);
    assert!(!out.status.success());
}

#[test]
fn cat_negative_offset_or_tail_is_fatal() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/catn"]);
    let src = server.dir.path().join("abc.txt");
    std::fs::write(&src, b"0123456789").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/catn/abc.txt"]);
    let out = server.rs3(&["cat", "--offset", "-1", "test/catn/abc.txt"]);
    assert!(!out.status.success());
    let out = server.rs3(&["cat", "--tail", "-1", "test/catn/abc.txt"]);
    assert!(!out.status.success());
}

#[test]
fn cat_part_number_with_tail_or_offset_is_fatal() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/catpn"]);
    let src = server.dir.path().join("abc.txt");
    std::fs::write(&src, b"0123456789").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/catpn/abc.txt"]);
    let out = server.rs3(&[
        "cat",
        "--part-number",
        "1",
        "--offset",
        "1",
        "test/catpn/abc.txt",
    ]);
    assert!(!out.status.success());
    let out = server.rs3(&[
        "cat",
        "--part-number",
        "1",
        "--tail",
        "1",
        "test/catpn/abc.txt",
    ]);
    assert!(!out.status.success());
}

#[test]
fn stat_recursive_walks_prefix() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/statr"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/statr/p/a.txt"]);
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/statr/p/b.txt"]);
    let out = server.rs3_ok(&["stat", "-r", "test/statr/p/"]);
    assert!(out.contains("a.txt") && out.contains("b.txt"), "out: {out}");
}

#[test]
fn stat_recursive_empty_prefix_is_success_no_output() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/statempty"]);
    let out = server.rs3_ok(&["stat", "-r", "test/statempty/nosuchprefix/"]);
    assert_eq!(out, "");
}

#[test]
fn stat_nonrecursive_missing_key_is_error() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/statmiss"]);
    let out = server.rs3(&["stat", "test/statmiss/nosuch.txt"]);
    assert!(!out.status.success());
}

#[test]
fn du_depth_semantics() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dub"]);
    let src = server.dir.path().join("k.bin");
    std::fs::write(&src, vec![0u8; 1024]).unwrap();
    for key in ["a/1.bin", "a/sub/2.bin", "b/3.bin"] {
        server.rs3_ok(&["put", src.to_str().unwrap(), &format!("test/dub/{key}")]);
    }
    // plain du => depth 1 => single total line for the target
    let out = server.rs3_ok(&["du", "test/dub"]);
    assert_eq!(out.lines().count(), 1, "plain du = one line: {out}");
    assert!(
        out.contains("3.0KiB") && out.contains("3 objects"),
        "out: {out}"
    );
    // du -r => unlimited => one line per directory level + total (a/sub, a, b, root = 4 lines)
    let out = server.rs3_ok(&["du", "-r", "test/dub"]);
    assert_eq!(out.lines().count(), 4, "out: {out}");
    assert!(
        out.lines()
            .any(|l| l.ends_with("a/sub") && l.contains("1 object")),
        "out: {out}"
    );
    // du -d 1: children aggregated silently, single top line
    let json = server.rs3_ok(&["--json", "du", "test/dub"]);
    let v: serde_json::Value = serde_json::from_str(json.lines().last().unwrap()).unwrap();
    assert_eq!(v["objects"], 3);
    assert_eq!(v["size"], 3072);
}

#[test]
fn tree_draws_branches_and_json_aliases_ls() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/treeb"]);
    let src = server.dir.path().join("t.txt");
    std::fs::write(&src, b"x").unwrap();
    for key in ["a/1.txt", "a/sub/2.txt", "b/3.txt"] {
        server.rs3_ok(&["put", src.to_str().unwrap(), &format!("test/treeb/{key}")]);
    }
    let out = server.rs3_ok(&["tree", "test/treeb"]);
    assert!(out.contains("├─ a") || out.contains("├─ a/"), "out: {out}");
    assert!(out.contains("└─ "), "out: {out}");
    assert!(
        !out.contains("1.txt"),
        "files hidden without --files: {out}"
    );
    // byte-for-byte against a real `mc tree` run on the same layout
    // ([task 11] ground-truth comparison): root line first (verbatim
    // target), then "a" (non-last, "├─ "), its child "sub" continued under
    // "│  ", then "b" (last, "└─ ").
    assert_eq!(
        out, "test/treeb\n├─ a\n│  └─ sub\n└─ b\n",
        "byte-for-byte vs real mc tree: {out}"
    );

    let out = server.rs3_ok(&["tree", "--files", "test/treeb"]);
    assert!(out.contains("1.txt"), "out: {out}");
    assert_eq!(
        out, "test/treeb\n├─ a\n│  ├─ 1.txt\n│  └─ sub\n│     └─ 2.txt\n└─ b\n   └─ 3.txt\n",
        "byte-for-byte vs real mc tree --files: {out}"
    );

    // --depth 1: level-1 dirs (a, b) are expanded, but level-2 dirs (sub)
    // are shown without being descended into further.
    let out = server.rs3_ok(&["tree", "--files", "--depth", "1", "test/treeb"]);
    assert_eq!(
        out, "test/treeb\n├─ a\n│  ├─ 1.txt\n│  └─ sub\n└─ b\n   └─ 3.txt\n",
        "out: {out}"
    );

    let out = server.rs3(&["tree", "--depth", "0", "test/treeb"]);
    assert!(!out.status.success(), "depth 0 must be rejected");
    let out = server.rs3(&["tree", "--depth", "-2", "test/treeb"]);
    assert!(!out.status.success(), "depth < -1 must be rejected");

    // a target with no visible children (fully empty bucket) prints
    // nothing at all -- not even the root line.
    server.rs3_ok(&["mb", "test/emptytreeb"]);
    let out = server.rs3_ok(&["tree", "test/emptytreeb"]);
    assert_eq!(out, "", "empty target prints nothing: {out}");

    let out = server.rs3_ok(&["--json", "tree", "test/treeb"]);
    let first: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
    assert!(
        first.get("key").is_some(),
        "tree --json = ls -r --json: {out}"
    );
    assert_eq!(
        out.lines().count(),
        3,
        "recursive, one line per object: {out}"
    );
}
