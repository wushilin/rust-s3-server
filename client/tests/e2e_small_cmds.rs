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
