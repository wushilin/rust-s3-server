mod common;
use common::TestServer;

fn seed(server: &TestServer, bucket: &str, keys: &[&str]) {
    server.rs3_ok(&["mb", &format!("test/{bucket}")]);
    let src = server.dir.path().join("seed.txt");
    std::fs::write(&src, b"x").unwrap();
    for key in keys {
        server.rs3_ok(&[
            "put",
            src.to_str().unwrap(),
            &format!("test/{bucket}/{key}"),
        ]);
    }
}

#[test]
fn rm_recursive_requires_force() {
    let server = TestServer::start();
    seed(&server, "bk1", &["a/1.txt"]);
    let out = server.rs3(&["rm", "--recursive", "test/bk1/a/"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--force"));
    // object must still exist
    server.rs3_ok(&["stat", "test/bk1/a/1.txt"]);
}

#[test]
fn rm_recursive_force_deletes_prefix_only() {
    let server = TestServer::start();
    seed(
        &server,
        "bk2",
        &["keep.txt", "p/1.txt", "p/2.txt", "p/deep/3.txt"],
    );
    server.rs3_ok(&["rm", "--recursive", "--force", "test/bk2/p/"]);
    let listing = server.rs3_ok(&["ls", "--recursive", "test/bk2"]);
    assert!(listing.contains("keep.txt"), "listing: {listing}");
    assert!(!listing.contains("p/"), "listing: {listing}");
}

#[test]
fn rm_dry_run_deletes_nothing() {
    let server = TestServer::start();
    seed(&server, "bk3", &["p/1.txt"]);
    let out = server.rs3_ok(&["rm", "--recursive", "--dry-run", "test/bk3/p/"]);
    assert!(out.contains("p/1.txt"));
    server.rs3_ok(&["stat", "test/bk3/p/1.txt"]);
}

#[test]
fn rm_missing_key_fails_but_later_targets_run() {
    let server = TestServer::start();
    seed(&server, "bk4", &["real.txt"]);
    let out = server.rs3(&["rm", "test/bk4/ghost.txt", "test/bk4/real.txt"]);
    assert!(!out.status.success());
    // second target was still processed
    let stat = server.rs3(&["stat", "test/bk4/real.txt"]);
    assert!(!stat.status.success());
}

#[test]
fn rm_recursive_force_deletes_folder_markers_too() {
    let server = TestServer::start();
    seed(&server, "bk6", &["p/f.txt"]);
    // Zero-byte "folder marker" object, as produced by some S3 clients/UIs
    // when creating an empty "folder". rs3's own `put` can't create a key
    // ending in `/`, so we PUT it directly via aws-sdk-s3.
    server.put_marker("bk6", "p/sub/");
    server.rs3_ok(&["rm", "--recursive", "--force", "test/bk6/p/"]);
    let listing = server.rs3_ok(&["ls", "--recursive", "test/bk6"]);
    assert!(!listing.contains("p/"), "listing: {listing}");
    // The bucket must now be truly empty -- including the marker -- so a
    // plain `rb` (no --force) succeeds.
    server.rs3_ok(&["rb", "test/bk6"]);
}

#[test]
fn rm_recursive_no_trailing_slash_does_not_leak_into_sibling_prefix() {
    let server = TestServer::start();
    seed(&server, "bk5", &["p/1.txt", "prefix2/x.txt"]);
    // No trailing slash on `p` -- must not match sibling prefix `prefix2/`.
    server.rs3_ok(&["rm", "--recursive", "--force", "test/bk5/p"]);
    let stat_gone = server.rs3(&["stat", "test/bk5/p/1.txt"]);
    assert!(!stat_gone.status.success());
    server.rs3_ok(&["stat", "test/bk5/prefix2/x.txt"]);
}
