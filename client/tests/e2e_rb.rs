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
