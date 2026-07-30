mod common;
use common::TestServer;

fn seed(server: &TestServer, bucket: &str, keys: &[&str]) {
    server.rs3_ok(&["mb", &format!("test/{bucket}")]);
    let src = server.dir.path().join("filters-seed.txt");
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
fn rm_recursive_older_than_excludes_fresh_objects() {
    let server = TestServer::start();
    seed(&server, "flt1", &["a.txt", "b.txt"]);
    // Freshly-created objects have age ~0, so --older-than 1000d (which
    // includes only objects whose age >= 1000d) excludes both. A
    // filtered-to-zero `rm -r --force` is still success, and deletes
    // nothing.
    server.rs3_ok(&[
        "rm",
        "--recursive",
        "--force",
        "--older-than",
        "1000d",
        "test/flt1/",
    ]);
    server.rs3_ok(&["stat", "test/flt1/a.txt"]);
    server.rs3_ok(&["stat", "test/flt1/b.txt"]);
}

#[test]
fn rm_recursive_newer_than_includes_fresh_objects() {
    let server = TestServer::start();
    seed(&server, "flt2", &["a.txt", "b.txt"]);
    // --newer-than 1000d includes only objects whose age < 1000d, which
    // both freshly-created objects satisfy, so both get removed.
    server.rs3_ok(&[
        "rm",
        "--recursive",
        "--force",
        "--newer-than",
        "1000d",
        "test/flt2/",
    ]);
    let a = server.rs3(&["stat", "test/flt2/a.txt"]);
    let b = server.rs3(&["stat", "test/flt2/b.txt"]);
    assert!(!a.status.success(), "a.txt should have been removed");
    assert!(!b.status.success(), "b.txt should have been removed");
}

#[test]
fn cp_recursive_older_than_copies_nothing() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/flt3"]);
    let src = server.dir.path().join("cpsrc3");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap();
    std::fs::write(src.join("b.txt"), b"b").unwrap();
    server.rs3_ok(&[
        "cp",
        "--recursive",
        "--older-than",
        "1000d",
        src.to_str().unwrap(),
        "test/flt3/p",
    ]);
    let listing = server.rs3_ok(&["ls", "--recursive", "test/flt3"]);
    assert!(
        listing.trim().is_empty(),
        "listing should be empty: {listing}"
    );
}

#[test]
fn cp_recursive_newer_than_copies_everything() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/flt4"]);
    let src = server.dir.path().join("cpsrc4");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("a.txt"), b"a").unwrap();
    std::fs::write(src.join("b.txt"), b"b").unwrap();
    server.rs3_ok(&[
        "cp",
        "--recursive",
        "--newer-than",
        "1000d",
        src.to_str().unwrap(),
        "test/flt4/p",
    ]);
    let listing = server.rs3_ok(&["ls", "--recursive", "test/flt4"]);
    assert!(
        listing.contains("a.txt") && listing.contains("b.txt"),
        "listing: {listing}"
    );
}

#[test]
fn bad_filter_grammar_is_fatal_before_any_delete() {
    let server = TestServer::start();
    seed(&server, "flt5", &["a.txt"]);
    let out = server.rs3(&[
        "rm",
        "--recursive",
        "--force",
        "--older-than",
        "5x",
        "test/flt5/",
    ]);
    assert!(
        !out.status.success(),
        "bad duration grammar must be rejected"
    );
    // Validation happens up front, before any listing/delete work, so the
    // object must be untouched.
    server.rs3_ok(&["stat", "test/flt5/a.txt"]);
}

#[test]
fn cp_single_object_older_than_skips_silently() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/flt6"]);
    let src = server.dir.path().join("single.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/flt6/src.txt"]);
    // Non-recursive single-object cp: --older-than 1000d excludes the
    // fresh source object, so the copy is skipped -- success, no target.
    server.rs3_ok(&[
        "cp",
        "--older-than",
        "1000d",
        "test/flt6/src.txt",
        "test/flt6/dst.txt",
    ]);
    let dst = server.rs3(&["stat", "test/flt6/dst.txt"]);
    assert!(
        !dst.status.success(),
        "filtered-out cp must not create a target"
    );
}
