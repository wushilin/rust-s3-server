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

#[test]
fn mirror_remove_does_not_delete_age_excluded_but_still_present_source() {
    // Regression test: mc applies --older-than/--newer-than per diff-URL,
    // AFTER planning, and only to skip COPIES (mc's own
    // isOlder/isNewer, cmd/mirror-main.go:826-838) -- source presence for
    // --remove's extraneous-target detection is always computed
    // unfiltered. If the time filter were applied to source_entries
    // *before* plan_mirror, an age-excluded (but still present) source
    // object would look absent, so its still-current target twin would
    // be wrongly deleted under --remove.
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/flt7"]);
    let src = server.dir.path().join("cpsrc7");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("keep.txt"), b"keep").unwrap();

    // First mirror (no filter) establishes the target twin.
    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/flt7/p"]);
    server.rs3_ok(&["stat", "test/flt7/p/keep.txt"]);

    // keep.txt is unchanged in source (age ~0), so --older-than 1000d
    // excludes it from being re-copied -- but it must NOT be treated as
    // absent for --remove's delete side, since it's still really there.
    let out = server.rs3(&[
        "mirror",
        "--remove",
        "--older-than",
        "1000d",
        src.to_str().unwrap(),
        "test/flt7/p",
    ]);
    assert!(
        out.status.success(),
        "mirror --remove --older-than failed: stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("Removed"),
        "age-excluded-but-present source must not trigger a delete: {stdout}"
    );
    server.rs3_ok(&["stat", "test/flt7/p/keep.txt"]);
}

// --attr threads a system header (Content-Type, routed onto PutObject's own
// field) and a user-metadata key (X-Amz-Meta-Color, routed through
// `.metadata()`, prefix stripped since the SDK re-adds it on the wire) in
// the same quoted `key=value;key=value` spec ([SEM] §2). rusts3's put
// handler only persists Content-Type/Content-Encoding/Content-Language plus
// user metadata (see `extract_user_meta` in `src/server/mod.rs`) -- it drops
// Cache-Control/Content-Disposition/Expires entirely, so this asserts
// against the two headers the real e2e server actually round-trips rather
// than the full system-header set the client itself supports.
#[test]
fn put_attr_sets_system_header_and_user_metadata() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/attr1"]);
    let src = server.dir.path().join("a.txt");
    std::fs::write(&src, b"hello").unwrap();
    server.rs3_ok(&[
        "put",
        "--attr",
        "Content-Type=text/x-custom;X-Amz-Meta-Color=red",
        src.to_str().unwrap(),
        "test/attr1/a.txt",
    ]);
    let out = server.rs3_ok(&["stat", "--json", "test/attr1/a.txt"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(v["metadata"]["Content-Type"], "text/x-custom");
    // rusts3 lowercases user-metadata keys on the wire (`extract_user_meta`),
    // so the SDK's HeadObject response -- and thus rs3's own stat output --
    // reports it back as `color`, not the canonical `Color` the --attr spec
    // parsed to.
    assert_eq!(v["metadata"]["color"], "red");
}

#[test]
fn put_if_not_exists_rejects_second_write_to_same_key() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/attr2"]);
    let src = server.dir.path().join("a.txt");
    std::fs::write(&src, b"first").unwrap();
    server.rs3_ok(&[
        "put",
        "--if-not-exists",
        src.to_str().unwrap(),
        "test/attr2/a.txt",
    ]);
    let second = server.rs3(&[
        "put",
        "--if-not-exists",
        src.to_str().unwrap(),
        "test/attr2/a.txt",
    ]);
    assert!(
        !second.status.success(),
        "second --if-not-exists put should fail once the key exists"
    );
    // The original object must survive the rejected second write.
    server.rs3_ok(&["stat", "test/attr2/a.txt"]);
}

// NOTE: there is no multipart counterpart to
// `put_if_not_exists_rejects_second_write_to_same_key` here. The client
// wires `if_not_exists` onto CompleteMultipartUpload's `If-None-Match: *`
// per [SEM] §2 (not CreateMultipartUpload), but rusts3's
// `complete_multipart` handler doesn't implement conditional-write
// preconditions at all (unlike its `put_object` handler, which does) --
// see `src/server/handlers/complete_multipart/lib.rs`. That path is
// implemented per spec but isn't e2e-verifiable against this test server.

// cp/mirror only support --attr for uploads (local -> S3); a same- or
// cross-endpoint S3 -> S3 copy path would need metadata_directive=REPLACE
// threaded through several copy code paths, which is out of scope here, so
// it hard-errors instead of silently ignoring the flag.
#[test]
fn cp_attr_s3_to_s3_is_rejected() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/attr4"]);
    let src = server.dir.path().join("a.txt");
    std::fs::write(&src, b"hello").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/attr4/a.txt"]);
    let out = server.rs3(&[
        "cp",
        "--attr",
        "X-Amz-Meta-Color=red",
        "test/attr4/a.txt",
        "test/attr4/b.txt",
    ]);
    assert!(
        !out.status.success(),
        "cp --attr between two S3 objects should be rejected"
    );
}

// `--preserve`/`-a` round-trips filesystem attributes (mode, timestamps)
// through `X-Amz-Meta-Mc-Attrs`: upload encodes the source file's stat into
// the header, download parses it back out and applies it to the new local
// file ([SEM] §2). rusts3 persists arbitrary user metadata, so this is
// fully e2e-verifiable against the real test server, unlike the
// if-not-exists multipart case above.
#[cfg(unix)]
#[test]
fn cp_preserve_roundtrips_mode_and_mtime() {
    use std::os::unix::fs::PermissionsExt;

    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/preserve1"]);
    let src = server.dir.path().join("src.txt");
    std::fs::write(&src, b"hello").unwrap();
    std::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o600)).unwrap();
    let src_mtime = std::fs::metadata(&src).unwrap().modified().unwrap();

    server.rs3_ok(&[
        "cp",
        "--preserve",
        src.to_str().unwrap(),
        "test/preserve1/src.txt",
    ]);

    let dst = server.dir.path().join("dst.txt");
    server.rs3_ok(&[
        "cp",
        "--preserve",
        "test/preserve1/src.txt",
        dst.to_str().unwrap(),
    ]);

    let dst_meta = std::fs::metadata(&dst).unwrap();
    assert_eq!(
        dst_meta.permissions().mode() & 0o777,
        0o600,
        "mode should round-trip through --preserve"
    );
    let dst_mtime = dst_meta.modified().unwrap();
    let src_secs = src_mtime
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let dst_secs = dst_mtime
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert_eq!(
        src_secs, dst_secs,
        "mtime should round-trip through --preserve (to the second)"
    );
}

// Without `--preserve`, cp/put never touch the target's mode/mtime -- a
// plain cp of a chmod'd file must not leak fs attrs onto the new local
// file.
#[cfg(unix)]
#[test]
fn cp_without_preserve_does_not_apply_attrs() {
    use std::os::unix::fs::PermissionsExt;

    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/preserve2"]);
    let src = server.dir.path().join("src.txt");
    std::fs::write(&src, b"hello").unwrap();
    std::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o600)).unwrap();

    server.rs3_ok(&["cp", src.to_str().unwrap(), "test/preserve2/src.txt"]);

    let dst = server.dir.path().join("dst.txt");
    // Pre-create with a different mode; a non-preserving cp should not
    // touch it since it never even sees an `Mc-Attrs` header to apply.
    std::fs::write(&dst, b"placeholder").unwrap();
    std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o644)).unwrap();
    server.rs3_ok(&["cp", "test/preserve2/src.txt", dst.to_str().unwrap()]);

    let dst_meta = std::fs::metadata(&dst).unwrap();
    assert_eq!(
        dst_meta.permissions().mode() & 0o777,
        0o644,
        "cp without --preserve must not apply the source's mode"
    );
}

// The streaming cross-endpoint S3-to-S3 path can't cheaply carry source
// metadata onto the target PUT the way same-endpoint CopyObject's COPY
// directive does for free, so `--preserve` hard-errors there rather than
// silently dropping it -- mirrors `cp_attr_s3_to_s3_is_rejected` above,
// which does the same for `--attr`.
#[test]
fn cp_preserve_s3_to_s3_same_endpoint_succeeds() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/preserve3"]);
    let src = server.dir.path().join("a.txt");
    std::fs::write(&src, b"hello").unwrap();
    server.rs3_ok(&[
        "put",
        "--preserve",
        src.to_str().unwrap(),
        "test/preserve3/a.txt",
    ]);
    // Same-endpoint S3->S3 copy: server-side CopyObject's default COPY
    // metadata directive carries `Mc-Attrs` over with zero extra code, so
    // this must succeed (unlike the cross-endpoint streaming path).
    server.rs3_ok(&[
        "cp",
        "--preserve",
        "test/preserve3/a.txt",
        "test/preserve3/b.txt",
    ]);
    let out = server.rs3_ok(&["stat", "--json", "test/preserve3/b.txt"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert!(
        v["metadata"]["mc-attrs"].is_string(),
        "Mc-Attrs should have been carried over by server-side copy: {out}"
    );
}
