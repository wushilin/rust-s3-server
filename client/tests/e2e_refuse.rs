mod common;
use common::TestServer;

// rs3 only declares flags it can actually honour. Anything rusts3 does not
// implement -- object versioning, object lock, `--zip` archive listing,
// `--rewind` point-in-time views, `mirror --watch` -- is simply absent from
// the CLI, so clap's own "unexpected argument" parser refuses it. There is no
// bespoke "not implemented yet" message to keep in sync, and `--help` never
// advertises something that cannot work.
//
// A second, much smaller category survives: real capability gaps on flags
// that *do* work in other combinations (e.g. `--attr` works local-to-S3 but
// not S3-to-S3). Those still hard-error at runtime, because the flag itself
// is legitimate and only the specific combination is unsupported.
#[test]
fn unsupported_flags_are_clap_rejected() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/rfc"]);
    let cases: &[&[&str]] = &[
        // versioning / lock / rewind / zip: no rusts3 support, never declared
        &["ls", "--rewind", "7d", "test/rfc"],
        &["ls", "--versions", "test/rfc"],
        &["ls", "--zip", "test/rfc"],
        &["mb", "--with-lock", "test/rfc2"],
        &["mb", "--with-versioning", "test/rfc3"],
        &["rm", "--versions", "test/rfc/nonexistent"],
        &["rm", "--version-id", "abc", "test/rfc/nonexistent"],
        &["get", "--version-id", "abc", "test/rfc/nonexistent"],
        &["head", "--rewind", "7d", "test/rfc/x"],
        &["tree", "--rewind", "7d", "test/rfc"],
        // continuous-watch modes: rs3 is a one-shot transfer tool
        &["mirror", "--watch", "test/rfc", "test/rfc-mirror-target"],
        &["find", "test/rfc", "--watch"],
        // client-side checksum flags rs3 does not implement
        &["cp", "--md5", "a", "b"],
        // `alias import` reads a config blob from stdin; not implemented
        &["alias", "import"],
    ];
    for args in cases {
        let out = server.rs3(args);
        assert!(!out.status.success(), "expected failure for {args:?}");
        // clap's own usage-error path: no app-level refusal text, because no
        // rs3 handler code ever runs -- these flags don't exist at all.
        assert!(
            !String::from_utf8_lossy(&out.stderr).contains("not implemented"),
            "expected a clap usage error (not an app-level refusal) for {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    // buckets from refused mb calls must not exist
    let listing = server.rs3_ok(&["ls", "test/"]);
    assert!(
        !listing.contains("rfc2") && !listing.contains("rfc3"),
        "listing: {listing}"
    );
}

#[test]
fn unsupported_combinations_hard_error() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/rfd"]);
    server.put_marker("rfd", "src.txt");
    // `--attr` is honoured for uploads, but an S3-to-S3 copy has no place to
    // apply it without re-reading the object, so the combination is refused.
    let out = server.rs3(&[
        "cp",
        "--attr",
        "X-Amz-Meta-Color=red",
        "test/rfd/src.txt",
        "test/rfd/dst.txt",
    ]);
    assert!(!out.status.success(), "expected failure");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("not implemented"),
        "wrong error: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
