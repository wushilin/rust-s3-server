mod common;
use common::TestServer;

// rs3 has two distinct refusal styles for mc flags it doesn't support, and
// this file exercises both:
//
// 1. **Runtime hard-errors**: flags that clap *does* declare (so they parse
//    fine, show up in `--help`, and are visible to `mc`-familiar users) but
//    whose handler immediately bails with an `anyhow!("... is not
//    implemented yet")` before touching the network. These are versioning
//    flags and a handful of others that are meaningful enough to document
//    in `--help` even though rs3 refuses to act on them. Asserted via
//    `unimplemented_flags_hard_error` below: exit non-zero and stderr
//    contains "not implemented".
//
// 2. **Clap-level rejections**: flags rs3 never bothered to declare at all
//    on the relevant `*Args` struct, because they're either MinIO-specific
//    (versioning/lock/`--zip`) or simply out of scope for this tier. clap's
//    own "unexpected argument" parser error does the refusing here -- no
//    handler code needed, no bespoke message to keep in sync. Asserted via
//    `undeclared_flags_are_clap_rejected` below: exit non-zero only (clap
//    exits with its own usage-error code, not the app's error path or
//    message).
#[test]
fn unimplemented_flags_hard_error() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/rfb"]);
    let cases: &[&[&str]] = &[
        &["ls", "--rewind", "7d", "test/rfb"],
        &["ls", "--versions", "test/rfb"],
        &["ls", "--zip", "test/rfb"],
        &["mb", "--with-lock", "test/rfb2"],
        &["mb", "--with-versioning", "test/rfb3"],
        &["rm", "--versions", "test/rfb/nonexistent"],
        &["rm", "--version-id", "abc", "test/rfb/nonexistent"],
        &["get", "--version-id", "abc", "test/rfb/nonexistent"],
        &["mirror", "--watch", "test/rfb", "test/rfb-mirror-target"],
        &["alias", "import"],
    ];
    for args in cases {
        let out = server.rs3(args);
        assert!(!out.status.success(), "expected failure for {args:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("not implemented"),
            "wrong error for {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    // buckets from refused mb calls must not exist
    let listing = server.rs3_ok(&["ls", "test/"]);
    assert!(
        !listing.contains("rfb2") && !listing.contains("rfb3"),
        "listing: {listing}"
    );
}

#[test]
fn undeclared_flags_are_clap_rejected() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/rfc"]);
    let cases: &[&[&str]] = &[
        &["head", "--rewind", "7d", "test/rfc/x"],
        &["find", "test/rfc", "--watch"],
        &["tree", "--rewind", "7d", "test/rfc"],
        &["cp", "--md5", "a", "b"],
    ];
    for args in cases {
        let out = server.rs3(args);
        assert!(!out.status.success(), "expected failure for {args:?}");
        // clap's own usage-error path: no "not implemented" text, because
        // no rs3 handler code ever ran -- these flags don't exist on the
        // relevant `*Args` struct at all.
        assert!(
            !String::from_utf8_lossy(&out.stderr).contains("not implemented"),
            "expected a clap usage error (not an app-level refusal) for {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
