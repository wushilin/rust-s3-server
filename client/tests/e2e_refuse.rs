mod common;
use common::TestServer;

#[test]
fn unimplemented_flags_hard_error() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/rfb"]);
    let cases: &[&[&str]] = &[
        &["ls", "--rewind", "7d", "test/rfb"],
        &["ls", "--versions", "test/rfb"],
        &["ls", "--zip", "test/rfb"],
        &["cat", "--offset", "5", "test/rfb/x"],
        &["cat", "--tail", "5", "test/rfb/x"],
        &["stat", "--recursive", "test/rfb"],
        &["mb", "--with-lock", "test/rfb2"],
        &["mb", "--with-versioning", "test/rfb3"],
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
