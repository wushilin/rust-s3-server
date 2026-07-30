mod common;
use common::TestServer;

#[test]
fn put_ls_get_roundtrip() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/smoke"]);

    let src = server.dir.path().join("hello.txt");
    std::fs::write(&src, b"hello rs3 e2e").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/smoke/hello.txt"]);

    let listing = server.rs3_ok(&["ls", "test/smoke"]);
    assert!(listing.contains("hello.txt"), "listing was: {listing}");

    let dst = server.dir.path().join("hello.out");
    server.rs3_ok(&["get", "test/smoke/hello.txt", dst.to_str().unwrap()]);
    assert_eq!(std::fs::read(&dst).unwrap(), b"hello rs3 e2e");
}
