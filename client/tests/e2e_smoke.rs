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

#[test]
fn part_size_flag_lowers_multipart_threshold() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mpb"]);
    // 12 MiB of patterned data with 5 MiB parts -> must take the multipart path.
    let src = server.dir.path().join("big.bin");
    let data: Vec<u8> = (0..12 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&[
        "put",
        "--part-size",
        "5MiB",
        src.to_str().unwrap(),
        "test/mpb/big.bin",
    ]);
    let stat = server.rs3_ok(&["stat", "test/mpb/big.bin"]);
    // Multipart ETags contain "-<parts>"; 12MiB / 5MiB = 3 parts.
    assert!(stat.contains("-3"), "expected multipart etag, stat: {stat}");
    let dst = server.dir.path().join("big.out");
    server.rs3_ok(&["get", "test/mpb/big.bin", dst.to_str().unwrap()]);
    assert_eq!(std::fs::read(&dst).unwrap(), data);
}

#[test]
fn mb_ignore_existing_only_ignores_existing() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dup"]);
    server.rs3_ok(&["mb", "--ignore-existing", "test/dup"]);
    // A genuinely invalid bucket name must still fail even with -p.
    let out = server.rs3(&["mb", "--ignore-existing", "test/Invalid_Bucket_NAME"]);
    assert!(!out.status.success());
}
