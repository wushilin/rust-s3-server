mod common;
use common::TestServer;

#[test]
fn large_download_via_cp_is_correct() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dlb"]);
    let src = server.dir.path().join("big.bin");
    // 17 MiB so 5 MiB parts give 4 ranges with a short tail.
    let data: Vec<u8> = (0..17 * 1024 * 1024u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
        .collect();
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&[
        "put",
        "--part-size",
        "5MiB",
        src.to_str().unwrap(),
        "test/dlb/big.bin",
    ]);

    let dst = server.dir.path().join("big.out");
    server.rs3_ok(&[
        "cp",
        "--part-size",
        "5MiB",
        "--parallel",
        "4",
        "test/dlb/big.bin",
        dst.to_str().unwrap(),
    ]);
    assert_eq!(
        std::fs::read(&dst).unwrap(),
        data,
        "parallel download corrupted data"
    );
    // no leftover temp file
    assert!(!server.dir.path().join("big.out.rs3.part").exists());
}

#[test]
fn cp_multipart_completes_with_p1() {
    // -P 1 with leaf-only permits must serialize parts, never deadlock: a
    // single stream-budget permit is handed out at a time, so each part
    // future must be able to acquire, run to completion, and release before
    // the next one starts. If a permit were ever held across an await that
    // depends on another part (or acquired twice by the same task), this
    // test hangs indefinitely instead of failing -- there's no process-level
    // timeout in this harness, only `TestServer::start`'s own 30s
    // server-readiness deadline (which this test is already past by the
    // time it would deadlock). A hang here blocks the whole test run.
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/p1b"]);
    let src = server.dir.path().join("p1.bin");
    // 12 MiB so 5 MiB parts give exactly 3 parts (5 + 5 + 2).
    let data: Vec<u8> = (0..12 * 1024 * 1024u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
        .collect();
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&[
        "put",
        "--part-size",
        "5MiB",
        "--parallel",
        "1",
        src.to_str().unwrap(),
        "test/p1b/p1.bin",
    ]);

    let dst = server.dir.path().join("p1.out");
    server.rs3_ok(&[
        "cp",
        "--part-size",
        "5MiB",
        "--parallel",
        "1",
        "test/p1b/p1.bin",
        dst.to_str().unwrap(),
    ]);
    assert_eq!(
        std::fs::read(&dst).unwrap(),
        data,
        "P=1 roundtrip corrupted data"
    );
    // no leftover temp file
    assert!(!server.dir.path().join("p1.out.rs3.part").exists());
}

#[test]
fn failed_download_leaves_no_partial_output() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dl2b"]);
    let dst = server.dir.path().join("ghost.out");
    let out = server.rs3(&["get", "test/dl2b/ghost.bin", dst.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(!dst.exists());
    assert!(!server.dir.path().join("ghost.out.rs3.part").exists());
}
