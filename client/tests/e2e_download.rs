mod common;
use common::TestServer;

/// A download stages under `<parent>/__rs3_staging_<unique>/` and removes
/// the directory on both the success and failure paths. Any survivor is a
/// leak -- and, since `set_len` makes the partial object full-size and
/// sparse, a survivor next to real data is exactly what a mirror must never
/// mistake for content.
fn no_staging_left(dir: &std::path::Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|rd| {
        !rd.filter_map(Result::ok)
            .any(|e| e.file_name().to_string_lossy().starts_with("__rs3_staging_"))
    })
}

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
    assert!(no_staging_left(server.dir.path()), "staging dir survived");
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
    assert!(no_staging_left(server.dir.path()), "staging dir survived");
}

/// An object smaller than one part still goes through the ranged path (one
/// range, no part label), rather than the plain unranged GET it used to get.
#[test]
fn single_range_download_is_byte_exact() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dl1r"]);
    let src = server.dir.path().join("small.bin");
    // Well under the 5 MiB part size below: exactly one range.
    let data: Vec<u8> = (0..300_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
        .collect();
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/dl1r/small.bin"]);

    let dst = server.dir.path().join("small.out");
    server.rs3_ok(&[
        "cp",
        "--part-size",
        "5MiB",
        "test/dl1r/small.bin",
        dst.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(&dst).unwrap(), data);
    assert!(no_staging_left(server.dir.path()), "staging dir survived");
}

/// A zero-byte object has no range to ask for -- `bytes=0--1` is not
/// expressible -- so it short-circuits to creating the empty file. Left
/// unguarded this underflows the range arithmetic.
#[test]
fn empty_object_downloads_as_an_empty_file() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dl0b"]);
    let src = server.dir.path().join("empty.bin");
    std::fs::write(&src, b"").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/dl0b/empty.bin"]);

    let dst = server.dir.path().join("empty.out");
    server.rs3_ok(&["get", "test/dl0b/empty.bin", dst.to_str().unwrap()]);
    assert!(dst.exists(), "empty object must still produce a file");
    assert_eq!(std::fs::read(&dst).unwrap(), Vec::<u8>::new());
    assert!(no_staging_left(server.dir.path()), "staging dir survived");
}

#[test]
fn failed_download_leaves_no_partial_output() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dl2b"]);
    let dst = server.dir.path().join("ghost.out");
    let out = server.rs3(&["get", "test/dl2b/ghost.bin", dst.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(!dst.exists());
    assert!(no_staging_left(server.dir.path()), "staging dir survived");
}
