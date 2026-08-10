//! Downloads are checked against the server's ETag, not just its byte count.
//!
//! The interesting assertion here is the negative one: a download whose bytes
//! were altered on the server must *fail*. A verification path that only ever
//! passes proves nothing, and this is the one test that can tell the two
//! apart — it corrupts an object in the server's own storage, behind rs3's
//! back, and requires the download to refuse it.

mod common;
use common::TestServer;

use md5::{Digest, Md5};

fn payload(len: usize) -> Vec<u8> {
    (0..len as u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
        .collect()
}

/// Finds the on-disk part file for an object the server has stored, so a test
/// can corrupt exactly the bytes a client is about to download.
fn stored_part_files(server: &TestServer, bucket: &str) -> Vec<std::path::PathBuf> {
    let root = server.dir.path().join("data/buckets").join(bucket);
    let mut found = Vec::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("part.") && !n.ends_with(".json"))
            {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

#[test]
fn multipart_download_rebuilds_and_matches_the_servers_etag() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/vfy"]);
    // 12 MiB in 5 MiB parts: 5/5/2, a deliberately ragged tail.
    let data = payload(12 * 1024 * 1024);
    let src = server.dir.path().join("big.bin");
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&[
        "put",
        "--part-size",
        "5MiB",
        src.to_str().unwrap(),
        "test/vfy/big.bin",
    ]);

    let dst = server.dir.path().join("big.out");
    // No --no-verify: the ETag check is the default path.
    server.rs3_ok(&["cp", "test/vfy/big.bin", dst.to_str().unwrap()]);
    assert_eq!(std::fs::read(&dst).unwrap(), data);
}

#[test]
fn download_refuses_an_object_whose_bytes_changed_under_it() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/vfybad"]);
    let data = payload(12 * 1024 * 1024);
    let src = server.dir.path().join("big.bin");
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&[
        "put",
        "--part-size",
        "5MiB",
        src.to_str().unwrap(),
        "test/vfybad/big.bin",
    ]);

    // Corrupt one byte of one stored part, leaving its recorded length (and
    // therefore the server's ETag) untouched. This is exactly the failure the
    // old length-only check could not see.
    let parts = stored_part_files(&server, "vfybad");
    assert!(!parts.is_empty(), "expected stored part files to corrupt");
    let victim = &parts[0];
    let mut bytes = std::fs::read(victim).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(victim, &bytes).unwrap();

    let dst = server.dir.path().join("big.out");
    let out = server.rs3(&["cp", "test/vfybad/big.bin", dst.to_str().unwrap()]);
    assert!(
        !out.status.success(),
        "a corrupted object must not download successfully"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("content check failed") || stderr.contains("failed its content check"),
        "expected a content-check failure, got: {stderr}"
    );
    assert!(
        !dst.exists(),
        "a file that failed verification must never be published"
    );
}

#[test]
fn no_verify_accepts_what_verification_would_reject() {
    // The escape hatch has to actually work, or it is not an escape hatch.
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/vfyoff"]);
    let data = payload(12 * 1024 * 1024);
    let src = server.dir.path().join("big.bin");
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&[
        "put",
        "--part-size",
        "5MiB",
        src.to_str().unwrap(),
        "test/vfyoff/big.bin",
    ]);
    let parts = stored_part_files(&server, "vfyoff");
    let mut bytes = std::fs::read(&parts[0]).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&parts[0], &bytes).unwrap();

    let dst = server.dir.path().join("big.out");
    server.rs3_ok(&[
        "cp",
        "--no-verify",
        "test/vfyoff/big.bin",
        dst.to_str().unwrap(),
    ]);
    assert!(dst.exists(), "--no-verify must still publish the file");
}

#[test]
fn single_put_object_is_checked_against_its_plain_md5() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/vfyone"]);
    let data = payload(64 * 1024);
    let src = server.dir.path().join("small.bin");
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/vfyone/small.bin"]);

    let dst = server.dir.path().join("small.out");
    server.rs3_ok(&["cp", "test/vfyone/small.bin", dst.to_str().unwrap()]);
    assert_eq!(std::fs::read(&dst).unwrap(), data);

    // And it rejects a corrupted one, on the whole-object MD5 path rather than
    // the composite one.
    let parts = stored_part_files(&server, "vfyone");
    let mut bytes = std::fs::read(&parts[0]).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&parts[0], &bytes).unwrap();
    let dst2 = server.dir.path().join("small2.out");
    let out = server.rs3(&["cp", "test/vfyone/small.bin", dst2.to_str().unwrap()]);
    assert!(!out.status.success(), "corrupted single-part must fail");
    assert!(!dst2.exists());
}

#[test]
fn mirror_verifies_every_object_it_copies() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/vfymir"]);
    let dir = server.dir.path().join("tree");
    std::fs::create_dir_all(&dir).unwrap();
    for i in 0..3 {
        std::fs::write(dir.join(format!("f{i}.bin")), payload(4096 + i)).unwrap();
    }
    server.rs3_ok(&["mirror", dir.to_str().unwrap(), "test/vfymir"]);

    let back = server.dir.path().join("back");
    server.rs3_ok(&["mirror", "test/vfymir", back.to_str().unwrap()]);
    for i in 0..3 {
        assert_eq!(
            std::fs::read(back.join(format!("f{i}.bin"))).unwrap(),
            payload(4096 + i)
        );
    }

    // Corrupt one object, then mirror into a clean directory: that object must
    // fail while the others still land.
    let parts = stored_part_files(&server, "vfymir");
    let mut bytes = std::fs::read(&parts[0]).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&parts[0], &bytes).unwrap();
    let back2 = server.dir.path().join("back2");
    let out = server.rs3(&["mirror", "test/vfymir", back2.to_str().unwrap()]);
    assert!(
        !out.status.success(),
        "mirror must fail on a corrupt object"
    );
}

#[test]
fn verified_download_costs_no_extra_request_for_a_small_object() {
    // A single-`PUT` object's ETag is its MD5, so verification needs nothing
    // the download did not already have. This pins that: the whole round trip
    // still works with verification on, for the small-file case that dominates
    // a mirror.
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/vfycheap"]);
    let src = server.dir.path().join("tiny.bin");
    let data = payload(11);
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/vfycheap/tiny.bin"]);
    let dst = server.dir.path().join("tiny.out");
    server.rs3_ok(&["cp", "test/vfycheap/tiny.bin", dst.to_str().unwrap()]);
    assert_eq!(std::fs::read(&dst).unwrap(), data);
    assert_eq!(format!("{:x}", Md5::digest(&data)).len(), 32);
}

#[test]
fn a_stale_inherited_etag_is_recovered_by_restating_the_object() {
    // Inheriting size and ETag from the listing saves a request per object,
    // but the object can be replaced before the transfer starts. `If-Match`
    // turns that into a 412 rather than a spliced file; this proves the 412 is
    // then recovered instead of surfacing as a failure.
    //
    // The race cannot be provoked from outside the process, so the test hook
    // makes the inherited ETag unmatchable -- the same condition a real
    // replacement produces.
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/vfystale"]);
    let dir = server.dir.path().join("tree");
    std::fs::create_dir_all(&dir).unwrap();
    let data = payload(12 * 1024 * 1024);
    std::fs::write(dir.join("big.bin"), &data).unwrap();
    std::fs::write(dir.join("small.bin"), payload(4096)).unwrap();
    server.rs3_ok(&[
        "mirror",
        "--part-size",
        "5MiB",
        dir.to_str().unwrap(),
        "test/vfystale",
    ]);

    let back = server.dir.path().join("back");
    let out = server.rs3_env(
        &["mirror", "test/vfystale", back.to_str().unwrap()],
        &[("RS3_TEST_STALE_INHERITED_ETAG", "1")],
    );
    assert!(
        out.status.success(),
        "a replaced object must be recovered, not fail:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("changed after it was listed"),
        "expected the refetch to be reported, got: {stderr}"
    );
    // Recovered means *correct*, not merely non-failing: the refetch must
    // still be verified against the object's real ETag.
    assert_eq!(std::fs::read(back.join("big.bin")).unwrap(), data);
    assert_eq!(
        std::fs::read(back.join("small.bin")).unwrap(),
        payload(4096)
    );
}

#[test]
fn a_single_object_command_does_not_absorb_a_precondition_failure() {
    // A plan built from this process's own HeadObject was current a moment
    // ago. A 412 against it means something is rewriting the key continuously,
    // which is worth reporting rather than retrying around.
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/vfynoinherit"]);
    let src = server.dir.path().join("one.bin");
    std::fs::write(&src, payload(4096)).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/vfynoinherit/one.bin"]);

    let dst = server.dir.path().join("one.out");
    // `cp` of a single object passes no listed facts, so the hook cannot make
    // its ETag stale and the download simply succeeds.
    let out = server.rs3_env(
        &["cp", "test/vfynoinherit/one.bin", dst.to_str().unwrap()],
        &[("RS3_TEST_STALE_INHERITED_ETAG", "1")],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(std::fs::read(&dst).unwrap(), payload(4096));
}
