mod common;
use common::TestServer;
use std::fs;

fn write(dir: &std::path::Path, rel: &str, contents: &[u8]) {
    let path = dir.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

#[test]
fn mirror_is_incremental() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mb1"]);
    let src = server.dir.path().join("srcdir");
    write(&src, "a.txt", b"aaa");
    write(&src, "sub/b.txt", b"bbb");
    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/mb1/pfx"]);
    let listing = server.rs3_ok(&["ls", "--recursive", "test/mb1"]);
    assert!(listing.contains("pfx/a.txt") && listing.contains("pfx/sub/b.txt"));

    // second run with nothing changed must transfer nothing. A no-op
    // session still prints its final `accountStat` summary line (mc's
    // "--quiet is not silence" behavior applies to the default non-tty
    // mode this test harness runs under too), but must emit zero
    // `MirrorMessage` copy lines ("`src` -> `dst`", tier-2's replacement
    // for the old "Mirrored `...`." prose).
    let out = server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/mb1/pfx"]);
    assert!(!out.contains(" -> "), "second run re-copied: {out}");

    // touch one file with new content -> exactly one transfer
    std::thread::sleep(std::time::Duration::from_millis(1100)); // S3 mtime granularity
    write(&src, "a.txt", b"aaa2");
    let out = server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/mb1/pfx"]);
    assert_eq!(out.matches(" -> ").count(), 1, "out: {out}");
}

#[test]
fn mirror_remove_deletes_extraneous() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mb2"]);
    let src = server.dir.path().join("srcdir2");
    write(&src, "keep.txt", b"k");
    write(&src, "drop.txt", b"d");
    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/mb2/p"]);
    fs::remove_file(src.join("drop.txt")).unwrap();
    server.rs3_ok(&["mirror", "--remove", src.to_str().unwrap(), "test/mb2/p"]);
    let listing = server.rs3_ok(&["ls", "--recursive", "test/mb2"]);
    assert!(listing.contains("keep.txt"));
    assert!(!listing.contains("drop.txt"), "listing: {listing}");
}

/// A download's staging directory is not content. Left unfiltered a
/// local-to-S3 mirror uploads what is inside it -- and since `set_len` makes
/// the partial object full-size and sparse, an interrupted large download
/// would upload its whole nominal size as mostly zeros.
///
/// The file inside carries the object's *real* name, which is the point of
/// staging by directory: there is no suffix to key off, and a file that
/// merely looks like scratch (`notes.rs3.part` here) is real data that must
/// still be mirrored.
#[test]
fn mirror_never_uploads_download_staging() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mbscr"]);
    let src = server.dir.path().join("srcstaging");
    write(&src, "real.txt", b"real");
    write(&src, "notes.rs3.part", b"a real file that looks like scratch");
    write(&src, "__rs3_staging_1234_abc_0/big.bin", b"half a download");
    write(&src, "sub/__rs3_staging_9_def_1/other.bin", b"another");

    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/mbscr/p"]);
    let listing = server.rs3_ok(&["ls", "--recursive", "test/mbscr"]);
    assert!(listing.contains("real.txt"), "listing: {listing}");
    assert!(
        listing.contains("notes.rs3.part"),
        "a real file must not be skipped for looking like scratch: {listing}"
    );
    assert!(
        !listing.contains("staging"),
        "staging was uploaded: {listing}"
    );
    assert!(
        !listing.contains("big.bin") && !listing.contains("other.bin"),
        "staged contents were uploaded: {listing}"
    );
}

/// This host's name, formatted the way `staging_dir_for` stamps it.
fn host() -> String {
    let raw = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .or_else(|_| std::fs::read_to_string("/etc/hostname"))
        .unwrap_or_default();
    let cleaned: String = raw
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect();
    if cleaned.is_empty() {
        "unknown".into()
    } else {
        cleaned
    }
}

/// Abandoned staging is collected by the next writing walk, but only when it
/// is provably abandoned: our host, and an owning pid that is gone. Staging
/// owned by a live process -- a concurrent rs3 mid-download -- must survive,
/// because unlinking it breaks that download at its final rename.
#[test]
fn mirror_reclaims_dead_staging_but_spares_live() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mbscr2"]);
    let src = server.dir.path().join("srcstaging2");
    write(&src, "a.txt", b"a");
    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/mbscr2/p"]);

    let dest = server.dir.path().join("deststaging2");
    // pid 1 exists on any running system; this process is alive by definition.
    let live = format!("__rs3_staging_{}_{}_beef_0", host(), std::process::id());
    // A pid that cannot be running: above the kernel's own ceiling.
    let dead = format!("__rs3_staging_{}_4294967290_beef_1", host());
    // Another machine's staging: its pid means nothing here, so hands off.
    let remote = "__rs3_staging_someotherhost_4294967290_beef_2".to_string();
    write(&dest, &format!("{live}/live.bin"), b"in flight");
    write(&dest, &format!("{dead}/orphan.bin"), b"abandoned");
    write(&dest, &format!("{remote}/theirs.bin"), b"not ours");

    server.rs3_ok(&["mirror", "test/mbscr2/p", dest.to_str().unwrap()]);

    assert!(
        dest.join(&live).exists(),
        "a live process's staging must survive"
    );
    assert!(
        !dest.join(&dead).exists(),
        "abandoned staging should have been reclaimed"
    );
    assert!(
        dest.join(&remote).exists(),
        "another host's staging must survive"
    );
    assert!(dest.join("a.txt").exists(), "the mirror itself still ran");
}

#[test]
fn mirror_dry_run_prints_plan_and_does_nothing() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mb3"]);
    let src = server.dir.path().join("srcdir3");
    write(&src, "x.txt", b"x");
    let out = server.rs3_ok(&["mirror", "--dry-run", src.to_str().unwrap(), "test/mb3/p"]);
    assert!(out.contains("PUT"), "out: {out}");
    assert!(out.contains("Planned 1 put(s)"), "out: {out}");
    let listing = server.rs3_ok(&["ls", "--recursive", "test/mb3"]);
    assert!(!listing.contains("x.txt"));
}

#[test]
fn mirror_json_dry_run_is_silent() {
    // `--json --dry-run` must not leak the plain-text PUT/DEL/"Planned N
    // put(s)" prose into what's otherwise a JSON-lines stream: mc has no
    // documented json contract for a dry-run plan, so rs3's conservative
    // choice is to emit nothing on stdout at all (see README known
    // divergences).
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mb3j"]);
    let src = server.dir.path().join("srcdir3j");
    write(&src, "x.txt", b"x");
    let out = server.rs3(&[
        "--json",
        "mirror",
        "--dry-run",
        src.to_str().unwrap(),
        "test/mb3j/p",
    ]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stdout.is_empty(),
        "expected zero stdout bytes, got: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let listing = server.rs3_ok(&["ls", "--recursive", "test/mb3j"]);
    assert!(!listing.contains("x.txt"));
}

#[test]
fn mirror_s3_to_s3_and_back_to_local() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/m4a"]);
    server.rs3_ok(&["mb", "test/m4b"]);
    let src = server.dir.path().join("srcdir4");
    write(&src, "one.bin", b"11111");
    write(&src, "d/two.bin", b"22222");
    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/m4a/p"]);
    server.rs3_ok(&["mirror", "test/m4a/p", "test/m4b/q"]);
    let dst = server.dir.path().join("outdir4");
    server.rs3_ok(&["mirror", "test/m4b/q", dst.to_str().unwrap()]);
    assert_eq!(fs::read(dst.join("one.bin")).unwrap(), b"11111");
    assert_eq!(fs::read(dst.join("d/two.bin")).unwrap(), b"22222");
}

#[test]
fn mirror_local_to_local_is_rejected_and_does_not_delete() {
    let server = TestServer::start();
    let src = server.dir.path().join("la");
    let dst = server.dir.path().join("lb");
    write(&src, "a.txt", b"a");
    write(&dst, "stale.txt", b"stale");
    let out = server.rs3(&[
        "mirror",
        "--remove",
        src.to_str().unwrap(),
        dst.to_str().unwrap(),
    ]);
    assert!(
        !out.status.success(),
        "mirror between two local paths should fail, stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        dst.join("stale.txt").exists(),
        "stale file was deleted despite local->local mirror being unsupported"
    );
}

#[test]
fn mirror_does_not_leak_sibling_prefix_objects() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mb5"]);
    let src = server.dir.path().join("srcdir5");
    write(&src, "a.txt", b"a");
    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/mb5/p"]);
    let extra = server.dir.path().join("extra5");
    write(&extra, "x.txt", b"x");
    server.rs3_ok(&["mirror", extra.to_str().unwrap(), "test/mb5/p2"]);

    let dst = server.dir.path().join("outdir5");
    let out = server.rs3(&["mirror", "test/mb5/p", dst.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "mirror failed: stdout: {} stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(dst.join("a.txt").exists(), "a.txt was not mirrored");
    assert!(
        !dst.join("2/x.txt").exists(),
        "sibling prefix `p2/x.txt` leaked into the mirror as `2/x.txt`"
    );
    assert!(
        !dst.join("x.txt").exists(),
        "sibling prefix `p2/x.txt` leaked into the mirror as `x.txt`"
    );
}
