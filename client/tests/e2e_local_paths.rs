//! Local-path handling in `cp`/`mv`: same-file protection, symlink
//! following, relative paths that look like `ALIAS/BUCKET`, and quiet
//! EPIPE on stdout.
mod common;
use common::TestServer;
use std::path::Path;
use std::process::{Command, Output, Stdio};

fn rs3_in(server: &TestServer, cwd: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rs3"))
        .args(args)
        .current_dir(cwd)
        .env("MC_HOST_TEST", server.mc_host())
        .env("MC_CONFIG_DIR", server.dir.path().join("mc-config"))
        .output()
        .expect("run rs3")
}

fn assert_ok(out: &Output, what: &str) {
    assert!(
        out.status.success(),
        "{what} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn cp_and_mv_onto_same_file_refuse_and_keep_data() {
    let server = TestServer::start();
    let cwd = server.dir.path().join("same");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::write(cwd.join("file.txt"), b"precious").unwrap();

    for verb in ["cp", "mv"] {
        for target in ["./", ".", "file.txt", "./file.txt"] {
            let out = rs3_in(&server, &cwd, &[verb, "file.txt", target]);
            assert!(!out.status.success(), "{verb} file.txt {target} must fail");
            assert_eq!(
                std::fs::read(cwd.join("file.txt")).unwrap(),
                b"precious",
                "{verb} file.txt {target} damaged the file"
            );
        }
    }
    let leftovers: Vec<_> = std::fs::read_dir(&cwd).unwrap().collect();
    assert_eq!(leftovers.len(), 1, "no temp files left behind");
}

#[test]
fn cp_and_mv_recursive_onto_same_dir_refuse_and_keep_data() {
    let server = TestServer::start();
    let cwd = server.dir.path().join("samedir");
    std::fs::create_dir_all(cwd.join("d/sub")).unwrap();
    std::fs::write(cwd.join("d/a.txt"), b"aaa").unwrap();
    std::fs::write(cwd.join("d/sub/b.txt"), b"bbb").unwrap();

    for (verb, target) in [
        ("cp", "d"),
        ("cp", "./d/"),
        ("mv", "d"),
        ("mv", "."),
        ("mv", "./"),
    ] {
        let out = rs3_in(&server, &cwd, &[verb, "-r", "d", target]);
        assert!(!out.status.success(), "{verb} -r d {target} must fail");
        assert_eq!(std::fs::read(cwd.join("d/a.txt")).unwrap(), b"aaa");
        assert_eq!(std::fs::read(cwd.join("d/sub/b.txt")).unwrap(), b"bbb");
    }
}

#[test]
fn cp_overwrites_existing_destination_atomically() {
    let server = TestServer::start();
    let cwd = server.dir.path().join("atomic");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::write(cwd.join("src.txt"), b"new content").unwrap();
    std::fs::write(cwd.join("dst.txt"), b"old").unwrap();
    assert_ok(&rs3_in(&server, &cwd, &["cp", "src.txt", "dst.txt"]), "cp");
    assert_eq!(std::fs::read(cwd.join("dst.txt")).unwrap(), b"new content");
    let names: Vec<_> = std::fs::read_dir(&cwd)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names.len(), 2, "no temp files left: {names:?}");
}

#[cfg(unix)]
#[test]
fn cp_recursive_follows_symlinks_and_warns_on_broken_ones() {
    use std::os::unix::fs::symlink;
    let server = TestServer::start();
    let root = server.dir.path().join("links");
    let outside = root.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("o.txt"), b"outside").unwrap();
    let src = root.join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("real.txt"), b"real").unwrap();
    symlink(src.join("real.txt"), src.join("filelink.txt")).unwrap();
    symlink(&outside, src.join("dirlink")).unwrap();
    symlink(&src, src.join("loop")).unwrap();
    symlink(root.join("nowhere"), src.join("broken")).unwrap();

    let dst = root.join("dst");
    let out = rs3_in(
        &server,
        &root,
        &["cp", "-r", src.to_str().unwrap(), dst.to_str().unwrap()],
    );
    assert_ok(&out, "cp -r");
    assert_eq!(std::fs::read(dst.join("real.txt")).unwrap(), b"real");
    assert_eq!(std::fs::read(dst.join("filelink.txt")).unwrap(), b"real");
    assert_eq!(
        std::fs::read(dst.join("dirlink/o.txt")).unwrap(),
        b"outside"
    );
    assert!(
        !dst.join("loop").exists(),
        "a link back to the root is not re-entered"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("broken"),
        "warns on broken symlink: {stderr}"
    );

    // mv removes the directory link, never the files it points at.
    let dst2 = root.join("dst2");
    std::fs::remove_file(src.join("broken")).unwrap();
    std::fs::remove_file(src.join("loop")).unwrap();
    // (a file link to a sibling would dangle once that sibling is moved)
    std::fs::remove_file(src.join("filelink.txt")).unwrap();
    let out = rs3_in(
        &server,
        &root,
        &["mv", "-r", src.to_str().unwrap(), dst2.to_str().unwrap()],
    );
    assert_ok(&out, "mv -r");
    assert_eq!(
        std::fs::read(dst2.join("dirlink/o.txt")).unwrap(),
        b"outside"
    );
    assert!(
        outside.join("o.txt").exists(),
        "files behind a dir symlink survive mv"
    );
    assert!(!src.join("dirlink").exists());
}

#[test]
fn relative_paths_with_slashes_are_local_unless_alias() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/rel"]);
    let cwd = server.dir.path().join("rel");
    std::fs::create_dir_all(cwd.join("data/tree/sub")).unwrap();
    std::fs::write(cwd.join("data/file.txt"), b"hello").unwrap();
    std::fs::write(cwd.join("data/tree/sub/t.txt"), b"tree").unwrap();

    // local -> local, both spelled like ALIAS/BUCKET
    assert_ok(
        &rs3_in(&server, &cwd, &["cp", "data/file.txt", "out/copy.txt"]),
        "local cp",
    );
    assert_eq!(std::fs::read(cwd.join("out/copy.txt")).unwrap(), b"hello");

    // local -> S3 and back to a relative local path
    assert_ok(
        &rs3_in(&server, &cwd, &["cp", "data/file.txt", "test/rel/k.txt"]),
        "upload",
    );
    assert_ok(
        &rs3_in(&server, &cwd, &["cp", "test/rel/k.txt", "out/dl.txt"]),
        "download",
    );
    assert_eq!(std::fs::read(cwd.join("out/dl.txt")).unwrap(), b"hello");

    // recursive, relative on both sides of an S3 hop
    assert_ok(
        &rs3_in(&server, &cwd, &["cp", "-r", "data/tree", "test/rel/tree"]),
        "recursive upload",
    );
    assert_ok(
        &rs3_in(&server, &cwd, &["cp", "-r", "test/rel/tree/", "out/tree"]),
        "recursive download",
    );
    assert_eq!(
        std::fs::read(cwd.join("out/tree/sub/t.txt")).unwrap(),
        b"tree"
    );

    // an existing local path wins even when its first segment is an alias
    std::fs::create_dir_all(cwd.join("test/x")).unwrap();
    std::fs::write(cwd.join("test/x/local.txt"), b"local").unwrap();
    assert_ok(
        &rs3_in(&server, &cwd, &["cp", "test/x/local.txt", "out/local.txt"]),
        "existing local path",
    );
    assert_eq!(std::fs::read(cwd.join("out/local.txt")).unwrap(), b"local");
}

/// Spawns rs3 with its stdout reader already closed, so the first write
/// hits EPIPE.
fn run_with_closed_stdout(server: &TestServer, args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rs3"))
        .args(args)
        .env("MC_HOST_TEST", server.mc_host())
        .env("MC_CONFIG_DIR", server.dir.path().join("mc-config"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rs3");
    drop(child.stdout.take());
    child.wait_with_output().expect("wait rs3")
}

#[test]
fn closed_stdout_is_not_a_panic() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/pipe"]);
    let src = server.dir.path().join("p.txt");
    std::fs::write(&src, b"line\n".repeat(1000)).unwrap();
    for i in 0..3 {
        server.rs3_ok(&["put", src.to_str().unwrap(), &format!("test/pipe/o{i}.txt")]);
    }
    for args in [
        &["ls", "-r", "test/pipe"][..],
        &["--json", "ls", "-r", "test/pipe"][..],
        &["cat", "test/pipe/o0.txt"][..],
        &["head", "-n", "500", "test/pipe/o0.txt"][..],
    ] {
        let out = run_with_closed_stdout(&server, args);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.contains("panicked") && !stderr.contains("Broken pipe"),
            "{args:?}: {stderr}"
        );
        assert_eq!(out.status.code(), Some(0), "{args:?}: {stderr}");
    }
}
