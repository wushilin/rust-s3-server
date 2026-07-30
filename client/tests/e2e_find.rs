mod common;
use common::TestServer;

/// Writes a tiny executable shell script that records `argc` (`$#`) to
/// `out_file`, for verifying `find --exec`'s argv splitting.
#[cfg(unix)]
fn make_argc_script(server: &TestServer, out_file: &std::path::Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = server.dir.path().join("argc.sh");
    let script = format!("#!/bin/sh\necho \"argc=$#\" > '{}'\n", out_file.display());
    std::fs::write(&path, script).unwrap();
    let mut perms = std::fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&path, perms).unwrap();
    path
}

fn seed(server: &TestServer, bucket: &str) {
    server.rs3_ok(&["mb", &format!("test/{bucket}")]);
    let small = server.dir.path().join("small.log");
    std::fs::write(&small, vec![b'x'; 100]).unwrap();
    let big = server.dir.path().join("big.log");
    std::fs::write(&big, vec![b'y'; 2000]).unwrap();
    let note = server.dir.path().join("note.txt");
    std::fs::write(&note, b"hello").unwrap();
    server.rs3_ok(&[
        "put",
        small.to_str().unwrap(),
        &format!("test/{bucket}/a/foo/small.log"),
    ]);
    server.rs3_ok(&[
        "put",
        big.to_str().unwrap(),
        &format!("test/{bucket}/a/bar/big.log"),
    ]);
    server.rs3_ok(&[
        "put",
        note.to_str().unwrap(),
        &format!("test/{bucket}/a/foo/note.txt"),
    ]);
}

#[test]
fn find_name_filter_matches_only_logs() {
    let server = TestServer::start();
    seed(&server, "fnd1");
    let out = server.rs3_ok(&["find", "test/fnd1", "--name", "*.log"]);
    let mut lines: Vec<&str> = out.lines().collect();
    lines.sort();
    assert_eq!(
        lines,
        vec!["test/fnd1/a/bar/big.log", "test/fnd1/a/foo/small.log",],
        "out: {out}"
    );
    assert!(!out.contains("note.txt"), "out: {out}");
}

#[test]
fn find_larger_excludes_small_object() {
    let server = TestServer::start();
    seed(&server, "fnd2");
    let out = server.rs3_ok(&["find", "test/fnd2", "--larger", "1ki"]);
    assert!(out.contains("big.log"), "out: {out}");
    assert!(!out.contains("small.log"), "out: {out}");
    assert!(!out.contains("note.txt"), "out: {out}");
}

#[test]
fn find_exec_false_aborts_with_nonzero_exit() {
    let server = TestServer::start();
    seed(&server, "fnd3");
    let out = server.rs3(&["find", "test/fnd3", "--exec", "false"]);
    assert!(!out.status.success(), "expected nonzero exit");
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn find_print_renders_tokens() {
    let server = TestServer::start();
    seed(&server, "fnd4");
    let out = server.rs3_ok(&[
        "find",
        "test/fnd4",
        "--name",
        "note.txt",
        "--print",
        "{base} {size}",
    ]);
    assert_eq!(out.trim(), "note.txt 5B", "out: {out}");
}

#[test]
fn find_ignore_excludes_matching_objects() {
    let server = TestServer::start();
    seed(&server, "fnd5");
    let out = server.rs3_ok(&["find", "test/fnd5", "--ignore", "a/bar/*"]);
    assert!(!out.contains("big.log"), "out: {out}");
    assert!(out.contains("small.log"), "out: {out}");
    assert!(out.contains("note.txt"), "out: {out}");
}

#[test]
fn find_maxdepth_truncates_printed_path_and_matching() {
    let server = TestServer::start();
    seed(&server, "fnd6");
    // Ground-truth-verified (real `mc`): a maxdepth shallow enough to
    // truncate the basename off a matched key means `--name` no longer
    // sees the real filename, so it stops matching entirely, not just
    // display.
    let out = server.rs3_ok(&["find", "test/fnd6", "--maxdepth", "2"]);
    let mut lines: Vec<&str> = out.lines().collect();
    lines.sort();
    lines.dedup();
    assert_eq!(lines, vec!["test/fnd6/a/"], "out: {out}");

    let out = server.rs3_ok(&["find", "test/fnd6", "--maxdepth", "2", "--name", "note.txt"]);
    assert!(out.trim().is_empty(), "out: {out}");
}

#[test]
fn find_json_matches_content_message_shape() {
    let server = TestServer::start();
    seed(&server, "fnd7");
    let out = server.rs3_ok(&["--json", "find", "test/fnd7", "--name", "note.txt"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(v["status"], "success");
    assert_eq!(v["size"], 5);
    assert_eq!(v["key"], "test/fnd7/a/foo/note.txt");
    assert!(v.get("lastModified").is_some());
}

/// Ground-truth-verified regression: `--exec` must split its *raw*
/// template with `shell-words` *before* substituting `{}`, not after.
/// Substitute-then-split would hand a spacey key to the child as two argv
/// words instead of one.
#[test]
#[cfg(unix)]
fn find_exec_key_with_space_is_single_argv_word() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/fnd8"]);
    let src = server.dir.path().join("sp.txt");
    std::fs::write(&src, b"hi").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/fnd8/sp file.txt"]);

    let out_file = server.dir.path().join("argc-space.out");
    let script = make_argc_script(&server, &out_file);
    let exec_template = format!("{} {{}}", script.display());
    server.rs3_ok(&[
        "find",
        "test/fnd8",
        "--name",
        "sp file.txt",
        "--exec",
        &exec_template,
    ]);
    let recorded = std::fs::read_to_string(&out_file).expect("argc script ran and wrote output");
    assert_eq!(recorded.trim(), "argc=1", "recorded: {recorded}");
}

/// Ground-truth-verified regression: a key with an unbalanced double quote
/// must not abort the whole find run. Substitute-then-split would hand
/// `shell-words::split` a string with a stray quote (since the quote
/// comes from the *substituted key*, not the template) and error out;
/// split-then-substitute never re-parses the already-isolated `{}` word,
/// so the literal quote character passes through untouched.
#[test]
#[cfg(unix)]
fn find_exec_key_with_unbalanced_quote_does_not_abort() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/fnd9"]);
    let src = server.dir.path().join("uq.txt");
    std::fs::write(&src, b"hi").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/fnd9/unbal\"file.txt"]);

    let out_file = server.dir.path().join("argc-quote.out");
    let script = make_argc_script(&server, &out_file);
    let exec_template = format!("{} {{}}", script.display());
    let out = server.rs3(&[
        "find",
        "test/fnd9",
        "--name",
        "unbal\"file.txt",
        "--exec",
        &exec_template,
    ]);
    assert!(
        out.status.success(),
        "expected success, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let recorded = std::fs::read_to_string(&out_file).expect("argc script ran and wrote output");
    assert_eq!(recorded.trim(), "argc=1", "recorded: {recorded}");
}
