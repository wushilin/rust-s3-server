mod common;
use common::TestServer;

fn regex_lite(pattern: &str) -> impl Fn(&str) -> bool {
    let re = regex::Regex::new(pattern).unwrap();
    move |s: &str| re.is_match(s)
}

#[test]
fn ls_json_is_content_message() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lsjson"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"hello").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lsjson/dir/f.txt"]);
    let out = server.rs3_ok(&["--json", "ls", "test/lsjson/dir/"]);
    let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
    assert_eq!(v["status"], "success");
    assert_eq!(v["type"], "file");
    assert_eq!(v["size"], 5);
    assert_eq!(v["key"], "f.txt");
    assert!(v["lastModified"].as_str().unwrap().contains('T'));
    let etag = v["etag"].as_str().expect("etag present");
    assert!(
        !etag.starts_with('"'),
        "etag must be quote-stripped like real mc: {etag:?}"
    );

    // Same object via `stat --json`: StatMessage's etag must also be
    // quote-stripped (mc's cmd/stat.go:189-190 does the same TrimPrefix/
    // TrimSuffix `"` as cmd/ls.go:146-148).
    let stat_out = server.rs3_ok(&["--json", "stat", "test/lsjson/dir/f.txt"]);
    let sv: serde_json::Value = serde_json::from_str(stat_out.trim()).unwrap();
    let stat_etag = sv["etag"].as_str().expect("stat etag present");
    assert!(
        !stat_etag.starts_with('"'),
        "stat etag must be quote-stripped like real mc: {stat_etag:?}"
    );
}

#[test]
fn ls_recursive_no_trailing_slash_strips_directory_prefix() {
    // mc auto-detects a bare (non-slash-terminated) directory target and
    // displays entries relative to it, same as a slash-terminated one --
    // `ls -r bucket/dir` must show `f.txt`/`sub/g.txt`, not `dir/f.txt` (a
    // leading-`/`-prefixed leftover from a naive `strip_prefix("dir")`) or
    // the raw un-stripped full key.
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lsnoslash"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"hello").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lsnoslash/dir/f.txt"]);
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lsnoslash/dir/sub/g.txt"]);
    let out = server.rs3_ok(&["--json", "ls", "-r", "test/lsnoslash/dir"]);
    let keys: Vec<String> = out
        .lines()
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).unwrap();
            v["key"].as_str().unwrap().to_string()
        })
        .collect();
    assert!(
        keys.contains(&"f.txt".to_string()),
        "keys: {keys:?}, out: {out}"
    );
    assert!(
        keys.contains(&"sub/g.txt".to_string()),
        "keys: {keys:?}, out: {out}"
    );
    assert!(
        keys.iter().all(|k| !k.starts_with('/')),
        "no key should have a stray leading '/': {keys:?}"
    );
}

#[test]
fn ls_exact_object_key_shows_basename_relative_to_parent() {
    // `ls bucket/dir/f.txt` (a real object, not a directory-style prefix)
    // must show that one object with its key relative to its *parent*
    // directory, i.e. just "f.txt" -- not the full "dir/f.txt" ([SEM] §11:
    // mc `Stat()`s a bare target first; a hit is listed as exactly that one
    // object, keyed relative to its parent).
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lsexact"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"hello").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lsexact/dir/f.txt"]);
    let out = server.rs3_ok(&["--json", "ls", "test/lsexact/dir/f.txt"]);
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 1, "expected exactly one entry, out: {out}");
    let v: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(v["type"], "file");
    assert_eq!(v["key"], "f.txt", "out: {out}");
}

#[test]
fn ls_bare_directory_no_trailing_slash_non_recursive_matches_slash_form() {
    // A bare target that isn't a real object but has children ("dir" with
    // no trailing slash, no `-r`) must be treated exactly like the
    // slash-terminated form: list `dir`'s immediate contents relative to
    // it, not collapse to a single empty-keyed CommonPrefix entry ([SEM]
    // §11: mc re-lists with a trailing separator appended once `Stat`
    // confirms the bare target is a directory).
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lsbaredir"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"hello").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lsbaredir/dir/f.txt"]);
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lsbaredir/dir/sub/g.txt"]);
    let out = server.rs3_ok(&["--json", "ls", "test/lsbaredir/dir"]);
    let keys: Vec<String> = out
        .lines()
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).unwrap();
            v["key"].as_str().unwrap().to_string()
        })
        .collect();
    assert!(
        keys.contains(&"f.txt".to_string()),
        "keys: {keys:?}, out: {out}"
    );
    assert!(
        keys.contains(&"sub/".to_string()),
        "keys: {keys:?}, out: {out}"
    );
    assert!(
        keys.iter().all(|k| !k.is_empty()),
        "no key should be empty: {keys:?}"
    );
}

#[test]
fn ls_bare_prefix_with_no_directory_children_falls_back_to_partial_name_match() {
    // A bare target that isn't a real object AND has no children under
    // `<target>/` isn't a directory at all -- it's a partial-filename
    // search (`ls bucket/pre` matching `prefix1.txt`). The HeadObject miss
    // in `ls` must not unconditionally treat every miss as "append '/' and
    // list as a directory": that would silently drop `prefix1.txt` from
    // the result (real mc, and rs3 before the bare-directory fix, list it
    // by falling back to a literal-prefix listing; `client-s3.go:1684-1697`
    // gates the directory interpretation on a 1-key probe under
    // `pre + "/"` actually finding something).
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lspartial"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"hello").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lspartial/prefix1.txt"]);
    let out = server.rs3_ok(&["--json", "ls", "test/lspartial/pre"]);
    let lines: Vec<&str> = out.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 1, "expected exactly one entry, out: {out:?}");
    let v: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(v["key"], "prefix1.txt", "out: {out}");
}

#[test]
fn ls_nested_bare_prefix_partial_match_strips_only_parent_dir() {
    // Same partial-filename-search path as the test above, but nested one
    // directory down. mc's `generateContentMessages` (cmd/ls.go) truncates
    // the strip boundary to the bare prefix's *parent directory*
    // (`prefixPath = prefixPath[:strings.LastIndex(prefixPath, "/")+1]`),
    // so `ls bucket/dir/pre` matching `dir/prefix1.txt` displays
    // `prefix1.txt` -- relative to `dir/`, not the full key. The bare
    // prefix must still be the *query* prefix (that's what scopes the
    // search), which is why `dir2/other.txt` must not appear at all.
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lsnested"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"hello").unwrap();
    server.rs3_ok(&[
        "put",
        src.to_str().unwrap(),
        "test/lsnested/dir/prefix1.txt",
    ]);
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lsnested/dir2/other.txt"]);
    let out = server.rs3_ok(&["--json", "ls", "test/lsnested/dir/pre"]);
    let lines: Vec<&str> = out.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 1, "expected exactly one entry, out: {out:?}");
    let v: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(v["key"], "prefix1.txt", "out: {out}");
}

#[test]
fn ls_summarize_and_human_format() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lssum"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, vec![0u8; 1024]).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lssum/a.bin"]);
    let out = server.rs3_ok(&["ls", "--summarize", "test/lssum"]);
    assert!(out.contains("Total Size: 1.0KiB"), "out: {out}");
    assert!(out.contains("Total Objects: 1"), "out: {out}");
    // human per-object line: "[YYYY-MM-DD HH:MM:SS +ZZ:ZZ]  1.0KiB a.bin"
    let re =
        regex_lite("\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} [^\\]]+\\] +1\\.0KiB a\\.bin");
    assert!(out.lines().any(re), "out: {out}");
}

#[test]
fn rb_json_always_compact_and_rm_modtime_null() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/msg1"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/msg1/f.txt"]);
    let out = server.rs3_ok(&["--json", "rm", "test/msg1/f.txt"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert!(
        v["modTime"].is_null(),
        "modTime must be literal null: {out}"
    );
    assert_eq!(v["dryRun"], false);
    let out = server.rs3_ok(&["--json", "rb", "test/msg1"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(v["status"], "success");
    assert_eq!(v["bucket"], "test/msg1");
}

#[test]
fn ls_incomplete_lists_multipart_uploads() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/inc1"]);
    // start a multipart upload directly via the SDK and don't complete it
    common::start_incomplete_multipart(&server, "inc1", "pending/upload.bin");
    let out = server.rs3_ok(&["ls", "--incomplete", "test/inc1"]);
    assert!(out.contains("pending/upload.bin"), "out: {out}");
}

#[test]
fn ls_storage_class_filters_non_matching_but_keeps_empty_class() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/lssc"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/lssc/f.txt"]);
    // The server always reports STANDARD for plain objects, so a matching
    // filter keeps the object and a non-matching one excludes it.
    let matching = server.rs3_ok(&["ls", "--storage-class", "STANDARD", "test/lssc"]);
    assert!(matching.contains("f.txt"), "out: {matching}");
    let non_matching = server.rs3_ok(&["ls", "--storage-class", "GLACIER", "test/lssc"]);
    assert!(!non_matching.contains("f.txt"), "out: {non_matching}");
    // "*" disables the filter entirely.
    let unfiltered = server.rs3_ok(&["ls", "--storage-class", "*", "test/lssc"]);
    assert!(unfiltered.contains("f.txt"), "out: {unfiltered}");
}

#[test]
fn cp_quiet_and_json_emit_per_object_lines_plus_summary() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/cpout"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"hello").unwrap();
    // --json: one copyMessage line + one accountStat line
    let out = server.rs3_ok(&["--json", "cp", src.to_str().unwrap(), "test/cpout/f.txt"]);
    let lines: Vec<serde_json::Value> = out
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2, "expected copyMessage + accountStat: {out}");
    assert_eq!(lines[0]["target"], "test/cpout/f.txt");
    assert_eq!(lines[0]["size"], 5);
    assert!(
        lines[1]["duration"].is_u64() || lines[1]["duration"].is_i64(),
        "duration must be raw ns int"
    );
    assert_eq!(lines[1]["transferred"], 5);
    // --quiet human: "`src` -> `dst`" line + summary line
    let out = server.rs3_ok(&["--quiet", "cp", src.to_str().unwrap(), "test/cpout/f2.txt"]);
    assert!(out.contains("` -> `test/cpout/f2.txt`"), "out: {out}");
    assert!(out.contains("Transferred:"), "out: {out}");
}

#[test]
fn mirror_json_emits_mirror_messages() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mirout"]);
    let dir = server.dir.path().join("mo");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("a.txt"), b"a").unwrap();
    let out = server.rs3_ok(&["--json", "mirror", dir.to_str().unwrap(), "test/mirout/p"]);
    let first: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
    assert_eq!(first["status"], "success");
    assert!(
        first["target"].as_str().unwrap().ends_with("p/a.txt"),
        "out: {out}"
    );
}

#[test]
fn errors_are_mc_shaped() {
    let server = TestServer::start();
    // human mode: stderr line starts with "rs3: <ERROR> ", exit 1
    let out = server.rs3(&["stat", "test/nosuchbucket/key"]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.starts_with("rs3: <ERROR> "), "stderr was: {err}");
    // json mode: stdout gets the error envelope, single line (piped => compact)
    let out = server.rs3(&["--json", "stat", "test/nosuchbucket/key"]);
    assert!(!out.status.success());
    let line = String::from_utf8_lossy(&out.stdout);
    let v: serde_json::Value = serde_json::from_str(line.trim()).expect("valid json");
    assert_eq!(v["status"], "error");
    assert_eq!(v["error"]["type"], "fatal");
    assert!(!v["error"]["message"].as_str().unwrap().is_empty());
    assert!(
        !line.trim().contains('\n'),
        "must be single-line when piped"
    );
}
