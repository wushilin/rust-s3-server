//! Regression tests for transfer safety: keys that would escape a download
//! directory, symlinks on the local side of a mirror, metadata on
//! cross-endpoint copies, and concurrent pipe uploads.
mod common;
use common::TestServer;
use std::fs;

fn put_raw(server: &TestServer, bucket: &str, key: &str, body: &'static [u8]) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = server.sdk_client().await;
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(body))
            .send()
            .await
            .expect("put raw object");
    });
}

fn keys(server: &TestServer, bucket: &str) -> Vec<String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = server.sdk_client().await;
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .send()
            .await
            .unwrap();
        resp.contents()
            .iter()
            .filter_map(|o| o.key().map(str::to_string))
            .collect()
    })
}

/// A key is attacker-controlled. `x/../../escape.txt` joined onto the
/// destination used to write outside it; now the object is refused (the run
/// fails) and every safe key still lands.
#[test]
fn recursive_download_refuses_keys_that_escape_the_destination() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/trav"]);
    put_raw(&server, "trav", "ok.txt", b"fine");
    put_raw(&server, "trav", "x/../../escape.txt", b"pwned");
    let listed = keys(&server, "trav");
    assert!(
        listed.iter().any(|k| k.contains("..")),
        "the server must store the traversal key verbatim for this test to mean anything: {listed:?}"
    );

    let base = server.dir.path().join("base");
    for cmd in ["mirror", "cp"] {
        let dst = base.join(format!("dst-{cmd}"));
        fs::create_dir_all(&dst).unwrap();
        let args: Vec<&str> = match cmd {
            "mirror" => vec!["mirror", "test/trav", dst.to_str().unwrap()],
            _ => vec!["cp", "--recursive", "test/trav", dst.to_str().unwrap()],
        };
        let out = server.rs3(&args);
        assert!(
            !out.status.success(),
            "{cmd}: an unsafe key must fail the run: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("unsafe"),
            "{cmd}: stderr should say why: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(fs::read(dst.join("ok.txt")).unwrap(), b"fine", "{cmd}");
        assert!(
            !base.join("escape.txt").exists(),
            "{cmd}: wrote outside dst"
        );
        assert!(!dst.join("escape.txt").exists(), "{cmd}");
    }
}

/// Symlinked files and directories are mirrored as what they point at; a
/// broken link is warned about and its remote twin is never deleted by
/// `--remove`.
#[cfg(unix)]
#[test]
fn mirror_follows_symlinks_and_never_deletes_for_a_broken_one() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/sym"]);
    let outside = server.dir.path().join("outside");
    let src = server.dir.path().join("symsrc");
    fs::create_dir_all(outside.join("d")).unwrap();
    fs::create_dir_all(&src).unwrap();
    fs::write(outside.join("f.txt"), b"linked file").unwrap();
    fs::write(outside.join("d/g.txt"), b"linked dir").unwrap();
    fs::write(src.join("real.txt"), b"real").unwrap();
    std::os::unix::fs::symlink(outside.join("f.txt"), src.join("flink.txt")).unwrap();
    std::os::unix::fs::symlink(outside.join("d"), src.join("dlink")).unwrap();

    server.rs3_ok(&["mirror", src.to_str().unwrap(), "test/sym/p"]);
    let mut listed = keys(&server, "sym");
    listed.sort();
    assert_eq!(listed, vec!["p/dlink/g.txt", "p/flink.txt", "p/real.txt"]);

    // The link target goes away: the name is still there on the source, so
    // `--remove` must leave the remote copy alone.
    fs::remove_file(outside.join("f.txt")).unwrap();
    fs::write(src.join("real.txt"), b"real, changed").unwrap();
    let out = server.rs3(&[
        "mirror",
        "--remove",
        "--overwrite",
        src.to_str().unwrap(),
        "test/sym/p",
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("broken symlink"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let listed = keys(&server, "sym");
    assert!(listed.contains(&"p/flink.txt".to_string()), "{listed:?}");
    assert!(listed.contains(&"p/dlink/g.txt".to_string()), "{listed:?}");
}

/// A copy between two aliases that are not the same endpoint streams the
/// bytes through the client, which used to drop Content-Type and user
/// metadata. `localhost` vs `127.0.0.1` makes the same server a different
/// endpoint as far as rs3 is concerned.
#[test]
fn cross_endpoint_copy_carries_content_type_and_metadata() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/xep"]);
    let other = format!(
        "http://{}:{}@localhost:{}",
        server.access_key, server.secret_key, server.port
    );
    let small = server.dir.path().join("small.txt");
    fs::write(&small, b"hello").unwrap();
    let big = server.dir.path().join("big.bin");
    let data: Vec<u8> = (0..12 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    fs::write(&big, &data).unwrap();
    for (src, name) in [(&small, "small"), (&big, "big")] {
        server.rs3_ok(&[
            "put",
            "--part-size",
            "5MiB",
            "--attr",
            "Content-Type=text/x-rs3;X-Amz-Meta-Color=blue",
            src.to_str().unwrap(),
            &format!("test/xep/{name}.src"),
        ]);
        let out = server.rs3_env(
            &[
                "cp",
                "--part-size",
                "5MiB",
                &format!("test/xep/{name}.src"),
                &format!("other/xep/{name}.dst"),
            ],
            &[("MC_HOST_OTHER", &other), ("RS3_DEBUG_COPY", "1")],
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(out.status.success(), "{name}: {stderr}");
        assert!(
            stderr.contains("falling back to streaming copy"),
            "{name}: expected the cross-endpoint path: {stderr}"
        );
        let stat = server.rs3_ok(&["stat", "--json", &format!("test/xep/{name}.dst")]);
        let v: serde_json::Value = serde_json::from_str(stat.trim()).unwrap();
        assert_eq!(
            v["metadata"]["Content-Type"], "text/x-rs3",
            "{name}: {stat}"
        );
        assert_eq!(v["metadata"]["color"], "blue", "{name}: {stat}");
    }
    let back = server.dir.path().join("big.back");
    server.rs3_ok(&["get", "test/xep/big.dst", back.to_str().unwrap()]);
    assert_eq!(fs::read(back).unwrap(), data);
}

/// `pipe --concurrent` with several parts in flight must still assemble the
/// stream in order.
#[test]
fn pipe_concurrent_uploads_assemble_in_order() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/pipec"]);
    let data: Vec<u8> = (0..23 * 1024 * 1024u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rs3"));
    cmd.args([
        "pipe",
        "--part-size",
        "5MiB",
        "--concurrent",
        "3",
        "test/pipec/big.bin",
    ])
    .env("MC_HOST_TEST", server.mc_host())
    .env("MC_CONFIG_DIR", server.dir.path().join("mc-config"))
    .stdin(std::process::Stdio::piped())
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    {
        use std::io::Write;
        let mut stdin = child.stdin.take().unwrap();
        // Dribble the input so reads and uploads genuinely overlap.
        for chunk in data.chunks(1024 * 1024) {
            stdin.write_all(chunk).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stat = server.rs3_ok(&["stat", "test/pipec/big.bin"]);
    assert!(stat.contains("-5"), "expected 5 parts: {stat}");
    let back = server.dir.path().join("pipec.out");
    server.rs3_ok(&["get", "test/pipec/big.bin", back.to_str().unwrap()]);
    assert!(fs::read(back).unwrap() == data, "content mismatch");
}
