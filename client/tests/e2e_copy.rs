mod common;
use common::TestServer;

#[test]
fn same_alias_copy_small_object() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/src"]);
    server.rs3_ok(&["mb", "test/dst"]);
    let src = server.dir.path().join("obj.bin");
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/src/dir/obj name.bin"]);
    server.rs3_ok(&["cp", "test/src/dir/obj name.bin", "test/dst/copy.bin"]);
    let out = server.dir.path().join("copy.bin");
    server.rs3_ok(&["get", "test/dst/copy.bin", out.to_str().unwrap()]);
    assert_eq!(std::fs::read(&out).unwrap(), data);

    // verify server-side path was taken
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rs3"))
        .args(["cp", "test/src/dir/obj name.bin", "test/dst/copy2.bin"])
        .env("MC_HOST_TEST", server.mc_host())
        .env("MC_CONFIG_DIR", server.dir.path().join("mc-config"))
        .env("RS3_DEBUG_COPY", "1")
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains("falling back to streaming copy"),
        "same-endpoint copy took the streaming path"
    );
}

#[test]
fn same_alias_copy_large_object_uses_multipart() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/big"]);
    let src = server.dir.path().join("big.bin");
    let data: Vec<u8> = (0..12 * 1024 * 1024u32).map(|i| (i % 249) as u8).collect();
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&[
        "put",
        "--part-size",
        "5MiB",
        src.to_str().unwrap(),
        "test/big/a.bin",
    ]);
    server.rs3_ok(&[
        "cp",
        "--part-size",
        "5MiB",
        "test/big/a.bin",
        "test/big/b.bin",
    ]);
    let stat = server.rs3_ok(&["stat", "test/big/b.bin"]);
    assert!(
        stat.contains("-3"),
        "expected multipart etag on copy target: {stat}"
    );
    let out = server.dir.path().join("b.out");
    server.rs3_ok(&["get", "test/big/b.bin", out.to_str().unwrap()]);
    assert_eq!(std::fs::read(&out).unwrap(), data);

    // verify server-side path was taken
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rs3"))
        .args([
            "cp",
            "--part-size",
            "5MiB",
            "test/big/a.bin",
            "test/big/c.bin",
        ])
        .env("MC_HOST_TEST", server.mc_host())
        .env("MC_CONFIG_DIR", server.dir.path().join("mc-config"))
        .env("RS3_DEBUG_COPY", "1")
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains("falling back to streaming copy"),
        "same-endpoint copy took the streaming path"
    );
}
