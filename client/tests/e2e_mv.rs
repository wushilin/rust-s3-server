mod common;
use common::TestServer;

#[test]
fn mv_moves_object() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mv1"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"move me").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/mv1/a.txt"]);
    server.rs3_ok(&["mv", "test/mv1/a.txt", "test/mv1/b.txt"]);
    assert!(
        !server.rs3(&["stat", "test/mv1/a.txt"]).status.success(),
        "source must be gone"
    );
    server.rs3_ok(&["stat", "test/mv1/b.txt"]);
}

#[test]
fn mv_recursive_and_local_source() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mv2"]);
    let dir = server.dir.path().join("mvdir");
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("sub/x.txt"), b"x").unwrap();
    server.rs3_ok(&["mv", "-r", dir.to_str().unwrap(), "test/mv2/p"]);
    server.rs3_ok(&["stat", "test/mv2/p/sub/x.txt"]);
    assert!(
        !dir.join("sub/x.txt").exists(),
        "local source file must be deleted"
    );
}

#[test]
fn mv_subdirectory_guard() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mv3"]);
    let out = server.rs3(&["mv", "-r", "test/mv3/p", "test/mv3/p/sub"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("subdirectories of each other"));
}
