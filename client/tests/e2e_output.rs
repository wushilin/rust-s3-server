mod common;
use common::TestServer;

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
    assert!(v["error"]["message"].as_str().unwrap().len() > 0);
    assert!(
        !line.trim().contains('\n'),
        "must be single-line when piped"
    );
}
