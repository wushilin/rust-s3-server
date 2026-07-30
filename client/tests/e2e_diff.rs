mod common;
use common::TestServer;

#[test]
fn diff_markers_and_json_int() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dfa"]);
    server.rs3_ok(&["mb", "test/dfb"]);
    let f = |name: &str, content: &[u8]| {
        let p = server.dir.path().join(name);
        std::fs::write(&p, content).unwrap();
        p
    };
    let same = f("same.txt", b"same");
    let sized = f("sized.txt", b"12345");
    let sized2 = f("sized2.txt", b"123456789");
    let only1 = f("only1.txt", b"1");
    server.rs3_ok(&["put", same.to_str().unwrap(), "test/dfa/same.txt"]);
    server.rs3_ok(&["put", same.to_str().unwrap(), "test/dfb/same.txt"]);
    server.rs3_ok(&["put", sized.to_str().unwrap(), "test/dfa/sized.txt"]);
    server.rs3_ok(&["put", sized2.to_str().unwrap(), "test/dfb/sized.txt"]);
    server.rs3_ok(&["put", only1.to_str().unwrap(), "test/dfa/only1.txt"]);
    let out = server.rs3_ok(&["diff", "test/dfa", "test/dfb"]);
    assert!(out.contains("< test/dfa/only1.txt"), "out: {out}");
    assert!(out.contains("! test/dfb/sized.txt"), "out: {out}");
    assert!(
        !out.contains("same.txt"),
        "equal objects print nothing: {out}"
    );
    let out = server.rs3_ok(&["--json", "diff", "test/dfa", "test/dfb"]);
    let diffs: Vec<serde_json::Value> = out
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let only = diffs
        .iter()
        .find(|d| d["first"].as_str().unwrap().contains("only1"))
        .unwrap();
    assert_eq!(only["diff"], 5, "only-in-first is raw int 5");
    let size = diffs
        .iter()
        .find(|d| d["second"].as_str().unwrap().contains("sized"))
        .unwrap();
    assert_eq!(size["diff"], 2, "size diff is raw int 2");
}
