//! Holds `rusts3` to real S3's behaviour, over real HTTP.
//!
//! `compat/aws/aws-golden.json` is a recording of what
//! `s3.ap-southeast-1.amazonaws.com` actually returned for 39 operations:
//! status, headers and XML for the whole object and multipart lifecycle plus
//! the interesting errors. This test replays the identical scenario against a
//! freshly started `rusts3` — signed SigV4 requests on a real socket, not
//! in-process router calls — and fails on any difference that is not listed,
//! with a reason, in `compat/aws/accepted-divergences.json`.
//!
//! Two properties make this worth more than the unit tests around it:
//!
//! 1. **The oracle is Amazon, not the author.** Every other test in this repo
//!    asserts what someone believed S3 does. This asserts what it did.
//! 2. **No credentials, no network.** The recording is checked in, so the
//!    comparison runs in CI exactly as it runs here. Refreshing the recording
//!    needs AWS access; checking against it never does.
//!
//! When this fails, read the printed findings. Either the server drifted, or
//! the divergence is deliberate — in which case add a rule *with its reason*
//! to the accepted-divergences file. Silently widening a rule to make the test
//! pass throws away the only thing it measures.

use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The bucket and prefix baked into the golden recording. The probe normalises
/// both out of the captured bodies, but it still has to *ask* for the same
/// names so the requests it signs are the same shape.
const GOLDEN_BUCKET: &str = "compat-probe-bucket";
const GOLDEN_PREFIX: &str = "rs3-compat-probe3";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn have_python() -> bool {
    Command::new("python3")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// A `rusts3` running on a real port, killed on drop.
struct Server {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    fn start(binary: &Path) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let port = free_port();
        let config = format!(
            "server:\n  bind_address: 127.0.0.1\n  bind_port: {port}\n  base_dir: {data}\n\
             ui:\n  enabled: false\n\
             auth:\n  enabled: true\n  credentials:\n    - access_key: compatkey\n      \
             secret_key: compatsecret\n\
             logging:\n  level: warn\n  enable_bandwidth_report: false\n",
            data = dir.path().join("data").display()
        );
        let config_path = dir.path().join("config.yaml");
        std::fs::File::create(&config_path)
            .and_then(|mut f| f.write_all(config.as_bytes()))
            .expect("write config");

        let child = Command::new(binary)
            .args(["run", "-c"])
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn rusts3");
        let server = Server {
            child,
            port,
            _dir: dir,
        };
        server.wait_ready();
        server
    }

    fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("rusts3 did not start listening on port {}", self.port);
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

/// Builds `rusts3` if it is not already built. The test drives the real binary
/// over a socket, so an in-process router is not a substitute.
fn rusts3_binary() -> PathBuf {
    if let Ok(path) = std::env::var("RUSTS3_BIN") {
        return PathBuf::from(path);
    }
    let root = repo_root();
    for profile in ["release", "debug"] {
        let candidate = root.join("target").join(profile).join("rusts3");
        if candidate.exists() {
            return candidate;
        }
    }
    let status = Command::new("cargo")
        .args(["build", "--bin", "rusts3"])
        .current_dir(&root)
        .status()
        .expect("cargo build");
    assert!(status.success(), "building rusts3 failed");
    root.join("target/debug/rusts3")
}

fn probe(args: &[&str]) -> std::process::Output {
    Command::new("python3")
        .arg(repo_root().join("compat/aws/s3probe.py"))
        .args(args)
        .env("AWS_ACCESS_KEY_ID", "compatkey")
        .env("AWS_SECRET_ACCESS_KEY", "compatsecret")
        .current_dir(repo_root())
        .output()
        .expect("run s3probe")
}

#[test]
fn responses_match_the_recorded_aws_behaviour() {
    if !have_python() {
        eprintln!("skipping aws_compat: python3 not available");
        return;
    }
    let root = repo_root();
    let golden = root.join("compat/aws/aws-golden.json");
    let accepted = root.join("compat/aws/accepted-divergences.json");
    assert!(golden.exists(), "missing golden capture at {golden:?}");

    let server = Server::start(&rusts3_binary());
    let workdir = tempfile::tempdir().expect("tempdir");
    let capture = workdir.path().join("rusts3.json");

    // The scenario assumes its bucket already exists, as it does on AWS.
    let created = probe(&[
        "create-bucket",
        "--endpoint",
        &server.endpoint(),
        "--region",
        "us-east-1",
        "--bucket",
        GOLDEN_BUCKET,
    ]);
    assert!(
        created.status.success(),
        "create bucket failed:\n{}",
        String::from_utf8_lossy(&created.stderr)
    );

    let captured = probe(&[
        "capture",
        "--endpoint",
        &server.endpoint(),
        "--region",
        "us-east-1",
        "--bucket",
        GOLDEN_BUCKET,
        "--prefix",
        GOLDEN_PREFIX,
        "--out",
        capture.to_str().unwrap(),
    ]);
    assert!(
        captured.status.success(),
        "capture failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&captured.stdout),
        String::from_utf8_lossy(&captured.stderr)
    );

    let diff = probe(&[
        "diff",
        golden.to_str().unwrap(),
        capture.to_str().unwrap(),
        "--accepted",
        accepted.to_str().unwrap(),
        "--strict",
    ]);
    assert!(
        diff.status.success(),
        "rusts3 diverges from recorded AWS behaviour:\n{}",
        String::from_utf8_lossy(&diff.stdout)
    );
}
