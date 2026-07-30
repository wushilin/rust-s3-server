#![allow(dead_code)]
use std::io::Write;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

pub struct TestServer {
    child: Child,
    pub port: u16,
    pub dir: tempfile::TempDir,
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

fn rusts3_binary() -> PathBuf {
    if let Ok(bin) = std::env::var("RUSTS3_BIN") {
        return PathBuf::from(bin);
    }
    // client/ crate root -> repo root
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("client has parent")
        .to_path_buf();
    let bin = repo_root.join("target/release/rusts3");
    if !bin.exists() {
        let status = Command::new("cargo")
            .args(["build", "--release"])
            .current_dir(&repo_root)
            .status()
            .expect("run cargo build for rusts3");
        assert!(status.success(), "building rusts3 failed");
    }
    bin
}

impl TestServer {
    pub fn start() -> Self {
        let dir = tempfile::tempdir().expect("create temp dir");
        let port = free_port();
        let config = format!(
            "server:\n  bind_address: 127.0.0.1\n  bind_port: {port}\n  base_dir: {data}\nui:\n  enabled: false\nauth:\n  enabled: true\n  credentials:\n    - access_key: testkey\n      secret_key: testsecret\nlogging:\n  level: warn\n  enable_bandwidth_report: false\n",
            data = dir.path().join("data").display()
        );
        let config_path = dir.path().join("config.yaml");
        std::fs::File::create(&config_path)
            .and_then(|mut f| f.write_all(config.as_bytes()))
            .expect("write server config");
        let child = Command::new(rusts3_binary())
            .args(["run", "-c"])
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn rusts3");
        let server = TestServer { child, port, dir };
        server.wait_ready();
        server
    }

    pub fn alias(&self) -> &'static str {
        "test"
    }

    fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let out = self.rs3(&["ls", "test/"]);
            if out.status.success() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "rusts3 did not become ready on port {}: {}",
                self.port,
                String::from_utf8_lossy(&out.stderr)
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    pub fn rs3(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_rs3"))
            .args(args)
            .env(
                "MC_HOST_TEST",
                format!("http://testkey:testsecret@127.0.0.1:{}", self.port),
            )
            .env("MC_CONFIG_DIR", self.dir.path().join("mc-config"))
            .output()
            .expect("run rs3")
    }

    pub fn rs3_ok(&self, args: &[&str]) -> String {
        let out = self.rs3(args);
        assert!(
            out.status.success(),
            "rs3 {:?} failed:\nstdout: {}\nstderr: {}",
            args,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// Directly PUT a zero-byte folder-marker object (key ending in `/`)
    /// using the aws-sdk-s3 crate, bypassing rs3's own `put` command (which
    /// has no way to create a key ending in `/`). Used to reproduce
    /// marker-object bugs that only manifest against a real S3-compatible
    /// server.
    pub fn put_marker(&self, bucket: &str, key: &str) {
        use aws_config::{BehaviorVersion, Region};
        use aws_credential_types::Credentials;
        use aws_sdk_s3::Client;
        use aws_sdk_s3::config::SharedCredentialsProvider;
        use aws_sdk_s3::primitives::ByteStream;

        let bucket = bucket.to_string();
        let key = key.to_string();
        let port = self.port;
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let creds = Credentials::new("testkey", "testsecret", None, None, "rs3-test");
                let sdk_cfg = aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new("us-east-1"))
                    .credentials_provider(SharedCredentialsProvider::new(creds))
                    .load()
                    .await;
                let s3_cfg = aws_sdk_s3::config::Builder::from(&sdk_cfg)
                    .endpoint_url(format!("http://127.0.0.1:{port}"))
                    .force_path_style(true)
                    .build();
                let client = Client::from_conf(s3_cfg);
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from_static(b""))
                    .send()
                    .await
                    .expect("put marker object");
            });
    }
}

/// Start (but never complete) a multipart upload directly via the
/// aws-sdk-s3 crate, bypassing rs3's own `put` command (which has no way to
/// leave an upload dangling). Used to reproduce `ls --incomplete` against a
/// real in-progress multipart upload.
pub fn start_incomplete_multipart(server: &TestServer, bucket: &str, key: &str) {
    use aws_config::{BehaviorVersion, Region};
    use aws_credential_types::Credentials;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::config::SharedCredentialsProvider;

    let bucket = bucket.to_string();
    let key = key.to_string();
    let port = server.port;
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let creds = Credentials::new("testkey", "testsecret", None, None, "rs3-test");
            let sdk_cfg = aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(SharedCredentialsProvider::new(creds))
                .load()
                .await;
            let s3_cfg = aws_sdk_s3::config::Builder::from(&sdk_cfg)
                .endpoint_url(format!("http://127.0.0.1:{port}"))
                .force_path_style(true)
                .build();
            let client = Client::from_conf(s3_cfg);
            client
                .create_multipart_upload()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .expect("create multipart upload");
        });
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
