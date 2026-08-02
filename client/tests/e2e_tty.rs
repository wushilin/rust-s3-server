//! Terminal-simulation coverage for the live progress UI.
//!
//! The worker-lane grid, the `> IDLE` rows, the `TOTAL` bar, and the
//! control-plane spinner lines are all gated on `ui_enabled()`, which requires
//! stdout to be a real TTY (`progress.rs`). Every other e2e test in this crate
//! runs rs3 with piped stdio, so none of them exercise a single line of the
//! rendering path -- it was previously verified only by unit tests over the
//! accounting, plus manual eyeballing.
//!
//! These tests close that gap by running rs3 under a pty allocated by
//! `script(1)` (util-linux), which is the only pty allocator we can rely on
//! without taking a new dependency. `script -qec CMD /dev/null` runs `CMD`
//! with stdin/stdout/stderr all bound to a fresh pty and writes the typescript
//! to `/dev/null`, while the command's own output still arrives on script's
//! stdout -- which is what `Command::output()` captures here.
//!
//! The whole file skips itself when `script` is unavailable (non-Linux hosts,
//! or a container without util-linux), rather than failing the suite.

mod common;
use common::TestServer;

/// Rows/cols forced inside the pty. Wide enough that a bar label isn't
/// condensed to the point of losing its verb, and tall enough that
/// `lane_count` never has to clamp a `-P` we ask for in these tests.
const PTY_ROWS: usize = 40;
const PTY_COLS: usize = 200;

fn script_available() -> bool {
    std::process::Command::new("script")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Runs `rs3 <args>` inside a pty and returns everything it painted
/// (stdout and stderr are the same pty, so bars and stdout lines interleave
/// exactly as a user would see them).
///
/// The command is assembled as a shell string because that is `script -c`'s
/// interface; every argument is single-quote escaped, and `stty` fixes the
/// window size up front so `lane_count` sees a deterministic terminal rather
/// than whatever size the pty was born with.
fn run_in_pty(server: &TestServer, args: &[&str]) -> String {
    fn shell_quote(s: &str) -> String {
        format!("'{}'", s.replace('\'', r#"'\''"#))
    }
    let rs3 = env!("CARGO_BIN_EXE_rs3");
    let command = format!(
        "stty rows {PTY_ROWS} cols {PTY_COLS}; {} {}",
        shell_quote(rs3),
        args.iter()
            .map(|a| shell_quote(a))
            .collect::<Vec<_>>()
            .join(" ")
    );
    let out = std::process::Command::new("script")
        .args(["-qec", &command, "/dev/null"])
        .env("MC_HOST_TEST", server.mc_host())
        .env("MC_CONFIG_DIR", server.dir.path().join("mc-config"))
        .env("TERM", "xterm-256color")
        .output()
        .expect("run rs3 under script(1)");
    assert!(
        out.status.success(),
        "rs3 {args:?} failed under a pty:\n{}",
        String::from_utf8_lossy(&out.stdout)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Seeds `count` small objects under `bucket` and returns the local dir they
/// were uploaded from.
fn seed_objects(server: &TestServer, bucket: &str, count: usize) -> std::path::PathBuf {
    server.rs3_ok(&["mb", &format!("test/{bucket}")]);
    let dir = server.dir.path().join(format!("seed-{bucket}"));
    std::fs::create_dir_all(&dir).unwrap();
    for i in 0..count {
        // Big enough that a bar is on screen for more than one draw tick.
        let body: Vec<u8> = (0..400_000u32)
            .map(|b| (b.wrapping_add(i as u32) % 251) as u8)
            .collect();
        std::fs::write(dir.join(format!("obj{i}.bin")), &body).unwrap();
    }
    dir
}

#[test]
fn transfer_on_a_tty_renders_worker_lanes_idle_rows_and_total() {
    if !script_available() {
        eprintln!("skipping: script(1) not available");
        return;
    }
    let server = TestServer::start();
    let dir = seed_objects(&server, "lanes", 3);

    // -P 6 with only 3 objects guarantees at least three lanes never get work,
    // which is precisely what must render as `> IDLE` rather than collapsing
    // the grid -- the whole point of the fixed-lane design.
    let painted = run_in_pty(
        &server,
        &[
            "cp",
            "-r",
            "-P",
            "6",
            dir.to_str().unwrap(),
            "test/lanes/dest",
        ],
    );

    assert!(
        painted.contains("IDLE"),
        "expected idle lane rows in the grid:\n{painted}"
    );
    assert!(
        painted.contains("TOTAL"),
        "expected the overall TOTAL bar:\n{painted}"
    );
    assert!(
        painted.contains("Uploading"),
        "expected a verbed bar label:\n{painted}"
    );
    // The objects really did land, i.e. the UI didn't replace the transfer.
    let listing = server.rs3_ok(&["ls", "--recursive", "test/lanes"]);
    for i in 0..3 {
        assert!(listing.contains(&format!("obj{i}.bin")), "{listing}");
    }
}

#[test]
fn multipart_upload_on_a_tty_shows_control_plane_spinner_lines() {
    if !script_available() {
        eprintln!("skipping: script(1) not available");
        return;
    }
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mpu"]);
    let src = server.dir.path().join("big.bin");
    let body: Vec<u8> = (0..12 * 1024 * 1024u32).map(|i| (i % 249) as u8).collect();
    std::fs::write(&src, &body).unwrap();

    let painted = run_in_pty(
        &server,
        &[
            "put",
            "--part-size",
            "5MiB",
            src.to_str().unwrap(),
            "test/mpu/big.bin",
        ],
    );

    // Byte-less control-plane calls are dispatched as worker tasks and render
    // as transient `<label> <spinner> <ApiName>` lines. Only the *opening*
    // call is asserted: a task line is painted at the next draw tick and
    // erased the moment the call returns, so against a loopback server an op
    // that completes in under a tick may never appear on screen at all.
    // `CreateMultipartUpload` is reliable because it runs while the grid is
    // otherwise empty; `CompleteMultipartUpload` races the UI's own shutdown
    // and is genuinely not observable here, so asserting it would be flaky.
    assert!(
        painted.contains("CreateMultipartUpload"),
        "expected a CreateMultipartUpload task line:\n{painted}"
    );
    // The part bars themselves carry a `part N/M` suffix and are on screen for
    // the whole upload, so they are deterministic.
    assert!(
        painted.contains("part "),
        "expected per-part bar labels:\n{painted}"
    );
    // The upload really completed, whether or not its final line was painted.
    let stat = server.rs3_ok(&["stat", "test/mpu/big.bin"]);
    assert!(
        stat.contains("-3"),
        "expected a 3-part multipart etag: {stat}"
    );
}

#[test]
fn standalone_command_on_a_tty_has_task_lines_but_no_total_bar() {
    if !script_available() {
        eprintln!("skipping: script(1) not available");
        return;
    }
    let server = TestServer::start();
    let dir = seed_objects(&server, "standalone", 2);
    server.rs3_ok(&["cp", "-r", dir.to_str().unwrap(), "test/standalone/d"]);

    let painted = run_in_pty(&server, &["ls", "--recursive", "test/standalone"]);

    // Tasks-only mode: no persistent overall bar, so `ls` output can never be
    // glued onto a bar line.
    assert!(
        !painted.contains("TOTAL"),
        "standalone commands must not render a TOTAL bar:\n{painted}"
    );
    // The listing itself still reached stdout intact.
    for i in 0..2 {
        assert!(
            painted.contains(&format!("obj{i}.bin")),
            "listing lost an object under a TTY:\n{painted}"
        );
    }
}

#[test]
fn json_and_quiet_suppress_the_ui_even_on_a_tty() {
    if !script_available() {
        eprintln!("skipping: script(1) not available");
        return;
    }
    let server = TestServer::start();
    let dir = seed_objects(&server, "suppressed", 2);

    for flag in ["--json", "--quiet"] {
        let painted = run_in_pty(
            &server,
            &[
                "cp",
                "-r",
                flag,
                dir.to_str().unwrap(),
                &format!("test/suppressed/via{}", flag.trim_start_matches('-')),
            ],
        );
        assert!(
            !painted.contains("IDLE") && !painted.contains("TOTAL"),
            "{flag} must suppress the lane grid on a TTY:\n{painted}"
        );
    }
}

#[test]
fn p1_multipart_completes_on_a_tty() {
    if !script_available() {
        eprintln!("skipping: script(1) not available");
        return;
    }
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/ponebudget"]);
    let src = server.dir.path().join("p1.bin");
    let body: Vec<u8> = (0..12 * 1024 * 1024u32).map(|i| (i % 247) as u8).collect();
    std::fs::write(&src, &body).unwrap();

    // Control-plane ops take a budget token just like part uploads do, so a
    // budget of exactly 1 is the case where a naive implementation deadlocks:
    // CompleteMultipartUpload would wait for a token the finished parts have
    // already released. Asserted live under a TTY, where the UI is also
    // holding lanes.
    run_in_pty(
        &server,
        &[
            "put",
            "-P",
            "1",
            "--part-size",
            "5MiB",
            src.to_str().unwrap(),
            "test/ponebudget/big.bin",
        ],
    );

    let out = server.dir.path().join("p1.out");
    server.rs3_ok(&["get", "test/ponebudget/big.bin", out.to_str().unwrap()]);
    assert_eq!(std::fs::read(&out).unwrap(), body);
}
