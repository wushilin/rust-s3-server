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
    let (painted, status) = run_in_pty_allowing_failure(server, args);
    assert!(status, "rs3 {args:?} failed under a pty:\n{painted}");
    painted
}

/// [`run_in_pty`] without the success assertion, for the cases whose whole
/// point is what the UI does while rs3 is reporting failures. Returns the
/// painted bytes and whether rs3 exited 0.
fn run_in_pty_allowing_failure(server: &TestServer, args: &[&str]) -> (String, bool) {
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
        // `-e` makes script(1) exit with the command's own status.
        .args(["-qec", &command, "/dev/null"])
        .env("MC_HOST_TEST", server.mc_host())
        .env("MC_CONFIG_DIR", server.dir.path().join("mc-config"))
        .env("TERM", "xterm-256color")
        .output()
        .expect("run rs3 under script(1)");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        out.status.success(),
    )
}

/// Replays a pty byte stream into the lines a user would end up looking at.
///
/// Every other assertion in this file greps the raw stream, which answers
/// "did rs3 write these bytes" -- not "did the user get to read them". Those
/// are different questions the moment a live `MultiProgress` is on screen:
/// it redraws by walking the cursor back up over the block it painted last
/// time and rewriting it, so a message written into that block is erased by
/// the next redraw, and a redraw that starts from the wrong row duplicates
/// the grid down the screen instead of updating it. Both look perfectly fine
/// in the raw bytes. Replaying them is the only way to tell.
///
/// Deliberately an *infinite-height* model -- no scrolling, no fixed row
/// count. Content that scrolls off a real terminal is still content the user
/// saw, and it is exactly where stale duplicated grids pile up; dropping it
/// would discard the evidence. Handles what indicatif actually emits: `\r`,
/// `\n`, `ESC[nA`/`ESC[nB` (cursor up/down), `ESC[2K` (clear line), and SGR
/// colour runs, which are skipped.
fn replay(painted: &str) -> Vec<String> {
    let mut lines: Vec<String> = vec![String::new()];
    let (mut row, mut col) = (0usize, 0usize);
    let mut chars = painted.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\r' => col = 0,
            '\n' => {
                row += 1;
                while lines.len() <= row {
                    lines.push(String::new());
                }
            }
            '\x1b' => {
                if chars.peek() != Some(&'[') {
                    continue; // not a CSI sequence; nothing here emits others
                }
                chars.next();
                let mut params = String::new();
                let mut final_byte = ' ';
                for c in chars.by_ref() {
                    if c.is_ascii_digit() || c == ';' || c == '?' {
                        params.push(c);
                    } else {
                        final_byte = c;
                        break;
                    }
                }
                let n: usize = params.parse().unwrap_or(1).max(1);
                match final_byte {
                    'A' => row = row.saturating_sub(n),
                    'B' => {
                        row += n;
                        while lines.len() <= row {
                            lines.push(String::new());
                        }
                    }
                    'C' => col += n,
                    'D' => col = col.saturating_sub(n),
                    'K' => {
                        // 2K clears the whole line, 0K (default) from the
                        // cursor rightwards.
                        let keep = if params.starts_with('2') { 0 } else { col };
                        lines[row] = lines[row].chars().take(keep).collect();
                    }
                    _ => {} // 'm' (SGR) and anything else: no effect on text
                }
            }
            c => {
                let line = &mut lines[row];
                let mut cells: Vec<char> = line.chars().collect();
                while cells.len() <= col {
                    cells.push(' ');
                }
                cells[col] = c;
                *line = cells.into_iter().collect();
                col += 1;
            }
        }
    }
    lines
        .into_iter()
        .map(|l| l.trim_end().to_string())
        .collect()
}

/// Seeds `count` small objects under `bucket` and returns the local dir they
/// were uploaded from. For tests that only need the objects to exist.
fn seed_objects(server: &TestServer, bucket: &str, count: usize) -> std::path::PathBuf {
    sized_objects(server, bucket, count, 400_000)
}

/// Seeds `count` objects of `bytes` each. Tests that assert on what a *transfer
/// bar* painted must size these against the draw rate, not against convenience:
/// indicatif redraws a terminal target ~20x/sec, and against a loopback server
/// a 400 KB object is gone in a couple of milliseconds — no frame ever catches
/// the lane mid-upload, and the assertion fails perhaps one run in three.
fn sized_objects(
    server: &TestServer,
    bucket: &str,
    count: usize,
    bytes: usize,
) -> std::path::PathBuf {
    server.rs3_ok(&["mb", &format!("test/{bucket}")]);
    let dir = server.dir.path().join(format!("seed-{bucket}"));
    std::fs::create_dir_all(&dir).unwrap();
    for i in 0..count {
        let body: Vec<u8> = (0..bytes as u32)
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
    // 24 MiB each: this test asserts on the mid-flight `Uploading` label, so
    // the objects have to outlive several draw frames (see `sized_objects`).
    let dir = sized_objects(&server, "lanes", 3, 24 * 1024 * 1024);

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

/// Every `x/y objects` denominator the TOTAL row ever painted, in order.
fn total_denominators(painted: &str) -> Vec<u64> {
    painted
        .match_indices(" objects")
        .filter_map(|(at, _)| {
            let pair = painted[..at].rsplit(|c: char| c.is_whitespace()).next()?;
            pair.split_once('/')?.1.parse().ok()
        })
        .collect()
}

/// A recursive `cp` plans before it copies, so the TOTAL row must know the
/// whole workload from its first frame -- the denominator leads the work
/// rather than trailing it.
///
/// Regression guard: the total used to accrete one object at a time from
/// inside the transfer functions, which pinned it to `done + parallel`. With
/// `-P 2` over 8 objects that painted `0/2`, `1/3`, `2/4` ... and the run
/// never showed its real size until it was over. Asserting on the whole
/// sequence of denominators (rather than catching one lucky frame) keeps
/// this independent of how many frames the transfer happens to span.
#[test]
fn recursive_cp_declares_its_whole_total_before_transferring() {
    if !script_available() {
        eprintln!("skipping: script(1) not available");
        return;
    }
    let server = TestServer::start();
    // Big enough to span many draw frames, so a regressed denominator would
    // have every opportunity to be caught mid-climb.
    let dir = sized_objects(&server, "plan", 8, 4 * 1024 * 1024);

    let painted = run_in_pty(
        &server,
        &["cp", "-r", "-P", "2", dir.to_str().unwrap(), "test/plan/dest"],
    );

    let denominators = total_denominators(&painted);
    assert!(
        denominators.contains(&8),
        "expected the full 8-object total on the TOTAL row:\n{painted}"
    );
    // 0 is the idle `0/0 objects` row painted while the plan is still being
    // built; anything else between 1 and 7 is the total trailing the work.
    assert!(
        denominators.iter().all(|&d| d == 0 || d == 8),
        "TOTAL denominator climbed instead of leading: {denominators:?}\n{painted}"
    );
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

// --- what the user actually ends up reading -------------------------------
//
// These replay the pty stream (see `replay`) instead of grepping it. They
// cover the failure mode that grepping cannot see: a write that reaches the
// terminal but is then erased, or that corrupts the live grid's redraw so it
// duplicates itself down the screen.

/// A per-object failure message must survive as a readable line, and must
/// not leave the grid duplicated behind it.
///
/// Regression: `mirror`'s per-object failure used a bare `eprintln!`. Under
/// a TTY that lands *inside* the block the `MultiProgress` is about to
/// redraw in place -- so the message is overwritten by the next frame, and
/// the frame after that is painted a row lower, leaving a stale copy of the
/// grid stranded on screen for every message. With enough failures the
/// terminal fills with `> IDLE` rows and the user never learns what went
/// wrong. Every write now goes through `progress::suspend_bars`.
#[test]
fn mirror_failure_messages_survive_and_leave_no_stale_grid() {
    if !script_available() {
        eprintln!("skipping: script(1) not available");
        return;
    }
    let server = TestServer::start();
    let dir = seed_objects(&server, "failures", 6);
    server.rs3_ok(&["cp", "-r", dir.to_str().unwrap(), "test/failures/src"]);

    // Every object fails, and fails locally rather than over the wire, so
    // the messages arrive fast and interleaved with the live grid -- the
    // worst case for the redraw. A directory where a file must be written
    // is `EISDIR` for every download.
    let dest = server.dir.path().join("mirror-dest");
    std::fs::create_dir_all(&dest).unwrap();
    for i in 0..6 {
        std::fs::create_dir_all(dest.join(format!("obj{i}.bin"))).unwrap();
    }

    let (painted, ok) = run_in_pty_allowing_failure(
        &server,
        &["mirror", "test/failures/src", dest.to_str().unwrap()],
    );
    assert!(!ok, "every object should have failed:\n{painted}");
    let screen = replay(&painted);

    // 1. Every failure is readable. Before the fix roughly a third of these
    //    were painted and then overwritten by the next frame.
    for i in 0..6 {
        let needle = format!("mirror: `obj{i}.bin` failed");
        let hits = screen.iter().filter(|l| l.contains(&needle)).count();
        assert_eq!(
            hits,
            1,
            "expected exactly one readable line for obj{i}.bin, found {hits}:\n{}",
            screen.join("\n")
        );
    }

    // 2. Each one is a line of its own, not glued onto a bar row.
    for line in &screen {
        if let Some(idx) = line.find("mirror: `") {
            assert_eq!(
                idx,
                0,
                "failure message glued onto a bar row: {line:?}\n{}",
                screen.join("\n")
            );
        }
    }

    // 3. Nothing of the grid is left stranded. Slots are cleared as they
    //    release and the run ends on the TOTAL row alone, so a surviving
    //    `> IDLE`/`Downloading`/`Inspecting` row means a frame was painted
    //    at an origin the next redraw never went back to.
    let stale: Vec<&String> = screen
        .iter()
        .filter(|l| l.contains("IDLE") || l.contains("Downloading") || l.contains("Inspecting"))
        .collect();
    assert!(
        stale.is_empty(),
        "stale progress rows left on screen: {stale:#?}\n{}",
        screen.join("\n")
    );
}

/// The healthy path's counterpart: a completed transfer leaves the finished
/// `TOTAL` row and nothing else -- no leftover slot rows, and no second copy
/// of the grid from the planning phase (`mirror` builds one `ProgressUi` for
/// the whole invocation, not one per phase).
#[test]
fn completed_mirror_leaves_only_the_total_row() {
    if !script_available() {
        eprintln!("skipping: script(1) not available");
        return;
    }
    let server = TestServer::start();
    let dir = seed_objects(&server, "clean", 4);
    server.rs3_ok(&["cp", "-r", dir.to_str().unwrap(), "test/clean/src"]);
    let dest = server.dir.path().join("clean-dest");

    let painted = run_in_pty(
        &server,
        &["mirror", "test/clean/src", dest.to_str().unwrap()],
    );
    let screen = replay(&painted);
    let leftover: Vec<&String> = screen
        .iter()
        .filter(|l| !l.is_empty() && !l.starts_with("TOTAL "))
        .collect();
    assert!(
        leftover.is_empty(),
        "expected only the TOTAL row to remain, also found: {leftover:#?}"
    );
    let totals = screen.iter().filter(|l| l.starts_with("TOTAL ")).count();
    assert_eq!(totals, 1, "exactly one TOTAL row:\n{}", screen.join("\n"));
    assert!(
        screen.iter().any(|l| l.contains("4/4 objects")),
        "TOTAL should report every object done:\n{}",
        screen.join("\n")
    );
    for i in 0..4 {
        assert!(
            dest.join(format!("obj{i}.bin")).exists(),
            "the UI didn't replace the transfer"
        );
    }
}

/// Standalone commands print their own stdout between dispatch calls. Those
/// prints go through the same `suspend_bars` choke point, so every listed
/// object must come out as its own readable line with no bar text fused to
/// it -- the failure e534ce7 originally hit, now prevented by suspending
/// rather than by having no persistent rows to collide with.
#[test]
fn standalone_command_output_is_never_fused_to_a_bar_row() {
    if !script_available() {
        eprintln!("skipping: script(1) not available");
        return;
    }
    let server = TestServer::start();
    let dir = seed_objects(&server, "interleaved", 5);
    server.rs3_ok(&["cp", "-r", dir.to_str().unwrap(), "test/interleaved/d"]);

    let painted = run_in_pty(&server, &["ls", "--recursive", "test/interleaved"]);
    let screen = replay(&painted);

    for i in 0..5 {
        let name = format!("obj{i}.bin");
        let hits: Vec<&String> = screen.iter().filter(|l| l.contains(&name)).collect();
        assert_eq!(hits.len(), 1, "expected one line for {name}, got {hits:#?}");
        // An `ls` line starts with its `[timestamp]`; anything before that
        // is bar text the print was glued onto.
        assert!(
            hits[0].starts_with('['),
            "listing line fused to a bar row: {:?}",
            hits[0]
        );
    }
    assert!(
        !screen.iter().any(|l| l.contains("IDLE")),
        "no slot rows should survive the command:\n{}",
        screen.join("\n")
    );
}
