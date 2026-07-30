# rs3 mc-Compatibility Tier 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make rs3's output contract (--json, human formats, --quiet, progress, errors, exit codes) and remaining portable commands/flags (`mv`, `head`, `du`, `tree`, `pipe`, `diff`, `find`, `share`, time filters, `--attr`, `--preserve`, `--if-not-exists`, cat/stat/ls gap flags) match MinIO `mc` for S3-portable operations, per the mc source research.

**Architecture:** A new `output.rs` module becomes the single print choke point (mc's `printMsg` equivalent): every command emits typed message structs that render to mc-shape human strings or mc-shape JSON depending on global flags and TTY state. New commands build on tier-1's transfer/list/mirror engines. Every format decision is anchored to two committed research documents extracted from the mc source — cite them, don't guess.

**Tech Stack:** Rust edition 2024, aws-sdk-s3, clap 4, serde_json, chrono. New deps (only these): `indicatif = "0.17"` (progress bar), `flate2 = "1"` (head gzip), `regex = "1"` (find --regex), `shell-words = "1"` (find --exec tokenizing).

## Global Constraints

- **Normative references** (committed in-repo; every task MUST read the cited sections before coding): `docs/superpowers/research/mc-research-output.md` (called **[OUT]**) and `docs/superpowers/research/mc-research-semantics.md` (called **[SEM]**). When this plan and a research doc disagree, the research doc wins; note the discrepancy in the task report.
- All work in `client/`; never modify the server crate at repo root; never modify `~/workspace/mc`.
- MinIO-specific families stay out of scope: no `ping`/`ready` ([SEM] §14 — MinIO admin API), no `find --metadata/--tags` (MinIO-only), no versioning flags (`--rewind`/`--versions`/`--version-id` stay refused), no `--zip`, no `--md5`/`--checksum` (refused), no `mirror --watch`.
- JSON output policy ([OUT] §1.2, gotcha 4): pretty JSON with **one-space indent** when stdout is a TTY; **compact single-line** when stdout is not a TTY. Exceptions ([OUT] gotcha 2): `rb`'s message is ALWAYS compact; `ls --summarize`'s summary uses empty-string indent (multi-line, no leading spaces) when pretty.
- e2e tests capture rs3 via pipes → stdout is never a TTY → **e2e always asserts compact single-line JSON**. Pretty mode is unit-tested directly.
- Error output ([OUT] §1.3-1.4): human mode prints `` {argv0-basename}: <ERROR> {msg}{punct} {cause}. `` to **stderr** (for rs3 invoked normally that is `rs3: <ERROR> ...`); JSON mode prints `{"status":"error","error":{"message":...,"cause":{"message":...},"type":"fatal"|"error"}}` to **stdout**. Punctuation rule verbatim from [OUT] §1.4: if msg doesn't end in `:` or `.`, append `.` when cause starts uppercase else `:`; cause gets a trailing `.` if missing.
- Exit codes ([OUT] §3): 0 = fully clean, 1 = any error anywhere (boolean, never a count). No other codes are produced by rs3 itself.
- Human sizes: mc's `humanize.IBytes` with spaces stripped — IEC units, `%.1f` when the scaled value < 10 else `%.0f`, e.g. `1.0KiB`, `15KiB`, `79MiB`, `5B` ([OUT] §2 ls).
- Human dates: mc's `printDate` = local time `YYYY-MM-DD HH:MM:SS ZONE`. rs3 renders the zone as chrono's `%Z` for `Local` (numeric offset like `+00:00` where a zone name is unavailable) — accepted divergence from Go's zone names; tests match with a regex, not an exact zone string.
- Every new/changed flag that is NOT implemented must hard-error (`... is not implemented yet`), continuing tier-1 policy.
- Every task: `cargo fmt`, `cargo build` (no new warnings), full `cargo test` green, commit ending with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01EsC8WkurPSRq13FeKyfA5W`
- e2e harness: `client/tests/common/mod.rs` (`TestServer`); bucket names ≥3 chars.

---

### Task 1: Output core — printer, humanize, error machinery, exit codes

**Files:**
- Create: `client/src/output.rs`
- Modify: `client/src/main.rs` (Cli global flags already exist: json/quiet/no_color; initialize output state from them in `main()` before dispatch; route the final `Err` through the error machinery)
- Test: unit tests inside `output.rs` + `client/tests/e2e_output.rs`

**Interfaces (everything later tasks build on):**
```rust
// output.rs
pub(crate) struct OutputOpts { pub json: bool, pub quiet: bool, pub no_color: bool, pub stdout_tty: bool }
pub(crate) fn init_output(opts: OutputOpts);          // OnceLock; called once in main()
pub(crate) fn out() -> &'static OutputOpts;
pub(crate) enum JsonStyle { Standard, AlwaysCompact, EmptyIndent } // rb / ls-summarize quirks
pub(crate) trait McMessage {
    fn human(&self) -> String;                        // no trailing newline
    fn json(&self) -> serde_json::Value;              // includes "status" field where mc has one
    fn json_style(&self) -> JsonStyle { JsonStyle::Standard }
}
pub(crate) fn print_msg(msg: &dyn McMessage);          // stdout; picks human vs json by out().json
pub(crate) fn render_json(v: &serde_json::Value, style: JsonStyle, tty: bool) -> String; // pure, unit-testable
pub(crate) fn print_error(context_msg: &str, cause: &str, fatal: bool); // stderr human / stdout json envelope
pub(crate) fn humanize_ibytes(bytes: u64) -> String;   // "1.0KiB", "15KiB", "5B"
pub(crate) fn print_date(t: chrono::DateTime<chrono::Utc>) -> String; // local "%Y-%m-%d %H:%M:%S %Z"
```
- `render_json` semantics: `Standard` + tty → `serde_json::` pretty with ONE-SPACE indent (use `serde_json::Serializer::with_formatter` + `PrettyFormatter::with_indent(b" ")`); `Standard` + !tty → `to_string` compact; `AlwaysCompact` → compact regardless; `EmptyIndent` + tty → PrettyFormatter with `b""`; `EmptyIndent` + !tty → compact.
- `print_error` human: build per the punctuation rule in Global Constraints, prefix `{argv0}: <ERROR> `, write to stderr. JSON: envelope from [OUT] §1.3 with `"type": "fatal"` when `fatal` else `"error"`, rendered via `render_json(Standard)`, written to stdout.
- `main()` change: on `Err(e)` from a command, call `print_error(&format!("{e:#}"), "", true)`… — NO. Keep it simpler and mc-shaped: commands keep returning `anyhow::Result<()>`; `main()` matches `Err(e)`, splits `e.to_string()` as the message with empty cause (rs3's anyhow chains already embed cause text), calls `print_error(msg, "", true)`, then `std::process::exit(1)`. This replaces anyhow's default `Error: ...` print. TTY detection: `std::io::IsTerminal` on stdout (std, no new dep).

- [ ] **Step 1: Write failing unit tests** (inside `output.rs` `#[cfg(test)]`):

```rust
#[test]
fn ibytes_matches_mc() {
    assert_eq!(humanize_ibytes(0), "0B");
    assert_eq!(humanize_ibytes(5), "5B");
    assert_eq!(humanize_ibytes(1024), "1.0KiB");
    assert_eq!(humanize_ibytes(15 * 1024), "15KiB");
    assert_eq!(humanize_ibytes(82_854_982), "79MiB");
    assert_eq!(humanize_ibytes(5 * 1024 * 1024 * 1024), "5.0GiB");
}
#[test]
fn render_json_styles() {
    let v = serde_json::json!({"status":"success","bucket":"b"});
    assert_eq!(render_json(&v, JsonStyle::Standard, false), r#"{"status":"success","bucket":"b"}"#);
    let pretty = render_json(&v, JsonStyle::Standard, true);
    assert!(pretty.contains("\n \"bucket\""), "one-space indent, got: {pretty}");
    assert_eq!(render_json(&v, JsonStyle::AlwaysCompact, true), r#"{"status":"success","bucket":"b"}"#);
    let empty = render_json(&v, JsonStyle::EmptyIndent, true);
    assert!(empty.contains("\n\"bucket\""), "empty indent, got: {empty}");
}
#[test]
fn error_punctuation_rule() {
    // msg no terminal punct + cause starts uppercase => msg gets '.'
    assert_eq!(join_error_text("Unable to stat `x`", "Access Denied"), "Unable to stat `x`. Access Denied.");
    // cause starts lowercase => msg gets ':'
    assert_eq!(join_error_text("Unable to stat `x`", "no such key"), "Unable to stat `x`: no such key.");
    // msg already ends with '.' => untouched
    assert_eq!(join_error_text("Failed.", "boom"), "Failed. boom.");
    // empty cause => msg unchanged
    assert_eq!(join_error_text("Failed hard", ""), "Failed hard");
}
```
(`join_error_text(msg, cause) -> String` is the pure helper `print_error` uses; note field-order preservation in JSON requires `serde_json::json!` maps to preserve insertion order — enable the `preserve_order` feature of serde_json in Cargo.toml: `serde_json = { version = "1", features = ["preserve_order"] }`.)

- [ ] **Step 2: Run to verify failure**: `cargo test ibytes_matches_mc render_json_styles error_punctuation_rule` → FAIL (module absent).

- [ ] **Step 3: Implement `output.rs`.** `humanize_ibytes`: divide by 1024 until < 1024 through units `["B","KiB","MiB","GiB","TiB","PiB","EiB"]`; format `{:.1}` if scaled value < 10 (and unit != B) else `{:.0}`; bytes (<1024) always integer with `B`. `render_json`/`join_error_text`/`print_error`/`print_msg` per Interfaces. `init_output`/`out()` with `OnceLock<OutputOpts>` defaulting (for unit tests that never init) to `json:false,quiet:false,no_color:true,stdout_tty:false`.

- [ ] **Step 4: Wire `main()`**: after `Cli::parse()`, `init_output(OutputOpts{ json: cli.json, quiet: cli.quiet, no_color: cli.no_color, stdout_tty: std::io::stdout().is_terminal() })`. Replace the implicit anyhow error return: `if let Err(e) = run(cli).await { print_error(&format!("{e:#}"), "", true); std::process::exit(1); }` (extract the old match into `async fn run(cli: Cli) -> Result<()>`).

- [ ] **Step 5: e2e for the error path** (`client/tests/e2e_output.rs`):
```rust
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
    assert!(!line.trim().contains('\n'), "must be single-line when piped");
}
```
Add `serde_json` to `[dev-dependencies]` is unnecessary — it's already a main dependency; tests can use it.

- [ ] **Step 6: Run** `cargo test` (all green) → **Commit** `feat: mc-shaped output core (printer, humanize, error envelope, exit codes)`.

---

### Task 2: mc message shapes for ls / mb / rb / rm / stat (+ ls --summarize, --incomplete, --storage-class)

**Files:**
- Create: `client/src/messages.rs` (message structs shared by all commands)
- Modify: `client/src/main.rs` (`ls`, `mb`, `rb`, `stat` + `rm`/`remove_prefix` print paths; remove the ls guards for `--summarize`/`--incomplete`/`--storage-class` added in tier-1)
- Test: `client/tests/e2e_output.rs` (extend)

**Interfaces** — structs in `messages.rs`, all implementing `McMessage` ([OUT] §2 is normative for field names/order):
```rust
pub(crate) struct ContentMessage { pub status: String /*"success"*/, pub filetype: String /*"file"|"folder"*/,
    pub time: DateTime<Utc>, pub size: u64, pub key: String, pub etag: String,
    pub storage_class: Option<String> }
// json keys: status, type, lastModified (RFC3339), size, key, etag, storageClass (omit when None)
// human: "[{print_date}] {size:>7} {key}" where size = humanize_ibytes; folders: size renders as "0B", key gets trailing "/"
pub(crate) struct SummaryMessage { pub total_objects: u64, pub total_size: u64 }
// json: {"totalObjects":N,"totalSize":N}; style EmptyIndent; human: "\nTotal Size: {ibytes}\nTotal Objects: {n}"
pub(crate) struct MakeBucketMessage { pub bucket: String }  // json: status/bucket/region(always "")
// human: "Bucket created successfully `{bucket}`."
pub(crate) struct RemoveBucketMessage { pub bucket: String } // json: status/bucket; style AlwaysCompact
// human: "Removed `{bucket}` successfully."
pub(crate) struct RmMessage { pub key: String, pub dry_run: bool, pub mod_time: Option<DateTime<Utc>> }
// json: {"status":"success","key":...,"deleteMarker":false,"versionID":"","modTime":null,"dryRun":bool}
//   modTime: null when None (NOT omitted) — [OUT] gotcha 17
// human: "Removed `{key}`."  |  dry-run: "DRYRUN: Removing `{key}`."
pub(crate) struct StatMessage { pub key: String, pub date: DateTime<Utc>, pub size: u64,
    pub etag: String, pub content_type: Option<String>, pub metadata: BTreeMap<String,String> }
// json keys: status,name,lastModified,size,etag,type("file"),metadata(omit when empty)
// human ([OUT] §2 stat): "%-10s: value" lines in order Name,Date,Size,ETag,Type,Metadata(one aligned line per key); trailing blank line
```

- [ ] **Step 1: Failing e2e tests** (append to `e2e_output.rs`; all `--json` asserts parse the line and check exact fields):

```rust
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
    assert!(v.get("etag").is_some());
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
    let re = regex_lite("\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} [^\\]]+\\] +1\\.0KiB a\\.bin");
    assert!(out.lines().any(|l| re(l)), "out: {out}");
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
    assert!(v["modTime"].is_null(), "modTime must be literal null: {out}");
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
```
Add helper `start_incomplete_multipart(server, bucket, key)` to `tests/common/mod.rs` (SDK client via harness port/creds, `create_multipart_upload`, tokio mini-runtime — same pattern as the tier-1 marker helper). Add helper `regex_lite(pattern) -> impl Fn(&str)->bool` using the `regex` crate (add `regex = "1"` to `[dependencies]` now; Task 13 uses it at runtime).

- [ ] **Step 2: Run to verify failure** (ls emits `[DIR]`-era format today; summarize/incomplete are refused; rm prints tier-1 prose).

- [ ] **Step 3: Implement.** `messages.rs` structs + `McMessage` impls; convert `ls` (per-object ContentMessage, `--summarize` accumulates and appends SummaryMessage, `--incomplete` lists via `list_multipart_uploads` mapping each upload to a ContentMessage with size = bytes-so-far (sum `list_parts` sizes; acceptable to report 0 if parts listing is empty), `--storage-class` client-side filter with the empty-class exemption from [SEM] §11: objects reporting an empty storage class are NEVER filtered out); `mb`/`rb`/`stat` swap `println!` for `print_msg`; `rm`/`remove_prefix` emit RmMessage per key (dry-run flag threaded). Alias-only `ls` (bucket list) prints ContentMessage per bucket with `filetype:"folder"`, size 0, bucket creation date.

- [ ] **Step 4: Run** new tests then full `cargo test` → PASS. **Step 5: Commit** `feat: mc message shapes for ls/mb/rb/rm/stat with summarize/incomplete/storage-class`.

---

### Task 3: Transfer output — copyMessage / mirrorMessage / accountStat, --quiet semantics, progress bar

**Files:**
- Modify: `client/Cargo.toml` (add `indicatif = "0.17"`)
- Modify: `client/src/messages.rs` (add CopyMessage, MirrorMessage, AccountStat)
- Modify: `client/src/transfer.rs`, `client/src/mirror.rs`, `client/src/main.rs` (put/cp/get call sites)
- Test: `client/tests/e2e_output.rs` (extend)

**Interfaces** ([OUT] §2 cp / §2 mirror / §5 accountStat, §4 quiet, gotcha 3):
```rust
pub(crate) struct CopyMessage { pub source: String, pub target: String, pub size: u64,
    pub total_count: u64, pub total_size: u64 }
// json keys: status,source,target,size,totalCount,totalSize; human: "`{source}` -> `{target}`"
pub(crate) struct MirrorMessage { pub source: String, pub target: String, pub size: u64,
    pub total_count: u64, pub total_size: u64, pub removed: bool }
// removed=false → json eventType "" eventTime ""; human "`{src}` -> `{dst}`"
// removed=true  → human "Removed `{target}`."; json eventType "s3:ObjectRemoved:Delete"
pub(crate) struct AccountStat { pub total: u64, pub transferred: u64, pub duration_ns: u128, pub speed_bps: f64 }
// json: {"status":"success","total":N,"transferred":N,"duration":<RAW NANOSECONDS INT>,"speed":F}
// human (accepted divergence from mc's bordered table, documented): 
//   "Total: {ibytes} | Transferred: {ibytes} | Duration: {secs:.2}s | Speed: {MB/s:.2} MB/s"
pub(crate) struct TransferSession { ... }  // created per cp/put/get/mv/mirror invocation
impl TransferSession {
    pub fn new(label: &str) -> Self;         // decides mode: Bar iff out().stdout_tty && !quiet && !json
    pub fn object_done(&mut self, msg: CopyMessage);  // Bar: tick bar; else: print_msg(msg)
    pub fn add_total(&mut self, bytes: u64);
    pub fn finish(self);                      // Bar: clear, print nothing; else: print_msg(AccountStat)
}
```
mc auto-forces quiet when stdout size is unknowable on non-Windows ([OUT] §1.2); rs3 equivalent: `Bar` mode already requires `stdout_tty`, which covers it.

- [ ] **Step 1: Failing e2e tests**:
```rust
#[test]
fn cp_quiet_and_json_emit_per_object_lines_plus_summary() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/cpout"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"hello").unwrap();
    // --json: one copyMessage line + one accountStat line
    let out = server.rs3_ok(&["--json", "cp", src.to_str().unwrap(), "test/cpout/f.txt"]);
    let lines: Vec<serde_json::Value> = out.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert_eq!(lines.len(), 2, "expected copyMessage + accountStat: {out}");
    assert_eq!(lines[0]["target"], "test/cpout/f.txt");
    assert_eq!(lines[0]["size"], 5);
    assert!(lines[1]["duration"].is_u64() || lines[1]["duration"].is_i64(), "duration must be raw ns int");
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
    assert!(first["target"].as_str().unwrap().ends_with("p/a.txt"), "out: {out}");
}
```
- [ ] **Step 2: Run → FAIL** (current output is tier-1 prose, single lines).
- [ ] **Step 3: Implement**: `TransferSession` in `messages.rs` or `output.rs`; wire `upload_file`/`download_key_to_path` callers (`put`, `get`, `cp` single + recursive via mirror, `mirror` copies and deletes) to route per-object completion through the session; mirror's `Mirrored`/`Removed` prints become MirrorMessages; `cp`'s prints become CopyMessages with `source`/`target` as the user-facing aliased paths (e.g. `local/path` and `test/bucket/key`). Progress bar (indicatif `ProgressBar::new(total_bytes)` with bytes style) only in Bar mode; per-object `inc`.
- [ ] **Step 4: Full suite** — EXPECT tier-1 e2e assertions on old prose (`Removed `, `Mirrored`, `Uploaded`) to need updating: update those assertions to the new mc-shaped strings in the same commit (they are format assertions, not behavior assertions; list every changed assertion in the report). **Step 5: Commit** `feat: mc transfer output (copy/mirror messages, accountStat, quiet, progress bar)`.

---

### Task 4: Time filters --older-than / --newer-than (cp, mirror, rm)

**Files:**
- Create: `client/src/timefilter.rs`
- Modify: `client/src/main.rs` (remove tier-1 refusal guards on cp; add flags to RmArgs + MirrorArgs), `client/src/mirror.rs` (filter source entries), `client/src/urls.rs` (nothing) 
- Test: unit in `timefilter.rs`, e2e in `client/tests/e2e_filters.rs`

**Interfaces** ([SEM] §1 is normative — polarity is the trap):
```rust
pub(crate) fn parse_mc_duration(s: &str) -> Result<chrono::Duration>; // units ns,us,µs,μs,ms,s,m,h,d(24h),w(7d),y(365d); fractional ok; "0" ok; empty=err
pub(crate) fn parse_time_ref(s: &str) -> Result<TimeRef>;  // duration, else absolute date fallback:
//   "%Y.%m.%d", "%Y.%m.%dT%H:%M", "%Y.%m.%dT%H:%M:%S", RFC3339, "%Y-%m-%d %H:%M:%S %Z"-style
pub(crate) fn include_older_than(object_time: DateTime<Utc>, spec: &str) -> Result<bool>; // true iff age >= spec
pub(crate) fn include_newer_than(object_time: DateTime<Utc>, spec: &str) -> Result<bool>; // true iff age <  spec
```

- [ ] **Step 1: Failing unit tests**:
```rust
#[test]
fn duration_grammar() {
    assert_eq!(parse_mc_duration("1d2h30m").unwrap().num_minutes(), 24*60 + 150);
    assert_eq!(parse_mc_duration("1.5d").unwrap().num_hours(), 36);
    assert_eq!(parse_mc_duration("1w").unwrap().num_days(), 7);
    assert_eq!(parse_mc_duration("0").unwrap().num_seconds(), 0);
    assert!(parse_mc_duration("").is_err());
    assert!(parse_mc_duration("5x").is_err());
}
#[test]
fn polarity_matches_mc() {
    let now = chrono::Utc::now();
    let ten_days_old = now - chrono::Duration::days(10);
    let one_hour_old = now - chrono::Duration::hours(1);
    // --older-than 7d includes only objects whose age >= 7d
    assert!(include_older_than(ten_days_old, "7d").unwrap());
    assert!(!include_older_than(one_hour_old, "7d").unwrap());
    // --newer-than 7d includes only objects whose age < 7d
    assert!(include_newer_than(one_hour_old, "7d").unwrap());
    assert!(!include_newer_than(ten_days_old, "7d").unwrap());
}
#[test]
fn absolute_date_fallback() {
    assert!(include_older_than(chrono::Utc::now(), "2099.01.01").is_ok());
}
```
- [ ] **Step 2: FAIL** → **Step 3: Implement** parser (regex-free hand loop: repeatedly read `[0-9.]+` then unit letters; multiply; sum), wire: `cp` filters at source-selection time (single-object cp: filter on the HeadObject/fs mtime; recursive: filter entries), `mirror` filters source entries BEFORE planning ([SEM] §1: applied to SourceContent.Time), `rm --recursive` filters listed objects; a filtered-to-zero operation is success (exit 0) with no output, matching mc. Validate the spec string once up front (bad grammar = fatal before any transfer).
- [ ] **Step 4: e2e** (`e2e_filters.rs`): seed two objects, run `rm -r --force --older-than 1000d` (deletes nothing, exit 0, both still present), then `rm -r --force --newer-than 1000d` (deletes both); `cp -r --newer-than 1000d` copies, `--older-than 1000d` copies nothing. Write, run, PASS. **Step 5: Commit** `feat: mc time filters for cp/mirror/rm`.

---

### Task 5: --attr metadata + put --if-not-exists

**Files:**
- Create: `client/src/attr.rs` (the state-machine parser)
- Modify: `client/src/main.rs` (add `--attr` to PutArgs/CpArgs/MirrorArgs; add `--if-not-exists` to PutArgs), `client/src/transfer.rs` (upload_file + multipart_upload gain `metadata: &BTreeMap<String,String>` and `if_not_exists: bool`)
- Test: unit in `attr.rs`, e2e in `e2e_filters.rs`

**Interfaces** ([SEM] §2 --attr; exact error message):
```rust
pub(crate) fn parse_attrs(s: &str) -> Result<BTreeMap<String, String>>;
// grammar: key1=value1;key2=value2;...  values may be 'single' or "double" quoted (quotes stripped, ; and = allowed inside)
// keys canonicalized to HTTP header case (Cache-Control, X-Amz-Meta-Foo); NO auto X-Amz-Meta- prefixing
// error text exactly: "specified metadata should be of form key1=value1;key2=value2;... and so on"
```
Upload wiring: keys that (case-insensitively) match a known S3 system header (`Cache-Control`, `Content-Type`, `Content-Encoding`, `Content-Disposition`, `Content-Language`, `Expires`) set the corresponding PutObject builder field; everything else goes into `.metadata(k_without_xamzmeta_prefix, v)` (strip a leading `X-Amz-Meta-` if the user supplied it — the SDK re-adds the wire prefix). `--if-not-exists` → `.if_none_match("*")` on PutObject (and on CreateMultipartUpload? No — conditional only supported on single PUT and CompleteMultipartUpload's IfNoneMatch; set `.if_none_match("*")` on `complete_multipart_upload` for the multipart path).

- [ ] **Step 1: Failing unit tests**:
```rust
#[test]
fn attr_parser_quotes_and_canonical_keys() {
    let m = parse_attrs("Cache-Control=\"max-age=90000,min-fresh=9000\";key1=value1").unwrap();
    assert_eq!(m["Cache-Control"], "max-age=90000,min-fresh=9000");
    assert_eq!(m["Key1"], "value1"); // canonical header case
    let m = parse_attrs("a='v;with;semis'").unwrap();
    assert_eq!(m["A"], "v;with;semis");
    assert!(parse_attrs("noequals").is_err());
    assert!(parse_attrs("a=\"unterminated").is_err());
    assert!(parse_attrs(";=x").is_err());
}
```
- [ ] **Step 2: FAIL** → **Step 3: Implement** parser (char loop with Key/Value token state + Normal/Quote('\'')/Quote('"') mode; first `=` in a segment switches to value, later `=` literal; `;` outside quotes commits) + wiring.
- [ ] **Step 4: e2e**: `put --attr "X-Amz-Meta-Color=red;Cache-Control=no-store"` then `stat --json` asserts `metadata` contains `Color: red` (or the exact key shape rs3's stat reports — assert on the stat JSON's metadata map) and cache-control surfaces; `put --if-not-exists` twice on the same key: second invocation exits non-zero. Run, PASS. **Step 5: Commit** `feat: --attr metadata and put --if-not-exists`.

---

### Task 6: --preserve (filesystem attrs)

**Files:**
- Modify: `client/src/attr.rs` (encode/decode), `client/src/transfer.rs` (upload: attach `X-Amz-Meta-Mc-Attrs`; download: apply), `client/src/main.rs` (add `-a/--preserve` to PutArgs/CpArgs/MirrorArgs/GetArgs? — mc has it on cp/mirror/put; NOT get)
- Test: unit + e2e in `e2e_filters.rs`

**Interfaces** ([SEM] §2 --preserve; encoding verbatim):
```rust
pub(crate) fn encode_fs_attrs(meta: &std::fs::Metadata) -> String;
// "atime:<sec>#<nsec>/gid:<gid>/mode:<st_mode-as-decimal>/mtime:<sec>#<nsec>/uid:<uid>"
// (gname/uname omitted — mc omits them when lookup fails; rs3 omits always, read-compatible)
pub(crate) fn apply_fs_attrs(path: &Path, encoded: &str) -> Result<()>;
// parse "/"-separated key:value; apply mode (chmod, parse as u32 with 0-prefix octal accepted via
// u32::from_str_radix fallback — mc uses ParseUint(val, 0, 32) i.e. base auto-detect), mtime+atime
// (filetime-free: std::fs::File times via libc utimensat? use the `filetime` crate? NO new deps —
// use std::fs::FileTimes (stable since 1.75): File::options().write(true).open(path).set_times(...)),
// uid/gid: apply via std::os::unix::fs::chown (stable since 1.73); ignore failures (mc defaults -1)
```
Metadata key: `X-Amz-Meta-Mc-Attrs` — SDK `.metadata("Mc-Attrs", encoded)`. Download side (cp/mirror S3→local with `--preserve`): after rename, read the object's `Mc-Attrs`/`mc-attrs` user metadata from the HeadObject already performed in `download_key_to_path` (thread the metadata map out) and `apply_fs_attrs`. S3→S3 copies with `--preserve`: server-side CopyObject already preserves metadata (COPY directive default); streaming path must copy the source's user metadata onto the target PUT ([SEM] §2 last paragraph). Windows: not a target platform for rs3 tests; guard the unix-only calls with `#[cfg(unix)]` and hard-error `--preserve` on non-unix.

- [ ] **Step 1: Failing unit test**: `encode_fs_attrs` on a temp file returns a string containing `mode:` and `mtime:`; roundtrip `apply_fs_attrs` onto a second temp file sets its mode to 0o600 when encoded from a 0o600 file (assert via `metadata.permissions().mode() & 0o777`).
- [ ] **Step 2: FAIL** → **Step 3: Implement** → **Step 4: e2e**: `chmod 600` a source file, `cp --preserve` to S3, `cp --preserve` back to a new local name, assert mode 600 and mtime equality (to the second). **Step 5: Commit** `feat: --preserve filesystem attributes via X-Amz-Meta-Mc-Attrs`.

---

### Task 7: mv

**Files:**
- Modify: `client/src/main.rs` (new `Mv(CpArgs)` variant — mc's mv flags are a strict subset of cp's; reuse CpArgs, hard-erroring the cp-only extras is unnecessary since CpArgs has none of the excluded ones)
- Test: `client/tests/e2e_mv.rs`

**Semantics** ([SEM] §3, [OUT] gotcha 8): mv = cp, then per-object delete only after that object's copy succeeded. Non-transactional; delete failures are logged (stderr) but do NOT affect exit status; copy failures do (exit 1) and leave the source object in place. Output: **CopyMessage, identical to cp** (no move discriminator). Guards: source dir without `-r` errors (existing cp behavior); `mv` with exactly 2 args where one URL is a path-prefix of the other → fatal `The source ... and destination ... cannot be subdirectories of each other`.

- [ ] **Step 1: Failing e2e tests**:
```rust
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
    assert!(!server.rs3(&["stat", "test/mv1/a.txt"]).status.success(), "source must be gone");
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
    assert!(!dir.join("sub/x.txt").exists(), "local source file must be deleted");
}
#[test]
fn mv_subdirectory_guard() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/mv3"]);
    let out = server.rs3(&["mv", "-r", "test/mv3/p", "test/mv3/p/sub"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("subdirectories of each other"));
}
```
- [ ] **Step 2: FAIL** (`mv` unknown subcommand) → **Step 3: Implement**: route through the cp machinery with an `is_mv` flag threaded to the per-object completion callback (S3 source: `delete_object`; local source: `fs::remove_file`; after recursive local moves, prune now-empty dirs best-effort). The subdirectory guard: applies when both are S3 URLs (prefix-of-each-other on alias/bucket/key string with `/` boundary) or both local (Path::starts_with either way).
- [ ] **Step 4: PASS** full suite. **Step 5: Commit** `feat: mv as cp + per-object delete-after-success`.

---

### Task 8: head

**Files:**
- Modify: `client/src/main.rs` (new HeadArgs: `-n/--lines` i64 default 10; positional targets)
- Test: `client/tests/e2e_small_cmds.rs` (new file, shared by head/du/tree/cat gaps)

**Semantics** ([SEM] §4): full un-ranged GET, read first N lines client-side, stop early, drop the stream. Each emitted line printed with `\n` (normalizes CRLF). Content-Type containing `gzip` → decompress via flate2 before line-splitting (`bzip` unsupported: hard-error `head: bzip2-compressed objects are not supported yet`). `--rewind/--version-id/--zip` → refused. No message structs — raw bytes to stdout ([OUT] gotcha 14: `--json` has no effect on payload).

- [ ] **Step 1: Failing e2e**:
```rust
#[test]
fn head_prints_first_n_lines() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/headb"]);
    let src = server.dir.path().join("lines.txt");
    std::fs::write(&src, (1..=50).map(|i| format!("line{i}\n")).collect::<String>()).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/headb/lines.txt"]);
    let out = server.rs3_ok(&["head", "test/headb/lines.txt"]);
    assert_eq!(out.lines().count(), 10, "default -n 10: {out}");
    assert_eq!(out.lines().next().unwrap(), "line1");
    let out = server.rs3_ok(&["head", "-n", "3", "test/headb/lines.txt"]);
    assert_eq!(out.lines().collect::<Vec<_>>(), vec!["line1", "line2", "line3"]);
}
```
- [ ] **Step 2: FAIL** → **Step 3: Implement** (tokio `BufReader::read_until(b'\n')` loop over `resp.body.into_async_read()`, strip trailing `\r?\n`, print + `\n`, stop at N; gzip: buffer through `flate2::read::MultiGzDecoder` via `SyncIoBridge`? — simpler: read the whole compressed stream into the decoder incrementally with a spawn_blocking bridge; objects are small for head use; document). Add `flate2 = "1"` dep. **Step 4: PASS** → **Step 5: Commit** `feat: head with client-side line limit and gzip decode`.

---

### Task 9: cat/stat gap flags — --offset, --tail, --part-number; stat --recursive

**Files:**
- Modify: `client/src/main.rs` (CatArgs gains `part_number: Option<i32>`; remove refusals; stat recursive)
- Test: `client/tests/e2e_small_cmds.rs`

**Semantics** ([SEM] §12, §13): `--offset N` → `Range: bytes=N-`; `--tail N` → Stat first, `RangeStart = max(size - N, 0)` (tail > size = whole object); mutual exclusions verbatim: tail+offset fatal, negative fatal, part-number with tail/offset fatal. `--part-number` → `.part_number(n)` on GetObject. `stat --recursive` → walk the prefix (collect_objects) and print a full StatMessage per object (HeadObject each for metadata/etag parity with single-stat output).

- [ ] **Step 1: Failing e2e**:
```rust
#[test]
fn cat_offset_and_tail() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/catb"]);
    let src = server.dir.path().join("abc.txt");
    std::fs::write(&src, b"0123456789").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/catb/abc.txt"]);
    assert_eq!(server.rs3_ok(&["cat", "--offset", "7", "test/catb/abc.txt"]), "789");
    assert_eq!(server.rs3_ok(&["cat", "--tail", "3", "test/catb/abc.txt"]), "789");
    assert_eq!(server.rs3_ok(&["cat", "--tail", "99", "test/catb/abc.txt"]), "0123456789");
    let out = server.rs3(&["cat", "--tail", "1", "--offset", "1", "test/catb/abc.txt"]);
    assert!(!out.status.success());
}
#[test]
fn stat_recursive_walks_prefix() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/statr"]);
    let src = server.dir.path().join("f.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/statr/p/a.txt"]);
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/statr/p/b.txt"]);
    let out = server.rs3_ok(&["stat", "-r", "test/statr/p/"]);
    assert!(out.contains("a.txt") && out.contains("b.txt"), "out: {out}");
}
```
- [ ] **Step 2: FAIL** → **Step 3: Implement** → **Step 4: PASS** → **Step 5: Commit** `feat: cat --offset/--tail/--part-number, stat --recursive`.

---

### Task 10: du

**Files:**
- Modify: `client/src/main.rs` (new DuArgs: `-d/--depth` i32 default 0, `-r/--recursive`; targets)
- Modify: `client/src/messages.rs` (DuMessage)
- Test: `client/tests/e2e_small_cmds.rs`

**Semantics** ([SEM] §5 verbatim — the 3-way branch): effective depth = `-d N` if set; else `-1` (unlimited) if `-r`; else `1`. Recursion: depth==1 → one flat recursive listing summed to ONE line; depth!=1 → non-recursive listing, recurse into each dir with depth-1 (−1 stays −1), print one DuMessage per prefix visited EXCEPT when the countdown hits exactly 0 (aggregate silently into parent). DuMessage: json `{"prefix":...,"size":N,"objects":N,"status":"success","isVersions":false}`; human `"{ibytes}\t{N} object(s)\t{prefix}"` (singular `object` when N==1).

- [ ] **Step 1: Failing e2e**:
```rust
#[test]
fn du_depth_semantics() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/dub"]);
    let src = server.dir.path().join("k.bin");
    std::fs::write(&src, vec![0u8; 1024]).unwrap();
    for key in ["a/1.bin", "a/sub/2.bin", "b/3.bin"] {
        server.rs3_ok(&["put", src.to_str().unwrap(), &format!("test/dub/{key}")]);
    }
    // plain du => depth 1 => single total line for the target
    let out = server.rs3_ok(&["du", "test/dub"]);
    assert_eq!(out.lines().count(), 1, "plain du = one line: {out}");
    assert!(out.contains("3.0KiB") && out.contains("3 objects"), "out: {out}");
    // du -r => unlimited => one line per directory level + total (a/sub, a, b, root = 4 lines)
    let out = server.rs3_ok(&["du", "-r", "test/dub"]);
    assert_eq!(out.lines().count(), 4, "out: {out}");
    assert!(out.lines().any(|l| l.ends_with("a/sub") && l.contains("1 object")), "out: {out}");
    // du -d 1: children aggregated silently, single top line
    let json = server.rs3_ok(&["--json", "du", "test/dub"]);
    let v: serde_json::Value = serde_json::from_str(json.lines().last().unwrap()).unwrap();
    assert_eq!(v["objects"], 3);
    assert_eq!(v["size"], 3072);
}
```
- [ ] **Step 2: FAIL** → **Step 3: Implement** (recursive async fn `du_walk(client, bucket, prefix, depth) -> (size, objects)` mirroring [SEM] §5's structure: `recursive = depth == 1` flat-list branch; else delimiter-list, recurse dirs at depth-1, count files at this level into own total, `if depth != 0 { print }`). Folder markers excluded from object counts (paginator already skips). **Step 4: PASS** → **Step 5: Commit** `feat: du with mc depth semantics`.

---

### Task 11: tree

**Files:**
- Modify: `client/src/main.rs` (TreeArgs: `-f/--files`, `-d/--depth` i32 default -1)
- Test: `client/tests/e2e_small_cmds.rs`

**Semantics** ([SEM] §6, [OUT] §2 tree + gotcha 9): glyphs `├─ `, `└─ `, `│`, two-space level pad. Depth validation: `0` or `< -1` → fatal (`please set a proper depth...`). Root line printed first (the target itself). Directories always shown; files only with `--files`. Descend while `depth == -1 || level <= depth` (dirs AT the boundary level are shown but not descended). `tree --json` = alias to the `ls --recursive --json` code path (ContentMessage lines, no tree shape).

- [ ] **Step 1: Failing e2e**:
```rust
#[test]
fn tree_draws_branches_and_json_aliases_ls() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/treeb"]);
    let src = server.dir.path().join("t.txt");
    std::fs::write(&src, b"x").unwrap();
    for key in ["a/1.txt", "a/sub/2.txt", "b/3.txt"] {
        server.rs3_ok(&["put", src.to_str().unwrap(), &format!("test/treeb/{key}")]);
    }
    let out = server.rs3_ok(&["tree", "test/treeb"]);
    assert!(out.contains("├─ a") || out.contains("├─ a/"), "out: {out}");
    assert!(out.contains("└─ "), "out: {out}");
    assert!(!out.contains("1.txt"), "files hidden without --files: {out}");
    let out = server.rs3_ok(&["tree", "--files", "test/treeb"]);
    assert!(out.contains("1.txt"), "out: {out}");
    let out = server.rs3(&["tree", "--depth", "0", "test/treeb"]);
    assert!(!out.status.success(), "depth 0 must be rejected");
    let out = server.rs3_ok(&["--json", "tree", "test/treeb"]);
    let first: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
    assert!(first.get("key").is_some(), "tree --json = ls -r --json: {out}");
}
```
- [ ] **Step 2: FAIL** → **Step 3: Implement** (recursive delimiter listing; per-directory child list buffered to know which is last; branch prefix built per [SEM] §6: continuation columns `│  ` for open ancestors, `   ` for closed). **Step 4: PASS** → **Step 5: Commit** `feat: tree with mc branch drawing; --json aliases ls -r`.

---

### Task 12: pipe

**Files:**
- Modify: `client/src/main.rs` (PipeArgs: `--storage-class/-sc`, `--attr`, `--part-size` default `16MiB`, `--concurrent` default 1; optional TARGET), `client/src/transfer.rs` (new `upload_stream`)
- Test: `client/tests/e2e_small_cmds.rs`

**Semantics** ([SEM] §8): unknown total size; default part size **16MiB** (pipe-specific — NOT rs3's 256MiB); no target = passthrough stdin→stdout. Implementation: read stdin into `part_size` buffers; if EOF before the first buffer fills → single PutObject; else multipart with sequential part upload (respect `--concurrent N` by keeping up to N in-flight part uploads, each holding its buffer — warn nothing, mc's memory caveat is the user's problem). PipeMessage json `{"status":"success","target":...,"size":N}`, human `"{size} bytes -> `{target}`"` — printed once at the end (plus AccountStat session summary consistent with Task 3's transfer session? mc pipes through the same accounting — YES: wrap in TransferSession).

- [ ] **Step 1: Failing e2e**:
```rust
#[test]
fn pipe_uploads_stdin() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/pipeb"]);
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rs3"));
    cmd.args(["pipe", "test/pipeb/from-stdin.txt"])
        .env("MC_HOST_TEST", format!("http://testkey:testsecret@127.0.0.1:{}", server.port))
        .env("MC_CONFIG_DIR", server.dir.path().join("mc-config"))
        .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    use std::io::Write;
    child.stdin.take().unwrap().write_all(b"streamed bytes").unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let dst = server.dir.path().join("back.txt");
    server.rs3_ok(&["get", "test/pipeb/from-stdin.txt", dst.to_str().unwrap()]);
    assert_eq!(std::fs::read(dst).unwrap(), b"streamed bytes");
}
```
Also a multipart case: pipe 12MiB of patterned bytes with `--part-size 5MiB`, get back, byte-compare, and `stat` ETag contains `-3`.
- [ ] **Step 2: FAIL** → **Step 3: Implement** `upload_stream(client, reader, bucket, key, part_size, concurrent, attrs, storage_class)` in transfer.rs. **Step 4: PASS** → **Step 5: Commit** `feat: pipe streams stdin to object with unknown-size multipart`.

---

### Task 13: diff

**Files:**
- Modify: `client/src/main.rs` (DiffArgs: exactly two dir-like targets, no own flags), `client/src/messages.rs` (DiffMessage), `client/src/mirror.rs` (expose entry collection for reuse)
- Test: `client/tests/e2e_diff.rs`

**Semantics** ([OUT] §2 diff, [SEM] §9): compare name+size+type+mtime-heuristic, never content. JSON `{"status":"success","first":...,"second":...,"diff":<RAW INT>}` with ints: size=2, type=4, only-in-first=5, only-in-second=6, aa-mtime=7 ([OUT] gotcha 6). Human markers: `< {firstURL}` (only-in-first), `> {secondURL}` (only-in-second), `! {secondURL}` (size/type/mtime). Equal pairs print nothing. URLs are the user-facing aliased paths (`test/bkt1/p/x.txt`). Local dirs allowed as either side (reuse mirror's `collect_local_entries`/`collect_s3_entries`). Exit code 0 even when differences exist (mc: diff output isn't an error) — verify in mc source if in doubt; differences are normal output.

- [ ] **Step 1: Failing e2e**:
```rust
mod common;
use common::TestServer;

#[test]
fn diff_markers_and_json_int() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/da"]);
    server.rs3_ok(&["mb", "test/db"]);
    let f = |name: &str, content: &[u8]| {
        let p = server.dir.path().join(name);
        std::fs::write(&p, content).unwrap();
        p
    };
    let same = f("same.txt", b"same");
    let sized = f("sized.txt", b"12345");
    let sized2 = f("sized2.txt", b"123456789");
    let only1 = f("only1.txt", b"1");
    server.rs3_ok(&["put", same.to_str().unwrap(), "test/da/same.txt"]);
    server.rs3_ok(&["put", same.to_str().unwrap(), "test/db/same.txt"]);
    server.rs3_ok(&["put", sized.to_str().unwrap(), "test/da/sized.txt"]);
    server.rs3_ok(&["put", sized2.to_str().unwrap(), "test/db/sized.txt"]);
    server.rs3_ok(&["put", only1.to_str().unwrap(), "test/da/only1.txt"]);
    let out = server.rs3_ok(&["diff", "test/da", "test/db"]);
    assert!(out.contains("< test/da/only1.txt"), "out: {out}");
    assert!(out.contains("! test/db/sized.txt"), "out: {out}");
    assert!(!out.contains("same.txt"), "equal objects print nothing: {out}");
    let out = server.rs3_ok(&["--json", "diff", "test/da", "test/db"]);
    let diffs: Vec<serde_json::Value> = out.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    let only = diffs.iter().find(|d| d["first"].as_str().unwrap().contains("only1")).unwrap();
    assert_eq!(only["diff"], 5, "only-in-first is raw int 5");
    let size = diffs.iter().find(|d| d["second"].as_str().unwrap().contains("sized")).unwrap();
    assert_eq!(size["diff"], 2, "size diff is raw int 2");
}
```
- [ ] **Step 2: FAIL** → **Step 3: Implement** (sorted merge-join over the two entry lists; per-pair: size≠ → 2; source strictly newer mtime → 7? — NO: plain diff without metadata mode only emits size/type/only-in; the mtime heuristic `differInAASourceMTime` fires when source `.Time.After(target)` — include it per [SEM] §9 step 3; local-side type mismatches are impossible in rs3's entry model, skip type=4). **Step 4: PASS** → **Step 5: Commit** `feat: diff with mc markers and raw-int json codes`.

---

### Task 14: find

**Files:**
- Create: `client/src/findcmd.rs`
- Modify: `client/src/main.rs` (FindArgs: `--exec`, `--ignore`, `--name`, `--newer-than`, `--older-than`, `--path`, `--print`, `--regex`, `--larger`, `--smaller`, `--maxdepth` u32; refuse `--watch/--metadata/--tags`)
- Test: unit in `findcmd.rs` (matchers, tokens) + `client/tests/e2e_find.rs`

**Semantics** ([SEM] §7 verbatim — three traps: name-fallback, flat wildcard, maxdepth-truncates-print-only):
- All filters AND'ed against the **prefix-relative path**.
- `--name`: glob (`*`,`?`,`[...]`) against basename via a small glob matcher (implement `glob_match(pattern, name)` by hand — `*` does not cross `/` but basenames have no `/`; standard recursive matcher); fallback: exact string equality against ANY path component.
- `--path`: flat wildcard match (`*` crosses `/`) against full relative path; `--ignore`: same matcher, inverted, evaluated first.
- `--regex`: `regex` crate against full relative path (RE2-compatible syntax).
- `--larger`/`--smaller`: strict `>` / `<`; parse sizes with BOTH metric (k/m/g/t = 1000-based) and IEC (ki/mi/gi/ti = 1024-based) units, case-insensitive, optional trailing `b`.
- `--older-than`/`--newer-than`: Task 4's `include_*` functions (positive-match polarity: older-than matches age ≥ spec).
- `--maxdepth N`: truncate the DISPLAYED path to N components below the search root; does NOT filter or stop matching.
- Actions: `--exec` wins over `--print`; default prints FindMessage (bare aliased key; JSON = ContentMessage exactly). Token substitution in exec/print strings, applied in this order: `{}`→full aliased key, `{""}`→quoted, `{base}`, `{"base"}`, `{dir}`, `{"dir"}`, `{size}` (humanize_ibytes), `{"size"}`, `{time}` (print_date), `{"time"}` (quoted = Rust `{:?}`-style double-quote escaping — use `serde_json::to_string(&s)`). `{url}`/`{version}` → hard-error (`find {url} is not implemented yet` — presign lands in Task 15; wire it there if time allows, else leave refused).
- `--exec`: tokenize the substituted string with `shell-words`, run via `std::process::Command` (no shell); on child failure print its stderr and exit rs3 with the child's exit code immediately (abort the loop).

- [ ] **Step 1: Failing unit tests**:
```rust
#[test]
fn name_glob_and_component_fallback() {
    assert!(name_match("*.txt", "a/b/note.txt"));
    assert!(!name_match("*.txt", "a/b/note.rs"));
    assert!(name_match("foo", "a/foo/bar.rs"), "component exact-match fallback");
    assert!(!name_match("f*o", "a/foo/bar.rs"), "fallback is exact, not glob");
}
#[test]
fn path_wildcard_is_flat() {
    assert!(path_match("a/*.txt", "a/b/c.txt"), "* crosses / in mc wildcard");
    assert!(!path_match("z*", "a/b/c.txt"));
}
#[test]
fn size_grammar_metric_and_iec() {
    assert_eq!(parse_find_size("1k").unwrap(), 1000);
    assert_eq!(parse_find_size("1ki").unwrap(), 1024);
    assert_eq!(parse_find_size("5MB").unwrap(), 5_000_000);
    assert_eq!(parse_find_size("5MiB").unwrap(), 5 * 1024 * 1024);
    assert_eq!(parse_find_size("64").unwrap(), 64);
}
#[test]
fn token_substitution() {
    let s = substitute_tokens("echo {} {base} {\"size\"}", "test/b/a/f.txt", "a/f.txt", 1024, sample_time());
    assert_eq!(s, "echo test/b/a/f.txt f.txt \"1.0KiB\"");
}
```
- [ ] **Step 2: FAIL** → **Step 3: Implement** matchers + `run_find` (recursive listing via collect_objects; apply ignore→name→path→regex→larger→smaller→older→newer; act per match). **Step 4: e2e** (`e2e_find.rs`): seed a small tree; `find test/bkt --name '*.log'` prints only logs; `find --larger 1ki` excludes a 100-byte object; `find --exec "false"` exits non-zero on first match; `find --print '{base} {size}'` renders. Write→FAIL→wire→PASS. **Step 5: Commit** `feat: find with mc matchers, tokens, and exec`.

---

### Task 15: share download / upload / list

**Files:**
- Create: `client/src/share.rs`
- Modify: `client/src/main.rs` (Share subcommand: `download`/`upload`/`list`), `client/src/messages.rs` (ShareMessage)
- Test: `client/tests/e2e_share.rs`

**Semantics** ([SEM] §10, [OUT] §2 share): `--expire/-E` default `168h`, parsed with GO-native units ONLY (h/m/s/ms/us/ns — implement `parse_go_duration`; `7d` is INVALID), clamped fatal outside [1s, 604800s]. ShareMessage json: `{"status":"success","url":<object url>,"share":<presigned url>,"timeLeft":<RAW NANOSECONDS INT>}` (+`"contentType"` for upload when set); human: `URL: {objectURL}\nExpire: {humanized}\nShare: {shareURL}\n` (+`Content-Type:` line for upload). Un-escape `&`,`<`,`>` in JSON (serde_json doesn't HTML-escape — nothing to do; note in report).
- `download`: presigned GET via `aws_sdk_s3::presigning::PresigningConfig::expires_in(dur)` + `client.get_object().presigned(...)`; `--recursive` iterates objects under the prefix, one message each.
- `upload`: browser-POST policy signed client-side — build the policy JSON (expiration, conditions: bucket, key or key starts-with for `--recursive`, optional content-type), SigV4-sign it (derive signing key exactly as rusts3's browser POST expects: date/region/s3/aws4_request HMAC chain; fields: `x-amz-algorithm=AWS4-HMAC-SHA256`, `x-amz-credential`, `x-amz-date`, `policy` (base64), `x-amz-signature`), then render the curl template verbatim from [SEM] §10: `curl {postURL} -F {field}={value} ... -F key={key}<NAME-if-recursive> -F file=@<FILE>` with literal `<FILE>`/`<NAME>` placeholders.
- `list upload|download`: local JSON DB at `{config_dir}/share/uploads.json`/`downloads.json` (config_dir from `config_path()`'s parent); entries `{url, share, date, expiry}`; `timeLeft = expiry - (now - date)`, expired entries still listed.

- [ ] **Step 1: Failing e2e**:
```rust
mod common;
use common::TestServer;

#[test]
fn share_download_url_works() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/shb"]);
    let src = server.dir.path().join("s.txt");
    std::fs::write(&src, b"shared!").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/shb/s.txt"]);
    let out = server.rs3_ok(&["--json", "share", "download", "test/shb/s.txt"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    let url = v["share"].as_str().unwrap();
    assert!(url.contains("X-Amz-Signature"), "presigned: {url}");
    assert!(v["timeLeft"].as_u64().unwrap() > 0, "raw ns int");
    // the presigned URL must actually download without credentials
    let body = std::process::Command::new("curl").args(["-sf", url]).output().unwrap();
    assert!(body.status.success(), "curl failed on presigned url");
    assert_eq!(body.stdout, b"shared!");
    // share list remembers it
    let out = server.rs3_ok(&["share", "list", "download"]);
    assert!(out.contains("s.txt"), "out: {out}");
}
#[test]
fn share_expire_rules() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/shc"]);
    let src = server.dir.path().join("s.txt");
    std::fs::write(&src, b"x").unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/shc/s.txt"]);
    assert!(!server.rs3(&["share", "download", "--expire", "7d", "test/shc/s.txt"]).status.success(), "7d invalid (Go units only)");
    assert!(!server.rs3(&["share", "download", "--expire", "200h", "test/shc/s.txt"]).status.success(), "over 7-day cap");
    server.rs3_ok(&["share", "download", "--expire", "30m", "test/shc/s.txt"]);
}
#[test]
fn share_upload_curl_template() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/shd"]);
    let out = server.rs3_ok(&["share", "upload", "test/shd/up.bin"]);
    assert!(out.contains("curl "), "out: {out}");
    assert!(out.contains("-F file=@<FILE>"), "literal <FILE> placeholder: {out}");
    assert!(out.contains("x-amz-signature") || out.contains("X-Amz-Signature"), "out: {out}");
}
```
(If rusts3 rejects the generated POST policy in manual verification, debug the policy fields against `src/server/auth.rs`'s browser-POST verifier — read-only — before touching the template.)
- [ ] **Step 2: FAIL** → **Step 3: Implement** (`parse_go_duration`: number+unit pairs, units ns/us/µs/ms/s/m/h only). Presign via SDK for download; hand-rolled policy for upload (hmac-sha256 via the `aws-sigv4`? — the SDK stack already vendors `hmac`/`sha2` transitively; add explicit `hmac = "0.12"`, `sha2 = "0.10"`, `base64 = "0.22"` deps — allowed as part of this task, note in report). **Step 4: PASS** → **Step 5: Commit** `feat: share download/upload/list with presigned GET and POST policy`.

---

### Task 16: Final sweep — refusal-list cleanup, README, full verification

**Files:**
- Modify: `client/src/main.rs` (drop refusal guards that Tasks 2-15 implemented; keep/add refusals for: all versioning flags, `--zip`, `--md5`, `--checksum`, `find --watch/--metadata/--tags`, `mirror --watch`, `head --rewind/--version-id/--zip`)
- Modify: `client/README.md`
- Modify: `client/tests/e2e_refuse.rs` (update the refusal matrix to the new reality)
- Test: everything

- [ ] **Step 1**: Update `e2e_refuse.rs`: remove now-implemented cases (ls summarize/incomplete/sc, cat offset/tail, cp older/newer-than, stat -r), add new refusal cases (`find --watch x`, `head --rewind 7d x`, `cp --md5 a b`). Run: the removed cases' commands now succeed (covered by their feature tests); the new cases fail correctly.
- [ ] **Step 2**: README rewrite of the Implemented/Not-implemented sections: full command table (now 19 commands), output-compatibility section (--json JSON-lines contract, --quiet semantics matching mc's "no bar, per-object lines + summary", error format `rs3: <ERROR>`, exit codes 0/1), documented divergences (accountStat human line vs mc's table; date zone rendered as offset; head bzip2; error-prefix uses `rs3` not `mc`; `{url}`/`{version}` find tokens if still refused).
- [ ] **Step 3**: Full verification: `cargo fmt && cargo build --release && cargo test`. Fix anything red.
- [ ] **Step 4**: Commit `feat: tier-2 refusal cleanup and mc-compat documentation`.

---

## Self-Review Notes

- **Coverage vs goal**: output contract (Tasks 1-3), filters/metadata (4-6), new commands (7-15), cleanup (16). Deliberately excluded, consistent with "ignore MinIO-specific": ping/ready, find --metadata/--tags/--watch, versioning flags, --zip, --md5/--checksum, mirror --watch. Deferred beyond tier 2 (documented in Task 16's README step): mc's `--exclude` on mirror, `--retry/--skip-errors/--summary/--max-workers`, byte-exact accountStat table, find `{url}` token if not folded into Task 15.
- **Type consistency**: `McMessage`/`print_msg`/`render_json`/`humanize_ibytes`/`print_date` (Task 1) consumed by Tasks 2,3,10,13,14,15; `include_older_than`/`include_newer_than` (Task 4) consumed by 14; `parse_attrs` (5) by 12; `collect_local_entries`/`collect_s3_entries` (tier-1 mirror.rs) by 13; ContentMessage (2) by 11 (tree --json) and 14 (find JSON).
- **Test-format honesty**: e2e asserts compact JSON because pipes are never TTYs; TTY-dependent behavior (pretty indent, progress bar) is unit-tested or accepted as manually-verified; human-date assertions use regexes to avoid timezone-name divergence.
- **Known plan risks**: Task 3 rewrites output of commands that tier-1 e2e tests assert on — the task explicitly budgets updating those assertions; Task 15's POST policy may need iteration against rusts3's verifier (debug path named); indicatif bar behavior in non-TTY test environments is Bar-mode-off by construction.
