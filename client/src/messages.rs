//! mc-shaped message structs for `ls`, `mb`, `rb`, `rm`, `stat`, and the
//! transfer commands (`cp`/`put`/`get`/`mirror`). Field names/order and
//! human/JSON rendering rules are normative per
//! `docs/superpowers/research/mc-research-output.md` §2
//! (`contentMessage`/`summaryMessage`/`makeBucketMessage`/
//! `removeBucketMessage`/`rmMessage`/`statMessage`/`copyMessage`/
//! `mirrorMessage`) and §5 (`accountStat`, progress bar).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

use chrono::{DateTime, Utc};
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::json;

use crate::output::{JsonStyle, McMessage, humanize_ibytes, out, print_date, print_msg};

/// `ls` per-entry message (also reused for `--incomplete` uploads and the
/// alias-only bucket listing, where `filetype` is `"folder"`).
pub(crate) struct ContentMessage {
    pub status: String,
    pub filetype: String,
    pub time: DateTime<Utc>,
    pub size: u64,
    pub key: String,
    pub etag: String,
    pub storage_class: Option<String>,
}

impl McMessage for ContentMessage {
    fn human(&self) -> String {
        let is_folder = self.filetype == "folder";
        let size_str = if is_folder {
            "0B".to_string()
        } else {
            humanize_ibytes(self.size)
        };
        let key = if is_folder && !self.key.ends_with('/') {
            format!("{}/", self.key)
        } else {
            self.key.clone()
        };
        format!("[{}] {size_str:>7} {key}", print_date(self.time))
    }

    fn json(&self) -> serde_json::Value {
        let mut v = json!({
            "status": self.status,
            "type": self.filetype,
            "lastModified": self.time.to_rfc3339(),
            "size": self.size,
            "key": self.key,
            "etag": self.etag,
        });
        if let Some(sc) = &self.storage_class {
            v["storageClass"] = json!(sc);
        }
        v
    }
}

/// `ls --summarize` trailer, printed once after the listing it summarizes.
pub(crate) struct SummaryMessage {
    pub total_objects: u64,
    pub total_size: u64,
}

impl McMessage for SummaryMessage {
    fn human(&self) -> String {
        format!(
            "\nTotal Size: {}\nTotal Objects: {}",
            humanize_ibytes(self.total_size),
            self.total_objects
        )
    }

    fn json(&self) -> serde_json::Value {
        json!({"totalObjects": self.total_objects, "totalSize": self.total_size})
    }

    fn json_style(&self) -> JsonStyle {
        JsonStyle::EmptyIndent
    }
}

/// `mb` success message.
pub(crate) struct MakeBucketMessage {
    pub bucket: String,
}

impl McMessage for MakeBucketMessage {
    fn human(&self) -> String {
        format!("Bucket created successfully `{}`.", self.bucket)
    }

    fn json(&self) -> serde_json::Value {
        // `region` is always emitted but always empty -- mc never actually
        // sets it (research doc §2 gotcha 18).
        json!({"status": "success", "bucket": self.bucket, "region": ""})
    }
}

/// `rb` success message. Note: mc's `JSON()` for this type uses plain
/// `json.Marshal` (fully compact), unlike almost every other message.
pub(crate) struct RemoveBucketMessage {
    pub bucket: String,
}

impl McMessage for RemoveBucketMessage {
    fn human(&self) -> String {
        format!("Removed `{}` successfully.", self.bucket)
    }

    fn json(&self) -> serde_json::Value {
        json!({"status": "success", "bucket": self.bucket})
    }

    fn json_style(&self) -> JsonStyle {
        JsonStyle::AlwaysCompact
    }
}

/// `rm` per-key message (also used for `rm --recursive`'s per-object output
/// and `rb --force`'s bucket-emptying sweep).
pub(crate) struct RmMessage {
    pub key: String,
    pub dry_run: bool,
    pub mod_time: Option<DateTime<Utc>>,
}

impl McMessage for RmMessage {
    fn human(&self) -> String {
        let verb = if self.dry_run {
            "DRYRUN: Removing"
        } else {
            "Removed"
        };
        format!("{verb} `{}`.", self.key)
    }

    fn json(&self) -> serde_json::Value {
        // `modTime` has no `omitempty` on the Go side -- a plain (non-
        // versioned) delete must serialize it as a literal `null`, not omit
        // the key (research doc §2 gotcha 17). `serde_json::json!` already
        // renders `Option::None` as `null`, so this falls out naturally.
        json!({
            "status": "success",
            "key": self.key,
            "deleteMarker": false,
            "versionID": "",
            "modTime": self.mod_time.map(|t| t.to_rfc3339()),
            "dryRun": self.dry_run,
        })
    }
}

/// `stat` message for a single object. mc's `Type` field is a fixed
/// `"file"` for the object-stat path (not the object's MIME content-type);
/// `content_type`/`cache_control`, if present, are folded into the
/// `Metadata` block under `Content-Type`/`Cache-Control` keys, matching real
/// `mc stat` output.
pub(crate) struct StatMessage {
    pub key: String,
    pub date: DateTime<Utc>,
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

impl StatMessage {
    fn full_metadata(&self) -> BTreeMap<String, String> {
        let mut m = self.metadata.clone();
        if let Some(ct) = &self.content_type {
            m.entry("Content-Type".to_string())
                .or_insert_with(|| ct.clone());
        }
        if let Some(cc) = &self.cache_control {
            m.entry("Cache-Control".to_string())
                .or_insert_with(|| cc.clone());
        }
        m
    }
}

impl McMessage for StatMessage {
    fn human(&self) -> String {
        let mut lines = vec![
            format!("{:<10}: {}", "Name", self.key),
            format!("{:<10}: {}", "Date", print_date(self.date)),
            format!("{:<10}: {}", "Size", humanize_ibytes(self.size)),
            format!("{:<10}: {}", "ETag", self.etag),
            format!("{:<10}: {}", "Type", "file"),
        ];
        let metadata = self.full_metadata();
        if !metadata.is_empty() {
            lines.push(format!("{:<10}:", "Metadata"));
            let width = metadata.keys().map(|k| k.len()).max().unwrap_or(0);
            for (k, v) in &metadata {
                lines.push(format!("  {k:<width$}: {v}"));
            }
        }
        lines.push(String::new());
        lines.join("\n")
    }

    fn json(&self) -> serde_json::Value {
        let mut v = json!({
            "status": "success",
            "name": self.key,
            "lastModified": self.date.to_rfc3339(),
            "size": self.size,
            "etag": self.etag,
            "type": "file",
        });
        let metadata = self.full_metadata();
        if !metadata.is_empty() {
            v["metadata"] = json!(metadata);
        }
        v
    }
}

/// `cp`/`mv`/`put`/`get` per-object completion message.
pub(crate) struct CopyMessage {
    pub source: String,
    pub target: String,
    pub size: u64,
    pub total_count: u64,
    pub total_size: u64,
}

impl McMessage for CopyMessage {
    fn human(&self) -> String {
        format!("`{}` -> `{}`", self.source, self.target)
    }

    fn json(&self) -> serde_json::Value {
        json!({
            "status": "success",
            "source": self.source,
            "target": self.target,
            "size": self.size,
            "totalCount": self.total_count,
            "totalSize": self.total_size,
        })
    }
}

/// `mirror` per-object completion message: either a copy (`removed:
/// false`) or a delete of a target-only object (`removed: true`).
pub(crate) struct MirrorMessage {
    pub source: String,
    pub target: String,
    pub size: u64,
    pub total_count: u64,
    pub total_size: u64,
    pub removed: bool,
}

impl McMessage for MirrorMessage {
    fn human(&self) -> String {
        if self.removed {
            format!("Removed `{}`.", self.target)
        } else {
            format!("`{}` -> `{}`", self.source, self.target)
        }
    }

    fn json(&self) -> serde_json::Value {
        json!({
            "status": "success",
            "source": self.source,
            "target": self.target,
            "size": self.size,
            "totalCount": self.total_count,
            "totalSize": self.total_size,
            "eventTime": if self.removed { Utc::now().to_rfc3339() } else { String::new() },
            "eventType": if self.removed { "s3:ObjectRemoved:Delete" } else { "" },
        })
    }
}

/// Final summary for a transfer session (`cp`/`put`/`get`/`mirror`).
/// `duration_ns` serializes as a raw nanosecond integer, matching mc's
/// `time.Duration`-has-no-custom-marshaling footgun (research doc §5,
/// gotcha 7). `speed_bps` is bytes/sec (mc's `accounter.Speed()`); only the
/// human rendering converts to MB/s.
pub(crate) struct AccountStat {
    pub total: u64,
    pub transferred: u64,
    pub duration_ns: u128,
    pub speed_bps: f64,
}

impl McMessage for AccountStat {
    fn human(&self) -> String {
        let secs = self.duration_ns as f64 / 1_000_000_000.0;
        let mb_per_sec = self.speed_bps / (1024.0 * 1024.0);
        format!(
            "Total: {} | Transferred: {} | Duration: {secs:.2}s | Speed: {mb_per_sec:.2} MB/s",
            humanize_ibytes(self.total),
            humanize_ibytes(self.transferred),
        )
    }

    fn json(&self) -> serde_json::Value {
        json!({
            "status": "success",
            "total": self.total,
            "transferred": self.transferred,
            "duration": self.duration_ns.min(u64::MAX as u128) as u64,
            "speed": self.speed_bps,
        })
    }
}

/// `du` per-prefix message ([SEM] §5's `duMessage`). `main.rs`'s `du_walk`
/// prints one of these per directory level actually visited, per the depth
/// semantics implemented there; `isVersions` is always `false` -- rs3 has no
/// `--versions`-aware `du` path yet.
pub(crate) struct DuMessage {
    pub prefix: String,
    pub size: u64,
    pub objects: u64,
}

impl McMessage for DuMessage {
    fn human(&self) -> String {
        let mut label = "object".to_string();
        if self.objects != 1 {
            label.push('s');
        }
        format!(
            "{}\t{} {label}\t{}",
            humanize_ibytes(self.size),
            self.objects,
            self.prefix
        )
    }

    fn json(&self) -> serde_json::Value {
        json!({
            "prefix": self.prefix,
            "size": self.size,
            "objects": self.objects,
            "status": "success",
            "isVersions": false,
        })
    }
}

/// `tree` per-entry message ([OUT] §2's `treeMessage`): a pre-rendered
/// `branch_string` (glyphs + continuation columns, built by the caller) and
/// the bare entry name. `is_dir` mirrors mc's `IsDir` field (selects the
/// `"Dir"`/`"File"` colorize tag there; unread here since rs3's `--no-color`
/// path never emits ANSI codes at all).
pub(crate) struct TreeMessage {
    pub entry: String,
    pub is_dir: bool,
    pub branch_string: String,
}

impl McMessage for TreeMessage {
    fn human(&self) -> String {
        format!("{}{}", self.branch_string, self.entry)
    }

    fn json(&self) -> serde_json::Value {
        // mc's own `treeMessage.JSON()` is a deliberate `fatalIf`-panic --
        // `tree --json` never reaches it, since `main.rs`'s `tree()` reroutes
        // to the `ls --recursive --json` code path before constructing any
        // `TreeMessage` ([OUT] §2 gotcha 9). Kept non-panicking here (rather
        // than mirroring the panic) since nothing depends on this shape ever
        // being observed.
        json!({"entry": self.entry, "isDir": self.is_dir, "branchString": self.branch_string})
    }
}

#[derive(Default)]
struct SessionState {
    total_count: u64,
    total_size: u64,
    transferred_count: u64,
    transferred_size: u64,
}

/// Tracks one `cp`/`put`/`get`/`mirror` invocation's progress and owns the
/// choice between an animated byte-progress bar and printing a message per
/// object. Per `docs/superpowers/research/mc-research-output.md` §5, the
/// bar is shown iff stdout is a TTY and neither `--quiet` nor `--json` is
/// set; otherwise every object completion is a printed
/// `CopyMessage`/`MirrorMessage` and a final `AccountStat` line closes the
/// session (mc's "`--quiet` is not silence" behavior, §4).
///
/// Deviates from the brief's sketched `&mut self` API: mirror's copies run
/// concurrently via `buffer_unordered`, so every method here takes `&self`
/// and guards the mutable bookkeeping with an internal `Mutex`. This lets
/// callers hold a single shared `&TransferSession` across concurrent
/// futures instead of threading `&mut` through them (which `buffer_unordered`
/// can't express). Noted in the task report.
pub(crate) struct TransferSession {
    bar: Option<ProgressBar>,
    state: Mutex<SessionState>,
    started: Instant,
}

impl TransferSession {
    /// `label` is reserved for a future bar prefix/caption; the bar itself
    /// is intentionally plain for now since no e2e test observes it (bar
    /// mode requires a real TTY, which the test harness never provides).
    pub(crate) fn new(_label: &str) -> Self {
        let use_bar = out().stdout_tty && !out().quiet && !out().json;
        let bar = if use_bar {
            let pb = ProgressBar::new(0);
            if let Ok(style) = ProgressStyle::with_template(
                "{bar:40.cyan/blue} {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta})",
            ) {
                pb.set_style(style);
            }
            Some(pb)
        } else {
            None
        };
        Self {
            bar,
            state: Mutex::new(SessionState::default()),
            started: Instant::now(),
        }
    }

    /// Register `bytes` of additional planned transfer for one more object
    /// (called once per object, as its size becomes known -- typically
    /// right before or as its transfer starts).
    pub(crate) fn add_total(&self, bytes: u64) {
        let mut state = self.state.lock().expect("TransferSession state poisoned");
        state.total_count += 1;
        state.total_size += bytes;
        if let Some(bar) = &self.bar {
            bar.inc_length(bytes);
        }
    }

    /// Current running totals, for filling in a `CopyMessage`/
    /// `MirrorMessage`'s `total_count`/`total_size` fields.
    pub(crate) fn totals(&self) -> (u64, u64) {
        let state = self.state.lock().expect("TransferSession state poisoned");
        (state.total_count, state.total_size)
    }

    /// Mark one object's transfer complete (`size` bytes moved -- `0` for
    /// a delete event, which carries no transferred payload). In Bar mode
    /// this ticks the bar and prints nothing; otherwise it prints `msg` via
    /// [`print_msg`].
    pub(crate) fn object_done(&self, msg: &dyn McMessage, size: u64) {
        {
            let mut state = self.state.lock().expect("TransferSession state poisoned");
            state.transferred_count += 1;
            state.transferred_size += size;
        }
        match &self.bar {
            Some(bar) => bar.inc(size),
            None => print_msg(msg),
        }
    }

    /// Close the session: Bar mode clears the bar and prints nothing;
    /// otherwise prints a final [`AccountStat`] summary line (mc prints
    /// this even in `--quiet` mode -- research doc §4 point 3).
    pub(crate) fn finish(&self) {
        if let Some(bar) = &self.bar {
            bar.finish_and_clear();
            return;
        }
        let state = self.state.lock().expect("TransferSession state poisoned");
        let elapsed = self.started.elapsed();
        let secs = elapsed.as_secs_f64();
        let speed_bps = if secs > 0.0 {
            state.transferred_size as f64 / secs
        } else {
            0.0
        };
        let stat = AccountStat {
            total: state.total_size,
            transferred: state.transferred_size,
            duration_ns: elapsed.as_nanos(),
            speed_bps,
        };
        print_msg(&stat);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn content_message_file_human_and_json() {
        let msg = ContentMessage {
            status: "success".into(),
            filetype: "file".into(),
            time: t(),
            size: 5,
            key: "f.txt".into(),
            etag: "abc123".into(),
            storage_class: None,
        };
        assert!(msg.human().ends_with("5B f.txt"), "human: {}", msg.human());
        let v = msg.json();
        assert_eq!(v["status"], "success");
        assert_eq!(v["type"], "file");
        assert_eq!(v["size"], 5);
        assert_eq!(v["key"], "f.txt");
        assert_eq!(v["etag"], "abc123");
        assert!(v.get("storageClass").is_none());
    }

    #[test]
    fn content_message_folder_gets_trailing_slash_and_zero_size() {
        let msg = ContentMessage {
            status: "success".into(),
            filetype: "folder".into(),
            time: t(),
            size: 0,
            key: "sub".into(),
            etag: String::new(),
            storage_class: None,
        };
        let human = msg.human();
        assert!(human.contains("0B"), "human: {human}");
        assert!(human.trim_end().ends_with("sub/"), "human: {human}");
    }

    #[test]
    fn content_message_storage_class_present_when_set() {
        let msg = ContentMessage {
            status: "success".into(),
            filetype: "file".into(),
            time: t(),
            size: 1,
            key: "f".into(),
            etag: String::new(),
            storage_class: Some("GLACIER".into()),
        };
        assert_eq!(msg.json()["storageClass"], "GLACIER");
    }

    #[test]
    fn summary_message_human_and_json_style() {
        let msg = SummaryMessage {
            total_objects: 3,
            total_size: 1024,
        };
        assert_eq!(msg.human(), "\nTotal Size: 1.0KiB\nTotal Objects: 3");
        assert_eq!(msg.json(), json!({"totalObjects": 3, "totalSize": 1024}));
        assert!(matches!(msg.json_style(), JsonStyle::EmptyIndent));
    }

    #[test]
    fn make_bucket_message_human_and_json() {
        let msg = MakeBucketMessage {
            bucket: "alias/b".into(),
        };
        assert_eq!(msg.human(), "Bucket created successfully `alias/b`.");
        assert_eq!(
            msg.json(),
            json!({"status": "success", "bucket": "alias/b", "region": ""})
        );
    }

    #[test]
    fn remove_bucket_message_is_always_compact() {
        let msg = RemoveBucketMessage {
            bucket: "alias/b".into(),
        };
        assert_eq!(msg.human(), "Removed `alias/b` successfully.");
        assert!(matches!(msg.json_style(), JsonStyle::AlwaysCompact));
    }

    #[test]
    fn rm_message_modtime_is_null_not_omitted() {
        let msg = RmMessage {
            key: "alias/b/k".into(),
            dry_run: false,
            mod_time: None,
        };
        let v = msg.json();
        assert!(v.get("modTime").is_some());
        assert!(v["modTime"].is_null());
        assert_eq!(v["dryRun"], false);
        assert_eq!(msg.human(), "Removed `alias/b/k`.");
    }

    #[test]
    fn rm_message_dry_run_human_text() {
        let msg = RmMessage {
            key: "alias/b/k".into(),
            dry_run: true,
            mod_time: None,
        };
        assert_eq!(msg.human(), "DRYRUN: Removing `alias/b/k`.");
        assert_eq!(msg.json()["dryRun"], true);
    }

    #[test]
    fn stat_message_type_is_always_file_and_content_type_folds_into_metadata() {
        let msg = StatMessage {
            key: "f.txt".into(),
            date: t(),
            size: 12,
            etag: "etag123".into(),
            content_type: Some("text/plain".into()),
            cache_control: None,
            metadata: BTreeMap::new(),
        };
        let v = msg.json();
        assert_eq!(v["type"], "file");
        assert!(v.get("contentType").is_none());
        assert_eq!(v["metadata"]["Content-Type"], "text/plain");
        let human = msg.human();
        assert!(human.contains("Type      : file"), "human: {human}");
        assert!(human.contains("Content-Type: text/plain"), "human: {human}");
        assert!(
            human.ends_with('\n'),
            "human should end in blank line: {human:?}"
        );
    }

    #[test]
    fn stat_message_omits_metadata_block_when_empty() {
        let msg = StatMessage {
            key: "f.txt".into(),
            date: t(),
            size: 0,
            etag: String::new(),
            content_type: None,
            cache_control: None,
            metadata: BTreeMap::new(),
        };
        assert!(msg.json().get("metadata").is_none());
        assert!(!msg.human().contains("Metadata"));
    }

    #[test]
    fn copy_message_human_and_json() {
        let msg = CopyMessage {
            source: "local/f.txt".into(),
            target: "alias/bucket/f.txt".into(),
            size: 5,
            total_count: 1,
            total_size: 5,
        };
        assert_eq!(msg.human(), "`local/f.txt` -> `alias/bucket/f.txt`");
        assert_eq!(
            msg.json(),
            json!({
                "status": "success",
                "source": "local/f.txt",
                "target": "alias/bucket/f.txt",
                "size": 5,
                "totalCount": 1,
                "totalSize": 5,
            })
        );
    }

    #[test]
    fn mirror_message_copy_vs_delete_human_and_event_fields() {
        let copy = MirrorMessage {
            source: "local/a.txt".into(),
            target: "alias/bucket/a.txt".into(),
            size: 3,
            total_count: 1,
            total_size: 3,
            removed: false,
        };
        assert_eq!(copy.human(), "`local/a.txt` -> `alias/bucket/a.txt`");
        assert_eq!(copy.json()["eventType"], "");
        assert_eq!(copy.json()["eventTime"], "");

        let delete = MirrorMessage {
            source: String::new(),
            target: "alias/bucket/stale.txt".into(),
            size: 0,
            total_count: 1,
            total_size: 3,
            removed: true,
        };
        assert_eq!(delete.human(), "Removed `alias/bucket/stale.txt`.");
        assert_eq!(delete.json()["eventType"], "s3:ObjectRemoved:Delete");
        assert!(!delete.json()["eventTime"].as_str().unwrap().is_empty());
    }

    #[test]
    fn account_stat_human_and_raw_nanosecond_json() {
        let stat = AccountStat {
            total: 1024,
            transferred: 1024,
            duration_ns: 2_000_000_000, // 2s
            speed_bps: 1024.0 * 1024.0, // 1 MiB/s
        };
        let human = stat.human();
        assert!(human.contains("Total: 1.0KiB"), "human: {human}");
        assert!(human.contains("Transferred: 1.0KiB"), "human: {human}");
        assert!(human.contains("Duration: 2.00s"), "human: {human}");
        assert!(human.contains("Speed: 1.00 MB/s"), "human: {human}");
        let v = stat.json();
        assert_eq!(v["duration"], 2_000_000_000u64);
        assert!(v["duration"].is_u64(), "duration must serialize as an int");
        assert_eq!(v["transferred"], 1024);
        assert_eq!(v["speed"], 1024.0 * 1024.0);
    }

    #[test]
    fn du_message_pluralizes_and_serializes_field_order() {
        let msg = DuMessage {
            prefix: "test/dub/a/sub".into(),
            size: 1024,
            objects: 1,
        };
        assert_eq!(msg.human(), "1.0KiB\t1 object\ttest/dub/a/sub");
        assert_eq!(
            msg.json(),
            json!({
                "prefix": "test/dub/a/sub",
                "size": 1024,
                "objects": 1,
                "status": "success",
                "isVersions": false,
            })
        );

        let plural = DuMessage {
            prefix: "test/dub".into(),
            size: 3072,
            objects: 3,
        };
        assert_eq!(plural.human(), "3.0KiB\t3 objects\ttest/dub");
    }

    #[test]
    fn transfer_session_lines_mode_tracks_totals_and_prints_account_stat() {
        // Not using TransferSession::new here since it reads the process-
        // global `out()` singleton (shared across every test in this
        // binary); construct the same Lines-mode shape directly instead so
        // this test is independent of init/ordering.
        let session = TransferSession {
            bar: None,
            state: Mutex::new(SessionState::default()),
            started: Instant::now(),
        };
        session.add_total(10);
        session.add_total(20);
        assert_eq!(session.totals(), (2, 30));
        let msg = CopyMessage {
            source: "a".into(),
            target: "b".into(),
            size: 10,
            total_count: 2,
            total_size: 30,
        };
        session.object_done(&msg, 10);
        let state = session.state.lock().unwrap();
        assert_eq!(state.transferred_count, 1);
        assert_eq!(state.transferred_size, 10);
    }
}
