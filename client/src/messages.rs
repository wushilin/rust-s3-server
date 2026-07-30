//! mc-shaped message structs for `ls`, `mb`, `rb`, `rm`, and `stat`. Field
//! names/order and human/JSON rendering rules are normative per
//! `docs/superpowers/research/mc-research-output.md` §2
//! (`contentMessage`/`summaryMessage`/`makeBucketMessage`/
//! `removeBucketMessage`/`rmMessage`/`statMessage`).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::output::{JsonStyle, McMessage, humanize_ibytes, print_date};

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
/// `content_type`, if present, is folded into the `Metadata` block under a
/// `Content-Type` key, matching real `mc stat` output.
pub(crate) struct StatMessage {
    pub key: String,
    pub date: DateTime<Utc>,
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

impl StatMessage {
    fn full_metadata(&self) -> BTreeMap<String, String> {
        let mut m = self.metadata.clone();
        if let Some(ct) = &self.content_type {
            m.entry("Content-Type".to_string())
                .or_insert_with(|| ct.clone());
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
            metadata: BTreeMap::new(),
        };
        assert!(msg.json().get("metadata").is_none());
        assert!(!msg.human().contains("Metadata"));
    }
}
