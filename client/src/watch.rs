//! `rs3 watch ALIAS/BUCKET[/PREFIX]`: follows a bucket's changelog (a
//! rusts3 extension, see `docs/spec-replication.md` §4) and prints every
//! change as it commits — a subscriber on the command line, the same feed
//! webhooks and replication links read. Needs `s3:GetBucketChangelog` on
//! the bucket.
//!
//! By default it starts at the log's head (new changes only) and
//! long-polls until stopped. `--from-start` replays the retained log,
//! `--after N` resumes after a position, `--snapshot` lists the current
//! objects first (`--versions`: every version) and then follows. A log
//! that was replaced (409) or trimmed past the position (410) is reported
//! and followed from its head, so the watch never dies on its own.

use anyhow::{Context, Result, anyhow};
use clap::Args;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::{Digest, Sha256};

use crate::config::{client_for_alias, resolve_region};
use crate::output::{out, render_json, JsonStyle};
use crate::share::{derive_signing_key, hex_hmac};
use crate::urls::parse_s3_url;

#[derive(Args, Debug)]
pub(crate) struct WatchArgs {
    #[arg(help = "ALIAS/BUCKET[/PREFIX]: the bucket (and key prefix) to watch")]
    pub(crate) target: String,
    #[arg(long, help = "resume after this changelog position (default: the head, new changes only)")]
    pub(crate) after: Option<u64>,
    #[arg(long, help = "replay the whole retained log first")]
    pub(crate) from_start: bool,
    #[arg(long, help = "list the current objects first, then follow")]
    pub(crate) snapshot: bool,
    #[arg(long, help = "with --snapshot: every version and delete marker, not just the current states")]
    pub(crate) versions: bool,
    #[arg(long, help = "only these operations, comma-separated: put,delete,remove_version,tags,bucket")]
    pub(crate) ops: Option<String>,
    #[arg(long, default_value_t = 500, help = "events per request (at most 10000)")]
    pub(crate) limit: usize,
    #[arg(long, default_value_t = 20, help = "seconds a request waits for a change before answering empty (at most 60)")]
    pub(crate) wait: u64,
    #[arg(long, help = "stop once the log has been read to its head")]
    pub(crate) once: bool,
}

/// Everything but RFC 3986 unreserved characters, as SigV4 wants.
const QUERY: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

fn encode(value: &str) -> String {
    utf8_percent_encode(value, QUERY).to_string()
}

struct Reader {
    http: reqwest::Client,
    endpoint: String,
    host: String,
    access_key: String,
    secret_key: String,
    region: String,
    bucket: String,
}

impl Reader {
    /// A SigV4-signed GET of `/{bucket}?{query}` (header signing, unsigned
    /// payload), canonicalised exactly as the server verifies it.
    async fn get(&self, params: &[(&str, String)]) -> Result<(reqwest::StatusCode, serde_json::Value)> {
        let mut pairs: Vec<(String, String)> = params.iter().map(|(k, v)| (encode(k), encode(v))).collect();
        let query = pairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        pairs.sort();
        let canonical_query = pairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        let datetime = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let date = &datetime[..8];
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let path = format!("/{}", encode(&self.bucket));
        let canonical_headers = format!("host:{}\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:{datetime}\n", self.host);
        let signed = "host;x-amz-content-sha256;x-amz-date";
        let canonical = format!("GET\n{path}\n{canonical_query}\n{canonical_headers}\n{signed}\nUNSIGNED-PAYLOAD");
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{}", hex_sha256(canonical.as_bytes()));
        let signature = hex_hmac(&derive_signing_key(&self.secret_key, date, &self.region, "s3"), string_to_sign.as_bytes());
        let authorization = format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}", self.access_key);
        let response = self
            .http
            .get(format!("{}{path}?{query}", self.endpoint))
            .header("host", &self.host)
            .header("x-amz-date", &datetime)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .header("authorization", authorization)
            .send()
            .await
            .context("changelog request")?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let value = serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({ "error": text.trim() }));
        Ok((status, value))
    }
}

fn hex_sha256(data: &[u8]) -> String {
    Sha256::digest(data).iter().map(|b| format!("{b:02x}")).collect()
}

/// One line per event: time, op, key, and what identifies the state.
fn human(event: &serde_json::Value, prefix: &str) -> String {
    let op = event["op"].as_str().unwrap_or("?");
    let key = event["key"].as_str().unwrap_or("");
    let when = event["ts_ms"].as_i64().or_else(|| event["last_modified_ms"].as_i64()).map(|ms| {
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms).map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string()).unwrap_or_default()
    });
    let seq = event["seq"].as_u64().map(|s| format!("#{s}")).unwrap_or_else(|| "snapshot".to_string());
    let origin = event["origin"].as_str().map(|o| format!(" by {o}")).unwrap_or_default();
    let detail = match op {
        "put" | "exists" => format!("{} bytes, etag {}, version {}", event["size"], event["etag"].as_str().unwrap_or(""), event["version"].as_str().unwrap_or("")),
        "delete" | "deleted" => format!("removed version {}", event["version"].as_str().filter(|v| !v.is_empty()).unwrap_or("(none here)")),
        "remove_version" => format!("version {} gone for good", event["version"].as_str().unwrap_or("")),
        "tags" => format!("tags {}", event["tags"]),
        "bucket" => format!("settings {}", event["settings"]),
        _ => String::new(),
    };
    let stamp = event["stamp"].as_object().map(|s| format!(" stamp {}.{}.{}", s["ms"], s["c"], s["site"].as_str().unwrap_or(""))).unwrap_or_default();
    format!("{} {seq:>9} {op:<14} {prefix}{key}  {detail}{stamp}{origin}", when.unwrap_or_default())
}

fn emit(event: &serde_json::Value, prefix: &str) {
    if out().json {
        crate::progress::ui_println!("{}", render_json(event, JsonStyle::AlwaysCompact, false));
    } else {
        crate::progress::ui_println!("{}", human(event, prefix));
    }
}

fn note(message: &str) {
    if !out().quiet {
        crate::progress::ui_eprintln!("{message}");
    }
}

pub(crate) async fn run_watch(args: WatchArgs) -> Result<()> {
    let url = parse_s3_url(&args.target)?;
    let bucket = url.bucket.clone().ok_or_else(|| anyhow!("target must be ALIAS/BUCKET[/PREFIX]"))?;
    let prefix = url.key.clone().unwrap_or_default();
    let (_, alias) = client_for_alias(&url.alias).await?;
    let region = resolve_region(&alias);
    let endpoint = alias.url.trim_end_matches('/').to_string();
    let host = endpoint
        .split("://")
        .nth(1)
        .map(|h| h.split('/').next().unwrap_or(h).to_string())
        .ok_or_else(|| anyhow!("alias url {} has no host", alias.url))?;
    let reader = Reader {
        http: reqwest::Client::builder().user_agent(concat!("rs3/", env!("CARGO_PKG_VERSION"))).build()?,
        endpoint,
        host,
        access_key: alias.access_key.clone(),
        secret_key: alias.secret_key.clone(),
        region,
        bucket: bucket.clone(),
    };
    let limit = args.limit.clamp(1, 10_000).to_string();
    let wait = args.wait.min(60).to_string();
    let ops = args.ops.clone().unwrap_or_default();
    let mut common: Vec<(&str, String)> = Vec::new();
    if !prefix.is_empty() {
        common.push(("prefix", prefix.clone()));
    }
    if !ops.is_empty() {
        common.push(("ops", ops.clone()));
    }

    // Where to start: the head (default), a given position, or the start
    // of the retained log. A snapshot fixes the head first, then lists.
    let mut params = vec![("x-rs3-changelog", String::new()), ("after", "0".to_string()), ("limit", "1".to_string()), ("wait", "0".to_string())];
    let (status, first) = reader.get(&params).await?;
    if !status.is_success() {
        return Err(anyhow!("{}: {}", status, first["error"].as_str().unwrap_or("changelog not readable (s3:GetBucketChangelog?)")));
    }
    let mut log_id = first["log_id"].as_str().unwrap_or_default().to_string();
    let head = first["head"].as_u64().unwrap_or(0);
    let trimmed = first["trimmed_through"].as_u64().unwrap_or(0);
    let mut after = match (args.after, args.from_start) {
        (Some(after), _) => after,
        (None, true) => trimmed,
        (None, false) => head,
    };
    note(&format!(
        "watching {}/{bucket}{} at site {} ({}), log {log_id}, head {head}{}",
        url.alias,
        if prefix.is_empty() { String::new() } else { format!("/{prefix}") },
        first["site_name"].as_str().unwrap_or(""),
        first["site_id"].as_str().unwrap_or(""),
        if after < head { format!(", replaying from {after}") } else { String::new() }
    ));

    if args.snapshot {
        let mut cursor: Option<String> = None;
        let mut version_cursor: Option<String> = None;
        loop {
            let mut page_params = vec![("x-rs3-changelog-snapshot", String::new()), ("limit", limit.clone())];
            page_params.extend(common.iter().cloned());
            if args.versions {
                page_params.push(("versions", "1".to_string()));
            }
            if let Some(cursor) = &cursor {
                page_params.push(("after", cursor.clone()));
            }
            if let Some(version) = &version_cursor {
                page_params.push(("after_version", version.clone()));
            }
            let (status, page) = reader.get(&page_params).await?;
            if !status.is_success() {
                return Err(anyhow!("snapshot: {}: {}", status, page["error"]));
            }
            for entry in page["entries"].as_array().into_iter().flatten() {
                emit(entry, "");
            }
            match page["next_after"].as_str() {
                Some(next) => {
                    cursor = Some(next.to_string());
                    version_cursor = page["next_after_version"].as_str().map(str::to_string);
                }
                None => break,
            }
        }
        // Live continues after the snapshot's head: everything that
        // changed during the listing is replayed.
        after = after.min(head);
    }

    loop {
        params = vec![
            ("x-rs3-changelog", String::new()),
            ("after", after.to_string()),
            ("limit", limit.clone()),
            ("wait", if args.once { "0".to_string() } else { wait.clone() }),
            ("log_id", log_id.clone()),
        ];
        params.extend(common.iter().cloned());
        let (status, page) = match reader.get(&params).await {
            Ok(reply) => reply,
            Err(err) => {
                note(&format!("retrying in 5s: {err:#}"));
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        match status.as_u16() {
            200 => {}
            409 => {
                // The log was replaced (an index rebuild): follow the new one.
                let new_log = page["log_id"].as_str().unwrap_or_default().to_string();
                let new_head = page["head"].as_u64().unwrap_or(0);
                note(&format!("the bucket's changelog was replaced (now {new_log}, head {new_head}): earlier positions no longer apply; following from its head"));
                log_id = new_log;
                after = new_head;
                continue;
            }
            410 => {
                let trimmed = page["trimmed_through"].as_u64().unwrap_or(0);
                note(&format!("position {after} is behind the log's retention (trimmed through {trimmed}): changes in between are lost to this watch; continuing after {trimmed}"));
                after = trimmed;
                continue;
            }
            503 => {
                note("bucket unavailable (index rebuild or layout upgrade): retrying in 5s");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
            _ => return Err(anyhow!("{}: {}", status, page["error"].as_str().unwrap_or("unexpected answer"))),
        }
        for event in page["events"].as_array().into_iter().flatten() {
            emit(event, "");
        }
        after = page["next"].as_u64().unwrap_or(after);
        if args.once && !page["more"].as_bool().unwrap_or(false) {
            return Ok(());
        }
    }
}
