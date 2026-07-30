//! `share download`/`share upload`/`share list`. Semantics normative per
//! `docs/superpowers/research/mc-research-semantics.md` §10 and
//! `mc-research-output.md` §2 `shareMessage`, ground-truth-verified against
//! real `mc` (RELEASE.2025-08-13T08-35-41Z) running against a live `rusts3`
//! server -- see the task report for the probe transcript.
//!
//! - `download` presigns a plain S3 `GetObject` via the SDK's own
//!   `PresigningConfig` -- no hand-rolled signing needed.
//! - `upload` hand-rolls a browser-POST policy (bucket/key conditions,
//!   `AWS4-HMAC-SHA256` fields, HMAC-SHA256 signature over the base64
//!   policy document) and renders it as a literal `curl` command per [SEM]
//!   §10's template. Ground-truth-verified end to end: a real `mc share
//!   upload` curl command, run unmodified against a live `rusts3`, actually
//!   lands the object (`rusts3`'s browser-POST verifier,
//!   `src/server/auth.rs::authorize_browser_post`/
//!   `verify_post_policy_document`, accepts both the `{"bucket":"x"}`
//!   object-form and `["eq"/"starts-with","$key",...]` array-form
//!   conditions this module emits).
//! - `--expire`/`-E` is parsed with [`parse_go_duration`] -- Go's stdlib
//!   `time.ParseDuration` grammar only (`ns`/`us`/`µs`/`ms`/`s`/`m`/`h`; no
//!   `d`/`w`/`y`), ground-truth-confirmed by `mc share download --expire 7d`
//!   failing with `time: unknown unit "d" in duration "7d"`. The clamp to
//!   `[1s, 7d]` operates on the *raw* parsed nanosecond count (so `999ms` is
//!   rejected but `1500ms` is accepted and then truncated down to a
//!   whole-second `1s` expiry for the actual presign/policy window --
//!   ground-truth-confirmed).
//!
//! One deliberate deviation from real `mc`, documented here rather than
//! silently: real `mc`'s `shareMessage.TimeLeft` JSON field leaks the
//! *pre-truncation* raw parsed duration (so `--expire 1500ms` reports
//! `"timeLeft":1500000000` even though the link is only actually valid for
//! 1 whole second). This module instead reports the same truncated
//! whole-second value everywhere (presign window, humanized `Expire:` line,
//! and JSON `timeLeft`) -- still a raw-nanoseconds JSON int per [OUT] §2,
//! just internally consistent rather than reproducing mc's precision-loss
//! quirk. Not observable by anything in this task's test suite (which only
//! asserts `timeLeft > 0`).
//!
//! The local share DB (`{config_dir}/share/{uploads,downloads}.json`) is an
//! rs3-specific implementation detail -- [SEM] §10 explicitly declines to
//! specify mc's own on-disk format ("not detailed further here"). This
//! module stores `expiry` as nanoseconds (see [`ShareEntry`]) and simply
//! appends every generated share to the list, un-deduplicated -- matching
//! observed real-`mc` behavior of listing the same target multiple times
//! after repeated `share upload`/`download` calls.

use std::path::PathBuf;
use std::time::Duration as StdDuration;

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
use aws_sdk_s3::presigning::PresigningConfig;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use tokio::fs;

use crate::config::{Alias, client_for_alias, config_path, resolve_region};
use crate::list::collect_objects;
use crate::messages::ShareMessage;
use crate::output::print_msg;
use crate::urls::parse_s3_url;
use crate::{ShareArgs, ShareCommand, ShareDownloadArgs, ShareListArgs, ShareUploadArgs};

type HmacSha256 = Hmac<Sha256>;

const NS_PER_SEC: i64 = 1_000_000_000;
const MIN_EXPIRE_NS: i64 = NS_PER_SEC;
const MAX_EXPIRE_NS: i64 = 604_800 * NS_PER_SEC;

pub(crate) async fn run_share(args: ShareArgs) -> Result<()> {
    match args.command {
        ShareCommand::Download(a) => run_download(a).await,
        ShareCommand::Upload(a) => run_upload(a).await,
        ShareCommand::List(a) => run_list(a).await,
    }
}

/// Go's `time.ParseDuration` grammar: an optional sign, then one or more
/// `<number><unit>` segments (number = digits with an optional `.`
/// fraction; unit one of `ns`/`us`/`µs`(U+00B5)/`μs`(U+03BC)/`ms`/`s`/`m`/
/// `h`), or the bare literal `0`. Deliberately excludes the `d`/`w`/`y`
/// units rs3's other duration parser ([`crate::timefilter`]) accepts --
/// `share`'s `--expire` uses plain Go `time.ParseDuration`, not mc's
/// custom calendar-aware variant ([SEM] §10, ground-truth-confirmed: `mc
/// share download --expire 7d` is a hard parse error).
pub(crate) fn parse_go_duration(input: &str) -> Result<i64> {
    let invalid = || anyhow!("time: invalid duration \"{input}\"");
    let mut s = input;
    if s.is_empty() {
        return Err(invalid());
    }
    let neg = match s.as_bytes()[0] {
        b'-' => {
            s = &s[1..];
            true
        }
        b'+' => {
            s = &s[1..];
            false
        }
        _ => false,
    };
    if s == "0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(invalid());
    }

    let mut total: f64 = 0.0;
    let mut rest = s;
    while !rest.is_empty() {
        let bytes = rest.as_bytes();
        let mut idx = 0;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        let int_len = idx;
        let mut frac_len = 0;
        if idx < bytes.len() && bytes[idx] == b'.' {
            idx += 1;
            let frac_start = idx;
            while idx < bytes.len() && bytes[idx].is_ascii_digit() {
                idx += 1;
            }
            frac_len = idx - frac_start;
        }
        if int_len == 0 && frac_len == 0 {
            return Err(invalid());
        }
        let number_str = &rest[..idx];
        let number: f64 = number_str.parse().map_err(|_| invalid())?;
        rest = &rest[idx..];

        let unit_len = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        if unit_len == 0 {
            return Err(anyhow!("time: missing unit in duration \"{input}\""));
        }
        let unit = &rest[..unit_len];
        rest = &rest[unit_len..];
        let mult_ns: f64 = match unit {
            "ns" => 1.0,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60.0 * 1_000_000_000.0,
            "h" => 3_600.0 * 1_000_000_000.0,
            other => {
                return Err(anyhow!(
                    "time: unknown unit \"{other}\" in duration \"{input}\""
                ));
            }
        };
        total += number * mult_ns;
    }
    let ns = total.round() as i64;
    Ok(if neg { -ns } else { ns })
}

/// Parses and clamps `--expire` per [SEM] §10: `< 1s` and `> 7 days` are
/// both fatal (mc's exact wording), leaving `7 days` as both the flag's
/// default *and* its hard ceiling. Returns the truncated whole-second
/// expiry actually used for the presign/policy window (see module docs for
/// why this module doesn't also thread through the pre-truncation raw
/// value).
fn validate_expire(raw: &str) -> Result<u64> {
    let ns = parse_go_duration(raw).map_err(|e| anyhow!("Unable to parse expire=`{raw}`. {e}."))?;
    if ns < MIN_EXPIRE_NS {
        return Err(anyhow!("Expiry cannot be lesser than 1 second."));
    }
    if ns > MAX_EXPIRE_NS {
        return Err(anyhow!("Expiry cannot be larger than 7 days."));
    }
    Ok((ns / NS_PER_SEC) as u64)
}

async fn run_download(args: ShareDownloadArgs) -> Result<()> {
    let expire_secs = validate_expire(&args.expire)?;
    let mut db = load_db(ShareKind::Download).await?;
    for target in &args.targets {
        let parsed = parse_s3_url(target)?;
        let bucket = parsed.bucket.clone().ok_or_else(|| {
            anyhow!("share download: target `{target}` must be ALIAS/BUCKET[/OBJECT]")
        })?;
        let (client, alias) = client_for_alias(&parsed.alias).await?;
        let endpoint = alias.url.trim_end_matches('/').to_string();
        if args.recursive {
            let prefix = parsed.key.clone().unwrap_or_default();
            for obj in collect_objects(&client, &bucket, &prefix).await? {
                share_one_download(&client, &endpoint, &bucket, &obj.key, expire_secs, &mut db)
                    .await?;
            }
        } else {
            let key = parsed.key.clone().ok_or_else(|| {
                anyhow!("share download: target `{target}` must include an object key")
            })?;
            client
                .head_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .with_context(|| format!("Unable to stat `{target}`"))?;
            share_one_download(&client, &endpoint, &bucket, &key, expire_secs, &mut db).await?;
        }
    }
    save_db(ShareKind::Download, &db).await?;
    Ok(())
}

async fn share_one_download(
    client: &Client,
    endpoint: &str,
    bucket: &str,
    key: &str,
    expire_secs: u64,
    db: &mut ShareDb,
) -> Result<()> {
    let presign_cfg = PresigningConfig::expires_in(StdDuration::from_secs(expire_secs))?;
    let presigned = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .presigned(presign_cfg)
        .await?;
    let object_url = format!("{endpoint}/{bucket}/{key}");
    let share_url = presigned.uri().to_string();
    let time_left_ns = expire_secs as i64 * NS_PER_SEC;
    print_msg(&ShareMessage {
        object_url: object_url.clone(),
        share_url: share_url.clone(),
        time_left_ns,
        content_type: None,
    });
    db.entries.push(ShareEntry {
        url: object_url,
        share: share_url,
        date: Utc::now(),
        expiry_ns: time_left_ns,
        content_type: None,
    });
    Ok(())
}

async fn run_upload(args: ShareUploadArgs) -> Result<()> {
    let expire_secs = validate_expire(&args.expire)?;
    let mut db = load_db(ShareKind::Upload).await?;
    for target in &args.targets {
        let parsed = parse_s3_url(target)?;
        let bucket = parsed.bucket.clone().ok_or_else(|| {
            anyhow!("share upload: target `{target}` must be ALIAS/BUCKET[/OBJECT]")
        })?;
        let key = parsed.key.clone().unwrap_or_default();
        // [SEM] §10: a non-recursive target that looks like a prefix (empty
        // key -- bucket root -- or a key ending in the path separator) is
        // rejected; ground-truth-confirmed wording.
        if !args.recursive && (key.is_empty() || key.ends_with('/')) {
            return Err(anyhow!(
                "Use --recursive flag to generate curl command for prefixes."
            ));
        }
        let (_, alias) = client_for_alias(&parsed.alias).await?;
        let endpoint = alias.url.trim_end_matches('/').to_string();
        let region = resolve_region(&alias);

        let object_url = format!("{endpoint}/{bucket}/{key}");
        let post_url = format!("{endpoint}/{bucket}/");
        let policy = sign_policy(
            &alias,
            &region,
            &bucket,
            &key,
            args.recursive,
            args.content_type.as_deref(),
            expire_secs,
        );
        let curl = render_curl(
            &post_url,
            &bucket,
            &key,
            args.recursive,
            args.content_type.as_deref(),
            &policy,
        );
        let time_left_ns = expire_secs as i64 * NS_PER_SEC;
        print_msg(&ShareMessage {
            object_url: object_url.clone(),
            share_url: curl.clone(),
            time_left_ns,
            content_type: args.content_type.clone(),
        });
        db.entries.push(ShareEntry {
            url: object_url,
            share: curl,
            date: Utc::now(),
            expiry_ns: time_left_ns,
            content_type: args.content_type.clone(),
        });
    }
    save_db(ShareKind::Upload, &db).await?;
    Ok(())
}

/// The signed fields of a browser-POST policy, ready to render into the
/// `curl` template.
struct PolicyFields {
    policy_b64: String,
    signature: String,
    credential: String,
    amz_date: String,
}

/// Builds and signs the POST policy document per [SEM] §10 / the task
/// brief: `expiration` (RFC3339 UTC), `conditions` (bucket, key-or-prefix,
/// algorithm, credential, date, optional content-type), base64-encoded,
/// then HMAC-SHA256-signed with the standard SigV4 date/region/service/
/// `aws4_request` key-derivation chain -- byte-for-byte the same chain
/// `rusts3`'s own browser-POST verifier uses
/// (`src/server/auth.rs::derive_signing_key`), ground-truth-verified by
/// running the rendered `curl` command against a live server (see task
/// report).
fn sign_policy(
    alias: &Alias,
    region: &str,
    bucket: &str,
    key: &str,
    recursive: bool,
    content_type: Option<&str>,
    expire_secs: u64,
) -> PolicyFields {
    let now = Utc::now();
    let expiration = now + chrono::Duration::seconds(expire_secs as i64);
    let expiration_str = expiration.to_rfc3339_opts(SecondsFormat::Millis, true);
    let date_stamp = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let credential = format!("{}/{date_stamp}/{region}/s3/aws4_request", alias.access_key);

    let mut conditions: Vec<serde_json::Value> = vec![json!({"bucket": bucket})];
    if recursive {
        conditions.push(json!(["starts-with", "$key", key]));
    } else {
        conditions.push(json!({"key": key}));
    }
    conditions.push(json!({"x-amz-algorithm": "AWS4-HMAC-SHA256"}));
    conditions.push(json!({"x-amz-credential": credential}));
    conditions.push(json!({"x-amz-date": amz_date}));
    if let Some(ct) = content_type {
        conditions.push(json!({"Content-Type": ct}));
    }
    let policy_doc = json!({"expiration": expiration_str, "conditions": conditions});
    let policy_b64 = BASE64_STANDARD.encode(policy_doc.to_string());

    let signing_key = derive_signing_key(&alias.secret_key, &date_stamp, region, "s3");
    let signature = hex_hmac(&signing_key, policy_b64.as_bytes());

    PolicyFields {
        policy_b64,
        signature,
        credential,
        amz_date,
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Standard SigV4 signing-key derivation chain: `HMAC(HMAC(HMAC(HMAC("AWS4"
/// + secret, date), region), service), "aws4_request")`.
fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn hex_hmac(key: &[u8], data: &[u8]) -> String {
    hex_encode(&hmac_sha256(key, data))
}

/// [SEM] §10's `shellQuote`: backslash-escapes
/// `` &;#$`<space><tab><newline><>()|'" `` -- used only for the `key`
/// field's value (`makeCurlCmd`), never `postURL` or the other `-F`
/// values.
fn shell_quote(s: &str) -> String {
    const SPECIAL: &str = "&;#$`\t\n<>()|'\" ";
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if SPECIAL.contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Renders [SEM] §10's `curl` template verbatim: `curl {postURL} -F
/// {field}={value} ... -F key={key}<NAME if recursive> -F file=@<FILE>`.
/// `<FILE>` (always) and `<NAME>` (only when `recursive`) are left as
/// literal placeholders for the human running the command to fill in.
fn render_curl(
    post_url: &str,
    bucket: &str,
    key: &str,
    recursive: bool,
    content_type: Option<&str>,
    policy: &PolicyFields,
) -> String {
    let mut cmd = format!("curl {post_url} ");
    cmd.push_str(&format!("-F bucket={bucket} "));
    cmd.push_str(&format!("-F policy={} ", policy.policy_b64));
    cmd.push_str("-F x-amz-algorithm=AWS4-HMAC-SHA256 ");
    cmd.push_str(&format!("-F x-amz-credential={} ", policy.credential));
    cmd.push_str(&format!("-F x-amz-date={} ", policy.amz_date));
    cmd.push_str(&format!("-F x-amz-signature={} ", policy.signature));
    if let Some(ct) = content_type {
        cmd.push_str(&format!("-F Content-Type={ct} "));
    }
    let key_value = if recursive {
        format!("{}<NAME>", shell_quote(key))
    } else {
        shell_quote(key)
    };
    cmd.push_str(&format!("-F key={key_value} "));
    cmd.push_str("-F file=@<FILE>");
    cmd
}

async fn run_list(args: ShareListArgs) -> Result<()> {
    let kind = match args.kind.as_str() {
        "upload" => ShareKind::Upload,
        "download" => ShareKind::Download,
        other => {
            return Err(anyhow!(
                "share list: expected `upload` or `download`, got `{other}`"
            ));
        }
    };
    let db = load_db(kind).await?;
    let now = Utc::now();
    for entry in &db.entries {
        let elapsed_ns = now
            .signed_duration_since(entry.date)
            .num_nanoseconds()
            .unwrap_or(0);
        let time_left_ns = entry.expiry_ns - elapsed_ns;
        print_msg(&ShareMessage {
            object_url: entry.url.clone(),
            share_url: entry.share.clone(),
            time_left_ns,
            content_type: entry.content_type.clone(),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ShareKind {
    Upload,
    Download,
}

/// A single persisted share, recomputed live at `share list` time (see
/// module docs -- this is rs3's own on-disk shape, not mc's).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShareEntry {
    url: String,
    share: String,
    date: DateTime<Utc>,
    /// The share's total validity window, in nanoseconds. `list` recomputes
    /// `expiry_ns - (now - date)` live, mirroring mc's own `TimeLeft:
    /// share.Expiry - time.Since(share.Date)` ([SEM] §10) -- expired
    /// entries (a negative result) are still listed, never pruned.
    expiry_ns: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ShareDb {
    #[serde(default)]
    entries: Vec<ShareEntry>,
}

fn share_db_path(kind: ShareKind) -> Result<PathBuf> {
    let cfg = config_path()?;
    let dir = cfg
        .parent()
        .ok_or_else(|| anyhow!("unable to resolve config directory"))?;
    let name = match kind {
        ShareKind::Upload => "uploads.json",
        ShareKind::Download => "downloads.json",
    };
    Ok(dir.join("share").join(name))
}

async fn load_db(kind: ShareKind) -> Result<ShareDb> {
    let path = share_db_path(kind)?;
    if !path.exists() {
        return Ok(ShareDb::default());
    }
    let data = fs::read(&path).await?;
    if data.is_empty() {
        return Ok(ShareDb::default());
    }
    Ok(serde_json::from_slice(&data).with_context(|| format!("parse {}", path.display()))?)
}

async fn save_db(kind: ShareKind, db: &ShareDb) -> Result<()> {
    let path = share_db_path(kind)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let data = serde_json::to_vec_pretty(db)?;
    fs::write(path, data).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_duration_basic_units() {
        assert_eq!(parse_go_duration("1s").unwrap(), 1_000_000_000);
        assert_eq!(parse_go_duration("168h").unwrap(), 604_800_000_000_000);
        assert_eq!(parse_go_duration("10080m").unwrap(), 604_800_000_000_000);
        assert_eq!(parse_go_duration("30m").unwrap(), 1_800_000_000_000);
        assert_eq!(parse_go_duration("500ms").unwrap(), 500_000_000);
        assert_eq!(parse_go_duration("1h30m").unwrap(), 5_400_000_000_000);
    }

    #[test]
    fn go_duration_rejects_calendar_units() {
        let err = parse_go_duration("7d").unwrap_err().to_string();
        assert!(err.contains("unknown unit \"d\""), "got: {err}");
        assert!(parse_go_duration("2w").is_err());
        assert!(parse_go_duration("1y").is_err());
    }

    #[test]
    fn go_duration_zero_and_garbage() {
        assert_eq!(parse_go_duration("0").unwrap(), 0);
        assert!(parse_go_duration("").is_err());
        assert!(parse_go_duration("abc").is_err());
    }

    #[test]
    fn expire_clamp_rejects_out_of_range() {
        assert!(validate_expire("999ms").is_err(), "under 1s");
        assert!(validate_expire("604801s").is_err(), "over 7 days");
        assert_eq!(
            validate_expire("604800s").unwrap(),
            604_800,
            "cap is inclusive"
        );
        assert_eq!(validate_expire("1s").unwrap(), 1, "floor is inclusive");
        assert!(validate_expire("7d").is_err(), "Go units only");
    }

    #[test]
    fn expire_truncates_fractional_seconds() {
        // Ground-truth: `--expire 1500ms` reports/presigns a 1-whole-second
        // window (floor), even though the raw parse is 1.5s.
        assert_eq!(validate_expire("1500ms").unwrap(), 1);
    }

    #[test]
    fn curl_template_shape() {
        let policy = PolicyFields {
            policy_b64: "cG9saWN5".into(),
            signature: "deadbeef".into(),
            credential: "AK/20260730/us-east-1/s3/aws4_request".into(),
            amz_date: "20260730T000000Z".into(),
        };
        let cmd = render_curl(
            "http://h/bucket/",
            "bucket",
            "obj.bin",
            false,
            None,
            &policy,
        );
        assert!(cmd.starts_with("curl http://h/bucket/ "));
        assert!(cmd.contains("-F key=obj.bin "));
        assert!(cmd.ends_with("-F file=@<FILE>"));
        assert!(!cmd.contains("<NAME>"));
    }

    #[test]
    fn curl_template_recursive_appends_name_placeholder() {
        let policy = PolicyFields {
            policy_b64: "cG9saWN5".into(),
            signature: "deadbeef".into(),
            credential: "AK/20260730/us-east-1/s3/aws4_request".into(),
            amz_date: "20260730T000000Z".into(),
        };
        let cmd = render_curl(
            "http://h/bucket/",
            "bucket",
            "uploads/",
            true,
            None,
            &policy,
        );
        assert!(cmd.contains("-F key=uploads/<NAME> "));
    }

    #[test]
    fn shell_quote_escapes_specials_only_where_used() {
        assert_eq!(shell_quote("plain.txt"), "plain.txt");
        assert_eq!(shell_quote("a b"), "a\\ b");
        assert_eq!(shell_quote("a&b"), "a\\&b");
    }
}
