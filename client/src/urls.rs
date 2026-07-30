use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};

#[derive(Debug)]
pub(crate) struct S3Url {
    pub(crate) alias: String,
    pub(crate) bucket: Option<String>,
    pub(crate) key: Option<String>,
}

pub(crate) fn parse_s3_url(input: &str) -> Result<S3Url> {
    let normalized = input.trim_matches('/');
    let mut parts = normalized.splitn(3, '/');
    let alias = parts.next().unwrap_or_default();
    if alias.is_empty() {
        return Err(anyhow!("target must be ALIAS[/BUCKET[/OBJECT]]"));
    }
    Ok(S3Url {
        alias: alias.to_string(),
        bucket: parts.next().map(str::to_string),
        key: parts.next().map(str::to_string),
    })
}

pub(crate) fn is_s3_url(input: &str) -> bool {
    !input.starts_with('/')
        && !input.starts_with("./")
        && !input.starts_with("../")
        && input.split('/').count() >= 2
}

pub(crate) fn join_s3_target(target: &str, rel: &str) -> String {
    format!(
        "{}/{}",
        target.trim_end_matches('/'),
        rel.trim_start_matches('/')
    )
}

pub(crate) fn join_key(prefix: &str, rel: &str) -> String {
    if prefix.is_empty() {
        rel.trim_start_matches('/').to_string()
    } else {
        format!(
            "{}/{}",
            prefix.trim_end_matches('/'),
            rel.trim_start_matches('/')
        )
    }
}

pub(crate) fn parse_size(input: &str) -> Result<u64> {
    let s = input.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let number: u64 = s[..split].parse()?;
    let unit = s[split..].to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        _ => return Err(anyhow!("unsupported size unit `{}`", &s[split..])),
    };
    Ok(number * multiplier)
}

#[allow(dead_code)]
pub(crate) fn format_time(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}
