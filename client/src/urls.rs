use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};

#[derive(Debug)]
pub(crate) struct S3Url {
    pub(crate) alias: String,
    pub(crate) bucket: Option<String>,
    pub(crate) key: Option<String>,
}

pub(crate) fn parse_s3_url(input: &str) -> Result<S3Url> {
    let normalized = input.trim_start_matches('/');
    let mut parts = normalized.splitn(3, '/');
    let alias = parts.next().unwrap_or_default().to_string();
    if alias.is_empty() {
        return Err(anyhow!("target must be ALIAS[/BUCKET[/OBJECT]]"));
    }
    let bucket = parts.next().filter(|b| !b.is_empty()).map(str::to_string);
    let key = parts.next().filter(|k| !k.is_empty()).map(str::to_string);
    Ok(S3Url { alias, bucket, key })
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

pub(crate) const DEFAULT_PART_SIZE: u64 = 256 * 1024 * 1024;

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
    let result = number * multiplier;
    if result == 0 {
        return Err(anyhow!("size must be greater than zero"));
    }
    Ok(result)
}

#[allow(dead_code)]
pub(crate) fn format_time(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_preserves_trailing_slash_on_key() {
        let u = parse_s3_url("alias/bucket/prefix/").unwrap();
        assert_eq!(u.alias, "alias");
        assert_eq!(u.bucket.as_deref(), Some("bucket"));
        assert_eq!(u.key.as_deref(), Some("prefix/"));
    }

    #[test]
    fn parse_bucket_root_with_trailing_slash_has_no_key() {
        let u = parse_s3_url("alias/bucket/").unwrap();
        assert_eq!(u.bucket.as_deref(), Some("bucket"));
        assert_eq!(u.key, None);
    }

    #[test]
    fn parse_plain_object_key() {
        let u = parse_s3_url("alias/bucket/dir/obj.bin").unwrap();
        assert_eq!(u.key.as_deref(), Some("dir/obj.bin"));
    }

    #[test]
    fn parse_alias_only() {
        let u = parse_s3_url("alias").unwrap();
        assert_eq!(u.bucket, None);
        assert_eq!(u.key, None);
    }

    #[test]
    fn parse_size_normal_case() {
        assert_eq!(parse_size("5MiB").unwrap(), 5 * 1024 * 1024);
    }

    #[test]
    fn parse_size_rejects_zero() {
        assert!(parse_size("0").is_err());
    }

    #[test]
    fn parse_size_rejects_zero_with_unit() {
        assert!(parse_size("0MiB").is_err());
    }
}
