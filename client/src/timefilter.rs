//! `--older-than` / `--newer-than` time filters for `cp`, `mirror`, and `rm`.
//!
//! Grammar and comparison semantics are normative per
//! `docs/superpowers/research/mc-research-semantics.md` §1. **Read the
//! polarity warning there carefully**: mc's own `isOlder`/`isNewer` helpers
//! are named backwards from what a caller would guess (`isOlder(ti, "7d")`
//! returns true when `ti` is *not* old enough to be skipped). The functions
//! here are named for the user-facing, positive-match behavior instead:
//!
//! - `--older-than SPEC` includes an object iff its age is `>= SPEC`
//!   (it really is that old or older).
//! - `--newer-than SPEC` includes an object iff its age is `< SPEC`
//!   (it really is younger than that).
//!
//! Duration grammar is a custom extension of Go's `time.ParseDuration`
//! (`cmd/duration.go` in the mc source): `ns`, `us`/`µs`/`μs`, `ms`, `s`,
//! `m`, `h`, `d` (24h), `w` (7d), `y` (365d, no leap handling), fractional
//! values allowed (`1.5d`), a bare `"0"` is valid, empty string and unknown
//! units are errors. If duration parsing fails, the spec is retried against
//! a fixed set of absolute-date formats (mc's `rewindSupportedFormat`).

use anyhow::{Result, anyhow};
use chrono::{DateTime, NaiveDateTime, Utc};

/// A parsed `--older-than`/`--newer-than` spec: either a relative duration
/// or an absolute point in time (from the date-format fallback).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TimeRef {
    Duration(chrono::Duration),
    Absolute(DateTime<Utc>),
}

/// Parses mc's duration grammar by hand (no regex): repeatedly read a
/// `[0-9.]+` run, then a run of unit letters, multiply, and sum. A bare
/// `"0"` (no unit) is a special case straight out of Go's stdlib
/// `time.ParseDuration`, which mc's `ParseDuration` inherits.
pub(crate) fn parse_mc_duration(s: &str) -> Result<chrono::Duration> {
    if s.is_empty() {
        return Err(anyhow!("invalid duration {s}"));
    }
    if s == "0" {
        return Ok(chrono::Duration::zero());
    }

    let mut chars = s.chars().peekable();
    let negative = match chars.peek() {
        Some('-') => {
            chars.next();
            true
        }
        Some('+') => {
            chars.next();
            false
        }
        _ => false,
    };

    let mut total_ns: f64 = 0.0;
    let mut any_segment = false;
    while chars.peek().is_some() {
        let mut num_str = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '.' {
                num_str.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if num_str.is_empty() {
            return Err(anyhow!("invalid duration {s}"));
        }
        let mut unit_str = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '.' || c == '+' || c == '-' {
                break;
            }
            unit_str.push(c);
            chars.next();
        }
        if unit_str.is_empty() {
            return Err(anyhow!("invalid duration {s}"));
        }
        let value: f64 = num_str
            .parse()
            .map_err(|_| anyhow!("invalid duration {s}"))?;
        let unit_ns: f64 = match unit_str.as_str() {
            "ns" => 1.0,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60.0 * 1_000_000_000.0,
            "h" => 3_600.0 * 1_000_000_000.0,
            "d" => 24.0 * 3_600.0 * 1_000_000_000.0,
            "w" => 7.0 * 24.0 * 3_600.0 * 1_000_000_000.0,
            "y" => 365.0 * 24.0 * 3_600.0 * 1_000_000_000.0,
            other => return Err(anyhow!("unknown unit {other} in duration {s}")),
        };
        total_ns += value * unit_ns;
        any_segment = true;
    }
    if !any_segment {
        return Err(anyhow!("invalid duration {s}"));
    }
    if negative {
        total_ns = -total_ns;
    }
    Ok(chrono::Duration::nanoseconds(total_ns as i64))
}

/// mc's absolute-date fallback formats (`rewindSupportedFormat`,
/// `cmd/ls-main.go:118`), tried in order once duration parsing fails.
const ABSOLUTE_DATETIME_FORMATS: &[&str] = &["%Y.%m.%dT%H:%M:%S", "%Y.%m.%dT%H:%M"];

pub(crate) fn parse_time_ref(s: &str) -> Result<TimeRef> {
    if let Ok(d) = parse_mc_duration(s) {
        return Ok(TimeRef::Duration(d));
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y.%m.%d")
        && let Some(naive) = date.and_hms_opt(0, 0, 0)
    {
        return Ok(TimeRef::Absolute(naive.and_utc()));
    }
    for fmt in ABSOLUTE_DATETIME_FORMATS {
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(TimeRef::Absolute(naive.and_utc()));
        }
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(TimeRef::Absolute(dt.with_timezone(&Utc)));
    }
    // mc's `printDate` format ("2006-01-02 15:04:05 MST"); the zone
    // abbreviation is consumed but not resolved to a real offset here,
    // matching mc's own `time.Parse` (not `time.ParseInLocation`) fallback.
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S %Z") {
        return Ok(TimeRef::Absolute(naive.and_utc()));
    }
    Err(anyhow!("invalid time spec `{s}`"))
}

/// The threshold duration a spec resolves to, as of `now`. A duration spec
/// is used as-is; an absolute-date spec is converted to "how far in the
/// past that date is from `now`", so `--older-than 2020.01.01` behaves like
/// `--older-than <age of 2020.01.01>` (mc's `isOlder`/`isNewer` do the same
/// conversion internally before comparing against `objectAge`).
fn threshold(spec: &str, now: DateTime<Utc>) -> Result<chrono::Duration> {
    match parse_time_ref(spec)? {
        TimeRef::Duration(d) => Ok(d),
        TimeRef::Absolute(dt) => Ok(now - dt),
    }
}

/// `true` iff `object_time`'s age is `>= spec` (the object really is that
/// old or older) -- see module docs for the polarity note.
pub(crate) fn include_older_than(object_time: DateTime<Utc>, spec: &str) -> Result<bool> {
    let now = Utc::now();
    let age = now - object_time;
    Ok(age >= threshold(spec, now)?)
}

/// `true` iff `object_time`'s age is `< spec` (the object really is younger
/// than that) -- see module docs for the polarity note.
pub(crate) fn include_newer_than(object_time: DateTime<Utc>, spec: &str) -> Result<bool> {
    let now = Utc::now();
    let age = now - object_time;
    Ok(age < threshold(spec, now)?)
}

/// Validates `--older-than`/`--newer-than` spec strings up front, so a bad
/// grammar is a fatal error before any transfer/delete work starts (rather
/// than failing partway through, on the first object that needs it).
pub(crate) fn validate_time_filters(
    older_than: Option<&str>,
    newer_than: Option<&str>,
) -> Result<()> {
    if let Some(spec) = older_than {
        parse_time_ref(spec).map_err(|e| anyhow!("--older-than `{spec}`: {e}"))?;
    }
    if let Some(spec) = newer_than {
        parse_time_ref(spec).map_err(|e| anyhow!("--newer-than `{spec}`: {e}"))?;
    }
    Ok(())
}

/// Combines both filters (AND semantics, matching mc) into a single
/// inclusion check. An object with no known time (`object_time: None`)
/// cannot have its age evaluated, so it is always included -- this matches
/// mc, which always has a real `content.Time` to compare against, and
/// covers rs3's own `CommonPrefixes`/directory-marker entries that have no
/// real modification time.
pub(crate) fn passes_filters(
    object_time: Option<DateTime<Utc>>,
    older_than: Option<&str>,
    newer_than: Option<&str>,
) -> Result<bool> {
    let Some(t) = object_time else {
        return Ok(true);
    };
    if let Some(spec) = older_than
        && !include_older_than(t, spec)?
    {
        return Ok(false);
    }
    if let Some(spec) = newer_than
        && !include_newer_than(t, spec)?
    {
        return Ok(false);
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_grammar() {
        assert_eq!(
            parse_mc_duration("1d2h30m").unwrap().num_minutes(),
            24 * 60 + 150
        );
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

    #[test]
    fn duration_units_and_micro_variants() {
        assert_eq!(
            parse_mc_duration("1000ns").unwrap().num_microseconds(),
            Some(1)
        );
        assert_eq!(parse_mc_duration("1000us").unwrap().num_milliseconds(), 1);
        assert_eq!(
            parse_mc_duration("1000\u{b5}s").unwrap().num_milliseconds(),
            1
        );
        assert_eq!(
            parse_mc_duration("1000\u{3bc}s")
                .unwrap()
                .num_milliseconds(),
            1
        );
        assert_eq!(parse_mc_duration("1000ms").unwrap().num_seconds(), 1);
        assert_eq!(parse_mc_duration("1y").unwrap().num_days(), 365);
    }

    #[test]
    fn validate_rejects_bad_grammar_up_front() {
        assert!(validate_time_filters(Some("5x"), None).is_err());
        assert!(validate_time_filters(None, Some("bogus-not-a-date")).is_err());
        assert!(validate_time_filters(Some("7d"), Some("1h")).is_ok());
    }

    #[test]
    fn passes_filters_includes_unknown_time() {
        assert!(passes_filters(None, Some("7d"), None).unwrap());
        assert!(passes_filters(None, None, Some("7d")).unwrap());
    }

    #[test]
    fn passes_filters_ands_both_bounds() {
        let now = chrono::Utc::now();
        let three_days_old = now - chrono::Duration::days(3);
        // age 3d: older-than 1d (age>=1d, true) AND newer-than 7d (age<7d, true) => included
        assert!(passes_filters(Some(three_days_old), Some("1d"), Some("7d")).unwrap());
        // age 3d: older-than 5d (age>=5d, false) => excluded regardless of newer bound
        assert!(!passes_filters(Some(three_days_old), Some("5d"), Some("7d")).unwrap());
    }
}
