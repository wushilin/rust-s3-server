//! `find` -- mc-shaped matchers, action-flag token substitution, and
//! `--exec`. Semantics normative per
//! `docs/superpowers/research/mc-research-semantics.md` §7, with several
//! corrections below ground-truth-verified against the real `mc` binary
//! (RELEASE.2025-08-13) where the written doc's summary turned out to be
//! incomplete:
//!
//! - **`--maxdepth` is *not* purely a display-only truncation.** The doc
//!   claims it "does NOT filter or stop matching", but a live `mc find
//!   --maxdepth N --name ...` run shows the opposite: `mc` truncates
//!   `fileContent.Key` itself (via `getAliasedPath`) *before* `matchFind`
//!   ever runs, and *before* the action's token substitution too -- so
//!   `--name`/`--path`/`--regex`/`{}`/`{base}`/`{dir}` (and even the
//!   `--json` `"key"` field) all see the truncated key, while
//!   `--larger`/`--smaller`/`--older-than`/`--newer-than` are untouched
//!   (they read `fileContent.Size`/`.Time` directly, never `.Key`). This
//!   module reproduces that mutate-then-match behavior (see
//!   [`truncate_for_maxdepth`] and its callers in [`run_find`]), not the
//!   doc's "display only" description.
//! - The exact truncation arithmetic (`kept = root_parts + maxdepth - 1`,
//!   `maxdepth == 0` meaning "disabled") was reverse-engineered from
//!   several `--maxdepth N` runs against a real target with a known
//!   directory depth; see [`truncate_for_maxdepth`].
//! - `find --json`'s `ContentMessage` has `"type":""` and `"etag":""` and
//!   omits `"storageClass"` entirely for every match, even though `ls
//!   --json` against the very same objects populates all three. `find`'s
//!   own listing evidently never fetches that metadata, so this module's
//!   `ContentMessage`s are built the same threadbare way (see
//!   [`to_content_message`]) rather than trying to backfill real values.
//! - `--json` mode silently ignores `--print` (falls back to the default
//!   `ContentMessage` line) but still fully honors `--exec` -- see the
//!   action dispatch in [`run_find`].
//! - On a failing `--exec`, the child's *captured* stdout is discarded
//!   (never shown) and only its stderr is forwarded, to rs3's own stdout
//!   (not stderr) -- confirmed by piping each stream separately against
//!   the real binary. See [`run_exec`].
//! - **`--exec` splits the raw template into words *before* substituting
//!   tokens, not after.** Both the doc and this task's original brief
//!   described substitute-then-split; ground truth is the opposite (see
//!   [`run_exec`]'s doc comment for the two probes -- a spacey key and an
//!   unbalanced-quote key -- that only agree with real `mc` under
//!   split-then-substitute). `--print` is unaffected: its output is never
//!   tokenized at all, only ever the raw substituted string.

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use regex::Regex;
use std::io::Write;

use crate::FindArgs;
use crate::config::client_for_alias;
use crate::messages::ContentMessage;
use crate::output::{McMessage, humanize_ibytes, out, print_date, print_msg};
use crate::timefilter::{include_newer_than, include_older_than, validate_time_filters};
use crate::urls::{is_s3_url, parse_s3_url};

/// A single `[...]` character-class match against `c`, honoring `!`/`^`
/// negation and `a-z` ranges (`filepath.Match`'s class syntax). Returns
/// `None` if `class` (the slice *between* the brackets) is malformed in a
/// way that can't reasonably match anything.
fn char_class_matches(class: &[char], c: char) -> bool {
    let (negate, class) = match class.first() {
        Some('!') | Some('^') => (true, &class[1..]),
        _ => (false, class),
    };
    let mut matched = false;
    let mut i = 0;
    while i < class.len() {
        if i + 2 < class.len() && class[i + 1] == '-' {
            if c >= class[i] && c <= class[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if c == class[i] {
                matched = true;
            }
            i += 1;
        }
    }
    matched != negate
}

/// Hand-rolled recursive glob matcher supporting `*` (any run of any
/// characters, `/` included -- this is always "flat" wildcard semantics;
/// see module docs / [SEM] §7's trap (b)), `?` (exactly one character),
/// and `[...]` character classes ([SEM] §7's `--name` trap (a) references
/// `filepath.Match`'s class syntax). Used for both `--name`'s basename
/// glob (where flatness is moot -- a basename never contains `/`) and
/// `--path`/`--ignore`'s full-relative-path flat match, so one
/// implementation covers both.
pub(crate) fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    wildcard_match_inner(&p, &t)
}

fn wildcard_match_inner(p: &[char], t: &[char]) -> bool {
    if p.is_empty() {
        return t.is_empty();
    }
    match p[0] {
        '*' => {
            wildcard_match_inner(&p[1..], t) || (!t.is_empty() && wildcard_match_inner(p, &t[1..]))
        }
        '?' => !t.is_empty() && wildcard_match_inner(&p[1..], &t[1..]),
        '[' => match p.iter().position(|&c| c == ']').filter(|&i| i > 0) {
            Some(close) => {
                !t.is_empty()
                    && char_class_matches(&p[1..close], t[0])
                    && wildcard_match_inner(&p[close + 1..], &t[1..])
            }
            // No closing `]` found: treat `[` as a literal character.
            None => !t.is_empty() && t[0] == '[' && wildcard_match_inner(&p[1..], &t[1..]),
        },
        c => !t.is_empty() && t[0] == c && wildcard_match_inner(&p[1..], &t[1..]),
    }
}

/// `--name PATTERN`: [SEM] §7 trap (a). First try `PATTERN` as a glob
/// against the *basename* of `relative_path`; if that fails, fall back to
/// a plain exact-equality scan over every `/`-separated component of
/// `relative_path` (not a glob -- a literal `==`).
pub(crate) fn name_match(pattern: &str, relative_path: &str) -> bool {
    let basename = relative_path.rsplit('/').next().unwrap_or(relative_path);
    if wildcard_match(pattern, basename) {
        return true;
    }
    relative_path
        .split('/')
        .any(|component| component == pattern)
}

/// `--path`/`--ignore PATTERN`: [SEM] §7 trap (b) -- flat wildcard match
/// (`*` crosses `/`) against the *full* relative path, not just the
/// basename.
pub(crate) fn path_match(pattern: &str, relative_path: &str) -> bool {
    wildcard_match(pattern, relative_path)
}

/// `--larger`/`--smaller SIZE`: metric (`k`/`m`/`g`/`t`, 1000-based) and
/// IEC (`ki`/`mi`/`gi`/`ti`, 1024-based) units, case-insensitive, an
/// optional trailing `b`, no suffix meaning plain bytes.
pub(crate) fn parse_find_size(input: &str) -> Result<u64> {
    let s = input.trim();
    if s.is_empty() {
        return Err(anyhow!("invalid size ``"));
    }
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (num_str, unit_str) = s.split_at(split);
    if num_str.is_empty() {
        return Err(anyhow!("invalid size `{s}`"));
    }
    let value: f64 = num_str.parse().map_err(|_| anyhow!("invalid size `{s}`"))?;
    let mut unit = unit_str.to_ascii_lowercase();
    if let Some(stripped) = unit.strip_suffix('b') {
        unit = stripped.to_string();
    }
    let multiplier: f64 = match unit.as_str() {
        "" => 1.0,
        "k" => 1_000.0,
        "m" => 1_000_000.0,
        "g" => 1_000_000_000.0,
        "t" => 1_000_000_000_000.0,
        "ki" => 1024.0,
        "mi" => 1024.0 * 1024.0,
        "gi" => 1024.0 * 1024.0 * 1024.0,
        "ti" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        other => return Err(anyhow!("unsupported size unit `{other}` in `{s}`")),
    };
    Ok((value * multiplier).round() as u64)
}

/// The `{"..."}`-quoted token variants wrap the substituted value with
/// JSON-string escaping (`strconv.Quote` in real `mc`; `serde_json`'s
/// string serialization is the documented rs3 equivalent).
fn quote(s: &str) -> String {
    serde_json::to_string(s).expect("string serialization of a &str never fails")
}

/// Substitutes `find`'s action-flag tokens into `template`
/// (`--exec`/`--print`'s argument), in the exact order [SEM] §7
/// documents. `key` is the (already maxdepth-truncated, per
/// [`truncate_for_maxdepth`]) aliased path used for `{}`/`{base}`/`{dir}`
/// -- ground-truth-verified against real `mc`, `{base}`/`{dir}` derive
/// from this *same* key, not a separately-relative path (`_rel_key` is
/// accepted for signature symmetry with [`run_find`]'s per-match
/// `(key, rel)` pair but is not itself a substitution source: its
/// basename is always identical to `key`'s, since it is a path suffix of
/// `key`, and real `mc`'s `{dir}` is verified to use the full/aliased
/// key's dirname rather than a target-relative one).
pub(crate) fn substitute_tokens(
    template: &str,
    key: &str,
    _rel_key: &str,
    size: u64,
    time: DateTime<Utc>,
) -> String {
    let base = key.rsplit('/').next().unwrap_or(key);
    let dir = match key.rfind('/') {
        Some(i) => &key[..i],
        None => ".",
    };
    let size_str = humanize_ibytes(size);
    let time_str = print_date(time);

    let mut rendered = template.to_string();
    rendered = rendered.replace("{}", key);
    rendered = rendered.replace("{\"\"}", &quote(key));
    rendered = rendered.replace("{base}", base);
    rendered = rendered.replace("{\"base\"}", &quote(base));
    rendered = rendered.replace("{dir}", dir);
    rendered = rendered.replace("{\"dir\"}", &quote(dir));
    rendered = rendered.replace("{size}", &size_str);
    rendered = rendered.replace("{\"size\"}", &quote(&size_str));
    rendered = rendered.replace("{time}", &time_str);
    rendered = rendered.replace("{\"time\"}", &quote(&time_str));
    rendered
}

/// `{url}`/`{"url"}`/`{version}`/`{"version"}` require a presigned-URL
/// call (`share`'s machinery, Task 15) that isn't wired up yet; hard-error
/// rather than silently leaving the literal token in place.
fn reject_unsupported_tokens(template: &str) -> Result<()> {
    for tok in ["{url}", "{\"url\"}", "{version}", "{\"version\"}"] {
        if template.contains(tok) {
            return Err(anyhow!("find {tok} is not implemented yet"));
        }
    }
    Ok(())
}

/// Truncates `full_key` (the untruncated `alias/bucket/objectKey` path) to
/// the printed/matched form `--maxdepth N` produces, ground-truth-derived
/// (see module docs) from several `mc find --maxdepth N` runs against
/// targets with a known root depth:
///
/// - `maxdepth == 0` is mc's "unset" sentinel (also its CLI default) --
///   truncation is disabled entirely, `full_key` is returned unchanged.
/// - Otherwise, `kept = root_parts + (maxdepth - 1)` `/`-separated
///   components of `full_key` (where `root_parts` is the component count
///   of the search target's own aliased path, e.g. `alias/bucket/prefix`
///   with any trailing `/` trimmed) are kept; if that's `>=` the key's
///   total component count the key is already short enough and is
///   returned unchanged, otherwise the kept components are rejoined with
///   a forced trailing `/` (matching a truncated result always looking
///   like a "directory").
fn truncate_for_maxdepth(full_key: &str, root_parts: usize, maxdepth: u32) -> String {
    if maxdepth == 0 {
        return full_key.to_string();
    }
    let parts: Vec<&str> = full_key.split('/').collect();
    let kept = root_parts + (maxdepth as usize - 1);
    if kept >= parts.len() {
        full_key.to_string()
    } else {
        format!("{}/", parts[..kept].join("/"))
    }
}

/// `find`'s default-action message: `contentMessage` embedded verbatim for
/// `JSON()` (see module docs -- `find --json` is byte-identical to
/// `ls --json`'s shape, modulo `find`'s own threadbare field population),
/// but `String()` prints only the bare key (optionally with `` (versionId)
/// `` -- `--versions` isn't implemented, so that suffix never applies
/// here).
struct FindMessage(ContentMessage);

impl McMessage for FindMessage {
    fn human(&self) -> String {
        self.0.key.clone()
    }

    fn json(&self) -> serde_json::Value {
        self.0.json()
    }
}

/// `find --json`'s `ContentMessage` never has real type/etag/storageClass
/// data (see module docs) -- only `key`/`size`/`lastModified` are ever
/// populated from the actual object.
fn to_content_message(key: &str, size: u64, time: DateTime<Utc>) -> ContentMessage {
    ContentMessage {
        status: "success".into(),
        filetype: String::new(),
        time,
        size,
        key: key.to_string(),
        etag: String::new(),
        storage_class: None,
    }
}

/// `find --exec`: tokenizes the **raw, un-substituted** `--exec` template
/// with `shell-words` first, then substitutes tokens into each
/// already-isolated word (no shell interposed -- ground-truth-verified: a
/// literal `>` in the command line is passed through as a plain argv token,
/// never interpreted as a redirection), and runs it via
/// `std::process::Command`.
///
/// **Split-before-substitute, not substitute-before-split.**
/// Ground-truth-verified against real `mc` (RELEASE.2025-08-13) with two
/// probes that only disagree between the two orderings: a key containing a
/// space (`sp file.txt`) reaches the child as a single argv word under
/// real `mc` (splitting the *template* `"script {}"` gives `["script",
/// "{}"]`, and only then does `{}` get replaced with the literal,
/// unsplit key); substituting first and re-splitting the result would
/// instead hand the child two words. Symmetrically, a key containing an
/// unbalanced double quote (`unbal"file.txt`) runs *without error* under
/// real `mc` -- substitute-then-split would feed shell-words a string with
/// a stray quote and abort the whole find run on a shell-words parse
/// error, which is not what happens. `mc-research-semantics.md` §7
/// (and this task's original brief) both described the substitute-then-
/// split order; this is corrected to split-then-substitute per the real
/// binary. `--print`'s output, by contrast, is never tokenized at all --
/// ground-truth-verified to render the raw substituted string byte-for-
/// byte (including any spaces/quotes from the key) -- so only `--exec`
/// needed this fix.
///
/// On success the child's captured stdout is forwarded to rs3's own
/// stdout; on failure only its stderr is forwarded (its stdout is
/// discarded, ground-truth-verified) and the whole process exits
/// immediately with the child's exit code, aborting the rest of the find
/// loop.
fn run_exec(
    template: &str,
    key: &str,
    rel_key: &str,
    size: u64,
    time: DateTime<Utc>,
) -> Result<()> {
    let raw_words = shell_words::split(template)
        .map_err(|e| anyhow!("find --exec: invalid command `{template}`: {e}"))?;
    let tokens: Vec<String> = raw_words
        .iter()
        .map(|word| substitute_tokens(word, key, rel_key, size, time))
        .collect();
    let Some((prog, rest)) = tokens.split_first() else {
        return Err(anyhow!("find --exec: empty command"));
    };
    let output = std::process::Command::new(prog)
        .args(rest)
        .output()
        .map_err(|e| anyhow!("find --exec: failed to spawn `{prog}`: {e}"))?;
    if output.status.success() {
        let _ = std::io::stdout().write_all(&output.stdout);
        Ok(())
    } else {
        let mut stdout = std::io::stdout();
        let _ = stdout.write_all(&output.stderr);
        let _ = stdout.flush();
        let code = output.status.code().unwrap_or(1);
        std::process::exit(code);
    }
}

pub(crate) async fn run_find(args: FindArgs) -> Result<()> {
    if let Some(template) = &args.exec {
        reject_unsupported_tokens(template)?;
    }
    if let Some(template) = &args.print {
        reject_unsupported_tokens(template)?;
    }
    // rs3 has no local-filesystem `find`; only S3 targets are supported.
    if !is_s3_url(&args.target) {
        return Err(anyhow!("find on local filesystems is not implemented yet"));
    }
    let parsed = parse_s3_url(&args.target)?;
    let bucket = parsed
        .bucket
        .clone()
        .ok_or_else(|| anyhow!("find: target must be ALIAS/BUCKET[/PREFIX]"))?;
    let prefix = parsed.key.clone().unwrap_or_default();

    validate_time_filters(args.older_than.as_deref(), args.newer_than.as_deref())?;
    let larger = args.larger.as_deref().map(parse_find_size).transpose()?;
    let smaller = args.smaller.as_deref().map(parse_find_size).transpose()?;
    let regex = args
        .regex
        .as_deref()
        .map(Regex::new)
        .transpose()
        .map_err(|e| anyhow!("find --regex: {e}"))?;

    let (client, _) = client_for_alias(&parsed.alias).await?;

    let root_raw = if prefix.is_empty() {
        format!("{}/{bucket}", parsed.alias)
    } else {
        format!("{}/{bucket}/{}", parsed.alias, prefix.trim_end_matches('/'))
    };
    let root_parts = root_raw.split('/').count();
    let root_with_slash = format!("{root_raw}/");
    let maxdepth = args.maxdepth.unwrap_or(0);

    let ui = crate::progress::worker_ui();
    let budget = crate::budget::StreamBudget::new(5);
    let objects =
        crate::list::collect_objects_with(&client, &bucket, &prefix, Some((&budget, ui.as_ref())))
            .await?;
    for obj in objects {
        // Real `mc` unconditionally skips `StorageClass == "GLACIER"`
        // objects before any filter ([SEM] §7); rs3's own listing helper
        // (`collect_objects`) doesn't currently surface storage class
        // (`find`'s own JSON output doesn't expose it either, see module
        // docs), and rs3-backed buckets never produce GLACIER objects, so
        // there is nothing to check here yet -- documented as a known,
        // currently-unreachable scope cut rather than silently dropped.

        let full_key = format!("{}/{bucket}/{}", parsed.alias, obj.key);
        let effective_key = truncate_for_maxdepth(&full_key, root_parts, maxdepth);
        let relative = effective_key
            .strip_prefix(&root_with_slash)
            .unwrap_or(&effective_key);

        if let Some(pattern) = &args.ignore
            && path_match(pattern, relative)
        {
            continue;
        }
        if let Some(pattern) = &args.name
            && !name_match(pattern, relative)
        {
            continue;
        }
        if let Some(pattern) = &args.path
            && !path_match(pattern, relative)
        {
            continue;
        }
        if let Some(re) = &regex
            && !re.is_match(relative)
        {
            continue;
        }
        if let Some(threshold) = larger
            && obj.size <= threshold
        {
            continue;
        }
        if let Some(threshold) = smaller
            && obj.size >= threshold
        {
            continue;
        }
        let time = obj.modified.unwrap_or_else(Utc::now);
        if let Some(spec) = args.older_than.as_deref()
            && !include_older_than(time, spec)?
        {
            continue;
        }
        if let Some(spec) = args.newer_than.as_deref()
            && !include_newer_than(time, spec)?
        {
            continue;
        }

        if let Some(exec_template) = &args.exec {
            run_exec(exec_template, &effective_key, relative, obj.size, time)?;
        } else if let Some(print_template) = &args.print {
            // Ground-truth-verified: `--json` silently ignores `--print`
            // and falls back to the default `ContentMessage` line (module
            // docs); `--print` only ever affects human-mode output.
            if out().json {
                print_msg(&FindMessage(to_content_message(
                    &effective_key,
                    obj.size,
                    time,
                )));
            } else {
                let rendered =
                    substitute_tokens(print_template, &effective_key, relative, obj.size, time);
                println!("{rendered}");
            }
        } else {
            print_msg(&FindMessage(to_content_message(
                &effective_key,
                obj.size,
                time,
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_time() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn name_glob_and_component_fallback() {
        assert!(name_match("*.txt", "a/b/note.txt"));
        assert!(!name_match("*.txt", "a/b/note.rs"));
        assert!(
            name_match("foo", "a/foo/bar.rs"),
            "component exact-match fallback"
        );
        assert!(
            !name_match("f*o", "a/foo/bar.rs"),
            "fallback is exact, not glob"
        );
    }

    #[test]
    fn path_wildcard_is_flat() {
        assert!(
            path_match("a/*.txt", "a/b/c.txt"),
            "* crosses / in mc wildcard"
        );
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
        let s = substitute_tokens(
            "echo {} {base} {\"size\"}",
            "test/b/a/f.txt",
            "a/f.txt",
            1024,
            sample_time(),
        );
        assert_eq!(s, "echo test/b/a/f.txt f.txt \"1.0KiB\"");
    }

    #[test]
    fn name_glob_char_class_and_question_mark() {
        assert!(name_match("note.??t", "a/note.txt"));
        assert!(name_match("[nN]ote.txt", "a/note.txt"));
        assert!(!name_match("[!n]ote.txt", "a/note.txt"));
    }

    #[test]
    fn quoted_token_variants_use_json_escaping() {
        let s = substitute_tokens(
            "{\"\"} {\"base\"} {\"dir\"} {\"time\"}",
            "a/b \"c\".txt",
            "b \"c\".txt",
            0,
            sample_time(),
        );
        assert!(s.starts_with("\"a/b \\\"c\\\".txt\""), "got: {s}");
    }

    #[test]
    fn maxdepth_zero_is_disabled_sentinel() {
        assert_eq!(
            truncate_for_maxdepth("alias/bucket/a/foo/note.txt", 2, 0),
            "alias/bucket/a/foo/note.txt"
        );
    }

    #[test]
    fn maxdepth_truncates_kept_components_below_root() {
        // Ground-truth: target `alias/bucket` (root_parts=2), maxdepth=1
        // keeps 0 relative components (just the root); maxdepth=2 keeps 1
        // ("a/"); a maxdepth large enough to cover the whole key leaves it
        // untouched.
        let key = "alias/bucket/a/foo/note.txt";
        assert_eq!(truncate_for_maxdepth(key, 2, 1), "alias/bucket/");
        assert_eq!(truncate_for_maxdepth(key, 2, 2), "alias/bucket/a/");
        assert_eq!(truncate_for_maxdepth(key, 2, 4), key);
        assert_eq!(truncate_for_maxdepth(key, 2, 100), key);
    }

    #[test]
    fn maxdepth_root_parts_account_for_target_prefix() {
        // Ground-truth: target `alias/bucket/a` (root_parts=3).
        let key = "alias/bucket/a/foo/note.txt";
        assert_eq!(truncate_for_maxdepth(key, 3, 1), "alias/bucket/a/");
        assert_eq!(truncate_for_maxdepth(key, 3, 3), key);
    }

    #[test]
    fn rejects_url_and_version_tokens() {
        assert!(reject_unsupported_tokens("{url}").is_err());
        assert!(reject_unsupported_tokens("{\"url\"}").is_err());
        assert!(reject_unsupported_tokens("{version}").is_err());
        assert!(reject_unsupported_tokens("{\"version\"}").is_err());
        assert!(reject_unsupported_tokens("{} {base}").is_ok());
    }
}
