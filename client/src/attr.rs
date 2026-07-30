//! `--attr` metadata grammar, mirroring mc's hand-rolled parser
//! (`cmd/cp-main_contrib.go:31-138`, [SEM] §2).
//!
//! Format: `key1=value1;key2=value2;...`. Values may be `'single'` or
//! `"double"` quoted so they can contain a literal `;` or `=`; quotes are
//! stripped, not preserved. Keys are canonicalized to HTTP header case
//! (`Cache-Control`, `X-Amz-Meta-Foo`) -- `--attr` never auto-prefixes a bare
//! key with `X-Amz-Meta-`, the caller must spell it out if they want user
//! metadata under that wire name.
//!
//! This is a character-by-character state machine, not a naive `;`/`=`
//! split: a `KEY`/`VALUE` token state tracks which buffer is being built,
//! and a `Normal`/`Single('\'')`/`Double('"')` quote state tracks whether
//! `;` and `=` are structural or literal. The first `=` in a segment (while
//! outside quotes) switches `KEY` -> `VALUE`; a `;` outside quotes commits
//! the pending pair and resets to `KEY`. Errors (ported directly from mc's
//! `getMetaDataEntry`, `cmd/cp-main_contrib.go:63-131`): a `;` outside
//! quotes while still in `KEY` state (no `=` seen yet for this segment --
//! this also covers a *trailing* `;`, since it resets into a fresh, never
//! populated `KEY` segment); EOF while still inside an open quote; or EOF
//! while still in `KEY` state (covers both a non-empty pending key with no
//! `=`, and the empty segment left by a trailing `;`). EOF in `VALUE` state
//! always commits, even with an empty value (`"a="` is valid) -- mc commits
//! unconditionally on `pt == VALUE`, with no key-emptiness check.

use std::collections::BTreeMap;

use anyhow::{Result, anyhow};

const ERR_MSG: &str = "specified metadata should be of form key1=value1;key2=value2;... and so on";

#[derive(PartialEq, Eq, Clone, Copy)]
enum Token {
    Key,
    Value,
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Mode {
    Normal,
    Single,
    Double,
}

/// Canonicalizes a header key to `Http-Header-Case`: each `-`-separated
/// segment gets its first character upper-cased and the rest lower-cased
/// (matching Go's `http.CanonicalHeaderKey`, which mc's parser applies to
/// every key it stores -- [SEM] §2).
pub(crate) fn canonical_header_key(key: &str) -> String {
    key.split('-')
        .map(|seg| {
            let mut chars = seg.chars();
            match chars.next() {
                Some(first) => {
                    first.to_ascii_uppercase().to_string() + &chars.as_str().to_ascii_lowercase()
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join("-")
}

/// Parses an `--attr` value into a canonical-key metadata map.
pub(crate) fn parse_attrs(s: &str) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    let mut key = String::new();
    let mut value = String::new();
    let mut token = Token::Key;
    let mut mode = Mode::Normal;

    for c in s.chars() {
        match mode {
            Mode::Normal => match c {
                '\'' => mode = Mode::Single,
                '"' => mode = Mode::Double,
                '=' if token == Token::Key => token = Token::Value,
                ';' => {
                    // Unconditional per mc: `;` while still in KEY state is
                    // always malformed, regardless of whether any key chars
                    // were seen yet (so a trailing `;` after a well-formed
                    // pair is caught here too, once it resets to KEY and
                    // then hits EOF below).
                    if token == Token::Key {
                        return Err(anyhow!(ERR_MSG));
                    }
                    map.insert(canonical_header_key(&key), std::mem::take(&mut value));
                    key.clear();
                    token = Token::Key;
                }
                _ => {
                    if token == Token::Key {
                        key.push(c);
                    } else {
                        value.push(c);
                    }
                }
            },
            Mode::Single | Mode::Double => {
                let closing = if mode == Mode::Single { '\'' } else { '"' };
                if c == closing {
                    mode = Mode::Normal;
                } else if token == Token::Key {
                    key.push(c);
                } else {
                    value.push(c);
                }
            }
        }
    }

    // EOF while a quote was still open, or still in KEY state (no `=` ever
    // seen for the pending segment -- including an empty segment left by a
    // trailing `;`), is malformed. EOF in VALUE state always commits, even
    // with an empty value: mc has no key-emptiness check here either.
    if mode != Mode::Normal || token == Token::Key {
        return Err(anyhow!(ERR_MSG));
    }
    map.insert(canonical_header_key(&key), value);
    Ok(map)
}

/// Filesystem-attribute preservation for `--preserve`/`-a`
/// (`pkg/disk/stat_linux.go`, `client-fs.go:preserveAttributes`, [SEM] §2).
///
/// Encoded as a `/`-separated list of `key:value` pairs, sorted
/// alphabetically by field name: `atime:<sec>#<nsec>/gid:<gid>/mode:<st_mode
/// int>/mtime:<sec>#<nsec>/uid:<uid>`. mc also writes `gname`/`uname` when
/// `/etc/passwd`/`/etc/group` lookups succeed, and omits them on lookup
/// failure; rs3 omits them unconditionally, which is read-compatible with
/// mc-written values since `parseAttribute` treats missing fields as simply
/// absent.
#[cfg(unix)]
pub(crate) fn encode_fs_attrs(meta: &std::fs::Metadata) -> String {
    use std::os::unix::fs::MetadataExt;
    format!(
        "atime:{}#{}/gid:{}/mode:{}/mtime:{}#{}/uid:{}",
        meta.atime(),
        meta.atime_nsec(),
        meta.gid(),
        meta.mode(),
        meta.mtime(),
        meta.mtime_nsec(),
        meta.uid(),
    )
}

/// Parses a Go `strconv.ParseUint(val, 0, 32)`-style base-auto-detected
/// unsigned integer (mc's `preserveAttributes` parses `mode` this way):
/// `0x`/`0X` hex, `0o`/`0O` octal, `0b`/`0B` binary, a bare leading `0`
/// old-style octal, otherwise decimal.
#[cfg(unix)]
fn parse_uint_auto(s: &str) -> Option<u32> {
    if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u32::from_str_radix(rest, 16).ok()
    } else if let Some(rest) = s.strip_prefix("0o").or_else(|| s.strip_prefix("0O")) {
        u32::from_str_radix(rest, 8).ok()
    } else if let Some(rest) = s.strip_prefix("0b").or_else(|| s.strip_prefix("0B")) {
        u32::from_str_radix(rest, 2).ok()
    } else if s.len() > 1 && s.starts_with('0') {
        u32::from_str_radix(&s[1..], 8).ok()
    } else {
        s.parse::<u32>().ok()
    }
}

/// Parses a `<sec>` or `<sec>#<nsec>` timestamp field (`cmd/utils.go:
/// parseAtimeMtime`) into a [`std::time::SystemTime`].
#[cfg(unix)]
fn parse_sec_nsec(v: &str) -> Option<std::time::SystemTime> {
    let (sec, nsec) = match v.split_once('#') {
        Some((s, n)) => (s.parse::<i64>().ok()?, n.parse::<u32>().ok()?),
        None => (v.parse::<i64>().ok()?, 0),
    };
    let base = std::time::SystemTime::UNIX_EPOCH;
    if sec >= 0 {
        base.checked_add(std::time::Duration::new(sec as u64, nsec))
    } else {
        base.checked_sub(std::time::Duration::new((-sec) as u64, 0))
            .and_then(|t| t.checked_add(std::time::Duration::new(0, nsec)))
    }
}

/// Applies a `--preserve`-encoded attribute string to `path`: mode via
/// chmod, uid/gid via chown, atime/mtime via `File::set_times`. Every field
/// is independently best-effort -- a malformed value, an absent field, or a
/// permission failure on the underlying syscall is skipped rather than
/// failing the whole call, matching mc's `preserveAttributes` (mode/uid/gid
/// default to "leave unchanged" on parse failure, and mc's own
/// `probe.Error` from these calls is logged, not fatal, at the call site).
///
/// Order matters: atime/mtime are applied *before* mode. Setting times
/// needs the file opened `write(true)`, and a preserved read-only mode
/// (e.g. `0o444`) would make that open fail (`EACCES`) if mode were
/// applied first -- silently dropping the timestamp restore along with it,
/// since every field here is best-effort. Applying times while the file
/// still has its original (writable-by-us) permissions, then chmod-ing
/// last, avoids that ordering trap.
#[cfg(unix)]
pub(crate) fn apply_fs_attrs(path: &std::path::Path, encoded: &str) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut fields: BTreeMap<&str, &str> = BTreeMap::new();
    for part in encoded.split('/') {
        if part.is_empty() {
            continue;
        }
        match part.split_once(':') {
            Some((k, v)) => {
                fields.insert(k, v);
            }
            None => {
                fields.insert(part, "");
            }
        }
    }

    let atime = fields.get("atime").and_then(|v| parse_sec_nsec(v));
    let mtime = fields.get("mtime").and_then(|v| parse_sec_nsec(v));
    if atime.is_some() || mtime.is_some() {
        if let Ok(file) = std::fs::OpenOptions::new().write(true).open(path) {
            let mut times = std::fs::FileTimes::new();
            if let Some(t) = atime {
                times = times.set_accessed(t);
            }
            if let Some(t) = mtime {
                times = times.set_modified(t);
            }
            let _ = file.set_times(times);
        }
    }

    if let Some(mode) = fields.get("mode").and_then(|v| parse_uint_auto(v)) {
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode & 0o7777));
    }

    let uid = fields.get("uid").and_then(|v| v.parse::<u32>().ok());
    let gid = fields.get("gid").and_then(|v| v.parse::<u32>().ok());
    if uid.is_some() || gid.is_some() {
        let _ = std::os::unix::fs::chown(path, uid, gid);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn canonicalizes_x_amz_meta_prefix() {
        let m = parse_attrs("x-amz-meta-color=red").unwrap();
        assert_eq!(m["X-Amz-Meta-Color"], "red");
    }

    #[test]
    fn error_message_matches_mc_exactly() {
        let err = parse_attrs("noequals").unwrap_err();
        assert_eq!(
            err.to_string(),
            "specified metadata should be of form key1=value1;key2=value2;... and so on"
        );
    }

    #[test]
    fn empty_value_is_allowed() {
        let m = parse_attrs("key1=").unwrap();
        assert_eq!(m["Key1"], "");
    }

    #[test]
    fn trailing_semicolon_is_rejected() {
        // mc's EOF check fires while `pt == KEY` -- a trailing `;` resets
        // into a fresh KEY segment, so EOF right after it is an error, even
        // though the pair before the `;` was well-formed.
        assert!(parse_attrs("key1=value1;").is_err());
    }

    #[test]
    fn key_only_segment_is_rejected() {
        // mc treats `;` while `pt == KEY` as unconditionally malformed --
        // "abc" never saw a `=`, so hitting `;` before one is an error, even
        // though a well-formed segment (`def=ghi`) follows.
        assert!(parse_attrs("abc;def=ghi").is_err());
    }

    #[test]
    fn multiple_pairs() {
        let m = parse_attrs("a=1;b=2;c=3").unwrap();
        assert_eq!(m["A"], "1");
        assert_eq!(m["B"], "2");
        assert_eq!(m["C"], "3");
    }

    #[test]
    fn extra_equals_in_value_is_literal() {
        let m = parse_attrs("a=b=c=d").unwrap();
        assert_eq!(m["A"], "b=c=d");
    }

    #[cfg(unix)]
    #[test]
    fn encode_fs_attrs_contains_mode_and_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.txt");
        std::fs::write(&path, b"hello").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        let encoded = encode_fs_attrs(&meta);
        assert!(encoded.contains("mode:"), "encoded: {encoded}");
        assert!(encoded.contains("mtime:"), "encoded: {encoded}");
        assert!(encoded.contains("atime:"), "encoded: {encoded}");
        assert!(encoded.contains("uid:"), "encoded: {encoded}");
        assert!(encoded.contains("gid:"), "encoded: {encoded}");
    }

    #[cfg(unix)]
    #[test]
    fn apply_fs_attrs_roundtrips_mode() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"hello").unwrap();
        std::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o600)).unwrap();
        let src_meta = std::fs::metadata(&src).unwrap();
        let encoded = encode_fs_attrs(&src_meta);

        let dst = dir.path().join("dst.txt");
        std::fs::write(&dst, b"world").unwrap();
        std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o644)).unwrap();

        apply_fs_attrs(&dst, &encoded).unwrap();

        let dst_meta = std::fs::metadata(&dst).unwrap();
        assert_eq!(dst_meta.permissions().mode() & 0o777, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn apply_fs_attrs_parses_octal_and_hex_mode() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("dst.txt");
        std::fs::write(&dst, b"world").unwrap();

        apply_fs_attrs(&dst, "mode:0640").unwrap();
        let dst_meta = std::fs::metadata(&dst).unwrap();
        assert_eq!(dst_meta.permissions().mode() & 0o777, 0o640);

        apply_fs_attrs(&dst, "mode:0x1A4").unwrap(); // 0x1A4 == 0644
        let dst_meta = std::fs::metadata(&dst).unwrap();
        assert_eq!(dst_meta.permissions().mode() & 0o777, 0o644);
    }

    // Regression test: a preserved read-only mode (0o444) must not prevent
    // atime/mtime from being restored. If mode were applied before times,
    // the `File::options().write(true).open(path)` needed to set times
    // would fail with EACCES against the now-read-only file -- and since
    // every field here is independently best-effort (by design, matching
    // mc), that failure is silently swallowed, so the whole call still
    // returns `Ok(())` while quietly dropping the timestamp restore. Order
    // must be times-then-mode so the file is still writable-by-us when
    // times are applied.
    #[cfg(unix)]
    #[test]
    fn apply_fs_attrs_restores_mtime_even_with_readonly_mode() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.txt");
        std::fs::write(&src, b"hello").unwrap();
        // Pin the source's mtime to a value far from "now" (rather than
        // relying on its just-created timestamp), so this test can't pass
        // by coincidence: dst is also created moments later in this same
        // test, and on a coarse-grained fs clock the two "just created"
        // timestamps could land in the same tick even without the code
        // under test ever actually applying anything.
        let pinned =
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::new(1_600_000_000, 123_000);
        let file = std::fs::OpenOptions::new().write(true).open(&src).unwrap();
        let times = std::fs::FileTimes::new()
            .set_modified(pinned)
            .set_accessed(pinned);
        file.set_times(times).unwrap();
        drop(file);
        std::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o444)).unwrap();
        let src_meta = std::fs::metadata(&src).unwrap();
        let encoded = encode_fs_attrs(&src_meta);

        let dst = dir.path().join("dst.txt");
        std::fs::write(&dst, b"world").unwrap();
        std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o644)).unwrap();

        apply_fs_attrs(&dst, &encoded).unwrap();

        let dst_meta = std::fs::metadata(&dst).unwrap();
        assert_eq!(
            dst_meta.permissions().mode() & 0o777,
            0o444,
            "mode should still apply"
        );
        assert_eq!(
            dst_meta.modified().unwrap(),
            pinned,
            "mtime must be restored even though the encoded mode is read-only"
        );
    }
}
