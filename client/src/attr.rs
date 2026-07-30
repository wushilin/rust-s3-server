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
}
