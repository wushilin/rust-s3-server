//! Layout 3 — the layout this build writes. One directory per key, one
//! directory per version inside it:
//!
//! ```text
//! objects/<4hex fanout>/<40hex sha1(key)>/<stamp dir>/meta.json
//! objects/<4hex fanout>/<40hex sha1(key)>/<stamp dir>/part.N
//! ```
//!
//! The stamp dir is the version's stamp zero-padded so a plain sorted
//! listing is version order: `<ms:019>.<c:010>.<site>`. It is also the
//! version id clients see. `meta.json` is format 2: every blob carries its
//! stamp and `versioned` bit; a delete marker is a stamp dir whose
//! `meta.json` says `delete_marker` (no parts). Every version of a key,
//! current or not, live or marker, sits beside the others, so a rebuild
//! recovers versions and deletes from the tree alone.
//!
//! Once this layout has shipped, this file is sealed: a change in shape is
//! layout 4.

use super::{components, is_hex, LayoutSpec};
use crate::storage::encoding::{fanout_segment, key_hash};
use crate::storage::stamp::Stamp;

pub const VERSION: u32 = 3;

pub const SPEC: LayoutSpec = LayoutSpec {
    version: VERSION,
    meta_format: 2,
    summary: "per-key dirs objects/<4hex>/<sha1(key)>/<stamp>, meta.json format 2 (stamps, versions, delete markers)",
    is_blob_path,
};

/// `objects/<4hex>/<40hex>/<stamp dir>`.
pub fn is_blob_path(blob_rel: &str) -> bool {
    let c = components(blob_rel);
    c.len() == 4 && c[0] == "objects" && is_hex(c[1], 4) && is_hex(c[2], 40) && parse_stamp_dir(c[3]).is_some()
}

/// The key's directory, relative to the bucket dir.
pub fn key_dir_rel(bucket: &str, key: &str) -> String {
    format!("objects/{}/{}", fanout_segment(bucket, key), key_hash(key))
}

/// The blob dir of version `stamp` of `key`, relative to the bucket dir.
pub fn blob_rel(bucket: &str, key: &str, stamp: &Stamp) -> String {
    format!("{}/{}", key_dir_rel(bucket, key), stamp_dir(stamp))
}

/// The version directory name (and version id) of a stamp: zero-padded so
/// lexical order is stamp order.
pub fn stamp_dir(stamp: &Stamp) -> String {
    format!("{:019}.{:010}.{}", stamp.ms.max(0), stamp.c, stamp.site)
}

/// Parses a version directory name back into its stamp.
pub fn parse_stamp_dir(name: &str) -> Option<Stamp> {
    let mut parts = name.splitn(3, '.');
    let (ms, c, site) = (parts.next()?, parts.next()?, parts.next()?);
    if ms.len() != 19 || c.len() != 10 || !ms.bytes().all(|b| b.is_ascii_digit()) || !c.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    crate::storage::stamp::validate_site_id(site).ok()?;
    Some(Stamp {
        ms: ms.parse().ok()?,
        c: c.parse().ok()?,
        site: site.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamp_dirs_round_trip_and_sort_in_stamp_order() {
        let a = Stamp { ms: 1_791_900_298_853, c: 0, site: "bd7e41df94febe9a".into() };
        let b = Stamp { ms: 1_791_900_298_853, c: 7, site: "bd7e41df94febe9a".into() };
        let c = Stamp { ms: 1_791_900_298_854, c: 0, site: "0000000000000001".into() };
        for s in [&a, &b, &c] {
            assert_eq!(parse_stamp_dir(&stamp_dir(s)).as_ref(), Some(s));
        }
        assert!(stamp_dir(&a) < stamp_dir(&b));
        assert!(stamp_dir(&b) < stamp_dir(&c));
        assert!(parse_stamp_dir("V1AB3F7C_9F00").is_none());
        assert!(parse_stamp_dir("1791900298853.0.bd7e41df94febe9a").is_none(), "unpadded header form is not a dir name");
        let rel = blob_rel("bucket", "a/b.txt", &a);
        assert!(is_blob_path(&rel), "{rel}");
        assert!(rel.starts_with(&key_dir_rel("bucket", "a/b.txt")));
    }
}
