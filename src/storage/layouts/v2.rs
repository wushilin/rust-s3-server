//! Layout 2 — SEALED. The single-level fanout:
//! `objects/<4hex>/V1XXXXXX_YYYY` (one four-hex-char level from the hash of
//! bucket and key, then a leaf named by a prefix of the key's hash and a
//! random suffix). `meta.json` format 1: stamps optional. Read-only in this
//! build; the upgrade relocates every blob to the current layout.
//!
//! Do not change this file's shape rules: a bucket written by a build of
//! this layout must keep opening. New shapes are new layouts.

use super::{components, is_hex, LayoutSpec};

pub const VERSION: u32 = 2;

pub const SPEC: LayoutSpec = LayoutSpec {
    version: VERSION,
    meta_format: 1,
    summary: "single-level fanout objects/<4hex>/<V1prefix_random>, meta.json format 1 (stamps optional)",
    is_blob_path,
};

/// `objects/<4hex>/<leaf>` where the leaf is `V1` + 6 hex + `_` + 4 hex.
pub fn is_blob_path(blob_rel: &str) -> bool {
    let c = components(blob_rel);
    c.len() == 3 && c[0] == "objects" && is_hex(c[1], 4) && is_leaf(c[2])
}

fn is_leaf(leaf: &str) -> bool {
    let Some(rest) = leaf.strip_prefix("V1") else {
        return false;
    };
    let Some((prefix, suffix)) = rest.split_once('_') else {
        return false;
    };
    is_hex(prefix, 6) && is_hex(suffix, 4)
}
