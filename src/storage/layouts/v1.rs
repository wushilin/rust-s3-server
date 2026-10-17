//! Layout 1 — SEALED. The original 4-level fanout:
//! `objects/aa/bb/cc/dd/<leaf>` (four two-hex-char levels from the key's
//! hash, then the blob dir). Read-only in this build; the upgrade relocates
//! every blob to the current layout.
//!
//! Do not change this file's shape rules: a bucket written by a build of
//! this layout must keep opening. New shapes are new layouts.

use super::{components, is_hex, LayoutSpec};

pub const VERSION: u32 = 1;

pub const SPEC: LayoutSpec = LayoutSpec {
    version: VERSION,
    meta_format: 1,
    summary: "4-level fanout objects/aa/bb/cc/dd/<leaf>, meta.json format 1 (stamps optional)",
    is_blob_path,
};

/// `objects/<2hex>/<2hex>/<2hex>/<2hex>/<leaf>`.
pub fn is_blob_path(blob_rel: &str) -> bool {
    let c = components(blob_rel);
    c.len() == 6 && c[0] == "objects" && c[1..5].iter().all(|level| is_hex(level, 2))
}
