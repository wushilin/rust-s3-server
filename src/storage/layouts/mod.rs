//! The on-disk blob layouts this build reads, and the one it writes.
//!
//! A *layout* is the shape of the object tree under a bucket's `objects/`
//! directory. Every layout this build has ever written is kept, one sealed
//! file per layout (`v1.rs`, `v2.rs`, `v3.rs`), so a bucket written by any
//! earlier build still opens here, and the registry below is the record of
//! what is supported. The rules:
//!
//! - A layout file is **sealed**: once a build has shipped with it, its path
//!   shape never changes. A change in shape is a new layout, a new file, and
//!   a new registry entry — never an edit of an existing one.
//! - Reads never depend on the layout: the index row stores each blob's
//!   path, and `meta.json` describes the blob. The layout only decides where
//!   a *new* blob goes and how a rebuild walks the tree.
//! - A bucket is served while `stable` on any supported layout
//!   ([`crate::storage::metadata::BucketLayout`]); new blobs always use the
//!   current layout, so an un-upgraded bucket holds a mix, every path of
//!   which is still readable. The explicit, gated upgrade
//!   (`LocalObjectStore::migrate_bucket_layout`) relocates what is left and
//!   is the only thing that must understand an old layout's shape.
//! - Dropping a layout from [`SUPPORTED`] is a deliberate act: the test
//!   `every_supported_layout_still_opens_and_upgrades` builds a bucket in
//!   each one and proves this build reads and upgrades it.

pub mod v1;
pub mod v2;
pub mod v3;

/// The layout this build writes.
pub const CURRENT: u32 = v3::VERSION;

/// One supported layout.
#[derive(Debug, Clone, Copy)]
pub struct LayoutSpec {
    pub version: u32,
    /// The `meta.json` format blobs on this layout were written with
    /// (`crate::storage::metadata::META_FORMAT` history).
    pub meta_format: u32,
    pub summary: &'static str,
    /// Whether a bucket-relative blob dir path (`objects/...`) has this
    /// layout's shape.
    pub is_blob_path: fn(&str) -> bool,
}

/// Every layout this build reads, oldest first. The last entry is
/// [`CURRENT`].
pub const SUPPORTED: [LayoutSpec; 3] = [v1::SPEC, v2::SPEC, v3::SPEC];

pub fn spec(version: u32) -> Option<&'static LayoutSpec> {
    SUPPORTED.iter().find(|s| s.version == version)
}

/// The layout a blob dir path is on, by its shape; `None` for a path no
/// layout wrote. Checked newest first (an older shape never matches a
/// newer path).
pub fn layout_of(blob_rel: &str) -> Option<u32> {
    SUPPORTED.iter().rev().find(|s| (s.is_blob_path)(blob_rel)).map(|s| s.version)
}

/// Whether a blob dir path is on the current layout.
pub fn is_current(blob_rel: &str) -> bool {
    (v3::SPEC.is_blob_path)(blob_rel)
}

pub(crate) fn components(blob_rel: &str) -> Vec<&str> {
    blob_rel.split(['/', '\\']).filter(|s| !s.is_empty()).collect()
}

pub(crate) fn is_hex(s: &str, len: usize) -> bool {
    s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_registry_is_ordered_and_ends_at_current() {
        let versions: Vec<u32> = SUPPORTED.iter().map(|s| s.version).collect();
        assert_eq!(versions, [1, 2, 3]);
        assert_eq!(SUPPORTED.last().unwrap().version, CURRENT);
        assert_eq!(crate::storage::metadata::CURRENT_LAYOUT, CURRENT);
        for s in SUPPORTED {
            assert!(spec(s.version).is_some());
        }
    }

    #[test]
    fn every_layout_recognises_only_its_own_paths() {
        let v1 = "objects/aa/bb/cc/dd/V1AB3F7C_9F00";
        let v2 = "objects/ab3f/V1AB3F7C_9F00";
        let v3 = "objects/ab3f/0123456789abcdef0123456789abcdef01234567/0000001791900298853.0000000000.bd7e41df94febe9a";
        assert_eq!(layout_of(v1), Some(1));
        assert_eq!(layout_of(v2), Some(2));
        assert_eq!(layout_of(v3), Some(3));
        assert_eq!(layout_of("objects/ab3f"), None);
        assert_eq!(layout_of("staging/put/x"), None);
        assert!(is_current(v3));
        assert!(!is_current(v2));
        assert_eq!(layout_of(&v3.replace('/', "\\")), Some(3), "Windows separators");
    }
}
