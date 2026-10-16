//! Every layout this build supports still opens here and upgrades to the
//! current one, and the upgrade is fail-safe: a bucket is readable at every
//! point of an interrupted upgrade, nothing is lost or duplicated, and a
//! rebuild from the tree alone reproduces the catalog afterwards.
//!
//! A bucket in an older layout is built through this build's write path and
//! then aged: its blob dirs moved to the older layout's shape and, for the
//! pre-stamp era, its `meta.json` rewritten at format 1 without a stamp.

use std::collections::BTreeMap;

use tokio_util::sync::CancellationToken;

use super::index::Displaced;
use super::layouts::{self, SUPPORTED};
use super::metadata::{BucketLayout, BucketVersioning, ObjectMeta, CURRENT_LAYOUT, META_FORMAT};
use super::store::{CompletePartRequest, LocalObjectStore, WriteOrigin};
use super::time::now_ms;
use crate::storage::changelog::ChangeOrigin;

const BUCKET: &str = "bucket";

/// Relaxed durability: these tests exercise the layout logic, and the 5 MiB
/// multipart fixtures would otherwise fsync-storm the disk under the rest
/// of the suite.
async fn fresh() -> (tempfile::TempDir, LocalObjectStore) {
    let tmp = tempfile::tempdir().unwrap();
    let config = super::config::StorageConfig { durability: super::config::DurabilityMode::Relaxed, ..Default::default() };
    let store = LocalObjectStore::from_storage_config(tmp.path(), &config);
    store.create_bucket(BUCKET).await.unwrap();
    (tmp, store)
}

async fn read_json<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> T {
    serde_json::from_slice(&tokio::fs::read(path).await.unwrap()).unwrap()
}

async fn body_of(store: &LocalObjectStore, key: &str) -> Vec<u8> {
    let read = store.read_object(BUCKET, key).await.unwrap();
    let mut body = Vec::new();
    for part in &read.meta.parts {
        body.extend(tokio::fs::read(read.object_dir.join(&part.file)).await.unwrap());
    }
    body
}

/// A multipart object of two parts (5 MiB + a tail), so its ETag is the
/// multipart form and its parts layout is non-trivial.
async fn put_multipart(store: &LocalObjectStore, key: &str) -> (String, Vec<u8>) {
    let upload = store.initiate_multipart(BUCKET, key, Some("application/x-test"), None).await.unwrap();
    let first = vec![b'a'; 5 * 1024 * 1024];
    let second = b"tail-bytes".to_vec();
    let p1 = store.put_multipart_part(BUCKET, key, &upload, 1, &first, false).await.unwrap();
    let p2 = store.put_multipart_part(BUCKET, key, &upload, 2, &second, false).await.unwrap();
    let result = store
        .complete_multipart(
            BUCKET,
            key,
            &upload,
            &[
                CompletePartRequest { number: 1, etag: p1.etag },
                CompletePartRequest { number: 2, etag: p2.etag },
            ],
        )
        .await
        .unwrap();
    let mut body = first;
    body.extend(second);
    (result.etag, body)
}

/// The blob dir path of `key` in layout `layout`, as a build of that layout
/// would have placed it.
fn aged_rel(layout: u32, key: &str, leaf: &str) -> String {
    let hash = super::encoding::fanout_id(BUCKET, key);
    match layout {
        1 => format!("objects/{}/{}/{}/{}/{leaf}", &hash[0..2], &hash[2..4], &hash[4..6], &hash[6..8]),
        2 => format!("objects/{}/{leaf}", &hash[0..4]),
        other => panic!("no ageing rule for layout {other}"),
    }
}

/// Moves `key`'s blob dir to layout `layout` and, when `unstamped`, rewrites
/// its `meta.json` at format 1 without a stamp (an object from before
/// stamps) and clears the row's stamp.
async fn age(store: &LocalObjectStore, layout: u32, key: &str, unstamped: bool) {
    let index = store.index(BUCKET).await.unwrap();
    let row = index.get(key).await.unwrap().unwrap();
    let bucket_dir = store.layout().bucket_dir(BUCKET).unwrap();
    let leaf = format!("{}_{}", super::encoding::object_dir_prefix(key), super::encoding::object_dir_random_suffix());
    let old_rel = aged_rel(layout, key, &leaf);
    let old_dir = bucket_dir.join(&old_rel);
    tokio::fs::create_dir_all(old_dir.parent().unwrap()).await.unwrap();
    tokio::fs::rename(bucket_dir.join(&row.blob_dir), &old_dir).await.unwrap();
    let _ = tokio::fs::remove_dir(bucket_dir.join(&row.blob_dir).parent().unwrap()).await;
    assert_eq!(layouts::layout_of(&old_rel), Some(layout), "{old_rel}");
    let mut meta: ObjectMeta = read_json(&old_dir.join("meta.json")).await;
    if unstamped {
        meta.format_version = 1;
        meta.stamp = None;
    }
    meta.versioned = false;
    tokio::fs::write(old_dir.join("meta.json"), serde_json::to_vec_pretty(&meta).unwrap()).await.unwrap();
    assert!(index.update_blob_dir(key, &row.blob_dir, &old_rel).await.unwrap());
    if unstamped {
        assert!(index
            .update_object_attrs(key, &old_rel, row.size, &row.etag, row.last_modified_ms, row.parts.clone(), None)
            .await
            .unwrap());
    }
    store.forget_cached_meta(BUCKET, key);
}

/// A tombstone as a build before delete markers wrote it: in the index,
/// with no marker dir on disk.
async fn legacy_tombstone(store: &LocalObjectStore, key: &str) {
    let index = store.index(BUCKET).await.unwrap();
    let row = index.get(key).await.unwrap().unwrap();
    let removed = store.effective_stamp(row.stamp.as_ref(), row.last_modified_ms);
    let ids = index
        .commit_delete_from(key, Displaced::retire(&row.blob_dir), now_ms(), ChangeOrigin::Client, Some(store.next_stamp()), Some(removed), None, false)
        .await
        .unwrap();
    let bucket_dir = store.layout().bucket_dir(BUCKET).unwrap();
    tokio::fs::remove_dir_all(bucket_dir.join(&row.blob_dir)).await.unwrap();
    for id in ids {
        index.delete_intent(id).await.unwrap();
    }
    store.forget_cached_meta(BUCKET, key);
}

/// Strips the layout marker, as a `bucket.json` from before layouts were
/// recorded has none.
async fn forget_layout(store: &LocalObjectStore) {
    let path = store.layout().bucket_meta_path(BUCKET).unwrap();
    let mut meta: serde_json::Value = read_json(&path).await;
    meta.as_object_mut().unwrap().remove("layout");
    std::fs::write(&path, meta.to_string()).unwrap();
    store.forget_bucket_meta(BUCKET);
}

/// What a bucket built in `layout` holds, for the checks after an upgrade.
struct Fixture {
    singles: BTreeMap<String, Vec<u8>>,
    multipart: (String, String, Vec<u8>),
    deleted: String,
}

/// Builds a bucket as a build of `layout` would have left it: two single
/// objects (one from before stamps), a multipart object, and a deleted key
/// whose tombstone has no marker.
async fn build_in_layout(store: &LocalObjectStore, layout: u32) -> Fixture {
    let mut singles = BTreeMap::new();
    for (key, body, unstamped) in [("docs/old.txt", b"from before stamps".to_vec(), true), ("docs/new.txt", b"stamped".to_vec(), false)] {
        store.put_object(BUCKET, key, &body, Some("text/plain"), None, false).await.unwrap();
        age(store, layout, key, unstamped).await;
        singles.insert(key.to_string(), body);
    }
    let (etag, body) = put_multipart(store, "big/part.bin").await;
    age(store, layout, "big/part.bin", false).await;
    store.put_object(BUCKET, "gone/key", b"deleted", None, None, false).await.unwrap();
    age(store, layout, "gone/key", true).await;
    legacy_tombstone(store, "gone/key").await;
    forget_layout(store).await;
    Fixture { singles, multipart: ("big/part.bin".to_string(), etag, body), deleted: "gone/key".to_string() }
}

/// Everything the fixture promised, after whatever happened to the bucket.
async fn assert_fixture_intact(store: &LocalObjectStore, fixture: &Fixture) {
    for (key, body) in &fixture.singles {
        assert_eq!(&body_of(store, key).await, body, "{key}");
    }
    let (key, etag, body) = &fixture.multipart;
    let read = store.read_object(BUCKET, key).await.unwrap();
    assert_eq!(&read.meta.etag, etag, "multipart ETag kept");
    assert_eq!(read.meta.parts.len(), 2);
    assert_eq!(&body_of(store, key).await, body);
    assert!(matches!(store.read_object(BUCKET, &fixture.deleted).await, Err(super::errors::StorageError::ObjectNotFound { .. })));
    let (stamp, deleted) = store.key_stamp(BUCKET, &fixture.deleted).await.unwrap().expect("the tombstone");
    assert!(deleted, "{stamp:?}");
    let page = store.list_objects(BUCKET, "", None, None, 100).await.unwrap();
    let keys: Vec<&str> = page.entries.iter().map(|e| e.object_key.as_str()).collect();
    assert_eq!(keys, ["big/part.bin", "docs/new.txt", "docs/old.txt"]);
}

/// The bucket is entirely on the current layout at the current format.
async fn assert_on_current_layout(store: &LocalObjectStore) {
    assert_eq!(store.bucket_layout(BUCKET).await.unwrap(), BucketLayout::current());
    assert_eq!(store.detect_layout(BUCKET).await.unwrap(), CURRENT_LAYOUT);
    let index = store.index(BUCKET).await.unwrap();
    assert!(index.rows_off_layout(None, 10).await.unwrap().is_empty());
    assert!(index.tombstones_without_marker(None, 10).await.unwrap().is_empty());
    let bucket_dir = store.layout().bucket_dir(BUCKET).unwrap();
    for row in index.all_entries_after(None, 100).await.unwrap() {
        assert!(layouts::is_current(&row.blob_dir), "{}", row.blob_dir);
        let meta: ObjectMeta = read_json(&bucket_dir.join(&row.blob_dir).join("meta.json")).await;
        assert_eq!(meta.format_version, META_FORMAT, "{}", row.object_key);
        assert_eq!(meta.stamp, row.stamp, "{}: the row and the blob agree on the stamp", row.object_key);
        assert!(row.stamp.is_some());
        assert!(row.blob_dir.ends_with(&layouts::v3::stamp_dir(row.stamp.as_ref().unwrap())), "the dir is named by the stamp");
    }
    let (tombstones, _) = index.list_tombstones("", None, 100).await.unwrap();
    for t in tombstones {
        let dir = t.blob_dir.as_ref().expect("a marker dir");
        let meta: ObjectMeta = read_json(&bucket_dir.join(dir).join("meta.json")).await;
        assert!(meta.delete_marker && meta.stamp.as_ref() == Some(&t.stamp), "{}", t.object_key);
    }
}

async fn upgrade(store: &LocalObjectStore) -> u64 {
    store.start_layout_upgrade(BUCKET).await.unwrap();
    let moved = store.migrate_bucket_layout(BUCKET, &CancellationToken::new()).await.unwrap();
    store.end_rebuild(BUCKET);
    moved
}

#[tokio::test]
async fn every_supported_layout_still_opens_and_upgrades() {
    for spec in SUPPORTED.iter().filter(|s| s.version < CURRENT_LAYOUT) {
        let (_tmp, store) = fresh().await;
        let fixture = build_in_layout(&store, spec.version).await;

        // Detected from the tree, recorded at startup, and served as is.
        assert_eq!(store.bucket_layout(BUCKET).await.unwrap(), BucketLayout::Stable { version: spec.version }, "layout {}", spec.version);
        assert!(store.check_layouts().await.unwrap().is_empty());
        assert_eq!(store.bucket_meta(BUCKET).await.unwrap().layout, Some(BucketLayout::Stable { version: spec.version }));
        assert_fixture_intact(&store, &fixture).await;
        assert!(LocalObjectStore::layout_upgrade_available(store.bucket_layout(BUCKET).await.unwrap()));
        // Versioning needs the current layout.
        assert!(matches!(
            store.set_bucket_versioning(BUCKET, BucketVersioning::Enabled).await,
            Err(super::errors::StorageError::BucketInUse(_))
        ));

        // The upgrade relocates every object, materialises the stamps, and
        // writes the missing delete marker.
        let moved = upgrade(&store).await;
        assert_eq!(moved, 4, "layout {}: 3 objects and 1 tombstone", spec.version);
        assert_on_current_layout(&store).await;
        assert_fixture_intact(&store, &fixture).await;
        // Nothing of the old tree remains.
        let objects = store.layout().bucket_dir(BUCKET).unwrap().join("objects");
        let mut fanouts = tokio::fs::read_dir(&objects).await.unwrap();
        while let Some(fanout) = fanouts.next_entry().await.unwrap() {
            assert_eq!(fanout.file_name().len(), 4, "layout {}: {:?}", spec.version, fanout.file_name());
        }
        // Versioning can be enabled now, and a second upgrade is refused.
        store.set_bucket_versioning(BUCKET, BucketVersioning::Enabled).await.unwrap();
        assert!(matches!(store.start_layout_upgrade(BUCKET).await, Err(super::errors::StorageError::BucketInUse(_))));

        // A rebuild from the tree alone (the index lost) reproduces the
        // catalog: rows, the deleted key's tombstone, and the version ids.
        let before = store.list_object_versions(BUCKET, "", None, "", "", 100).await.unwrap();
        store.forget_index(BUCKET);
        tokio::fs::remove_dir_all(store.layout().bucket_dir(BUCKET).unwrap().join("index.rocksdb")).await.unwrap();
        assert_eq!(store.rebuild_index(BUCKET).await.unwrap(), 4, "3 objects and the delete marker");
        assert_fixture_intact(&store, &fixture).await;
        assert_on_current_layout(&store).await;
        let after = store.list_object_versions(BUCKET, "", None, "", "", 100).await.unwrap();
        assert_eq!(before, after, "layout {}", spec.version);
    }
}

/// Runs `f` on a task of its own so an injected crash (a panic) is observed
/// as a failed task, not as a failed test.
async fn crashing<F, Fut>(f: F) -> bool
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(f()).await.is_err()
}

/// The one window the index-driven resume cannot see: the row already
/// names the new dir, the old leaf is still on disk. Resuming must reap it
/// — otherwise the bucket is declared current while its tree still holds
/// the old layout, and the next rebuild has two copies of the key to pick
/// from.
#[tokio::test]
async fn a_crash_between_the_flip_and_the_unlink_leaves_no_old_leaf_after_resume() {
    for layout in [1, 2] {
        let (_tmp, store) = fresh().await;
        let fixture = build_in_layout(&store, layout).await;
        store.start_layout_upgrade(BUCKET).await.unwrap();

        store.arm_crash_point("migrate_after_flip");
        let s = store.clone();
        assert!(crashing(move || async move {
            s.migrate_bucket_layout(BUCKET, &CancellationToken::new()).await.unwrap();
        })
        .await);
        store.disarm_crash_point("migrate_after_flip");
        // At least one row was flipped with its old leaf left behind.
        assert_ne!(store.detect_layout(BUCKET).await.unwrap(), CURRENT_LAYOUT, "layout {layout}: old leaf still on disk");
        let index = store.index_unchecked(BUCKET).await.unwrap();
        let flipped = index.all_entries_after(None, 100).await.unwrap().into_iter().filter(|r| layouts::is_current(&r.blob_dir)).count();
        assert!(flipped >= 1, "layout {layout}");
        store.end_rebuild(BUCKET);
        assert_fixture_intact(&store, &fixture).await;
        store.try_begin_rebuild(BUCKET);

        store.migrate_bucket_layout(BUCKET, &CancellationToken::new()).await.unwrap();
        store.end_rebuild(BUCKET);
        assert_on_current_layout(&store).await;
        assert_fixture_intact(&store, &fixture).await;
        assert!(store.check_layouts().await.unwrap().is_empty(), "layout {layout}: nothing left to resume");
    }
}

#[tokio::test]
async fn an_upgrade_interrupted_at_every_step_resumes_without_loss() {
    let (_tmp, store) = fresh().await;
    let fixture = build_in_layout(&store, 2).await;
    store.start_layout_upgrade(BUCKET).await.unwrap();
    let migrating = BucketLayout::Migrating { from: 2, to: CURRENT_LAYOUT };
    assert_eq!(store.bucket_layout(BUCKET).await.unwrap(), migrating);

    // Crash after the hardlinks, before the index flip: the row still names
    // the old dir, the half-built new dir is a leftover.
    store.arm_crash_point("migrate_after_link");
    let s = store.clone();
    assert!(crashing(move || async move {
        s.migrate_bucket_layout(BUCKET, &CancellationToken::new()).await.unwrap();
    })
    .await);
    store.disarm_crash_point("migrate_after_link");
    assert_eq!(store.bucket_layout(BUCKET).await.unwrap(), migrating, "still migrating");
    store.end_rebuild(BUCKET);
    assert_fixture_intact(&store, &fixture).await;
    store.try_begin_rebuild(BUCKET);

    // Crash once an object has moved (others may be mid-move, since
    // relocation runs concurrently): the tree may be mixed, and every
    // object is readable through its row either way.
    store.arm_crash_point("migrate_after_object");
    let s = store.clone();
    assert!(crashing(move || async move {
        s.migrate_bucket_layout(BUCKET, &CancellationToken::new()).await.unwrap();
    })
    .await);
    store.disarm_crash_point("migrate_after_object");
    let index = store.index_unchecked(BUCKET).await.unwrap();
    let left = index.rows_off_layout(None, 10).await.unwrap();
    assert!(left.len() < 3, "at least one moved: {left:?}");
    assert_eq!(store.bucket_layout(BUCKET).await.unwrap(), migrating);
    store.end_rebuild(BUCKET);
    assert_fixture_intact(&store, &fixture).await;
    store.try_begin_rebuild(BUCKET);

    // A restart finds the bucket migrating and resumes it, gated.
    store.end_rebuild(BUCKET);
    store.forget_bucket_meta(BUCKET);
    let resume = store.check_layouts().await.unwrap();
    assert_eq!(resume, vec![(BUCKET.to_string(), migrating)]);
    assert!(matches!(store.index(BUCKET).await, Err(super::errors::StorageError::BucketRebuilding(_))), "gated while resuming");

    // Cancelled mid-way: the step reports it and the bucket stays migrating.
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(store.migrate_bucket_layout(BUCKET, &cancel).await.is_err());
    assert_eq!(store.bucket_layout(BUCKET).await.unwrap(), migrating);

    // The resumed step finishes what is left; nothing is duplicated.
    store.migrate_bucket_layout(BUCKET, &CancellationToken::new()).await.unwrap();
    store.end_rebuild(BUCKET);
    assert_on_current_layout(&store).await;
    assert_fixture_intact(&store, &fixture).await;
    let bucket_dir = store.layout().bucket_dir(BUCKET).unwrap();
    for row in index.all_entries_after(None, 100).await.unwrap() {
        let key_dir = bucket_dir.join(&row.blob_dir).parent().unwrap().to_path_buf();
        let mut entries = tokio::fs::read_dir(&key_dir).await.unwrap();
        let mut n = 0;
        while entries.next_entry().await.unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, 1, "{}: exactly one version dir after the interrupted attempts", row.object_key);
    }
    assert!(store.check_layouts().await.unwrap().is_empty(), "nothing left to resume");
}

#[tokio::test]
async fn a_rebuild_keeps_the_changelog_only_when_the_catalog_is_unchanged() {
    let (_tmp, store) = fresh().await;
    store.put_object(BUCKET, "a", b"1", None, None, false).await.unwrap();
    store.put_object(BUCKET, "b", b"2", None, None, false).await.unwrap();
    let index = store.index(BUCKET).await.unwrap();
    let (log_id, head) = (index.changelog_id().to_string(), index.changelog_head());
    drop(index);

    // The tree agrees with the index: the log (and its id) is carried.
    store.rebuild_index(BUCKET).await.unwrap();
    let index = store.index(BUCKET).await.unwrap();
    assert_eq!((index.changelog_id(), index.changelog_head()), (log_id.as_str(), head));
    drop(index);

    // A blob the index never knew (a restored backup, a copied tree): the
    // rebuilt catalog differs, so the log starts over and consumers
    // resnapshot rather than miss the change.
    store.put_object(BUCKET, "c", b"3", None, None, false).await.unwrap();
    let index = store.index(BUCKET).await.unwrap();
    let row = index.get("c").await.unwrap().unwrap();
    index.commit_delete("c", "objects/nowhere", now_ms()).await.unwrap();
    assert!(index.get("c").await.unwrap().is_none());
    let _ = row;
    drop(index);
    store.rebuild_index(BUCKET).await.unwrap();
    let index = store.index(BUCKET).await.unwrap();
    assert!(index.get("c").await.unwrap().is_some(), "the blob on disk is back in the catalog");
    assert_ne!(index.changelog_id(), log_id.as_str(), "a changed catalog gets a fresh log");
    drop(index);

    // The index lost entirely: nothing to carry, a fresh log.
    let fresh_id = store.index(BUCKET).await.unwrap().changelog_id().to_string();
    store.forget_index(BUCKET);
    tokio::fs::remove_dir_all(store.layout().bucket_dir(BUCKET).unwrap().join("index.rocksdb")).await.unwrap();
    assert_eq!(store.rebuild_index(BUCKET).await.unwrap(), 3);
    assert_ne!(store.index(BUCKET).await.unwrap().changelog_id(), fresh_id.as_str());
    let _ = WriteOrigin::Client;
}
