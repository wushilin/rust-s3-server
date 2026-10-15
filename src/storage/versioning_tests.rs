//! Object versioning as S3 defines it: enabled keeps every version and
//! deletes leave markers; suspended replaces only the `null` version;
//! version-scoped reads and deletes; and a bucket that never had versioning
//! still lists one `null` version per key.

use tokio_util::sync::CancellationToken;

use super::errors::StorageError;
use super::metadata::{BucketLayout, BucketVersioning};
use super::scan::{Finding, ScanProgress};
use super::store::{LocalObjectStore, WriteOrigin};

const BUCKET: &str = "bucket";

async fn fresh() -> (tempfile::TempDir, LocalObjectStore) {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path());
    store.create_bucket(BUCKET).await.unwrap();
    (tmp, store)
}

async fn put(store: &LocalObjectStore, key: &str, body: &[u8]) -> Option<String> {
    store.put_object(BUCKET, key, body, None, None, false).await.unwrap().version_id
}

async fn body_of(store: &LocalObjectStore, key: &str, version_id: Option<&str>) -> Vec<u8> {
    let read = match version_id {
        Some(v) => store.read_object_version(BUCKET, key, v).await.unwrap(),
        None => store.read_object(BUCKET, key).await.unwrap(),
    };
    let mut body = Vec::new();
    for part in &read.meta.parts {
        body.extend(tokio::fs::read(read.object_dir.join(&part.file)).await.unwrap());
    }
    body
}

/// `(version id, is current, is delete marker)` of every version, newest first.
async fn versions(store: &LocalObjectStore, key: &str) -> Vec<(String, bool, bool)> {
    store
        .list_versions(BUCKET, key)
        .await
        .unwrap()
        .into_iter()
        .map(|v| (v.version_id(), v.is_current, v.record.delete_marker))
        .collect()
}

async fn findings(store: &LocalObjectStore) -> Vec<Finding> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let collector = tokio::spawn(async move {
        let mut out = Vec::new();
        while let Some(finding) = rx.recv().await {
            out.push(finding);
        }
        out
    });
    store.scan_bucket(BUCKET, &CancellationToken::new(), &ScanProgress::default(), &tx).await.unwrap();
    drop(tx);
    collector.await.unwrap()
}

#[tokio::test]
async fn versioning_can_only_be_enabled_on_the_current_layout_and_never_turned_off() {
    let (_tmp, store) = fresh().await;
    assert_eq!(store.bucket_versioning(BUCKET).await.unwrap(), BucketVersioning::Off);
    store.set_bucket_layout(BUCKET, BucketLayout::Stable { version: 2 }).await.unwrap();
    let err = store.set_bucket_versioning(BUCKET, BucketVersioning::Enabled).await.unwrap_err();
    assert!(matches!(err, StorageError::BucketInUse(ref m) if m.contains("upgrade")), "{err}");
    store.set_bucket_layout(BUCKET, BucketLayout::current()).await.unwrap();
    store.set_bucket_versioning(BUCKET, BucketVersioning::Enabled).await.unwrap();
    assert_eq!(store.bucket_versioning(BUCKET).await.unwrap(), BucketVersioning::Enabled);
    assert!(matches!(store.set_bucket_versioning(BUCKET, BucketVersioning::Off).await, Err(StorageError::BucketInUse(_))));
    store.set_bucket_versioning(BUCKET, BucketVersioning::Suspended).await.unwrap();
    store.set_bucket_versioning(BUCKET, BucketVersioning::Enabled).await.unwrap();
    // Survives a reopen.
    store.forget_bucket_meta(BUCKET);
    assert_eq!(store.bucket_versioning(BUCKET).await.unwrap(), BucketVersioning::Enabled);
}

#[tokio::test]
async fn enabled_keeps_every_version_and_a_delete_leaves_a_marker() {
    let (_tmp, store) = fresh().await;
    assert_eq!(put(&store, "k", b"null").await, None, "no version id before versioning");
    assert_eq!(versions(&store, "k").await, [("null".to_string(), true, false)]);
    store.set_bucket_versioning(BUCKET, BucketVersioning::Enabled).await.unwrap();
    let v1 = put(&store, "k", b"one").await.unwrap();
    let v2 = put(&store, "k", b"two").await.unwrap();
    assert!(v1 < v2, "version ids sort in write order");
    assert_eq!(
        versions(&store, "k").await,
        [(v2.clone(), true, false), (v1.clone(), false, false), ("null".to_string(), false, false)]
    );
    assert_eq!(body_of(&store, "k", None).await, b"two");
    assert_eq!(body_of(&store, "k", Some(&v2)).await, b"two");
    assert_eq!(body_of(&store, "k", Some(&v1)).await, b"one");
    assert_eq!(body_of(&store, "k", Some("null")).await, b"null");
    assert!(matches!(store.read_object_version(BUCKET, "k", "nope").await, Err(StorageError::NoSuchVersion { .. })));
    assert_eq!(store.list_objects(BUCKET, "", None, None, 10).await.unwrap().entries.len(), 1, "one key");

    // A delete leaves a marker: the key is gone, every version stays.
    let result = store.delete_object_with(BUCKET, "k", super::store::WriteOptions::client(), None).await.unwrap();
    let marker = result.version_id.unwrap();
    assert!(result.delete_marker && result.size == Some(3));
    assert!(matches!(store.read_object(BUCKET, "k").await, Err(StorageError::ObjectNotFound { .. })));
    assert!(matches!(store.read_object_version(BUCKET, "k", &marker).await, Err(StorageError::VersionIsDeleteMarker { .. })));
    assert_eq!(body_of(&store, "k", Some(&v2)).await, b"two", "the old current is still readable by version");
    assert_eq!(
        versions(&store, "k").await,
        [(marker.clone(), true, true), (v2.clone(), false, false), (v1.clone(), false, false), ("null".to_string(), false, false)]
    );
    assert!(store.list_objects(BUCKET, "", None, None, 10).await.unwrap().entries.is_empty(), "a deleted key is not listed");
    let page = store.list_object_versions(BUCKET, "", "", "", 10).await.unwrap();
    let listed: Vec<(String, bool, bool)> = page.entries.iter().map(|e| (e.version_id.clone(), e.is_latest, e.delete_marker)).collect();
    assert_eq!(listed, versions(&store, "k").await, "ListObjectVersions sees the same");
    assert!(findings(&store).await.is_empty(), "the health scan knows every version and marker");

    // Removing the marker undeletes: the newest remaining version is current.
    assert_eq!(store.delete_object_version(BUCKET, "k", &marker, WriteOrigin::Client).await.unwrap(), Some(0));
    assert_eq!(body_of(&store, "k", None).await, b"two");
    assert_eq!(versions(&store, "k").await.len(), 3);
    // Removing the current version promotes the next one.
    assert_eq!(store.delete_object_version(BUCKET, "k", &v2, WriteOrigin::Client).await.unwrap(), Some(3));
    assert_eq!(body_of(&store, "k", None).await, b"one");
    assert_eq!(versions(&store, "k").await, [(v1.clone(), true, false), ("null".to_string(), false, false)]);
    // Removing a noncurrent version, and a version that is already gone.
    assert_eq!(store.delete_object_version(BUCKET, "k", "null", WriteOrigin::Client).await.unwrap(), Some(4));
    assert_eq!(store.delete_object_version(BUCKET, "k", "null", WriteOrigin::Client).await.unwrap(), None);
    assert_eq!(versions(&store, "k").await, [(v1.clone(), true, false)]);
    // Removing the last version leaves nothing: no row, no marker.
    assert_eq!(store.delete_object_version(BUCKET, "k", &v1, WriteOrigin::Client).await.unwrap(), Some(3));
    assert!(versions(&store, "k").await.is_empty());
    assert!(store.key_stamp(BUCKET, "k").await.unwrap().is_none());
    assert!(findings(&store).await.is_empty());
}

#[tokio::test]
async fn suspended_replaces_only_the_null_version() {
    let (_tmp, store) = fresh().await;
    store.set_bucket_versioning(BUCKET, BucketVersioning::Enabled).await.unwrap();
    let v1 = put(&store, "k", b"one").await.unwrap();
    let v2 = put(&store, "k", b"two").await.unwrap();
    store.set_bucket_versioning(BUCKET, BucketVersioning::Suspended).await.unwrap();

    // A write makes a null version current; the real versions stay.
    assert_eq!(put(&store, "k", b"null-1").await.as_deref(), Some("null"));
    assert_eq!(versions(&store, "k").await, [("null".to_string(), true, false), (v2.clone(), false, false), (v1.clone(), false, false)]);
    // The next write replaces the null version only.
    let trash_before = trash_count(&store).await;
    assert_eq!(put(&store, "k", b"null-2").await.as_deref(), Some("null"));
    assert_eq!(body_of(&store, "k", Some("null")).await, b"null-2");
    assert_eq!(versions(&store, "k").await.len(), 3);
    assert_eq!(trash_count(&store).await, trash_before + 1, "the replaced null version is retired");

    // A delete inserts a null marker in place of the null version.
    let result = store.delete_object_with(BUCKET, "k", super::store::WriteOptions::client(), None).await.unwrap();
    assert_eq!(result.version_id.as_deref(), Some("null"));
    assert_eq!(versions(&store, "k").await, [("null".to_string(), true, true), (v2.clone(), false, false), (v1.clone(), false, false)]);
    assert_eq!(body_of(&store, "k", Some(&v2)).await, b"two");
    // A write replaces the null marker; the object is back.
    assert_eq!(put(&store, "k", b"null-3").await.as_deref(), Some("null"));
    assert_eq!(versions(&store, "k").await, [("null".to_string(), true, false), (v2.clone(), false, false), (v1.clone(), false, false)]);
    assert_eq!(body_of(&store, "k", None).await, b"null-3");
    assert!(findings(&store).await.is_empty());

    // Enabled again: the null version is kept like any other.
    store.set_bucket_versioning(BUCKET, BucketVersioning::Enabled).await.unwrap();
    let v3 = put(&store, "k", b"three").await.unwrap();
    assert_eq!(versions(&store, "k").await, [(v3, true, false), ("null".to_string(), false, false), (v2, false, false), (v1, false, false)]);
}

async fn trash_count(store: &LocalObjectStore) -> usize {
    let trash = store.layout().trash_dir(BUCKET).unwrap();
    let Ok(mut entries) = tokio::fs::read_dir(&trash).await else {
        return 0;
    };
    let mut n = 0;
    while entries.next_entry().await.unwrap().is_some() {
        n += 1;
    }
    n
}

#[tokio::test]
async fn an_unversioned_bucket_retires_what_it_replaces_but_keeps_deletes_on_disk() {
    let (_tmp, store) = fresh().await;
    put(&store, "k", b"one").await;
    put(&store, "k", b"two").await;
    assert_eq!(versions(&store, "k").await, [("null".to_string(), true, false)]);
    assert_eq!(trash_count(&store).await, 1, "the overwritten null version is retired");
    store.delete_object(BUCKET, "k").await.unwrap();
    assert_eq!(trash_count(&store).await, 2);
    // The tombstone is on disk as a marker dir (for a rebuild), but S3
    // clients of an unversioned bucket see no delete markers.
    let index = store.index(BUCKET).await.unwrap();
    let t = index.get_tombstone("k").await.unwrap().unwrap();
    assert!(t.blob_dir.is_some() && !t.versioned);
    assert!(store.list_object_versions(BUCKET, "", "", "", 10).await.unwrap().entries.is_empty());
    assert!(findings(&store).await.is_empty());
    drop(index);

    // A rebuild from the tree alone finds the delete.
    store.forget_index(BUCKET);
    tokio::fs::remove_dir_all(store.layout().bucket_dir(BUCKET).unwrap().join("index.rocksdb")).await.unwrap();
    store.rebuild_index(BUCKET).await.unwrap();
    let read = store.read_object(BUCKET, "k").await.map(|r| r.object_dir);
    assert!(matches!(read, Err(StorageError::ObjectNotFound { .. })), "{read:?}");
    assert_eq!(store.key_stamp(BUCKET, "k").await.unwrap().map(|(_, deleted)| deleted), Some(true));

    // Expired tombstones take their marker dirs with them.
    assert_eq!(store.purge_tombstones(BUCKET, i64::MAX, 100).await.unwrap(), 1);
    assert!(store.key_stamp(BUCKET, "k").await.unwrap().is_none());
    assert_eq!(trash_count(&store).await, 3);
}

#[tokio::test]
async fn a_versioned_bucket_rebuilds_its_versions_and_markers_from_the_tree() {
    let (_tmp, store) = fresh().await;
    store.set_bucket_versioning(BUCKET, BucketVersioning::Enabled).await.unwrap();
    for key in ["a", "b"] {
        put(&store, key, b"1").await;
        put(&store, key, b"22").await;
    }
    store.delete_object(BUCKET, "b").await.unwrap();
    put(&store, "b", b"333").await;
    store.delete_object(BUCKET, "a").await.unwrap();
    let before_a = versions(&store, "a").await;
    let before_b = versions(&store, "b").await;
    let before = store.list_object_versions(BUCKET, "", "", "", 100).await.unwrap();

    store.forget_index(BUCKET);
    tokio::fs::remove_dir_all(store.layout().bucket_dir(BUCKET).unwrap().join("index.rocksdb")).await.unwrap();
    assert_eq!(store.rebuild_index(BUCKET).await.unwrap(), 7, "every version and marker is indexed");
    assert_eq!(versions(&store, "a").await, before_a);
    assert_eq!(versions(&store, "b").await, before_b);
    assert_eq!(store.list_object_versions(BUCKET, "", "", "", 100).await.unwrap(), before);
    assert_eq!(body_of(&store, "b", None).await, b"333");
    assert!(matches!(store.read_object(BUCKET, "a").await, Err(StorageError::ObjectNotFound { .. })));
    assert!(findings(&store).await.is_empty());
}

#[tokio::test]
async fn list_object_versions_pages_through_versions_with_both_markers() {
    let (_tmp, store) = fresh().await;
    store.set_bucket_versioning(BUCKET, BucketVersioning::Enabled).await.unwrap();
    for key in ["a", "b", "c"] {
        for n in 0..3 {
            put(&store, key, format!("{key}{n}").as_bytes()).await;
        }
    }
    store.delete_object(BUCKET, "b").await.unwrap();
    let mut all = Vec::new();
    let (mut key_marker, mut version_marker) = (String::new(), String::new());
    let mut pages = 0;
    loop {
        let page = store.list_object_versions(BUCKET, "", &key_marker, &version_marker, 4).await.unwrap();
        pages += 1;
        assert!(page.entries.len() <= 4);
        all.extend(page.entries.iter().map(|e| (e.key.clone(), e.version_id.clone(), e.is_latest, e.delete_marker)));
        if !page.is_truncated {
            break;
        }
        key_marker = page.next_key_marker.unwrap();
        version_marker = page.next_version_id_marker.unwrap();
        assert!(pages < 10);
    }
    assert_eq!(pages, 3, "10 entries in pages of 4");
    assert_eq!(all.len(), 10);
    let keys: Vec<&str> = all.iter().map(|e| e.0.as_str()).collect();
    assert_eq!(keys, ["a", "a", "a", "b", "b", "b", "b", "c", "c", "c"], "key order, every version");
    assert!(all.iter().filter(|e| e.2).count() == 3, "one latest per key");
    assert!(all[3].3 && all[3].2, "b's newest is its delete marker");
    for w in all.chunks(3) {
        if w.len() == 3 && w[0].0 == w[2].0 {
            assert!(w[0].1 > w[1].1 && w[1].1 > w[2].1, "newest first within a key");
        }
    }
}
