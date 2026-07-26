//! `object_store` ⇄ rusts3 compatibility tests.
//!
//! Each test drives the real `object_store` S3 client (the same one apps like
//! Delta Lake, DataFusion, Lance, and Iceberg use) against a running rusts3,
//! asserting the behaviour `object_store` promises its callers. The harness
//! (`run.sh`) provides a running server, a pre-created bucket, and credentials
//! via environment variables; these tests only exercise the object API.
//!
//! Tests share one bucket and run concurrently, so every test scopes its keys
//! under a unique prefix and cleans up after itself.

use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::aws::S3ConditionalPut;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, WriteMultipart};

fn store() -> Box<dyn ObjectStore> {
    let var = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} must be set by run.sh"));
    let s3 = AmazonS3Builder::new()
        .with_endpoint(var("RUSTS3_COMPAT_ENDPOINT"))
        .with_bucket_name(var("RUSTS3_COMPAT_BUCKET"))
        .with_access_key_id(var("RUSTS3_COMPAT_ACCESS_KEY"))
        .with_secret_access_key(var("RUSTS3_COMPAT_SECRET_KEY"))
        .with_region("us-east-1")
        .with_allow_http(true)
        // rusts3 is path-style (endpoint/bucket/key) unless a public hostname
        // is configured, which the harness does not set.
        .with_virtual_hosted_style_request(false)
        // Explicit (also the object_store default): atomic PutMode::Create is
        // expressed as `If-None-Match: *`, the same standard precondition MinIO
        // and R2 honour.
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .expect("build object_store S3 client");
    Box::new(s3)
}

fn body(s: &str) -> PutPayload {
    Bytes::copy_from_slice(s.as_bytes()).into()
}

async fn get_bytes(store: &dyn ObjectStore, path: &Path) -> Bytes {
    store.get(path).await.unwrap().bytes().await.unwrap()
}

#[tokio::test]
async fn put_get_roundtrip() {
    let s = store();
    let p = Path::from("compat/put_get/hello.txt");
    s.put(&p, body("hello object_store")).await.unwrap();
    assert_eq!(&get_bytes(&*s, &p).await[..], b"hello object_store");
    s.delete(&p).await.unwrap();
}

#[tokio::test]
async fn overwrite_replaces_contents() {
    let s = store();
    let p = Path::from("compat/overwrite/x");
    s.put(&p, body("v1")).await.unwrap();
    s.put(&p, body("second version")).await.unwrap();
    assert_eq!(&get_bytes(&*s, &p).await[..], b"second version");
    s.delete(&p).await.unwrap();
}

#[tokio::test]
async fn empty_object() {
    let s = store();
    let p = Path::from("compat/empty/zero");
    s.put(&p, PutPayload::from_static(b"")).await.unwrap();
    let meta = s.head(&p).await.unwrap();
    assert_eq!(meta.size as u64, 0);
    assert_eq!(get_bytes(&*s, &p).await.len(), 0);
    s.delete(&p).await.unwrap();
}

#[tokio::test]
async fn head_reports_size_and_etag() {
    let s = store();
    let p = Path::from("compat/head/obj");
    let payload = "0123456789";
    s.put(&p, body(payload)).await.unwrap();
    let meta = s.head(&p).await.unwrap();
    assert_eq!(meta.size as u64, payload.len() as u64);
    assert!(meta.e_tag.is_some(), "object_store relies on ETag for caching");
    assert_eq!(meta.location, p);
    s.delete(&p).await.unwrap();
}

#[tokio::test]
async fn get_range_returns_slice() {
    let s = store();
    let p = Path::from("compat/range/data");
    s.put(&p, body("abcdefghij")).await.unwrap();
    let mid = s.get_range(&p, 2..5).await.unwrap();
    assert_eq!(&mid[..], b"cde");
    // A range past the end is clamped, matching S3/object_store semantics.
    let tail = s.get_range(&p, 8..10).await.unwrap();
    assert_eq!(&tail[..], b"ij");
    s.delete(&p).await.unwrap();
}

#[tokio::test]
async fn get_ranges_multi() {
    let s = store();
    let p = Path::from("compat/ranges/data");
    s.put(&p, body("abcdefghij")).await.unwrap();
    let parts = s.get_ranges(&p, &[0..3, 5..7]).await.unwrap();
    assert_eq!(&parts[0][..], b"abc");
    assert_eq!(&parts[1][..], b"fg");
    s.delete(&p).await.unwrap();
}

#[tokio::test]
async fn get_missing_maps_to_not_found() {
    let s = store();
    let err = s
        .get(&Path::from("compat/missing/never-written"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, object_store::Error::NotFound { .. }),
        "expected NotFound, got {err:?}"
    );
}

#[tokio::test]
async fn list_returns_all_under_prefix() {
    let s = store();
    let prefix = Path::from("compat/list");
    for n in ["a.txt", "b.txt", "sub/c.txt"] {
        s.put(&Path::from(format!("compat/list/{n}")), body("x")).await.unwrap();
    }
    let mut stream = s.list(Some(&prefix));
    let mut names = Vec::new();
    while let Some(item) = stream.next().await {
        names.push(item.unwrap().location.to_string());
    }
    assert!(names.iter().any(|n| n.ends_with("compat/list/a.txt")));
    assert!(names.iter().any(|n| n.ends_with("compat/list/b.txt")));
    // list() is fully recursive — the nested key is included.
    assert!(names.iter().any(|n| n.ends_with("compat/list/sub/c.txt")));
    for n in ["a.txt", "b.txt", "sub/c.txt"] {
        s.delete(&Path::from(format!("compat/list/{n}"))).await.unwrap();
    }
}

#[tokio::test]
async fn list_with_delimiter_yields_common_prefixes() {
    let s = store();
    let prefix = Path::from("compat/delim");
    s.put(&Path::from("compat/delim/top.txt"), body("x")).await.unwrap();
    s.put(&Path::from("compat/delim/sub/inner.txt"), body("x")).await.unwrap();
    let res = s.list_with_delimiter(Some(&prefix)).await.unwrap();
    // The top-level object is returned directly…
    assert!(res.objects.iter().any(|m| m.location.to_string().ends_with("compat/delim/top.txt")));
    // …and the nested key collapses to a common prefix, not an object.
    assert!(
        res.common_prefixes.iter().any(|p| p.to_string().ends_with("compat/delim/sub")),
        "common_prefixes = {:?}",
        res.common_prefixes
    );
    assert!(!res.objects.iter().any(|m| m.location.to_string().contains("inner.txt")));
    s.delete(&Path::from("compat/delim/top.txt")).await.unwrap();
    s.delete(&Path::from("compat/delim/sub/inner.txt")).await.unwrap();
}

#[tokio::test]
async fn list_with_offset_skips_earlier_keys() {
    let s = store();
    let prefix = Path::from("compat/offset");
    for n in ["01", "02", "03", "04"] {
        s.put(&Path::from(format!("compat/offset/{n}")), body("x")).await.unwrap();
    }
    let offset = Path::from("compat/offset/02");
    let mut stream = s.list_with_offset(Some(&prefix), &offset);
    let mut names = Vec::new();
    while let Some(item) = stream.next().await {
        names.push(item.unwrap().location.to_string());
    }
    // Offset is exclusive: only keys strictly after it come back.
    assert!(names.iter().all(|n| n.ends_with("/03") || n.ends_with("/04")));
    assert!(names.iter().any(|n| n.ends_with("/03")));
    for n in ["01", "02", "03", "04"] {
        s.delete(&Path::from(format!("compat/offset/{n}"))).await.unwrap();
    }
}

#[tokio::test]
async fn copy_duplicates_object() {
    let s = store();
    let src = Path::from("compat/copy/src");
    let dst = Path::from("compat/copy/dst");
    s.put(&src, body("payload")).await.unwrap();
    s.copy(&src, &dst).await.unwrap();
    assert_eq!(&get_bytes(&*s, &dst).await[..], b"payload");
    // Source still exists after a copy.
    assert_eq!(&get_bytes(&*s, &src).await[..], b"payload");
    s.delete(&src).await.unwrap();
    s.delete(&dst).await.unwrap();
}

#[tokio::test]
async fn rename_moves_object() {
    let s = store();
    let src = Path::from("compat/rename/src");
    let dst = Path::from("compat/rename/dst");
    s.put(&src, body("movable")).await.unwrap();
    s.rename(&src, &dst).await.unwrap();
    assert_eq!(&get_bytes(&*s, &dst).await[..], b"movable");
    // The source is gone after a rename (copy + delete).
    assert!(matches!(
        s.get(&src).await.unwrap_err(),
        object_store::Error::NotFound { .. }
    ));
    s.delete(&dst).await.unwrap();
}

#[tokio::test]
async fn multipart_upload_large_object() {
    let s = store();
    let p = Path::from("compat/multipart/big.bin");
    // Two 6 MiB chunks — above S3's 5 MiB minimum part size, so this is a real
    // multi-part upload, the path large writes take in object_store.
    let chunk = vec![b'z'; 6 * 1024 * 1024];
    let upload = s.put_multipart(&p).await.unwrap();
    let mut writer = WriteMultipart::new(upload);
    writer.write(&chunk);
    writer.write(&chunk);
    writer.finish().await.unwrap();

    let meta = s.head(&p).await.unwrap();
    assert_eq!(meta.size as u64, (chunk.len() * 2) as u64);
    let got = get_bytes(&*s, &p).await;
    assert_eq!(got.len(), chunk.len() * 2);
    assert!(got.iter().all(|&b| b == b'z'));
    s.delete(&p).await.unwrap();
}

#[tokio::test]
async fn multipart_abort_discards() {
    let s = store();
    let p = Path::from("compat/multipart/aborted.bin");
    let mut upload = s.put_multipart(&p).await.unwrap();
    let payload: PutPayload = Bytes::from(vec![b'q'; 6 * 1024 * 1024]).into();
    upload.put_part(payload).await.unwrap();
    upload.abort().await.unwrap();
    // Nothing is visible after an aborted upload.
    assert!(matches!(
        s.get(&p).await.unwrap_err(),
        object_store::Error::NotFound { .. }
    ));
}

#[tokio::test]
async fn conditional_put_create_is_atomic() {
    // PutMode::Create is how Delta/Iceberg do atomic commits over object_store:
    // the first writer wins, a racing second Create must fail. Requires the
    // server to honour If-None-Match on PUT.
    let s = store();
    let p = Path::from("compat/conditional/commit");
    let _ = s.delete(&p).await; // ensure a clean slate
    let create = |data: &'static str| {
        let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
        s.put_opts(&p, body(data), opts)
    };
    create("first").await.unwrap();
    let second = create("second").await;
    assert!(
        matches!(second, Err(object_store::Error::AlreadyExists { .. })),
        "second Create must be rejected, got {second:?}"
    );
    assert_eq!(&get_bytes(&*s, &p).await[..], b"first");
    s.delete(&p).await.unwrap();
}
