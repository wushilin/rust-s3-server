//! End-to-end coverage for the two server APIs that expose a completed
//! multipart object's part layout: `GetObjectAttributes` (`?attributes`) and
//! `partNumber` on `GET`/`HEAD`.
//!
//! Driven by `aws-sdk-s3` against a live `rusts3`, not by hand-built HTTP.
//! The point of these APIs is that a *real* S3 client can read them, so a test
//! that parsed the XML itself would only prove the server agrees with this
//! file. Anything the SDK models but cannot decode -- a wrong element name, a
//! member sent under its modelled name instead of its wire name -- shows up
//! here as a `None` where a value is expected.
//!
//! Every multipart assertion runs against a genuinely large object: parts of
//! 5 MiB or more, because the server enforces S3's minimum part size on every
//! part but the last, so a small object cannot produce a real multi-part
//! layout to inspect.

mod common;
use common::TestServer;

use aws_sdk_s3::types::ObjectAttributes;
use md5::{Digest, Md5};

/// 5 MiB — the minimum S3 (and this server) accepts for any part but the last.
const PART_SIZE: usize = 5 * 1024 * 1024;
/// 17 MiB: four parts of 5/5/5/2 MiB. The short tail matters — a layout of
/// uniform parts would pass even if the server reported a guessed part size
/// instead of the real one.
const OBJECT_SIZE: usize = 17 * 1024 * 1024;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("build tokio runtime")
}

/// Deterministic pseudo-random bytes: compressible-looking data would let a
/// storage bug that dropped or duplicated a block still produce the right MD5.
fn payload(len: usize) -> Vec<u8> {
    (0..len as u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
        .collect()
}

fn md5_hex(bytes: &[u8]) -> String {
    Md5::digest(bytes).iter().map(|b| format!("{b:02x}")).collect::<String>()
}

fn md5_raw(bytes: &[u8]) -> Vec<u8> {
    Md5::digest(bytes).to_vec()
}

fn md5_base64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(md5_raw(bytes))
}

/// The S3 composite ETag: MD5 of the concatenated raw part digests, suffixed
/// with the part count. This is the whole reason a client needs the part
/// boundaries — without them the digest cannot be reproduced.
fn composite_etag(parts: &[&[u8]]) -> String {
    let mut concatenated = Vec::with_capacity(parts.len() * 16);
    for part in parts {
        concatenated.extend_from_slice(&md5_raw(part));
    }
    format!("{}-{}", md5_hex(&concatenated), parts.len())
}

fn unquote(etag: &str) -> &str {
    etag.trim_matches('"')
}

/// Uploads `data` as a real multipart object via rs3 and returns the bucket
/// and key.
fn upload_large_multipart(server: &TestServer, bucket: &str, name: &str, data: &[u8]) -> String {
    server.rs3_ok(&["mb", &format!("test/{bucket}")]);
    let src = server.dir.path().join(name);
    std::fs::write(&src, data).expect("write source file");
    let key = format!("test/{bucket}/{name}");
    server.rs3_ok(&["put", "--part-size", "5MiB", src.to_str().unwrap(), &key]);
    key
}

#[test]
fn get_object_attributes_reports_the_exact_part_layout_that_rebuilds_the_etag() {
    let server = TestServer::start();
    let data = payload(OBJECT_SIZE);
    upload_large_multipart(&server, "attrs", "big.bin", &data);

    rt().block_on(async {
        let client = server.sdk_client().await;
        let attrs = client
            .get_object_attributes()
            .bucket("attrs")
            .key("big.bin")
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::ObjectSize)
            .object_attributes(ObjectAttributes::StorageClass)
            .object_attributes(ObjectAttributes::ObjectParts)
            .send()
            .await
            .expect("get_object_attributes");

        assert_eq!(attrs.object_size(), Some(OBJECT_SIZE as i64));
        assert_eq!(
            attrs.storage_class().map(|sc| sc.as_str()),
            Some("STANDARD")
        );

        let parts = attrs
            .object_parts()
            .expect("multipart object reports parts");
        assert_eq!(parts.total_parts_count(), Some(4));
        assert_eq!(parts.is_truncated(), Some(false));
        let sizes: Vec<i64> = parts.parts().iter().map(|p| p.size().unwrap()).collect();
        assert_eq!(
            sizes,
            vec![
                PART_SIZE as i64,
                PART_SIZE as i64,
                PART_SIZE as i64,
                (OBJECT_SIZE - 3 * PART_SIZE) as i64
            ],
            "reported layout must be the real 5/5/5/2 split, not a uniform guess"
        );
        assert_eq!(
            parts
                .parts()
                .iter()
                .map(|p| p.part_number().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );

        // The payoff: slice the local bytes at the *reported* boundaries and
        // rebuild the composite ETag. If the boundaries were wrong by a single
        // byte this cannot match.
        let mut offset = 0usize;
        let mut slices: Vec<&[u8]> = Vec::new();
        for size in &sizes {
            let size = *size as usize;
            slices.push(&data[offset..offset + size]);
            offset += size;
        }
        assert_eq!(offset, OBJECT_SIZE, "reported sizes must cover the object");
        let rebuilt = composite_etag(&slices);
        assert_eq!(
            unquote(attrs.e_tag().expect("ETag attribute")),
            rebuilt,
            "composite ETag recomputed from the reported layout must match the server's"
        );
        // Verbatim, because the SDK models this field as an opaque string and
        // returns whatever the body carried -- only a verbatim assertion pins
        // the form. Real S3 sends this one ETag *unquoted*, alone among its
        // ETags, and this asserts we do the same.
        assert_eq!(attrs.e_tag(), Some(rebuilt.as_str()));

        // And the per-part digests the server volunteers must agree with the
        // same slices, so a downloader can verify a part on arrival instead of
        // waiting for the whole object.
        for (part, slice) in parts.parts().iter().zip(&slices) {
            assert_eq!(
                part.checksum_md5(),
                Some(md5_base64(slice)).as_deref(),
                "part {} checksum",
                part.part_number().unwrap()
            );
        }
    });
}

#[test]
fn get_object_attributes_paginates_parts_and_resumes_at_the_marker() {
    let server = TestServer::start();
    let data = payload(OBJECT_SIZE);
    upload_large_multipart(&server, "attrspage", "big.bin", &data);

    rt().block_on(async {
        let client = server.sdk_client().await;
        let first = client
            .get_object_attributes()
            .bucket("attrspage")
            .key("big.bin")
            .object_attributes(ObjectAttributes::ObjectParts)
            .max_parts(2)
            .send()
            .await
            .expect("first page");
        let first = first.object_parts().expect("parts");
        assert_eq!(first.parts().len(), 2);
        assert_eq!(first.is_truncated(), Some(true));
        // The total is the object's, not the page's -- a client sizing its
        // work from this must see 4.
        assert_eq!(first.total_parts_count(), Some(4));
        assert_eq!(first.max_parts(), Some(2));
        let marker = first
            .next_part_number_marker()
            .expect("truncated page carries a marker")
            .to_string();
        assert_eq!(marker, "2");

        let second = client
            .get_object_attributes()
            .bucket("attrspage")
            .key("big.bin")
            .object_attributes(ObjectAttributes::ObjectParts)
            .max_parts(2)
            .part_number_marker(marker)
            .send()
            .await
            .expect("second page");
        let second = second.object_parts().expect("parts");
        assert_eq!(
            second
                .parts()
                .iter()
                .map(|p| p.part_number().unwrap())
                .collect::<Vec<_>>(),
            vec![3, 4],
            "the marker must resume strictly after part 2"
        );
        assert_eq!(second.is_truncated(), Some(false));
        // AWS sends the marker on a final page too -- it is "last part number
        // in this page", not "there is more".
        assert_eq!(second.next_part_number_marker(), Some("4"));
    });
}

#[test]
fn get_object_attributes_omits_part_structure_for_a_plain_put() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/attrssingle"]);
    let data = payload(64 * 1024);
    let src = server.dir.path().join("small.bin");
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/attrssingle/small.bin"]);

    rt().block_on(async {
        let client = server.sdk_client().await;
        let attrs = client
            .get_object_attributes()
            .bucket("attrssingle")
            .key("small.bin")
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::ObjectSize)
            .object_attributes(ObjectAttributes::ObjectParts)
            .send()
            .await
            .expect("get_object_attributes");

        assert_eq!(attrs.object_size(), Some(data.len() as i64));
        // No part structure to report: an object that never went through a
        // multipart upload has a plain-MD5 ETag the client verifies unaided.
        assert!(
            attrs.object_parts().is_none(),
            "a single-part object must not advertise a part layout"
        );
        // Unquoted, as real S3 sends it for this operation only.
        assert_eq!(attrs.e_tag(), Some(md5_hex(&data)).as_deref());
    });
}

#[test]
fn get_object_attributes_projects_only_what_was_asked_for() {
    let server = TestServer::start();
    let data = payload(OBJECT_SIZE);
    upload_large_multipart(&server, "attrsproj", "big.bin", &data);

    rt().block_on(async {
        let client = server.sdk_client().await;
        let attrs = client
            .get_object_attributes()
            .bucket("attrsproj")
            .key("big.bin")
            .object_attributes(ObjectAttributes::ObjectSize)
            .send()
            .await
            .expect("get_object_attributes");

        assert_eq!(attrs.object_size(), Some(OBJECT_SIZE as i64));
        // Unrequested attributes are absent, not empty: this operation is a
        // projection, and a client that asked for one field paying for the
        // full part list of a 10,000-part object would be a real cost.
        assert!(attrs.e_tag().is_none());
        assert!(attrs.object_parts().is_none());
        assert!(attrs.storage_class().is_none());
    });
}

#[test]
fn get_object_attributes_rejects_an_unknown_attribute_name() {
    let server = TestServer::start();
    let data = payload(64 * 1024);
    let server_bucket = "attrsbad";
    server.rs3_ok(&["mb", &format!("test/{server_bucket}")]);
    let src = server.dir.path().join("small.bin");
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&[
        "put",
        src.to_str().unwrap(),
        &format!("test/{server_bucket}/small.bin"),
    ]);

    rt().block_on(async {
        let client = server.sdk_client().await;
        // `ObjectAttributes` is an open enum in the SDK, so an unmodelled name
        // reaches the wire unchanged -- which is what lets this exercise the
        // server's own validation.
        let err = client
            .get_object_attributes()
            .bucket(server_bucket)
            .key("small.bin")
            .object_attributes(ObjectAttributes::from("Bogus"))
            .send()
            .await
            .expect_err("unknown attribute must be rejected");
        let service = err.into_service_error();
        assert_eq!(
            service.meta().code(),
            Some("InvalidArgument"),
            "unexpected error: {service:?}"
        );
    });
}

#[test]
fn part_number_probe_reveals_the_layout_and_serves_the_right_bytes() {
    let server = TestServer::start();
    let data = payload(OBJECT_SIZE);
    upload_large_multipart(&server, "partnum", "big.bin", &data);

    rt().block_on(async {
        let client = server.sdk_client().await;

        // One HEAD answers both questions a verifying downloader has: is this
        // object multipart, and how is it split.
        let head = client
            .head_object()
            .bucket("partnum")
            .key("big.bin")
            .part_number(1)
            .send()
            .await
            .expect("head part 1");
        assert_eq!(head.parts_count(), Some(4));
        assert_eq!(head.content_length(), Some(PART_SIZE as i64));
        assert_eq!(
            head.content_range().map(str::to_string),
            Some(format!("bytes 0-{}/{OBJECT_SIZE}", PART_SIZE - 1))
        );

        // The last part is the short one; its length is what a uniform-part
        // guess would get wrong.
        let tail = client
            .head_object()
            .bucket("partnum")
            .key("big.bin")
            .part_number(4)
            .send()
            .await
            .expect("head part 4");
        assert_eq!(
            tail.content_length(),
            Some((OBJECT_SIZE - 3 * PART_SIZE) as i64)
        );

        // And a GET for a part returns exactly that part's bytes.
        let got = client
            .get_object()
            .bucket("partnum")
            .key("big.bin")
            .part_number(2)
            .send()
            .await
            .expect("get part 2");
        assert_eq!(got.parts_count(), Some(4));
        let body = got
            .body
            .collect()
            .await
            .expect("collect part body")
            .to_vec();
        assert_eq!(body.len(), PART_SIZE);
        assert_eq!(
            md5_hex(&body),
            md5_hex(&data[PART_SIZE..2 * PART_SIZE]),
            "part 2 must be the second 5 MiB of the object"
        );
    });
}

#[test]
fn part_number_past_the_end_is_unsatisfiable() {
    let server = TestServer::start();
    let data = payload(OBJECT_SIZE);
    upload_large_multipart(&server, "partnumbad", "big.bin", &data);

    rt().block_on(async {
        let client = server.sdk_client().await;
        let err = client
            .head_object()
            .bucket("partnumbad")
            .key("big.bin")
            .part_number(5)
            .send()
            .await
            .expect_err("part 5 of a 4-part object must fail");
        // HEAD carries no body, so only the status survives; 416 is what
        // distinguishes "no such part" from "no such object" (404).
        assert_eq!(
            err.raw_response().map(|r| r.status().as_u16()),
            Some(416),
            "unexpected error: {err:?}"
        );

        // A malformed part number is a different failure with a different
        // status on AWS -- 400, not 416 -- because no object could satisfy it.
        let err = client
            .get_object()
            .bucket("partnumbad")
            .key("big.bin")
            .part_number(0)
            .send()
            .await
            .expect_err("partNumber=0 is not a part number");
        assert_eq!(
            err.raw_response().map(|r| r.status().as_u16()),
            Some(400),
            "unexpected error: {err:?}"
        );
    });
}

#[test]
fn part_number_one_returns_a_whole_single_part_object_without_a_part_count() {
    let server = TestServer::start();
    server.rs3_ok(&["mb", "test/partnumsingle"]);
    let data = payload(64 * 1024);
    let src = server.dir.path().join("small.bin");
    std::fs::write(&src, &data).unwrap();
    server.rs3_ok(&["put", src.to_str().unwrap(), "test/partnumsingle/small.bin"]);

    rt().block_on(async {
        let client = server.sdk_client().await;
        let head = client
            .head_object()
            .bucket("partnumsingle")
            .key("small.bin")
            .part_number(1)
            .send()
            .await
            .expect("head part 1 of a single-part object");
        // The absent part count is the signal: this object's ETag is a plain
        // MD5, so no composite reconstruction is needed. AWS still answers
        // with a 206 and a whole-object Content-Range here.
        assert_eq!(head.parts_count(), None);
        assert_eq!(head.content_length(), Some(data.len() as i64));
        assert_eq!(
            head.content_range().map(str::to_string),
            Some(format!("bytes 0-{}/{}", data.len() - 1, data.len()))
        );
        assert_eq!(unquote(head.e_tag().unwrap()), md5_hex(&data));

        let err = client
            .head_object()
            .bucket("partnumsingle")
            .key("small.bin")
            .part_number(2)
            .send()
            .await
            .expect_err("a single-part object has no part 2");
        assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(416));
    });
}

#[test]
fn range_and_part_number_together_are_refused_rather_than_silently_resolved() {
    let server = TestServer::start();
    let data = payload(OBJECT_SIZE);
    upload_large_multipart(&server, "partnumboth", "big.bin", &data);

    rt().block_on(async {
        let client = server.sdk_client().await;
        let err = client
            .get_object()
            .bucket("partnumboth")
            .key("big.bin")
            .part_number(2)
            .range("bytes=0-1023")
            .send()
            .await
            .expect_err("two selectors for one slot must be refused");
        let service = err.into_service_error();
        // Honouring one and dropping the other would hand back bytes the
        // caller never asked for, under a status that says it worked.
        assert_eq!(
            service.meta().code(),
            Some("InvalidRequest"),
            "unexpected error: {service:?}"
        );
    });
}

#[test]
fn ordinary_reads_are_unchanged_by_the_new_selector() {
    // The partNumber path sits in the middle of the read handler; a full GET
    // and a byte-range GET must behave exactly as before.
    let server = TestServer::start();
    let data = payload(OBJECT_SIZE);
    upload_large_multipart(&server, "partnumplain", "big.bin", &data);

    rt().block_on(async {
        let client = server.sdk_client().await;

        let whole = client
            .get_object()
            .bucket("partnumplain")
            .key("big.bin")
            .send()
            .await
            .expect("plain get");
        assert_eq!(whole.parts_count(), None, "no partNumber, no part count");
        let body = whole.body.collect().await.unwrap().to_vec();
        assert_eq!(body.len(), OBJECT_SIZE);
        assert_eq!(md5_hex(&body), md5_hex(&data));

        // A byte range that deliberately straddles a part boundary.
        let start = PART_SIZE - 1024;
        let end = PART_SIZE + 1023;
        let ranged = client
            .get_object()
            .bucket("partnumplain")
            .key("big.bin")
            .range(format!("bytes={start}-{end}"))
            .send()
            .await
            .expect("ranged get");
        assert_eq!(
            ranged.content_range().map(str::to_string),
            Some(format!("bytes {start}-{end}/{OBJECT_SIZE}"))
        );
        let body = ranged.body.collect().await.unwrap().to_vec();
        assert_eq!(body, data[start..=end]);
    });
}
