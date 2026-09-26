//! Proving that a downloaded object is the object the server has.
//!
//! An S3 ETag is a checksum the server already computed, and until now rs3
//! ignored it: a download was checked for *length* (every range wrote the
//! bytes it promised) but never for *content*. Length is the weaker claim —
//! a truncated body is caught, a corrupted one is not.
//!
//! Making the ETag usable takes one thing: the object's real part layout.
//!
//! - A single-`PUT` object's ETag **is** the MD5 of its bytes, so it can be
//!   checked with no extra information at all.
//! - A multipart object's ETag is `md5(md5(part₁) ‖ … ‖ md5(partₙ))-N`, which
//!   depends on where the *uploader* drew its part boundaries. `-N` and the
//!   object's length together do not determine that split, and the split is
//!   not recoverable from the object. It has to be asked for.
//!
//! Which is why the download planner no longer chooses its own split for a
//! multipart object. It used to cut the object into `--part-size` chunks,
//! which is fine for moving bytes and useless for checking them: an MD5 of an
//! arbitrary slice corresponds to nothing the server knows. Now each transfer
//! unit *is* one upload part, so every part can be hashed as it streams and
//! checked the moment it lands, and the whole ETag reassembled at the end.
//!
//! Two ways to learn the layout, tried in that order:
//!
//! 1. [`GetObjectAttributes`] returns every part's size in one request, and
//!    against `rust-s3-server` it also returns each part's MD5 — so parts can
//!    be checked individually rather than only in aggregate.
//! 2. `HeadObject` with `partNumber=k`, once per part, reading each part's
//!    `Content-Length`. More requests, but it works where (1) does not: real
//!    S3 only returns the part list for uploads made with a *composite*
//!    checksum, and a plain multipart upload — the common case — comes back
//!    with a bare `PartsCount` and no sizes.
//!
//! Both are exact. Neither guesses a uniform part size: a guess that happened
//! to be wrong would produce an ETag mismatch indistinguishable from real
//! corruption, which is the one failure this module must never invent.

use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::ObjectAttributes;
use futures::stream::{self, StreamExt, TryStreamExt};
use md5::{Digest, Md5};

/// One upload part: how many bytes, and (when the server volunteered it) the
/// digest those bytes must have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartSpec {
    pub size: u64,
    /// The part's MD5, when the server reported one. `None` only costs
    /// per-part checking; the composite ETag still verifies the whole object.
    pub expected_md5: Option<[u8; 16]>,
}

/// What an object's ETag says about how to verify it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ObjectLayout {
    /// A single-`PUT` object: the ETag is the MD5 of the whole body.
    Single { expected_md5: [u8; 16] },
    /// A multipart object, with the uploader's exact part boundaries.
    Multipart {
        parts: Vec<PartSpec>,
        /// The ETag as the server reports it, unquoted, e.g. `abc…-4`.
        expected_etag: String,
    },
    /// The ETag is not an MD5 and proves nothing: SSE-KMS/SSE-C objects, some
    /// server-side copies, and S3-compatible servers that make no promise
    /// about the value. Downloading proceeds; verification is skipped, and
    /// saying so is more honest than failing a file that is probably fine.
    Opaque { reason: String },
}

impl ObjectLayout {
    /// The transfer plan this layout implies: one range per upload part for a
    /// multipart object, or `None` when the caller should fall back to its own
    /// `--part-size` split.
    pub(crate) fn part_sizes(&self) -> Option<&[PartSpec]> {
        match self {
            ObjectLayout::Multipart { parts, .. } => Some(parts),
            _ => None,
        }
    }
}

/// Splits an unquoted ETag into `(hex_digest, part_count)`.
///
/// Returns `None` for anything that is not S3's MD5 shape — which is the
/// signal to skip verification rather than to fail.
fn parse_etag(etag: &str) -> Option<([u8; 16], Option<u32>)> {
    let etag = etag.trim().trim_matches('"');
    let (hex, parts) = match etag.split_once('-') {
        Some((hex, count)) => (hex, Some(count.parse::<u32>().ok()?)),
        None => (etag, None),
    };
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut digest = [0u8; 16];
    for (i, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    // A part count of zero is not a multipart object, it is a malformed ETag.
    match parts {
        Some(0) => None,
        other => Some((digest, other)),
    }
}

/// Formats the composite ETag a set of part digests must produce.
pub(crate) fn composite_etag(part_digests: &[[u8; 16]]) -> String {
    let mut concatenated = Vec::with_capacity(part_digests.len() * 16);
    for digest in part_digests {
        concatenated.extend_from_slice(digest);
    }
    format!("{}-{}", hex(&Md5::digest(&concatenated)), part_digests.len())
}

pub(crate) fn hex(digest: &[u8]) -> String {
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Establishes how `bucket/key` must be verified.
///
/// `etag` and `size` come from the `HeadObject` the caller already made, so a
/// single-part object costs no extra request at all — its ETag alone is the
/// answer.
pub(crate) async fn discover_layout(
    client: &Client,
    bucket: &str,
    key: &str,
    etag: Option<&str>,
    size: u64,
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) -> Result<ObjectLayout> {
    let Some(etag) = etag else {
        return Ok(ObjectLayout::Opaque {
            reason: "the server reported no ETag".to_string(),
        });
    };
    let Some((digest, part_count)) = parse_etag(etag) else {
        return Ok(ObjectLayout::Opaque {
            reason: format!("ETag `{}` is not an MD5", etag.trim_matches('"')),
        });
    };
    let Some(part_count) = part_count else {
        return Ok(ObjectLayout::Single {
            expected_md5: digest,
        });
    };

    let expected_etag = etag.trim().trim_matches('"').to_string();
    let parts = match parts_via_attributes(client, bucket, key, part_count, budget, progress).await
    {
        Ok(Some(parts)) => parts,
        // Not an error worth failing on: the operation is newer than many
        // S3-compatible servers, and real S3 answers it without part sizes for
        // uploads made without a composite checksum. Either way the per-part
        // HEAD path still gets exact boundaries.
        Ok(None) | Err(_) => {
            parts_via_head(client, bucket, key, part_count, budget, progress).await?
        }
    };

    // A layout that does not add up to the object cannot be verified against,
    // and the overwhelmingly likely cause is a server that accepted the
    // request and ignored it -- one that does not implement `partNumber`
    // answers every per-part HEAD with the *whole* object's length, so N parts
    // report N times the size.
    //
    // This degrades rather than fails. Refusing the download would mean rs3
    // could not fetch a multipart object at all from any S3-compatible server
    // without both discovery APIs, turning a missing *check* into a missing
    // *feature*. Verification is best-effort against an unknown server; the
    // transfer is not.
    if !layout_covers(&parts, size) {
        let total: u64 = parts.iter().map(|p| p.size).sum();
        return Ok(ObjectLayout::Opaque {
            reason: format!(
                "the server did not report a usable part layout (parts total {total} bytes, \
                 object is {size}) -- it likely does not implement GetObjectAttributes or \
                 partNumber"
            ),
        });
    }
    Ok(ObjectLayout::Multipart {
        parts,
        expected_etag,
    })
}

/// Does a discovered layout actually describe an object of this size?
///
/// The check that catches a server which accepted `partNumber` and ignored it:
/// such a server answers every per-part `HEAD` with the whole object's length,
/// so an N-part object reports N times its own size.
fn layout_covers(parts: &[PartSpec], size: u64) -> bool {
    parts.iter().map(|p| p.size).sum::<u64>() == size
}

/// One `GetObjectAttributes` (paginated) for the whole layout.
///
/// `Ok(None)` means "answered, but without a usable part list" — the shape
/// real S3 returns for a multipart upload that declared no composite checksum.
async fn parts_via_attributes(
    client: &Client,
    bucket: &str,
    key: &str,
    part_count: u32,
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) -> Result<Option<Vec<PartSpec>>> {
    let mut parts: Vec<PartSpec> = Vec::new();
    let mut marker: Option<String> = None;
    loop {
        let mut req = client
            .get_object_attributes()
            .bucket(bucket)
            .key(key)
            .object_attributes(ObjectAttributes::ObjectParts)
            .max_parts(1000);
        if let Some(marker) = marker.take() {
            req = req.part_number_marker(marker);
        }
        let resp = crate::budget::dispatch(
            budget,
            progress,
            crate::progress::TransferLabel {
                verb: crate::progress::Verb::Inspecting,
                path: format!("{bucket}/{key}"),
                part: None,
            },
            "GetObjectAttributes",
            req.send(),
        )
        .await?;
        let Some(page) = resp.object_parts() else {
            return Ok(None);
        };
        if page.parts().is_empty() {
            // A part list that is empty when the object has parts means the
            // server reported only a count. Nothing to build a plan from.
            return Ok(if parts.is_empty() { None } else { Some(parts) });
        }
        for part in page.parts() {
            parts.push(PartSpec {
                size: part.size().unwrap_or_default() as u64,
                expected_md5: part.checksum_md5().and_then(decode_md5_base64),
            });
        }
        if page.is_truncated() != Some(true) {
            break;
        }
        match page.next_part_number_marker() {
            Some(next) => marker = Some(next.to_string()),
            None => break,
        }
    }
    if parts.len() as u32 != part_count {
        return Ok(None);
    }
    Ok(Some(parts))
}

/// One `HeadObject` per part, reading each part's length.
///
/// The fallback for servers that will not list parts. Requests run through the
/// same `-P` budget as everything else, so a 10,000-part object cannot flood
/// the connection pool with head requests.
async fn parts_via_head(
    client: &Client,
    bucket: &str,
    key: &str,
    part_count: u32,
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) -> Result<Vec<PartSpec>> {
    let sizes: Vec<(u32, u64)> = stream::iter((1..=part_count).map(|number| {
        let client = client.clone();
        let budget = budget.clone();
        let progress = progress.cloned();
        let bucket = bucket.to_string();
        let key = key.to_string();
        async move {
            let resp = crate::budget::dispatch(
                &budget,
                progress.as_ref(),
                crate::progress::TransferLabel {
                    verb: crate::progress::Verb::Inspecting,
                    path: format!("{bucket}/{key}"),
                    part: Some((number as u64, part_count as u64)),
                },
                "HeadObject",
                client
                    .head_object()
                    .bucket(&bucket)
                    .key(&key)
                    .part_number(number as i32)
                    .send(),
            )
            .await
            .with_context(|| format!("stat `{bucket}/{key}` part {number}"))?;
            Ok::<(u32, u64), anyhow::Error>((
                number,
                resp.content_length().unwrap_or_default() as u64,
            ))
        }
    }))
    .buffer_unordered(8)
    .try_collect()
    .await?;

    let mut sizes = sizes;
    sizes.sort_by_key(|(number, _)| *number);
    Ok(sizes
        .into_iter()
        .map(|(_, size)| PartSpec {
            size,
            expected_md5: None,
        })
        .collect())
}

fn decode_md5_base64(value: &str) -> Option<[u8; 16]> {
    use base64::Engine as _;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(value.trim())
        .ok()?;
    raw.try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_etag_reads_single_and_multipart_shapes() {
        let (digest, parts) = parse_etag("d41d8cd98f00b204e9800998ecf8427e").unwrap();
        assert_eq!(hex(&digest), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(parts, None);

        let (digest, parts) = parse_etag("\"d41d8cd98f00b204e9800998ecf8427e-4\"").unwrap();
        assert_eq!(hex(&digest), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(parts, Some(4));
    }

    #[test]
    fn parse_etag_declines_anything_that_is_not_an_md5() {
        // SSE-KMS ETags, server-side copies on some backends, and servers that
        // promise nothing. Each must skip verification, not fail the download.
        assert!(parse_etag("not-a-digest").is_none());
        assert!(parse_etag("").is_none());
        assert!(parse_etag("d41d8cd98f00b204e9800998ecf8427").is_none());
        assert!(parse_etag("d41d8cd98f00b204e9800998ecf8427e-0").is_none());
        assert!(parse_etag("d41d8cd98f00b204e9800998ecf8427e-x").is_none());
        assert!(parse_etag("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_none());
    }

    #[test]
    fn composite_etag_matches_the_s3_construction() {
        // md5("a"*4) and md5("b"*4), concatenated raw, then hashed.
        let a: [u8; 16] = Md5::digest(b"aaaa").into();
        let b: [u8; 16] = Md5::digest(b"bbbb").into();
        let mut expected_input = Vec::new();
        expected_input.extend_from_slice(&a);
        expected_input.extend_from_slice(&b);
        assert_eq!(
            composite_etag(&[a, b]),
            format!("{}-2", hex(&Md5::digest(&expected_input)))
        );
    }

    #[test]
    fn decode_md5_base64_round_trips() {
        // base64 of md5("") -- what GetObjectAttributes reports per part.
        assert_eq!(
            decode_md5_base64("1B2M2Y8AsgTpgAmY7PhCfg==").map(|d| hex(&d)),
            Some("d41d8cd98f00b204e9800998ecf8427e".to_string())
        );
        assert_eq!(decode_md5_base64("not base64!!"), None);
        // Right encoding, wrong length: not a 128-bit digest.
        assert_eq!(decode_md5_base64("AAAA"), None);
    }

    #[test]
    fn layout_covers_rejects_the_ignored_part_number_signature() {
        let spec = |size| PartSpec {
            size,
            expected_md5: None,
        };
        // The real shape: a 12 MiB object in 3 parts, from a server that
        // answered every per-part HEAD with the whole object's length.
        let ignored = vec![spec(12_582_912), spec(12_582_912), spec(12_582_912)];
        assert!(!layout_covers(&ignored, 12_582_912));

        // A genuine ragged layout adds up and must be accepted.
        let real = vec![spec(5_242_880), spec(5_242_880), spec(2_097_152)];
        assert!(layout_covers(&real, 12_582_912));

        // Off by a single byte is still not a description of this object.
        assert!(!layout_covers(&real, 12_582_913));
    }

    #[test]
    fn layout_exposes_a_plan_only_for_multipart_objects() {
        let single = ObjectLayout::Single {
            expected_md5: [0u8; 16],
        };
        assert!(single.part_sizes().is_none());
        let opaque = ObjectLayout::Opaque {
            reason: "test".into(),
        };
        assert!(opaque.part_sizes().is_none());
        let multi = ObjectLayout::Multipart {
            parts: vec![PartSpec {
                size: 5,
                expected_md5: None,
            }],
            expected_etag: "abc-1".into(),
        };
        assert_eq!(multi.part_sizes().map(<[PartSpec]>::len), Some(1));
    }
}
