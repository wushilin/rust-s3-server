# MinIO Mint S3 Compatibility Test Report

Date: 2026-07-30

## Target

- Server: `target/release/rusts3`
- Endpoint tested from Podman: `127.0.0.1:29015` (`--network=host`)
- Access key: `minioadmin`
- Secret key: `minioadmin`
- Mint mode: `core`
- Continue on failure: `RUN_ON_FAIL=1`

## Commands

```sh
cargo test
cargo build --release
target/release/rusts3 -c mint-test-config.yaml

podman run --rm --security-opt label=disable --network=host \
  -e SERVER_ENDPOINT=127.0.0.1:29015 \
  -e ACCESS_KEY=minioadmin \
  -e SECRET_KEY=minioadmin \
  -e ENABLE_HTTPS=0 \
  -e RUN_ON_FAIL=1 \
  -e SERVER_REGION=us-east-1 \
  -e MINT_MODE=core \
  -v /tmp/rusts3-mint-log:/mint/log \
  minio/mint
```

## Result Summary

Parsed from `log.json`.

| Status | Count | Previous run (2026-07-19) |
| --- | ---: | ---: |
| PASS | 362 | 361 |
| FAIL | 25 | 40 |
| NA | 30 | 15 |
| TOTAL | 417 | 416 |

MINT executed all 15 suites and reported 8 suite-level successes (previously 7).

The failure count dropped and the not-applicable count rose for the same reason:
unimplemented bucket sub-resources are now refused with `NotImplemented` instead
of being silently accepted, so clients mark those features not-applicable rather
than asserting against a fabricated success. See "Behavior Changes" below.

## Suite Breakdown

| Suite | PASS | FAIL | NA | Previous (P/F/NA) |
| --- | ---: | ---: | ---: | --- |
| aws-sdk-go | 2 | 1 | 0 | 2/1/0 |
| aws-sdk-php | 12 | 0 | 1 | 12/1/0 |
| aws-sdk-ruby | 13 | 0 | 0 | 13/0/0 |
| awscli | 16 | 0 | 0 | 13/1/0 |
| healthcheck | 6 | 0 | 0 | 6/0/0 |
| mc | 26 | 1 | 0 | 26/1/0 |
| minio-go | 0 | 1 | 1 | 0/1/1 |
| minio-java | 34 | 16 | 23 | 43/21/11 |
| minio-js | 234 | 3 | 0 | 225/11/0 |
| minio-py | 11 | 1 | 5 | 13/1/3 |
| s3cmd | 8 | 0 | 0 | 8/0/0 |
| s3select | 0 | 1 | 0 | 0/1/0 |
| versioning | 0 | 1 | 0 | 0/1/0 |

## Behavior Changes Since the Previous Run

Two fixes in this run alter what Mint sees.

1. **`GET /{bucket}?versions` no longer reports retired trash blobs.** The bucket
   is unversioned, so each key has exactly one reportable version: the live one.
   Retired blobs stay in trash as an operator recovery buffer, but no API can
   address them — there is no `?versionId` route — and they would claim the same
   `VersionId=null` as the live object.

   This moves version-count assertions from PASS to NA/skipped. `minio-java`'s
   `testListObjects` and three `minio-js` version tests were previously satisfied
   by those phantom rows: they enable versioning, PUT one key N times, and expect
   the listing to return N rows. They were passing on rows a client could never
   fetch.

2. **Unimplemented bucket sub-resources are refused.** `PUT /{bucket}?<sub>` used
   to fall through to the idempotent CreateBucket and answer `200`, so
   `put-bucket-versioning` reported success on a server that does not version.
   `DELETE /{bucket}?<sub>` fell through to DeleteBucket — `delete-bucket-tagging`
   on an empty bucket **deleted the bucket**. Both verbs now return
   `501 NotImplemented` for the 19 sub-resources this server does not implement.

   Because `put-bucket-versioning` now fails honestly, Mint's clients detect that
   versioning is unsupported and skip their versioning groups instead of
   asserting against it — which is why `minio-java` completes its run (it
   previously aborted at `testListObjects`, losing 43 checks) and `minio-js`
   dropped from 11 failures to 3.

## Remaining Compatibility Gaps

All 25 failures are in feature areas listed as out of scope in the README:

- **S3 Select**: `aws-sdk-go`, `minio-js`, and the `s3select` suite fail on
  `SelectObjectContent` with `501 NotImplemented`.
- **Object versioning**: the dedicated `versioning` suite fails at
  `GetBucketVersioning` with `501 NotImplemented`.
- **Bucket policy via the S3 API**: MinIO Go `SetBucketPolicy` fails with
  `SignatureDoesNotMatch`; MinIO JS `setBucketPolicy` fails.
- **MinIO admin APIs**: eight Java admin tests (add/list/delete users and canned
  policies) hit `/minio/admin/v3/...`, which this server does not serve.
- **Bucket/object tagging and encryption**: Java `getObjectTags` and
  `getBucketEncryption` fail XML parsing; `deleteObjectTags` hits `NoSuchKey`.
- **Object lock / legal hold**: Java `isObjectLegalHoldEnabled` fails XML parsing.
- **Notifications**: Java `listenBucketNotification` fails with
  `SignatureDoesNotMatch`; the JS invalid-event test expects MinIO's error text.
- **Snowball archive extraction**: Java and Python snowball uploads fail, with a
  `removeBucket` / `BucketNotEmpty` cascade behind them.
- **Bucket replication**: `mc test_bucket_replication` fails configuring a remote.

## Passing Coverage

Core behavior passed broadly across the suites:

- bucket create/list/head/delete
- object put/head/get/delete
- range and partial reads
- ListObjects v1/v2, prefixes and delimiters, and **paginated listing with
  continuation tokens under `encoding-type=url`**
- multipart upload, list uploads/list parts, abort, and completion paths
- copy and compose object flows
- presigned GET/PUT and browser POST flows
- MinIO JS non-versioned force-delete-prefix
- health and metrics endpoints
- `s3cmd`, `awscli`, `aws-sdk-ruby`, `aws-sdk-php`, `aws-sdk-java`,
  `aws-sdk-java-v2`, and healthcheck suites had no failures in this run.
