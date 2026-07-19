# MinIO Mint S3 Compatibility Test Report

Date: 2026-07-19

## Target

- Server: `target/release/rusts3`
- Config: `/tmp/rusts3-mint-config-afterfix2-20260719.yaml`
- Endpoint tested from Podman: `host.containers.internal:29015`
- Access key: `minioadmin`
- Secret key: `minioadmin`
- Mint mode: `core`
- Continue on failure: `RUN_ON_FAIL=1`
- Mint logs: `/tmp/rusts3-mint-log-afterfix2-20260719`

## Commands

```sh
cargo test
cargo build --release
target/release/rusts3 -c /tmp/rusts3-mint-config-afterfix2-20260719.yaml

podman run --rm --security-opt label=disable \
  -e SERVER_ENDPOINT=host.containers.internal:29015 \
  -e ACCESS_KEY=minioadmin \
  -e SECRET_KEY=minioadmin \
  -e ENABLE_HTTPS=0 \
  -e RUN_ON_FAIL=1 \
  -e SERVER_REGION=us-east-1 \
  -v /tmp/rusts3-mint-log-afterfix2-20260719:/mint/log \
  minio/mint
```

## Result Summary

Parsed from `/tmp/rusts3-mint-log-afterfix2-20260719/log.json`.

| Status | Count |
| --- | ---: |
| PASS | 361 |
| FAIL | 40 |
| NA | 15 |
| TOTAL | 416 |

MINT executed all 15 suites and reported 7 suite-level successes.

## Suite Breakdown

| Suite | PASS | FAIL | NA |
| --- | ---: | ---: | ---: |
| aws-sdk-go | 2 | 1 | 0 |
| aws-sdk-php | 12 | 1 | 0 |
| aws-sdk-ruby | 13 | 0 | 0 |
| awscli | 13 | 1 | 0 |
| healthcheck | 6 | 0 | 0 |
| mc | 26 | 1 | 0 |
| minio-go | 0 | 1 | 1 |
| minio-java | 43 | 21 | 11 |
| minio-js | 225 | 11 | 0 |
| minio-py | 13 | 1 | 3 |
| s3cmd | 8 | 0 | 0 |
| s3select | 0 | 1 | 0 |
| versioning | 0 | 1 | 0 |

## Fixed In This Run

- `mc` `test_presigned_post_policy_error` now passes. Object-path `multipart/form-data` POSTs are routed to the object verb path and return `405 MethodNotAllowed`.
- MinIO JS non-versioned force-delete-prefix now passes. MinIO JS sends `DELETE /bucket/<prefix>` with `x-minio-force-delete: true`; `rusts3` now treats that as a prefix delete extension instead of deleting the literal key.

## Remaining Compatibility Gaps

Expected or explicitly unimplemented feature areas:

- Object versioning: the dedicated `versioning` suite failed with `501 NotImplemented`; MinIO JS versioning tests also failed because uploaded objects returned `versionId: null`.
- S3 Select: `aws-sdk-go`, `minio-js`, and `s3select` failed on `SelectObjectContent` with `501 NotImplemented`; `minio-py` marked it `NA`.
- Bucket lifecycle: AWS CLI `get-bucket-lifecycle-configuration` failed with `501 NotImplemented`; Java lifecycle reads were `NA`.
- Bucket policy: PHP `putBucketPolicy` failed; MinIO Go `SetBucketPolicy` failed with `SignatureDoesNotMatch`; MinIO JS `setBucketPolicy` returned a response parsed as `S3Error: 200`; Java marked `getBucketPolicy` as `NA`.
- Bucket replication: `mc test_bucket_replication` failed while configuring a remote target; Java replication set/get/delete tests were `NA`.
- Object lock, legal hold, and retention-related APIs: Java failed parsing object lock and legal-hold XML responses.
- Bucket/object tagging: Java `getObjectTags` failed XML parsing and `deleteObjectTags` hit `NoSuchKey`; Java bucket tag reads were `NA`; JS tag cleanup had `NoSuchBucket` cascade failures.
- Bucket encryption: Java `getBucketEncryption` failed XML parsing, and delete encryption cascaded into `NoSuchBucket`.
- Bucket notifications and listen APIs: Java listen failed with `SignatureDoesNotMatch`; JS invalid-event listen test got XML where it expected the MinIO notification error text; Java notification set/get/delete were `NA`.
- MinIO admin APIs: Java admin tests for add/list/delete users and canned policies all hit `/minio/admin/v3/...` paths and returned `NoSuchBucket`.
- Snowball object upload/extraction: Java and Python snowball upload tests failed because only part of the expected extracted object set was visible afterward.

## Passing Coverage

Core behavior passed broadly across the suites:

- bucket create/list/head/delete
- object put/head/get/delete
- range and partial reads
- basic list objects and list objects v2
- multipart upload, list uploads/list parts, abort, and completion paths
- copy and compose object flows
- presigned GET/PUT and browser POST flows
- MinIO JS non-versioned force-delete-prefix
- health and metrics endpoints
- `s3cmd`, `aws-sdk-ruby`, `aws-sdk-java`, `aws-sdk-java-v2`, and healthcheck suites had no failures in this run.
