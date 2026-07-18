# MinIO Mint S3 Compatibility Test Report

Date: 2026-06-26

## Target

- Server: `target/release/rusts3`
- Config: `mint-test-config.yaml`
- Endpoint tested from Podman: `host.containers.internal:19015`
- Access key: `minioadmin`
- Secret key: `minioadmin`
- Mint mode: `core`
- Mint logs: `/private/tmp/rusts3-mint-log`

## Commands

```sh
cargo build --release
target/release/rusts3 -c mint-test-config.yaml

podman run --rm \
  -e SERVER_ENDPOINT=host.containers.internal:19015 \
  -e ACCESS_KEY=minioadmin \
  -e SECRET_KEY=minioadmin \
  -e ENABLE_HTTPS=0 \
  -e MINT_MODE=core \
  -e RUN_ON_FAIL=1 \
  -e SERVER_REGION=us-east-1 \
  -v /private/tmp/rusts3-mint-log:/mint/log \
  minio/mint
```

## Result Summary

Parsed from `/private/tmp/rusts3-mint-log/log.json`:

| Status | Count |
| --- | ---: |
| PASS | 75 |
| FAIL | 12 |
| NA | 10 |
| TOTAL | 97 |

Grouped by client/test suite:

| Suite | PASS | FAIL | NA | TOTAL |
| --- | ---: | ---: | ---: | ---: |
| aws-sdk-php | 2 | 1 | 0 | 3 |
| aws-sdk-ruby | 12 | 1 | 0 | 13 |
| awscli | 8 | 1 | 0 | 9 |
| minio-java | 14 | 8 | 10 | 32 |
| minio-js | 39 | 1 | 0 | 40 |

## Failures

1. `aws-sdk-php`
   - Function: `listObjects ( array $params = [] )`
   - Error: expected `listObjects` with invalid arguments to fail with `InvalidArgument`, but it did not.

2. `aws-sdk-ruby`
   - Function: `presignedPost(bucket_name,file_name,expires_in_sec,max_byte_size)`
   - Error: expected object was not created.

3. `awscli`
   - Function: `head-object` after `copy-object`
   - Error: `StorageClass was not applied`.

4. `minio-java`
   - Function: `listBuckets()`
   - Error: dotted bucket name `minio-java-test-25gnlqr.withperiod` was rejected with `InvalidArgument`.

5. `minio-java`
   - Function: `getObjectLockConfiguration()`
   - Error: XML parser expected `ObjectLockEnabled` but response did not include it.

6. `minio-java`
   - Function: `getBucketEncryption()`
   - Error: XML parser mismatch for encryption configuration response.

7. `minio-java`
   - Function: `deleteBucketEncryption()`
   - Error: `NoSuchBucket` during follow-up location lookup.

8. `minio-java`
   - Function: `deleteBucketTags()`
   - Error: `NoSuchBucket`.

9. `minio-java`
   - Function: `deleteBucketPolicy()`
   - Error: `NoSuchBucket`.

10. `minio-java`
    - Function: `deleteBucketLifecycle()`
    - Error: `NoSuchBucket`.

11. `minio-java`
    - Function: `listenBucketNotification()`
    - Error: `SignatureDoesNotMatch`.

12. `minio-js`
    - Function: `statObject(bucketName, objectName, cb)`
    - Error: metadata `"randomstuff"` mismatch for `datafile-65-MB`.

Additional terminal output from `minio-java` reported:

```text
Exception in thread "main" java.lang.AssertionError: object count; expected=6, got=0 expected:<0> but was:<6>
    at FunctionalTest.testListObjects(FunctionalTest.java:1412)
```

## Caveats

The Mint image pulled was `linux/amd64`, while Podman reported the expected platform as `linux/arm64`. The Go-based Mint suites crashed inside the test binaries with SIGSEGV under emulation:

- `aws-sdk-go`
- `minio-go`
- `healthcheck`

Those crashes should be treated as test-runtime failures, not confirmed `rusts3` compatibility failures, unless reproduced on a native `amd64` runner or with a compatible Mint image.

The Mint run was manually interrupted after `minio-js` stopped making progress for several minutes following the metadata mismatch. Both the Mint container and `rusts3` server were stopped cleanly afterward.

## Notes

Core bucket/object operations showed substantial success across Java, Ruby, PHP, AWS CLI, and JS clients, including:

- bucket create/head/list/delete
- object put/head/get/delete
- range GET
- multipart upload, including a 65 MiB JS object
- server-side copy at the basic object-copy level

The highest-value follow-up fixes appear to be:

1. Preserve and return user metadata consistently, including multipart uploads and copied objects.
2. Validate list object parameters such as `max-keys=-1`.
3. Preserve/apply `StorageClass` metadata on copy/head flows.
4. Decide whether presigned POST support is in scope.
5. Return clearer unsupported-feature responses for object lock, encryption, tagging, policy, lifecycle, and notifications.
