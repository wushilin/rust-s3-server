# Rewrite Plan

Rewrite the current simple Rocket-based S3 test server into an async Axum service with a safer filesystem layout, per-bucket SQLite indexes, and S3-compatible object, multipart, download, range download, and listing behavior.

## Goals

- Keep the server simple and local-development friendly.
- Avoid huge flat directories when many objects share one prefix or folder.
- Avoid object-key path traversal and filesystem compatibility issues.
- Preserve deterministic object placement without hash collisions.
- Make prefix listing efficient through SQLite.
- Avoid unnecessary IO when completing multipart uploads.
- Keep object metadata authoritative enough to rebuild SQLite if the index is missing.
- Start with no authentication.
- Accept and ignore AWS auth-related headers such as `Authorization`, `x-amz-content-sha256`, `x-amz-date`, security-token headers, checksum headers, and other unrecognized `x-amz-*` headers.

## Non-Goals

- No distributed storage.
- No erasure coding.
- No S3 auth/signature validation initially.
- No object versioning initially.
- No lifecycle, replication, tagging, ACL, or policy support initially.
- No small-object inline optimization initially. It can be added later.

## Technology

- Runtime: `tokio`
- HTTP framework: `axum`
- SQLite: `sqlx` with the SQLite feature
- Serialization: `serde`, `serde_json`
- XML responses: initially formatted strings or a small XML helper
- Checksums: MD5 for S3 ETag compatibility
- Streaming: `tokio::fs::File`, `tokio_util::io::ReaderStream`, custom multipart/range readers as needed
- Request decoding must support plain request bodies and AWS streaming payload framing when `Content-Encoding: aws-chunked` or `x-amz-decoded-content-length` is present.

## Filesystem Layout

Use a root data directory:

```text
data/
  buckets/
    <bucket_name>/
      index.sqlite
      bucket.json
      objects/
        3f/
          a9/
            01/
              7c/
                <physical_id>/
                  part.1
                  part.2
                  meta.json
      staging/
        put/
          <epoch_ms>_<random>/
            part.1
            put.json
        multipart/
          <epoch_ms>_<random>/
            part.1
            part.1.meta.json
            part.2
            part.2.meta.json
            upload.json
```

Notes:

- Each bucket has its own directory and its own SQLite database.
- Bucket names are used directly as bucket directory names after bucket-name validation.
- `objects/` stores finalized object data.
- `staging/` stores incomplete uploads.
- `staging/put/` is for single PUT writes.
- `staging/multipart/` is for multipart uploads.
- Every staging directory name is `<epoch_ms>_<random>`.
- The `<random>` suffix must contain at least 16 hex characters from cryptographically secure random bytes.
- Staging ids and multipart upload ids must match `^[0-9]+_[0-9a-f]{16,}$` before they are used as filesystem path components.
- A simple PUT object stores bytes as `part.1`.
- A completed multipart object stores bytes as `part.N` files.
- `meta.json` decides which data files are live for the object.
- Do not split the whole physical id into two-character directories. Use bounded hash fanout only.

## Object Key Encoding

Use deterministic, reversible, filesystem-safe encoding rather than hashing.

Recommended encoding:

```text
physical_id = lowercase base32 without padding over UTF-8 bytes of object_key
```

Reasons:

- Reversible, so there are no hash collisions.
- Deterministic.
- Safe on common filesystems.
- Avoids `/`, `\`, `:`, null bytes, spaces, shell-sensitive characters, and Unicode normalization issues.
- Safer than base64url on case-insensitive filesystems.
- More compact than hex.

Example:

```text
object_key:  abc/def/af/好.txt
physical_id: <base32-lower-no-padding>
```

Physical identity and fanout are separate:

- `physical_id` is the reversible base32-encoded object key.
- `fanout_id` is a SHA-1 hex digest used only to distribute directories.
- A `fanout_id` collision is harmless. It only puts more object directories under the same fanout directory; the final `<physical_id>/` directory remains the collision-free object identity.

Fanout input:

```text
fanout_id = sha1(bucket + "\0" + object_key).hex()
```

Fanout path:

```text
objects/<fanout[0..2]>/<fanout[2..4]>/<fanout[4..6]>/<fanout[6..8]>/<physical_id>/
```

The final object directory is always exactly `<physical_id>/` under the bounded fanout path.

Four two-character SHA-1 fanout levels provide `256^4` possible leaf fanout directories. This avoids common-prefix congestion and sequential-key congestion, including keys where only the last few characters differ.

Path component length caveat:

- Most filesystems cap one path component at roughly 255 bytes.
- S3 object keys can be up to 1024 bytes.
- Base32 expands data by roughly 1.6x, so a long key cannot always fit as one `<physical_id>` path component.
- Initial implementation should support object keys whose encoded `physical_id` fits the filesystem path-component limit.
- If the encoded `physical_id` exceeds the configured component limit, reject the request with an S3-compatible invalid-key or not-implemented error instead of attempting filesystem IO.
- Before claiming full 1024-byte S3 key support, add a long-key physical layout that chunks the reversible `physical_id` into bounded path components without using a hash as identity.

## Metadata

Each finalized object has exactly one `meta.json` file.

Metadata is rewritten only when the object itself is rewritten. Metadata must not be mutated independently for the initial implementation.

Suggested schema:

```json
{
  "format_version": 1,
  "bucket": "my-bucket",
  "object_key": "path/file.bin",
  "physical_id": "base32...",
  "storage": "single",
  "size": 12345,
  "etag": "md5hex",
  "last_modified_ms": 1718791234567,
  "content_type": "application/octet-stream",
  "parts": [
    {
      "number": 1,
      "file": "part.1",
      "size": 12345,
      "etag": "md5hex"
    }
  ]
}
```

For multipart:

```json
{
  "format_version": 1,
  "bucket": "my-bucket",
  "object_key": "big/file.bin",
  "physical_id": "base32...",
  "storage": "multipart",
  "size": 15728640,
  "etag": "multipart-etag",
  "last_modified_ms": 1718791234567,
  "content_type": "application/octet-stream",
  "parts": [
    {
      "number": 1,
      "file": "part.1",
      "size": 5242880,
      "etag": "part-md5"
    },
    {
      "number": 2,
      "file": "part.2",
      "size": 5242880,
      "etag": "part-md5"
    }
  ]
}
```

The metadata must contain enough information to rebuild SQLite:

- bucket
- object_key
- physical_id
- storage mode
- size
- etag
- last modified time
- part list for multipart objects

ETag handling:

- Store ETags internally without surrounding quotes.
- Return ETags in HTTP headers with double quotes, for example `ETag: "md5hex"`.
- Return ETags in S3 XML response bodies with double quotes.
- Accept ETags from multipart completion XML with or without quotes, but compare using the unquoted internal form.

Timestamp rendering:

- Store `last_modified_ms` and bucket `created_at_ms` as milliseconds since epoch.
- Render HTTP `Last-Modified` headers using RFC 7231 HTTP-date format, for example `Mon, 01 Jan 2024 00:00:00 GMT`.
- Render XML `LastModified` and `CreationDate` values using ISO 8601 UTC with milliseconds, for example `2024-01-01T00:00:00.000Z`.

## SQLite

Use one SQLite database per bucket:

```text
data/buckets/<bucket>/index.sqlite
```

Initial schema:

```sql
CREATE TABLE IF NOT EXISTS objects (
  object_key TEXT PRIMARY KEY,
  physical_id TEXT NOT NULL UNIQUE,
  size INTEGER NOT NULL,
  etag TEXT NOT NULL,
  last_modified_ms INTEGER NOT NULL
);
```

SQLite stores listing-critical fields so `ListObjectsV2` can return `Key`, `Size`, `ETag`, and `LastModified` without reading one `meta.json` per object. Full object layout and part details still live in `meta.json`.

Rationale for per-bucket SQLite:

- S3 object keys are bucket-local.
- Bucket delete/rebuild is simpler.
- Indexes remain smaller.
- Prefix scans do not contend across unrelated buckets.
- A missing/corrupt index can be rebuilt per bucket.

## Prefix Listing

Use SQLite for all prefix and delimiter listing.

Prefix listing must not rely on filesystem layout. Many workloads only PUT and GET and never list; the physical layout is optimized for direct object lookup and directory distribution, while SQLite is the listing index.

For prefix `p`, use an indexed range query:

```sql
SELECT object_key, physical_id, size, etag, last_modified_ms
FROM objects
WHERE object_key >= ? AND object_key < ?
ORDER BY object_key
LIMIT ?;
```

The upper bound is the smallest string greater than all strings with prefix `p`. Implement a helper for prefix-end calculation. If that helper cannot produce an upper bound, fall back to `object_key >= ?` plus application-side prefix filtering.

Delimiter listing (`delimiter=/`) should convert matching keys into either:

- `Contents` entries for direct objects
- `CommonPrefixes` entries for grouped prefixes

Delimiter algorithm:

- For each SQLite key after `prefix`, remove the prefix and search for the first delimiter.
- If no delimiter remains, emit a `Contents` entry.
- If a delimiter remains, emit one `CommonPrefixes` entry for `prefix + segment + delimiter`.
- Deduplicate `CommonPrefixes` application-side.
- `CommonPrefixes` count against `max-keys` together with `Contents`.
- Pagination must not duplicate a `CommonPrefixes` entry when a page boundary falls inside a prefix group. The continuation token should include enough state to resume after the last scanned key, not only the last emitted key.

Pagination:

- Support `max-keys`, `continuation-token`, and `start-after` for ListObjectsV2.
- Encode continuation tokens as URL-safe base64 JSON.
- Token payload should include at least:

```json
{
  "v": 1,
  "last_key": "previous/object/key"
}
```

- Decode the token and continue with `object_key > last_key`.
- Query `max_keys + 1` rows to decide whether another page exists.
- Return at most `max_keys` visible results.
- Return `NextContinuationToken` when another page exists.
- For ListObjectsV1, support `marker`.
- For ListObjectsV1, include `NextMarker` only when `delimiter` is set. Without a delimiter, clients use the last returned `Key` as the next marker.

ListObjectsV2 response shape:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix></Prefix>
  <KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <ContinuationToken>incoming-token</ContinuationToken>
  <NextContinuationToken>next-token</NextContinuationToken>
  <StartAfter>start-after-key</StartAfter>
  <Contents>
    <Key>object.txt</Key>
    <LastModified>2024-01-01T00:00:00.000Z</LastModified>
    <ETag>"md5hex"</ETag>
    <Size>12345</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <CommonPrefixes>
    <Prefix>folder/</Prefix>
  </CommonPrefixes>
</ListBucketResult>
```

ListObjectsV1 uses the same root element and object entry shape. It uses `Marker` instead of `ContinuationToken`, and `NextMarker` is included only when `delimiter` is set.

Only include `ContinuationToken`, `NextContinuationToken`, and `StartAfter` when applicable. `NextContinuationToken` is required when `IsTruncated` is `true`.

## List Buckets

`GET /` returns S3-compatible bucket listing XML:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>local</ID>
    <DisplayName>local</DisplayName>
  </Owner>
  <Buckets>
    <Bucket>
      <Name>my-bucket</Name>
      <CreationDate>2024-01-01T00:00:00.000Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>
```

Bucket creation time:

- Store bucket metadata in `data/buckets/<bucket>/bucket.json`.
- `bucket.json` should include `created_at_ms`.
- If `bucket.json` is missing, use the bucket directory creation time if available.
- If neither is available, use a static local-dev fallback timestamp.

## Bucket Name Validation

Bucket names are used directly as directory names, so validation must be strict.

Initial rules:

- 3 to 63 characters.
- Lowercase letters, digits, and hyphens only.
- Must start and end with a lowercase letter or digit.
- Must not look like an IPv4 address.
- Must not be `_staging`, `objects`, `staging`, or any internal reserved name.
- Reject names containing `.`, `/`, `\`, `:`, whitespace, control characters, or Unicode.

## Object Key Validation

Initial rules:

- Reject empty keys.
- Reject keys containing null bytes.
- Reject keys longer than 1024 bytes before encoding.
- Reject keys whose encoded physical id exceeds the configured filesystem component limit.
- Allow Unicode and ordinary punctuation at the S3 key layer; filesystem safety is provided by encoding.

## Single PUT Object Flow

1. Validate bucket name.
2. Validate object key.
3. Compute `physical_id`.
4. Compute final object directory.
5. Write request body to a staging file:

```text
staging/put/<epoch_ms>_<random>/part.1
```

6. Compute size and MD5 while streaming.
7. Write staging metadata.
8. Acquire an object-level async write lock for `(bucket, object_key)`.
9. Create final object directory.
10. Fully validate the staged object before touching the existing object. Validation includes size, ETag, metadata shape, and that the staged `part.1` file exists.
11. If overwriting an existing object, delete object visibility first. Remove old `meta.json`, then remove the SQLite row. Old part files may remain temporarily.
12. Atomically rename staging `part.1` to final `part.1`.
13. Update SQLite in a transaction:

```sql
INSERT INTO objects(object_key, physical_id, size, etag, last_modified_ms)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(object_key) DO UPDATE SET
  physical_id = excluded.physical_id,
  size = excluded.size,
  etag = excluded.etag,
  last_modified_ms = excluded.last_modified_ms;
```

14. Write final `meta.json` to a temp file and rename into place. This is the final visibility marker.
15. Release the object-level write lock.
16. Leave any leftover files in the object directory for background cleanup.

`put.json` schema:

```json
{
  "bucket": "my-bucket",
  "object_key": "path/file.bin",
  "physical_id": "base32...",
  "write_id": "1718791234567_0123456789abcdef",
  "initiated_at_ms": 1718791234567,
  "size": 12345,
  "etag": "md5hex",
  "content_type": "application/octet-stream"
}
```

Content type:

- Store `Content-Type` from the request if present.
- Default to `application/octet-stream` if absent.

Request body decoding:

- If the request uses AWS streaming payload framing (`Content-Encoding: aws-chunked` or `x-amz-decoded-content-length`), decode the framed stream and store only the decoded object bytes.
- AWS chunked framing is not normal HTTP chunked transfer encoding. Each payload chunk is:

```text
<hex-size>;chunk-signature=<signature>\r\n
<chunk-data>\r\n
```

- A zero-size chunk terminates the stream.
- After the zero-size chunk, read and discard trailing headers until the blank line.
- Common trailers include checksum headers such as `x-amz-checksum-crc32`.
- Consuming trailers is required for HTTP/1.1 keep-alive correctness.
- Because auth is not implemented, ignore `chunk-signature` values but still parse and remove the framing.
- If AWS streaming payload decoding is not implemented yet, Java SDK tests must explicitly disable request checksum/chunked streaming. Server-side decoding is preferred.

Content-MD5:

- Accept `Content-MD5` on PUT Object and Upload Part.
- Do not validate it in the initial local-dev implementation.
- The server computes and stores its own MD5 ETag from decoded object bytes.

Because `physical_id` is derived from object key, overwriting the same key usually reuses the same physical directory. The commit path should still be written carefully so interrupted writes do not leave a half-valid object.

Write visibility model:

- Replacement writes are pre-validated in staging before touching the old object.
- Commits for the same `(bucket, object_key)` are serialized by an object-level async write lock.
- Replacement commits use delete-visible-first-then-add-back semantics: remove old `meta.json` and index visibility before publishing new `meta.json`.
- Physical data is written first.
- SQLite is updated before final metadata is published.
- `meta.json` is always written last.
- An object is considered readable only when `meta.json` exists and all referenced physical files exist.
- If SQLite contains an object but `meta.json` is missing, the entry is an orphan index entry.
- If `meta.json` exists but SQLite is missing the key, the object is valid and the sweeper should repair SQLite.
- For stronger atomic overwrites later, use generation directories under `<physical_id>/` and make `meta.json` point to the active generation.

Object-level write lock implementation:

- Store locks in an in-memory map keyed by `(bucket, object_key)`.
- A practical implementation is `DashMap<(String, String), Arc<Mutex<()>>>` or equivalent.
- Writes to different object keys must not block each other.
- Remove lock entries after release when no task is waiting, or use a small reference-counted cleanup policy.
- Locks do not survive server restart. This is acceptable because crash recovery relies on `meta.json`, SQLite, staging, and the sweeper.

Overwrite crash limitation:

- The initial delete-visible-first overwrite model has a data-loss window.
- If the process crashes after old `meta.json` and SQLite visibility are removed but before the new `meta.json` is published, the object is invisible.
- Old physical part files may still exist, but they are no longer authoritative and may be removed by the sweeper.
- Generation directories are the future fix if overwrite atomicity becomes a requirement.

## Multipart Upload Flow

### Initiate

Create:

```text
staging/multipart/<epoch_ms>_<random>/upload.json
```

The server-generated S3 `uploadId` should be the staging directory name, so it also uses `<epoch_ms>_<random>`.

`upload.json` should include:

- bucket
- object_key
- upload_id
- initiated_at_ms
- physical_id
- content_type

Store `Content-Type` from the initiate request if present. Default to `application/octet-stream` if absent.

Return S3-compatible XML with `UploadId`.

Initiate response:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>bucket</Bucket>
  <Key>key</Key>
  <UploadId>1718791234567_0123456789abcdef</UploadId>
</InitiateMultipartUploadResult>
```

### Upload Part

For each part:

```text
staging/multipart/<epoch_ms>_<random>/part.N
staging/multipart/<epoch_ms>_<random>/part.N.meta.json
```

Part metadata:

```json
{
  "number": 1,
  "size": 5242880,
  "etag": "md5hex"
}
```

Return the part ETag header.

Upload part request body decoding follows the same rules as single PUT: store decoded object bytes, not AWS chunk framing bytes.

Upload part validation:

- Validate `uploadId` against `^[0-9]+_[0-9a-f]{16,}$` before using it in any path.
- Read `upload.json`.
- Reject with `NoSuchUpload` if the upload id is malformed, missing, or belongs to a different bucket/key.
- Validate `partNumber` is an integer in the range `1..=10000`.
- Reject out-of-range part numbers with `InvalidPart`.

### Complete Multipart

Do not concatenate part files.

1. Parse the complete multipart XML body.
2. Validate all requested parts exist.
3. Validate part ETags if supplied.
4. Validate `uploadId` against `^[0-9]+_[0-9a-f]{16,}$`, read `upload.json`, and reject with `NoSuchUpload` if it is malformed, missing, or belongs to a different bucket/key.
5. Enforce S3 multipart constraints where needed:
   - all parts except last must be at least 5 MiB
   - parts in the completion XML must be in ascending order
   - non-contiguous part numbers are accepted, matching S3 behavior
6. Fully validate the staged multipart object before touching the existing object. Validation includes part order, part sizes, part ETags, final ETag, total size, and staged part file existence.
7. Acquire an object-level async write lock for `(bucket, object_key)`.
8. Re-check that the staging directory still exists while holding the lock.
9. If overwriting an existing object, delete object visibility first. Remove old `meta.json`, then remove the SQLite row. Old part files may remain temporarily.
10. Create final object directory.
11. Rename each staging part into final object directory:

```text
staging/multipart/<epoch_ms>_<random>/part.N -> objects/.../<physical_id>/part.N
```

12. Use the total size computed during validation.
13. Use the S3-style multipart ETag computed during validation:

```text
md5(concat(binary_md5(part1), binary_md5(part2), ...)) + "-" + part_count
```

14. Update SQLite with `object_key`, `physical_id`, total size, multipart ETag, and last modified time.
15. Write final multipart `meta.json`. This is the final visibility marker.
16. Release the object-level write lock.
17. Remove staging upload directory.

Complete response:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://host-from-request/bucket/key</Location>
  <Bucket>bucket</Bucket>
  <Key>key</Key>
  <ETag>"etag-3"</ETag>
</CompleteMultipartUploadResult>
```

This eliminates the expensive compose/copy IO during complete.

Construct `Location` from the incoming request host, for example `http://<Host header>/<bucket>/<key>`.

### Abort Multipart

For `DELETE /<bucket>/<key>?uploadId=...`:

1. Validate bucket and object key.
2. Validate `uploadId` against `^[0-9]+_[0-9a-f]{16,}$` before using it in any path.
3. Resolve `uploadId` to `staging/multipart/<uploadId>/`.
4. Read `upload.json` if the staging directory exists.
5. Reject with `NoSuchUpload` if the upload id is malformed, missing, or belongs to a different bucket/key.
6. Acquire the object-level async write lock for the `bucket/object_key` recorded in `upload.json`.
7. Re-check the staging directory still exists while holding the lock.
8. Remove the entire staging directory.
9. Return `204 No Content`.

If the staging directory is missing, return `404 NoSuchUpload`, including after the upload was already completed or aborted.

## GET Object Flow

Single GET does not require SQLite in the request path.

1. Validate bucket and object key.
2. Compute `physical_id`.
3. Open final object directory.
4. Read `meta.json`.
5. If `storage == "single"`, stream `part.1`.
6. If `storage == "multipart"`, stream part files in the order of the `parts` array in `meta.json`, not by directory listing or lexicographic filename sort.
7. Validate that every physical file referenced by `meta.json` exists before streaming.
8. Do not synchronously query SQLite during GET.
9. If `meta.json` is missing or referenced physical files are missing, treat the object as not found and leave cleanup to orphan sweeping.
10. Missing SQLite rows are repaired by the background sweeper, not by the GET path.
11. Return S3 headers:
   - `ETag`
   - `Last-Modified`
   - `Content-Length`
   - `Content-Type`
   - `Accept-Ranges: bytes`

## Range GET

Support:

```text
Range: bytes=start-end
Range: bytes=start-
Range: bytes=-suffix_length
```

Malformed Range headers are treated as absent: return `200 OK` with the full object body.

For multi-range requests such as `Range: bytes=0-99,200-299`, match S3 local compatibility behavior by ignoring the Range header and returning `200 OK` with the full object body.

For single-file objects, seek directly in `part.1`.

For multipart objects:

1. Use part sizes from `meta.json`.
2. Map the requested byte range to part number, part offset, and length.
3. Stream only the relevant slices across one or more part files.
4. Return `206 Partial Content`.
5. Include:

```text
Content-Range: bytes start-end/total_size
Content-Length: selected_length
```

Syntactically valid but unsatisfiable ranges return `416 Range Not Satisfiable` and include:

```text
Content-Range: bytes */total_size
```

## HEAD Object Flow

HEAD mirrors GET metadata validation but returns headers only.

1. Validate bucket and object key.
2. Compute `physical_id`.
3. Read `meta.json`.
4. Validate that every physical file referenced by `meta.json` exists.
5. If `meta.json` is missing or any referenced physical file is absent, return `404 NoSuchKey`.
6. Return the same headers GET would return:
   - `ETag`
   - `Last-Modified`
   - `Content-Length`
   - `Content-Type`
   - `Accept-Ranges: bytes`
7. Return no response body.

## Bucket Location Flow

`GET /<bucket>?location` should return a minimal S3-compatible XML response:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">ap-southeast-1</LocationConstraint>
```

The initial implementation can return the configured server region for every bucket.

## Delete Object

1. Compute `physical_id` from bucket and key.
2. Remove `meta.json` first. This makes the object immediately invisible to GET/HEAD.
3. Remove the SQLite row in a transaction.
4. Leave physical part files and the object directory to the orphan sweeper on its next pass. Do not spawn per-delete cleanup tasks.
5. Return success even if the object did not exist, matching S3 delete behavior.

Delete must not scan the filesystem.

## Delete Bucket

Initial behavior:

- Only allow delete if bucket is empty.
- Use SQLite to check emptiness.
- In-progress staging uploads block bucket deletion.
- If `staging/put/` or `staging/multipart/` contains active, non-expired staging directories, return `409 Conflict` / `BucketNotEmpty`.
- Remove bucket directory after deleting `index.sqlite`, `objects`, and `staging`.

## Rebuild SQLite

If `index.sqlite` is missing, corrupt, or explicitly rebuilt:

1. Walk `objects/`.
2. Find `meta.json` files.
3. Parse metadata.
4. Validate:
   - metadata bucket matches bucket directory
   - object key re-encodes to physical id
   - `meta.json.physical_id` matches the leaf object directory name
   - referenced data files exist
5. Recreate `index.sqlite`.
6. Insert `(object_key, physical_id, size, etag, last_modified_ms)` for each valid object.

Do not trust path names alone. Use `meta.json` as the source for rebuild.

## Orphan Sweeping

An orphan is any state that is not currently readable according to the visibility model.

Types:

- SQLite row exists, but `meta.json` is missing.
- SQLite row exists, but `meta.json` references missing physical files.
- Physical object directory exists, but `meta.json` is missing and there is no active writer.
- Staging upload directory is expired or has no valid active upload.

Valid but under-indexed state:

- `meta.json` exists and all referenced physical files exist, but SQLite has no row. This is not an orphan. The background sweeper or a rebuild should restore the row.

Sweeper behavior:

1. Run in the background after startup and periodically.
2. Process at most `orphan_sweep_batch_size` objects per tick.
3. Default `orphan_sweep_batch_size` can be small, for example `100`.
4. After each object deletion or repair action, yield back to the async runtime.
5. Continue from the last scan position on the next tick.
6. A pass is complete only after the sweeper has scanned all buckets and object/index candidates once.
7. After a full pass completes, sleep until the next configured sweep interval.
8. Log every deletion or repair with bucket, object key when known, physical id, reason, and path.
9. Sweeping is best-effort and must yield to normal read/write operations; leftover files do not affect correctness because `meta.json` defines the live files.

For SQLite orphans:

1. Read a bounded page of SQLite rows.
2. For each row, compute the physical object directory.
3. If `meta.json` is missing and the object directory/index row is older than `orphan_grace_period`, delete the SQLite row.
4. If `meta.json` exists but physical files are missing, delete the physical directory if present, then delete the SQLite row.
5. Yield after each deletion.

SQLite orphan age source:

- Use `last_modified_ms` as the effective row age for the SQLite orphan grace-period check.
- This protects rows inserted by an active writer because their `last_modified_ms` is recent.
- Trade-off: if an old object's `meta.json` is accidentally deleted, its old `last_modified_ms` may make the SQLite row eligible for cleanup on the next sweep without waiting a fresh grace period.

For physical metadata candidates:

1. Walk `objects/` incrementally.
2. If an object directory has no `meta.json` and is older than `orphan_grace_period`, remove the physical directory.
3. If `meta.json` is valid and physical files exist but SQLite is missing the row, insert the SQLite row.
4. If an object directory contains files not referenced by the current `meta.json`, remove those unreferenced files in the background and log the cleanup.
5. Yield after each repair or deletion.

For staging directories:

1. Walk `staging/put/` and `staging/multipart/` incrementally.
2. Parse each staging directory name as `<epoch_ms>_<random>`.
3. If the name cannot be parsed, use filesystem modification time as the fallback age source.
4. If the staging directory age exceeds `staging_expiry_ms`, remove the entire staging directory.
5. `staging_expiry_ms` must be configurable.
6. Use a conservative default, for example 24 hours.
7. Log each staging cleanup with staging kind, upload/write id, age, expiry, and path.
8. Yield after each staging directory deletion.

Sweeper timing configuration:

- `orphan_grace_period` protects in-progress object commits and should be short, for example 5 minutes.
- `staging_expiry_ms` protects in-progress uploads and should be much longer, for example 24 hours.
- Expected relationship: `orphan_grace_period` must be much less than `staging_expiry_ms`.

## Crash Consistency

Use temp files and atomic rename where possible:

```text
meta.json.tmp.<random> -> meta.json
index.sqlite transaction commit
```

Commit order for finalized object:

1. Data files are fully present in final directory.
2. SQLite is updated.
3. `meta.json` is written and renamed into place as the final visibility marker.

If SQLite update fails before metadata is committed, the object is not visible and the sweeper can clean or repair the leftover physical files. If metadata is committed but SQLite is missing, reads and rebuilds can repair the SQLite row from `meta.json`.

Delete order:

1. Remove `meta.json` first.
2. Remove SQLite row.
3. Remove physical part files and directory in background cleanup.

Startup and periodic background sweeping must clean stale staging directories older than `staging_expiry_ms`.

## S3 API Surface For Initial Rewrite

Implement:

- `PUT /<bucket>` create bucket
- `GET /` list buckets
- `DELETE /<bucket>` delete bucket if empty
- `PUT /<bucket>/<key>` put object
- `GET /<bucket>/<key>` get object
- `HEAD /<bucket>/<key>` head object
- `DELETE /<bucket>/<key>` delete object
- `GET /<bucket>?list-type=2&prefix=...&delimiter=...` list objects v2
- `GET /<bucket>?prefix=...&delimiter=...` list objects v1 if needed by AWS CLI
- `POST /<bucket>/<key>?uploads` initiate multipart upload
- `PUT /<bucket>/<key>?partNumber=N&uploadId=...` upload part
- `POST /<bucket>/<key>?uploadId=...` complete multipart upload
- `DELETE /<bucket>/<key>?uploadId=...` abort multipart upload
- `GET /<bucket>?location` bucket location

Bucket creation is idempotent for this no-auth local server: creating an existing bucket returns `200 OK`.

Bucket creation writes `bucket.json` with `created_at_ms` if the bucket is newly created.

PUT bucket response:

- Return `200 OK`.
- Response body is empty.

Query routing priority for `GET /<bucket>?...`:

1. If query contains `location`, route to bucket location.
2. Else if query contains `list-type=2`, route to ListObjectsV2.
3. Else if query contains known-but-unimplemented bucket operations such as `uploads`, `versioning`, `cors`, `website`, `lifecycle`, `policy`, `acl`, or `tagging`, return `501 NotImplemented` with S3 error XML.
4. Else route to ListObjectsV1.
5. `GET /<bucket>` with no query parameters is ListObjectsV1 with empty prefix.

PUT object response:

- Return `200 OK`.
- Include quoted `ETag` header.
- Response body is empty.

HTTP status codes:

| Operation | Status |
|-----------|--------|
| PUT bucket | `200 OK` |
| PUT existing bucket | `200 OK` |
| PUT object | `200 OK` |
| GET object | `200 OK` |
| GET object range | `206 Partial Content` |
| GET object multi-range | `200 OK` full body |
| HEAD object | `200 OK` |
| DELETE object | `204 No Content` |
| DELETE bucket | `204 No Content` |
| Initiate multipart | `200 OK` |
| Upload part | `200 OK` |
| Complete multipart | `200 OK` |
| Abort multipart | `204 No Content` |
| Abort missing/completed multipart upload | `404 NoSuchUpload` |
| NoSuchKey / NoSuchBucket | `404 Not Found` |
| BucketNotEmpty | `409 Conflict` |
| Invalid range | `416 Range Not Satisfiable` |
| Invalid request | `400 Bad Request` |
| Known but unimplemented S3 operation | `501 NotImplemented` |

Common response headers:

- Include `x-amz-request-id` on every response.
- A static, sequential, or random request id is acceptable for the local-dev server.
- Include quoted `ETag` wherever S3 expects an ETag header.

S3 error XML:

All S3 errors should return XML in this shape:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <RequestId>1</RequestId>
  <HostId>1</HostId>
</Error>
```

The HTTP status code must match the error code. Java SDK compatibility depends on XML error bodies.

## Tests

Add tests around storage functions first, then HTTP compatibility.

Storage tests:

- object key encoding round trip
- encoded path is filesystem-safe
- fanout path is bounded
- object key validation rejects empty, null-byte, over-1024-byte, and over-component-limit keys
- rebuild rejects metadata whose `physical_id` does not match the leaf object directory
- PUT writes data and metadata
- PUT stores content type from request or defaults to `application/octet-stream`
- PUT decodes AWS chunked request framing instead of storing framed bytes
- AWS chunked decoder consumes trailing headers after the terminal chunk
- `Content-MD5` is accepted and ignored
- overwrite updates metadata
- overwrite crash window is documented and tested as object-not-visible until repair/new write
- concurrent PUTs to the same key are serialized and leave matching SQLite/meta/data
- SQLite maps object key to physical id
- SQLite rebuild from metadata
- sweeper repairs missing SQLite row when `meta.json` and physical files are valid
- SQLite row with missing `meta.json` is treated as orphan
- SQLite row with missing physical file is treated as orphan
- orphan sweeper removes bounded batches and yields after each deletion
- SQLite orphan sweep respects `orphan_grace_period`
- staging directories use `<epoch_ms>_<random>` names
- expired staging directories are removed as whole directories after configurable `staging_expiry_ms`
- malformed multipart `uploadId` cannot escape staging path and returns `NoSuchUpload`
- upload part validates `upload.json` bucket/key against request URL
- complete multipart validates `upload.json` bucket/key against request URL
- upload part rejects part numbers outside `1..=10000`
- multipart `upload.json` preserves content type from initiate request
- abort multipart is serialized with complete via object-level write lock
- abort after complete returns `404 NoSuchUpload`
- multipart complete renames parts without composing
- multipart complete accepts non-contiguous ascending part numbers
- multipart upload part decodes AWS chunked request framing
- multipart ETags are stored unquoted but returned quoted in headers/XML
- range mapping across multipart parts
- long object keys whose encoded physical id exceeds component limit are rejected predictably
- `put.json` schema is written and parsed for cleanup logging

HTTP/S3 tests:

- create bucket
- list buckets
- upload object with AWS CLI or SDK-compatible client
- download object
- HEAD object
- HEAD missing object or missing physical file returns `404 NoSuchKey`
- range download
- unsatisfiable range returns `416` with `Content-Range: bytes */total_size`
- list objects with prefix
- list objects with delimiter
- list objects v2 pagination with `max-keys` and `continuation-token`
- list objects v2 truncated response includes `NextContinuationToken` and echoes applicable request fields
- list objects XML includes `StorageClass`, `KeyCount`, quoted ETag, and ISO 8601 `LastModified`
- bucket location XML
- list buckets XML includes `Owner`, `CreationDate`, and bucket names
- bucket name validation rejects uppercase, dots, slash, IP-style names, and reserved names
- bucket creation is idempotent for existing buckets
- S3 error responses are XML with request IDs
- every response includes `x-amz-request-id`
- ETag headers and XML ETags are quoted
- delete object
- delete bucket fails with `BucketNotEmpty` when active staging uploads exist
- initiate multipart upload
- upload multipart parts
- complete multipart upload
- large file multipart upload
- download completed multipart object
- range download completed multipart object
- multi-range request returns `200 OK` with full object body
- abort multipart upload
- abort multipart rejects upload ids for a different bucket/key
- known but unimplemented bucket query operations return `501 NotImplemented`

AWS CLI compatibility tests:

Use a local endpoint:

```text
http://127.0.0.1:8000
```

Use dummy credentials because auth is not implemented:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=ap-southeast-1
```

Basic bucket/object flow:

```bash
aws --endpoint-url http://127.0.0.1:8000 s3 mb s3://cli-test
aws --endpoint-url http://127.0.0.1:8000 s3 ls
aws --endpoint-url http://127.0.0.1:8000 s3 cp ./fixtures/hello.txt s3://cli-test/hello.txt
aws --endpoint-url http://127.0.0.1:8000 s3 cp s3://cli-test/hello.txt ./tmp/hello.downloaded.txt
diff ./fixtures/hello.txt ./tmp/hello.downloaded.txt
aws --endpoint-url http://127.0.0.1:8000 s3 rm s3://cli-test/hello.txt
```

Prefix, delimiter, and many-object listing:

```bash
aws --endpoint-url http://127.0.0.1:8000 s3 cp ./fixtures/hello.txt s3://cli-test/a/b/c/hello.txt
aws --endpoint-url http://127.0.0.1:8000 s3 cp ./fixtures/hello.txt s3://cli-test/a/b/d/hello.txt
aws --endpoint-url http://127.0.0.1:8000 s3 ls s3://cli-test/a/
aws --endpoint-url http://127.0.0.1:8000 s3 ls s3://cli-test/a/b/ --recursive
```

Unicode and filesystem-unsafe logical keys:

```bash
aws --endpoint-url http://127.0.0.1:8000 s3 cp ./fixtures/hello.txt 's3://cli-test/abc/def/af/好.txt'
aws --endpoint-url http://127.0.0.1:8000 s3 cp ./fixtures/hello.txt 's3://cli-test/spaces and symbols/# file?.txt'
aws --endpoint-url http://127.0.0.1:8000 s3 ls s3://cli-test/abc/def/af/
```

Large object and multipart flow:

```bash
dd if=/dev/urandom of=./tmp/large.bin bs=1M count=20
aws --endpoint-url http://127.0.0.1:8000 s3 cp ./tmp/large.bin s3://cli-test/large.bin
aws --endpoint-url http://127.0.0.1:8000 s3 cp s3://cli-test/large.bin ./tmp/large.downloaded.bin
diff ./tmp/large.bin ./tmp/large.downloaded.bin
```

Explicit low-level multipart upload:

```bash
dd if=/dev/urandom of=./tmp/mp-part-1.bin bs=1M count=6
dd if=/dev/urandom of=./tmp/mp-part-2.bin bs=1M count=6
dd if=/dev/urandom of=./tmp/mp-part-3.bin bs=1M count=1
cat ./tmp/mp-part-1.bin ./tmp/mp-part-2.bin ./tmp/mp-part-3.bin > ./tmp/mp-full.bin

UPLOAD_ID=$(aws --endpoint-url http://127.0.0.1:8000 s3api create-multipart-upload \
  --bucket cli-test \
  --key multipart-large.bin \
  --query UploadId \
  --output text)

ETAG1=$(aws --endpoint-url http://127.0.0.1:8000 s3api upload-part \
  --bucket cli-test \
  --key multipart-large.bin \
  --part-number 1 \
  --upload-id "$UPLOAD_ID" \
  --body ./tmp/mp-part-1.bin \
  --query ETag \
  --output text)

ETAG2=$(aws --endpoint-url http://127.0.0.1:8000 s3api upload-part \
  --bucket cli-test \
  --key multipart-large.bin \
  --part-number 2 \
  --upload-id "$UPLOAD_ID" \
  --body ./tmp/mp-part-2.bin \
  --query ETag \
  --output text)

ETAG3=$(aws --endpoint-url http://127.0.0.1:8000 s3api upload-part \
  --bucket cli-test \
  --key multipart-large.bin \
  --part-number 3 \
  --upload-id "$UPLOAD_ID" \
  --body ./tmp/mp-part-3.bin \
  --query ETag \
  --output text)

aws --endpoint-url http://127.0.0.1:8000 s3api complete-multipart-upload \
  --bucket cli-test \
  --key multipart-large.bin \
  --upload-id "$UPLOAD_ID" \
  --multipart-upload "{\"Parts\":[{\"PartNumber\":1,\"ETag\":$ETAG1},{\"PartNumber\":2,\"ETag\":$ETAG2},{\"PartNumber\":3,\"ETag\":$ETAG3}]}"

aws --endpoint-url http://127.0.0.1:8000 s3 cp s3://cli-test/multipart-large.bin ./tmp/mp-downloaded.bin
diff ./tmp/mp-full.bin ./tmp/mp-downloaded.bin
```

Range download should be tested with low-level `s3api` because `aws s3 cp` does not expose range directly:

```bash
aws --endpoint-url http://127.0.0.1:8000 s3api get-object \
  --bucket cli-test \
  --key large.bin \
  --range bytes=1048576-2097151 \
  ./tmp/range.bin
```

Abort multipart should be tested with `s3api`:

```bash
UPLOAD_ID=$(aws --endpoint-url http://127.0.0.1:8000 s3api create-multipart-upload \
  --bucket cli-test \
  --key aborted.bin \
  --query UploadId \
  --output text)

aws --endpoint-url http://127.0.0.1:8000 s3api upload-part \
  --bucket cli-test \
  --key aborted.bin \
  --part-number 1 \
  --upload-id "$UPLOAD_ID" \
  --body ./tmp/large.bin

aws --endpoint-url http://127.0.0.1:8000 s3api abort-multipart-upload \
  --bucket cli-test \
  --key aborted.bin \
  --upload-id "$UPLOAD_ID"
```

AWS Java SDK compatibility tests:

Use AWS SDK for Java v2. Configure:

- endpoint override: `http://127.0.0.1:8000`
- region: `ap-southeast-1`
- static dummy credentials
- path-style access enabled

Java client setup:

```java
S3Client s3 = S3Client.builder()
    .endpointOverride(URI.create("http://127.0.0.1:8000"))
    .region(Region.AP_SOUTHEAST_1)
    .credentialsProvider(
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create("test", "test")))
    .forcePathStyle(true)
    .build();
```

Java SDK test cases:

- `createBucket`
- `listBuckets`
- `putObject` with small byte array
- `putObject` with file request body
- `headObject`
- `getObject` and byte-for-byte compare
- `getObject` with `Range` header
- `listObjectsV2` with prefix
- `listObjectsV2` with delimiter
- `deleteObject`
- manual multipart:
  - `createMultipartUpload`
  - `uploadPart` for multiple parts
  - `completeMultipartUpload`
  - `getObject` and byte-for-byte compare
  - ranged `getObject` across part boundaries
  - `abortMultipartUpload`

Java SDK multipart completion should verify the returned ETag shape:

```text
<md5-of-part-md5s>-<part-count>
```

Java SDK request-body compatibility:

- Prefer server-side support for AWS chunked payload decoding.
- Include at least one Java SDK `putObject` file-body test that uses the SDK default request behavior.
- If server-side AWS chunked decoding is intentionally deferred, configure the Java test client to disable checksum/chunked request framing and mark default Java SDK upload as unsupported until implemented.

Java SDK large multipart test:

- create a file larger than two S3 minimum part sizes, for example 13 MiB
- upload three parts, where the first two parts are at least 5 MiB
- complete the multipart upload
- download and compare SHA-256 with the source file
- issue a ranged `getObject` that starts in part 1 and ends in part 2
- issue a ranged `getObject` that starts in part 2 and ends in part 3

The Java tests should run in CI as integration tests only when the local server binary is available. They should start the server on an available localhost port, create a temporary data directory, run all tests, then remove the temporary data directory.

Regression tests:

- many flat numeric object keys in one logical prefix
- Unicode object keys
- keys containing spaces and punctuation allowed by S3 but unsafe for direct filesystem names
- bucket with many objects does not create a huge single directory
- interrupted write after SQLite update but before `meta.json` publish is swept as orphan
- interrupted delete after `meta.json` removal but before SQLite row deletion is handled by sweeping the SQLite orphan row

## Implementation Phases

1. Add new Axum server skeleton while keeping old code untouched if possible.
2. Implement bucket directory and per-bucket SQLite management.
3. Implement object key encoding and physical path mapping.
4. Implement metadata structs and read/write helpers.
5. Implement single PUT, GET, HEAD, DELETE.
6. Implement SQLite-backed listing.
7. Implement multipart initiate/upload/complete/abort using part manifests.
8. Implement range reads for single and multipart objects.
9. Add AWS CLI compatibility tests.
10. Add AWS SDK for Java v2 compatibility tests.
11. Remove or retire old Rocket path once replacement passes tests.

## Open Decisions

No open storage-layout decisions remain for the initial rewrite.
