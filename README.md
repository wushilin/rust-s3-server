# rusts3 — S3-compatible local dev server

A lightweight S3-compatible object storage server written in Rust.
Built for **local development and CI/CD** — not a production replacement for AWS S3 or MinIO.

---

## Design goals

| Goal | Notes |
|------|-------|
| S3 wire-compatible | Works with the AWS CLI, AWS SDKs, and any library that targets the S3 API |
| Low overhead | Streaming reads/writes — objects are never fully buffered in memory |
| Durable | Complete object directories are published with atomic renames; each bucket has a derived SQLite listing index |
| Self-healing | GET/DELETE and targeted visibility-repair rows reconcile stale SQLite entries; the index can still be rebuilt explicitly via HTTP |
| Observable | Structured per-request logging (method, URI, status, size, latency) and periodic bandwidth reports; rolling log file with configurable rotation and optional gzip compression |
| Configurable | Single `config.yaml` controls network, storage, logging, auth, and background maintenance; `rusts3 init` generates a fully-documented template |
| No hot-spot directories | Objects are spread across a 4-level SHA-1 fanout tree — no single directory ever accumulates more than a handful of entries, regardless of how many objects the bucket contains |

## Explicit non-goals

The following S3 features are **intentionally not implemented**.
If you need them, use MinIO or a real S3-compatible service.

- **Versioning** — only the latest version of each object exists
- **ACLs / IAM** — auth is all-or-nothing per credential pair
- **Lifecycle policies** — no automatic object expiry or tiering
- **Server-side encryption (SSE)** — objects are stored in plaintext
- **TLS** — run behind [Caddy](https://caddyserver.com/) or [Ferron](https://ferron.rs/) for HTTPS

---

## Building

```bash
cargo build --release
# binary: target/release/rusts3
```

---

## Running

```text
rusts3 run [-c FILE]          Start the server (default: ./config.yaml)
rusts3 validate [-c FILE]     Validate configuration without starting
rusts3 genpassword            Generate a bcrypt console password
rusts3 verifypassword [HASH]  Verify a bcrypt console password
rusts3 init                   Write a documented config.yaml
```

### Quickstart

```bash
# Generate a fully-documented config.yaml in the current directory:
target/release/rusts3 init

# Edit config.yaml to taste, then start:
target/release/rusts3 run
```

---

## Configuration reference (`config.yaml`)

Run `rusts3 init` to generate a fully-documented `config.yaml` in the current directory.
All fields are optional — built-in defaults are used for anything omitted.

### `server` — network and storage root

| Field | Default | Description |
|-------|---------|-------------|
| `bind_address` | `"0.0.0.0"` | IP address to listen on. `"0.0.0.0"` binds all interfaces; use `"127.0.0.1"` to restrict to localhost. |
| `bind_port` | `8002` | TCP port the HTTP server listens on. |
| `base_dir` | `"./rusts3-data"` | Root directory where all bucket data is stored on disk. Created automatically if it does not exist. |

### `storage` — storage engine tuning

| Field | Default | Description |
|-------|---------|-------------|
| `sqlite_max_connections` | `50` | Maximum SQLite connections kept open per bucket index pool. Increase if you see pool-exhaustion errors under high concurrency. |
| `meta_cache_capacity` | `200000` | Maximum object metadata entries in the in-process LRU cache. Each entry is roughly 400–700 bytes; 200 000 entries ≈ 80–140 MB. `HEAD` and `GET` requests are served from cache on warm hits without touching SQLite. |
| `sqlite_repair_cache_capacity` | `200000` | Maximum entries in the SQLite-repair suppression cache. Prevents the server from repeatedly attempting to repair the same broken index row within a short window. Each entry is roughly 100 bytes; 200 000 ≈ 20 MB. |

### `logging` — log output and rotation

| Field | Default | Description |
|-------|---------|-------------|
| `level` | `"info"` | Minimum log level. Accepted values: `trace`, `debug`, `info`, `warn`, `error`. |
| `enable_bandwidth_report` | `true` | When `true`, logs aggregate read/write bandwidth totals and rates every 10 seconds. Useful for monitoring throughput without a metrics backend. |
| `file` | *(absent)* | Path to a rolling log file (e.g. `"/var/log/rusts3/rusts3.log"`). When omitted, all output goes to stdout only. When set, logs go to **both** stdout and the file. |
| `rotation_size_mb` | `100` | Rotate the log file when it reaches this size in MiB. Only applies when `file` is set. |
| `keep_files` | `5` | Number of archived (rotated) log files to keep alongside the active file. Older archives are deleted automatically. |
| `compress` | `false` | When `true`, archived log files are compressed with gzip (`.gz` extension appended). |

### `sweeper` — background maintenance

The sweeper performs bounded maintenance only. It cleans old staging/trash directories and processes targeted visibility-repair rows; it does not automatically crawl all live objects or all SQLite index rows.

| Field | Default | Description |
|-------|---------|-------------|
| `interval_secs` | `300` | How often (in seconds) background maintenance wakes up for each bucket. |
| `visibility_repair_batch_size` | `100` | Targeted visibility repair batch size. One maintenance pass drains all eligible rows in batches of this size. |
| `visibility_repair_grace_period_secs` | `86400` | Minimum age (seconds) of a targeted visibility-repair row during normal runtime. Startup repair bypasses this delay because pre-restart requests are no longer running. |
| `staging_expiry_secs` | `86400` | Minimum idle age (seconds) an abandoned staging directory must have before the sweeper deletes it. Covers interrupted single-PUT and multipart uploads. Default is 24 hours. |
| `trash_expiry_secs` | `600` | Minimum idle age (seconds) a trash directory must have before recursive deletion. Default is 10 minutes. |

Older configs using `visibility_repair_max_per_pass`, `max_objects_per_pass`,
and `orphan_grace_period_secs` are still accepted as aliases, but new configs
should use the visibility-repair names above.

### `auth` — SigV4 authentication

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | When `false`, the server accepts all requests without credentials (open access). Set to `true` to require AWS SigV4 signatures on every request. |
| `credentials` | `[]` | List of `{access_key, secret_key}` pairs. Only used when `enabled: true`. Clients must present one of these pairs to authenticate. |
| `public_hostname` | *(absent)* | The hostname (and optional port) that S3 clients are configured to use, e.g. `"mys3.company.com"` or `"localhost:8002"`. When set, presigned URL verification substitutes this value for the `host` signed header instead of reading it from the incoming HTTP request. This makes signature verification proxy-safe — a reverse proxy may rewrite the `Host` header, but the client and server still agree on the configured public hostname. When omitted, the incoming `Host` header is used directly. |
| `public_scheme` | `"http"` | Scheme used when the console generates presigned share links. Accepted values are `http` and `https`. Signature verification is scheme-independent. |

Built-in console passwords may be bcrypt hashes (recommended) or cleartext
for backward compatibility. Generate and verify hashes with
`rusts3 genpassword` and `rusts3 verifypassword`. Built-in `api_keys` are
optional and remain cleartext because S3 SigV4/V2 verification requires the
original HMAC secret; a built-in user without API keys is a valid console-only
administrator.

**Example with auth enabled:**

```yaml
auth:
  enabled: true
  public_hostname: "mys3.company.com"
  public_scheme: https
  credentials:
    - access_key: "minioadmin"
      secret_key: "minioadmin"
    - access_key: "AKIAIOSFODNN7EXAMPLE"
      secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

---

## Using with the AWS CLI

Configure a profile that points at the local server:

```bash
aws configure --profile local
# AWS Access Key ID:     minioadmin   (must match config.yaml when auth.enabled = true)
# AWS Secret Access Key: minioadmin
# Default region:        us-east-1
# Default output format: json
```

Add `--profile local --endpoint-url http://127.0.0.1:8002` to every command, or set:

```bash
export AWS_PROFILE=local
export AWS_ENDPOINT_URL=http://127.0.0.1:8002
```

### Common operations

```bash
# Create bucket
aws s3 mb s3://my-bucket

# Upload
aws s3 cp ~/file.bin s3://my-bucket/path/file.bin

# Download
aws s3 cp s3://my-bucket/path/file.bin ./file.bin

# List
aws s3 ls s3://my-bucket/

# Delete single object
aws s3 rm s3://my-bucket/path/file.bin

# Delete multiple objects (bulk delete)
aws s3 rm s3://my-bucket/path/ --recursive

# Copy object (server-side)
aws s3 cp s3://my-bucket/src.bin s3://my-bucket/dst.bin

# Multipart upload (automatic for files > 8 MB by default)
aws s3 cp ~/large.iso s3://my-bucket/large.iso

# List in-progress multipart uploads
aws s3api list-multipart-uploads --bucket my-bucket

# List parts of an in-progress upload
aws s3api list-parts --bucket my-bucket --key large.iso --upload-id <id>
```

### Pre-signed URLs

```bash
# Generate a pre-signed download URL valid for 1 hour:
aws s3 presign s3://my-bucket/file.bin --expires-in 3600
```

The full SigV4 signature is verified on use. The URL works with any HTTP client —
no AWS credentials needed at download time.

#### Presigned URLs behind a reverse proxy

When a reverse proxy (nginx, Caddy, etc.) sits in front of rusts3 and rewrites
the `Host` header, presigned URL signatures will fail because the client signed
against the public hostname but the server sees the internal one.

Set `auth.public_hostname` to the hostname (and port, if non-standard) that
clients use:

```yaml
auth:
  enabled: true
  public_hostname: "mys3.company.com"
  public_scheme: https
  credentials:
    - access_key: "minioadmin"
      secret_key: "minioadmin"
```

With this set, presigned URL verification always substitutes the configured
value for the `host` signed header, regardless of what the proxy forwards.

---

## S3 API reference

### Bucket operations

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | List all buckets |
| `PUT` | `/{bucket}` | Create bucket |
| `HEAD` | `/{bucket}` | Check bucket exists (200 / 404) |
| `DELETE` | `/{bucket}` | Delete bucket (must be empty) |
| `GET` | `/{bucket}` | List objects (v2 `continuation-token` / v1 `marker`; `prefix`, `delimiter`) |
| `GET` | `/{bucket}?location` | Get bucket region |
| `GET` | `/{bucket}?uploads` | List in-progress multipart uploads |
| `POST` | `/{bucket}?delete` | Delete multiple objects (bulk delete, quiet mode supported) |
| `POST` | `/{bucket}?rebuildIndex` | Rebuild SQLite index from `meta.json` files on disk |

### Object operations

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/{bucket}/{key}` | Upload object (plain or `aws-chunked` SigV4 streaming) |
| `PUT` | `/{bucket}/{key}` + `x-amz-copy-source` | Server-side object copy |
| `GET` | `/{bucket}/{key}` | Download object; `Range` header supported; `If-None-Match` / `If-Modified-Since` → 304 |
| `HEAD` | `/{bucket}/{key}` | Object metadata; `If-None-Match` / `If-Modified-Since` → 304 |
| `DELETE` | `/{bucket}/{key}` | Delete object |
| `POST` | `/{bucket}/{key}?uploads` | Initiate multipart upload |
| `PUT` | `/{bucket}/{key}?uploadId=…&partNumber=…` | Upload a part |
| `GET` | `/{bucket}/{key}?uploadId=…` | List uploaded parts |
| `POST` | `/{bucket}/{key}?uploadId=…` | Complete multipart upload |
| `DELETE` | `/{bucket}/{key}?uploadId=…` | Abort multipart upload |

### Authentication

| Flow | How it works |
|------|-------------|
| Open (default) | `auth.enabled: false` — all requests accepted without credentials |
| SigV4 | `auth.enabled: true` — every request must carry a valid `Authorization` header |
| Presigned URLs | Standard `?X-Amz-Signature=…` query-string auth; expiry enforced |
| Proxy-safe presigned | Set `auth.public_hostname` so server and clients agree on the hostname |

---

## Source layout

```
src/
  storage/          # storage engine (no HTTP knowledge)
    store.rs        # public API: put/get/delete/list/copy/multipart
    resolver.rs     # find or allocate an object directory by (bucket, key)
    index.rs        # SQLite index for sorted key listing
    encoding.rs     # object dir naming (V1 prefix + fanout)
    layout.rs       # maps (bucket, key) → fanout leaf directory path
    metadata.rs     # ObjectMeta / PutMeta / UploadMeta / BucketMeta structs
    staging.rs      # staging directory helpers
    sweeper.rs      # bounded background maintenance
    cache.rs        # LRU metadata cache
    locks.rs        # per-key async mutex
    aws_chunked.rs  # aws-chunked streaming decoder
    config.rs       # StorageConfig
    errors.rs       # StorageError type
    time.rs         # timestamp helpers

  server/           # HTTP layer (depends on storage, not vice versa)
    mod.rs          # Axum routing, handlers, middleware, integration tests
    auth.rs         # SigV4 / presigned-URL verification
    config.rs       # AppConfig (re-exports StorageConfig)
    logging.rs      # rolling log setup
    range.rs        # HTTP Range header parsing
    xml.rs          # S3 XML serialisation / deserialisation

  bin/
    rusts3.rs       # CLI entry point
```

---

## On-disk layout

```
<base_dir>/
  buckets/
    <bucket>/
      bucket.json            # bucket metadata (creation time, storage_version)
      index.sqlite           # SQLite object index
      objects/
        <2>/<2>/<2>/<2>/     # 4-level SHA-1 fanout (8 hex chars of SHA1(bucket+key))
          V1<6hex>/          # object directory  e.g. V1AB3F7C/
          V1<6hex>_<4hex>/   # collision suffix (rare)  e.g. V1AB3F7C_91FE/
            meta.json        # object metadata + part list
            part.1           # object data (single-part)
            part.1, part.2   # multipart data (one file per part)
      staging/
        put/<id>/            # unpublished single PUT (cleaned by maintenance)
        multipart/<id>/      # unpublished multipart upload (cleaned by maintenance)
      trash/
        <id>/                # complete objects removed from live visibility
```

PUT and multipart completion prepare a complete object directory in staging,
including `meta.json`, then publish it into `objects/` with an atomic rename.
DELETE moves the live object directory to `trash/` with an atomic rename and
removes the SQLite row afterward. Overwrite PUT is documented as delete-then-put:
if the process crashes between moving the old object to trash and publishing the
new object, the key may be absent, but no partial live object directory is left
behind.

SQLite stores a derived listing index. The live `objects/` namespace is
authoritative; reads/deletes and targeted visibility-repair rows reconcile SQLite
when drift is detected.

### Object Directory Names

Object directory names are always 8 chars (`V1` + 6 uppercase hex digits), e.g.
`V1AB3F7C`. The optional `_<4hex>` suffix (e.g. `V1AB3F7C_91FE`) handles the
rare case where two different keys hash to the same 6-char prefix within the
same fanout leaf. Lookup scans entries starting with the key-derived prefix and
confirms the match via `meta.json`.

SQLite stores only logical listing fields (`object_key`, size, ETag, and last
modified time). It does not store physical directory names. Physical location is
resolved from the key's deterministic fanout leaf and the `meta.json` files
inside that leaf.

`bucket.json` carries `storage_version: "v1"` for this initial on-disk format.

### Uniform object distribution — no directory hot-spots

The 4-level fanout is derived from `SHA1(bucket + key)`, not from the key name
itself. Objects are spread uniformly across `256^4 = 4 billion` possible leaf
directories regardless of naming patterns — sequential keys, common prefixes,
and deeply nested paths all distribute evenly. No single directory ever
accumulates more than a handful of entries, so filesystem limits caused by
oversized flat directories are not a concern.

---

## Index rebuild

If the SQLite index ever drifts from disk (e.g. after a crash or manual file
manipulation), trigger a background rebuild:

```bash
curl -X POST http://127.0.0.1:8002/my-bucket?rebuildIndex
# 202 Accepted — running in background
# 409 Conflict — already running
```

The rebuild streams the live `objects/` tree and upserts each discovered object
directly into SQLite with a rebuild timestamp. When the scan finishes, SQLite
rows not seen since the rebuild started are removed. This keeps memory bounded
and fixes both missing rows and stale rows. Progress is logged at INFO level
every 100 objects.

---

## Graceful shutdown

`SIGINT` (Ctrl-C) triggers a graceful shutdown:
1. The HTTP server stops accepting new connections and drains in-flight requests.
2. The background maintenance task and any running index rebuild are cooperatively cancelled.
3. The process exits cleanly.
