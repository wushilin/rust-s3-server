# rusts3 — S3-compatible local dev server

A lightweight S3-compatible object storage server written in Rust.
Built for **local development and CI/CD** — not a production replacement for AWS S3 or MinIO.

---

## Design goals

| Goal | Notes |
|------|-------|
| S3 wire-compatible | Works with the AWS CLI, AWS SDKs, and any library that targets the S3 API |
| Low overhead | Streaming reads/writes — objects are never fully buffered in memory |
| Durable | Each bucket has a SQLite index; `meta.json` acts as an atomic visibility gate |
| Self-healing | On GET/DELETE of an orphaned key the stale index row and directory are cleaned up automatically; the index can be rebuilt in the background via HTTP |
| Observable | Structured per-request logging (method, URI, status, size, latency) via log4rs |
| Configurable | Single `config.yaml` controls network, logging, and auth; CLI flags override individual fields |

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

```
rusts3 [OPTIONS]

Options:
  -c, --config <FILE>   Path to config.yaml
      --init            Write a default config.yaml and exit
  -h, --help            Print help
```

### Quickstart

```bash
# Generate a documented config.yaml in the current directory:
target/release/rusts3 --init

# Edit config.yaml to taste, then start:
target/release/rusts3 -c config.yaml
```

Copy `config.yaml` from the repo root and edit to taste — every field is documented inline.

---

## Configuration reference (`config.yaml`)

```yaml
server:
  bind_address: "0.0.0.0"   # all interfaces
  bind_port: 8002
  base_dir: "./rusts3-data"

logging:
  level: "info"              # trace | debug | info | warn | error
  # file: "/var/log/rusts3/rusts3.log"   # omit for stdout only
  rotation_size_mb: 100
  keep_files: 5
  compress: false            # gzip archives

auth:
  enabled: false
  # credentials:
  #   - access_key: "minioadmin"
  #     secret_key: "minioadmin"
```

---

## Using with the AWS CLI

Configure a profile that points at the local server:

```bash
aws configure --profile local
# AWS Access Key ID:     minioadmin   (must match config.yaml when auth.enabled = true)
# AWS Secret Access Key: minioadmin
# Default region:        ap-southeast-1
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

# Delete
aws s3 rm s3://my-bucket/path/file.bin

# Multipart upload (automatic for files > 8 MB by default)
aws s3 cp ~/large.iso s3://my-bucket/large.iso
```

### Pre-signed URLs

```bash
aws s3 presign s3://my-bucket/file.bin --expires-in 3600
# Returns a URL valid for 1 hour; full SigV4 signature is verified on use.
```

---

## Operations reference

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | List all buckets |
| `PUT` | `/{bucket}` | Create bucket |
| `DELETE` | `/{bucket}` | Delete bucket (must be empty) |
| `GET` | `/{bucket}` | List objects (v2 with `continuation-token`, v1 with `marker`) |
| `POST` | `/{bucket}?rebuildIndex` | Rebuild SQLite index from `meta.json` files |
| `PUT` | `/{bucket}/{key}` | Upload object (plain or `aws-chunked` SigV4 streaming) |
| `GET` | `/{bucket}/{key}` | Download object; supports `Range` header |
| `HEAD` | `/{bucket}/{key}` | Object metadata |
| `DELETE` | `/{bucket}/{key}` | Delete object |
| `POST` | `/{bucket}/{key}?uploads` | Initiate multipart upload |
| `PUT` | `/{bucket}/{key}?uploadId=…&partNumber=…` | Upload a part |
| `POST` | `/{bucket}/{key}?uploadId=…` | Complete multipart upload |
| `DELETE` | `/{bucket}/{key}?uploadId=…` | Abort multipart upload |

---

## On-disk layout

```
<base_dir>/
  buckets/
    <bucket>/
      bucket.json          # bucket metadata (creation time)
      index.sqlite         # SQLite object index
      objects/
        <2>/<2>/<2>/<2>/   # 4-level SHA-1 fanout of the physical ID
          <physical-id>/
            meta.json      # object metadata + part list (visibility gate)
            part.1         # object data (single-part)
            part.1, part.2 # multipart data
      staging/
        put/<id>/          # in-progress single PUT (cleaned by sweeper)
        multipart/<id>/    # in-progress multipart (cleaned by sweeper)
```

`meta.json` is written **last** on PUT and removed **first** on DELETE.  This
gives atomic object visibility without locking.  A missing `meta.json` always
means the object does not exist, regardless of what the SQLite index says.

---

## Index rebuild

If the SQLite index ever drifts from disk (e.g. after a crash or manual file
manipulation), trigger a background rebuild:

```bash
curl -X POST http://127.0.0.1:8002/my-bucket?rebuildIndex
# 202 Accepted — running in background
# 409 Conflict — already running
```

Progress is logged at INFO level every 100 objects.

---

## Graceful shutdown

`SIGINT` (Ctrl-C) triggers a graceful shutdown:
1. The HTTP server stops accepting new connections and drains in-flight requests.
2. The sweeper task and any running index rebuild are cooperatively cancelled.
3. The process exits cleanly.
