# rusts3 — S3-compatible single-node S3 server

`rusts3` is a lightweight S3-compatible server written in Rust. It is designed
for local development, integration tests, CI, and small trusted deployments. It
is not a drop-in replacement for the complete AWS S3 or MinIO feature set.

## Highlights

- Streaming single-part and multipart uploads and downloads; object bodies are
  not buffered in memory.
- AWS Signature V4 and V2 authentication, including presigned URLs, SigV4
  streaming (`aws-chunked`), and signed browser POST uploads.
- A built-in management console on a separate port for buckets, objects, IAM
  users, groups, policies, access keys, share links, and live tasks.
- AWS-style IAM policy evaluation with explicit deny, wildcard actions and
  resources, prefix/delimiter conditions, user policies, and group policies.
- Server-side copy, multipart copy, range reads, conditional reads/copies,
  bulk delete, metadata, storage-class headers, and ListObjects v1/v2.
- SQLite-primary object indexes with immutable blob directories, write-ahead
  intents, atomic publication, crash recovery, and configurable durability.
- Automatic index rebuilds, migration from the legacy four-level layout,
  staging/trash cleanup, and empty-directory reclamation.
- Request, authentication, authorization, and operation audit logs; live
  throughput and task progress; MinIO-compatible health and metrics paths.
## Screenshots

### Management console

<img width="1728" height="908" alt="Management console" src="https://github.com/user-attachments/assets/1505c045-4664-460d-b910-48ad6c3ce562" />

### IAM and policies

<img width="818" height="621" alt="IAM and policies" src="https://github.com/user-attachments/assets/b9755499-1cd0-415a-85d6-eadcac48f157" />

### Object browser

<img width="1728" height="907" alt="Object browser" src="https://github.com/user-attachments/assets/2c01dd4c-d65b-4a63-b959-c47de7e5a696" />

### Live tasks

<img width="457" height="175" alt="Live tasks" src="https://github.com/user-attachments/assets/b8adaa59-d218-473d-9d15-d6af9954be63" />

## Compatibility snapshot

The latest MinIO Mint core run exercised all 15 suites and recorded **361
passes out of 416 checks**, with 40 failures and 15 not-applicable results.
Seven suites reported suite-level success. Core bucket/object CRUD, ranged
reads, ListObjects v1/v2, multipart flows, copy/compose, presigned GET/PUT,
browser POST, health checks, and metrics all passed broadly across clients.

| Client/suite | Result in the latest run |
|---|---|
| `s3cmd` | 8 pass, 0 fail |
| AWS SDK for Ruby | 13 pass, 0 fail |
| Health checks | 6 pass, 0 fail |
| AWS CLI | 13 pass, 1 expected feature-gap failure |
| MinIO Client (`mc`) | 26 pass, 1 feature-gap failure |
| MinIO JavaScript | 225 pass, 11 feature-gap failures |

See the dated [MinIO Mint compatibility report](test_report.md) for the exact
command, suite-by-suite results, passing coverage, and remaining gaps. The
report is the source of truth; the summary above is intentionally not a claim
of complete S3 or MinIO compatibility.

## Intentional limits

The following APIs are not implemented:

- bucket versioning and delete markers;
- bucket policies through the S3 API (rusts3 IAM policies are managed in the
  console instead), ACLs, CORS, websites, lifecycle rules, and tagging;
- object lock, retention/legal hold, replication, notifications, and S3 Select;
- server-side encryption and storage-tier behavior (a storage-class value is
  metadata only);
- MinIO admin APIs and Snowball archive extraction;
- TLS termination. Put rusts3 behind a reverse proxy such as
  [Caddy](https://caddyserver.com/) for HTTPS.

`GET /{bucket}?versions` is a compatibility view of the current object and any
overwritten/deleted snapshots still present in trash. It is not durable S3
versioning: the normal trash-retention job eventually removes those snapshots,
and `?versioning` is not implemented.

## Build and run

```bash
cargo build --release
target/release/rusts3 init
# Edit config.yaml, then:
target/release/rusts3 run
```

The binary is `target/release/rusts3`. The CLI commands are:

```text
rusts3 run [-c FILE]                   Start the server (default: config.yaml)
rusts3 validate [-c FILE]              Validate configuration and exit
rusts3 genpassword [--cost N]          Generate a bcrypt console password
rusts3 verifypassword [HASH]           Verify a bcrypt console password
rusts3 init                            Write a documented config.yaml
```

Running `rusts3` without a subcommand remains supported: it uses built-in
defaults, or the file supplied with the legacy `-c` option. Only one process
may own a data directory; rusts3 enforces this with `<base_dir>/.rusts3.lock`.

## Configuration

Every field is optional. `rusts3 init` writes a commented configuration file.

### Server and UI

| Field | Default | Description |
|---|---:|---|
| `server.bind_address` | `0.0.0.0` | S3 API listen address. |
| `server.bind_port` | `8002` | S3 API listen port. |
| `server.base_dir` | `./rusts3-data` | Object, index, and IAM data root. |
| `ui.enabled` | `true` | Enable the management console. |
| `ui.bind_address` | S3 bind address | Optional separate console listen address. |
| `ui.bind_port` | `8003` | Console listen port. |

The S3 API and console are deliberately separate. S3 clients authenticate with
access keys; console users authenticate with username/password sessions.

### Storage

| Field | Default | Description |
|---|---:|---|
| `storage.sqlite_max_connections` | `50` | SQLite reader connections per bucket. Mutations use a dedicated writer. |
| `storage.meta_cache_capacity` | `200000` | Maximum cached object metadata entries. |
| `storage.durability` | `full` | `full` fsyncs blobs and uses SQLite `synchronous=FULL`; `relaxed` skips per-PUT blob fsync and uses `NORMAL`. |
| `storage.rebuild_reader_threads` | `0` | Parallel index-rebuild workers; `0` selects one per CPU core. |
| `storage.rebuild_queue_bound` | `1000` | Bounded rebuild pipeline queue. |
| `storage.rebuild_batch_size` | `1000` | SQLite rows written per rebuild transaction. |

`full` is the safe default for power-loss durability. `relaxed` improves write
throughput but can lose the last acknowledged writes after power loss. A normal
process crash preserves committed writes in either mode.

### Authentication and IAM

| Field | Default | Description |
|---|---:|---|
| `auth.enabled` | `false` | Require signatures on the S3 API. Health and metrics endpoints remain public. |
| `auth.credentials` | `[]` | Legacy unrestricted `{access_key, secret_key}` pairs. |
| `auth.users` | `[]` | Built-in, unrestricted bootstrap administrators. |
| `auth.public_hostname` | absent | Public hostname and optional port used to verify proxy-safe signatures and generate console share links. Do not include a scheme. |
| `auth.public_scheme` | `http` | `http` or `https`, used for generated share links. |

A built-in administrator can have a console password, any number of S3 API
keys, or both:

```yaml
auth:
  enabled: true
  public_hostname: "s3.example.test"
  public_scheme: https
  users:
    - user: admin
      password: "$2b$12$..." # generate with: rusts3 genpassword
      api_keys:
        - ak: "ROOTKEY001"
          secret: "replace-this-secret"

ui:
  enabled: true
  bind_port: 8003
```

Built-in users live in configuration, cannot be edited at runtime, and bypass
policy checks. Runtime users, groups, access keys, and policies live in
`<base_dir>/admin.sqlite`. Runtime users are default-deny unless an attached
user/group policy allows the request; a matching explicit deny wins. The
console provides both a read/write rule builder and a JSON policy editor.

Bcrypt is recommended for built-in console passwords. Cleartext remains
accepted for compatibility. S3 secrets must remain recoverable because request
authentication requires the original HMAC key.

### Logging

| Field | Default | Description |
|---|---:|---|
| `logging.level` | `info` | `trace`, `debug`, `info`, `warn`, or `error`. |
| `logging.enable_bandwidth_report` | `true` | Log aggregate bytes, rates, request totals, and QPS every 10 seconds. |
| `logging.dir` | absent | Split logs into `auth.log`, `authz.log`, `audit.log`, and `server.log`. Takes precedence over `file`. |
| `logging.file` | absent | Combined rolling log file. With neither `dir` nor `file`, logs go to stdout only. |
| `logging.rotation_size_mb` | `100` | Rotate a file at this size. |
| `logging.keep_files` | `5` | Number of rotated archives to keep. |
| `logging.compress` | `false` | Gzip rotated archives. |

File logging also echoes to stdout. Every S3 and console request receives a
correlation ID, returned in `x-amz-request-id` and included in logs.

### Background maintenance

| Field | Default | Description |
|---|---:|---|
| `sweeper.interval_secs` | `300` | Schedule interval for intent resolution, staging/trash cleanup, and legacy-layout migration. |
| `sweeper.intent_batch_size` | `100` | Stale intents processed per batch; a run drains all eligible batches. |
| `sweeper.intent_grace_period_secs` | `3600` | Minimum intent age during normal operation. Startup recovery bypasses the grace period. |
| `sweeper.staging_expiry_secs` | `86400` | Idle age before abandoned PUT/multipart staging is removed. |
| `sweeper.trash_expiry_secs` | `86400` | Idle age before retired blobs are removed; values below 10800 (3 hours) are rejected. |
| `sweeper.reclaim_interval_secs` | `300` | Interval for reclaiming empty fanout directories. |

Older visibility-repair setting names are accepted as aliases for the intent
batch/grace settings.

## Management console

With its defaults, open `http://127.0.0.1:8003`. Configure at least one
`auth.users` entry with a password to bootstrap console administration.

The console supports:

- creating/deleting buckets and browsing, uploading, downloading, and deleting
  objects;
- generating presigned share links (requires `auth.public_hostname`);
- creating runtime users and groups, resetting passwords, rotating access
  keys, and editing user/group policies;
- bucket statistics and operator-triggered index rebuilds;
- a WebSocket-powered task monitor for active/recent S3 requests and jobs,
  transfer progress and throughput, with cancellation for safe cancellable
  work.

Console-generated share links carry the creator's current authority. Deleting
the user or changing their policy therefore revokes or narrows existing links.

## AWS CLI

Configure a profile whose keys match a built-in or runtime access key:

```bash
aws configure --profile local
# region: us-east-1

export AWS_PROFILE=local
export AWS_ENDPOINT_URL=http://127.0.0.1:8002

aws s3 mb s3://my-bucket
aws s3 cp ./file.bin s3://my-bucket/path/file.bin
aws s3 ls s3://my-bucket/
aws s3 cp s3://my-bucket/path/file.bin ./download.bin
aws s3 cp s3://my-bucket/path/file.bin s3://my-bucket/copy.bin
aws s3 rm s3://my-bucket/path/ --recursive
aws s3 presign s3://my-bucket/copy.bin --expires-in 3600
```

Multipart upload is selected automatically by the AWS CLI for sufficiently
large files. Low-level multipart operations are also available through
`aws s3api`.

When a reverse proxy changes the `Host` header, set `auth.public_hostname` to
the exact hostname (including a nonstandard port) configured in clients. This
keeps presigned SigV4 verification consistent through the proxy.

## Supported S3 operations

### Buckets

| Method | Resource | Operation |
|---|---|---|
| `GET` | `/` | List buckets. |
| `PUT` / `HEAD` / `DELETE` | `/{bucket}` | Create, inspect, or delete an empty bucket. |
| `GET` | `/{bucket}` | ListObjects v1/v2 with prefix, delimiter, marker/continuation token, encoding, and pagination. |
| `GET` | `/{bucket}?location` | Get bucket location. |
| `GET` | `/{bucket}?uploads` | List multipart uploads. |
| `GET` | `/{bucket}?versions` | Compatibility listing of retained current/retired snapshots; not S3 versioning. |
| `POST` | `/{bucket}?delete` | Multi-object delete, including quiet mode. |
| `POST` | `/{bucket}?rebuildIndex` | Start an index rebuild (`202`; `409` if already running). |
| `POST` | `/{bucket}` | SigV4 browser form upload with policy validation. |

### Objects

| Method | Resource | Operation |
|---|---|---|
| `PUT` | `/{bucket}/{key}` | Streaming upload, including `aws-chunked`, content hashes, metadata, and storage class. |
| `GET` / `HEAD` | `/{bucket}/{key}` | Streaming read, single byte ranges, conditional reads, response-header overrides, and metadata. |
| `DELETE` | `/{bucket}/{key}` | Idempotent delete. `forceDelete=true` / `x-minio-force-delete` deletes a prefix for MinIO compatibility. |
| `PUT` | object + `x-amz-copy-source` | Server-side copy with metadata directive and source preconditions. |
| `POST` | object + `?uploads` | Initiate multipart upload. |
| `PUT` | object + `uploadId`, `partNumber` | Upload a part, or copy a source/range into a part. |
| `GET` | object + `uploadId` | List uploaded parts. |
| `POST` | object + `uploadId` | Complete multipart upload. |
| `DELETE` | object + `uploadId` | Abort multipart upload. |

Authentication supports header and query-string SigV4/SigV2 requests.
Presigned SigV4 URLs enforce AWS's seven-day maximum expiry. Browser POST
policies validate expiry and form conditions before accepting the object.

## Health and metrics

These unauthenticated compatibility endpoints are available on the S3 port:

```text
GET /minio/health/live
GET /minio/health/ready
GET /minio/prometheus/metrics
GET /minio/v2/metrics/{cluster,node,bucket,resource}
```

The metrics endpoints currently expose a minimal `rusts3_up 1` Prometheus
gauge. Detailed request/byte totals and rates are available in periodic logs
and the console task view.

## Storage and recovery model

Each bucket has an authoritative SQLite index. Object data is stored in an
immutable, self-describing blob directory and the index records that directory's
path. A mutation follows this protocol:

1. Stream a complete blob into staging.
2. Commit a publish intent.
3. Atomically rename the blob into the live tree.
4. Atomically switch the index row and record retirement of any old blob.
5. Move the old blob to trash and clear the retirement intent.

After a crash, startup drains the small intent table instead of scanning every
object. Reads and listings use SQLite as the source of truth. Per-key locking,
monotonic modification timestamps, and immutable snapshots keep concurrent
overwrites/deletes consistent with in-flight downloads.

The current layout uses a single 16-bit fanout directory:

```text
<base_dir>/
  .rusts3.lock
  admin.sqlite
  buckets/<bucket>/
    bucket.json
    index.sqlite
    objects/<4-hex>/V1<6-hex>_<unique>/
      meta.json
      part.1, part.2, ...
    staging/put/<id>/
    staging/multipart/<upload-id>/
    trash/<id>/
```

Older four-level fanout objects remain readable and are migrated in the
background. Empty directories are reclaimed without deleting non-empty object
directories.

If an index is missing or uses an old schema, rusts3 starts a parallel rebuild
from `meta.json`. The affected bucket returns `503 SlowDown` while rebuilding,
so normal S3 retry behavior applies. An operator can also trigger the same
tracked job from the console or with:

```bash
curl -X POST 'http://127.0.0.1:8002/my-bucket?rebuildIndex'
```

## Graceful shutdown

`SIGINT` (Ctrl-C) stops accepting new connections, drains the S3 server,
cancels background work through cooperative cancellation, releases the
data-root lock, and exits cleanly.
