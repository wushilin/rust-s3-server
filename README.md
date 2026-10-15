# rusts3 — single-node S3-compatible server

Why not RustFS? RustFS uses a lot of CPU for useless stuff. I used ZFS with integrity check 
yet RustFS spends huge amount of CPU time when idling. 

Rust-S3-Server uses 0% CPU when you are not using it. It does zero background magic.

It does FSYNC when you upload objects (you mean realize the upload of many objects might look slower), 
it prioritizes integrity over naive speed.

A lightweight, single-node S3-compatible server in Rust, with a built-in
management console. The core AWS S3 API — authentication, bucket/object CRUD,
listings, multipart, copy, presigning, browser POST, and both path-style and
virtual-hosted-style addressing — is verified continuously against the MinIO
Mint suite and real AWS SDKs.

Good fit for local development, integration tests, CI, and small trusted
production deployments. Features that only make sense at AWS scale (ACLs,
S3 Select, object lock) are [deliberately out of scope](#intentional-limits).

## Quick start

```bash
cargo build --release
target/release/rusts3 init      # writes config.yaml with every option at its default
target/release/rusts3 run
```

Or with Docker — the image runs with no configuration at all:

```bash
docker build -t rusts3:latest .
docker run -d --name rusts3 \
  -p 8002:8002 -p 8003:8003 \
  -v rusts3-data:/data \
  -e RUSTS3_ADMIN_PASSWORD=change-me \
  -e RUSTS3_ACCESS_KEY=myaccesskey -e RUSTS3_SECRET_KEY=mysecretkey \
  rusts3:latest
```

- **S3 API** → `http://127.0.0.1:8002` (point the AWS CLI/SDKs, `mc`, `s3cmd`, rclone here)
- **Console** → `http://127.0.0.1:8003` (username/password login)

The image also ships `rs3`, the bundled high-performance CLI client
(see [`client/README.md`](client/README.md)), so a running container can be
driven without installing anything else:

```bash
docker exec -e MC_HOST_LOCAL=http://myaccesskey:mysecretkey@127.0.0.1:8002 \
  rusts3 rs3 ls --recursive local/
```

## The two surfaces

rusts3 exposes two independent HTTP surfaces from one binary:

| | S3 API (`8002`) | Management console (`8003`) |
|---|---|---|
| Speaks | AWS S3 wire protocol | Operator web UI |
| Auth | Access keys (SigV4/SigV2, presigned, browser POST) | Username/password sessions |
| For | Any S3 client or SDK | Day-to-day operation and IAM admin |

Credentials never mix: a leaked console password grants no S3 API access, and
vice versa. Each surface can be firewalled or proxied independently.

## Highlights

- **Fully streaming** single-part and multipart uploads/downloads — object
  bodies are never buffered in memory, on the API or in the console.
- **SigV4 and SigV2** authentication, presigned URLs, `aws-chunked` streaming,
  and signed browser POST uploads.
- **Path-style and virtual-hosted-style addressing**, with adaptive SigV4
  verification for both shapes.
- **AWS-style IAM policy evaluation**: explicit deny, wildcards, prefix/delimiter
  conditions, user and group policies — managed from the console.
- **Management console**: object browser with multi-select and bulk delete,
  folder drag-and-drop upload, transparent multipart for large files (to the
  5 TiB S3 ceiling), in-flight multipart visibility with one-click abort, IAM
  administration, share links, and a live task monitor.
- **Crash-safe storage**: RocksDB-primary indexes over immutable blob
  directories, write-ahead intents, atomic publication, automatic index
  rebuilds, and configurable durability.
- Request/auth/authz/operation audit logs, live throughput, and
  MinIO-compatible health and metrics paths.

<details>
<summary><strong>Screenshots</strong></summary>

### Login

<img width="1728" height="908" alt="Management console" src="https://github.com/user-attachments/assets/1505c045-4664-460d-b910-48ad6c3ce562" />

### Object browser

<img width="1726" height="900" alt="Object browser" src="https://github.com/user-attachments/assets/f3047bc9-7d65-4022-a8c3-903b20fb6b1f" />

### IAM and policies

<img width="818" height="621" alt="IAM and policies" src="https://github.com/user-attachments/assets/b9755499-1cd0-415a-85d6-eadcac48f157" />

### Policy builder

<img width="762" height="505" alt="Policy builder" src="https://github.com/user-attachments/assets/4b667451-3a59-4db6-9027-4023541f9bd6" />

### Live tasks

<img width="457" height="175" alt="Live tasks" src="https://github.com/user-attachments/assets/b8adaa59-d218-473d-9d15-d6af9954be63" />

### Object scanner

<img width="1082" height="799" alt="Object scanner" src="https://github.com/user-attachments/assets/06745c48-5660-4b3c-aed7-c1d0807ced2e" />

</details>

## Compatibility

### MinIO Mint

The latest MinIO Mint core run exercised all 15 suites: **362 passes out of 417
checks**, 25 failures, 30 not applicable. Core bucket/object CRUD, ranged reads,
ListObjects v1/v2, multipart, copy/compose, presigned GET/PUT, browser POST,
health checks, and metrics passed broadly across clients. Unimplemented features
now report `NotImplemented` instead of being silently accepted, which is why the
not-applicable count is higher and the failure count lower than in earlier runs.

| Client/suite | Result |
|---|---|
| `s3cmd` | 8 pass, 0 fail |
| AWS SDK for Ruby | 13 pass, 0 fail |
| Health checks | 6 pass, 0 fail |
| AWS CLI | 16 pass, 0 fail |
| MinIO Client (`mc`) | 26 pass, 1 feature-gap failure |
| MinIO JavaScript | 234 pass, 3 feature-gap failures |

The dated [MinIO Mint compatibility report](test_report.md) is the source of
truth — the summary above is not a claim of complete S3 or MinIO compatibility.

### The Rust data stack (`object_store`)

rusts3 ships a dedicated harness that drives the
[`object_store`](https://crates.io/crates/object_store) crate (0.14, `aws`
feature) against a live server — the S3 client underneath **Delta Lake,
DataFusion, Iceberg, Lance**, and much of the Rust data ecosystem. If it passes,
those applications' storage layer works on rusts3.

Fifteen tests cover put/get roundtrip, overwrite, empty objects, `head` (size +
ETag), `get_range` / `get_ranges`, `NotFound` mapping, recursive `list`,
`list_with_delimiter`, `list_with_offset`, `copy`, `rename`, multipart upload
and abort, and **conditional PUT** (`PutMode::Create` via `If-None-Match: *` —
the atomic-commit primitive Delta Lake and Iceberg depend on). No known gaps.

```sh
cd compat/object_store && ./run.sh
```

The launcher is self-contained: it builds rusts3, starts a throwaway instance on
a temp data dir and free ports, creates the bucket, runs the tests, and tears
everything down. See [`compat/object_store/README.md`](compat/object_store/README.md)
for filtering, port overrides, and running against an existing server.

## Intentional limits

Not implemented:

- bucket policies via the S3 API (use console IAM policies instead), ACLs,
  websites, lifecycle rules, tagging;
- object lock, retention/legal hold, replication, notifications, S3 Select;
- server-side encryption and storage-tier behavior (storage class is metadata only);
- MinIO admin APIs and Snowball archive extraction;
- **TLS termination** — put rusts3 behind a reverse proxy (see below).

On a bucket that never had versioning, `GET /{bucket}?versions` reports the
current object of each key as its single `null` version, exactly as S3 does.
Overwritten and deleted blobs of such a bucket are retained in trash for the
retention window as an operator recovery buffer, not as versions. See
[Object versioning](#object-versioning) for buckets that keep versions.

## Configuration

Every field is optional and falls back to the default below. `rusts3 init`
writes a complete `config.yaml` by serializing the built-in defaults, so it can
never drift from the parser — the trade-off is no inline comments, which is why
field documentation lives here and in `src/server/config.rs` /
`src/storage/config.rs`.

A minimal authenticated setup:

```yaml
server:
  bind_port: 8002
  base_dir: ./rusts3-data

auth:
  enabled: true
  public_hostname: "s3.example.test"   # no scheme; enables host-style + share links
  public_scheme: https
  users:
    - user: admin
      password: "$2b$12$..."           # generate with: rusts3 genpassword
      api_keys:
        - ak: "ROOTKEY001"
          secret: "replace-this-secret"

ui:
  enabled: true
  bind_port: 8003
```

Built-in users live in configuration, cannot be edited at runtime, and bypass
policy checks. Runtime users, groups, access keys, and policies live in
`<base_dir>/admin.rocksdb` and are **default-deny** unless an attached
user/group policy allows the request; a matching explicit deny always wins.
A user's effective policy is their own plus that of every group they belong to,
and any change to it applies to the next request of an already-issued key.

- **The `admin` group is root.** Its members are unrestricted exactly like
  built-in users: no policy is evaluated for them, so no deny — from another
  group or attached to them directly — can bind them.
- **Listing buckets is filtered, not refused.** `ListBuckets` (and the console)
  show a caller the buckets their policy may reach, including one they hold only
  a prefix of. Granting `s3:ListAllMyBuckets` lists every bucket, a deny on a
  whole bucket hides it, and an explicit deny of `s3:ListAllMyBuckets` refuses
  the call. Seeing a bucket grants nothing in it.
- **Access keys carry tags and a last-used stamp.** Every key needs at least one
  tag (letters, digits, and spaces; up to 100 tags of 256 characters) saying what
  uses it; tags are the only editable part of a key. The console shows when and
  from which address each key — built-in ones included — last authenticated.
  Behind a reverse proxy on a private address the client address is taken from
  `X-Forwarded-For`/`X-Real-IP`, and behind a TCP load balancer from the PROXY
  protocol header (v1 or v2, auto-detected on both ports). The stamps live in
  `<base_dir>/key_usage.rocksdb`, apart from the IAM database and its export.

Bcrypt is recommended for console passwords (cleartext is still accepted). S3
secrets must stay recoverable, because request authentication needs the original
HMAC key.

<details>
<summary><strong>Full field reference</strong></summary>

### Server and UI

| Field | Default | Description |
|---|---:|---|
| `server.bind_address` | `0.0.0.0` | S3 API listen address. |
| `server.bind_port` | `8002` | S3 API listen port. |
| `server.base_dir` | `./rusts3-data` | Object, index, and IAM data root. |
| `server.site_name` | unset | This site's readable name, shown in the console and to replication peers. Only a label. Without this setting, a two-word name (like `quiet-falcon`) is generated on first start and an admin can change it any time in the console ("Change", bottom of the sidebar), effective at once; with it, the configured name is applied at every start and the console cannot change it. (The **site id** itself is not configurable: it is generated on first start, kept in `<base_dir>/site.json`, and never changes — it is in every version stamp the site writes and is how replication peers know it. A config that still sets `server.site_id` is refused at start.) |
| `server.proxy_protocol` | `auto` | PROXY protocol v1/v2 on both listeners. `auto` detects it per connection — a header from a loopback/private peer names the real client, a connection without one is plain HTTP; a header from a public peer is dropped. `off` never looks for one. |
| `ui.enabled` | `true` | Enable the management console. |
| `ui.bind_address` | S3 bind address | Optional separate console listen address. |
| `ui.bind_port` | `8003` | Console listen port. |
| `ui.public_hostname` | absent | Public console hostname and optional port; its exact origin is allowed to PUT through the S3 CORS layer. Required for direct console uploads. |
| `ui.public_scheme` | `http` | Console origin scheme (`http` or `https`). |

### Storage

| Field | Default | Description |
|---|---:|---|
| `storage.meta_cache_capacity` | `200000` | Maximum cached object metadata entries. |
| `storage.durability` | `full` | `full` fsyncs blobs and syncs the RocksDB index WAL on every write; `relaxed` skips per-PUT blob fsync and leaves the WAL to the OS. |
| `storage.rebuild_reader_threads` | `0` | Parallel index-rebuild workers; `0` selects one per CPU core. |
| `storage.rebuild_queue_bound` | `1000` | Bounded rebuild pipeline queue. |
| `storage.rebuild_batch_size` | `1000` | Index rows written per rebuild batch. |

`full` is the safe default for power-loss durability. `relaxed` improves write
throughput but can lose the last acknowledged writes after power loss. A normal
process crash preserves committed writes in either mode.

**Filesystem.** Put the data directory on a checksumming filesystem:
**Btrfs** or **ZFS**. Both detect a flipped bit on read (and repair it from
a mirror or parity copy), scale to the directory counts a large bucket
produces, and compress transparently, which matters for text-heavy buckets.
ext4 and XFS work but neither verifies data integrity, so a silent disk error
reaches the client as a wrong byte with a matching ETag; the health scan
catches size and metadata drift, not bit flips. The server never relies on
filesystem features beyond rename, hardlink and fsync, so any POSIX
filesystem is supported — the recommendation is about what the disk can do
to your data.

**Storage layout versions.** Each bucket records its on-disk layout in its
`bucket.json`: `"layout": {"state": "stable", "version": 3}`, or while an
upgrade runs `{"state": "migrating", "from": 2, "to": 3}`. This build writes
layout 3 and still serves layouts 1 and 2:

| Layout | Shape | Written by |
|---:|---|---|
| 1 | `objects/aa/bb/cc/dd/<leaf>`, 4-level fanout, `meta.json` format 1 | the first releases |
| 2 | `objects/<4hex>/<leaf>`, single-level fanout, `meta.json` format 1 | releases before versioning |
| 3 | `objects/<4hex>/<sha1(key)>/<stamp>/`, one dir per key, one per version, `meta.json` format 2 (every blob carries its stamp; delete markers on disk) | this build |

Every layout lives in its own sealed file under `src/storage/layouts/` with
a registry of what is supported; a change in shape is a new layout, never an
edit of an old one, and a test builds a bucket in every supported layout and
proves this build reads and upgrades it. At startup the server records the
layout of buckets that never had one, refuses to start on a layout written by
a newer build, and resumes any upgrade that was started and interrupted. An upgrade is started by an admin
(bucket settings → **Storage layout** → **Upgrade now**, or
`POST /api/buckets/{name}/upgrade-layout`); the console flags buckets that
can be upgraded. While it runs the bucket answers `503` to every data request
and shows in the tasks panel; a bucket is never served on a half-migrated
layout, and a step that fails is retried every `sweeper.interval_secs` with
the bucket still unavailable. Moving an object is a hardlink and an index
update, so no data is copied. The step to layout 3 also materialises a stamp
for objects from before stamps existed and writes the delete marker of every
tombstone, so afterwards a rebuild from the tree alone recovers every object,
every version and every delete.

**Index rebuilds and the changelog.** A rebuild walks the tree and rewrites
the index. Its changelog (what webhooks and replication links read) is
carried across only when the rebuilt catalog is the same catalog; if the tree
disagreed with the prior index, or the index was lost, the log starts over
under a new id and consumers detect the switch and resnapshot rather than
miss a change.

### Authentication and IAM

| Field | Default | Description |
|---|---:|---|
| `auth.enabled` | `false` | Require signatures on the S3 API. Health and metrics endpoints remain public. |
| `auth.credentials` | `[]` | Legacy unrestricted `{access_key, secret_key}` pairs. |
| `auth.users` | `[]` | Built-in, unrestricted bootstrap administrators. |
| `auth.public_hostname` | absent | Public hostname and optional port used to verify proxy-safe signatures, generate share links, and enable virtual-hosted-style addressing (`<bucket>.<public_hostname>`). No scheme. |
| `auth.public_scheme` | `http` | `http` or `https`, used for generated share links. |

### Logging

| Field | Default | Description |
|---|---:|---|
| `logging.level` | `info` | `trace`, `debug`, `info`, `warn`, or `error`. |
| `logging.enable_bandwidth_report` | `true` | Log aggregate bytes, rates, request totals, and QPS every 10 seconds. |
| `logging.dir` | absent | Split logs into `auth.log`, `authz.log`, `audit.log`, `server.log`. Takes precedence over `file`. |
| `logging.file` | absent | Combined rolling log file. With neither `dir` nor `file`, logs go to stdout only. |
| `logging.rotation_size_mb` | `100` | Rotate a file at this size. |
| `logging.keep_files` | `5` | Number of rotated archives to keep. |
| `logging.compress` | `false` | Gzip rotated archives. |

File logging also echoes to stdout. Every request receives a correlation ID,
returned in `x-amz-request-id` and included in logs. Access lines (S3 API and
console), failed authentication and authorization, and console logins end in
`from=<client address>` — the PROXY protocol client behind a TCP load balancer,
a private reverse proxy's `X-Forwarded-For`/`X-Real-IP`, otherwise the TCP peer.

### Background maintenance

| Field | Default | Description |
|---|---:|---|
| `sweeper.interval_secs` | `300` | Schedule interval for intent resolution and staging/trash cleanup; also the retry interval of a failed storage layout upgrade. |
| `sweeper.intent_batch_size` | `100` | Stale intents processed per batch; a run drains all eligible batches. |
| `sweeper.intent_grace_period_secs` | `3600` | Minimum intent age during normal operation. Startup recovery bypasses the grace period. |
| `sweeper.staging_expiry_secs` | `86400` | Idle age before abandoned single-PUT staging is removed. |
| `sweeper.multipart_upload_expiry_secs` | `2592000` | Idle age before an incomplete multipart upload is removed; `0` keeps them forever (S3 behavior). |
| `sweeper.trash_expiry_secs` | `86400` | Idle age before retired blobs are removed; values below 10800 (3 hours) are rejected. |
| `sweeper.reclaim_interval_secs` | `300` | Interval for reclaiming empty fanout directories. |

Older visibility-repair setting names are accepted as aliases for the intent
batch/grace settings.

</details>

<details>
<summary><strong>Changelog, webhooks and replication</strong></summary>

| Field | Default | Description |
|---|---:|---|
| `changelog.retention_days` | `14` | Changelog entries older than this are trimmed, whether or not every subscription has read them. |
| `changelog.trim_interval_secs` | `600` | How often the trim and the tombstone purge run. |
| `replication.check_interval_secs` | `5` | Supervisor tick: an enabled replication link without a live worker gets one within this. |
| `replication.worker_stale_secs` | `600` | A link worker silent for this long is replaced. A worker renews its lease while it makes progress (every chunk of a transfer counts, renewed at most every 5s), so a slow transfer of a large object is never mistaken for a hang. |
| `replication.parallelism` | `8` | Changes a link applies concurrently within one batch. |
| `changelog.tombstone_days` | `14` | How long a delete's tombstone (its stamp) is kept, so an older replicated write cannot bring the object back. |
| `webhooks.check_interval_secs` | `5` | Checker tick: muted webhooks whose mute has passed resume on the next tick. |
| `webhooks.mute_secs` | `30` | Mute after any unsuccessful delivery (per-webhook override). A longer `Retry-After` wins. |
| `webhooks.max_mute_secs` | `900` | Cap on a receiver's `Retry-After`. |
| `webhooks.request_timeout_secs` | `10` | Per-delivery HTTP timeout. |
| `webhooks.max_concurrent_deliveries` | `16` | Deliveries in flight across all webhooks (each webhook has at most one). |
| `webhooks.batch_max_events` | `500` | Events per batch (per-subscription override). |
| `webhooks.batch_max_bytes` | `1048576` | Approximate bytes per batch (per-subscription override). |
| `webhooks.allow_http` | `false` | Allow plain `http://` webhook URLs. |
| `webhooks.allow_private_addresses` | `false` | Allow webhook URLs resolving to loopback / private / link-local addresses (SSRF guard). |

</details>

## CLI

```text
rusts3 run [-c FILE]                   Start the server (default: config.yaml)
rusts3 validate [-c FILE]              Validate configuration and exit
rusts3 genpassword [--cost N]          Generate a bcrypt console password
rusts3 verifypassword [HASH]           Verify a bcrypt console password
rusts3 init                            Write a config.yaml with every option at its default
rusts3 healthcheck [-c FILE]           Probe a running server; exit non-zero if unhealthy
```

Running `rusts3` with no subcommand is still supported (built-in defaults, or
the file given with the legacy `-c`). Only one process may own a data directory;
this is enforced with `<base_dir>/.rusts3.lock`.

## Using the AWS CLI

```bash
aws configure --profile local          # region: us-east-1
export AWS_PROFILE=local
export AWS_ENDPOINT_URL=http://127.0.0.1:8002

aws s3 mb s3://my-bucket
aws s3 cp ./file.bin s3://my-bucket/path/file.bin
aws s3 ls s3://my-bucket/
aws s3 cp s3://my-bucket/path/file.bin s3://my-bucket/copy.bin
aws s3 rm s3://my-bucket/path/ --recursive
aws s3 presign s3://my-bucket/copy.bin --expires-in 3600
```

Multipart is selected automatically for large files; low-level multipart is
available via `aws s3api`.

## Reverse proxy and TLS

rusts3 serves plain HTTP and delegates TLS to a fronting proxy. A production
setup typically publishes three names:

| Public name | Backend | Purpose |
|---|---|---|
| `rusts3.example.com` | `127.0.0.1:8003` | Management console. |
| `s3.example.com` | `127.0.0.1:8002` | S3 API, path-style. Set as `auth.public_hostname`. |
| `*.s3.example.com` | `127.0.0.1:8002` | S3 API, virtual-hosted-style (bucket subdomains). |

Whatever proxy you use, it **must**:

1. **preserve the `Host` header** — SigV4 verification and host-style bucket
   detection both depend on it;
2. stream request/response bodies without size limits or buffering (console
   multipart parts are 256 MiB);
3. pass WebSocket upgrades for the console's `/api/tasks/ws` task monitor.

Then set `auth.public_hostname: s3.example.com` and `auth.public_scheme: https`
so presigned links and host-style detection agree with the public names.

<details>
<summary><strong>Proxy choices and virtual-hosted-style details</strong></summary>

### Recommended: tlsproxy_rs

[`tlsproxy_rs`](https://github.com/wushilin/tlsproxy_rs) is a hostname-routed
proxy with **integrated ACME certificate management** — it obtains and renews
Let's Encrypt certificates automatically via TLS-ALPN-01, issuing per-hostname
certificates on demand as routes are hit (renewals start 15 days before expiry).
That makes it a natural fit: point the three names above at their backend ports
through its admin console, and certificates for the console, the API endpoint,
and each bucket subdomain are provisioned and rotated with no manual handling.
It also provides an audited admin UI with configuration revisions and rollback,
hot reload, and load-balanced backend pools.

### Alternative: Caddy

[Caddy](https://caddyserver.com/) works equally well; the wildcard certificate
for bucket subdomains requires the DNS-01 challenge (a DNS provider plugin):

```caddyfile
rusts3.example.com {
    reverse_proxy 127.0.0.1:8003
}
s3.example.com, *.s3.example.com {
    reverse_proxy 127.0.0.1:8002
}
```

### Virtual-hosted-style addressing

Setting `auth.public_hostname` enables host-style requests: any request whose
`Host` is `<bucket>.<public_hostname>` is served as an access to that bucket,
while requests to the bare hostname stay path-style — the same dual behavior as
AWS S3. SigV4 verification is adaptive: each request is verified against exactly
the path and host the client signed, in either style. The proxy must provide
wildcard DNS and TLS for `*.<public_hostname>` and preserve `Host`.

</details>

## Releases and CI

Every push runs both crates' test suites on Linux (x86_64 and aarch64), macOS,
and Windows, and builds and exercises the container image (`docker-test.sh`).

Releases are cut by pushing a tag; GitHub Actions builds, smoke-tests, and
publishes them (`.github/workflows/release.yml`):

| Tag | Component | Assets |
|---|---|---|
| `v<version>` | server `rusts3` | Linux x86_64/aarch64 (static musl), macOS aarch64/x86_64, FreeBSD x86_64, Windows x86_64/aarch64; `SHA256SUMS`; Docker image `wushilin/rusts3:<version>` and `:latest` (multi-arch, when Docker Hub credentials are configured as the `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` repository secrets) |
| `rs3-v<version>` | client `rs3` | the same platforms |

The tag must match the `version` in the crate's `Cargo.toml`. FreeBSD aarch64
is not built: it is a Tier 3 Rust target with no standard library shipped by
rustup. The FreeBSD server binary links the system RocksDB (`pkg install
rocksdb`); librocksdb-sys does not build RocksDB from source there. Linux
binaries are static (`musl.sh`; `MUSL_ARCH=aarch64` for arm) and run on any
distribution.

## Management console

Open `http://127.0.0.1:8003`. Configure at least one `auth.users` entry with a
password to bootstrap administration. The console supports:

- creating/deleting buckets; browsing, uploading, downloading, deleting objects;
- per-bucket CORS settings, with the configured console origin implicitly allowed;
- folder upload (button or drag-and-drop), recreating the tree under the current prefix;
- transparent multipart: files over 256 MiB are split into streamed parts (no
  browser or server buffering) up to 5 TiB, with progress and cancel-with-abort;
- per-bucket view of in-flight multipart uploads with one-click abort — covering
  both console uploads and sessions started by external S3 clients;
- multi-select and bulk delete (recursive for folders), plus prefix-scoped bulk
  delete with a typed prefix, warning dialog, and live progress;
- presigned share links (requires `auth.public_hostname`);
- runtime users and groups, password resets, access-key rotation, policy editing
  via both a rule builder and a JSON editor;
- IAM export, and **staged import**: a read-only preview shows per-family row
  counts and sample names (never secrets) before anything is written;
- bucket statistics and operator-triggered index rebuilds;
- a WebSocket task monitor for active/recent requests and jobs, with throughput
  and cancellation for safely cancellable work.

Share links carry the creator's current authority — deleting the user or
narrowing their policy revokes or narrows existing links.

## Object versioning

Buckets can keep every version of every object, with S3's semantics:

- **Enable** it in the bucket settings (or `PUT /{bucket}?versioning` with
  `<Status>Enabled</Status>`). The bucket must be on the current storage
  layout; the console says so and offers the upgrade. Objects that exist at
  that point are the key's `null` version.
- **Enabled:** every PUT, copy and multipart completion is a new version,
  reported as `x-amz-version-id`; the previous version stays. A DELETE
  inserts a delete marker (the key answers 404 with `x-amz-delete-marker`)
  and keeps every version. `GET`/`HEAD ?versionId=` read a version;
  `DELETE ?versionId=` removes exactly that version or marker, permanently —
  removing the current one makes the next newest current, removing a marker
  undeletes the key.
- **Suspended:** existing versions stay; a write becomes the key's `null`
  version and replaces only the previous `null` one; a delete replaces it
  with a `null` delete marker. A bucket that was enabled can be suspended,
  never returned to never-enabled.
- `GET /{bucket}?versions` lists everything, newest first per key, with
  `DeleteMarker` entries and `IsLatest`, paginated by `key-marker` and
  `version-id-marker`.

A version id is the version's stamp (`§2.2` of the replication spec) in a
zero-padded, sortable form, so it is the same on every site that holds the
version. On disk every version is a directory beside the others under its
key, so an index rebuild recovers versions and delete markers from the tree
alone. The console's object details panel lists a key's versions with a
download and a permanent delete for each.

**Versions and replication.** A version is immutable and named by its
stamp on every site, so two versioning-enabled buckets hold the same set
of versions: history is a union, and last-writer-wins only decides which
version is current. A link into a versioning-enabled bucket applies every
event (nothing is collapsed within a batch), fetches each state by its
version id, keeps a state older than the current one as a noncurrent
version instead of refusing it, and forwards version removals
(`DeleteObject ?versionId`, logged as `remove_version`) so a removed
version, or a removed delete marker, goes on every site and cannot come
back from a late copy. A link requires the same versioning setting on both
buckets, so that both replicate the same thing: it is refused at creation
otherwise, and a running link stops at once when a setting changes on one
side (the console shows the reason) and continues from its position once
both match again. Not yet: lifecycle expiry of noncurrent versions.

## Changelog, webhooks and replication

The wire protocol is specified in [docs/spec-replication.md](docs/spec-replication.md)
(changelog reads, events, stamps, replicated writes, peer checks) and
[docs/spec-webhooks.md](docs/spec-webhooks.md) (webhook requests and
batches); `src/server/spec_tests.rs` checks the server against both. What
follows is the guide.

Every bucket keeps a **changelog**: each committed PUT, copy, multipart
completion and delete is recorded, in the same atomic write as the change
itself, with a per-bucket sequence number (`seq`). Sequence numbers increase
strictly but are not contiguous. Entries are kept for
`changelog.retention_days` (14 by default).

There are two ways to consume it:

- **Read it** (pull). Any credential granted `s3:GetBucketChangelog` can read
  a bucket's changelog over the S3 API, and so can a replication token. No
  setup is needed on the server. The reader keeps its own position.
- **Webhooks** (push). An admin configures them in the console (**Webhooks &
  Replication**). The server tracks each webhook's position and POSTs batches
  to it.

Both can filter by key prefix, key suffix and operation.

### Batch format

Webhooks receive this as the POST body. Reads return the same `events` (see
below).

```json
{
  "subscription": "wh_3f2a…", "bucket": "photos",
  "phase": "live", "from_seq": 101, "to_seq": 250,
  "snapshot_begin": false, "snapshot_end": false,
  "events": [
    {"seq": 117, "op": "put", "key": "2026/a.jpg", "etag": "9b2c…", "size": 482113,
     "last_modified_ms": 1791800000123, "version": "V1A9F3K2_7Q1M",
     "origin": "client", "ts_ms": 1791800000130,
     "parts": {"multipart": true, "count": 3, "size": 8388608, "last": 1048576}}
  ]
}
```

- `version` identifies the exact object state. It changes on every write, even
  when the ETag does not.
  - GET and HEAD return it as `x-rs3-version`.
  - Sending `x-rs3-if-version: <version>` on a GET makes it fail with `412` if
    the object has changed since. A replicator can therefore fetch exactly the
    state an event names.
- Delete events also carry `removed_stamp`, the stamp of the version they
  removed.
- `stamp` is the version stamp `{"ms", "c", "site"}`: when the state was
  written (epoch ms), a counter for writes within the same millisecond
  (almost always 0), and the id of the site that wrote it.
  - For a delete, it is the delete's own stamp.
  - Stamps compare by `ms`, then `c`, then `site`, identically on every site,
    so two sites holding different states of a key agree which one is newer.
  - A site's stamps only ever increase, including across restarts and backwards
    clock steps, and a local write always outranks any stamp the site has seen.
  - GET and HEAD return it as `x-rs3-stamp: <ms>.<c>.<site>`.
  - Objects written before stamps existed report `<last-modified>.0.<this
    site>`.
- `parts` is how the object was uploaded. Upload the same bytes the same way
  and the other side computes the identical ETag:
  - `{"count": 1}`: a single PUT (ETag = MD5 of the content).
  - `"multipart": true`: a multipart upload with `count` parts. Every part is
    `size` except the `last`, or the parts are listed in `sizes` when they are
    irregular. Even a one-part multipart upload differs from a single PUT (its
    ETag ends in `-1`).
  - Absent for objects stored before layouts were recorded; use
    GetObjectAttributes for those.
- `origin` is `client` (a client of that site wrote it), `replication`
  (replication applied it there; the stamp's `site` says where it was made)
  or `repair`. A `repair` delete means the health scan removed an index row
  whose data was lost. It does not mean a client deleted the object.
- A snapshot arrives as `phase: "snapshot"` batches of `op: "exists"` entries,
  which have no `seq`:
  - The first batch has `snapshot_begin`; the last has `snapshot_end`.
  - Live changes then continue after `snapshot_through`.
  - A receiver that mirrors the bucket should delete any key it did not see
    between `snapshot_begin` and `snapshot_end`.
- `[from_seq, to_seq]` is the range a batch covers. Changes that a filter
  excludes still advance the subscription's position, so consecutive batches
  can leave gaps between them.
- Delivery is **at least once**, in order. Receivers should skip any event whose
  `seq` is at or below the last one they applied.

### Webhooks

Each webhook delivers batches one at a time, and every request is signed:

- `X-RS3-Signature: t=<unix>,v1=<hex HMAC-SHA256(secret, "<t>.<body>")>`
- `X-RS3-Delivery`, `X-RS3-Subscription`
- any custom headers configured on the webhook. Header values are stored as
  secrets and masked in the console.

The signing secret is shown once, when the webhook is created and each time it
is rotated.

Delivery and failure handling:

- A commit wakes its bucket's webhooks. One that is up to date delivers
  immediately. One that is muted or busy picks the change up later, from its
  backlog.
- **Any** unsuccessful delivery (a non-2xx response, a timeout, or a connection
  error) mutes the webhook for `mute_secs`. A longer `Retry-After` from the
  receiver takes precedence. The checker tick resumes the webhook afterwards,
  and it drains the backlog in back-to-back batches.
- Webhook URLs must be `https://` and must not resolve to internal addresses.
  Both rules can be relaxed in the config. Redirects are not followed.

### Reading the changelog

Reads are **stateless**: the server stores nothing per reader. A reader asks
for the changes after the `seq` it has applied and saves the returned `next`
itself. Saving it next to the data it applied is best, so that if that data is
rolled back, the position rolls back with it. Re-reading is always safe.

With SigV4 credentials that have `s3:GetBucketChangelog` (read-only; the admin
group's `s3:*` includes it):

```text
GET /{bucket}?x-rs3-changelog&after=<seq>[&limit=N][&min=N][&wait=secs|&wait_ms=N][&prefix=][&suffix=][&ops=put,delete][&log_id=]
GET /{bucket}?x-rs3-changelog-snapshot[&after=<key>][&limit=N][&prefix=][&suffix=]
```

- **Changes:** returns `{bucket, log_id, head, trimmed_through, next, more,
  events}`. `next` moves past filtered-out changes too.
- **Batching, Kafka-style:** a response holds up to `limit` changes (default
  500, max 10,000), like `max.poll.records`. The request waits until at least
  `min` matching changes are available (default 1, like `fetch.min.bytes`) or
  until the wait passes (`wait` in seconds or `wait_ms`; default 20s, max 60s,
  like `fetch.max.wait.ms`), then returns what it has. A full page returns
  immediately. Pass `wait=0` for an immediate answer.
- **Snapshot listing:** pages through the current objects as `op: "exists"`
  entries. Save the **first** page's `head`, page through to the end
  (`next_after` is null), then read changes with `after=<that head>`. That
  replays anything that changed during the listing.
- **Errors:**
  - `410 Gone`: `after` is older than what retention kept. The response names
    the oldest available position. Re-snapshot, or accept the loss and
    continue from there.
  - `409 Conflict`: the position doesn't belong to this log, either because it
    is beyond the head, or because `log_id` doesn't match after a rebuild
    replaced the log.

Instead of `after`, pass `since=<epoch ms>` to start at the first change
committed at or after that time; `next` is a seq as usual. Commit times never
decrease in seq order, so the server finds the position with a binary search.
If changes from that time may already have been removed by retention, the
response is `410`. Responses also carry the server's `site_id`.

**Metadata updates keep the bytes.** A CopyObject onto the same key (S3's way
of changing metadata) doesn't copy any data. The new version hardlinks the
existing part files, so the update costs the same regardless of object size and
keeps the part layout and the ETag, including a multipart `-N` ETag. If the
filesystem can't hardlink, it falls back to a byte copy. A copy to a different
key is still a full copy, stored as a single part.

**Replicated writes.** A PUT, CompleteMultipartUpload, CopyObject or DELETE
that sends `x-rs3-stamp: <ms>.<c>.<site>` keeps that stamp instead of getting a
new local one. For a write, `Last-Modified` becomes the stamp's time. This is
how a replica keeps the origin's stamp. A CreateMultipartUpload sent with
`x-rs3-replicate: true` (also needing `s3:ReplicateWrite`) starts a replicated
upload, which a read-only bucket accepts; the stamp the object keeps is the
one sent with the completion.

- **Last-writer-wins, with version-scoped deletes.** Every state of a key has a
  rank, computed identically on every site.
  - A live object ranks by its stamp.
  - A delete ranks by the stamp of the version it **removed**, just above that
    version. It does not rank by when the delete happened.
  - A stamped change applies only if it outranks the key's current state.
    Otherwise the server answers `412` with the current stamp in
    `x-rs3-stamp-current`, and nothing is written or logged. Replays and echoes
    are therefore no-ops, and every site keeps the same winner whatever order
    changes arrive in.
  - So a delete only removes what its author saw. A version written
    concurrently on another site, even slightly earlier in wall-clock time,
    survives a delete of an older version.
  - A replicated DELETE sends the removed version's stamp in
    `x-rs3-removed-stamp`. Without it, the delete's own stamp is used.
- **Tombstones.** A delete leaves a tombstone holding its stamp, kept for
  `changelog.tombstone_days` (14). It stops an older write from bringing the
  object back. A stamped delete of a key that doesn't exist still records one.
  - HEAD or GET of a deleted key answers `404` with `x-rs3-stamp` and
    `x-rs3-deleted: true`.
  - `?x-rs3-changelog-snapshot&deleted=1` lists the tombstones.
  - A site offline for longer than the tombstone retention must re-snapshot
    rather than resume.
- **Permission.** Setting a stamp decides conflicts, so it requires the custom
  IAM action `s3:ReplicateWrite` on the object, in addition to the write or
  delete permission. Stamps more than 10 minutes in the future are refused.
  Like every `x-amz-*` header, an `x-rs3-*` header must be covered by the
  request's signature (`403` otherwise): a stamp decides which version wins,
  so it is never taken from an unsigned header.
- Local (unstamped) writes and deletes are never conditional, and always
  outrank every state the site has seen.

**Replication tokens** (created by an admin in the console) give the same reads
without S3 credentials. They are scoped to one bucket and the token's filter:

```text
GET /_rs3/changelog?after=<seq>[&limit=N][&min=N][&wait=secs|&wait_ms=N][&log_id=]
GET /_rs3/changelog/snapshot[?after=<key>][&limit=N]
```

Send `Authorization: Bearer rs3cl_…` on the S3 port, using path-style
addressing. A bucket name can't start with `_`, so `/_rs3/` never clashes with
a bucket. On a virtual-hosted request (`<bucket>.<public_hostname>/_rs3/…`) the
path is an ordinary object key and goes to S3. Pausing a token in the console
disables it (`403`).

### Replication links

A replication link replicates a whole bucket with a bucket on another rusts3
site (the peer). It is configured in the console (**Webhooks & Replication →
Replication links**) on one site, and that site's server runs it: it is the
actor for both directions. There are no prefix filters on links (webhooks can
filter by prefix).

- **Mirror** (this ⟵ peer: one way, remote wins): the local bucket becomes an
  exact copy of the peer's. Its initial snapshot also deletes local objects the
  peer doesn't have. The bucket is made **read-only** so only replication
  writes it; that's what keeps "remote wins" true.
- **Merge** (this ⟷ peer: both ways, newer wins): the link pulls the peer's
  changes in and pushes the local bucket's changes out. Both buckets end up as
  the union of their objects, every key at its highest-stamped state; deletes
  win or lose by stamp too, via tombstones. Nothing is configured on the peer:
  its console lists the link under **Managed on other sites** (links seen
  reading its changelogs in the last 5 minutes).

**Site ids and names.** Each server has a site id, generated on its first
start (16 hex characters, kept in `<base_dir>/site.json`) and never changed:
every version stamp carries the id of the site that wrote it, and peers know a
site's changelog by it. The site name is a free label shown beside it
(generated as two words, changeable in the console or set by
`server.site_name`). A
copy of a data directory carries `site.json` along and is the same site;
links refuse to run between two servers reporting one id. To set up a new
site, start it from an empty data directory and let a link fill it.

**Two keys, both explicit.** A link carries everything it is allowed to do:

| | Key | Mirror needs | Merge needs |
|---|---|---|---|
| On this site | **local access key**: an access key of this site, named only (no secret: the server acts *as* it) | `s3:PutObject`, `s3:DeleteObject`, `s3:ReplicateWrite` | the same, plus `s3:GetObject`, `s3:GetBucketChangelog` |
| On the peer | **peer access key and secret** (SigV4) | `s3:GetObject`, `s3:GetBucketChangelog` | the same, plus `s3:PutObject`, `s3:DeleteObject`, `s3:ReplicateWrite` |

Each action is needed on the whole bucket (every key). `s3:ReplicateWrite` is
the permission to write with a change's original stamp: replicated writes keep
the stamp of the site that made them, which is what decides conflicts, so
setting it is a privilege of its own. Admin keys (`s3:*`) have everything.
In the console's policy editor, the rule builder's **Replication** access
level grants exactly these on a whole bucket (bucket `*` for every bucket),
and the JSON presets **Replicate all buckets** (`ReplicateAllBuckets`) and
**Replicate one bucket** (`ReplicateOneBucket`) do the same. Both cover
either key of either mode; `s3:AbortMultipartUpload` is included so a failed
pushed multipart copy can clean up.

- **Checked when the link is created** (and when its peer or keys change): the
  peer must answer, not be this server, have the bucket, and its key must have
  every permission above (the peer reports them for the calling key:
  `GET /{bucket}?x-rs3-access`); the local key must exist and have its
  permissions too. A merge into a read-only peer bucket is refused. So is a
  merge link for a pair the peer already serves with its own merge link.
- **Bound to two site ids.** A link records this site's id and the peer's
  when it is created (the peer's from the check; a link created without the
  check records it on first contact). If another site answers later (the
  endpoint now reaches a different or rebuilt server), the link stops with an
  error naming the change, and nothing moves in either direction: the push also confirms the peer's id (`x-rs3-site`, on
  object reads) before every write. **Accept site ids** (console, or action
  `accept_site_ids`) re-checks the peer, records the ids both sides report
  now, and starts again from a fresh snapshot.
- **Checked while it runs:** the local key's permissions every step (delete
  the key or narrow its policy and the link stops with an error saying what
  is missing); the peer enforces its key on every request.
- **Rules the server enforces:**
  - A mirror link needs a read-only target. The console makes the bucket
    read-only when it creates the link.
  - A mirror is its bucket's only link.
  - A mirrored bucket can't be switched back to read-write while its mirror
    link exists, even if the link is paused. Deleting the link leaves the bucket
    read-only; making it writable again is a separate, explicit step.
  - One link per bucket pair on a site. If both sites end up with a merge link
    for the same pair anyway (one was created while the other site was down),
    the one on the site with the higher site id stops with an error saying so,
    and the other keeps syncing both ways. Delete one of them.

How it works:

- The link first walks the peer's snapshot, then follows its changelog. A
  merge link's push does the same with the local bucket: it walks the local
  objects and tombstones, then follows the local changelog (read in-process,
  the same code that serves the changelog API), writing to the peer with
  stamped S3 requests.
- Every change is applied with its original stamp, under last-writer-wins.
  - A change whose stamp is not newer than the receiving side's state is
    skipped without fetching or sending anything (the push asks the peer with
    a HEAD first). Changes the push would send back to the site they came from
    are skipped too, which is how echoes stop.
  - Identical content with a newer stamp becomes a metadata-only update; no
    bytes move.
  - Object data is read pinned to the change's version (`x-rs3-if-version`),
    so changes the peer has since superseded are skipped.
- Multipart objects are re-uploaded with the peer's part layout, so ETags
  match.
- **Parallelism:** each batch is collapsed to the latest change per key, then
  applied up to `replication.parallelism` (8) at a time. Batches run one after
  another, and the link checkpoints only after every change in a batch has
  succeeded. One failure holds the checkpoint, and the batch is retried; the
  changes that already succeeded are cheap no-ops on replay. Concurrent applies
  are safe because the last-writer-wins check and the commit are atomic under
  each key's lock.
- Deletes caused by a health-scan repair (`origin: repair`) are never
  replicated. The other side's copy is kept.
- **A change that moved on vs. lost data.** If the object a change names was
  overwritten or deleted since, the change is skipped: a later change in the
  log carries the object's current state. If the index still names the
  object but its bytes are gone (a disk lost them), nothing is skipped: the
  link stops at that object and reports it (for example `corrupt object: row
  for photos/a.jpg references unreadable blob`) until it is resolved (restore
  from peer, or delete the key).
- **Restore from peer.** When the storage scan finds an object whose bytes are
  missing or corrupt in a bucket that a link pulls into, the recommended repair
  is **Restore from replication peer**. It fetches the object back with its
  original stamp, layout and metadata. Deleting the row is the last resort.
  - It only restores the lost version or a newer one.
  - If the peer has only an older version, the repair fails and explains why.
    **Restore older version from peer** accepts the rollback explicitly.

Operation:

- **Position:** each link's position (and a merge link's push position in the
  local changelog) is stored in the local bucket's own index, next to the data
  it describes, and is carried across index rebuilds.
- **Re-snapshot triggers:** a link re-snapshots on its own when the peer's
  changelog no longer has its position (`410`), when the peer's log was
  replaced, or when its checkpoint is older than the tombstone retention
  while it is behind (a caught-up link refreshes its checkpoint as it idles,
  so a quiet bucket is never re-walked for nothing).
- **Supervision:** a supervisor checks every `replication.check_interval_secs`
  (5s) and starts a worker for every enabled link that lacks one, for example
  after a crash or a restart. It replaces a worker that has been silent for
  `replication.worker_stale_secs` (600s), and never runs two workers for one
  link. A worker renews its lease while it makes progress (every chunk of a
  transfer) and while a request to the peer is pending (such a wait is
  bounded by the client's timeouts), so only a worker stuck where no timeout
  applies is ever replaced.
- **Safety:**
  - A link whose peer reports this server's own site id (a copied data
    directory) stops with an error.
  - A bucket that is the target of a link can't be deleted until the link is
    removed.

### Read-only buckets

An admin can mark a bucket **read-only** (console: bucket settings, or `PUT
/api/buckets/{name}/read-only {"read_only": true}`). A read-only bucket accepts
only replicated writes: writes carrying a version stamp, which come from
replication links (including the mirror sweep and restore from peer) or from
`s3:ReplicateWrite` clients sending `x-rs3-stamp`.

Every other write is refused with `403`: PUT, copy (including metadata
updates), multipart upload, DELETE, multi-delete, browser POST, and console
uploads and deletes. Uploads are refused before any bytes are sent. Reads are
unaffected. This is meant for replica buckets.

### Falling behind retention

A reader that falls behind retention gets `410 Gone` and decides for itself
what to do.

If a webhook's position falls behind what retention has kept, the events it
missed are gone, so it **pauses** instead of skipping them silently:

- **Resume** is refused with an explanation.
- The admin chooses one of: **resume from oldest retained**, **re-snapshot**,
  **skip to head**, or **set offset**.

The console warns when a webhook's oldest pending change is close to
expiring. A bucket cannot be deleted while it has subscriptions. An index
rebuild carries the changelog over, so positions stay valid.

## Health and metrics

Unauthenticated compatibility endpoints on the S3 port:

```text
GET /minio/health/live
GET /minio/health/ready
GET /minio/prometheus/metrics
GET /minio/v2/metrics/{cluster,node,bucket,resource}
```

The metrics endpoints currently expose a minimal `rusts3_up 1` gauge. Detailed
request/byte totals and rates are in the periodic logs and the console task view.

<details>
<summary><strong>Supported S3 operations (full table)</strong></summary>

### Buckets

| Method | Resource | Operation |
|---|---|---|
| `GET` | `/` | List buckets. |
| `PUT` / `HEAD` / `DELETE` | `/{bucket}` | Create, inspect, or delete an empty bucket. |
| `GET` | `/{bucket}` | ListObjects v1/v2 with prefix, delimiter, marker/continuation token, encoding, and pagination. |
| `GET` | `/{bucket}?location` | Get bucket location. |
| `GET` | `/{bucket}?uploads` | List multipart uploads. |
| `GET/PUT/DELETE` | `/{bucket}?cors` | Read, replace, or remove bucket CORS rules. |
| `GET` | `/{bucket}?versions` | ListObjectVersions: every version and delete marker, newest first per key, paginated by `key-marker` / `version-id-marker`. |
| `GET` / `PUT` | `/{bucket}?versioning` | Read or set the bucket's versioning state (`Enabled` / `Suspended`; enabling needs the current storage layout). |
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

Authentication supports header and query-string SigV4/SigV2. Presigned SigV4
URLs enforce AWS's seven-day maximum expiry. Browser POST policies validate
expiry and form conditions before accepting the object.

</details>

<details>
<summary><strong>Docker: config templating, storage permissions, health</strong></summary>

### Config templating

The image ships `config.docker.yaml` at `/etc/rusts3/config.yaml`, in which
every value is a placeholder:

```yaml
server:
  bind_port: {{RUSTS3_PORT:8002}}
  base_dir: "{{RUSTS3_DATA_DIR:/data}}"
```

`{{NAME:default}}` expands to `$NAME` when set and non-empty, else `default`. So
the image runs with no configuration, and any single field can be overridden
with `-e` — there is no separate list of environment bindings to keep in step
with the schema, because the config file *is* the list.

That short form is one *provider* — `env` — with its name implicit. The general
shape is `{{provider<sep>param<sep>param…}}`, where the character immediately
after the provider name is the separator, so a value containing colons can
choose something else:

```text
{{RUSTS3_PORT:8002}}            shorthand: env lookup with a default
{{env:RUSTS3_PORT:8002}}        the same thing, said explicitly
{{env|DSN|postgres://h:5432}}   separator '|', so the colons are data
{{upper:env:RUSTS3_REGION}}     transforms compose with lookups
{{lower$env$RUSTS3_ADMIN_USER}} any single character works as the separator
```

The first segment is a provider only if it *both* matches `[A-Za-z_]+` (letters
and underscores, no digits) and names a registered provider. Failing either, the
placeholder falls back to the `env` shorthand — so `{{a:b}}` is `$a` defaulting
to `b` unless `a` is registered, and `RUSTS3_PORT` cannot be read as a call at
all, because the digit disqualifies it first. Everything after the first
separator is the last parameter, so in `{{a:b:c:d}}` the default is `b:c:d`.

Providers today are `env`, `upper`, and `lower`. Extra parameters are accepted
and ignored, so the calling convention can grow without breaking existing files.
Adding a provider means implementing `Resolver` and registering it; a resolver
receives the whole call including the registry, so it can resolve nested calls
the way `upper` does. If you have an environment variable named after a
provider, disambiguate explicitly: `{{env:ENV}}` reads `$ENV`.

`{{NAME}}` without a default is **required**: if unset, the server refuses to
start and names the variable. Delete the defaults from the `auth` entries in
`config.docker.yaml` if you would rather the container fail than come up with
known credentials.

Substitution is textual and happens before YAML parsing, so quote a placeholder
whose value might contain spaces or YAML punctuation. To go further than
individual overrides, mount your own file over `/etc/rusts3/config.yaml`.

### Unfilled slots

Templating cannot express "if this, then not that", so a container config is
*flat*: it lists every slot a deployment might use and lets unused ones expand
to nothing. An unfilled slot is treated as an absent one.

```yaml
auth:
  users:
    - user: "{{RUSTS3_ADMIN_USER:admin}}"
      password: "{{RUSTS3_ADMIN_PASSWORD:changeme}}"
      api_keys:
        - ak: "{{RUSTS3_ACCESS_KEY:rusts3admin}}"
          secret: "{{RUSTS3_SECRET_KEY:rusts3admin}}"
        - ak: "{{RUSTS3_ACCESS_KEY_2:}}"      # unset → the whole entry is dropped
          secret: "{{RUSTS3_SECRET_KEY_2:}}"
    - user: "{{RUSTS3_USER_2:}}"              # unset → this user does not exist
      password: "{{RUSTS3_PASSWORD_2:}}"
```

**Empty is the marker**, rather than a reserved word like `none`. An unset
variable already produces empty, so there is no convention to remember and no
escaping rule for the day someone genuinely wants a value of `none`; and empty
can never be a valid username, access key, or secret, so nothing legitimate is
discarded by accident. The rule applies to optional scalars too — an empty
`public_hostname` reads as "not configured". Anything dropped is named on
stderr at startup:

```text
config: ignoring 3 unfilled entries (an unfilled api_key of user "admin",
an unfilled auth.users entry, an unfilled auth.users entry)
```

One case is a safety fix rather than a convenience. A built-in user's password
is compared against the candidate, so a user left with an *empty* password would
authenticate against the empty string. An unfilled password is therefore turned
into no password at all, so that user simply cannot log in. A credential missing
either half is discarded for the same reason.

### Storage permissions

`/data` holds everything durable: buckets, the IAM database, scan history, logs.
A named volume is the simplest thing that works. A **host directory** needs one
extra thought, because the image runs as a non-root user (uid 10001) — either
give that uid ownership:

```bash
sudo chown -R 10001:10001 /srv/rusts3
docker run -d -v /srv/rusts3:/data ... rusts3:latest
```

or run the container as a user that already owns it:

```bash
docker run -d -v /srv/rusts3:/data --user "$(id -u):$(id -g)" ... rusts3:latest
```

Getting this wrong is not mysterious — the server names the directory, the uid
it was running as, and both remedies.

### Health and shutdown

`HEALTHCHECK` runs `rusts3 healthcheck`, which reads the same config the server
did (so it probes whatever port is configured) and requests the unauthenticated
`/minio/health/live`. It needs no `curl` in the image. Podman ignores
`HEALTHCHECK` unless the image is built with `--format docker`.

The server handles `SIGTERM` as well as `SIGINT`, which is what makes
`docker stop` clean rather than a ten-second wait followed by `SIGKILL` — in a
container the entrypoint is PID 1, and PID 1 gets no default signal handling
from the kernel. In-flight connections are given a few seconds to drain and then
the process exits regardless, so a client holding a keep-alive socket cannot
delay a stop.

### Testing an image

`./docker-test.sh` starts the image and drives it with the AWS CLI: bucket
lifecycle, single and multipart round-trips compared byte for byte, ranged
reads, server-side copy, prefix and delimiter listing, presigned download, a
restart to prove the volume is durable, and deletion. Works with docker or
podman.

</details>

<details>
<summary><strong>Storage and recovery model</strong></summary>

Each bucket has an authoritative RocksDB index. Object data lives in an
immutable, self-describing blob directory, and the index records that
directory's path. A mutation follows this protocol:

1. Stream a complete blob into staging.
2. Commit a publish intent.
3. Atomically rename the blob into the live tree.
4. Atomically switch the index row and record retirement of any old blob.
5. Move the old blob to trash and clear the retirement intent.

After a crash, startup drains the small intent table instead of scanning every
object. Reads and listings use the RocksDB index as the source of truth.
Per-key locking, monotonic modification timestamps, and immutable snapshots keep
concurrent overwrites/deletes consistent with in-flight downloads.

The current layout uses a single 16-bit fanout directory:

```text
<base_dir>/
  .rusts3.lock
  admin.rocksdb/
  buckets/<bucket>/
    bucket.json
    index.rocksdb/
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
so normal S3 retry behavior applies. An operator can trigger the same tracked
job from the console or with:

```bash
curl -X POST 'http://127.0.0.1:8002/my-bucket?rebuildIndex'
```

**Graceful shutdown.** `SIGINT` (Ctrl-C) stops accepting new connections, drains
the S3 server, cancels background work through cooperative cancellation,
releases the data-root lock, and exits cleanly.

</details>
