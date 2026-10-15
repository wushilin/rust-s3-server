# rusts3 replication protocol — specification v1

This document specifies the wire protocol two rusts3 sites (or any
compatible implementation) use to replicate a bucket: how changes are
read, how object state is identified and ordered, how a replicated write
is made, and how a link checks a peer. Everything here is covered by the
conformance tests in `src/server/spec_tests.rs`; a change to this document
is a change to those tests.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

The webhook delivery format shares the event model of §3 and is specified
in [spec-webhooks.md](spec-webhooks.md).

## 1. Transport, authentication and signing

- All endpoints are served on the S3 port. Bucket subresources use S3
  path-style or virtual-hosted addressing; `/_rs3/…` paths are path-style
  only (a bucket name can never start with `_`).
- Bucket subresources are authenticated with AWS SigV4 and authorized by
  IAM like any S3 request. The replication-token endpoints (§4.3) take
  `Authorization: Bearer rs3cl_…` instead.
- Every request header named `x-rs3-*` MUST be covered by the request's
  signature (listed in `SignedHeaders`), exactly like `x-amz-*`. A request
  carrying an unsigned `x-rs3-*` header is refused with `403
  SignatureDoesNotMatch`, whatever the header, and nothing is written.
  Rationale: `x-rs3-stamp` decides which version of an object wins.
- Response headers and JSON bodies MAY gain new fields in later versions.
  A reader MUST ignore fields it does not know and MUST NOT depend on
  field order.

## 2. Identity: sites, stamps, versions, ranks

### 2.1 Site id

Each server has a **site id**: 1–64 characters, generated once at first
start, never changed. Every stamp carries the id of the site that made the
state. Two servers reporting the same site id are the same site (a copied
data directory); a link MUST refuse to run between them.

### 2.2 Stamp

A **stamp** identifies one state of a key and orders it against every
other state of that key on every site.

- JSON: `{"ms": <i64>, "c": <u32>, "site": "<site id>"}`.
- Header form: `<ms>.<c>.<site>`, e.g. `1791800000123.0.bd7e41df94febe9a`.
- Order: by `ms`, then `c`, then `site` (bytewise), identical everywhere.
- A site's stamps are strictly increasing, including across restarts and
  backwards clock steps, and a local write always outranks every stamp the
  site has observed (received through replication).
- A stamp more than 10 minutes ahead of the receiving server's clock is
  refused (`400 InvalidArgument`).
- Objects written before stamps existed report the fallback stamp
  `<last_modified_ms>.0.<site of the server holding it>`.

### 2.3 Version

A **version** names an exact stored state: an opaque string that changes
on every publish of the key, even when the ETag does not. It is reported
as `x-rs3-version` on GET/HEAD and as `version` in events, and is what a
replicator pins a read to (§5.1). A replicator MUST treat it as opaque.
(On the current storage layout it is the stamp in its zero-padded directory
form, `<ms:019>.<c:010>.<site>`, and so identical on every site holding the
version; a site on an older layout reports a site-local name.)

A bucket with S3 versioning enabled keeps the displaced states as
noncurrent versions. Replication is unaffected: the changelog describes the
key's *current* state, a replicated write into a versioned bucket is kept
as a version of that bucket under the origin's stamp, and a delete leaves a
delete marker there. A version-scoped delete (`DeleteObject?versionId`) of
a noncurrent version is logged as a `delete` of that version and ranks
below the current state, so peers ignore it.

### 2.4 Rank: last-writer-wins with version-scoped deletes

Every state of a key has a **rank**; the state with the highest rank wins
on every site, whatever order changes arrive in.

- A live object ranks `(stamp, live)`.
- A delete ranks `(removed, deleted)`, where `removed` is the stamp of the
  version the delete removed. A delete ranks just above that version and
  does not rank by when the delete happened.
- Ranks compare by stamp first; for an equal stamp, deleted ranks above
  live.
- A change **applies** only if its rank is strictly above the key's
  current state (live object, or tombstone). An equal or lower rank is a
  no-op refused with `412` (§5.4). Replays and echoes are therefore
  harmless.
- Consequence: a delete only removes what its author saw. A version
  written concurrently on another site survives a delete of an older
  version, even if it is slightly earlier in wall-clock time.

### 2.5 Tombstones

A delete leaves a **tombstone** holding the delete's stamp and the removed
stamp, kept for the tombstone retention (14 days by default). A delete of
a key that is absent still records one when it names a version. A site
offline longer than the retention MUST re-snapshot rather than resume.
On the current storage layout the tombstone is also a delete-marker
directory beside the key's versions, so an index rebuilt from the tree
alone still knows the delete.

## 3. The event

One JSON object describes one change or one snapshot entry. The same
object is delivered by the changelog reads (§4), the replication-token
reads (§4.3) and webhooks.

```json
{"seq": 117, "op": "put", "key": "2026/a.jpg", "etag": "9b2c…", "size": 482113,
 "last_modified_ms": 1791800000123, "version": "V1A9F3K2_7Q1M",
 "origin": "client", "ts_ms": 1791800000130,
 "parts": {"multipart": true, "count": 3, "size": 8388608, "last": 1048576},
 "stamp": {"ms": 1791800000123, "c": 0, "site": "bd7e41df94febe9a"}}
```

| Field | Type | Present | Meaning |
|---|---|---|---|
| `seq` | u64 | live events only | Position in the bucket's changelog. Strictly increasing, not contiguous. |
| `op` | string | always | `put`, `delete`, `remove_version`, `tags` (live); `exists`, `deleted` (snapshot). A reader MUST NOT guess the meaning of an op it does not know. |
| `key` | string | always | The object key. |
| `etag` | string | always | `put`/`exists`: the state's ETag (bare, no quotes). `delete`/`deleted`: the removed version's; empty when the delete removed nothing here (§3.1). |
| `size` | u64 | always | Bytes of the state (`0` with an empty `etag`). |
| `last_modified_ms` | i64 | always | The state's Last-Modified (epoch ms). For a delete: the removed version's; the delete's stamp time when it removed nothing. |
| `version` | string | always | The state's version (§2.3); the removed version's for a delete; empty when the delete removed nothing. |
| `origin` | string | live events | `client`: a client of that site wrote it. `replication`: replication applied it there (its `stamp.site` names where it was made). `repair`: the site's health scan changed local state. |
| `ts_ms` | i64 | live events | When the change committed (that server's clock). Informational; order by `seq`. |
| `parts` | object | when known | Upload layout to reproduce for an identical ETag (§3.2). Absent for objects indexed before layouts were recorded. |
| `stamp` | stamp | always (fallback for old states) | `put`/`exists`: the state's stamp. `delete`/`deleted`: the delete's own stamp. |
| `removed_stamp` | stamp | deletes | The stamp of the version the delete removed: what it ranks by. A reader MUST use `stamp` when it is absent. |
| `versioned` | bool | when known | `true`: the state is a real version of a versioning-enabled bucket (its stamp is its version id). `false`: the key's `null` version. Absent from events written before versioning existed; a reader treats that as unknown (§5.4). |

`tags` is logged when a version's tags are replaced
(`PutObjectTagging` / `DeleteObjectTagging`): `stamp` and `version` name
the version, `tags` (an object of key/value strings) is its new tag set,
`etag`, `size` and `parts` describe the version, which did not change. A
reader applies it by identity: that version's tags become exactly the
set (§5.4); a version the reader does not hold gets its tags when it
fetches the version (§5.1, `x-rs3-tags`).

`remove_version` is logged by `DeleteObject?versionId`: the version named
by `stamp` (and `version`, its directory form) was permanently removed —
a noncurrent version, the current one (the next newest became current),
or a delete marker (the key is undeleted). `etag`, `size` and `parts`
describe what was removed. It is applied by identity (§5.4), never by
rank.

### 3.1 Deletes that removed nothing

A replicated delete arriving at a site that never had the version (or
that had already deleted it) still records a tombstone and is logged as a
`delete` with empty `etag`, `size: 0`, empty `version`, and its stamps.
Consumers MUST accept such events; replicators apply them like any delete
(they rank by `removed_stamp`).

### 3.2 Part layout

`parts` says how the bytes were uploaded, which fixes the ETag:

- `{"count": 1}`: one PUT; ETag = MD5 of the content.
- `{"multipart": true, "count": N, "size": S, "last": L}`: N parts, each
  S bytes except the last (L). ETag = MD5 of the part MD5s, `-N`.
- `{"multipart": true, "count": N, "sizes": [..]}`: irregular part sizes.
- Even a one-part multipart upload differs from a single PUT (`-1`).

### 3.3 Repair deletes

A `delete` with `origin: "repair"` means the site lost the object's data
and removed its index row. It is not a client's delete: a replicator MUST
NOT apply it to the other side (which keeps its copy, and can restore the
lost one).

## 4. Reading the changelog

Reads are stateless: the server stores nothing per reader. The reader
keeps the `next` it last received.

### 4.1 Changes

```
GET /{bucket}?x-rs3-changelog[&after=<seq>|&since=<epoch ms>][&limit=N][&min=N]
    [&wait=<secs>|&wait_ms=<ms>][&prefix=][&suffix=][&ops=put,delete][&log_id=<id>]
```

Permission: `s3:GetBucketChangelog` on the bucket.

Parameters:

| Name | Default | Meaning |
|---|---|---|
| `after` | 0 | Return changes with `seq > after`. |
| `since` | — | Instead of `after`: start at the first change committed at or after this time. Passing both is `400`. |
| `limit` | 500 | Max events per response, clamped to 1…10000. |
| `min` | 1 | Hold the request until at least this many matching changes are available, clamped to 1…`limit`. |
| `wait` / `wait_ms` | 20 s | Long-poll bound, max 60 s. `wait=0` answers at once. Passing both is `400`. |
| `prefix`, `suffix` | empty | Key filter. |
| `ops` | both | Comma list of `put`, `delete`; anything else is `400`. |
| `log_id` | — | Refuse (`409`) unless the bucket's log is this one. |

Response `200`, `application/json`:

```json
{"bucket": "photos", "site_id": "bd7e41df94febe9a", "site_name": "Demo site A",
 "log_id": "…", "head": 250, "trimmed_through": 0, "next": 250, "more": false,
 "events": [ … §3 … ]}
```

| Field | Meaning |
|---|---|
| `site_id`, `site_name` | The answering site (§2.1). |
| `log_id` | The identity of this bucket's changelog. Positions are only meaningful within one `log_id`. |
| `head` | The newest readable seq now. |
| `trimmed_through` | Changes with `seq <=` this were removed by retention. |
| `next` | Pass as `after` next time. It moves past filtered-out changes too. |
| `more` | More changes are available after `next` right now. |

A request waits until `min` matching changes are available, a full page
is ready (`more`), or the wait passes; an empty `events` with `next ==
after` is a normal answer.

Errors (JSON `{"error": "<message>", …}`):

| Status | When | Extra fields |
|---|---|---|
| `400` | bad parameter | — |
| `404` | no such bucket | — |
| `409` | `after` is beyond `head`, or `log_id` does not match: the position does not belong to this log | `log_id`, `head` |
| `410` | `after` (or `since`) is older than what retention kept | `trimmed_through`, `head` |
| `503` | the bucket's index is being rebuilt | — |

### 4.2 Snapshot

```
GET /{bucket}?x-rs3-changelog-snapshot[&after=<key>][&limit=N][&prefix=][&suffix=][&deleted=1]
GET /{bucket}?x-rs3-changelog-snapshot&versions=1[&after=<key>&after_version=<version>][&limit=N][&prefix=]
```

Pages through the current objects in key order as `exists` entries (no
`seq`), or with `deleted=1` through the tombstones as `deleted` entries.
With `versions=1` it pages through **every version of every key** in key
order, newest first per key: live versions as `exists`, delete markers as
`deleted`, each with its `stamp`, `versioned` bit and (markers)
`removed_stamp`; a key's versions may be split across pages.

```json
{"bucket": "photos", "site_id": "…", "site_name": "…", "log_id": "…", "head": 250,
 "entries": [ … ], "next_after": "2026/zz.jpg", "next_after_version": "0000001791…"}
```

- `next_after`: pass as `after` for the next page; `null` when the listing
  is done. With `versions=1`, `next_after_version` comes with it and is
  passed as `after_version`.
- `head`: the changelog head when the page was read. A reader saves the
  **first** page's `head`, walks to the end, then reads changes with
  `after=<that head>`; that replays anything that changed during the walk.
- A reader with `ops=delete` only gets an empty listing.

### 4.3 Replication tokens

The same reads without S3 credentials, scoped to one bucket and the
token's filter (configured by an admin):

```
GET /_rs3/changelog?…            (as 4.1, minus prefix/suffix/ops)
GET /_rs3/changelog/snapshot?…   (as 4.2)
Authorization: Bearer rs3cl_…
```

A missing or unknown token is `401`; a paused token `403`. Responses and
errors are as in 4.1 and 4.2.

### 4.4 Naming the reader

A replication link SHOULD send `x-rs3-replicator` on every changelog read
so the peer's console can list it, and so the peer can report an existing
merge link for the pair (§6.2). Value: named fields (§7):

`site=<site id>;site_name=<name>;mode=<mirror|merge>;bucket=<reader's bucket>;link=<link id>;name=<link name>`

## 5. Objects

### 5.1 Reading a state

GET and HEAD of an object return, besides the S3 headers:

| Header | Value |
|---|---|
| `x-rs3-version` | the state's version (§2.3) |
| `x-rs3-stamp` | the state's stamp, header form (§2.2) |
| `x-rs3-site` | the answering site's id |
| `x-rs3-tags` | the state's object tags as named fields (§7), when it has any (S3's `x-amz-tagging-count` gives the count) |

A replicator writes the tags it reads here with the copy
(`x-amz-tagging` on PUT, CreateMultipartUpload or copy), so a replicated
state carries its tags from the start.

Request header `x-rs3-if-version: <version>` makes the read fail with
`412 PreconditionFailed` unless the object is exactly that state, so a
replicator never tags newer bytes with an older version. (`If-Match` on
the ETag is not enough: a metadata update keeps the ETag.) Request header
`x-rs3-if-stamp: <stamp>` pins the same state by its stamp; when both are
sent the read succeeds if either matches. A replicator SHOULD send both:
a storage layout upgrade on the serving site renames every version (§2.3)
without logging a change, while stamps never change, so a pin by version
alone would skip every object whose event predates the upgrade.

A deleted key (one with a tombstone) answers `404 NoSuchKey` with
`x-rs3-site`, `x-rs3-stamp` (the delete's own stamp), `x-rs3-deleted:
true` and `x-rs3-removed-stamp` (what it ranks by). A key that never
existed answers `404` with `x-rs3-site` only.

### 5.2 Replicated writes

A PUT, CopyObject, CompleteMultipartUpload or DELETE carrying
`x-rs3-stamp: <ms>.<c>.<site>` is a **replicated write**: the new state
keeps that stamp instead of getting a local one, `Last-Modified` becomes
the stamp's time, and the write is adjudicated by rank (§2.4). It
requires the IAM action `s3:ReplicateWrite` on the object in addition to
the write or delete action; without it the request is `403
AccessDenied`. A malformed stamp is `400 InvalidArgument`.

- `x-rs3-versioned: true|false` beside the stamp says whether the state
  is a real version on the site it comes from; a versioning-enabled bucket
  keeps it so (a `null` version stays `null`). Without the stamp it is
  `400 InvalidArgument`; another value is too.
- PUT: the body and metadata as usual (`Content-Type`, `x-amz-meta-*`,
  `x-amz-storage-class`, `Content-Encoding`, `Content-Language`). The
  stamp is kept; the ETag is computed from the bytes.
- Multipart: CreateMultipartUpload carries `x-rs3-replicate: true` (no
  state exists yet for a stamp to name; the header marks the upload as
  replication's, which a read-only bucket requires, and needs
  `s3:ReplicateWrite`); UploadPart carries nothing; the stamp the object
  keeps goes on CompleteMultipartUpload with `x-rs3-stamp`. A replicator
  SHOULD always send `x-rs3-replicate` on the create.
- CopyObject onto the same key with `x-amz-metadata-directive: REPLACE`
  and a stamp: a metadata-only update that keeps the bytes, the layout
  and the ETag, and takes the stamp.
- DELETE with `x-rs3-stamp` and `x-rs3-removed-stamp: <stamp>` (the
  removed version's stamp, what the delete ranks by). Without
  `x-rs3-removed-stamp`, the delete ranks by its own stamp.
  `x-rs3-removed-stamp` without `x-rs3-stamp` is `400`.
- The resulting event has `origin: "replication"` and the given `stamp`.

### 5.3 Read-only buckets

A read-only bucket refuses every unstamped write with `403 AccessDenied`
before any body is read (PUT, copy, multipart create, DELETE, bulk delete)
and accepts replicated writes.

### 5.4 Stale writes

A replicated write whose rank is not above the key's current state
changes nothing and answers `412 PreconditionFailed` with
`x-rs3-stamp-current: <stamp>` (the current state's stamp; suffixed
` (deleted)` when that state is a tombstone). A replicator treats it as
"already superseded" and moves on.

Unstamped (client) writes are never stale: they always apply.

## 6. Checking a peer

### 6.1 Access report

```
GET /{bucket}?x-rs3-access
```

Any identity may call it (no specific action). Response `200`:

```json
{"site_id": "…", "site_name": "…", "bucket": "photos", "read_only": false,
 "actions": {"s3:GetObject": true, "s3:GetBucketChangelog": true,
             "s3:PutObject": true, "s3:DeleteObject": true, "s3:ReplicateWrite": true},
 "merge_back": null}
```

`actions` reports, for exactly these five actions, whether the calling
key may do them on the bucket and every key in it. A mirror link needs
the first two on its peer; a merge link needs all five.

### 6.2 Merge back

When a request carries `x-rs3-replicator` (§4.4) and the answering site
runs a merge link pairing the same bucket with the caller's site, the
server reports it:

- on the access report: `"merge_back": "link=<id>;name=<name>"`,
- on changelog reads: response header `x-rs3-merge-back: link=<id>;name=<name>`.

A link seeing a merge-back for its pair MUST stand down if its site id is
the higher of the two (the pair is already served); creation of a second
merge link for a served pair MUST be refused.

### 5.4 Versioning-enabled buckets: history by identity

A version is immutable and named by its stamp on every site, so two
versioning-enabled buckets hold the **same set of versions**: history is
a union, and rank (§2.4) only decides which member is current — every
site computes that the same way. On a bucket with versioning enabled:

- A replicated write or delete that does **not** outrank the current
  state is not stale: it is kept as a noncurrent version (a marker, for a
  delete) under its stamp, with its `x-rs3-versioned` bit, and answers as
  a success. A version the site already holds, or already removed, is
  dropped silently. A `null` state never joins a key that has one.
- `GET`/`HEAD ?versionId=<stamp dir>` reads any version the site holds,
  current or not (§5.1 pins still apply).
- `DELETE ?versionId=<stamp dir>` with `x-rs3-stamp` is a replicated
  removal: the version goes if held, and the site remembers it as removed
  for the tombstone retention, so a copy arriving later is refused. It is
  logged as `remove_version` either way.
- `PUT ?tagging&versionId=<stamp dir>` with `x-rs3-stamp` replaces that
  version's tags (`404 NoSuchVersion` when the site does not hold it: the
  tags come with the version). Unchanged tags log nothing, so a change
  that has gone round a merge pair stops there.

A replicator between two versioning-enabled buckets therefore applies
**every** event (nothing collapses within a batch), decides by identity
(is this version known here?) rather than by rank, fetches a put by its
version id, and forwards `remove_version` events. An unversioned side
keeps today's rules: one state per key, latest by rank; a superseded
state it is asked for by version id answers `404 NoSuchVersion`, which
the reader skips.

**A link requires the same versioning setting on both buckets** (both
`off`, both `enabled`, or both `suspended`): what is replicated — every
version, or the current states — must be the same thing on both sides.
The access report (§6.1), every changelog page (§4.1) and every snapshot
page (§4.2) carry the answering bucket's `versioning`; a link is refused
at creation when the settings differ, and a running link stops at once
when it sees a page from a bucket whose setting differs from its own. A
link checkpoints only after a batch is applied, so once both settings
match again it continues from its position; no re-snapshot is needed as
long as the position is within the peer's retention.

## 7. Named header fields

`x-rs3-replicator`, `x-rs3-merge-back` and `merge_back` use one encoding:
`key=value;key=value`, each key and value percent-encoded (so a value may
contain `;` or `=`). Readers look fields up by name, ignore unknown
fields, and do not depend on order. Required fields of `x-rs3-replicator`:
`site`, `mode`, `bucket`, `link`; optional: `name`, `site_name`.
`x-rs3-merge-back`: `link` required, `name` optional.

## 8. A conforming replicator

A link following this specification:

1. Checks the peer (§6.1): different site id, bucket present, actions,
   the same versioning setting as its own bucket (§5.4).
2. Walks the snapshot (§4.2), saving the first page's `head` — with
   `versions=1` when its own bucket has versioning enabled; a mirror
   deletes local keys the peer does not list; a merge also walks the
   tombstones (`deleted=1`).
3. Reads changes from that head (§4.1), batching with `limit`/`wait`.
4. Applies each event: on an unversioned bucket by rank (skips it if the
   local state ranks at or above it; only the latest event per key in a
   batch matters), on a versioning-enabled bucket by identity (§5.4,
   every event, skipping versions already held); skips repair deletes
   (§3.3); forwards `remove_version` as `DELETE ?versionId` with the
   stamp; otherwise fetches the exact version (§5.1, `?versionId` plus
   `x-rs3-if-version` and `x-rs3-if-stamp`; a `412` or `404 NoSuchVersion`
   means the state moved on and a later event carries the current one)
   and writes it with the event's stamp and `versioned` bit (§5.2),
   reproducing the part layout (§3.2).
5. Checkpoints `next` only after every event in the batch is applied.
6. On `410` or a changed `log_id` (`409`): re-snapshots.
7. Refuses to run if the peer's site id equals its own, or changes.
