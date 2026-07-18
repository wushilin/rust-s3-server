# SQLite-as-Truth Storage Redesign

Status: design agreed 2026-07-18; Phases 0–3 implemented the same day
(schema v2 + intents, commit protocol, row-primary reads, rebuild pipeline
with 503 gate, intent-resolver sweeper, startup drain + flock, crash-point
test harness). Phase 4 dividends (hardlink copy, group commit, VACUUM INTO
snapshots, multipart claim) remain open. Migration decision: no in-place
schema migration — legacy/missing indexes are detected via PRAGMA
user_version and rebuilt from meta.json with progress logging; blob layout
unchanged (4-level fanout, V1-prefixed dirs).

## 1. Summary

Today the filesystem (hash-fanout blob dirs) is the source of truth and the
per-bucket SQLite index is a derived cache. Keeping the two in sync is the
origin of the list-after-write consistency bugs and of a large amount of
repair machinery (`visibility_repair_queue`, read-path repairs, rebuild
sweeps).

The redesign inverts authority:

- **The SQLite row is the object.** A PUT is committed the instant its row
  commits; a DELETE is committed the instant its row is deleted. Lists and
  reads resolve through rows only.
- **The filesystem is a dumb, immutable blob heap.** Blob dirs carry a
  self-describing `meta.json`, never change after publish, and their paths
  are stored in the row (`blob_dir`) — never derived from the key.
- **Every failure lands on an orphan** (an unreferenced, never-visible blob
  dir), cleaned by an intent-driven resolver. No failure can produce a
  visible object with missing data.

Commit protocol (all mutations): **intent → add → flip row atomically with
intent removal → retire late.**

### Schema

```sql
CREATE TABLE objects (
    object_key       TEXT PRIMARY KEY,
    blob_dir         TEXT NOT NULL,      -- relative to bucket dir
    size             INTEGER NOT NULL,
    etag             TEXT NOT NULL,
    last_modified_ms INTEGER NOT NULL
);

CREATE TABLE intents (
    id          INTEGER PRIMARY KEY,
    op          TEXT NOT NULL,           -- 'publish' | 'retire'
    object_key  TEXT NOT NULL,
    blob_dir    TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0
);
```

Removed: `visibility_repair_queue`, `objects.index_seen_at_ms`.
`PRAGMA user_version` identifies the schema for migration detection.

### Operation sequences

PUT / CopyObject / CompleteMultipart (overwrite included):

1. Outside lock: stage blob (parts + `meta.json`), **fsync files + staging dir**.
2. Insert `publish` intent (own transaction, committed).
3. Lock key (in-memory per-key mutex).
4. Read current row (old `blob_dir`, if any).
5. `last_modified_ms = max(now_ms(), old.last_modified_ms + 1)` (monotonic).
6. Rename staged dir → fresh uniquely-suffixed live path; fsync parent dir.
   Retry `create_dir_all` + `rename` on `ENOENT` (leaf-cleaner race).
7. One transaction: upsert row, delete publish intent, insert `retire`
   intent for the old dir (if overwrite).
8. Move old dir to trash; delete retire intent.
9. Unlock, ack, remove staging leftovers.

DELETE:

1. Lock key. Read row; absent → success (idempotent 204).
2. One transaction: delete row + insert `retire` intent.
3. Move blob dir to trash; delete retire intent.
4. Unlock, ack.

GET/HEAD (strictly read-only — never deletes, never repairs):

1. Read row; absent → 404.
2. Open `meta.json` / part files at `blob_dir` **early** (open FDs survive
   concurrent retirement).
3. On `ENOENT`: re-read the row once (covers overwrite race); still
   inconsistent → 500 + log. No mutation from the read path, ever.

LIST: pure SQL over `objects` (existing `list()` pagination logic kept).

### Crash / failure model

The only orphan-creating windows, all covered by intents:

| # | Window                                   | Intent shape |
|---|------------------------------------------|--------------|
| 1 | PUT publish before row commit            | publish      |
| 2 | CopyObject publish before row commit     | publish      |
| 3 | CompleteMultipart publish before commit  | publish      |
| 4 | Overwrite old dir after row flip         | retire       |
| 5 | DELETE blob after row delete             | retire       |

Invariant (enforce in review): **no rename into or out of the live tree
without an intent row first**, and **no blob file is ever modified after
publish** (this immutability is also what makes hardlink-based copy safe).

Recovery after crash = `SELECT * FROM intents`: publish intent → whatever is
at `blob_dir` is garbage (trash it); retire intent → finish the trash move.
O(in-flight), not O(tree).

A background intent resolver (replaces `process_visibility_repairs`) handles
stale intents during normal operation: age > longest plausible op, key not
currently locked; bump `attempts`, log loudly on repeated failure.

### Durability modes (config)

- `full` (default): blob fsync before intent, `synchronous=FULL`, group
  commit batching for the writer connection. Acked = survives power loss.
- `relaxed`: skip per-put blob fsync, `synchronous=NORMAL`; requires the
  startup reconciliation (verify recent rows' blobs; walk trash `meta.json`
  for resurrected rows) after unclean shutdown. Acked writes from the last
  seconds may vanish; consistency is never violated.

fsync is per-file — SQLite's own fsync orders nothing about blob writeback,
so the blob fsync cannot be elided by reasoning about the WAL.

### Concurrency

- Per-key in-memory async mutex (existing `locks.rs`) held only for the
  commit section. SQLite transactions stay microsecond-short and never span
  a filesystem call.
- SQLite: one dedicated writer connection (group commit lives there) + a
  reader pool. Sqlite has no row locks; do not use it as the mutex.
- **Startup flock on the data root**; refuse to start if held. The in-memory
  locks are only meaningful with exactly one process per data dir.
- Empty fanout-leaf cleaner (if kept): only remove empty dirs older than a
  grace window (mtime); publisher retries on ENOENT. Kernel `rmdir`
  (fails on non-empty) is the hard floor — no global lock.

### Layout

Deferred decision — rows store full paths, so layout binds only new writes:

- Option A (default for the first release): keep 4-level fanout, unchanged.
- Option B: trim to 2 levels for new writes (65,536 leaves; dirs kept
  forever; leaf cleaner and its race deleted).
- Option C: time-based `YYYY/MM/DD/HH/MM/` leaves (granularity
  configurable); enables watermark GC; creation and cleanup temporally
  disjoint.

Old blobs never move regardless of choice.

### Rebuild (DR + migration)

Trigger: missing DB or old `user_version`. **Blocking**: the bucket serves
503 + Retry-After for ALL requests until rebuild completes (explicit
decision — no partial serving).

Pipeline: 1 walker → bounded queue (default 1000) → N reader/parser threads
(default 8, config) → 1 batch writer (1000 rows/txn, newer-wins upsert:
`ON CONFLICT ... WHERE excluded.last_modified_ms > objects.last_modified_ms`).
Build into `index.sqlite.tmp` with `synchronous=OFF`, checkpoint, atomic
rename.

Rebuild doubles as the full audit (tree is frozen — no age-gating needed):

- Per-key duplicate losers → trashed at conflict time (writer knows both paths).
- Live-tree dirs with missing/invalid `meta.json` → definitionally garbage
  (publishes are staged complete) → trashed.
- Empty dirs → rmdir.
- Excludes `trash/` and `staging/` (scanning trash resurrects deleted objects).
- Semantics are faithfully generous: a well-formed never-acked orphan is
  indistinguishable from a live object and gets adopted.

Rollback is symmetric: the old binary rebuilds its own schema from the same
self-describing tree.

Ops: pause any cleaner after restoring from backup until a rebuild runs;
periodic `VACUUM INTO` snapshot of the index makes full rebuilds rare.

## 2. Code review findings (current code vs. this design)

Reviewed: `store.rs`, `index.rs`, `resolver.rs`, `locks.rs`, `sweeper.rs`,
`encoding.rs`, `layout.rs`.

1. **No fsync anywhere in the storage layer.** No `sync_all`/`sync_data` in
   any write path; `write_json_atomic` (store.rs:1586) only flushes
   userspace buffers before its rename. Acked data does not survive power
   loss, independent of the index design.
2. **Overwrites reuse the old dir name and have a vanish window.**
   `prepare_live_publish_path` (store.rs:1549) returns the *old* dir path
   for overwrites; `commit_staged_put` (store.rs:707–716) then moves the old
   dir to trash **before** renaming the new dir to the same name. Crash or
   error between the two ⇒ the object has no on-disk dir at all while the
   index row still exists. Same pattern in `complete_multipart`
   (store.rs:1324–1333). This is the mechanism most likely behind the
   original "written objects not visible / inconsistent" symptoms.
3. **DELETE retires before committing.** `delete_object` (store.rs:866–869)
   trashes the blob dir before `index.delete`. Crash between ⇒ index lists
   an object whose data is already in trash.
4. **The read path mutates.** `read_object` enqueues repairs
   (store.rs:762, 787), deletes candidate dirs with `remove_dir_all`
   (store.rs:789), and `cleanup_invisible_candidate_dirs` (store.rs:1599)
   removes meta-less dirs with no lock and no age gate — reads racing
   writes can destroy in-flight state. The redesign forbids all of it.
5. **Every GET pays FS resolution.** `resolve_object_dir` readdirs the leaf
   and reads candidate `meta.json`s (resolver.rs:79–103), then a repair
   check may hit SQLite too (store.rs:823–850), with a 200k-entry
   `sqlite_repair_cache` + TTL to soften it. All replaced by one row read.
6. **`last_modified_ms = now_ms()` with no monotonic clamp**
   (store.rs:671, 1295). Newer-wins rebuild adjudication is unsound under
   clock steps until clamped per key.
7. **Multipart complete/abort race on the staging dir.** `complete_multipart`
   validates and reads parts (store.rs:1256–1291) before taking the key
   lock (1313); `abort_multipart` can remove the staging dir concurrently,
   and two completes of one upload can both proceed. Needs a claim step
   (atomic rename of staging dir to `<id>.completing`).
8. **Cache validation is fragile.** `meta_cache` validates by `meta.json`
   mtime+len (store.rs:803–806); mtime granularity makes false-fresh
   possible. Redesign: validate against the row's `(etag, last_modified_ms)`.
9. **Sweeper deletes staging dirs by mtime with a 24h default** — fine, kept;
   but `repairs_queued_sqlite_orphan_after_grace_period` semantics invert
   under the new design (rows are truth; a missing blob is a 500 + operator
   signal, not an index fix).
10. **Connection pool is 50 undifferentiated connections** (index.rs:36);
    no dedicated writer, no group commit.

## 3. What gets deleted

- `resolver.rs` entirely (`resolve_object_dir`, `allocate_object_dir`).
- `fanout_locks` / `FanoutLockKey` / `object_fanout_read_lock` (store.rs).
- `sqlite_repair_cache`, `SQLITE_REPAIR_CACHE_TTL_MS`,
  `mark_sqlite_repaired`, `sqlite_repair_recently_checked`.
- `visibility_repair_queue` + its six methods in index.rs;
  `enqueue_visibility_repair`, `reconcile_object_index`,
  `process_visibility_repairs`, `VisibilityRepairBatch` in store.rs.
- `cleanup_invisible_candidate_dirs`; the repair block in `read_object`.
- `index_seen_at_ms`, `put_seen_at`, `delete_entries_not_seen_since`,
  `replace_all`, background `start_rebuild_background` drift-rebuild path.
- `object_dir_prefix` key-hashed naming (dir names become opaque unique IDs).

## 4. Implementation phases

Each phase lands green and shippable.

**Phase 0 — harness first.**
Crash-point injection: label each step of the commit sequences; test-only
hook that panics/errors after a named step. Invariant checker used by all
tests: (a) every row's `blob_dir` exists with valid meta; (b) recovery
resolves every intent; (c) no live-tree dir is referenced by zero rows
*after* resolver + audit run. Baseline S3-behavior tests (list pagination,
multipart, copy) locked in before any change.

**Phase 1 — schema + commit protocol (the core).**
New `index.rs` schema (`blob_dir`, `intents`, `user_version` bump; dedicated
writer connection). Rewrite `commit_staged_put`, `complete_multipart`,
`copy_object`, `delete_object` to the sequences above (always-fresh suffix,
fsync, monotonic clamp, intent bracketing). Rewrite `read_object` /
`list_object_versions` row-primary with the single-retry rule. Cache
re-keyed to row identity. Startup flock. Delete the Section-3 list.

**Phase 2 — rebuild + migration.**
Parallel pipeline rebuild (walker/readers/batch-writer, config knobs),
tmp-db + atomic swap, duplicate/invalid/empty cleanup, trash+staging
exclusion. `user_version` detection → blocking rebuild; 503 gate in
`server/mod.rs` while `rebuilding` contains the bucket. Remove old
background-rebuild trigger.

**Phase 3 — maintenance loop.**
Intent resolver replaces visibility repairs in the sweeper; staging/trash
expiry sweeps kept as-is. Startup reconciliation for `relaxed` durability
(unclean-shutdown marker file). Admin audit command = rebuild pipeline in
verify-only mode.

**Phase 4 — dividends (optional, independent).**
CopyObject / copy-part via hardlink (immutability makes it safe).
Group-commit batching on the writer connection. `VACUUM INTO` snapshot job.
Layout option B or C for new writes. Multipart `.completing` claim.

## 5. Testing focus

- Crash-point matrix: every labeled step × {PUT new, overwrite, delete,
  complete-multipart, copy} → recover → invariants hold, acked objects
  readable, unacked at-worst-orphaned.
- Concurrency: same-key PUT×PUT, PUT×DELETE, GET during overwrite/delete
  (retry path), complete×abort.
- Power-loss ordering can't be unit-tested honestly; document the fsync
  reasoning and keep `full` mode default.
- Rebuild: duplicate adjudication, trash exclusion (deleted object must not
  resurrect), invalid-dir trashing, newer-wins order-independence
  (shuffle reader completion order).
