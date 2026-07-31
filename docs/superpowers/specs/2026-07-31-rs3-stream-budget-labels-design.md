# rs3 cooperative stream budget + labeled fixed-layout bars — design

**Date:** 2026-07-31
**Status:** approved by user (scope: both pieces together; label format and
`123.3/256MiB` bytes column confirmed in conversation)

## Problem

1. `-P` today caps object-level and segment-level concurrency separately, so
   a mirror of large files can run up to P×P concurrent streams. The user
   wants `-P` to mean a **global budget of concurrent transfer streams**,
   cooperatively shared: `-P 4` = 4 small files at once, or 2 files × 2
   segments, or 1 file × 4 segments — allocator-and-returned-token style.
2. Detail-bar labels are bare (`a.img part 4/24`), the layout shifts as
   digit widths grow, and indicatif's `{wide_msg}` truncates the *tail* of
   long labels, eating the `part i/n` suffix (deferred finding from the
   progress-UI branch).

## User decisions (locked)

- Both pieces land together on one branch.
- Labels are explicit and verbed: `Uploading asdf/a.img part 4/24`,
  `Downloading b/…/big.iso part 1/8`, `Copying pics/x.jpg`. Verb by
  direction: Uploading (local→S3), Downloading (S3→local), Copying (S3→S3).
- Fixed progress-bar layout: fixed-width label column, fixed-width bar,
  aligned byte/speed columns.
- Long labels are condensed by dropping *middle* path components
  (`Uploading asdf/…/a.img part 4/24`), always preserving the verb, the
  filename tail, and the part suffix; trimming the filename itself is the
  last resort.
- Bytes column format: `123.3/256MiB` — transferred and total share one
  unit, printed once (on the total). Unit is chosen from the total; the
  transferred value is scaled to that unit with one decimal.
- Part-level bars remain the display unit, and a bar exists exactly while
  its stream holds a budget token — so the visible bar count always equals
  the live parallelism (3 bars = 3 streams). No idle/pending placeholder
  bars; this is what makes the fluctuating cooperative parallelism legible
  instead of confusing.
- Default `-P` changes from 4 to **5** (every command that takes
  `--parallel`).

## Design

### 1. Stream budget (`client/src/budget.rs`)

- `StreamBudget` wraps `Arc<tokio::sync::Semaphore>` with `-P` permits,
  created once per command invocation next to the `TransferSession`.
  `StreamBudget::acquire() -> StreamPermit` (an `OwnedSemaphorePermit`
  newtype); dropping the permit returns the token. tokio's semaphore is
  FIFO, so a large file's many parts queue fairly against other files.
- **Leaf-only acquisition (the deadlock rule):** exactly the code paths
  that move a stream acquire one permit each, held only for the duration of
  that stream:
  - each multipart upload part (acquire before opening the file segment /
    sending `UploadPart`)
  - each ranged download part and each single-shot GET download
  - each single-shot PUT upload
  - each cross-endpoint streamed part (one permit per GET→UploadPart
    pipeline, not two)
  - each server-side `UploadPartCopy` / single `CopyObject` (no client
    bytes, but the permit bounds outstanding requests consistently)
  - Object-level orchestration (HEAD, CreateMultipartUpload,
    CompleteMultipartUpload, mirror planning, deletes) **never holds a
    permit** — nested acquisition could deadlock once P objects each held
    one and waited for segment permits.
- The existing `buffer_unordered(parallel)` layers stay as structural
  bounds on how many objects/parts are *initiated*; the semaphore is the
  throughput governor. Net effect: total concurrent streams ≤ P always
  (today's worst case was P×P).
- `pipe`'s `stream_parts` keeps its own internal bound and takes no
  permits (consistent with pipe being outside the progress feature too).
- Behavior note (documented): for multi-large-file mirrors this *reduces*
  concurrency vs the old P×P — the intended, least-surprise meaning of
  `-P`. mc has no equivalent semantics to conflict with.

### 2. Structured labels (`client/src/progress.rs`)

- `ProgressUi::unit` gains a structured label:
  `TransferLabel { verb: Verb, path: String, part: Option<(u64, u64)> }`
  with `enum Verb { Uploading, Downloading, Copying }`. Transfer functions
  construct it from what they already know (direction + display path +
  part index/count); no call site invents strings anymore.
- Rendering: `label.render(width) -> String` condenses to fit the fixed
  label column:
  1. full: `Uploading asdf/deep/dir/a.img part 4/24`
  2. drop middle path components one by one, replacing with `…`:
     `Uploading asdf/…/a.img part 4/24`
  3. last resort: trim the filename with a leading `…`, keeping the verb
     and part suffix intact.
- Pure function, unit-tested against exact expected strings at several
  widths.

### 3. Fixed layout + bytes column

- Detail-bar template: fixed-width label field (pre-rendered to width by
  `render`, then padded), fixed `{bar:30}`, then `{bytes_pair}` and
  `{binary_bytes_per_sec}` right columns.
- `{bytes_pair}` is a custom indicatif template key
  (`ProgressStyle::with_key`) backed by a pure
  `format_bytes_pair(pos, len) -> String`: unit chosen from `len`
  (binary units), `pos` scaled to that unit with one decimal →
  `123.3/256MiB`, `0.0/256MiB`, `256.0/256MiB`. `len == 0` renders
  `0.0/0B`. Unit-tested.
- Label column width: `40` columns fixed (covers the mock the user
  approved; terminal-width adaptation is YAGNI for now and can follow).
- TOTAL bar keeps its template but adopts `{bytes_pair}` for consistency.

### 4. Docs

- README: `-P` semantics change ("global concurrent-stream budget, shared
  between objects and segments") + label/format description in the
  existing TTY-progress divergence entry.
- Spec/plan committed as usual.

### 5. Testing

- **budget.rs unit tests:** max-observed-concurrency ≤ P under a stress
  spawn of 100 tasks (counter around acquire/release); FIFO liveness with
  P=1 (all tasks complete — no-deadlock regression for leaf-only rule).
- **e2e:** new test running a multipart `cp` (small `-s`) with `-P 1` to
  completion — end-to-end deadlock regression; existing 186-test suite
  must stay green (non-TTY output byte-identical; budget changes must not
  alter any printed output).
- **label/format unit tests:** `render` condensing at several widths;
  `format_bytes_pair` exact strings.
- **Manual TTY smoke:** re-run the progress smoke; verify verbed labels,
  aligned columns, `123.3/256MiB` style bytes, condensing on a deep path,
  and that `-P 4` on a 2-large-file mirror shows ≤ 4 active part bars.

## Non-goals

- Terminal-width-adaptive label column (fixed 40 for now).
- `pipe` participation in the budget or labels.
- Env-var overrides for `-P`/`-s` (separate request, not asked for here).
- Any change to non-TTY/`--json`/`--quiet` output or exit codes.
