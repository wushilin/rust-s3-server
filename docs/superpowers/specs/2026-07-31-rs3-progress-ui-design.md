# rs3 multi-bar live progress — design

**Date:** 2026-07-31
**Status:** approved by user (bar layout, scope, and tick-source decisions all confirmed via menu)

## Problem

rs3's progress bar barely moves. `TransferSession` (`client/src/messages.rs`)
owns a single indicatif `ProgressBar` that is only ticked in `object_done` —
i.e. after an **entire object** finishes. During a multi-GiB `cp` the bar sits
at 0 until the very end. The transfers themselves are already
parallel-segmented (`multipart_upload`, ranged `download_to_temp` with
seek+write, parallel server-side copy — all `buffer_unordered`); only the
reporting is missing.

## User decisions (locked)

1. **Bar layout:** one persistent overall bar at the bottom
   (`TOTAL x/y objects [bar] bytes/total speed eta`) plus up to **10** detail
   bars above it, one per in-flight transfer unit. A *unit* is one multipart
   segment of a large file, or one whole small file (mirror).
2. **Scope:** `cp`, `mv`, `put`, `get`, `mirror` — everywhere
   `TransferSession` is used today. `pipe` (unknown size) and `cat` (streams
   to stdout) unchanged.
3. **Tick source:** true byte streaming. Downloads tick per network chunk;
   uploads tick via a request-body wrapper; retried parts rewind their
   counter. Server-side S3→S3 copy is the exception: no bytes flow through
   the client, so each segment ticks its full size on `UploadPartCopy`
   completion (documented limitation).

## Design

### 1. New module `client/src/progress.rs`

`ProgressUi` wraps `indicatif::MultiProgress`:

- **Overall bar** pinned at the bottom. Template:
  `TOTAL {x}/{y} objects [bar] {bytes}/{total_bytes} {bytes_per_sec} eta {eta}`.
  Length grows via the session's `add_total`.
- **Detail-bar pool, cap 10.** `unit(label, len) -> UnitHandle`. If a slot is
  free, a bar is inserted above the overall bar; if all 10 are busy the
  handle is *silent* — no bar, but its bytes still tick the overall bar.
  No waiting/promotion queue (YAGNI: in-flight units are bounded by transfer
  parallelism, so overflow is rare and short-lived).
- **`UnitHandle`** (cheap `Arc` clone, safe inside `buffer_unordered`
  futures): `inc(u64)`, `rewind()` (reset to 0 within the unit, for part
  retries), `finish()` (snap to 100%, remove bar, free slot — guarantees no
  undercount from chunk rounding). Every `inc`/`rewind`/`finish` also adjusts
  the overall bar by the same delta.
- Detail template: `{label} [bar] {bytes}/{total_bytes} {bytes_per_sec}` with
  label `<filename> part <i>/<n>` for segments or just `<filename>` for whole
  small files; long names truncated (indicatif `wide_msg`).

### 2. `TransferSession` integration (`messages.rs`)

- Replace the single `bar: Option<ProgressBar>` with `ui: Option<ProgressUi>`.
- **Activation predicate unchanged:** `stdout_tty && !quiet && !json`, and the
  existing global disable-progress-bar flag also disables the new UI.
- `add_total` extends the overall bar length and object count, as today.
- **No double counting:** in bar mode `object_done` advances only the `x/y`
  object counter; bytes come exclusively from `UnitHandle` ticks. (Today it
  does `bar.inc(size)` — that line's behavior moves to the handles.)
- Non-TTY / `--json` / `--quiet` paths are **byte-identical to today**:
  per-object printed messages + final `AccountStat`. The whole tested
  mc-compat contract is untouched.

### 3. Transfer plumbing (`transfer.rs`, call sites in `main.rs`/`mirror.rs`)

Transfer functions gain an optional `Option<ProgressUi>` (cheap clone)
parameter threaded from the session. The transfer functions themselves create
the per-segment `UnitHandle`s — they are the only place that knows segment
boundaries and counts; call sites in `main.rs`/`mirror.rs` just forward the
session's `ProgressUi`:

- **Downloads** (`download_key_to_path` single + ranged parallel): tick
  `handle.inc(chunk.len())` inside the existing chunk loop before seek+write.
- **Uploads** (`upload_file` single-part + `multipart_upload` parts): wrap
  the `SdkBody` in a `ProgressBody` that ticks as chunks are polled onto the
  wire. Built through the SDK's retryable-body constructor so each retry
  attempt calls `handle.rewind()` before re-streaming — no double counting.
  This is the fiddly piece; it gets dedicated unit tests.
- **Server-side copy** (`multipart_server_side_copy` and single
  `CopyObject`): tick full segment size on part completion.
- `upload_stream` (pipe) is out of scope and takes no sink.

### 4. mc-compat and docs

TTY multi-bar display is a deliberate divergence from mc's single aggregate
bar. Add it to `client/README.md` "Known divergences from mc". No message
structs, JSON shapes, or research docs change.

### 5. Testing

- **Unit (progress.rs):** slot cap — 11th concurrent unit is silent; slot
  freed by `finish()` is reusable; overall-bar totals equal the sum of unit
  ticks; `finish()` tops up a partially-ticked unit exactly once; `rewind()`
  subtracts correctly. Bars use a hidden draw target in tests.
- **Unit (ProgressBody):** wrapping an in-memory body of known size ticks
  exactly `len` bytes; a simulated second attempt (rebuilding the body via the
  retryable constructor) first rewinds, ending at exactly `len`, not `2*len`.
- **Regression:** all 172 existing tests green unchanged — the e2e harness
  has no TTY, so unchanged non-TTY output *is* the contract test.
- **Manual TTY smoke:** large-file `cp`/`get`/`mirror` against local rusts3
  (and optionally the scoped real-AWS test prefix) to eyeball bar behavior,
  including >10 concurrent units and a retry if reproducible.

## Non-goals

- `pipe`/`cat` progress (unknown size / stdout streaming).
- Configurable bar count (10 is fixed for now).
- Any change to `--json`/`--quiet`/non-TTY output or exit codes.
- Matching mc's TTY bar visuals (deliberate divergence).
