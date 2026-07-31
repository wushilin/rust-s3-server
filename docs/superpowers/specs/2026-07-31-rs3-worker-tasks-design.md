# rs3 universal worker-task dispatch — design

**Date:** 2026-07-31
**Status:** approved by user (menu: full worker pool; scope: everything everywhere)

## User decisions (locked)

1. **Every dispatched S3 operation follows the worker pattern** — one `-P`
   token per task, control-plane included: CreateMultipartUpload,
   CompleteMultipartUpload, AbortMultipartUpload, HeadObject, listing
   pages, deletes. Uniform "everything is a worker task" model.
2. **Scope: everywhere** — not just cp/mv/put/get/mirror; standalone
   commands (ls, rm, stat, du, tree, find, diff, mb, rb, cat, head…) also
   dispatch their S3 calls as worker tasks when the TTY UI is active.
3. **Display**: byte streams keep their block bars; byte-less tasks get a
   task line: padded label + spinner + the raw S3 API name (geeky on
   purpose): `Creating upload asdf/a.img   ⣹ CreateMultipartUpload`.

## Deadlock rule (unchanged, restated for the new scope)

A task acquires its token, runs, releases — and **never holds a token
while a child task acquires one**. Control-plane ops all run sequentially
relative to their children (create → parts → complete), so acquiring a
token per op is safe at any `-P` including 1. The e2e `-P 1` regression
test must stay green.

## Design

### 1. `progress.rs`: spinner task lines

- `Verb` grows control-plane variants: `Creating`, `Completing`,
  `Aborting`, `Inspecting`, `Listing`, `Removing` (display words; the API
  name column carries the exact operation).
- New `ProgressUi::task(label: TransferLabel, api: &'static str) ->
  UnitHandle`-style handle: occupies a detail-bar slot (same cap-10 pool),
  template `"{msg} {spinner} <api>"` with a steady tick (~80ms), finish
  removes it. Silent when slots are exhausted, like byte units. Handle
  reuses `UnitHandle` semantics (finish/Drop frees slot; no byte
  accounting — len 0, no overall-bar contribution).

### 2. `worker.rs` (or `budget.rs` extension): the dispatch helper

```
dispatch(budget, ui, label, api, future) -> future's output
```
acquires a token FIRST, then creates the task line (ordering rule:
visible task == token holder), awaits the future, finishes the line,
releases the token (RAII). Byte-stream leaves keep their existing inline
acquire + bar-unit code (they need the handle inside the body wrappers).

### 3. Wiring

- Transfer commands: create/complete/abort multipart (all three engines),
  HEAD/stat calls, mirror planning list pages, mirror `--remove` deletes
  go through `dispatch`.
- Standalone commands: each command that talks to S3 creates the same
  TTY-gated `ProgressUi` (predicate identical to `TransferSession`) and a
  `StreamBudget` (their ops are sequential; budget size 5 default), and
  routes its API calls through `dispatch`. Non-TTY/--json/--quiet: no UI
  (spinners are stderr+TTY only) — printed output stays byte-identical.
  Spinner lines are cleared before a command prints its stdout results
  (finish the task before printing), so listings never interleave.

## Testing

- Unit: `task()` slot accounting (shares cap 10, finish frees, silent
  overflow, no overall-bar byte contribution); dispatch helper acquires
  before task-line creation and releases on completion AND on error
  (future returning Err still frees token + slot).
- e2e: entire suite must stay byte-identical (non-TTY = no UI); `-P 1`
  multipart roundtrip stays green (control ops now consume tokens too).
- Manual TTY smoke: multipart cp shows Creating/Completing task lines
  around the part bars; ls/rm on TTY show transient spinners without
  corrupting their stdout output.

## Non-goals

- `pipe` remains outside the budget/UI (unknown size; prior non-goal).
- No new CLI flags; standalone commands use budget 5 internally.
- share/alias/config commands (no S3 data-plane traffic worth showing) —
  skip unless trivially uniform.
