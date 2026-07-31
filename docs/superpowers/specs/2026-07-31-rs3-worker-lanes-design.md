# rs3 fixed worker lanes (gradle-style) — design

**Date:** 2026-07-31
**Status:** approved by user ("based on parallelism, we should always show X
tasks. when not doing anything, we show something like gradle: idle";
clarified: "actually always P, unless P is higher than the max rows we can
render in this console")

## User decisions (locked)

1. The TTY UI renders a **fixed grid of worker lanes** — one row per
   worker, always visible for the whole run. Lane count = **P** (the
   command's parallelism / stream-budget size), capped only by what the
   console can render: `min(P, usable_rows)` where `usable_rows` is derived
   from the detected terminal height minus reserved rows (TOTAL bar +
   1 margin in transfer mode; 1 margin in tasks-only mode). Height
   undetectable → fall back to `min(P, 22)` (a 24-row classic terminal).
2. **An unoccupied lane shows an idle marker**, gradle-style: `> IDLE`,
   dim (the 240-gray already used for bar backgrounds).
3. Uniform everywhere the TTY UI is active — transfer commands and
   standalone commands alike (a sequential `ls` with budget 5 shows 4 idle
   lanes; lanes clear when the command finishes, before stdout results
   would need the space).

## Design

### progress.rs: lane engine replaces dynamic insert/remove

- `ProgressUi` construction takes `lanes: usize` (computed by a pure
  `lane_count(p, term_rows: Option<u16>, reserved: usize) -> usize`,
  unit-tested). All lane `ProgressBar`s are created upfront in idle style
  and stay in the `MultiProgress` for the UI's lifetime; TOTAL (transfer
  mode) stays pinned below them.
- `unit(label, len)` / `task(label, api)` **claim a free lane**: swap the
  lane's style+message to the block-bar or spinner template. `finish()`/
  `Drop` **revert the lane to idle** (idle style, `> IDLE` message,
  steady-tick disabled) instead of removing the bar. Slot bookkeeping
  (`active` count, silent overflow handles when in-flight > lanes) is
  unchanged in spirit; `MAX_DETAIL_BARS = 10` is replaced by the computed
  lane count.
- Spinner steady-tick is enabled on claim, disabled on release (idle rows
  must not burn redraws; the grid redraws on real progress events).
- Session end: tasks-only mode clears the whole grid; transfer mode clears
  lanes and finishes TOTAL in place (visible), as today.

### Threading P

- `TransferSession::new(label)` gains the parallelism:
  `TransferSession::new(label, parallel: usize)` — call sites (put, cp/mv,
  get, mirror) pass their `-P`/internal value.
- `worker_ui()` becomes `worker_ui(parallel: usize)`; standalone commands
  pass the same value they size their `StreamBudget` with (5).

## Testing

- Unit: `lane_count` (P below/above usable rows, undetectable fallback,
  degenerate tiny terminals ≥1); claim/release returns lanes to idle and
  reusable; overflow beyond lane count silent but counted; grid never
  grows/shrinks during a run (lane bar identity stable).
- Full suite byte-identical off-TTY (no UI there — unchanged).
- Manual pty smoke: cp -r with -P 5 shows 5 rows (bars + `> IDLE`) + TOTAL;
  ls shows 1 spinner + 4 idle rows then clean listing; -P 1 shows a single
  lane; a small terminal (script with rows=8) caps lanes.

## Non-goals

- Reacting to terminal resize mid-run (lane count fixed at UI creation).
- Per-lane worker IDs/numbering (rows are anonymous lanes).
- Any change to --json/--quiet/non-TTY output.
