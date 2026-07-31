# rs3 Fixed Worker Lanes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gradle-style fixed worker grid: always P lanes on the TTY UI (capped by console height), each showing its current task or a dim `> IDLE`; lanes are claimed/released in place instead of bars being inserted/removed.

**Spec:** `docs/superpowers/specs/2026-07-31-rs3-worker-lanes-design.md` (decisions locked).

## Global Constraints

- Non-TTY / `--json` / `--quiet` output byte-identical; full suite green is the proof.
- Lane grid is stable for the UI's lifetime: no bar insertion/removal during a run; claim/release only restyles.
- Idle rows must not consume steady-tick redraws (tick enabled only while a lane holds a spinner task).
- Overflow (in-flight > lanes) stays silent-but-counted, as today.
- Zero NEW clippy findings vs baseline; `cargo fmt` per commit; commands run from `client/`.
- Server crate untouched. Commit trailer:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01EsC8WkurPSRq13FeKyfA5W`

---

### Task 1: lane engine in progress.rs

**Files:** Modify `client/src/progress.rs` only.

**Interfaces produced:**
- `fn lane_count(p: usize, term_rows: Option<u16>, reserved: usize) -> usize` (pure):
  `min(p, usable)` where `usable = rows.saturating_sub(reserved)` clamped to ≥1; `None` rows → `min(p, 22)`; `p` already ≥1 by construction (budget clamps).
- `ProgressUi::new(parallel: usize)`, `ProgressUi::new_tasks_only(parallel: usize)`, `#[cfg(test)] hidden(parallel: usize)` — construction computes lanes via `lane_count(parallel, console rows, reserved)` with `reserved = 2` (transfer: TOTAL + margin) / `1` (tasks-only), creates ALL lane bars upfront in idle style (template `"{msg}"`, dim message `"> IDLE"`), TOTAL below them in transfer mode.
- `unit`/`task` keep their exact signatures; internally they claim a free lane (restyle to block-bar / spinner template, set rendered label, spinner lanes `enable_steady_tick(80ms)`); `finish`/`Drop` release: `disable_steady_tick()`, restore idle style+message, mark lane free. No `mp.add/remove/insert_before` after construction (except TOTAL at construction).
- Test accessors: keep `active_detail_bars()` (occupied lanes); add `lane_total()` (grid size).

- [ ] **Failing tests first** (adapt existing tests: `hidden()` calls become `hidden(5)` or a suitable P; the old "11th unit silent" tests still hold at P≥10 → use `hidden(10)` there):

```rust
#[test]
fn lane_count_is_p_capped_by_console_rows() {
    assert_eq!(lane_count(5, Some(40), 2), 5, "plenty of rows -> P lanes");
    assert_eq!(lane_count(32, Some(12), 2), 10, "capped by usable rows");
    assert_eq!(lane_count(32, None, 2), 22, "undetectable -> classic-terminal fallback");
    assert_eq!(lane_count(1, Some(40), 2), 1);
    assert_eq!(lane_count(8, Some(3), 2), 1, "degenerate terminal still >= 1");
}

#[test]
fn grid_is_stable_and_lanes_recycle() {
    let ui = ProgressUi::hidden(3);
    assert_eq!(ui.lane_total(), 3);
    assert_eq!(ui.active_detail_bars(), 0, "all idle at start");
    let a = ui.unit(lbl(Verb::Uploading, "a", None), 10);
    let t = ui.task(lbl(Verb::Listing, "b", None), "ListObjectsV2");
    assert_eq!(ui.active_detail_bars(), 2);
    assert_eq!(ui.lane_total(), 3, "grid never grows");
    let c = ui.unit(lbl(Verb::Uploading, "c", None), 10);
    let overflow = ui.unit(lbl(Verb::Uploading, "d", None), 10);
    assert_eq!(ui.active_detail_bars(), 3, "4th is silent overflow");
    overflow.inc(7);
    a.finish();
    assert_eq!(ui.active_detail_bars(), 2, "lane freed");
    let e = ui.unit(lbl(Verb::Uploading, "e", None), 10);
    assert_eq!(ui.active_detail_bars(), 3, "freed lane reclaimed");
    assert_eq!(ui.lane_total(), 3, "grid never shrinks");
    drop((t, c, e));
    assert_eq!(ui.active_detail_bars(), 0, "drops release lanes");
}
```

- [ ] Verify failure → implement → verify pass → full suite → fmt/clippy zero-new → commit `"feat: fixed worker-lane grid with idle rows in progress.rs"`.

Implementation notes: store `lanes: Vec<ProgressBar>` + `free: Vec<usize>` (or occupied bitmask) in `UiInner`; `UnitInner` records its lane index (`Option<usize>`); idle style is a plain `"{msg}"` template with message `"> IDLE"` styled dim via the template color (e.g. `"{msg:.240}"`); byte-unit and spinner templates are the existing ones. `Drop`/`finish` share one `release_lane` that restyles + returns the index to the free list. Constructor signatures changing means `transfer_ui()`/`worker_ui()` in this file take `parallel: usize` and forward — main.rs/messages.rs call-site updates happen in Task 2, so to keep this task compiling, KEEP thin compat shims `worker_ui()` (no args, defaults 5) DELEGATING to the new fns, marked `// Task-2 removes` — or update the few call sites in the same commit if simpler (allowed: messages.rs one-liner).

---

### Task 2: thread P through, smoke, README

**Files:** Modify `client/src/messages.rs` (`TransferSession::new(label, parallel)`), `client/src/main.rs` (put/cp/get call sites pass their parallel; standalone `worker_ui(5)` sites), `client/src/mirror.rs` (`TransferSession::new("mirror", parallel)`), `client/src/findcmd.rs`, `client/src/diff.rs` (worker_ui call sites), `client/src/progress.rs` (drop Task-1 compat shims), `client/README.md`.

- [ ] `TransferSession::new(label, parallel: usize)`; every call site passes its real `-P` (put/cp/mv/get: `args.parallel` / get's internal 5; mirror: its `parallel`). `worker_ui(parallel)` at all standalone sites (they all use 5, matching their budget).
- [ ] README: update the TTY-progress divergence entry: fixed gradle-style worker grid — always P lanes (capped by console height), idle lanes show `> IDLE`; replaces the "up to 10 bars" phrasing.
- [ ] Full suite green; fmt; clippy zero-new.
- [ ] **Manual pty smoke** (throwaway server recipe as before; record evidence):
  - `cp -r -P 5` of a few files: exactly 5 lane rows + TOTAL in frames; idle rows render `> IDLE`; lanes recycle (a row goes bar → IDLE → bar).
  - `-P 1` cp: single lane + TOTAL.
  - `ls`: 1 spinner + 4 `> IDLE` rows, then clean uncorrupted listing (glue regression check: grep typescript for content glued to IDLE/TOTAL fragments).
  - Small terminal: `script` with `stty rows 8` (or `LINES=8` via a nested `stty`) → lanes capped below P.
- [ ] Commit `"feat: always-P worker lanes with gradle-style IDLE rows"`.

## Self-review notes (applied)

- The grid-stability constraint is what prevents the prior insert/remove flicker class; the claim/release model keeps UnitHandle's external contract identical, so transfer.rs/budget.rs need zero changes.
- Off-TTY paths construct no UI regardless of the new params — byte-identity holds structurally.
