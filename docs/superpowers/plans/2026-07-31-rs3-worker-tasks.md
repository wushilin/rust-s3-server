# rs3 Universal Worker-Task Dispatch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Model every S3 operation as a dispatched worker task: one `-P` token each (control-plane included), spinner task lines with raw API names for byte-less ops, everywhere — transfer commands and standalone commands alike.

**Architecture:** `progress.rs` gains `ProgressUi::task(label, api)` spinner lines sharing the 10-slot pool; `budget.rs` gains an async `dispatch(budget, ui, label, api, future)` helper (token → task line → await → finish, RAII). Transfer engines wrap create/complete/abort/HEAD/planning/deletes in `dispatch`; standalone commands get a TTY-gated `ProgressUi` + `StreamBudget::new(5)` and route their S3 calls the same way.

**Spec:** `docs/superpowers/specs/2026-07-31-rs3-worker-tasks-design.md` (decisions locked; deadlock rule restated there).

## Global Constraints

- Non-TTY / `--json` / `--quiet` output **byte-identical** — the entire mc-compat contract must not move; all existing tests pass unchanged. Spinners are stderr+TTY-only.
- **Never hold a token while a child acquires one.** Each op: acquire → run → release. The `-P 1` e2e roundtrip must stay green with control-plane ops now consuming tokens.
- Task lines finish before a command prints stdout results (no interleave).
- Byte-stream leaves keep their existing inline acquire + bar-unit code — `dispatch` is for byte-less ops only.
- Zero NEW clippy findings vs baseline (~24 pre-existing incl. never_loop deny are out of scope); `cargo fmt` per commit; commands run from `client/`.
- Server crate (repo root `src/`) untouched. `pipe`, share/alias/config exempt (spec non-goals).
- Commit trailer:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01EsC8WkurPSRq13FeKyfA5W`

---

### Task 1: spinner task lines + dispatch helper

**Files:** Modify `client/src/progress.rs`, `client/src/budget.rs`.

**Interfaces produced (later tasks rely on these exact signatures):**
- `Verb` gains variants: `Creating`, `Completing`, `Aborting`, `Inspecting`, `Listing`, `Removing` (as_str: the capitalized word).
- `ProgressUi::task(&self, label: TransferLabel, api: &'static str) -> UnitHandle` — takes a detail slot from the same cap-10 pool; renders `"{msg} {spinner} <api>"` (label padded to LABEL_WIDTH like `unit`), `enable_steady_tick(Duration::from_millis(80))`; len 0 so `finish()` contributes nothing to the overall bar; silent handle when slots exhausted; Drop frees the slot.
- `budget.rs`: `pub(crate) async fn dispatch<T, F: Future<Output = T>>(budget: &StreamBudget, ui: Option<&crate::progress::ProgressUi>, label: crate::progress::TransferLabel, api: &'static str, fut: F) -> T` — acquires the token FIRST, then creates the task line, awaits, finishes the line, permit drops on return. Works for `T = Result<..>`: the line is finished on the error path too.

- [ ] Failing tests first (progress.rs tests module + budget.rs tests module):

```rust
#[test]
fn task_lines_share_the_slot_pool_and_add_no_bytes() {
    let ui = ProgressUi::hidden();
    ui.add_object(100);
    let bars: Vec<UnitHandle> =
        (0..9).map(|i| ui.unit(lbl(Verb::Uploading, "x", Some((i + 1, 9))), 10)).collect();
    let t1 = ui.task(lbl(Verb::Creating, "asdf/a.img", None), "CreateMultipartUpload");
    assert_eq!(ui.active_detail_bars(), 10, "task takes the 10th slot");
    let t2 = ui.task(lbl(Verb::Listing, "bucket/p", None), "ListObjectsV2");
    assert_eq!(ui.active_detail_bars(), 10, "11th is silent");
    t1.finish();
    t2.finish();
    assert_eq!(ui.active_detail_bars(), 9);
    assert_eq!(ui.overall_position(), 0, "tasks contribute no bytes");
    drop(bars);
}

#[tokio::test]
async fn dispatch_releases_token_and_slot_on_ok_and_err() {
    let ui = ProgressUi::hidden();
    let budget = StreamBudget::new(1);
    let ok: Result<u32, anyhow::Error> = dispatch(
        &budget, Some(&ui),
        lbl(Verb::Inspecting, "b/k", None), "HeadObject",
        async { Ok(7) },
    ).await;
    assert_eq!(ok.unwrap(), 7);
    let err: Result<u32, anyhow::Error> = dispatch(
        &budget, Some(&ui),
        lbl(Verb::Completing, "b/k", None), "CompleteMultipartUpload",
        async { Err(anyhow::anyhow!("boom")) },
    ).await;
    assert!(err.is_err());
    // budget of 1: a third dispatch only completes if both tokens were returned
    let again: Result<u32, anyhow::Error> = dispatch(
        &budget, Some(&ui),
        lbl(Verb::Listing, "b", None), "ListObjectsV2",
        async { Ok(1) },
    ).await;
    assert_eq!(again.unwrap(), 1);
    assert_eq!(ui.active_detail_bars(), 0, "all task lines cleared");
}
```

(`lbl` already exists in progress tests; budget test needs tiny local copies or `use crate::progress::*` — implementer's choice; `anyhow` is already a dependency.)

- [ ] Verify failure → implement → verify pass → full suite → fmt/clippy → commit `"feat: spinner worker-task lines + dispatch helper"`.

Implementation notes: spinner bar via `ProgressBar::new_spinner()` inserted with `insert_before(&overall)`; template `&format!("{{msg}} {{spinner}} {api}")`; reuse `UnitInner` with `len: 0`; make sure `release_slot`/Drop path is shared with byte units. `dispatch` creates `UnitHandle::noop()` when `ui` is `None`.

---

### Task 2: transfer engines' control-plane ops through dispatch

**Files:** Modify `client/src/transfer.rs`, `client/src/mirror.rs`.

Wrap with `crate::budget::dispatch(budget, progress, label, api, async { ...send().await })`:
- `multipart_upload`: CreateMultipartUpload (`Verb::Creating`, path = source display path), CompleteMultipartUpload (`Verb::Completing`), AbortMultipartUpload in the error path (`Verb::Aborting`).
- `multipart_copy_s3_to_s3` + `multipart_server_side_copy`: same three, path = source_key; plus `multipart_server_side_copy`'s HeadObject (`Verb::Inspecting`).
- `download_key_to_path`: HeadObject (`Verb::Inspecting`, path = `bucket/key`).
- `mirror.rs`: planning list pages (`Verb::Listing`, path = the listed prefix, api "ListObjectsV2") and `--remove` deletes (`Verb::Removing`, path = key, api "DeleteObject") — find the actual call sites by grepping `list_objects` / `delete_object` in mirror.rs and wrap each send.
- Byte-stream leaves keep their existing inline permits/bars — do NOT convert them to dispatch.
- Borrow note: `dispatch` takes the future by value; build the request first, pass `req.send()` (an `impl Future`) — e.g. `dispatch(budget, progress, label, "CreateMultipartUpload", create.send()).await?`. If a site's builder pattern makes that awkward, `async move { ... }` blocks are fine.

- [ ] Full suite green (incl. `cp_multipart_completes_with_p1` — control ops now take tokens, still strictly sequential relative to parts, so P=1 must complete) → fmt/clippy zero-new → commit `"feat: transfer control-plane ops dispatched as worker tasks"`.

---

### Task 3: standalone commands sweep

**Files:** Modify `client/src/main.rs` (+ any command module with S3 calls: grep `send().await` across `src/` — e.g. findcmd.rs, diff.rs, du/tree/ls helpers; skip share.rs/config.rs per spec, skip pipe).

For each standalone command that talks to S3 (ls, rm, stat, head, cat, du, tree, find, diff, mb, rb, mv's rm-half already covered via cp path):
- Create once per command invocation: `let ui = worker_ui();` where `worker_ui()` is a tiny new helper (put it in progress.rs) returning `Option<ProgressUi>` under the same predicate `TransferSession` uses (`stdout_tty && !quiet && !json && !no_color`); plus `let budget = crate::budget::StreamBudget::new(5);`.
- Route each S3 API call through `dispatch` with a sensible label: `Listing` bucket/prefix ("ListObjectsV2"/"ListBuckets"), `Removing` key ("DeleteObject"), `Inspecting` ("HeadObject"/"HeadBucket"), `Creating` ("CreateBucket"), `Removing` bucket ("DeleteBucket"), cat/head GET ("GetObject" — finish the task line when the response arrives, BEFORE streaming the body to stdout).
- Task lines must be finished before the command prints its stdout lines for that response (finish-then-print per page keeps stdout clean).
- Paginated loops: one task line per page request is correct (it shows repeated Listing pulses) — do not hold one line across the whole loop.

- [ ] Full suite green (non-TTY = no UI = byte-identical output) → fmt/clippy zero-new → commit `"feat: standalone commands dispatch S3 calls as worker tasks"`.

---

### Task 4: smoke + docs + gate

- [ ] README: extend the TTY-progress divergence entry: every S3 operation is a worker task consuming a `-P` token; byte-less ops show `<label> ⣹ <ApiName>` spinner lines; standalone commands show transient task lines on TTY; `--json`/`--quiet`/non-TTY unchanged (mc-matching).
- [ ] Manual TTY smoke (recipe as before: throwaway server, `script -qec` pty): multipart cp shows Creating → part bars → Completing lines; `ls` and `rm` on a TTY show transient spinners and their stdout output is uncorrupted and ordered; `-P 1` multipart cp completes live (token-per-control-op sanity beyond the e2e).
- [ ] Final gate: fmt --check, clippy zero-new, full `cargo test`. Commit docs.

## Self-review notes (applied)

- Deadlock: every dispatch is acquire→run→release with no nesting; parts and control ops are strictly sequential within an object's lifecycle; P=1 e2e is the regression net.
- Non-TTY: `worker_ui()` returns None; `dispatch` then uses a noop handle; token accounting still applies (harmless for sequential standalone ops).
- The spec's "clear before print" is realized as finish-then-print per page/response.
