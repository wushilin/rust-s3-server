# rs3 Stream Budget + Labeled Fixed-Layout Bars Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `-P` a global concurrent-stream budget shared between objects and segments (allocator/token pattern), and give every progress bar a verbed, self-condensing label in a fixed-column layout with `123.3/256MiB`-style byte counts.

**Architecture:** New `client/src/budget.rs` wraps a tokio `Semaphore`; exactly the leaf transfer operations in `transfer.rs` acquire one permit each (orchestration never holds one — the deadlock rule). `progress.rs` gains a structured `TransferLabel` (verb + path + part) with a pure middle-ellipsis `render`, a fixed-width label column, and a custom `{bytes_pair}` indicatif key.

**Tech Stack:** Rust (edition 2024), tokio `sync` feature (`Semaphore`/`OwnedSemaphorePermit`), indicatif 0.17 (`ProgressStyle::with_key`).

**Spec:** `docs/superpowers/specs/2026-07-31-rs3-stream-budget-labels-design.md` — decisions are locked.

## Global Constraints

- Non-TTY / `--json` / `--quiet` output stays **byte-identical**: all 186 existing tests pass unchanged. The budget changes scheduling only, never printed output.
- **Leaf-only permits:** one `StreamPermit` per in-flight stream (part upload, ranged/single GET, single PUT, cross-endpoint part pipeline = ONE permit, server-side part copy, single CopyObject). HEAD / CreateMultipartUpload / CompleteMultipartUpload / mirror planning / deletes NEVER hold a permit.
- `pipe` takes no permits and no labels (spec non-goal).
- Label column fixed at 40 chars; bar fixed at 30; condensing preserves verb, filename tail, and ` part i/n` suffix; trimming the filename is last resort.
- Bytes column: shared unit printed once, chosen from the total: `123.3/256MiB`.
- Clippy gate: zero NEW findings vs main (~24 pre-existing client lints are NOT in scope). `cargo fmt` before every commit. Commands run from `/home/code/workspace/rust-s3-server/client`.
- Server crate (repo root `src/`) untouched.
- Commit trailer:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01EsC8WkurPSRq13FeKyfA5W`

## File Structure

- Create: `client/src/budget.rs` — `StreamBudget`, `StreamPermit`, tests.
- Modify: `client/src/progress.rs` — `Verb`, `TransferLabel`, `render`, `format_bytes_pair`, `{bytes_pair}` key, fixed templates, `unit()` signature.
- Modify: `client/src/transfer.rs` — budget threading + leaf acquisitions; structured labels at unit() call sites.
- Modify: `client/src/main.rs`, `client/src/mirror.rs` — create/pass `StreamBudget`; `mod budget;`.
- Modify: `client/tests/e2e_cp.rs` (or the closest cp e2e file) — `-P 1` multipart deadlock-regression test.
- Modify: `client/README.md` — `-P` semantics + label/bytes format in the TTY-progress divergence entry.

---

### Task 1: `budget.rs` — the token allocator

**Files:**
- Create: `client/src/budget.rs`
- Modify: `client/src/main.rs` (add `mod budget;`)

**Interfaces:**
- Produces (later tasks rely on exact signatures):
  - `StreamBudget: Clone`; `StreamBudget::new(permits: usize) -> StreamBudget` (clamps 0 → 1)
  - `StreamBudget::acquire(&self) -> impl Future<Output = StreamPermit>`
  - `StreamPermit` — RAII token, returned on drop.

- [ ] **Step 1: Write the failing tests** (create `budget.rs` with only the test module + `use` lines; add `mod budget;` to main.rs)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn budget_caps_max_observed_concurrency() {
        let budget = StreamBudget::new(4);
        let current = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let budget = budget.clone();
            let current = current.clone();
            let peak = peak.clone();
            handles.push(tokio::spawn(async move {
                let _permit = budget.acquire().await;
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                current.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert!(peak.load(Ordering::SeqCst) <= 4, "peak {} > 4", peak.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn p1_serializes_and_completes() {
        // liveness regression for the leaf-only rule: P=1 must never deadlock
        let budget = StreamBudget::new(1);
        let done = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..20 {
            let budget = budget.clone();
            let done = done.clone();
            handles.push(tokio::spawn(async move {
                let _permit = budget.acquire().await;
                done.fetch_add(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(done.load(Ordering::SeqCst), 20);
    }

    #[tokio::test]
    async fn zero_permits_clamps_to_one() {
        let budget = StreamBudget::new(0);
        let _permit = budget.acquire().await; // must not hang or panic
    }
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test budget:: 2>&1 | tail -5` → compile FAILURE (`StreamBudget` undefined).

- [ ] **Step 3: Implement**

```rust
//! Global concurrent-stream budget for transfer commands: `-P` permits,
//! cooperatively shared between objects and segments (allocator/token
//! pattern). Exactly the leaf operations that move one stream hold one
//! [`StreamPermit`] for the stream's duration; orchestration (HEAD,
//! create/complete-multipart, planning, deletes) never holds one — nested
//! acquisition could deadlock once P objects each held a permit and
//! waited for segment permits. tokio's semaphore is FIFO, so a large
//! file's many parts queue fairly against other files' work.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub(crate) struct StreamBudget {
    sem: Arc<Semaphore>,
}

impl StreamBudget {
    pub(crate) fn new(permits: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(permits.max(1))),
        }
    }

    pub(crate) async fn acquire(&self) -> StreamPermit {
        StreamPermit {
            _permit: self
                .sem
                .clone()
                .acquire_owned()
                .await
                .expect("stream-budget semaphore never closes"),
        }
    }
}

/// RAII token: the stream slot is returned when this drops.
pub(crate) struct StreamPermit {
    _permit: OwnedSemaphorePermit,
}
```

Check `client/Cargo.toml`: tokio needs the `sync` feature — add it to the existing feature list if absent (`"sync"` alongside `"fs", "io-std", ...`).

- [ ] **Step 4: Verify pass** — `cargo test budget:: 2>&1 | tail -5` → 3 passed.
- [ ] **Step 5: fmt, clippy (no new findings), commit** — `git add src/budget.rs src/main.rs Cargo.toml && git commit -m "feat: StreamBudget token allocator for -P stream cap"`

---

### Task 2: Thread the budget through transfer leaves

**Files:**
- Modify: `client/src/transfer.rs` (all leaf sites), `client/src/main.rs` (put/get/cp call sites + `run_mirror` args), `client/src/mirror.rs` (`copy_entry` + caller)
- Test: add to the e2e file that covers multipart cp (find with `grep -l "part_size\|part-size" client/tests/e2e_*.rs`)

**Interfaces:**
- Consumes: Task 1's `StreamBudget`/`acquire`.
- Produces: every transfer fn that today takes `progress: Option<&ProgressUi>` gains a preceding-or-following `budget: &StreamBudget` parameter (pick one position and use it consistently — recommended: right before `progress`): `upload_file`, `multipart_upload`, `download_key_to_path`, `download_to_temp`, `download_object`, `transfer_object_between_s3`, `multipart_copy_s3_to_s3`, `multipart_server_side_copy`, and mirror's `copy_entry`.

- [ ] **Step 1: Wire creation.** In each command fn that creates a `TransferSession` for transfers (put, get/`download_object` caller, cp variants, mirror runner in main.rs), create `let stream_budget = crate::budget::StreamBudget::new(args.parallel);` and pass `&stream_budget` down. mirror.rs's runner receives it from main.rs (or constructs from its own `parallel` arg — match how `parallel` currently reaches it) and forwards into `copy_entry`.

- [ ] **Step 2: Leaf acquisitions in transfer.rs** — the permit variable is `let _permit = budget.acquire().await;` placed so it is held for the stream duration and dropped at scope end:
  - `upload_file` single-shot branch: acquire immediately before building the request body (so file open + PUT happen under the permit); the permit drops after `req.send().await?`.
  - `multipart_upload`: clone the budget before `stream::iter` (`let budget = budget.clone();` then per-closure clone like `progress`); INSIDE each part future, acquire as the FIRST await, before `ByteStream::read_from`.
  - `download_to_temp` small path: acquire before `get_object(...).send()`.
  - `download_to_temp` ranged path: inside each part future, first await.
  - `transfer_object_between_s3` same-endpoint single CopyObject: acquire before `.send()`.
  - Cross-endpoint single-shot: ONE acquire before the GET, held through the PUT (one pipeline = one permit).
  - `multipart_copy_s3_to_s3`: inside each part future, first await (covers GET + UploadPart).
  - `multipart_server_side_copy`: inside each part future, before `upload_part_copy(...).send()`.
  - NO acquisition in: HEAD calls, create/complete/abort multipart, mirror planning/deletes, `upload_stream` (pipe).
  - **Ordering rule (user-mandated legibility):** in every leaf, acquire the permit FIRST, and only then create the progress unit — a queued part must show no bar while waiting for a token, so the visible bar count always equals the live parallelism. Where a unit is currently created before the transfer call, move its creation to after the `acquire().await`.

- [ ] **Step 2b: Default `-P` becomes 5.** Change every `#[arg(short = 'P', long, default_value_t = 4)]` in main.rs to `default_value_t = 5` (grep for `default_value_t = 4` — put/cp/mirror and any other parallel args). If any e2e test asserts the old default, update it.

- [ ] **Step 3: e2e deadlock-regression test.** In the e2e file covering cp multipart, add (adapt helper names to the file's existing conventions — look at a neighboring multipart test and copy its setup):

```rust
#[test]
fn cp_multipart_completes_with_p1() {
    // -P 1 with leaf-only permits must serialize parts, never deadlock
    // upload: 12MiB file, 5MiB parts => 3 parts; then download it back
    // and byte-compare. Uses the same TestServer + rs3 invocation helpers
    // as the surrounding multipart tests, adding: -s 5MiB -P 1
}
```

The test body must actually run upload + download with `-P 1 -s 5MiB` on a ~12MiB random file and assert the round-tripped bytes are identical, using this file's existing harness helpers. Give it the standard e2e timeout behavior of its neighbors — if a deadlock regressed, the test hangs and the harness timeout fails it.

- [ ] **Step 4: Full suite** — `cargo test 2>&1 | tail -5` → everything green (186 + new).
- [ ] **Step 5: fmt, clippy (no new findings; add `#[allow(clippy::too_many_arguments)]` where the new param pushes over, matching branch precedent), commit** — `"feat: -P is a global stream budget (leaf-only permits)"`

---

### Task 3: Structured labels with condensing render

**Files:**
- Modify: `client/src/progress.rs` (Verb, TransferLabel, render + tests; `unit()` signature), `client/src/transfer.rs` (construct labels at every `unit(` call site)

**Interfaces:**
- Consumes: existing `ProgressUi::unit`.
- Produces:
  - `enum Verb { Uploading, Downloading, Copying }` (Copy, Clone)
  - `struct TransferLabel { verb: Verb, path: String, part: Option<(u64, u64)> }`
  - `TransferLabel::render(&self, width: usize) -> String` (pure)
  - `ProgressUi::unit(&self, label: TransferLabel, len: u64) -> UnitHandle` (signature change — all call sites updated in this task)
  - `pub(crate) const LABEL_WIDTH: usize = 40;`

- [ ] **Step 1: Failing tests** (append to progress.rs tests):

```rust
fn lbl(verb: Verb, path: &str, part: Option<(u64, u64)>) -> TransferLabel {
    TransferLabel { verb, path: path.into(), part }
}

#[test]
fn label_renders_full_when_it_fits() {
    assert_eq!(
        lbl(Verb::Uploading, "asdf/a.img", Some((4, 24))).render(40),
        "Uploading asdf/a.img part 4/24"
    );
    assert_eq!(lbl(Verb::Copying, "pics/x.jpg", None).render(40), "Copying pics/x.jpg");
}

#[test]
fn label_condenses_middle_components_first() {
    // full form is 47 chars: "Downloading backups/2026/07/31/big.iso part 1/8"
    let l = lbl(Verb::Downloading, "backups/2026/07/31/big.iso", Some((1, 8)));
    assert_eq!(l.render(40), "Downloading backups/…/big.iso part 1/8");
}

#[test]
fn label_drops_to_ellipsis_slash_tail_then_trims_filename() {
    let l = lbl(Verb::Uploading, "averyveryverylongdirectoryname/file.bin", Some((2, 9)));
    // "Uploading averyveryverylongdirectoryname/file.bin part 2/9" (58) -> first/…/tail == …/tail here
    assert_eq!(l.render(30), "Uploading …/file.bin part 2/9");
    // width too small even for the tail: trim filename from the left, keep verb + suffix
    let tight = lbl(Verb::Uploading, "d/really-long-filename-here.bin", Some((2, 9)));
    let out = tight.render(24);
    assert!(out.len() <= 24, "{out:?} too wide");
    assert!(out.starts_with("Uploading …"), "{out:?}");
    assert!(out.ends_with(" part 2/9"), "{out:?}");
}
```

(Character counts: `…` counts as ONE char — measure with `chars().count()`, not `len()`. If an exact expected string in these tests is off-by-one against your implementation's correct arithmetic, recount by hand and fix the TEST — the invariants are: fits within width, middle-components drop first, verb + tail + part suffix survive.)

- [ ] **Step 2: Verify compile failure.** `cargo test progress:: 2>&1 | tail -5`

- [ ] **Step 3: Implement** in progress.rs:

```rust
pub(crate) const LABEL_WIDTH: usize = 40;

#[derive(Clone, Copy)]
pub(crate) enum Verb {
    Uploading,
    Downloading,
    Copying,
}

impl Verb {
    fn as_str(self) -> &'static str {
        match self {
            Verb::Uploading => "Uploading",
            Verb::Downloading => "Downloading",
            Verb::Copying => "Copying",
        }
    }
}

pub(crate) struct TransferLabel {
    pub(crate) verb: Verb,
    pub(crate) path: String,
    pub(crate) part: Option<(u64, u64)>,
}

impl TransferLabel {
    /// Condense to `width` chars: full text first; then drop middle path
    /// components (`a/…/z`); then `…/z`; last resort trim the filename
    /// from the left. Verb and ` part i/n` suffix always survive.
    pub(crate) fn render(&self, width: usize) -> String {
        let verb = self.verb.as_str();
        let suffix = match self.part {
            Some((i, n)) => format!(" part {i}/{n}"),
            None => String::new(),
        };
        let fit = |path: &str| -> Option<String> {
            let s = format!("{verb} {path}{suffix}");
            (s.chars().count() <= width).then_some(s)
        };
        if let Some(s) = fit(&self.path) {
            return s;
        }
        let parts: Vec<&str> = self.path.split('/').collect();
        if parts.len() > 2 {
            if let Some(s) = fit(&format!("{}/…/{}", parts[0], parts[parts.len() - 1])) {
                return s;
            }
        }
        let tail = parts[parts.len() - 1];
        if let Some(s) = fit(&format!("…/{tail}")) {
            return s;
        }
        // trim the filename from the left to whatever room remains
        let overhead = format!("{verb} …{suffix}").chars().count();
        let room = width.saturating_sub(overhead);
        let kept: String = tail
            .chars()
            .rev()
            .take(room)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("{verb} …{kept}{suffix}")
    }
}
```

Change `ProgressUi::unit` to take `TransferLabel`, render at `LABEL_WIDTH`, left-pad to exactly `LABEL_WIDTH` (`format!("{rendered:<LABEL_WIDTH$}")` equivalent via `format!("{:<1$}", rendered, LABEL_WIDTH)` — note: pad by chars; simple format-padding by bytes is acceptable here since rendered is ASCII except `…`, so pad with `LABEL_WIDTH + (rendered.len() - rendered.chars().count())` — or just push spaces in a loop until `chars().count() == LABEL_WIDTH`). Update ALL existing `unit(` call sites in transfer.rs to build `TransferLabel`s:
  - `upload_file` single-shot: `Verb::Uploading`, path = `source.display().to_string()` (the path as the user gave it), part `None`.
  - `multipart_upload` parts: same path, `Some((part_index, part_count))`.
  - `download_to_temp` small/ranged: `Verb::Downloading`, path = `key` (full key, not just last component), part `None` / `Some((part_index + 1, part_count))`.
  - s3→s3 all four branches: `Verb::Copying`, path = `source_key`, per-part `Some((part_index, part_count))` where applicable.
  - Existing in-module tests that call `unit("...".into(), n)` switch to `lbl`-style construction.

- [ ] **Step 4: Full suite green; Step 5: fmt/clippy/commit** — `"feat: verbed self-condensing bar labels (TransferLabel)"`

---

### Task 4: Fixed layout + `{bytes_pair}` column

**Files:**
- Modify: `client/src/progress.rs` (templates + formatter + tests)

**Interfaces:**
- Produces: `pub(crate) fn format_bytes_pair(pos: u64, len: u64) -> String`; detail + TOTAL templates use `{bytes_pair}` via `ProgressStyle::with_key`.

- [ ] **Step 1: Failing tests:**

```rust
#[test]
fn bytes_pair_shares_unit_from_total() {
    assert_eq!(format_bytes_pair(129_248_805, 268_435_456), "123.3/256MiB");
    assert_eq!(format_bytes_pair(0, 268_435_456), "0.0/256MiB");
    assert_eq!(format_bytes_pair(268_435_456, 268_435_456), "256.0/256MiB");
    assert_eq!(format_bytes_pair(5_400_000, 16_777_216), "5.1/16MiB");
    assert_eq!(format_bytes_pair(512, 1000), "512/1000B");
    assert_eq!(format_bytes_pair(0, 0), "0/0B");
    // total with a fractional unit value keeps one decimal
    assert_eq!(format_bytes_pair(1_048_576, 1_572_864), "1.0/1.5MiB");
}
```

- [ ] **Step 2: Verify failure. Step 3: Implement:**

```rust
const UNITS: [(&str, u64); 5] = [
    ("TiB", 1 << 40),
    ("GiB", 1 << 30),
    ("MiB", 1 << 20),
    ("KiB", 1 << 10),
    ("B", 1),
];

/// `123.3/256MiB`: one shared unit, chosen from the total, printed once.
/// Transferred keeps one decimal; the total drops a trailing `.0`; the
/// bytes unit uses plain integers.
pub(crate) fn format_bytes_pair(pos: u64, len: u64) -> String {
    let (unit, div) = UNITS
        .iter()
        .find(|(_, div)| len >= *div)
        .copied()
        .unwrap_or(("B", 1));
    if div == 1 {
        return format!("{pos}/{len}{unit}");
    }
    let scale = |v: u64| v as f64 / div as f64;
    let total = scale(len);
    let total_s = if (total.fract()).abs() < 0.05 {
        format!("{total:.0}")
    } else {
        format!("{total:.1}")
    };
    format!("{:.1}/{}{}", scale(pos), total_s, unit)
}
```

Wire templates: detail bars `"{msg} [{bar:30.cyan/240}] {bytes_pair} {binary_bytes_per_sec}"`, TOTAL `"TOTAL {msg} [{bar:30.green/240}] {bytes_pair} {binary_bytes_per_sec} eta {eta}"`, each style getting

```rust
.with_key("bytes_pair", |state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
    let _ = w.write_str(&format_bytes_pair(state.pos(), state.len().unwrap_or(0)));
})
```

(If the closure signature differs in indicatif 0.17, check the vendored source for `ProgressState`/`TemplateError` details and adapt — the rendered output contract is what the tests pin.)

- [ ] **Step 4: All progress tests + full suite green. Step 5: fmt/clippy/commit** — `"feat: fixed bar layout with shared-unit bytes_pair column"`

---

### Task 5: Docs, TTY smoke, final gate

**Files:**
- Modify: `client/README.md`

- [ ] **Step 1: README.** In the TTY-progress divergence entry: `-P` is now a *global* concurrent-stream budget cooperatively shared between objects and segments (e.g. `-P 5` = 5 small files, or 2 files with segments sharing 5 tokens), **default 5** (changed from 4); a bar is visible exactly while its stream holds a token, so bar count = live parallelism; labels read `Uploading asdf/a.img part 4/24` with middle-ellipsis condensing; byte column reads `123.3/256MiB`. Note the intentional reduction from the old per-layer P×P worst case. Update any other README mention of the `-P` default (grep for `-P 4`/`default.*4`).
- [ ] **Step 2: Manual TTY smoke** (crib the recipe from the previous smoke in git history / e2e harness: build server + client release, boot throwaway server, `script -qec` pty, 1.5GB file):
  - `-s 64MiB -P 4` cp up + down: verify verbed labels, aligned fixed columns, `x.x/64MiB` byte pairs, ≤4 part bars ever active (budget!), TOTAL bar consistent.
  - a deep path (`mkdir -p a/b/c/d/` + long names) to see condensing in a live frame.
  - mirror of 2 large files with `-P 4`: total active part bars across BOTH files ≤ 4 at any frame (the cooperative-share proof), not 8.
  - roundtrip `cmp` must pass; cleanup temp dirs + server data; `git status` clean.
  Record concrete evidence (frames, grep counts) in the task report.
- [ ] **Step 3: Final gate** — `cargo fmt --check`, `cargo clippy --all-targets` (zero new vs main), full `cargo test`. Commit README — `"docs: -P stream-budget semantics and labeled-bar format"`

---

## Self-review notes (applied)

- Spec coverage: §1→Tasks 1-2, §2→Task 3, §3→Task 4, §4→Task 5, §5 tests→each task + Task 2's e2e + Task 5 smoke.
- Type consistency: `budget: &StreamBudget` positioned before `progress: Option<&ProgressUi>` in every signature; `unit(TransferLabel, u64)`; `LABEL_WIDTH = 40`.
- The label-render test strings were hand-counted; the tests themselves state the governing invariants in case of an off-by-one, with explicit permission to correct the expected string rather than contort the algorithm.
