# rs3 Multi-Bar Live Progress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Live byte-level progress for cp/mv/put/get/mirror: up to 10 stacked per-unit bars (one per in-flight multipart segment or small file) above a persistent overall bar, replacing today's bar that only ticks when a whole object completes.

**Architecture:** New `client/src/progress.rs` owns a `MultiProgress` pool (`ProgressUi` + `UnitHandle`). `TransferSession` (messages.rs) holds `Option<ProgressUi>` under the existing TTY predicate. Transfer functions in `transfer.rs` accept `Option<&ProgressUi>`, register each object's size the moment it is known, and tick bytes: downloads tick in their read loops; uploads wrap the request `SdkBody` with a `ProgressBody` that rewinds on retry; server-side copy ticks full segments on part completion.

**Tech Stack:** Rust (edition 2024), tokio, aws-sdk-s3, indicatif 0.17 (`MultiProgress`), aws-smithy-types `SdkBody::map`/`http-body-1-x`, pin-project-lite.

**Spec:** `docs/superpowers/specs/2026-07-31-rs3-progress-ui-design.md` — read it first; its decisions are locked.

## Global Constraints

- Non-TTY / `--json` / `--quiet` output must stay **byte-identical**: all 172 existing tests pass unchanged. The bar UI activates only when `out().stdout_tty && !out().quiet && !out().json && !out().no_color`.
- Never print bars to stdout. indicatif defaults to stderr — keep it.
- Detail-bar cap is exactly **10**; the 11th+ concurrent unit gets a silent handle (no bar, still ticks the overall bar). No waiting/promotion queue.
- No byte double-counting: in bar mode `TransferSession::object_done` must NOT `inc` bytes (bytes come only from `UnitHandle`s). `UnitHandle::finish()` tops up to the unit's full length exactly once.
- A retried upload part must not double-count: the retry body constructor calls `rewind()` first.
- The server crate at the repo root (`src/`) must not be touched.
- Rust edition 2024; run `cargo fmt` and `cargo clippy --all-targets -- -D warnings` inside `client/` before every commit.
- Commands in this plan run from `/home/code/workspace/rust-s3-server/client` unless stated otherwise.

## File Structure

- Create: `client/src/progress.rs` — `ProgressUi`, `UnitHandle`, `ProgressBody`, `instrument_body`. All indicatif/bar logic lives here.
- Modify: `client/Cargo.toml` — add `bytes`, `http-body`, `pin-project-lite`; add feature `http-body-1-x` to `aws-smithy-types`.
- Modify: `client/src/messages.rs` — `TransferSession` swaps its single `ProgressBar` for `Option<ProgressUi>`.
- Modify: `client/src/output.rs` — `no_color` is finally read; drop its `#[allow(dead_code)]`.
- Modify: `client/src/transfer.rs` — thread `Option<&ProgressUi>`, tick loops, wrap upload bodies.
- Modify: `client/src/main.rs`, `client/src/mirror.rs` — pass `session.ui()` through call sites.
- Modify: `client/README.md` — "Known divergences from mc" entry.

---

### Task 1: `progress.rs` core — bar pool and unit handles

**Files:**
- Create: `client/src/progress.rs`
- Modify: `client/src/main.rs` (add `mod progress;` next to the other `mod` lines near the top)

**Interfaces:**
- Consumes: nothing (self-contained; indicatif only).
- Produces (later tasks rely on these exact signatures):
  - `ProgressUi: Clone`; `ProgressUi::new() -> ProgressUi` (stderr), `ProgressUi::hidden() -> ProgressUi` (tests)
  - `ProgressUi::add_object(&self, bytes: u64)` — +1 object total, +bytes overall length
  - `ProgressUi::object_done(&self)` — +1 object done (display clamps total ≥ done)
  - `ProgressUi::unit(&self, label: String, len: u64) -> UnitHandle`
  - `ProgressUi::finish_and_keep(&self)` — remove detail bars, finish overall bar in place (stays visible)
  - `ProgressUi::overall_position(&self) -> u64`, `overall_length(&self) -> u64`, `active_detail_bars(&self) -> usize` (test accessors, also used by Task 3's tests)
  - `UnitHandle: Clone + Send + Sync`; `UnitHandle::noop() -> UnitHandle`; methods `inc(&self, n: u64)`, `rewind(&self)`, `finish(&self)`, `is_noop(&self) -> bool`

- [ ] **Step 1: Write the failing tests**

Create `client/src/progress.rs` containing only the test module for now (plus `use` lines), and add `mod progress;` to `main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eleventh_concurrent_unit_is_silent_but_still_counts() {
        let ui = ProgressUi::hidden();
        ui.add_object(11 * 100);
        let handles: Vec<UnitHandle> =
            (0..11).map(|i| ui.unit(format!("u{i}"), 100)).collect();
        assert_eq!(ui.active_detail_bars(), 10, "cap is 10");
        // the silent 11th unit still ticks the overall bar
        handles[10].inc(40);
        assert_eq!(ui.overall_position(), 40);
    }

    #[test]
    fn finish_frees_slot_for_next_unit() {
        let ui = ProgressUi::hidden();
        let handles: Vec<UnitHandle> =
            (0..10).map(|i| ui.unit(format!("u{i}"), 10)).collect();
        assert_eq!(ui.active_detail_bars(), 10);
        handles[0].finish();
        assert_eq!(ui.active_detail_bars(), 9);
        let _h = ui.unit("next".into(), 10);
        assert_eq!(ui.active_detail_bars(), 10);
    }

    #[test]
    fn finish_tops_up_to_len_exactly_once() {
        let ui = ProgressUi::hidden();
        ui.add_object(100);
        let h = ui.unit("f".into(), 100);
        h.inc(30);
        h.finish();
        assert_eq!(ui.overall_position(), 100, "topped up 30 -> 100");
        h.finish(); // idempotent
        assert_eq!(ui.overall_position(), 100);
    }

    #[test]
    fn rewind_subtracts_progress_for_retry() {
        let ui = ProgressUi::hidden();
        ui.add_object(100);
        let h = ui.unit("f".into(), 100);
        h.inc(40);
        assert_eq!(ui.overall_position(), 40);
        h.rewind();
        assert_eq!(ui.overall_position(), 0);
        h.inc(100);
        h.finish();
        assert_eq!(ui.overall_position(), 100);
    }

    #[test]
    fn drop_without_finish_frees_slot_but_does_not_top_up() {
        let ui = ProgressUi::hidden();
        ui.add_object(100);
        let h = ui.unit("f".into(), 100);
        h.inc(30);
        drop(h); // a failed part must not fake completion
        assert_eq!(ui.overall_position(), 30);
        assert_eq!(ui.active_detail_bars(), 0);
    }

    #[test]
    fn noop_handle_is_inert() {
        let h = UnitHandle::noop();
        assert!(h.is_noop());
        h.inc(5);
        h.rewind();
        h.finish(); // must not panic
    }

    #[test]
    fn add_object_grows_length_and_object_counts() {
        let ui = ProgressUi::hidden();
        ui.add_object(50);
        ui.add_object(70);
        assert_eq!(ui.overall_length(), 120);
        ui.object_done();
        ui.object_done();
        ui.object_done(); // mirror delete events: done may pass adds
        // must not panic; display clamps total >= done internally
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test progress:: 2>&1 | tail -20`
Expected: compile FAILURE — `ProgressUi` not defined.

- [ ] **Step 3: Implement `ProgressUi` and `UnitHandle`**

Above the test module in `progress.rs`:

```rust
//! Multi-bar live progress for transfer commands: up to
//! [`MAX_DETAIL_BARS`] per-unit bars (one multipart segment of a large
//! file, or one whole small file) stacked above a persistent overall bar.
//! Deliberate TTY-only divergence from mc's single aggregate bar — see
//! README "Known divergences from mc". All bars draw to stderr; stdout
//! (the mc-compat message contract) is never touched.

use std::sync::{Arc, Mutex};

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

const MAX_DETAIL_BARS: usize = 10;

struct UiState {
    objects_total: u64,
    objects_done: u64,
    active_bars: usize,
}

struct UiInner {
    mp: MultiProgress,
    overall: ProgressBar,
    state: Mutex<UiState>,
}

#[derive(Clone)]
pub(crate) struct ProgressUi {
    inner: Arc<UiInner>,
}

impl ProgressUi {
    pub(crate) fn new() -> Self {
        Self::with_target(ProgressDrawTarget::stderr())
    }

    /// Hidden draw target for unit tests: full accounting, no rendering.
    #[cfg(test)]
    pub(crate) fn hidden() -> Self {
        Self::with_target(ProgressDrawTarget::hidden())
    }

    fn with_target(target: ProgressDrawTarget) -> Self {
        let mp = MultiProgress::with_draw_target(target);
        let overall = mp.add(ProgressBar::new(0));
        if let Ok(style) = ProgressStyle::with_template(
            "TOTAL {msg} [{bar:30.green/240}] {bytes}/{total_bytes} {bytes_per_sec} eta {eta}",
        ) {
            overall.set_style(style.progress_chars("=> "));
        }
        overall.set_message("0/0 objects");
        Self {
            inner: Arc::new(UiInner {
                mp,
                overall,
                state: Mutex::new(UiState {
                    objects_total: 0,
                    objects_done: 0,
                    active_bars: 0,
                }),
            }),
        }
    }

    pub(crate) fn add_object(&self, bytes: u64) {
        let mut state = self.lock_state();
        state.objects_total += 1;
        self.inner.overall.inc_length(bytes);
        self.refresh_msg(&state);
    }

    pub(crate) fn object_done(&self) {
        let mut state = self.lock_state();
        state.objects_done += 1;
        self.refresh_msg(&state);
    }

    /// One in-flight transfer unit. If all detail slots are busy the handle
    /// is silent: no bar, but its ticks still advance the overall bar.
    pub(crate) fn unit(&self, label: String, len: u64) -> UnitHandle {
        let mut state = self.lock_state();
        let bar = if state.active_bars < MAX_DETAIL_BARS {
            state.active_bars += 1;
            let pb = self
                .inner
                .mp
                .insert_before(&self.inner.overall, ProgressBar::new(len));
            if let Ok(style) = ProgressStyle::with_template(
                "{wide_msg} [{bar:30.cyan/240}] {bytes}/{total_bytes} {bytes_per_sec}",
            ) {
                pb.set_style(style.progress_chars("=> "));
            }
            pb.set_message(label);
            Some(pb)
        } else {
            None
        };
        UnitHandle {
            inner: Some(Arc::new(UnitInner {
                ui: self.clone(),
                bar,
                len,
                state: Mutex::new(UnitProgress {
                    pos: 0,
                    finished: false,
                }),
            })),
        }
    }

    /// Session end: detail bars are already gone (finished or dropped);
    /// the overall bar finishes in place and stays visible, like mc's.
    pub(crate) fn finish_and_keep(&self) {
        let state = self.lock_state();
        self.refresh_msg(&state);
        self.inner.overall.finish();
    }

    fn refresh_msg(&self, state: &UiState) {
        // Mirror `--remove` delete events complete objects that never went
        // through a transfer function's add_object — clamp so the display
        // never shows done > total.
        let total = state.objects_total.max(state.objects_done);
        self.inner
            .overall
            .set_message(format!("{}/{} objects", state.objects_done, total));
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, UiState> {
        self.inner.state.lock().expect("ProgressUi state poisoned")
    }

    pub(crate) fn overall_position(&self) -> u64 {
        self.inner.overall.position()
    }

    pub(crate) fn overall_length(&self) -> u64 {
        self.inner.overall.length().unwrap_or(0)
    }

    pub(crate) fn active_detail_bars(&self) -> usize {
        self.lock_state().active_bars
    }

    fn release_slot(&self, bar: &Option<ProgressBar>) {
        if let Some(pb) = bar {
            pb.finish_and_clear();
            self.inner.mp.remove(pb);
            let mut state = self.lock_state();
            state.active_bars = state.active_bars.saturating_sub(1);
        }
    }
}

struct UnitProgress {
    pos: u64,
    finished: bool,
}

struct UnitInner {
    ui: ProgressUi,
    bar: Option<ProgressBar>,
    len: u64,
    state: Mutex<UnitProgress>,
}

/// Cheap-clone handle for one transfer unit; safe to tick from concurrent
/// futures and from inside a retryable-body closure.
#[derive(Clone)]
pub(crate) struct UnitHandle {
    inner: Option<Arc<UnitInner>>,
}

impl UnitHandle {
    /// Inert handle for when progress is disabled (non-TTY/--json/--quiet).
    pub(crate) fn noop() -> Self {
        Self { inner: None }
    }

    pub(crate) fn is_noop(&self) -> bool {
        self.inner.is_none()
    }

    pub(crate) fn inc(&self, n: u64) {
        let Some(inner) = &self.inner else { return };
        let mut state = inner.state.lock().expect("UnitHandle state poisoned");
        if state.finished {
            return;
        }
        state.pos += n;
        if let Some(bar) = &inner.bar {
            bar.inc(n);
        }
        inner.ui.inner.overall.inc(n);
    }

    /// Reset to the unit's start (an upload part retry re-streams from
    /// offset 0) — subtracts already-counted bytes from the overall bar.
    pub(crate) fn rewind(&self) {
        let Some(inner) = &self.inner else { return };
        let mut state = inner.state.lock().expect("UnitHandle state poisoned");
        if state.finished {
            return;
        }
        let pos = std::mem::take(&mut state.pos);
        if let Some(bar) = &inner.bar {
            bar.set_position(0);
        }
        let overall = &inner.ui.inner.overall;
        overall.set_position(overall.position().saturating_sub(pos));
    }

    /// Snap to 100% (top up any rounding shortfall exactly once), remove
    /// the bar, free the slot. Idempotent.
    pub(crate) fn finish(&self) {
        let Some(inner) = &self.inner else { return };
        let mut state = inner.state.lock().expect("UnitHandle state poisoned");
        if state.finished {
            return;
        }
        state.finished = true;
        let shortfall = inner.len.saturating_sub(state.pos);
        state.pos = inner.len;
        drop(state);
        inner.ui.inner.overall.inc(shortfall);
        inner.ui.release_slot(&inner.bar);
    }
}

impl Drop for UnitInner {
    /// A dropped-unfinished unit (failed part) frees its slot but must not
    /// fake completion by topping up bytes.
    fn drop(&mut self) {
        let finished = self
            .state
            .lock()
            .map(|s| s.finished)
            .unwrap_or(true);
        if !finished {
            self.ui.release_slot(&self.bar);
        }
    }
}
```

Note `release_slot` is called with `finished` already set in `finish()`, so `Drop` won't double-release (bar `finish_and_clear` is idempotent, and the slot count is only decremented in `release_slot`, reached exactly once per unit: either via `finish` or via `Drop`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test progress:: 2>&1 | tail -20`
Expected: 7 passed.

- [ ] **Step 5: fmt, clippy, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/progress.rs src/main.rs
git commit -m "feat: progress.rs multi-bar pool (ProgressUi/UnitHandle, cap 10)"
```

---

### Task 2: `TransferSession` swaps to `ProgressUi`

**Files:**
- Modify: `client/src/messages.rs:446-557` (`SessionState`/`TransferSession`)
- Modify: `client/src/output.rs:20-21` (drop `#[allow(dead_code)]` on `no_color`)

**Interfaces:**
- Consumes: Task 1's `ProgressUi` (`new`, `object_done`, `finish_and_keep`).
- Produces: `TransferSession::ui(&self) -> Option<&ProgressUi>` — later tasks pass this into transfer functions. All other `TransferSession` methods keep their exact signatures (`new(&str)`, `add_total(u64)`, `totals() -> (u64, u64)`, `object_done(&dyn McMessage, u64)`, `finish()`).

- [ ] **Step 1: Write the failing test**

Append to `messages.rs`'s existing `#[cfg(test)] mod tests` (create one at the bottom of the file if none exists — check first with `grep -n "mod tests" src/messages.rs`):

```rust
#[test]
fn session_has_no_ui_outside_tty() {
    // out() falls back to non-TTY defaults in unit tests, so the bar UI
    // must be off and the message/AccountStat path must be taken.
    let session = TransferSession::new("cp");
    assert!(session.ui().is_none());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test session_has_no_ui -- --nocapture 2>&1 | tail -5`
Expected: compile FAILURE — no method `ui`.

- [ ] **Step 3: Swap the engine**

In `messages.rs`:
1. Replace field `bar: Option<ProgressBar>` with `ui: Option<crate::progress::ProgressUi>` and delete the now-unused `use indicatif::{ProgressBar, ProgressStyle};` import.
2. `TransferSession::new`: predicate gains `no_color`:

```rust
pub(crate) fn new(_label: &str) -> Self {
    let use_bar =
        out().stdout_tty && !out().quiet && !out().json && !out().no_color;
    Self {
        ui: use_bar.then(crate::progress::ProgressUi::new),
        state: Mutex::new(SessionState::default()),
        started: Instant::now(),
    }
}

pub(crate) fn ui(&self) -> Option<&crate::progress::ProgressUi> {
    self.ui.as_ref()
}
```

3. `add_total`: keep the Mutex bookkeeping, **delete** the `bar.inc_length(bytes)` arm entirely (the overall bar's length now comes from `ProgressUi::add_object` inside the transfer functions, registered *before* bytes tick — see spec §1/§2).
4. `object_done`: keep the Mutex bookkeeping; replace the match:

```rust
match &self.ui {
    // bytes come exclusively from UnitHandle ticks — counting them here
    // too would double-count (spec §2)
    Some(ui) => ui.object_done(),
    None => print_msg(msg),
}
```

5. `finish`: replace the bar arm:

```rust
if let Some(ui) = &self.ui {
    ui.finish_and_keep();
    return;
}
```

(The `AccountStat` non-bar path is untouched.)

6. In `output.rs`, remove the `#[allow(dead_code)]` attribute above `pub no_color: bool` and the stale module-doc sentence about it being unread (lines 10-11).

- [ ] **Step 4: Run the full suite**

Run: `cargo test 2>&1 | tail -5`
Expected: all tests pass (172 + the new ones). The e2e harness is non-TTY, so nothing observable changed there.

- [ ] **Step 5: fmt, clippy, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/messages.rs src/output.rs
git commit -m "feat: TransferSession drives ProgressUi; --no-color disables bars"
```

---

### Task 3: `ProgressBody` — retry-safe upload byte ticks

**Files:**
- Modify: `client/Cargo.toml`
- Modify: `client/src/progress.rs` (add `ProgressBody` + `instrument_body`)
- Modify: `client/src/transfer.rs:129-304` (`upload_file`, `multipart_upload`)
- Modify call sites: `client/src/main.rs:1178` (put), `client/src/main.rs:1356` (cp local→s3), `client/src/mirror.rs:564` (transfer_one upload arm)

**Interfaces:**
- Consumes: Task 1's `UnitHandle` (`inc`, `rewind`, `finish`, `is_noop`, `noop`), Task 2's `session.ui()`.
- Produces:
  - `progress::instrument_body(body: ByteStream, unit: &UnitHandle) -> ByteStream`
  - `upload_file(source, target, part_size, parallel, disable_multipart, storage_class, metadata, if_not_exists, preserve, progress: Option<&crate::progress::ProgressUi>) -> Result<UploadOutcome>` (new last param)
  - `multipart_upload(client, source, bucket, key, total_size, part_size, parallel, storage_class, metadata, if_not_exists, progress: Option<&crate::progress::ProgressUi>) -> Result<()>` (new last param)

- [ ] **Step 1: Add dependencies**

In `client/Cargo.toml` `[dependencies]` (keep alphabetical order):

```toml
aws-smithy-types = { version = "1", features = ["http-body-1-x"] }
bytes = "1"
http-body = "1"
pin-project-lite = "0.2"
```

(`aws-smithy-types` is already listed — add the feature to the existing line rather than duplicating it.)

Run: `cargo check 2>&1 | tail -3` — must still compile.

- [ ] **Step 2: Write the failing tests**

Append to `progress.rs`'s test module:

```rust
#[tokio::test]
async fn progress_body_ticks_exact_len() {
    use aws_smithy_types::body::SdkBody;
    use aws_smithy_types::byte_stream::ByteStream;

    let ui = ProgressUi::hidden();
    ui.add_object(10);
    let unit = ui.unit("mem".into(), 10);
    let body = ByteStream::new(SdkBody::from("0123456789"));
    let wrapped = instrument_body(body, &unit);
    let data = wrapped.collect().await.expect("collect").into_bytes();
    assert_eq!(&data[..], b"0123456789");
    assert_eq!(ui.overall_position(), 10);
}

#[tokio::test]
async fn progress_body_retry_rewinds_instead_of_double_counting() {
    use aws_smithy_types::body::SdkBody;
    use aws_smithy_types::byte_stream::ByteStream;

    let ui = ProgressUi::hidden();
    ui.add_object(10);
    let unit = ui.unit("mem".into(), 10);
    let retryable = SdkBody::retryable(|| SdkBody::from("0123456789"));
    let wrapped = instrument_body(ByteStream::new(retryable), &unit);
    // The SDK's orchestrator clones a retryable body once per attempt;
    // each clone re-applies the map, whose closure rewinds first.
    let inner = wrapped.into_inner();
    let attempt1 = inner.try_clone().expect("retryable clone");
    let d1 = ByteStream::new(attempt1).collect().await.unwrap().into_bytes();
    assert_eq!(d1.len(), 10);
    assert_eq!(ui.overall_position(), 10, "first attempt counted");
    let attempt2 = inner.try_clone().expect("retryable clone");
    let d2 = ByteStream::new(attempt2).collect().await.unwrap().into_bytes();
    assert_eq!(d2.len(), 10);
    assert_eq!(ui.overall_position(), 10, "retry rewound: 10, not 20");
}

#[tokio::test]
async fn instrument_body_noop_passes_through() {
    use aws_smithy_types::body::SdkBody;
    use aws_smithy_types::byte_stream::ByteStream;

    let body = ByteStream::new(SdkBody::from("abc"));
    let wrapped = instrument_body(body, &UnitHandle::noop());
    let data = wrapped.collect().await.expect("collect").into_bytes();
    assert_eq!(&data[..], b"abc");
}
```

**Note to implementer:** the retry test drives clones the way the SDK's orchestrator does (`try_clone` per attempt). If `SdkBody::map`'s closure turns out not to be re-applied on `try_clone` in the pinned smithy version, ticks will read 20 and the test fails — in that case switch `instrument_body` to build the whole body via `SdkBody::retryable` yourself (closure body: `unit.rewind(); SdkBody::from_body_1_x(ProgressBody { inner: make_inner(), unit: unit.clone() })`) and adapt the callers. The assertion (10, not 20) is the contract; the mechanism may flex.

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test progress_body 2>&1 | tail -10`
Expected: compile FAILURE — `instrument_body` not defined.

- [ ] **Step 4: Implement `ProgressBody` + `instrument_body`**

In `progress.rs` (above the test module):

```rust
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;

pin_project_lite::pin_project! {
    /// Ticks a [`UnitHandle`] as each data frame is polled off the wire.
    struct ProgressBody {
        #[pin]
        inner: SdkBody,
        unit: UnitHandle,
    }
}

impl http_body::Body for ProgressBody {
    type Data = bytes::Bytes;
    type Error = aws_smithy_types::body::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let poll = this.inner.poll_frame(cx);
        if let std::task::Poll::Ready(Some(Ok(frame))) = &poll {
            if let Some(data) = frame.data_ref() {
                this.unit.inc(data.len() as u64);
            }
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Wraps an upload body so every chunk sent ticks `unit`. Applied through
/// `SdkBody::map`, which re-applies on each retry attempt's clone — the
/// closure rewinds first so a retried part never double-counts. No-op for
/// noop handles (progress disabled).
pub(crate) fn instrument_body(body: ByteStream, unit: &UnitHandle) -> ByteStream {
    if unit.is_noop() {
        return body;
    }
    let unit = unit.clone();
    ByteStream::new(body.into_inner().map(move |inner| {
        unit.rewind();
        SdkBody::from_body_1_x(ProgressBody {
            inner,
            unit: unit.clone(),
        })
    }))
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test progress 2>&1 | tail -10`
Expected: all progress tests pass (including Task 1's).

- [ ] **Step 6: Wire into uploads**

In `transfer.rs`:

1. `upload_file` gains final param `progress: Option<&crate::progress::ProgressUi>`. After `let file_meta = …` (line ~160) add:

```rust
if let Some(ui) = progress {
    ui.add_object(file_meta.len());
}
```

2. Single-shot branch (line ~172): create a whole-file unit and wrap the body; finish on success:

```rust
let unit = match progress {
    Some(ui) => ui.unit(source_name.to_string(), file_meta.len()),
    None => crate::progress::UnitHandle::noop(),
};
let body = crate::progress::instrument_body(
    ByteStream::from_path(source).await?,
    &unit,
);
let mut req = client
    .put_object()
    .bucket(&bucket)
    .key(&key)
    .body(body);
// … existing storage_class / attrs / if_not_exists …
req.send().await?;
unit.finish();
```

3. `multipart_upload` gains final param `progress: Option<&crate::progress::ProgressUi>`; `upload_file` forwards it. Inside the per-part closure (line ~241), the future is `'static`, so derive the label and handle *outside* `async move` … actually the unit must be created lazily when the part actually starts (buffer_unordered), otherwise all N bars appear at once. Create it inside the future from a cloned `Option<ProgressUi>`:

Before `stream::iter`, add `let progress = progress.cloned();` and `let file_label = source.file_name().and_then(|s| s.to_str()).unwrap_or("upload").to_string();`. Inside each part's `async move` (clone `progress`/`file_label` per closure like the other captures):

```rust
let unit = match &progress {
    Some(ui) => ui.unit(
        format!("{file_label} part {part_index}/{part_count}"),
        len,
    ),
    None => crate::progress::UnitHandle::noop(),
};
let body = crate::progress::instrument_body(
    ByteStream::read_from()
        .path(source)
        .offset(offset)
        .length(Length::Exact(len))
        .build()
        .await?,
    &unit,
);
// … existing upload_part().body(body).send().await? …
unit.finish();
```

(`unit.finish()` goes right after the successful `send`; on error the future returns early and `Drop` frees the slot without topping up.)

4. Update every caller of `upload_file`/`multipart_upload`:
   - `main.rs:1178` (put): pass `session.ui()`.
   - `main.rs:1356` (cp local→s3 loop): pass `session.ui()`.
   - `mirror.rs:564` (upload arm of `transfer_one`): `transfer_one` gains a `progress: Option<&crate::progress::ProgressUi>` parameter; its caller in `mirror.rs` (~line 379 region) passes `session.ui()`. Forward into `upload_file`.

- [ ] **Step 7: Full suite**

Run: `cargo test 2>&1 | tail -5`
Expected: all pass — non-TTY e2e sees `progress = None` (`session.ui()` is `None` off-TTY) and noop handles everywhere.

- [ ] **Step 8: fmt, clippy, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add Cargo.toml Cargo.lock src/progress.rs src/transfer.rs src/main.rs src/mirror.rs
git commit -m "feat: retry-safe byte-level upload progress (ProgressBody)"
```

---

### Task 4: Download byte ticks

**Files:**
- Modify: `client/src/transfer.rs:892-1008` (`download_key_to_path`, `download_to_temp`), `transfer.rs:1010+` (`download_object`)
- Modify call sites: `client/src/mirror.rs:590` (download arm)

**Interfaces:**
- Consumes: Tasks 1-2 (`ProgressUi::add_object`/`unit`, `UnitHandle`, `session.ui()`).
- Produces:
  - `download_key_to_path(client, bucket, key, output, part_size, parallel, preserve, progress: Option<&crate::progress::ProgressUi>) -> Result<u64>` (new last param)
  - `download_to_temp(client, bucket, key, tmp, size, part_size, parallel, progress: Option<&crate::progress::ProgressUi>) -> Result<()>` (new last param)

- [ ] **Step 1: Wire progress through downloads**

1. `download_key_to_path` gains final param `progress: Option<&crate::progress::ProgressUi>`. After `let size = head.content_length()…` add:

```rust
if let Some(ui) = progress {
    ui.add_object(size);
}
```

Forward `progress` into `download_to_temp`.

2. `download_to_temp` gains the same param. Derive a display label once: `let label = key.rsplit('/').next().unwrap_or(key).to_string();`

   **Small path** (`size <= part_size`, line ~960): replace `tokio::io::copy` with an explicit chunk loop that ticks:

```rust
let unit = match progress {
    Some(ui) => ui.unit(label.clone(), size),
    None => crate::progress::UnitHandle::noop(),
};
let resp = client.get_object().bucket(bucket).key(key).send().await?;
let mut reader = resp.body.into_async_read();
let file = fs::File::create(tmp).await?;
let mut writer = BufWriter::new(file);
let mut buf = vec![0u8; 64 * 1024];
loop {
    let n = tokio::io::AsyncReadExt::read(&mut reader, &mut buf).await?;
    if n == 0 {
        break;
    }
    tokio::io::AsyncWriteExt::write_all(&mut writer, &buf[..n]).await?;
    unit.inc(n as u64);
}
writer.flush().await?;
unit.finish();
return Ok(());
```

   **Ranged path** (line ~973): like Task 3's parts, clone `let progress = progress.cloned();` before `stream::iter`; inside each part future create `let unit = …` with label `format!("{label} part {}/{part_count}", part_index + 1)` and len `end - start + 1`, then replace its `tokio::io::copy(&mut reader, &mut file)` with the same 64 KiB read/`write_all`/`unit.inc` loop tracking `copied` manually, and call `unit.finish()` after the short-read check passes.

3. `download_object` (line ~1031): pass `session.ui()` into `download_key_to_path`.
4. `mirror.rs:590` download arm: forward `transfer_one`'s `progress` param (added in Task 3) into `download_key_to_path`.

- [ ] **Step 2: Full suite**

Run: `cargo test 2>&1 | tail -5`
Expected: all pass. The download e2e tests (`e2e_cp`/`e2e_mirror` etc.) exercise both paths with noop handles; the short-read guard still works because the loop counts `copied` the same way.

- [ ] **Step 3: fmt, clippy, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/transfer.rs src/mirror.rs
git commit -m "feat: byte-level download progress in single and ranged GET paths"
```

---

### Task 5: S3→S3 copy progress

**Files:**
- Modify: `client/src/transfer.rs:506-811` (`multipart_copy_s3_to_s3`, `transfer_object_between_s3`, `multipart_server_side_copy`)
- Modify call sites: `client/src/main.rs:1529` (`cp_s3_to_s3`), `client/src/mirror.rs:619` (s3→s3 arm)

**Interfaces:**
- Consumes: Tasks 1-3 (`add_object`, `unit`, `instrument_body`, `UnitHandle::noop`).
- Produces: `transfer_object_between_s3(…, preserve, progress: Option<&crate::progress::ProgressUi>) -> Result<()>`, `multipart_copy_s3_to_s3(…, parallel, progress: Option<&crate::progress::ProgressUi>)`, `multipart_server_side_copy(…, parallel, progress: Option<&crate::progress::ProgressUi>)` (new last params).

- [ ] **Step 1: Wire the three functions**

1. `transfer_object_between_s3` gains final param `progress: Option<&crate::progress::ProgressUi>`. First line of the body:

```rust
if let Some(ui) = progress {
    ui.add_object(size);
}
```

2. **Same-endpoint single `CopyObject`** (line ~628): no bytes cross the client — one unit covering the whole object, finished after `send`:

```rust
let unit = match progress {
    Some(ui) => ui.unit(
        source_key.rsplit('/').next().unwrap_or(source_key).to_string(),
        size,
    ),
    None => crate::progress::UnitHandle::noop(),
};
// … existing copy_object().send().await? …
unit.finish();
```

3. **`multipart_server_side_copy`** gains the param; per-part units created inside each future (clone `progress.cloned()` + a `label` derived from `source_key` before `stream::iter`, like Tasks 3-4), len `end - start + 1`, label `format!("{label} part {part_index}/{part_count}")`. `UploadPartCopy` moves no client bytes, so there is no mid-part tick: just `unit.finish()` after the successful `send` (spec: documented part-granularity limitation).
4. **Cross-endpoint streaming single-shot** (line ~664): bytes DO cross the client. Create a whole-object unit; wrap the GET body before handing it to `put_object`:

```rust
let body = crate::progress::instrument_body(resp.body, &unit);
// … put_object().body(body) …
unit.finish();
```

(A streamed GET body is not retryable, so the map's `rewind()` fires once at first attempt — harmless.)
5. **`multipart_copy_s3_to_s3`** gains the param; per-part units like the others; wrap each part's GET `body` with `instrument_body` before `upload_part(…).body(…)`; `unit.finish()` after send.
6. Call sites: `main.rs:1529` and `mirror.rs:619` pass `session.ui()` / `progress` respectively.

- [ ] **Step 2: Full suite**

Run: `cargo test 2>&1 | tail -5`
Expected: all pass (e2e covers same-endpoint copies incl. multipart server-side; cross-endpoint streaming is covered by the two-server mirror tests).

- [ ] **Step 3: fmt, clippy, commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add src/transfer.rs src/main.rs src/mirror.rs
git commit -m "feat: progress for s3-to-s3 copies (streamed bytes; server-side per part)"
```

---

### Task 6: README divergence note, manual TTY smoke, final gate

**Files:**
- Modify: `client/README.md` ("Known divergences from mc" section)
- No code changes expected; fixes discovered by the smoke test are in scope.

**Interfaces:** consumes everything; produces nothing new.

- [ ] **Step 1: README entry**

Append to the "Known divergences from mc" list in `client/README.md`:

```markdown
- **TTY progress display (deliberate):** during `cp`/`mv`/`put`/`get`/`mirror`
  on a terminal, rs3 shows up to 10 per-unit progress bars (one per in-flight
  multipart segment or small file) above an overall `TOTAL x/y objects` bar,
  where mc shows a single aggregate bar. Bytes tick live as they cross the
  wire; server-side S3→S3 copy parts tick on completion since no bytes cross
  the client. Non-TTY, `--json`, and `--quiet` output is unaffected.
  `--no-color` disables the bars.
```

- [ ] **Step 2: Manual TTY smoke**

From the repo root, boot a throwaway rusts3 server and drive rs3 under a forced TTY (`script` allocates a pty; bars draw to stderr, captured in the typescript file):

```bash
cd /home/code/workspace/rust-s3-server
cargo build --release 2>&1 | tail -2
cargo build --release --manifest-path client/Cargo.toml 2>&1 | tail -2
SMOKE=$(mktemp -d)
head -c 1500M /dev/urandom > "$SMOKE/big.bin"
# start server per README/test-harness conventions (data dir + creds + port),
# then:
export MC_CONFIG_DIR="$SMOKE/mc" RS3_HOST_SMOKE="http://smokekey:smokesecret@127.0.0.1:<port>"
script -qec "./client/target/release/rs3 mb smoke/bars && \
  ./client/target/release/rs3 cp -s 64MiB -P 6 $SMOKE/big.bin smoke/bars/ && \
  ./client/target/release/rs3 cp -s 64MiB -P 6 smoke/bars/big.bin $SMOKE/out.bin" \
  "$SMOKE/typescript.log"
grep -c "part " "$SMOKE/typescript.log"   # expect many per-part bar frames
cmp "$SMOKE/big.bin" "$SMOKE/out.bin" && echo ROUNDTRIP-OK
```

Verify by eye in `typescript.log` (it contains raw bar redraw frames):
- multiple simultaneous `big.bin part i/24` bars plus the `TOTAL 0/1 objects` line;
- byte counts on part bars move between frames (not 0→jump→full);
- upload (put) and download (get) both show live parts;
- final frame leaves the finished TOTAL bar visible.
Also check `-P 12` briefly: at most 10 part bars visible at once.
Then delete `$SMOKE` and the throwaway server data dir, and confirm `git status` shows no server-crate changes.

- [ ] **Step 3: Final gate**

```bash
cd client && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test 2>&1 | tail -5
git add README.md
git commit -m "docs: README divergence note for multi-bar TTY progress"
```

---

## Self-review notes (already applied)

- Spec coverage: §1→Task 1, §2→Task 2, §3 uploads→Task 3, downloads→Task 4, s3→s3→Task 5, §4 docs→Task 6, §5 unit tests→Tasks 1-3 / regression→every task's full-suite step / manual smoke→Task 6. `pipe`/`upload_stream` untouched (non-goal).
- Type consistency: `progress: Option<&crate::progress::ProgressUi>` is the threading type everywhere; owned `Option<ProgressUi>` clones (`progress.cloned()`) only where `'static` futures require it. `finish_and_keep` (not `finish_and_clear`) is the session-end call.
- Known API risk is confined to Task 3 Step 2's note (SdkBody map-on-clone semantics) with an explicit fallback strategy; the test contract (10, not 20) is what matters.
