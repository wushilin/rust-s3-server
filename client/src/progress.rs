//! Multi-bar live progress: two different display strategies depending on
//! mode, both sized off `-P` worker lanes via [`lane_count`].
//!
//! **Transfer (bar) mode** (`ProgressUi::new`, used by `TransferSession`):
//! a fixed gradle-style grid of `-P` lanes (one multipart segment of a
//! large file, or one whole small file), stacked above a persistent overall
//! bar. The grid is built once; lanes are claimed/restyled on use and
//! reverted to a dim idle row on release -- no bar is ever inserted or
//! removed mid-run. Safe because transfer commands never print interleaved
//! stdout while the bar is up (see [`UiInner::overall`]'s doc).
//!
//! **Tasks-only mode** (`ProgressUi::new_tasks_only`, used by [`worker_ui`]
//! for standalone commands): no persistent grid -- a transient spinner
//! bar is inserted on claim and fully removed on release, capped at the
//! same `-P`-derived count. A persistent idle row here would re-introduce
//! the stdout-glue bug e534ce7 fixed: standalone commands interleave their
//! own `println!`s with dispatch's task lines, and a bar that outlives its
//! own draw parks the cursor for the next print to glue onto (amended spec,
//! decision 3 -- see `docs/superpowers/specs/2026-07-31-rs3-worker-lanes-design.md`).
//!
//! Deliberate TTY-only divergence from mc's single aggregate bar — see
//! README "Known divergences from mc". All bars draw to stderr; stdout (the
//! mc-compat message contract) is never touched.

use std::sync::{Arc, Mutex};

use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

/// Classic-terminal fallback lane count when the terminal's row count can't
/// be detected (e.g. a hidden test target, or a genuinely weird TTY).
const FALLBACK_LANES: usize = 22;

/// Fixed on-screen width for a bar's label: long labels condense (see
/// [`TransferLabel::render`]), short ones are padded to this so bars don't
/// jiggle horizontally as different-length labels rotate through a slot.
pub(crate) const LABEL_WIDTH: usize = 40;

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
    let total = format!("{:.1}", scale(len));
    let total = total.strip_suffix(".0").unwrap_or(&total);
    format!("{:.1}/{}{}", scale(pos), total, unit)
}

/// What a transfer unit is doing, for the bar label -- distinct from the
/// server-side S3 operation (e.g. same-endpoint `Copying` still issues a
/// `CopyObject` or `UploadPartCopy`, not a GET+PUT).
#[derive(Clone, Copy)]
pub(crate) enum Verb {
    Uploading,
    Downloading,
    Copying,
    Creating,
    Completing,
    Aborting,
    Inspecting,
    // Wired to `dispatch` from `list.rs`'s shared `ObjectPaginator`
    // (`with_dispatch`) -- every standalone command's per-page
    // ListObjectsV2 call routes through here.
    Listing,
    Removing,
}

impl Verb {
    fn as_str(self) -> &'static str {
        match self {
            Verb::Uploading => "Uploading",
            Verb::Downloading => "Downloading",
            Verb::Copying => "Copying",
            Verb::Creating => "Creating",
            Verb::Completing => "Completing",
            Verb::Aborting => "Aborting",
            Verb::Inspecting => "Inspecting",
            Verb::Listing => "Listing",
            Verb::Removing => "Removing",
        }
    }
}

/// A bar's label before rendering: verb, the path being transferred, and an
/// optional `(part_index, part_count)` for multipart segments. Kept
/// structured (rather than a pre-formatted `String`) so [`render`] can
/// condense long paths to fit [`LABEL_WIDTH`] instead of truncating blindly.
///
/// [`render`]: TransferLabel::render
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
        if parts.len() > 2
            && let Some(s) = fit(&format!("{}/…/{}", parts[0], parts[parts.len() - 1]))
        {
            return s;
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

/// `ProgressStyle::with_key` callback rendering the `{bytes_pair}` column.
fn bytes_pair_key(state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write) {
    let _ = w.write_str(&format_bytes_pair(state.pos(), state.len().unwrap_or(0)));
}

/// `bucket/prefix` for a `TransferLabel`'s path, without a dangling `/`
/// when `prefix` is empty -- an empty-prefix `ListObjectsV2`/batch-delete
/// call operates on the whole bucket, not on a `bucket/` "sub-path", so the
/// label should read `Listing bucket`, not `Listing bucket/`.
pub(crate) fn bucket_prefix_label(bucket: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        bucket.to_string()
    } else {
        format!("{bucket}/{prefix}")
    }
}

struct UiState {
    objects_total: u64,
    objects_done: u64,
    // Transfer (bar) mode: free lane indices into `UiInner::lanes`, a LIFO
    // stack ordered so the *lowest* index is claimed first (initialized
    // high-to-low, popped low-to-high) -- purely cosmetic (claims fill the
    // grid top-down), no correctness dependency. Unused in tasks-only mode
    // (`UiInner::lanes` is always empty there).
    free: Vec<usize>,
    // Tasks-only mode: count of live transient task/spinner bars, capped at
    // `UiInner::cap`. Unused in transfer (bar) mode (occupied count there
    // is `lanes.len() - free.len()`).
    active: usize,
}

struct UiInner {
    mp: MultiProgress,
    // Transfer (bar) mode: the fixed grid, sized once at construction (see
    // `lane_count`) -- every entry exists for the life of this `ProgressUi`,
    // in idle style until claimed by `unit`/`task`, never grown, shrunk,
    // inserted into, or removed from after construction, only restyled in
    // place. **Empty in tasks-only mode** -- that mode has no persistent
    // grid at all (see the module doc's "Tasks-only mode" section); `unit`/
    // `task` there insert-and-remove a transient bar per claim instead, via
    // `ProgressUi::claim`/`release_claim`. `lanes.is_empty()` is the single
    // source of truth for which mode a given `ProgressUi` is in.
    lanes: Vec<ProgressBar>,
    // Concurrency cap, computed once via `lane_count`. In transfer mode
    // this equals `lanes.len()` (the grid size); in tasks-only mode it caps
    // `UiState::active` transient bars, since there's no grid to size against.
    cap: usize,
    // `None` in "tasks-only" mode (`ProgressUi::new_tasks_only`): standalone
    // commands print their own stdout lines interleaved with dispatch's
    // spinner task lines, and a *persistent* bar (one that outlives every
    // individual draw, unlike a task line that's removed the moment its
    // dispatch call resolves) parks the terminal cursor at the end of its
    // own line with no trailing newline -- the next `println!` then glues
    // onto it instead of starting a fresh line (reproduced under a pty:
    // `TOTAL 0/0 objects [...] ... [2026-07-31 ...] 14B f1.txt`). Bar mode
    // (`ProgressUi::new`, used by `TransferSession`) keeps the overall bar:
    // transfer commands never print per-object stdout lines while the bar
    // is active (bar-mode-silent, see `TransferSession::object_done`), so
    // there is nothing for it to glue onto. Always the last bar in `mp`,
    // below every lane -- lanes are added first at construction, so this is
    // naturally pinned at the bottom without ever needing `insert_before`.
    overall: Option<ProgressBar>,
    bar_width: usize,
    state: Mutex<UiState>,
}

#[derive(Clone)]
pub(crate) struct ProgressUi {
    inner: Arc<UiInner>,
}

/// Solid-block fill for the bars: full block, then eighth-width partial
/// blocks (widest to narrowest) so progress advances in sub-cell steps,
/// then a space for the unfilled remainder. No arrow heads.
const BAR_CHARS: &str = "█▉▊▋▌▍▎▏ ";

/// Non-bar columns around the `{bar}` field, worst case: 40-char label,
/// bracket pairs, `1023.9/1024KiB`-style byte pair, a speed column, and
/// the TOTAL row's `eta` tail. The bar gets whatever width remains.
const BAR_OVERHEAD: usize = 80;
const MIN_BAR_WIDTH: usize = 10;
const MAX_BAR_WIDTH: usize = 60;

/// Bar width for a terminal `cols` wide; `None` (width undetectable —
/// e.g. hidden test target) keeps the historical fixed 30.
fn bar_width_for(cols: Option<u16>) -> usize {
    match cols {
        None => 30,
        Some(c) => (c as usize)
            .saturating_sub(BAR_OVERHEAD)
            .clamp(MIN_BAR_WIDTH, MAX_BAR_WIDTH),
    }
}

/// Fixed template + message for an unclaimed lane: a plain message field
/// (no bar, no spinner) styled dim via ANSI 256 color 240 -- the same dim
/// used for the bars' unfilled fill -- so idle rows read as inactive at a
/// glance. `None` (template compile failure, never expected in practice)
/// leaves the lane in whatever style `indicatif` defaults to.
const IDLE_MESSAGE: &str = "> IDLE";

fn idle_style() -> Option<ProgressStyle> {
    ProgressStyle::with_template("{msg:.240}").ok()
}

/// Pure sizing for the fixed lane grid: `P` worker lanes capped by how many
/// rows the terminal can actually show. `reserved` rows are held back below
/// the grid for other persistent content -- 2 for transfer mode (the TOTAL
/// bar plus a margin row) or 1 for tasks-only mode (the margin row alone,
/// no TOTAL bar) -- and the usable remainder is floored at 1 so a
/// degenerate terminal still gets a grid, not an empty one. `term_rows` of
/// `None` (size undetectable -- a hidden test target, or a genuinely weird
/// TTY) skips the row math entirely and falls back to the classic-terminal
/// assumption of `FALLBACK_LANES` usable rows. `p` is assumed already >= 1:
/// `-P` is clamped by `StreamBudget::new` before it ever reaches here.
fn lane_count(p: usize, term_rows: Option<u16>, reserved: usize) -> usize {
    let p = p.max(1);
    match term_rows {
        Some(rows) => {
            let usable = (rows as usize).saturating_sub(reserved).max(1);
            p.min(usable)
        }
        None => p.min(FALLBACK_LANES),
    }
}

/// Shared bar-vs-lines predicate for both [`worker_ui`] and
/// [`crate::messages::TransferSession::new`]: stdout is a TTY, and none of
/// `--quiet`/`--json`/`--no-color` is set.
fn ui_enabled() -> bool {
    let out = crate::output::out();
    out.stdout_tty && !out.quiet && !out.json && !out.no_color
}

/// Standalone commands' (ls/rm/stat/head/cat/du/tree/find/diff/mb/rb/mv...)
/// equivalent of [`crate::messages::TransferSession::new`]'s bar-vs-lines
/// decision: `Some` under [`ui_enabled`], `None` otherwise so `dispatch`
/// degrades to its noop task line and non-TTY/`--json`/`--quiet` stdout
/// stays byte-identical. Always **tasks-only** mode (see
/// [`ProgressUi::new_tasks_only`]) -- these commands print their own stdout
/// lines, which a persistent bar would corrupt.
///
/// `parallel` is the caller's `-P`/internal worker count (see
/// `docs/superpowers/specs/2026-07-31-rs3-worker-lanes-design.md`), which
/// caps this mode's transient-bar concurrency via [`lane_count`] (no fixed
/// grid here -- see [`ProgressUi::new_tasks_only`]'s doc for why).
pub(crate) fn worker_ui(parallel: usize) -> Option<ProgressUi> {
    ui_enabled().then(|| ProgressUi::new_tasks_only(parallel))
}

/// [`crate::messages::TransferSession`]'s bar-vs-lines decision: `Some`
/// under the same [`ui_enabled`] predicate, full bar mode (persistent
/// overall bar) -- transfer commands never print per-object stdout lines
/// while the bar is active, so there's nothing for it to glue onto.
///
/// `parallel` is the transfer command's `-P` value, sizing the fixed lane
/// grid via [`lane_count`].
pub(crate) fn transfer_ui(parallel: usize) -> Option<ProgressUi> {
    ui_enabled().then(|| ProgressUi::new(parallel))
}

impl ProgressUi {
    /// `parallel` (`-P`) sizes the fixed lane grid via [`lane_count`],
    /// reserving 2 rows below it for the TOTAL bar plus a margin row.
    pub(crate) fn new(parallel: usize) -> Self {
        let (rows, cols) = Self::term_size();
        Self::with_target(
            ProgressDrawTarget::stderr(),
            bar_width_for(cols),
            true,
            parallel,
            rows,
        )
    }

    /// Spinner-task-only mode: no persistent overall bar, so only 1 row is
    /// reserved below the grid (a margin row; there's no TOTAL bar to
    /// reserve for). Used by [`worker_ui`] for standalone commands, which
    /// print their own stdout lines between dispatch calls -- see
    /// [`UiInner::overall`]'s doc for why a persistent bar is unsafe there.
    pub(crate) fn new_tasks_only(parallel: usize) -> Self {
        let (rows, cols) = Self::term_size();
        Self::with_target(
            ProgressDrawTarget::stderr(),
            bar_width_for(cols),
            false,
            parallel,
            rows,
        )
    }

    fn term_size() -> (Option<u16>, Option<u16>) {
        match console::Term::stderr().size_checked() {
            Some((rows, cols)) => (Some(rows), Some(cols)),
            None => (None, None),
        }
    }

    /// Hidden draw target for unit tests: full accounting, no rendering,
    /// undetectable terminal size (`lane_count`'s [`FALLBACK_LANES`] path).
    #[cfg(test)]
    pub(crate) fn hidden(parallel: usize) -> Self {
        Self::with_target(
            ProgressDrawTarget::hidden(),
            bar_width_for(None),
            true,
            parallel,
            None,
        )
    }

    /// Like [`Self::hidden`], but tasks-only (no overall bar) -- for tests
    /// exercising [`worker_ui`]'s mode specifically.
    #[cfg(test)]
    pub(crate) fn hidden_tasks_only(parallel: usize) -> Self {
        Self::with_target(
            ProgressDrawTarget::hidden(),
            bar_width_for(None),
            false,
            parallel,
            None,
        )
    }

    /// Transfer (bar) mode builds the fixed lane grid (all lanes in idle
    /// style, top to bottom) and the TOTAL bar below it -- lanes are added
    /// to `mp` before TOTAL, so TOTAL stays naturally pinned at the bottom
    /// without ever needing `insert_before`; no bar is added, removed, or
    /// reinserted into `mp` after this point for the life of the
    /// `ProgressUi`. Tasks-only mode builds no grid at all (`lanes` stays
    /// empty) -- see the module doc's "Tasks-only mode" section for why.
    fn with_target(
        target: ProgressDrawTarget,
        bar_width: usize,
        with_overall: bool,
        parallel: usize,
        term_rows: Option<u16>,
    ) -> Self {
        // Transfer mode reserves 2 rows below the grid (TOTAL + a margin
        // row); tasks-only mode reserves 1 (the margin row alone).
        let reserved = if with_overall { 2 } else { 1 };
        let lane_n = lane_count(parallel, term_rows, reserved);
        let mp = MultiProgress::with_draw_target(target);
        let lanes: Vec<ProgressBar> = if with_overall {
            (0..lane_n)
                .map(|_| {
                    let pb = mp.add(ProgressBar::new(0));
                    if let Some(style) = idle_style() {
                        pb.set_style(style);
                    }
                    pb.set_message(IDLE_MESSAGE);
                    pb
                })
                .collect()
        } else {
            Vec::new()
        };
        let overall = with_overall.then(|| {
            let overall = mp.add(ProgressBar::new(0));
            if let Ok(style) = ProgressStyle::with_template(&format!(
                "TOTAL {{msg}} [{{bar:{bar_width}.white/240}}] {{bytes_pair}} {{binary_bytes_per_sec}} eta {{eta}}",
            )) {
                overall.set_style(
                    style
                        .progress_chars(BAR_CHARS)
                        .with_key("bytes_pair", bytes_pair_key),
                );
            }
            overall.set_message("0/0 objects");
            overall
        });
        // Claim order is low-to-high (see `UiState::free`'s doc): initialize
        // high-to-low so the first `pop()` returns lane 0. Empty in
        // tasks-only mode (`lanes` is empty, nothing to index).
        let free = (0..lanes.len()).rev().collect();
        Self {
            inner: Arc::new(UiInner {
                mp,
                lanes,
                cap: lane_n,
                overall,
                bar_width,
                state: Mutex::new(UiState {
                    objects_total: 0,
                    objects_done: 0,
                    free,
                    active: 0,
                }),
            }),
        }
    }

    pub(crate) fn add_object(&self, bytes: u64) {
        let mut state = self.lock_state();
        state.objects_total += 1;
        if let Some(overall) = &self.inner.overall {
            overall.inc_length(bytes);
        }
        self.refresh_msg(&state);
    }

    pub(crate) fn object_done(&self) {
        let mut state = self.lock_state();
        state.objects_done += 1;
        self.refresh_msg(&state);
    }

    /// Claims a slot for a new `unit`/`task` handle. Transfer (bar) mode
    /// (`lanes` non-empty) claims a free index into the fixed grid;
    /// tasks-only mode (`lanes` empty) inserts a brand-new transient bar
    /// instead (see the module doc's "Tasks-only mode" section). `None` in
    /// either mode means the caller's handle goes silent -- no bar, but
    /// ticks still advance the overall bar in transfer mode (tasks-only has
    /// none) -- rather than exceeding the concurrency cap.
    fn claim(&self, spinner: bool, len: u64) -> Option<Claim> {
        if self.inner.lanes.is_empty() {
            self.claim_transient(spinner, len)
        } else {
            self.claim_lane().map(Claim::Lane)
        }
    }

    /// Transfer-mode claim: pops a free index into the fixed grid.
    fn claim_lane(&self) -> Option<usize> {
        self.lock_state().free.pop()
    }

    /// Tasks-only-mode claim: inserts a fresh transient bar, capped at
    /// `UiInner::cap` concurrent lines -- the pre-grid behavior e534ce7
    /// fixed stdout-glue with, restored here per the amended spec (decision
    /// 3): a persistent idle row is unsafe in this mode because standalone
    /// commands interleave `println!` with the UI.
    fn claim_transient(&self, spinner: bool, len: u64) -> Option<Claim> {
        let mut state = self.lock_state();
        if state.active >= self.inner.cap {
            return None;
        }
        state.active += 1;
        drop(state);
        let pb = self.inner.mp.add(if spinner {
            ProgressBar::new_spinner()
        } else {
            ProgressBar::new(len)
        });
        Some(Claim::Transient(pb))
    }

    /// Releases `claim`, however it was obtained. Shared by
    /// `UnitHandle::finish` and `UnitInner`'s `Drop` -- the only two
    /// release paths.
    fn release_claim(&self, claim: &Claim) {
        match claim {
            Claim::Lane(idx) => self.release_lane(*idx),
            Claim::Transient(pb) => self.release_transient(pb),
        }
    }

    /// Restyles lane `idx` back to its idle row (disabling any steady tick
    /// left over from a spinner claim) and returns it to the free list.
    fn release_lane(&self, idx: usize) {
        let pb = &self.inner.lanes[idx];
        pb.disable_steady_tick();
        if let Some(style) = idle_style() {
            pb.set_style(style);
        }
        pb.set_length(0);
        pb.set_position(0);
        pb.set_message(IDLE_MESSAGE);
        self.lock_state().free.push(idx);
    }

    /// Removes a transient bar entirely (finish + clear + remove from
    /// `mp`), restoring the pre-grid tasks-only behavior: no trace left on
    /// screen, unlike a grid lane which reverts to idle instead of vanishing.
    fn release_transient(&self, pb: &ProgressBar) {
        pb.disable_steady_tick();
        pb.finish_and_clear();
        self.inner.mp.remove(pb);
        let mut state = self.lock_state();
        state.active = state.active.saturating_sub(1);
    }

    /// One in-flight transfer unit. If every slot is claimed the handle is
    /// silent: no bar, but its ticks still advance the overall bar.
    pub(crate) fn unit(&self, label: TransferLabel, len: u64) -> UnitHandle {
        let claim = self.claim(false, len);
        if let Some(claim) = &claim {
            let pb = claim.bar(&self.inner.lanes);
            pb.set_length(len);
            pb.set_position(0);
            let bar_width = self.inner.bar_width;
            if let Ok(style) = ProgressStyle::with_template(&format!(
                "{{msg}} [{{bar:{bar_width}.white/240}}] {{bytes_pair}} {{binary_bytes_per_sec}}",
            )) {
                pb.set_style(
                    style
                        .progress_chars(BAR_CHARS)
                        .with_key("bytes_pair", bytes_pair_key),
                );
            }
            // Pad by char count, not byte count: `render` may emit a
            // multi-byte `…`, and byte-length padding would under-pad.
            let mut rendered = label.render(LABEL_WIDTH);
            let padding = LABEL_WIDTH.saturating_sub(rendered.chars().count());
            rendered.extend(std::iter::repeat_n(' ', padding));
            pb.set_message(rendered);
        }
        UnitHandle {
            inner: Some(Arc::new(UnitInner {
                ui: self.clone(),
                claim,
                len,
                state: Mutex::new(UnitProgress {
                    pos: 0,
                    finished: false,
                }),
            })),
        }
    }

    /// One in-flight byte-less S3 operation (HEAD, create/complete/abort
    /// multipart, list, delete...): a spinner line sharing the same
    /// concurrency cap as [`unit`](Self::unit), contributing zero bytes to
    /// the overall bar. Silent handle when every slot is claimed, same as
    /// `unit`.
    pub(crate) fn task(&self, label: TransferLabel, api: &'static str) -> UnitHandle {
        let claim = self.claim(true, 0);
        if let Some(claim) = &claim {
            let pb = claim.bar(&self.inner.lanes);
            if let Ok(style) = ProgressStyle::with_template(&format!("{{msg}} {{spinner}} {api}")) {
                pb.set_style(style);
            }
            pb.enable_steady_tick(std::time::Duration::from_millis(80));
            // Pad by char count, not byte count: `render` may emit a
            // multi-byte `…`, and byte-length padding would under-pad.
            let mut rendered = label.render(LABEL_WIDTH);
            let padding = LABEL_WIDTH.saturating_sub(rendered.chars().count());
            rendered.extend(std::iter::repeat_n(' ', padding));
            pb.set_message(rendered);
        }
        UnitHandle {
            inner: Some(Arc::new(UnitInner {
                ui: self.clone(),
                claim,
                len: 0,
                state: Mutex::new(UnitProgress {
                    pos: 0,
                    finished: false,
                }),
            })),
        }
    }

    /// Session end: detail bars are already gone (finished or dropped); the
    /// overall bar finishes in place and stays visible, like mc's. No-op in
    /// tasks-only mode (no overall bar to finish).
    ///
    /// Note: lane clearing relies on indicatif's default `ProgressFinish::AndClear`
    /// firing when the lane bars drop (verified against indicatif 0.17.11), so
    /// nobody later sets `on_finish` or defers the drop without realizing.
    pub(crate) fn finish_and_keep(&self) {
        let state = self.lock_state();
        self.refresh_msg(&state);
        if let Some(overall) = &self.inner.overall {
            overall.finish();
        }
    }

    /// Explicit pre-hard-exit teardown: clears every remaining bar/spinner
    /// from the terminal. `std::process::exit` skips `Drop`, so a caller
    /// about to call it (e.g. `find --exec`'s failure path) should call this
    /// first rather than rely on cleanup that will never run. Normally a
    /// no-op by the time it matters: task lines are finished synchronously
    /// inside `dispatch` well before any exit path, so there's nothing left
    /// to clear -- this is defense in depth, not a load-bearing fix.
    pub(crate) fn clear(&self) {
        let _ = self.inner.mp.clear();
    }

    fn refresh_msg(&self, state: &UiState) {
        let Some(overall) = &self.inner.overall else {
            return;
        };
        // Mirror `--remove` delete events complete objects that never went
        // through a transfer function's add_object — clamp so the display
        // never shows done > total.
        let total = state.objects_total.max(state.objects_done);
        overall.set_message(format!("{}/{} objects", state.objects_done, total));
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, UiState> {
        self.inner.state.lock().expect("ProgressUi state poisoned")
    }

    #[allow(dead_code)]
    pub(crate) fn overall_position(&self) -> u64 {
        self.inner
            .overall
            .as_ref()
            .map(|o| o.position())
            .unwrap_or(0)
    }

    #[allow(dead_code)]
    pub(crate) fn overall_length(&self) -> u64 {
        self.inner
            .overall
            .as_ref()
            .and_then(|o| o.length())
            .unwrap_or(0)
    }

    /// Occupied slots: `lane_total() - free lanes` in transfer mode, the
    /// live transient-bar count in tasks-only mode.
    #[allow(dead_code)]
    pub(crate) fn active_detail_bars(&self) -> usize {
        if self.inner.lanes.is_empty() {
            self.lock_state().active
        } else {
            self.inner.lanes.len() - self.lock_state().free.len()
        }
    }

    /// Fixed grid size: decided once at construction and never grown or
    /// shrunk for the life of this `ProgressUi` in transfer mode; always
    /// `0` in tasks-only mode, which has no persistent grid at all (see the
    /// module doc's "Tasks-only mode" section).
    #[allow(dead_code)]
    pub(crate) fn lane_total(&self) -> usize {
        self.inner.lanes.len()
    }
}

struct UnitProgress {
    pos: u64,
    finished: bool,
}

/// What `ProgressUi::claim` handed back: either an index into the fixed
/// grid (transfer mode) or a standalone bar inserted just for this claim
/// (tasks-only mode). See the module doc's "Tasks-only mode" section.
enum Claim {
    Lane(usize),
    Transient(ProgressBar),
}

impl Claim {
    fn bar<'a>(&'a self, lanes: &'a [ProgressBar]) -> &'a ProgressBar {
        match self {
            Claim::Lane(idx) => &lanes[*idx],
            Claim::Transient(pb) => pb,
        }
    }
}

struct UnitInner {
    ui: ProgressUi,
    // What this unit claimed, if anything (`None` when every slot was busy
    // at claim time -- a silent handle, per `unit`/`task`'s docs).
    claim: Option<Claim>,
    len: u64,
    state: Mutex<UnitProgress>,
}

impl UnitInner {
    fn bar(&self) -> Option<&ProgressBar> {
        self.claim.as_ref().map(|c| c.bar(&self.ui.inner.lanes))
    }
}

/// Cheap-clone handle for one transfer unit; safe to tick from concurrent
/// futures and from inside a retryable-body closure.
#[derive(Clone)]
pub(crate) struct UnitHandle {
    inner: Option<Arc<UnitInner>>,
}

impl UnitHandle {
    /// Inert handle for when progress is disabled (non-TTY/--json/--quiet/--no-color).
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
        if let Some(bar) = inner.bar() {
            bar.inc(n);
        }
        if let Some(overall) = &inner.ui.inner.overall {
            overall.inc(n);
        }
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
        if let Some(bar) = inner.bar() {
            bar.set_position(0);
        }
        if let Some(overall) = &inner.ui.inner.overall {
            overall.dec(pos);
        }
    }

    /// Snap to 100% (top up any rounding shortfall exactly once), release
    /// the claimed slot -- reverted to idle in transfer mode, removed
    /// entirely in tasks-only mode. Idempotent.
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
        if let Some(overall) = &inner.ui.inner.overall {
            overall.inc(shortfall);
        }
        if let Some(claim) = &inner.claim {
            inner.ui.release_claim(claim);
        }
    }
}

impl Drop for UnitInner {
    /// A dropped-unfinished unit (failed part) releases its slot but must
    /// not fake completion by topping up bytes.
    fn drop(&mut self) {
        let finished = self.state.lock().map(|s| s.finished).unwrap_or(true);
        if !finished && let Some(claim) = &self.claim {
            self.ui.release_claim(claim);
        }
    }
}

pin_project_lite::pin_project! {
    /// Ticks a [`UnitHandle`] as each data frame is polled onto the wire.
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
        if let std::task::Poll::Ready(Some(Ok(frame))) = &poll
            && let Some(data) = frame.data_ref()
        {
            this.unit.inc(data.len() as u64);
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
/// `SdkBody::map_preserve_contents` -- not plain `map` -- since this wrapper
/// only observes byte counts and never alters the data: `map` would drop
/// `bytes_contents`, which flips `body.bytes()` from `Some` to `None` and
/// silently downgrades SigV4 signing from a signed payload to
/// `UNSIGNED-PAYLOAD` for any in-memory body (aws-runtime's sigv4 only
/// selects `SignableBody::Bytes` when `bytes()` is `Some`).
/// `map_preserve_contents` re-applies on each retry attempt's clone just
/// like `map` does -- the closure rewinds first so a retried part never
/// double-counts. No-op for noop handles (progress disabled).
pub(crate) fn instrument_body(body: ByteStream, unit: &UnitHandle) -> ByteStream {
    if unit.is_noop() {
        return body;
    }
    let unit = unit.clone();
    ByteStream::new(body.into_inner().map_preserve_contents(move |inner| {
        unit.rewind();
        SdkBody::from_body_1_x(ProgressBody {
            inner,
            unit: unit.clone(),
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lane_count_is_p_capped_by_console_rows() {
        assert_eq!(lane_count(5, Some(40), 2), 5, "plenty of rows -> P lanes");
        assert_eq!(lane_count(32, Some(12), 2), 10, "capped by usable rows");
        assert_eq!(
            lane_count(32, None, 2),
            22,
            "undetectable -> classic-terminal fallback"
        );
        assert_eq!(lane_count(1, Some(40), 2), 1);
        assert_eq!(
            lane_count(8, Some(3), 2),
            1,
            "degenerate terminal still >= 1"
        );
        assert_eq!(lane_count(0, Some(40), 2), 1);
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

    #[test]
    fn eleventh_concurrent_unit_is_silent_but_still_counts() {
        let ui = ProgressUi::hidden(10);
        ui.add_object(11 * 100);
        let handles: Vec<UnitHandle> = (0..11)
            .map(|i| ui.unit(lbl(Verb::Uploading, &format!("u{i}"), None), 100))
            .collect();
        assert_eq!(ui.active_detail_bars(), 10, "cap is 10");
        // the silent 11th unit still ticks the overall bar
        handles[10].inc(40);
        assert_eq!(ui.overall_position(), 40);
    }

    #[test]
    fn finish_frees_slot_for_next_unit() {
        let ui = ProgressUi::hidden(10);
        let handles: Vec<UnitHandle> = (0..10)
            .map(|i| ui.unit(lbl(Verb::Uploading, &format!("u{i}"), None), 10))
            .collect();
        assert_eq!(ui.active_detail_bars(), 10);
        handles[0].finish();
        assert_eq!(ui.active_detail_bars(), 9);
        let _h = ui.unit(lbl(Verb::Uploading, "next", None), 10);
        assert_eq!(ui.active_detail_bars(), 10);
    }

    #[test]
    fn finish_tops_up_to_len_exactly_once() {
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let h = ui.unit(lbl(Verb::Uploading, "f", None), 100);
        h.inc(30);
        h.finish();
        assert_eq!(ui.overall_position(), 100, "topped up 30 -> 100");
        h.finish(); // idempotent
        assert_eq!(ui.overall_position(), 100);
    }

    #[test]
    fn rewind_subtracts_progress_for_retry() {
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let h = ui.unit(lbl(Verb::Uploading, "f", None), 100);
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
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let h = ui.unit(lbl(Verb::Uploading, "f", None), 100);
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
        let ui = ProgressUi::hidden(5);
        ui.add_object(50);
        ui.add_object(70);
        assert_eq!(ui.overall_length(), 120);
        ui.object_done();
        ui.object_done();
        ui.object_done(); // mirror delete events: done may pass adds
        // must not panic; display clamps total >= done internally
    }

    #[tokio::test]
    async fn progress_body_ticks_exact_len() {
        use aws_smithy_types::body::SdkBody;
        use aws_smithy_types::byte_stream::ByteStream;

        let ui = ProgressUi::hidden(5);
        ui.add_object(10);
        let unit = ui.unit(lbl(Verb::Uploading, "mem", None), 10);
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

        let ui = ProgressUi::hidden(5);
        ui.add_object(10);
        let unit = ui.unit(lbl(Verb::Uploading, "mem", None), 10);
        let retryable = SdkBody::retryable(|| SdkBody::from("0123456789"));
        let wrapped = instrument_body(ByteStream::new(retryable), &unit);
        // The SDK's orchestrator clones a retryable body once per attempt;
        // each clone re-applies the map, whose closure rewinds first.
        let inner = wrapped.into_inner();
        let attempt1 = inner.try_clone().expect("retryable clone");
        let d1 = ByteStream::new(attempt1)
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(d1.len(), 10);
        assert_eq!(ui.overall_position(), 10, "first attempt counted");
        let attempt2 = inner.try_clone().expect("retryable clone");
        let d2 = ByteStream::new(attempt2)
            .collect()
            .await
            .unwrap()
            .into_bytes();
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

    #[tokio::test]
    async fn instrument_body_preserves_content_length_and_retryability_for_file_body() {
        use aws_smithy_types::byte_stream::{ByteStream, Length};
        use std::io::Write;

        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(b"0123456789abcdef").expect("write");
        file.flush().expect("flush");

        let ui = ProgressUi::hidden(5);
        ui.add_object(10);
        let unit = ui.unit(lbl(Verb::Uploading, "part", None), 10);
        let body = ByteStream::read_from()
            .path(file.path())
            .offset(2)
            .length(Length::Exact(10))
            .build()
            .await
            .expect("build file body");
        let wrapped = instrument_body(body, &unit).into_inner();
        // Content-Length must survive wrapping -- put_object/upload_part
        // only set the header when this is `Some`.
        assert_eq!(wrapped.content_length(), Some(10));
        // Retryability must survive wrapping too, or a failed part could
        // never be retried by the SDK's orchestrator.
        assert!(
            wrapped.try_clone().is_some(),
            "file-backed body must remain retryable after instrument_body"
        );
    }

    #[tokio::test]
    async fn instrument_body_preserves_bytes_contents_for_in_memory_body() {
        // Pins Finding 1: `instrument_body` must use
        // `SdkBody::map_preserve_contents`, not plain `map` -- `map` drops
        // `bytes_contents`, flipping `body.bytes()` from `Some` to `None`
        // and silently downgrading SigV4 signing from a signed payload to
        // `UNSIGNED-PAYLOAD` for in-memory bodies.
        use aws_smithy_types::body::SdkBody;
        use aws_smithy_types::byte_stream::ByteStream;

        let ui = ProgressUi::hidden(5);
        ui.add_object(3);
        let unit = ui.unit(lbl(Verb::Uploading, "mem", None), 3);
        let body = ByteStream::new(SdkBody::from("abc"));
        let wrapped = instrument_body(body, &unit).into_inner();
        assert!(
            wrapped.bytes().is_some(),
            "wrapping must not drop bytes_contents (would downgrade SigV4 to UNSIGNED-PAYLOAD)"
        );
        assert_eq!(wrapped.bytes(), Some(b"abc".as_slice()));
    }

    fn lbl(verb: Verb, path: &str, part: Option<(u64, u64)>) -> TransferLabel {
        TransferLabel {
            verb,
            path: path.into(),
            part,
        }
    }

    #[test]
    fn label_renders_full_when_it_fits() {
        assert_eq!(
            lbl(Verb::Uploading, "asdf/a.img", Some((4, 24))).render(40),
            "Uploading asdf/a.img part 4/24"
        );
        assert_eq!(
            lbl(Verb::Copying, "pics/x.jpg", None).render(40),
            "Copying pics/x.jpg"
        );
    }

    #[test]
    fn label_condenses_middle_components_first() {
        // full form is 47 chars: "Downloading backups/2026/07/31/big.iso part 1/8"
        let l = lbl(
            Verb::Downloading,
            "backups/2026/07/31/big.iso",
            Some((1, 8)),
        );
        assert_eq!(l.render(40), "Downloading backups/…/big.iso part 1/8");
    }

    #[test]
    fn label_drops_to_ellipsis_slash_tail_then_trims_filename() {
        let l = lbl(
            Verb::Uploading,
            "averyveryverylongdirectoryname/file.bin",
            Some((2, 9)),
        );
        // "Uploading averyveryverylongdirectoryname/file.bin part 2/9" (58) -> first/…/tail == …/tail here
        assert_eq!(l.render(30), "Uploading …/file.bin part 2/9");
        // width too small even for the tail: trim filename from the left, keep verb + suffix
        let tight = lbl(
            Verb::Uploading,
            "d/really-long-filename-here.bin",
            Some((2, 9)),
        );
        let out = tight.render(24);
        // `…` is one char via chars().count() but 3 bytes in UTF-8 -- the
        // width contract (and the padding in `unit()`) is char-counted, so
        // this assertion is too (str::len() is bytes and would over-count
        // the ellipsis, failing at exactly the boundary this test targets).
        assert!(
            out.chars().count() <= 24,
            "{out:?} too wide ({} chars)",
            out.chars().count()
        );
        assert!(out.starts_with("Uploading …"), "{out:?}");
        assert!(out.ends_with(" part 2/9"), "{out:?}");
    }

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

    #[test]
    fn bytes_pair_total_never_shows_trailing_point_zero_after_rounding_up() {
        // Regression: the old pre-rounding fract() check only caught
        // fractions near 0, not fractions near 1 that round *up* to the
        // next whole number at `{:.1}` -- reintroducing a trailing `.0`.
        assert_eq!(format_bytes_pair(1_048_575, 1_048_575), "1024.0/1024KiB");
        assert_eq!(format_bytes_pair(2_097_100, 2_097_100), "2.0/2MiB");
    }

    #[test]
    fn bar_width_adapts_to_terminal_and_clamps() {
        assert_eq!(
            bar_width_for(None),
            30,
            "undetectable width keeps old fixed 30"
        );
        assert_eq!(bar_width_for(Some(120)), 40, "120 cols - 80 overhead");
        assert_eq!(bar_width_for(Some(200)), 60, "capped at MAX_BAR_WIDTH");
        assert_eq!(bar_width_for(Some(60)), 10, "floored at MIN_BAR_WIDTH");
        assert_eq!(
            bar_width_for(Some(0)),
            10,
            "degenerate terminal still floors"
        );
    }

    #[test]
    fn task_lines_share_the_slot_pool_and_add_no_bytes() {
        let ui = ProgressUi::hidden(10);
        ui.add_object(100);
        let bars: Vec<UnitHandle> = (0..9)
            .map(|i| ui.unit(lbl(Verb::Uploading, "x", Some((i + 1, 9))), 10))
            .collect();
        let t1 = ui.task(
            lbl(Verb::Creating, "asdf/a.img", None),
            "CreateMultipartUpload",
        );
        assert_eq!(ui.active_detail_bars(), 10, "task takes the 10th slot");
        let t2 = ui.task(lbl(Verb::Listing, "bucket/p", None), "ListObjectsV2");
        assert_eq!(ui.active_detail_bars(), 10, "11th is silent");
        t1.finish();
        t2.finish();
        assert_eq!(ui.active_detail_bars(), 9);
        assert_eq!(ui.overall_position(), 0, "tasks contribute no bytes");
        drop(bars);
    }

    #[test]
    fn concurrent_rewind_does_not_lose_other_unit_progress() {
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let h_a = ui.unit(lbl(Verb::Uploading, "a", None), 50);
        let h_b = ui.unit(lbl(Verb::Uploading, "b", None), 50);
        // Unit A increments by 40
        h_a.inc(40);
        assert_eq!(ui.overall_position(), 40);
        // Unit B increments by 25 (A's 40 + B's 25 = 65 total)
        h_b.inc(25);
        assert_eq!(ui.overall_position(), 65);
        // Unit A rewinds (atomic dec, not racy read-modify-write)
        h_a.rewind();
        // Only B's 25 should remain; A's 40 is atomically subtracted
        assert_eq!(
            ui.overall_position(),
            25,
            "rewind must atomically subtract without losing concurrent increments"
        );
    }

    // --- tasks-only mode (`worker_ui`'s `ProgressUi::new_tasks_only`) ---
    // F1 fix: standalone commands print their own stdout lines, so their
    // `ProgressUi` must have no persistent overall bar (see `UiInner::overall`'s
    // doc) or those prints glue onto it. These pin the resulting no-overall-bar
    // contract: `overall_position`/`overall_length` read as a well-defined `0`
    // rather than panicking, slot accounting still works, and `add_object`/
    // `object_done` (which a plain standalone command never calls, but which
    // must stay harmless if one did) don't panic either.

    #[test]
    fn tasks_only_mode_has_no_overall_position_or_length() {
        let ui = ProgressUi::hidden_tasks_only(5);
        assert_eq!(ui.overall_position(), 0, "no overall bar: reads as 0");
        assert_eq!(ui.overall_length(), 0, "no overall bar: reads as 0");
    }

    // Regression guard for the amended spec (decision 3): a persistent idle
    // grid in tasks-only mode re-introduces the stdout-glue bug e534ce7
    // fixed (standalone commands interleave `println!` with dispatch's
    // task lines; a persistent bar parks the cursor for the next print to
    // glue onto). Tasks-only mode must build zero persistent rows -- only
    // transient bars inserted on claim and fully removed on release.
    #[test]
    fn tasks_only_mode_has_no_persistent_grid() {
        let ui = ProgressUi::hidden_tasks_only(5);
        assert_eq!(
            ui.lane_total(),
            0,
            "tasks-only mode must not build a persistent lane grid"
        );
        let t = ui.task(lbl(Verb::Listing, "bucket/p", None), "ListObjectsV2");
        assert_eq!(
            ui.lane_total(),
            0,
            "grid stays empty even while a transient task line is active"
        );
        t.finish();
        assert_eq!(ui.active_detail_bars(), 0, "finished task line is gone");
    }

    #[test]
    fn tasks_only_mode_task_line_accounts_slots_but_not_bytes() {
        let ui = ProgressUi::hidden_tasks_only(5);
        assert_eq!(ui.active_detail_bars(), 0);
        let t = ui.task(lbl(Verb::Listing, "bucket/p", None), "ListObjectsV2");
        assert_eq!(ui.active_detail_bars(), 1, "task line takes a slot");
        assert_eq!(
            ui.overall_position(),
            0,
            "no overall bar to tick even while a task is active"
        );
        t.finish();
        assert_eq!(ui.active_detail_bars(), 0, "finish frees the slot");
    }

    #[test]
    fn tasks_only_mode_caps_at_ten_task_lines_like_bar_mode() {
        let ui = ProgressUi::hidden_tasks_only(10);
        let tasks: Vec<UnitHandle> = (0..10)
            .map(|i| ui.task(lbl(Verb::Listing, &format!("b/{i}"), None), "ListObjectsV2"))
            .collect();
        assert_eq!(ui.active_detail_bars(), 10);
        let eleventh = ui.task(lbl(Verb::Listing, "b/x", None), "ListObjectsV2");
        assert_eq!(
            ui.active_detail_bars(),
            10,
            "11th is silent, same cap as bar mode"
        );
        eleventh.finish();
        drop(tasks);
    }

    #[test]
    fn tasks_only_mode_add_object_and_object_done_do_not_panic() {
        // Not part of any standalone command's real usage (they never call
        // these), but `add_object`/`object_done`/`finish_and_keep` must
        // still degrade gracefully rather than unwrap a `None` overall bar,
        // in case a future caller reuses a tasks-only `ProgressUi` this way.
        let ui = ProgressUi::hidden_tasks_only(5);
        ui.add_object(100);
        ui.add_object(50);
        ui.object_done();
        ui.object_done();
        ui.object_done(); // more done than added: must not panic (bar mode clamps too)
        ui.finish_and_keep(); // no overall bar to finish: must not panic
        ui.clear(); // no bars left: must not panic or error
    }

    #[test]
    fn tasks_only_mode_unit_finish_does_not_panic_without_overall_bar() {
        // `unit()` isn't used by any tasks-only-mode caller today (standalone
        // commands only ever call `task()` via `dispatch`), but the no-overall
        // guard in `UnitHandle::inc`/`rewind`/`finish` must hold for it too.
        let ui = ProgressUi::hidden_tasks_only(5);
        let h = ui.unit(lbl(Verb::Downloading, "b/k", None), 10);
        h.inc(4);
        h.rewind();
        h.inc(10);
        h.finish();
        assert_eq!(ui.overall_position(), 0, "still reads as 0: no overall bar");
    }

    #[test]
    fn bucket_prefix_label_omits_trailing_slash_for_empty_prefix() {
        assert_eq!(bucket_prefix_label("mybucket", ""), "mybucket");
        assert_eq!(bucket_prefix_label("mybucket", "dir/"), "mybucket/dir/");
        assert_eq!(
            bucket_prefix_label("mybucket", "dir/f.txt"),
            "mybucket/dir/f.txt"
        );
    }
}
