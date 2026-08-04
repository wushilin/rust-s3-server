//! Multi-bar live progress. One display model, two invariants:
//!
//! **1. The screen is a fixed set of slots.** A `ProgressUi` builds a
//! gradle-style grid of `-P` lanes ([`lane_count`]) once, at construction.
//! Every in-flight unit of work -- a whole small file, one multipart
//! segment of a large one, or a byte-less control-plane call like a
//! `ListObjectsV2` page -- claims one slot ([`ProgressUi::unit`] /
//! [`ProgressUi::task`]), owns it exclusively for its whole life, and
//! returns it to a dim `> IDLE` row on release. Work that finds every slot
//! taken runs *silently* rather than growing the grid: no row is ever
//! inserted, removed, or reordered after construction, so the block of
//! lines the terminal has to redraw never changes height or meaning.
//! The one optional row is a persistent `TOTAL` bar below the grid
//! ([`UiInner::overall`]), for commands that have a knowable total.
//!
//! **2. Nothing else writes to the terminal.** A `MultiProgress` redraws by
//! walking the cursor back up over the block it painted last time; a single
//! stray `println!` invalidates that and the grid starts duplicating itself
//! down the screen. Every print in this crate therefore goes through
//! [`suspend_bars`] (directly, or via [`ui_println`]/[`ui_eprintln`]/
//! `output::print_msg`), which lifts the bars, writes, and repaints. That
//! is what lets the grid above be persistent even for commands that
//! interleave their own stdout with it -- it supersedes the earlier
//! workaround of giving standalone commands a gridless, insert-and-remove
//! display so there'd be no persistent row for their output to collide with
//! (`docs/superpowers/specs/2026-07-31-rs3-worker-lanes-design.md`,
//! decision 3), which cured the collision by having nothing stable on
//! screen rather than by making writes safe.
//!
//! **3. Counting and painting are separate rates.** Work reports as finely
//! as it likes -- a download calls [`ProgressNotifier::advance`] once per
//! 64KiB read -- and every report is counted the instant it arrives. What
//! is paced is handing that count to `indicatif`, once per
//! [`PAINT_INTERVAL`] per task, because that side costs a shared lock and
//! an ioctl per call whether or not a frame results. A row that changed is
//! drawn within the interval whether or not its task is still reporting
//! (a stalled stream still has its last bytes shown); a row that didn't
//! change is not drawn at all. See [`PAINT_INTERVAL`] and [`REFRESH_HZ`].
//!
//! Deliberate TTY-only divergence from mc's single aggregate bar — see
//! README "Known divergences from mc". All bars draw to stderr; stdout (the
//! mc-compat message contract) is never touched.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

/// Every `MultiProgress` that currently exists, newest last, each paired
/// with the id its owning [`UiInner`] deregisters itself by on drop.
///
/// A `MultiProgress` redraws by moving the cursor up over the block of rows
/// it painted last time and rewriting them in place. That accounting is
/// invalidated by *any* other write to the terminal: a raw `eprintln!` from
/// a failure path scrolls the block down by a line, the next redraw then
/// repaints the grid one row lower without erasing the old one, and the
/// terminal fills with stale copies of the lane grid -- while the message
/// that caused it scrolls away unread. [`suspend_bars`] is the choke point
/// that prevents it; this registry is what lets a print site far from any
/// `ProgressUi` handle (`output::print_error`, `mirror`'s per-object
/// failure line) reach the live bars without threading one through.
///
/// Registration is unconditional, including hidden (test) draw targets:
/// suspending a hidden `MultiProgress` just runs the closure.
static LIVE_BARS: Mutex<Vec<(u64, MultiProgress)>> = Mutex::new(Vec::new());
static NEXT_BARS_ID: AtomicU64 = AtomicU64::new(0);

fn lock_live_bars() -> std::sync::MutexGuard<'static, Vec<(u64, MultiProgress)>> {
    // A panic while the registry lock is held would otherwise poison every
    // later print; the data is a plain list, so recovering it is safe.
    LIVE_BARS.lock().unwrap_or_else(|e| e.into_inner())
}

/// Runs `f` with every live progress display cleared off the terminal,
/// restoring them afterwards -- so anything `f` prints lands above the bars
/// instead of shredding their cursor accounting (see [`LIVE_BARS`]).
/// A no-op wrapper when no `ProgressUi` exists (non-TTY, `--json`,
/// `--quiet`, or simply before/after the UI's lifetime), which is why call
/// sites can use it unconditionally.
pub(crate) fn suspend_bars<R>(f: impl FnOnce() -> R) -> R {
    // Clone the handles out and release the registry lock before running
    // `f`: `f` is arbitrary caller code and may construct or drop a
    // `ProgressUi` of its own.
    let bars: Vec<MultiProgress> = lock_live_bars().iter().map(|(_, mp)| mp.clone()).collect();
    let mut f = Some(f);
    let mut result = None;
    suspend_all(&bars, &mut || {
        result = Some(f.take().expect("suspend_all runs the closure once")());
    });
    result.expect("suspend_all always runs the closure")
}

/// Nests `MultiProgress::suspend` over every handle in `bars`. Deliberately
/// **not** generic over the closure: a generic recursive call would ask the
/// compiler to monomorphize one closure type per possible depth, which is a
/// recursion-limit error rather than a compiling program.
fn suspend_all(bars: &[MultiProgress], f: &mut dyn FnMut()) {
    match bars.split_first() {
        None => f(),
        Some((head, rest)) => head.suspend(|| suspend_all(rest, f)),
    }
}

/// `eprintln!` that is safe to call while the live progress UI is painting:
/// the bars are lifted, the line is written, the bars are put back. Every
/// `eprintln!` in this crate should be this instead -- see [`LIVE_BARS`] for
/// what a raw one does to the display.
macro_rules! ui_eprintln {
    ($($arg:tt)*) => {
        $crate::progress::suspend_bars(|| eprintln!($($arg)*))
    };
}
pub(crate) use ui_eprintln;

/// [`ui_eprintln`] for stdout. `output::print_msg` already suspends, so this
/// is only for the handful of sites that print a bare line without going
/// through the message contract (`find --print`'s rendered template, `rm`'s
/// zero-match notice).
macro_rules! ui_println {
    ($($arg:tt)*) => {
        $crate::progress::suspend_bars(|| println!($($arg)*))
    };
}
pub(crate) use ui_println;

/// Classic-terminal fallback lane count when the terminal's row count can't
/// be detected (e.g. a hidden test target, or a genuinely weird TTY).
const FALLBACK_LANES: usize = 22;

/// Fixed on-screen width for a bar's label: long labels condense (see
/// [`TransferLabel::render`]), short ones are padded to this so bars don't
/// jiggle horizontally as different-length labels rotate through a slot.
pub(crate) const LABEL_WIDTH: usize = 40;

/// Smallest gap between two pushes from one [`ProgressNotifier`] into
/// `indicatif`. Reports are *always* accepted and counted; this only paces
/// how often the counted position is handed over to be drawn.
///
/// The gate has to live here, on our side of the API, because `indicatif`'s
/// own rate limiting sits too late to help. Every `inc`/`set_position` on a
/// `MultiProgress` member takes the *shared* `MultiState` write lock, asks
/// the terminal for its width (a `TIOCGWINSZ` ioctl), and allocates, all
/// before the rate limiter is consulted and the draw is discarded. So the
/// cost is paid per report rather than per frame, and it is paid on a lock
/// every lane and the `TOTAL` row contend for. A download loop reporting
/// each 64KiB read (`transfer::download_range`) turns a fast object into
/// tens of thousands of ioctls a second through one global lock -- the
/// display then genuinely does throttle the transfer it is describing.
///
/// Coalescing here means one push per notifier per interval no matter how
/// finely the work reports, so the hot path is a `u64` add under a
/// per-notifier lock nobody else holds. `finish`, `rewind` and drop bypass
/// the gate (see [`NotifierInner::paint`]), so nothing buffered is ever
/// lost from the `TOTAL` row's accounting.
///
/// A gate driven only by reports would leave a *stalled* task showing a
/// stale figure for as long as the stall lasts, since the flush rides on
/// the very reports that stopped arriving. So the same interval also drives
/// a ticker ([`ProgressUi::spawn_stall_ticker`]) that pushes any task whose
/// counted position has moved since it was last painted, and skips the rest
/// -- making this a floor on how often a changed row is drawn as well as a
/// ceiling on how often an active one is.
const PAINT_INTERVAL: Duration = Duration::from_millis(100);

/// Terminal repaint ceiling, in frames per second: the second half of the
/// same 100ms budget. [`PAINT_INTERVAL`] caps how often *one* notifier
/// hands work over; with `-P 32` lanes each doing that independently the
/// `MultiProgress` would still be asked to redraw far more often than it
/// needs to, and `indicatif`'s default is 20/s. Matching the two means at
/// most one actual write to the terminal per interval -- which is what
/// costs over ssh, where the grid is tens of lines of cursor motion.
const REFRESH_HZ: u8 = 10;

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
    // Local filesystem tree walk (mirror/diff's local side) -- not an S3
    // operation at all, but it occupies a slot like one.
    Scanning,
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
            Verb::Scanning => "Scanning",
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
    // Set once a caller has handed over a known-complete total via
    // [`ProgressUi::declare_total`]. From then on `add_object` is inert:
    // the declared figure is the whole session's work, and letting
    // per-object calls keep incrementing on top of it would double-count
    // every object as its transfer starts.
    declared: bool,
    // Free slot indices into `UiInner::lanes`, a LIFO stack ordered so the
    // *lowest* index is claimed first (initialized high-to-low, popped
    // low-to-high) -- purely cosmetic (claims fill the grid top-down), no
    // correctness dependency. Its length is the whole occupancy model: a
    // slot is either here or owned by exactly one live `ProgressNotifier`.
    free: Vec<usize>,
    // Parallel to `UiInner::lanes`: the notifier currently occupying each
    // slot, for [`ProgressUi::repaint_stale`] to reach. Indexed by slot
    // rather than kept as a growing list precisely because the grid is a
    // fixed set of slots -- one entry per lane, set on claim, cleared on
    // release, so it never needs pruning however many tasks a run gets
    // through.
    //
    // Weak, so a notifier's own drop is what ends its life; a stale entry
    // here just fails to upgrade. Silent (slotless) tasks are deliberately
    // absent -- they paint nothing to go stale.
    watch: Vec<Option<std::sync::Weak<NotifierInner>>>,
}

struct UiInner {
    mp: MultiProgress,
    // The fixed slot grid, sized once at construction (see `lane_count`).
    // Every entry exists for the life of this `ProgressUi`, in idle style
    // until claimed by `unit`/`task`, and is never grown, shrunk, inserted
    // into, removed from, or reordered afterwards -- only restyled in place
    // by whichever handle currently owns it. That fixed height is what
    // keeps the `MultiProgress` redraw stable (module doc, invariant 1).
    lanes: Vec<ProgressBar>,
    // The optional persistent total row, for commands whose total is
    // knowable up front (`TransferSession`'s). `None` leaves the grid
    // alone on screen with nothing below it -- what standalone commands
    // (ls/rm/find/...) use, since they have no meaningful total, not
    // because a persistent row would be unsafe there: `suspend_bars`
    // (module doc, invariant 2) makes their interleaved stdout safe against
    // any persistent row, including the lanes themselves.
    //
    // Always the last bar in `mp`, below every lane -- lanes are added
    // first at construction, so this is naturally pinned at the bottom
    // without ever needing `insert_before`.
    overall: Option<ProgressBar>,
    bar_width: usize,
    /// [`PAINT_INTERVAL`] for a real display. Tests build with `ZERO`, which
    /// makes every report paint immediately -- so they can assert on
    /// accounting a line after producing it, without the wall clock
    /// deciding whether the assertion holds.
    paint_interval: Duration,
    state: Mutex<UiState>,
    // This UI's slot in the `LIVE_BARS` registry, released on drop.
    bars_id: u64,
}

impl Drop for UiInner {
    fn drop(&mut self) {
        lock_live_bars().retain(|(id, _)| *id != self.bars_id);
    }
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
    ProgressStyle::with_template("{prefix:.240}{msg:.240}").ok()
}

/// Puts a slot back into (or leaves it in) its unclaimed state: no spinner
/// tick, no bar, no leftover label or detail text from whichever task last
/// held it. The single definition of "idle", used both to build the grid
/// and to release a slot -- so a released slot is byte-for-byte a
/// never-used one, and stale text can't survive a handover.
fn reset_to_idle(slot: &ProgressBar) {
    slot.disable_steady_tick();
    if let Some(style) = idle_style() {
        slot.set_style(style);
    }
    slot.set_length(0);
    slot.set_position(0);
    slot.set_prefix(IDLE_MESSAGE);
    slot.set_message("");
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
/// equivalent of [`crate::messages::TransferSession::new`]'s display
/// decision: `Some` under [`ui_enabled`], `None` otherwise, so `dispatch`
/// degrades to a noop notifier and non-TTY/`--json`/`--quiet` stdout stays
/// byte-identical.
///
/// No `TOTAL` row -- these commands have no total worth showing -- but the
/// same fixed slot grid as a transfer. Their interleaved stdout is safe
/// against it because every print in the crate goes through
/// [`suspend_bars`] (module doc, invariant 2).
///
/// `parallel` is the caller's `-P`/internal worker count (see
/// `docs/superpowers/specs/2026-07-31-rs3-worker-lanes-design.md`), which
/// sizes the grid via [`lane_count`].
pub(crate) fn worker_ui(parallel: usize) -> Option<ProgressUi> {
    ui_enabled().then(|| ProgressUi::without_total(parallel))
}

/// [`crate::messages::TransferSession`]'s display decision: `Some` under
/// the same [`ui_enabled`] predicate, with the persistent `TOTAL` row below
/// the grid. `parallel` is the transfer command's `-P` value.
pub(crate) fn transfer_ui(parallel: usize) -> Option<ProgressUi> {
    ui_enabled().then(|| ProgressUi::with_total(parallel))
}

// ===================== the task abstraction =====================
//
// Two kinds of work can own a slot, and they differ in exactly one way:
// whether the work can say how far along it is.
//
//   ProgressAwareTask  -- can. Renders a bar plus a readout, and reports
//                         through `advance`/`set_done`/`set_fraction`.
//   ProgressOpaqueTask -- can't. Renders a spinner, and reports only the
//                         lifecycle transitions in `TaskState`.
//
// Everything else -- claiming a slot, owning it exclusively, handing it
// back to idle -- is common, lives in `ProgressUi::begin`, and neither kind
// gets to override it.

/// Internal full-scale value for [`Measure::Percent`]. Finer than 100 so a
/// bar drawn 60 cells wide still advances smoothly.
const PERCENT_SCALE: u64 = 1000;

/// How a [`ProgressAwareTask`] counts, and therefore how its slot renders
/// the readout between the bar and the right-hand detail text.
pub(crate) enum Measure {
    /// Bytes moved out of bytes planned: `123.3/256MiB 11.9 MiB/s`. The
    /// only flavor that is byte-shaped, i.e. that feeds the `TOTAL` row's
    /// byte bar -- see [`ProgressTask::byte_shaped`].
    Bytes { total: u64 },
    /// A count of discrete things, rendered through a caller-supplied
    /// template: `"{done}/{total} obj"`, `"{done}/{total} files"`. Both
    /// placeholders are optional and may repeat.
    Count { total: u64, template: &'static str },
    /// A bare fraction with no natural unit: `42%`. Reported with
    /// [`ProgressNotifier::set_fraction`]; the position is kept internally
    /// on a 0..=[`PERCENT_SCALE`] axis.
    #[allow(dead_code)] // no caller yet; the flavor completes the set
    Percent,
}

impl Measure {
    /// Full-scale value on this measure's own axis.
    fn span(&self) -> u64 {
        match self {
            Measure::Bytes { total } | Measure::Count { total, .. } => *total,
            Measure::Percent => PERCENT_SCALE,
        }
    }

    /// The template fragment rendered immediately right of the bar. The
    /// `Count` flavor defers to the `readout` key installed by [`style`].
    ///
    /// [`style`]: Measure::style
    fn readout(&self) -> &'static str {
        match self {
            Measure::Bytes { .. } => "{bytes_pair} {binary_bytes_per_sec}",
            Measure::Count { .. } => "{readout}",
            Measure::Percent => "{percent:>3}%",
        }
    }

    /// `{prefix} [bar] {readout} {msg}` -- label left, caller detail right.
    fn style(&self, bar_width: usize) -> Option<ProgressStyle> {
        let template = format!(
            "{{prefix}} [{{bar:{bar_width}.white/240}}] {} {{msg}}",
            self.readout()
        );
        let style = ProgressStyle::with_template(&template)
            .ok()?
            .progress_chars(BAR_CHARS)
            .with_key("bytes_pair", bytes_pair_key);
        Some(match self {
            Measure::Count { template, .. } => {
                let template = *template;
                style.with_key(
                    "readout",
                    move |state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
                        let _ = w.write_str(
                            &template
                                .replace("{done}", &state.pos().to_string())
                                .replace("{total}", &state.len().unwrap_or(0).to_string()),
                        );
                    },
                )
            }
            _ => style,
        })
    }
}

/// Where a task is in its life. An opaque task has nothing else to report,
/// so for it this *is* the progress model; an aware task reports position
/// as well, and uses these only to pick a style.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TaskState {
    /// Owns a slot but hasn't begun -- queued behind the stream budget.
    /// Renders dim and still, so a waiting task is visibly distinct from a
    /// working one.
    NotStarted,
    /// In flight.
    Running,
    /// Terminal. The slot has gone back to idle, so this is never rendered.
    Completed,
}

/// A unit of work that can own a display slot. Implementors describe *what*
/// is happening and how to paint it; they never claim, release, or reach
/// past their own slot.
pub(crate) trait ProgressTask: Send + Sync {
    /// Full-scale value on the notifier's axis: what `advance` counts up to
    /// and what [`ProgressNotifier::finish`] snaps to.
    fn span(&self) -> u64;

    /// Whether this task's axis is bytes on the wire, and so should feed
    /// the session's `TOTAL` row. False for count/percent work and for
    /// control-plane calls alike, so neither can inflate transfer totals.
    fn byte_shaped(&self) -> bool {
        false
    }

    /// Paint `slot` for `state`. Called once when the slot is claimed and
    /// again on every lifecycle transition, always on the slot this task
    /// exclusively owns.
    fn dress(&self, slot: &ProgressBar, bar_width: usize, state: TaskState);
}

/// Left-aligned label column: condensed to fit and padded to
/// [`LABEL_WIDTH`] so bars don't jiggle horizontally as different-length
/// labels rotate through a slot. Padded by char count, not byte count --
/// `render` may emit a multi-byte `…`, which byte-length padding under-pads.
fn label_column(label: &TransferLabel) -> String {
    let mut rendered = label.render(LABEL_WIDTH);
    let padding = LABEL_WIDTH.saturating_sub(rendered.chars().count());
    rendered.extend(std::iter::repeat_n(' ', padding));
    rendered
}

/// Work whose extent is known, so it can be drawn as a filling bar: a whole
/// small file, one multipart segment, a directory walk counting entries.
pub(crate) struct ProgressAwareTask {
    label: TransferLabel,
    measure: Measure,
}

impl ProgressAwareTask {
    /// Byte-shaped work -- the flavor that feeds the `TOTAL` row.
    pub(crate) fn bytes(label: TransferLabel, total: u64) -> Self {
        Self {
            label,
            measure: Measure::Bytes { total },
        }
    }

    /// Count-shaped work, rendered through a `{done}`/`{total}` template.
    pub(crate) fn count(label: TransferLabel, total: u64, template: &'static str) -> Self {
        Self {
            label,
            measure: Measure::Count { total, template },
        }
    }
}

impl ProgressTask for ProgressAwareTask {
    fn span(&self) -> u64 {
        self.measure.span()
    }

    fn byte_shaped(&self) -> bool {
        matches!(self.measure, Measure::Bytes { .. })
    }

    fn dress(&self, slot: &ProgressBar, bar_width: usize, state: TaskState) {
        if state == TaskState::NotStarted {
            // Queued: the label alone, dim, with no bar to imply motion.
            if let Some(style) = idle_style() {
                slot.set_style(style);
            }
        } else if let Some(style) = self.measure.style(bar_width) {
            slot.set_style(style);
        }
        slot.set_length(self.measure.span());
        slot.set_position(0);
        slot.set_prefix(label_column(&self.label));
        slot.set_message("");
    }
}

/// Work with no measurable extent: a byte-less S3 control-plane call (HEAD,
/// create/complete/abort multipart, a `ListObjectsV2` page, a batch delete).
/// All it can report is [`TaskState`], so it renders a spinner and the API
/// name rather than a bar.
pub(crate) struct ProgressOpaqueTask {
    label: TransferLabel,
    api: &'static str,
}

impl ProgressOpaqueTask {
    pub(crate) fn new(label: TransferLabel, api: &'static str) -> Self {
        Self { label, api }
    }
}

impl ProgressTask for ProgressOpaqueTask {
    fn span(&self) -> u64 {
        0
    }

    fn dress(&self, slot: &ProgressBar, _bar_width: usize, state: TaskState) {
        let api = self.api;
        let template = match state {
            // Dim, and no spinner: a queued call must not look like a
            // working one.
            TaskState::NotStarted => format!("{{prefix:.240}} {api} queued{{msg:.240}}"),
            _ => format!("{{prefix}} {{spinner}} {api}{{msg}}"),
        };
        if let Ok(style) = ProgressStyle::with_template(&template) {
            slot.set_style(style);
        }
        match state {
            TaskState::Running => slot.enable_steady_tick(std::time::Duration::from_millis(80)),
            _ => slot.disable_steady_tick(),
        }
        slot.set_prefix(label_column(&self.label));
        slot.set_message("");
    }
}

impl ProgressUi {
    /// `parallel` (`-P`) sizes the fixed slot grid via [`lane_count`],
    /// reserving 2 rows below it for the TOTAL bar plus a margin row.
    pub(crate) fn with_total(parallel: usize) -> Self {
        let (rows, cols) = Self::term_size();
        Self::with_target(
            ProgressDrawTarget::stderr_with_hz(REFRESH_HZ),
            bar_width_for(cols),
            true,
            parallel,
            rows,
            PAINT_INTERVAL,
        )
    }

    /// The same grid without the persistent TOTAL row, so only 1 row is
    /// reserved below it (the margin row alone). Used by [`worker_ui`] for
    /// standalone commands, which have no total worth showing.
    pub(crate) fn without_total(parallel: usize) -> Self {
        let (rows, cols) = Self::term_size();
        Self::with_target(
            ProgressDrawTarget::stderr_with_hz(REFRESH_HZ),
            bar_width_for(cols),
            false,
            parallel,
            rows,
            PAINT_INTERVAL,
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
    /// Unpaced (`paint_interval` `ZERO`) so every report lands immediately
    /// -- these tests are about what gets counted, not about when it is
    /// drawn; [`Self::hidden_paced`] is for the latter.
    #[cfg(test)]
    pub(crate) fn hidden(parallel: usize) -> Self {
        Self::with_target(
            ProgressDrawTarget::hidden(),
            bar_width_for(None),
            true,
            parallel,
            None,
            Duration::ZERO,
        )
    }

    /// Like [`Self::hidden`], but without the TOTAL row -- for tests
    /// exercising [`worker_ui`]'s shape specifically.
    #[cfg(test)]
    pub(crate) fn hidden_without_total(parallel: usize) -> Self {
        Self::with_target(
            ProgressDrawTarget::hidden(),
            bar_width_for(None),
            false,
            parallel,
            None,
            Duration::ZERO,
        )
    }

    /// [`Self::hidden`] with the paint gate actually engaged, for the tests
    /// that pin the gate's own behavior: that reports are counted while
    /// withheld, and that the paths which must not lose bytes flush anyway.
    #[cfg(test)]
    pub(crate) fn hidden_paced(parallel: usize, paint_interval: Duration) -> Self {
        Self::with_target(
            ProgressDrawTarget::hidden(),
            bar_width_for(None),
            true,
            parallel,
            None,
            paint_interval,
        )
    }

    /// Builds the whole display, once: the fixed slot grid (every slot in
    /// idle style, top to bottom) and, if asked, the TOTAL bar below it.
    /// Slots are added to `mp` before TOTAL, so TOTAL stays naturally
    /// pinned at the bottom without ever needing `insert_before`. No bar is
    /// added, removed, or reordered after this point for the life of the
    /// `ProgressUi` -- the fixed-height invariant the module doc opens with.
    fn with_target(
        target: ProgressDrawTarget,
        bar_width: usize,
        with_overall: bool,
        parallel: usize,
        term_rows: Option<u16>,
        paint_interval: Duration,
    ) -> Self {
        // With a TOTAL row, reserve 2 rows below the grid (TOTAL + a margin
        // row); without one, reserve 1 (the margin row alone).
        let reserved = if with_overall { 2 } else { 1 };
        let lane_n = lane_count(parallel, term_rows, reserved);
        // Assemble against a hidden target and attach the real one at the
        // end. indicatif's draw target rate-limits to `REFRESH_HZ` with a
        // burst budget of 20 draws, and *every* state-setter here (style, length,
        // position, prefix, message, per slot) counts as a draw attempt --
        // enough to drain the whole budget before the command has done any
        // work. A control-plane call that then completes in a millisecond,
        // like the `CreateMultipartUpload` opening a multipart upload,
        // would claim its slot, run, and release it entirely inside the
        // resulting 50ms blackout, never painting a single frame. Building
        // blind costs nothing and leaves the budget for real work; it also
        // means the grid first appears with a task already in it, rather
        // than flashing an all-idle frame first.
        let mp = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let lanes: Vec<ProgressBar> = (0..lane_n)
            .map(|_| {
                let pb = mp.add(ProgressBar::new(0));
                reset_to_idle(&pb);
                pb
            })
            .collect();
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
        // Fully assembled: connect it to the terminal, with a full draw
        // budget and nothing left to do but paint real work.
        mp.set_draw_target(target);
        // Claim order is low-to-high (see `UiState::free`'s doc): initialize
        // high-to-low so the first `pop()` returns slot 0.
        let free = (0..lanes.len()).rev().collect();
        let bars_id = NEXT_BARS_ID.fetch_add(1, Ordering::Relaxed);
        lock_live_bars().push((bars_id, mp.clone()));
        let watch = vec![None; lanes.len()];
        let ui = Self {
            inner: Arc::new(UiInner {
                mp,
                lanes,
                overall,
                bar_width,
                paint_interval,
                state: Mutex::new(UiState {
                    objects_total: 0,
                    objects_done: 0,
                    declared: false,
                    free,
                    watch,
                }),
                bars_id,
            }),
        };
        ui.spawn_stall_ticker();
        ui
    }

    /// Starts the thread that keeps a stalled task's row honest.
    ///
    /// The paint gate is driven by reports, so a task whose bytes stop
    /// arriving leaves whatever it last buffered unshown for as long as the
    /// stall lasts -- exactly when a user is staring at the row wondering
    /// what it is doing. This wakes every [`PAINT_INTERVAL`] and pushes any
    /// task whose counted position has moved since it was last painted.
    ///
    /// Unpaced UIs (tests, `paint_interval` `ZERO`) get no thread: with the
    /// gate open there is never anything buffered to flush.
    ///
    /// Holds only a `Weak`, and re-upgrades it each pass: the thread is a
    /// refresher for the display, not an owner of it, so the UI's lifetime
    /// stays exactly what its handles say it is and the thread retires on
    /// the first tick after the last one goes. Failing to spawn is ignored
    /// -- a missing refresher costs freshness during a stall, which is not
    /// worth failing a transfer over.
    fn spawn_stall_ticker(&self) {
        let interval = self.inner.paint_interval;
        if interval.is_zero() {
            return;
        }
        let weak = Arc::downgrade(&self.inner);
        let _ = std::thread::Builder::new()
            .name("rs3-progress".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    match weak.upgrade() {
                        Some(inner) => ProgressUi { inner }.repaint_stale(),
                        None => return,
                    }
                }
            });
    }

    /// One ticker pass: hands over every slot whose counted position has
    /// moved since its last paint, and leaves the rest alone. The "leaves
    /// the rest alone" half is [`NotifierInner::paint`]'s own no-change
    /// check, so an idle grid costs a walk over `watch` and nothing else --
    /// no lock on the `MultiProgress`, no terminal write.
    fn repaint_stale(&self) {
        // Collect the live notifiers and drop the grid lock *before*
        // painting any of them. `ProgressNotifier::finish` runs the two
        // locks the other way round -- notifier first, then the grid via
        // `release_slot` -- so holding both here would be the one lock cycle
        // in this module.
        let live: Vec<Arc<NotifierInner>> = {
            let state = self.lock_state();
            state
                .watch
                .iter()
                .filter_map(|slot| slot.as_ref()?.upgrade())
                .collect()
        };
        for notifier in live {
            notifier.report(|inner, state| inner.paint(state, true));
        }
    }

    /// Declares the session's *entire* workload up front: `objects` objects
    /// carrying `bytes` in total.
    ///
    /// Callers that plan before they transfer (mirror, and so `cp -r`/`mv -r`
    /// through it) know both figures exactly before the first byte moves, and
    /// must say so here. The alternative -- leaving the total to accrete one
    /// object at a time through [`add_object`] -- pins the denominator to
    /// `done + parallel`, because the only thing that grows it is a transfer
    /// starting and only `parallel` of those exist at once. The row then reads
    /// `12/18 objects` on a 2847-object mirror, and the bar, whose length is
    /// likewise only the bytes of objects already started, sits near full with
    /// `eta 0s` for the whole run: it tracks the last few objects rather than
    /// the transfer.
    ///
    /// Idempotent-ish by design: the first call wins and later `add_object`
    /// calls go quiet, so a planned session can share the same transfer
    /// functions as an unplanned one-object `cp` without double-counting.
    pub(crate) fn declare_total(&self, objects: u64, bytes: u64) {
        let mut state = self.lock_state();
        state.objects_total = objects;
        state.declared = true;
        if let Some(overall) = &self.inner.overall {
            overall.set_length(bytes);
        }
        self.refresh_msg(&state);
    }

    /// Adds one object to the total, for sessions with no plan to declare
    /// (a single-object `cp`/`put`/`get`). Inert once [`declare_total`] has
    /// been called -- see there for why.
    pub(crate) fn add_object(&self, bytes: u64) {
        let mut state = self.lock_state();
        if state.declared {
            return;
        }
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

    /// Starts `task` already running: claims a slot, dresses it, and hands
    /// back the [`ProgressNotifier`] that owns it until `finish`.
    ///
    /// When every slot is taken the notifier is **silent** -- it owns no
    /// slot and paints nothing -- rather than the grid growing a row for
    /// it. Its byte accounting still reaches the `TOTAL` row, so a silent
    /// task is invisible but never uncounted.
    pub(crate) fn start(&self, task: impl ProgressTask + 'static) -> ProgressNotifier {
        self.begin(Box::new(task), TaskState::Running)
    }

    /// Like [`start`](Self::start), but the task is queued rather than
    /// running: it takes its slot immediately (so waiting work is visible)
    /// and renders as [`TaskState::NotStarted`] until the caller reports
    /// [`ProgressNotifier::started`].
    pub(crate) fn enqueue(&self, task: impl ProgressTask + 'static) -> ProgressNotifier {
        self.begin(Box::new(task), TaskState::NotStarted)
    }

    fn begin(&self, task: Box<dyn ProgressTask>, life: TaskState) -> ProgressNotifier {
        // Pop a free slot index; `None` means every slot is busy and this
        // task runs silently. Slots are never created on demand.
        let slot = self.lock_state().free.pop();
        if let Some(idx) = slot {
            task.dress(&self.inner.lanes[idx], self.inner.bar_width, life);
        }
        let span = task.span();
        // Backdated a full interval so the *first* report paints on arrival:
        // the gate is there to thin out a stream of reports, and a task that
        // only ever makes one (or finishes inside the first interval) should
        // still show something rather than sit at zero until it ends.
        let now = Instant::now();
        let last_paint = now.checked_sub(self.inner.paint_interval).unwrap_or(now);
        let inner = Arc::new(NotifierInner {
            ui: self.clone(),
            slot,
            byte_shaped: task.byte_shaped(),
            task,
            state: Mutex::new(NotifierState {
                pos: 0,
                painted: 0,
                last_paint,
                span,
                life,
            }),
        });
        // Hand the stall ticker a way to reach this task for as long as it
        // holds the slot. Safe to do after the `Arc` exists rather than
        // atomically with the claim: nobody else can reach this notifier
        // yet, and the slot is already ours.
        if let Some(idx) = slot
            && !self.inner.paint_interval.is_zero()
        {
            self.lock_state().watch[idx] = Some(Arc::downgrade(&inner));
        }
        ProgressNotifier { inner: Some(inner) }
    }

    /// Hands slot `idx` back: reset to its dim `> IDLE` row and returned to
    /// the free list. The only release path, shared by
    /// [`ProgressNotifier::finish`] and `NotifierInner`'s `Drop`.
    fn release_slot(&self, idx: usize) {
        reset_to_idle(&self.inner.lanes[idx]);
        let mut state = self.lock_state();
        // Drop the ticker's handle in the same breath as the slot itself, so
        // it can never paint into a row its task no longer owns.
        state.watch[idx] = None;
        state.free.push(idx);
    }

    /// Session end: slots are already back to idle (finished or dropped);
    /// the overall bar finishes in place and stays visible, like mc's.
    /// No-op when there is no TOTAL row.
    ///
    /// Note: slot clearing relies on indicatif's default `ProgressFinish::AndClear`
    /// firing when the slot bars drop (verified against indicatif 0.17.11), so
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

    /// The object counter as rendered: `"12/2847 objects"`.
    #[allow(dead_code)]
    pub(crate) fn overall_message(&self) -> String {
        self.inner
            .overall
            .as_ref()
            .map(|o| o.message())
            .unwrap_or_default()
    }

    /// The overall bar's span: the denominator of `1.1/4.7GiB`, and what
    /// its percentage and ETA are computed against.
    #[allow(dead_code)]
    pub(crate) fn overall_length(&self) -> u64 {
        self.inner
            .overall
            .as_ref()
            .and_then(|o| o.length())
            .unwrap_or(0)
    }

    /// Slots currently owned by a task: the grid size minus the free list.
    #[allow(dead_code)]
    pub(crate) fn active_slots(&self) -> usize {
        self.inner.lanes.len() - self.lock_state().free.len()
    }

    /// Fixed grid size: decided once at construction and never grown or
    /// shrunk for the life of this `ProgressUi`.
    #[allow(dead_code)]
    pub(crate) fn slot_total(&self) -> usize {
        self.inner.lanes.len()
    }
}

struct NotifierState {
    /// Position on the task's own axis (bytes, items, or 0..PERCENT_SCALE).
    /// The truth: every report lands here immediately, whatever the paint
    /// gate does about showing it.
    pos: u64,
    /// How much of `pos` the bars have actually been told about. Lags `pos`
    /// by up to [`PAINT_INTERVAL`]; the difference is what the next
    /// [`NotifierInner::paint`] hands over. Tracked as an absolute rather
    /// than a pending delta so a backwards move ([`ProgressNotifier::rewind`])
    /// is the same subtraction as any other.
    painted: u64,
    /// When `painted` last caught up, i.e. what the gate measures against.
    last_paint: Instant,
    /// Full-scale value. Starts at `ProgressTask::span` and moves with
    /// [`ProgressNotifier::set_total`].
    span: u64,
    life: TaskState,
}

struct NotifierInner {
    ui: ProgressUi,
    /// The slot this task owns exclusively until release, or `None` if it
    /// found every slot taken and is running silently.
    slot: Option<usize>,
    /// Cached `task.byte_shaped()`: whether reports here also move the
    /// `TOTAL` row.
    byte_shaped: bool,
    task: Box<dyn ProgressTask>,
    state: Mutex<NotifierState>,
}

impl NotifierInner {
    fn bar(&self) -> Option<&ProgressBar> {
        self.slot.map(|idx| &self.ui.inner.lanes[idx])
    }

    fn overall(&self) -> Option<&ProgressBar> {
        self.ui.inner.overall.as_ref()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, NotifierState> {
        self.state.lock().expect("ProgressNotifier state poisoned")
    }

    /// Runs one report: drops it if the task is already completed,
    /// otherwise applies `f` to the bookkeeping and the slot together.
    ///
    /// Every public reporting method funnels through here, and `f` runs
    /// with the lock **held across the writes to the slot** -- that is the
    /// exclusivity guarantee, not mere tidiness. A slot is handed back the
    /// instant `finish` runs and can be re-claimed by an unrelated task
    /// immediately, so a report that checked "am I still running?",
    /// released the lock, and only then touched the bar would be a write
    /// into somebody else's row. Holding the lock makes
    /// check-and-write one step, and `finish` takes the same lock, so the
    /// two serialize.
    ///
    /// Safe to hold across those writes because the lock order has no
    /// cycle: this lock is only ever taken first, indicatif's internals
    /// never call back into a `NotifierState`, and `ProgressUi::begin`
    /// releases the grid's own lock before dressing the slot it claimed.
    fn report(&self, f: impl FnOnce(&Self, &mut NotifierState)) {
        let mut state = self.lock();
        if state.life == TaskState::Completed {
            return;
        }
        f(self, &mut state);
    }

    /// Hands the gap between `painted` and `pos` over to this task's slot
    /// and, for byte-shaped work, to the `TOTAL` row -- but only if
    /// [`PAINT_INTERVAL`] has passed since the last time, unless `force`.
    ///
    /// This is the whole throttle. The slot takes an absolute
    /// `set_position`, so a withheld report costs nothing but freshness;
    /// the `TOTAL` row is shared and takes the *delta*, so that it stays
    /// exactly the sum of every task's painted position rather than being
    /// overwritten by whichever lane reported last.
    ///
    /// `force` is for the paths where withholding would be a lie rather
    /// than a lag: [`ProgressNotifier::rewind`] (leaving already-debited
    /// bytes on the `TOTAL` row would overcount a retry that is about to
    /// re-send them), [`ProgressNotifier::finish`], and `Drop`.
    fn paint(&self, state: &mut NotifierState, force: bool) {
        // Nothing has moved since the last paint, so there is nothing to
        // draw: the bars already say what we would tell them. Checked first
        // so a ticker pass over an idle grid touches neither the clock nor
        // the `MultiProgress`, and so that a no-op pass doesn't reset the
        // gate's clock and delay the next real report by a whole interval.
        if state.painted == state.pos {
            return;
        }
        let now = Instant::now();
        if !force && now.duration_since(state.last_paint) < self.ui.inner.paint_interval {
            return;
        }
        state.last_paint = now;
        if let Some(bar) = self.bar() {
            bar.set_position(state.pos);
        }
        if self.byte_shaped
            && let Some(overall) = self.overall()
        {
            match state.pos.checked_sub(state.painted) {
                Some(delta) => overall.inc(delta),
                None => overall.dec(state.painted - state.pos),
            }
        }
        state.painted = state.pos;
    }

    /// Moves the task forward by `n` on its own axis, and the `TOTAL` row
    /// with it when the axis is bytes. The hot path -- a download reports
    /// here once per 64KiB read -- so it goes through the gate.
    fn credit(&self, state: &mut NotifierState, n: u64) {
        state.pos += n;
        self.paint(state, false);
    }

    /// The inverse: un-counts `n` (clamped at zero), including from the
    /// `TOTAL` row, for a retry that re-streams from the start. Forced
    /// through the gate -- see [`paint`](Self::paint).
    fn debit(&self, state: &mut NotifierState, n: u64) {
        state.pos -= n.min(state.pos);
        self.paint(state, true);
    }

    fn seek(&self, state: &mut NotifierState, done: u64) {
        match done < state.pos {
            true => self.debit(state, state.pos - done),
            false => self.credit(state, done - state.pos),
        }
    }
}

/// The only channel from running work to the screen. Operation code holds
/// one of these and never touches a `ProgressBar` or the grid: it reports
/// what it did, and the notifier drives whichever slot its task owns.
///
/// Cheap to clone and safe to report from concurrent futures and from
/// inside a retryable-body closure. Every method is inert on a
/// [`noop`](Self::noop) notifier (progress disabled) and on a silent one
/// (no slot was free), so callers never branch on whether a UI exists.
#[derive(Clone)]
pub(crate) struct ProgressNotifier {
    inner: Option<Arc<NotifierInner>>,
}

impl ProgressNotifier {
    /// Inert notifier for when progress is disabled (non-TTY/--json/
    /// --quiet/--no-color).
    pub(crate) fn noop() -> Self {
        Self { inner: None }
    }

    pub(crate) fn is_noop(&self) -> bool {
        self.inner.is_none()
    }

    /// [`TaskState::NotStarted`] -> [`TaskState::Running`]: the work this
    /// task was queued for has begun. Idempotent, and a no-op for a task
    /// that started running immediately.
    pub(crate) fn started(&self) {
        let Some(inner) = &self.inner else { return };
        inner.report(|inner, state| {
            if state.life != TaskState::NotStarted {
                return;
            }
            state.life = TaskState::Running;
            if let Some(bar) = inner.bar() {
                inner
                    .task
                    .dress(bar, inner.ui.inner.bar_width, TaskState::Running);
            }
        });
    }

    /// Report `n` more units done on the task's own axis. For a byte-shaped
    /// task this is also `n` more bytes on the `TOTAL` row.
    pub(crate) fn advance(&self, n: u64) {
        let Some(inner) = &self.inner else { return };
        inner.report(|inner, state| inner.credit(state, n));
    }

    /// Absolute form of [`advance`](Self::advance), for work that knows its
    /// running total rather than its increments. Moving backwards is
    /// allowed (and is what [`rewind`](Self::rewind) is built on).
    #[allow(dead_code)] // set_fraction is the caller today; kept as the general form
    pub(crate) fn set_done(&self, done: u64) {
        let Some(inner) = &self.inner else { return };
        inner.report(|inner, state| inner.seek(state, done));
    }

    /// Absolute progress as a 0.0..=1.0 fraction -- the natural report for
    /// a [`Measure::Percent`] task, and valid for any other flavor too.
    #[allow(dead_code)] // no Percent-flavored caller yet
    pub(crate) fn set_fraction(&self, fraction: f64) {
        let Some(inner) = &self.inner else { return };
        inner.report(|inner, state| {
            let done = (state.span as f64 * fraction.clamp(0.0, 1.0)).round() as u64;
            inner.seek(state, done);
        });
    }

    /// Re-scale the task mid-flight, for work that discovers more (or less)
    /// to do than it was created with -- a directory walk that finds
    /// another thousand files, a listing that turns out to have more pages.
    ///
    /// Deliberately does **not** touch the `TOTAL` row's length even for a
    /// byte-shaped task: that total is owned by
    /// [`add_object`](ProgressUi::add_object), which counts whole objects,
    /// and a per-part rescale here would double-count against it.
    pub(crate) fn set_total(&self, total: u64) {
        let Some(inner) = &self.inner else { return };
        inner.report(|inner, state| {
            state.span = total;
            if let Some(bar) = inner.bar() {
                bar.set_length(total);
            }
        });
    }

    /// Free-text status shown to the right of the readout -- whatever the
    /// operation wants to say that the numbers can't ("retrying", the
    /// current sub-path). Empty string clears it.
    pub(crate) fn set_detail(&self, detail: impl Into<std::borrow::Cow<'static, str>>) {
        let Some(inner) = &self.inner else { return };
        inner.report(|inner, _state| {
            if let Some(bar) = inner.bar() {
                bar.set_message(detail);
            }
        });
    }

    /// Back to the start of this task (an upload part retry re-streams from
    /// offset 0) -- un-counting whatever it had already reported, including
    /// from the `TOTAL` row.
    pub(crate) fn rewind(&self) {
        let Some(inner) = &self.inner else { return };
        inner.report(|inner, state| inner.debit(state, state.pos));
    }

    /// [`TaskState::Completed`]: hand the slot back to idle. Idempotent.
    ///
    /// A byte-shaped task also settles up, topping the `TOTAL` row by
    /// whatever it never got round to reporting -- a body that ticks
    /// slightly under its content length must not leave the total short.
    /// Count and percent work is left exactly where it got to instead:
    /// there is no rounding to settle, and snapping would overstate what
    /// happened (a half-failed delete phase reading as complete).
    pub(crate) fn finish(&self) {
        let Some(inner) = &self.inner else { return };
        // Marking completed and releasing the slot happen under the same
        // lock every report takes, so no in-flight report can still be
        // between its check and its write when the slot goes back.
        inner.report(|inner, state| {
            state.life = TaskState::Completed;
            if inner.byte_shaped {
                state.pos = state.pos.max(state.span);
            }
            // Forced: whatever the gate was still holding back settles up
            // here, along with the top-up above, so a task's contribution to
            // the `TOTAL` row is complete the moment it completes.
            inner.paint(state, true);
            if let Some(idx) = inner.slot {
                inner.ui.release_slot(idx);
            }
        });
    }
}

impl Drop for NotifierInner {
    /// A task dropped without finishing (a failed part) hands its slot back
    /// but must not fake completion by topping up the `TOTAL` row.
    ///
    /// Recovers a poisoned lock rather than bailing out: the alternative is
    /// a slot that no task owns and no task can ever claim, which shrinks
    /// the usable grid for the rest of the run. Reading through the poison
    /// is safe here because the flag it checks is exactly what decides
    /// between releasing once and not at all -- it can never double-release
    /// a slot `finish` already returned.
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let unfinished = state.life != TaskState::Completed;
        if unfinished {
            // Settle whatever the paint gate is still holding. A failed part
            // must not fake completion (that is `finish`'s top-up, and it
            // stays out of here), but the bytes it really did move were
            // counted before this change and must go on being counted: the
            // gate is a display delay, not a discard.
            self.paint(&mut state, true);
        }
        drop(state);
        if unfinished && let Some(idx) = self.slot {
            self.ui.release_slot(idx);
        }
    }
}

pin_project_lite::pin_project! {
    /// Reports to a [`ProgressNotifier`] as each data frame is polled onto
    /// the wire.
    struct ProgressBody {
        #[pin]
        inner: SdkBody,
        notifier: ProgressNotifier,
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
            this.notifier.advance(data.len() as u64);
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

/// Wraps an upload body so every chunk sent is reported to `notifier`. Applied through
/// `SdkBody::map_preserve_contents` -- not plain `map` -- since this wrapper
/// only observes byte counts and never alters the data: `map` would drop
/// `bytes_contents`, which flips `body.bytes()` from `Some` to `None` and
/// silently downgrades SigV4 signing from a signed payload to
/// `UNSIGNED-PAYLOAD` for any in-memory body (aws-runtime's sigv4 only
/// selects `SignableBody::Bytes` when `bytes()` is `Some`).
/// `map_preserve_contents` re-applies on each retry attempt's clone just
/// like `map` does -- the closure rewinds first so a retried part never
/// double-counts. No-op for noop notifiers (progress disabled).
pub(crate) fn instrument_body(body: ByteStream, notifier: &ProgressNotifier) -> ByteStream {
    if notifier.is_noop() {
        return body;
    }
    let notifier = notifier.clone();
    ByteStream::new(body.into_inner().map_preserve_contents(move |inner| {
        notifier.rewind();
        SdkBody::from_body_1_x(ProgressBody {
            inner,
            notifier: notifier.clone(),
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lbl(verb: Verb, path: &str, part: Option<(u64, u64)>) -> TransferLabel {
        TransferLabel {
            verb,
            path: path.into(),
            part,
        }
    }

    fn bytes_task(path: &str, len: u64) -> ProgressAwareTask {
        ProgressAwareTask::bytes(lbl(Verb::Uploading, path, None), len)
    }

    fn opaque_task(path: &str, api: &'static str) -> ProgressOpaqueTask {
        ProgressOpaqueTask::new(lbl(Verb::Listing, path, None), api)
    }

    #[test]
    fn lane_count_is_p_capped_by_console_rows() {
        assert_eq!(lane_count(5, Some(40), 2), 5, "plenty of rows -> P slots");
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

    // --- the slot model: fixed grid, exclusive ownership, back to idle ---

    #[test]
    fn grid_is_stable_and_slots_recycle() {
        let ui = ProgressUi::hidden(3);
        assert_eq!(ui.slot_total(), 3);
        assert_eq!(ui.active_slots(), 0, "all idle at start");
        let a = ui.start(bytes_task("a", 10));
        let t = ui.start(opaque_task("b", "ListObjectsV2"));
        assert_eq!(ui.active_slots(), 2);
        assert_eq!(ui.slot_total(), 3, "grid never grows");
        let c = ui.start(bytes_task("c", 10));
        let overflow = ui.start(bytes_task("d", 10));
        assert_eq!(ui.active_slots(), 3, "4th is silent overflow");
        overflow.advance(7);
        a.finish();
        assert_eq!(ui.active_slots(), 2, "slot freed");
        let e = ui.start(bytes_task("e", 10));
        assert_eq!(ui.active_slots(), 3, "freed slot reclaimed");
        assert_eq!(ui.slot_total(), 3, "grid never shrinks");
        drop((t, c, e));
        assert_eq!(ui.active_slots(), 0, "drops release slots");
    }

    /// Both task kinds draw from the same pool, and neither can add a row
    /// to escape it -- the invariant that keeps the drawn block a fixed
    /// height no matter how much work is in flight.
    #[test]
    fn both_task_kinds_share_one_fixed_pool() {
        let ui = ProgressUi::hidden(10);
        ui.add_object(100);
        let bars: Vec<ProgressNotifier> = (0..9)
            .map(|i| {
                ui.start(ProgressAwareTask::bytes(
                    lbl(Verb::Uploading, "x", Some((i + 1, 9))),
                    10,
                ))
            })
            .collect();
        let t1 = ui.start(ProgressOpaqueTask::new(
            lbl(Verb::Creating, "asdf/a.img", None),
            "CreateMultipartUpload",
        ));
        assert_eq!(ui.active_slots(), 10, "opaque task takes the 10th slot");
        let t2 = ui.start(opaque_task("bucket/p", "ListObjectsV2"));
        assert_eq!(ui.active_slots(), 10, "11th is silent");
        assert_eq!(ui.slot_total(), 10, "and the grid did not grow for it");
        t1.finish();
        t2.finish();
        assert_eq!(ui.active_slots(), 9);
        assert_eq!(ui.overall_position(), 0, "opaque tasks contribute no bytes");
        drop(bars);
    }

    /// A session that plans (mirror, and so `cp -r`/`mv -r`) hands over its
    /// whole workload at once. The transfer functions it shares with the
    /// unplanned paths still call `add_object` as each object starts, and
    /// those must not pile on top of the declared figure.
    #[test]
    fn declared_total_is_final_and_add_object_stops_counting() {
        let ui = ProgressUi::hidden(4);
        ui.declare_total(2847, 4_000_000);
        assert_eq!(ui.overall_length(), 4_000_000);
        assert_eq!(ui.overall_message(), "0/2847 objects");
        // What upload_file/download_object do as each object starts.
        for _ in 0..6 {
            ui.add_object(1000);
        }
        assert_eq!(ui.overall_length(), 4_000_000, "declared span is final");
        assert_eq!(
            ui.overall_message(),
            "0/2847 objects",
            "and so is the declared count"
        );
    }

    /// The denominator has to lead the work, not trail it: that is the
    /// entire point of declaring. Before this, the total was only ever
    /// `done + parallel`, so the row read `1/2 objects` here.
    #[test]
    fn declared_total_leads_the_work_it_describes() {
        let ui = ProgressUi::hidden(4);
        ui.declare_total(500, 500_000);
        let one = ui.start(bytes_task("a", 1000));
        one.advance(1000);
        one.finish();
        ui.object_done();
        assert_eq!(ui.overall_message(), "1/500 objects");
        assert_eq!(ui.overall_position(), 1000);
        assert_eq!(ui.overall_length(), 500_000);
    }

    /// No declaration, no plan: a single-object `cp`/`put`/`get` still
    /// grows its total one object at a time, which is correct there.
    #[test]
    fn undeclared_total_still_accretes_per_object() {
        let ui = ProgressUi::hidden(4);
        ui.add_object(500);
        ui.add_object(500);
        assert_eq!(ui.overall_length(), 1000);
        assert_eq!(ui.overall_message(), "0/2 objects");
    }

    #[test]
    fn eleventh_concurrent_task_is_silent_but_still_counts() {
        let ui = ProgressUi::hidden(10);
        ui.add_object(11 * 100);
        let handles: Vec<ProgressNotifier> = (0..11)
            .map(|i| ui.start(bytes_task(&format!("u{i}"), 100)))
            .collect();
        assert_eq!(ui.active_slots(), 10, "cap is 10");
        // the silent 11th task still reaches the overall bar
        handles[10].advance(40);
        assert_eq!(ui.overall_position(), 40);
    }

    #[test]
    fn finish_frees_slot_for_next_task() {
        let ui = ProgressUi::hidden(10);
        let handles: Vec<ProgressNotifier> = (0..10)
            .map(|i| ui.start(bytes_task(&format!("u{i}"), 10)))
            .collect();
        assert_eq!(ui.active_slots(), 10);
        handles[0].finish();
        assert_eq!(ui.active_slots(), 9);
        let _h = ui.start(bytes_task("next", 10));
        assert_eq!(ui.active_slots(), 10);
    }

    // --- ProgressOpaqueTask: the lifecycle IS its progress model ---

    #[test]
    fn enqueued_opaque_task_holds_its_slot_from_before_it_starts() {
        // `dispatch` enqueues before acquiring a budget token so queued
        // work is visible; the slot must be owned across that whole window,
        // not claimed at the moment work begins.
        let ui = ProgressUi::hidden(2);
        let t = ui.enqueue(opaque_task("bucket/p", "ListObjectsV2"));
        assert_eq!(ui.active_slots(), 1, "queued task already owns a slot");
        t.started();
        assert_eq!(ui.active_slots(), 1, "and keeps the same one when running");
        t.started(); // idempotent
        assert_eq!(ui.active_slots(), 1);
        t.finish();
        assert_eq!(ui.active_slots(), 0, "completed: slot back to idle");
    }

    #[test]
    fn started_after_finish_does_not_resurrect_a_completed_task() {
        let ui = ProgressUi::hidden(2);
        let t = ui.enqueue(opaque_task("bucket/p", "ListObjectsV2"));
        t.finish();
        assert_eq!(ui.active_slots(), 0);
        t.started();
        t.advance(5);
        assert_eq!(ui.active_slots(), 0, "terminal state stays terminal");
    }

    // --- render exclusivity: one slot, one owner, no stale writes ---

    #[test]
    fn a_stale_notifier_cannot_write_into_a_reassigned_slot() {
        // A `ProgressNotifier` is `Clone` and outlives its slot: `finish`
        // hands the slot straight back, and the very next task can own it.
        // Every report must therefore be inert after completion -- not just
        // the counting ones. `set_detail` originally checked nothing and
        // would paint one task's text into another task's row.
        let ui = ProgressUi::hidden(1); // exactly one slot: b is guaranteed a's
        let a = ui.start(bytes_task("a", 100));
        a.advance(10);
        let stale = a.clone();
        a.finish();

        let b = ui.start(ProgressAwareTask::count(
            lbl(Verb::Removing, "b", None),
            8,
            "{done}/{total} obj",
        ));
        b.advance(3);
        b.set_detail("b's text");

        stale.started();
        stale.advance(50);
        stale.set_done(90);
        stale.set_fraction(1.0);
        stale.set_total(999);
        stale.set_detail("a's text");
        stale.rewind();
        stale.finish();

        let slot = &ui.inner.lanes[0];
        assert_eq!(slot.message(), "b's text", "stale write reached the slot");
        assert_eq!(slot.length(), Some(8), "stale set_total resized the slot");
        assert_eq!(slot.position(), 3, "stale counting reached the slot");
        assert_eq!(ui.active_slots(), 1, "and b still owns it");
        // a was byte-shaped and finished at 100; nothing after that counts.
        assert_eq!(ui.overall_position(), 100);
        b.finish();
        assert_eq!(ui.active_slots(), 0);
    }

    #[test]
    fn a_released_slot_carries_nothing_over_to_its_next_owner() {
        let ui = ProgressUi::hidden(1);
        let a = ui.start(bytes_task("averylongpath/a.bin", 100));
        a.advance(60);
        a.set_detail("halfway");
        a.finish();
        let slot = &ui.inner.lanes[0];
        assert_eq!(slot.position(), 0, "released slot keeps no position");
        assert_eq!(slot.length(), Some(0), "released slot keeps no length");
        assert_eq!(slot.message(), "", "released slot keeps no detail text");
        assert_eq!(slot.prefix(), IDLE_MESSAGE, "released slot reads as idle");
    }

    #[test]
    fn slots_are_conserved_across_churn_of_both_task_kinds() {
        // The failure this guards is a slot leaked (released zero times) or
        // double-released (pushed twice, handing one row to two owners).
        // Either shows up as the free list not returning to its full size.
        let ui = ProgressUi::hidden(4);
        for round in 0..50u64 {
            let a = ui.start(bytes_task("a", 10));
            let b = ui.start(opaque_task("b", "ListObjectsV2"));
            let c = ui.enqueue(opaque_task("c", "HeadObject"));
            c.started();
            let d = ui.start(ProgressAwareTask::count(
                lbl(Verb::Removing, "d", None),
                4,
                "{done}/{total} obj",
            ));
            let overflow = ui.start(bytes_task("e", 10)); // no slot left
            assert_eq!(ui.active_slots(), 4, "round {round}");
            a.advance(4);
            match round % 3 {
                // finished explicitly
                0 => {
                    a.finish();
                    b.finish();
                    c.finish();
                    d.finish();
                }
                // dropped without finishing (the failed-part path)
                1 => drop((a, b, c, d)),
                // a mix, plus a double finish that must not double-release
                _ => {
                    a.finish();
                    a.finish();
                    drop((b, c));
                    d.finish();
                    drop(a);
                }
            }
            drop(overflow);
            assert_eq!(ui.active_slots(), 0, "every slot returned, round {round}");
            assert_eq!(ui.slot_total(), 4, "grid unchanged, round {round}");
        }
    }

    #[test]
    fn concurrent_churn_never_hands_one_slot_to_two_owners() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // Threads racing claim against release: if a slot were ever handed
        // out twice, the peak occupancy would exceed the grid size, and if
        // one leaked, the final count would not return to zero.
        let ui = ProgressUi::hidden(4);
        let peak = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|s| {
            for t in 0..8 {
                let ui = ui.clone();
                let peak = Arc::clone(&peak);
                s.spawn(move || {
                    for i in 0..200u64 {
                        let h = ui.start(bytes_task(&format!("t{t}-{i}"), 10));
                        peak.fetch_max(ui.active_slots(), Ordering::SeqCst);
                        h.advance(5);
                        if i % 2 == 0 { h.finish() } else { drop(h) }
                    }
                });
            }
        });
        assert!(
            peak.load(Ordering::SeqCst) <= 4,
            "peak occupancy {} exceeded the {} slots in the grid",
            peak.load(Ordering::SeqCst),
            ui.slot_total()
        );
        assert_eq!(ui.active_slots(), 0, "no slot leaked");
    }

    // --- ProgressAwareTask: the measures ---

    #[test]
    fn only_byte_shaped_work_reaches_the_total_row() {
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let counted = ui.start(ProgressAwareTask::count(
            lbl(Verb::Removing, "bucket/p", None),
            8,
            "{done}/{total} obj",
        ));
        counted.advance(3);
        assert_eq!(
            ui.overall_position(),
            0,
            "a count-shaped task must not move the byte total"
        );
        counted.finish();
        assert_eq!(ui.overall_position(), 0, "not on finish either");
        let moved = ui.start(bytes_task("f", 100));
        moved.advance(30);
        assert_eq!(ui.overall_position(), 30);
    }

    #[test]
    fn set_total_rescales_mid_flight_without_touching_the_byte_total() {
        // A directory walk that discovers more work (mirror's local scan).
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let scan = ui.start(ProgressAwareTask::count(
            lbl(Verb::Scanning, ".", None),
            1,
            "{done}/{total} dirs",
        ));
        scan.advance(1);
        scan.set_total(4); // found three more directories
        scan.advance(2);
        scan.finish();
        assert_eq!(
            ui.overall_position(),
            0,
            "rescaling a count task must not credit the byte total"
        );
        assert_eq!(ui.overall_length(), 100, "nor change its length");
    }

    #[test]
    fn byte_task_set_total_leaves_the_total_row_to_add_object() {
        // `add_object` owns the TOTAL length; a per-part rescale must not
        // also move it, or the two would double-count each other.
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        h.set_total(60);
        assert_eq!(ui.overall_length(), 100);
        h.advance(60);
        h.finish();
        assert_eq!(ui.overall_position(), 60, "finish tops up to the new span");
    }

    #[test]
    fn set_done_and_set_fraction_move_absolutely_in_both_directions() {
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        h.set_done(40);
        assert_eq!(ui.overall_position(), 40);
        h.set_done(70);
        assert_eq!(ui.overall_position(), 70, "forward is a delta advance");
        h.set_done(25);
        assert_eq!(ui.overall_position(), 25, "backward un-counts the delta");
        h.set_fraction(0.5);
        assert_eq!(ui.overall_position(), 50);
        h.set_fraction(3.0);
        assert_eq!(ui.overall_position(), 100, "clamped to full scale");
        h.set_fraction(-1.0);
        assert_eq!(ui.overall_position(), 0, "clamped to zero");
    }

    #[test]
    fn finish_tops_up_bytes_to_span_exactly_once() {
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        h.advance(30);
        h.finish();
        assert_eq!(ui.overall_position(), 100, "topped up 30 -> 100");
        h.finish(); // idempotent
        assert_eq!(ui.overall_position(), 100);
    }

    #[test]
    fn rewind_subtracts_progress_for_retry() {
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        h.advance(40);
        assert_eq!(ui.overall_position(), 40);
        h.rewind();
        assert_eq!(ui.overall_position(), 0);
        h.advance(100);
        h.finish();
        assert_eq!(ui.overall_position(), 100);
    }

    #[test]
    fn drop_without_finish_frees_slot_but_does_not_top_up() {
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        h.advance(30);
        drop(h); // a failed part must not fake completion
        assert_eq!(ui.overall_position(), 30);
        assert_eq!(ui.active_slots(), 0);
    }

    // --- the paint gate ---
    // Reports are always counted; handing them to indicatif is what gets
    // paced, because that side is a shared lock plus an ioctl per call (see
    // `PAINT_INTERVAL`). These use a deliberately huge interval so "the gate
    // is shut" is a fact about the test, not a race with the wall clock.

    const SHUT: Duration = Duration::from_secs(3600);

    #[test]
    fn gate_withholds_mid_stream_reports_but_never_drops_them() {
        let ui = ProgressUi::hidden_paced(5, SHUT);
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        // First report paints on arrival: a task that reports once must not
        // sit at zero for an interval.
        h.advance(10);
        assert_eq!(ui.overall_position(), 10, "first report is not withheld");
        h.advance(20);
        h.advance(30);
        assert_eq!(
            ui.overall_position(),
            10,
            "the interval has not passed: later reports are counted, not shown"
        );
        // ...and finishing settles every withheld byte, plus the top-up.
        h.finish();
        assert_eq!(ui.overall_position(), 100);
    }

    #[test]
    fn gate_settles_on_rewind_so_a_retry_cannot_overcount() {
        let ui = ProgressUi::hidden_paced(5, SHUT);
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        h.advance(40);
        h.advance(35); // withheld
        h.rewind();
        assert_eq!(
            ui.overall_position(),
            0,
            "rewind is forced through: bytes about to be re-sent must go now"
        );
        h.advance(100);
        h.finish();
        assert_eq!(ui.overall_position(), 100, "and the retry counts once");
    }

    #[test]
    fn gate_settles_on_drop_without_faking_completion() {
        let ui = ProgressUi::hidden_paced(5, SHUT);
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        h.advance(10);
        h.advance(20); // withheld
        drop(h);
        assert_eq!(
            ui.overall_position(),
            30,
            "a failed part still reports the bytes it moved, and only those"
        );
        assert_eq!(ui.active_slots(), 0);
    }

    #[test]
    fn ticker_flushes_a_task_that_stopped_reporting() {
        let ui = ProgressUi::hidden_paced(5, Duration::from_millis(50));
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        h.advance(10); // first report paints on arrival
        h.advance(20); // withheld, and then the stream stalls
        assert_eq!(ui.overall_position(), 10);
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(
            ui.overall_position(),
            30,
            "a stall must not park the row on a stale figure"
        );
        drop(h);
    }

    #[test]
    fn ticker_stops_at_the_slot_it_was_registered_for() {
        let ui = ProgressUi::hidden_paced(2, Duration::from_millis(20));
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        h.advance(10);
        h.finish();
        assert_eq!(ui.active_slots(), 0, "slot released");
        // The ticker keeps running against a grid whose slots are all idle;
        // it must find nothing to do rather than paint through a released
        // handle.
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(ui.overall_position(), 100, "finish's figure stands");
    }

    /// The ticker takes the grid lock and then a notifier's; `finish` takes
    /// them the other way round. This churns both against each other on a
    /// deliberately tiny tick so the two are constantly interleaved -- a
    /// lock cycle would hang here rather than in front of a user. The
    /// accounting assertions are the same ones the unpaced churn test
    /// makes, since the ticker must not perturb them either.
    #[test]
    fn ticker_racing_claim_and_release_neither_deadlocks_nor_miscounts() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let ui = ProgressUi::hidden_paced(4, Duration::from_millis(1));
        let peak = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|s| {
            for t in 0..8 {
                let ui = ui.clone();
                let peak = Arc::clone(&peak);
                s.spawn(move || {
                    for i in 0..200u64 {
                        let h = ui.start(bytes_task(&format!("t{t}-{i}"), 10));
                        peak.fetch_max(ui.active_slots(), Ordering::SeqCst);
                        h.advance(5);
                        if i % 2 == 0 { h.finish() } else { drop(h) }
                    }
                });
            }
        });
        assert!(
            peak.load(Ordering::SeqCst) <= 4,
            "a slot was handed out twice"
        );
        assert_eq!(ui.active_slots(), 0, "no slot leaked");
        // 8 threads x 200 tasks of 10 bytes, half finished and half dropped:
        // the finished ones top up to 10, the dropped ones stop at the 5
        // they moved. Exact regardless of how many of those pushes the
        // ticker made rather than the worker that produced them.
        assert_eq!(ui.overall_position(), 800 * 10 + 800 * 5);
    }

    #[test]
    fn gate_opens_once_the_interval_passes() {
        let ui = ProgressUi::hidden_paced(5, Duration::from_millis(1));
        ui.add_object(100);
        let h = ui.start(bytes_task("f", 100));
        h.advance(10);
        std::thread::sleep(Duration::from_millis(5));
        h.advance(20);
        assert_eq!(ui.overall_position(), 30, "interval elapsed: caught up");
    }

    #[test]
    fn noop_notifier_is_inert() {
        let h = ProgressNotifier::noop();
        assert!(h.is_noop());
        h.started();
        h.advance(5);
        h.set_done(3);
        h.set_fraction(0.5);
        h.set_total(9);
        h.set_detail("x");
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

    #[test]
    fn concurrent_rewind_does_not_lose_other_task_progress() {
        let ui = ProgressUi::hidden(5);
        ui.add_object(100);
        let h_a = ui.start(bytes_task("a", 50));
        let h_b = ui.start(bytes_task("b", 50));
        h_a.advance(40);
        assert_eq!(ui.overall_position(), 40);
        h_b.advance(25);
        assert_eq!(ui.overall_position(), 65);
        // Unit A rewinds (atomic dec, not racy read-modify-write)
        h_a.rewind();
        assert_eq!(
            ui.overall_position(),
            25,
            "rewind must atomically subtract without losing concurrent increments"
        );
    }

    // --- the no-TOTAL-row shape (`worker_ui`'s) ---
    // Standalone commands get the same grid, just without the TOTAL row.
    // These pin that the missing row is well-defined rather than a panic
    // waiting to happen, and that the grid itself is still there -- the
    // structural change from the old "tasks-only" mode, which had no
    // persistent rows at all because raw stdout writes would have collided
    // with them (now handled by `suspend_bars`).

    #[test]
    fn without_total_still_builds_the_same_fixed_grid() {
        let ui = ProgressUi::hidden_without_total(5);
        assert_eq!(ui.slot_total(), 5, "the grid is not the TOTAL row's job");
        let t = ui.start(opaque_task("bucket/p", "ListObjectsV2"));
        assert_eq!(ui.active_slots(), 1, "task line takes a slot");
        assert_eq!(ui.slot_total(), 5, "and the grid is unchanged by it");
        t.finish();
        assert_eq!(ui.active_slots(), 0, "finish frees the slot");
    }

    #[test]
    fn without_total_reads_position_and_length_as_zero() {
        let ui = ProgressUi::hidden_without_total(5);
        assert_eq!(ui.overall_position(), 0, "no TOTAL row: reads as 0");
        assert_eq!(ui.overall_length(), 0, "no TOTAL row: reads as 0");
    }

    #[test]
    fn without_total_caps_at_the_same_slot_count() {
        let ui = ProgressUi::hidden_without_total(10);
        let tasks: Vec<ProgressNotifier> = (0..10)
            .map(|i| ui.start(opaque_task(&format!("b/{i}"), "ListObjectsV2")))
            .collect();
        assert_eq!(ui.active_slots(), 10);
        let eleventh = ui.start(opaque_task("b/x", "ListObjectsV2"));
        assert_eq!(ui.active_slots(), 10, "11th is silent, same cap");
        eleventh.finish();
        drop(tasks);
    }

    #[test]
    fn without_total_session_calls_do_not_panic() {
        // A standalone command never calls these, but they must degrade
        // gracefully rather than unwrap a `None` TOTAL row.
        let ui = ProgressUi::hidden_without_total(5);
        ui.add_object(100);
        ui.add_object(50);
        ui.object_done();
        ui.object_done();
        ui.object_done(); // more done than added: must not panic
        ui.finish_and_keep(); // no TOTAL row to finish: must not panic
        ui.clear(); // no bars left: must not panic or error
    }

    #[test]
    fn without_total_byte_task_reports_harmlessly() {
        let ui = ProgressUi::hidden_without_total(5);
        let h = ui.start(ProgressAwareTask::bytes(
            lbl(Verb::Downloading, "b/k", None),
            10,
        ));
        h.advance(4);
        h.rewind();
        h.advance(10);
        h.finish();
        assert_eq!(ui.overall_position(), 0, "still reads as 0: no TOTAL row");
    }

    // --- `suspend_bars` / the `LIVE_BARS` registry ---
    // Every terminal write that isn't a bar redraw has to go through
    // `suspend_bars`, or it scrolls the block the `MultiProgress` is about to
    // redraw in place and the grid starts duplicating itself down the screen
    // (see `LIVE_BARS`'s doc). These pin the registry that makes that
    // possible from print sites with no `ProgressUi` handle of their own.
    //
    // They deliberately do not assert the *global* registry is empty at any
    // point: the whole test binary shares it, and the other tests in this
    // module hold live `ProgressUi`s concurrently. Each asserts only about
    // the ids it created itself.

    fn registry_contains(id: u64) -> bool {
        lock_live_bars().iter().any(|(other, _)| *other == id)
    }

    #[test]
    fn live_ui_registers_and_deregisters_on_drop() {
        let ui = ProgressUi::hidden(3);
        let id = ui.inner.bars_id;
        assert!(registry_contains(id), "a live UI must be suspendable");
        // A clone shares the `Arc`, so the registry entry outlives it.
        let clone = ui.clone();
        drop(ui);
        assert!(registry_contains(id), "still live through the clone");
        drop(clone);
        assert!(!registry_contains(id), "last handle gone: entry released");
    }

    #[test]
    fn suspend_bars_runs_the_closure_exactly_once() {
        // No bars at all (the non-TTY/--json/--quiet case, and every moment
        // before or after a UI exists): still runs, still returns the value.
        let mut calls = 0;
        let out = suspend_bars(|| {
            calls += 1;
            "value"
        });
        assert_eq!((calls, out), (1, "value"));

        // One live display.
        let ui = ProgressUi::hidden(2);
        let mut calls = 0;
        let out = suspend_bars(|| {
            calls += 1;
            7u32
        });
        assert_eq!((calls, out), (1, 7));

        // Two: `suspend_all` nests over both rather than picking one.
        let other = ProgressUi::hidden_without_total(2);
        let mut calls = 0;
        suspend_bars(|| calls += 1);
        assert_eq!(calls, 1, "nested suspend must not re-run the closure");
        drop((ui, other));
    }

    #[test]
    fn suspend_bars_is_reentrant_safe_for_a_ui_built_inside_the_closure() {
        // `suspend_bars` clones the handles out and drops the registry lock
        // before running the closure, so caller code that happens to build
        // or tear down a `ProgressUi` can't deadlock against it.
        let outer = ProgressUi::hidden(2);
        let id = suspend_bars(|| {
            let inner = ProgressUi::hidden_without_total(1);
            let id = inner.inner.bars_id;
            assert!(registry_contains(id));
            id
        });
        assert!(
            !registry_contains(id),
            "inner UI dropped inside the closure"
        );
        drop(outer);
    }

    /// Source-level guard for the invariant `suspend_bars` exists to keep:
    /// a bare `println!`/`eprintln!` anywhere in the crate is a write that
    /// bypasses the choke point, and the failure it causes (a duplicated,
    /// endlessly-growing slot grid, with the message that triggered it
    /// scrolled away) only reproduces under a real TTY with a live UI --
    /// i.e. in front of a user, not in CI. Cheaper to forbid the call than
    /// to reason at each new site about whether a UI can be up.
    ///
    /// `progress.rs` (the macros' own bodies) and `output.rs` (which calls
    /// `suspend_bars` directly, closing over the raw macro) are the two
    /// definitions of the choke point, so they are the two exemptions.
    #[test]
    fn no_terminal_write_bypasses_suspend_bars() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = Vec::new();
        for entry in std::fs::read_dir(&src).expect("read src/") {
            let path = entry.expect("dir entry").path();
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if path.extension().is_none_or(|e| e != "rs")
                || name == "progress.rs"
                || name == "output.rs"
            {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("read source file");
            // `eprintln!(` contains `println!(` as a substring, so one scan
            // finds both; walk back over the identifier to recover which.
            for (idx, _) in text.match_indices("println!(") {
                let start = text[..idx]
                    .rfind(|c: char| !c.is_alphanumeric() && c != '_')
                    .map_or(0, |i| i + 1);
                let macro_name = &text[start..idx + "println!".len()];
                if macro_name != "ui_println!" && macro_name != "ui_eprintln!" {
                    let line = text[..idx].lines().count();
                    offenders.push(format!("{name}:{line}: {macro_name}"));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "use ui_println!/ui_eprintln! (see `LIVE_BARS`) instead of:\n  {}",
            offenders.join("\n  ")
        );
    }

    // --- body instrumentation ---

    #[tokio::test]
    async fn progress_body_reports_exact_len() {
        use aws_smithy_types::body::SdkBody;
        use aws_smithy_types::byte_stream::ByteStream;

        let ui = ProgressUi::hidden(5);
        ui.add_object(10);
        let notifier = ui.start(ProgressAwareTask::bytes(
            lbl(Verb::Uploading, "mem", None),
            10,
        ));
        let body = ByteStream::new(SdkBody::from("0123456789"));
        let wrapped = instrument_body(body, &notifier);
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
        let notifier = ui.start(ProgressAwareTask::bytes(
            lbl(Verb::Uploading, "mem", None),
            10,
        ));
        let retryable = SdkBody::retryable(|| SdkBody::from("0123456789"));
        let wrapped = instrument_body(ByteStream::new(retryable), &notifier);
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
        let wrapped = instrument_body(body, &ProgressNotifier::noop());
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
        let notifier = ui.start(ProgressAwareTask::bytes(
            lbl(Verb::Uploading, "part", None),
            10,
        ));
        let body = ByteStream::read_from()
            .path(file.path())
            .offset(2)
            .length(Length::Exact(10))
            .build()
            .await
            .expect("build file body");
        let wrapped = instrument_body(body, &notifier).into_inner();
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
        let notifier = ui.start(ProgressAwareTask::bytes(
            lbl(Verb::Uploading, "mem", None),
            3,
        ));
        let body = ByteStream::new(SdkBody::from("abc"));
        let wrapped = instrument_body(body, &notifier).into_inner();
        assert!(
            wrapped.bytes().is_some(),
            "wrapping must not drop bytes_contents (would downgrade SigV4 to UNSIGNED-PAYLOAD)"
        );
        assert_eq!(wrapped.bytes(), Some(b"abc".as_slice()));
    }

    // --- pure formatting ---

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
        // width contract (and the padding in `label_column`) is
        // char-counted, so this assertion is too (str::len() is bytes and
        // would over-count the ellipsis, failing at exactly the boundary
        // this test targets).
        assert!(
            out.chars().count() <= 24,
            "{out:?} too wide ({} chars)",
            out.chars().count()
        );
        assert!(out.starts_with("Uploading …"), "{out:?}");
        assert!(out.ends_with(" part 2/9"), "{out:?}");
    }

    #[test]
    fn label_column_pads_to_a_fixed_width_by_chars() {
        let short = label_column(&lbl(Verb::Copying, "a.txt", None));
        assert_eq!(short.chars().count(), LABEL_WIDTH);
        // A condensed label carries a multi-byte `…`; padding is by chars,
        // so the column is still exactly LABEL_WIDTH wide on screen.
        let long = label_column(&lbl(
            Verb::Downloading,
            "backups/2026/07/31/big.iso",
            Some((1, 8)),
        ));
        assert_eq!(long.chars().count(), LABEL_WIDTH);
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
    fn every_measure_compiles_a_style_and_spans_correctly() {
        // A measure whose template fails to compile would silently leave a
        // claimed slot rendering in whatever style the previous task left.
        assert!(Measure::Bytes { total: 10 }.style(30).is_some());
        assert!(
            Measure::Count {
                total: 10,
                template: "{done}/{total} obj",
            }
            .style(30)
            .is_some()
        );
        assert!(Measure::Percent.style(30).is_some());
        assert_eq!(Measure::Bytes { total: 10 }.span(), 10);
        assert_eq!(
            Measure::Count {
                total: 7,
                template: "{done}/{total} obj",
            }
            .span(),
            7
        );
        assert_eq!(Measure::Percent.span(), PERCENT_SCALE);
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
