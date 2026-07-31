//! Multi-bar live progress for transfer commands: up to
//! [`MAX_DETAIL_BARS`] per-unit bars (one multipart segment of a large
//! file, or one whole small file) stacked above a persistent overall bar.
//! Deliberate TTY-only divergence from mc's single aggregate bar — see
//! README "Known divergences from mc". All bars draw to stderr; stdout
//! (the mc-compat message contract) is never touched.

use std::sync::{Arc, Mutex};

use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

const MAX_DETAIL_BARS: usize = 10;

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
    // Not yet constructed outside tests -- later tasks (dispatch call
    // sites for HEAD/create/complete/abort-multipart/list/delete) wire
    // these in.
    #[allow(dead_code)]
    Creating,
    #[allow(dead_code)]
    Completing,
    #[allow(dead_code)]
    Aborting,
    #[allow(dead_code)]
    Inspecting,
    #[allow(dead_code)]
    Listing,
    #[allow(dead_code)]
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

struct UiState {
    objects_total: u64,
    objects_done: u64,
    active_bars: usize,
}

struct UiInner {
    mp: MultiProgress,
    overall: ProgressBar,
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

impl ProgressUi {
    pub(crate) fn new() -> Self {
        let cols = console::Term::stderr().size_checked().map(|(_, c)| c);
        Self::with_target(ProgressDrawTarget::stderr(), bar_width_for(cols))
    }

    /// Hidden draw target for unit tests: full accounting, no rendering.
    #[cfg(test)]
    pub(crate) fn hidden() -> Self {
        Self::with_target(ProgressDrawTarget::hidden(), bar_width_for(None))
    }

    fn with_target(target: ProgressDrawTarget, bar_width: usize) -> Self {
        let mp = MultiProgress::with_draw_target(target);
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
        Self {
            inner: Arc::new(UiInner {
                mp,
                overall,
                bar_width,
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
    pub(crate) fn unit(&self, label: TransferLabel, len: u64) -> UnitHandle {
        let mut state = self.lock_state();
        let bar = if state.active_bars < MAX_DETAIL_BARS {
            state.active_bars += 1;
            let pb = self
                .inner
                .mp
                .insert_before(&self.inner.overall, ProgressBar::new(len));
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

    /// One in-flight byte-less S3 operation (HEAD, create/complete/abort
    /// multipart, list, delete...): a spinner line sharing the same
    /// cap-10 slot pool as [`unit`](Self::unit), contributing zero bytes
    /// to the overall bar. Silent handle when slots are exhausted, same
    /// as `unit`.
    // Not yet called outside tests -- `budget::dispatch` is its first
    // caller and later tasks wire dispatch into command call sites.
    #[allow(dead_code)]
    pub(crate) fn task(&self, label: TransferLabel, api: &'static str) -> UnitHandle {
        let mut state = self.lock_state();
        let bar = if state.active_bars < MAX_DETAIL_BARS {
            state.active_bars += 1;
            let pb = self
                .inner
                .mp
                .insert_before(&self.inner.overall, ProgressBar::new_spinner());
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
            Some(pb)
        } else {
            None
        };
        UnitHandle {
            inner: Some(Arc::new(UnitInner {
                ui: self.clone(),
                bar,
                len: 0,
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

    #[allow(dead_code)]
    pub(crate) fn overall_position(&self) -> u64 {
        self.inner.overall.position()
    }

    #[allow(dead_code)]
    pub(crate) fn overall_length(&self) -> u64 {
        self.inner.overall.length().unwrap_or(0)
    }

    #[allow(dead_code)]
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
        inner.ui.inner.overall.dec(pos);
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
        let finished = self.state.lock().map(|s| s.finished).unwrap_or(true);
        if !finished {
            self.ui.release_slot(&self.bar);
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
    fn eleventh_concurrent_unit_is_silent_but_still_counts() {
        let ui = ProgressUi::hidden();
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
        let ui = ProgressUi::hidden();
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
        let ui = ProgressUi::hidden();
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
        let ui = ProgressUi::hidden();
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
        let ui = ProgressUi::hidden();
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
        let ui = ProgressUi::hidden();
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

        let ui = ProgressUi::hidden();
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

        let ui = ProgressUi::hidden();
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

        let ui = ProgressUi::hidden();
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

        let ui = ProgressUi::hidden();
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
        let ui = ProgressUi::hidden();
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
        let ui = ProgressUi::hidden();
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
}
