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
        let handles: Vec<UnitHandle> = (0..11).map(|i| ui.unit(format!("u{i}"), 100)).collect();
        assert_eq!(ui.active_detail_bars(), 10, "cap is 10");
        // the silent 11th unit still ticks the overall bar
        handles[10].inc(40);
        assert_eq!(ui.overall_position(), 40);
    }

    #[test]
    fn finish_frees_slot_for_next_unit() {
        let ui = ProgressUi::hidden();
        let handles: Vec<UnitHandle> = (0..10).map(|i| ui.unit(format!("u{i}"), 10)).collect();
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
        let unit = ui.unit("part".into(), 10);
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
        let unit = ui.unit("mem".into(), 3);
        let body = ByteStream::new(SdkBody::from("abc"));
        let wrapped = instrument_body(body, &unit).into_inner();
        assert!(
            wrapped.bytes().is_some(),
            "wrapping must not drop bytes_contents (would downgrade SigV4 to UNSIGNED-PAYLOAD)"
        );
        assert_eq!(wrapped.bytes(), Some(b"abc".as_slice()));
    }

    #[test]
    fn concurrent_rewind_does_not_lose_other_unit_progress() {
        let ui = ProgressUi::hidden();
        ui.add_object(100);
        let h_a = ui.unit("a".into(), 50);
        let h_b = ui.unit("b".into(), 50);
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
