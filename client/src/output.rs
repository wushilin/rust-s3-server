//! mc-shaped output core: the printer, JSON rendering rules, error
//! machinery, humanize, and exit-code plumbing that every later tier-2 task
//! builds on. See `docs/superpowers/research/mc-research-output.md` §1/§3
//! for the normative `mc` behavior this mirrors.
//!
//! `McMessage`/`print_msg`/`JsonStyle`/`humanize_ibytes`/`print_date` are now
//! consumed by `messages.rs` and its callers in `main.rs` (task 2).
//! `OutputOpts::quiet` now drives the transfer commands' bar-vs-lines
//! decision (task 3, `messages::TransferSession::new`). `no_color` is read
//! by `TransferSession::new` to disable the progress bar when `--no-color`
//! is set (task 2).

use std::sync::OnceLock;

/// Global output configuration, derived once from CLI flags + TTY
/// detection in `main()` and read everywhere via [`out()`].
pub(crate) struct OutputOpts {
    pub json: bool,
    pub quiet: bool,
    pub no_color: bool,
    pub stdout_tty: bool,
}

static OUTPUT_OPTS: OnceLock<OutputOpts> = OnceLock::new();

/// Initialize the global output state. Must be called at most once, before
/// any call to [`out()`]; typically the first thing `main()` does after
/// parsing CLI args.
pub(crate) fn init_output(opts: OutputOpts) {
    let _ = OUTPUT_OPTS.set(opts);
}

/// Read the global output state. Falls back to a quiet-ish, non-TTY,
/// non-JSON default if [`init_output`] was never called (e.g. in unit
/// tests that exercise pure helpers directly).
pub(crate) fn out() -> &'static OutputOpts {
    OUTPUT_OPTS.get_or_init(|| OutputOpts {
        json: false,
        quiet: false,
        no_color: true,
        stdout_tty: false,
    })
}

/// Controls how [`render_json`] indents/compacts a JSON value. Mirrors mc's
/// per-command quirks (e.g. `rb`/`ls --summarize` use a bare `\n` with no
/// leading spaces instead of the usual one-space indent).
pub(crate) enum JsonStyle {
    Standard,
    AlwaysCompact,
    EmptyIndent,
}

/// Implemented by every command's output message type. Mirrors mc's
/// `message` interface (`JSON()`/`String()`).
pub(crate) trait McMessage {
    /// Human-readable line, no trailing newline.
    fn human(&self) -> String;
    /// JSON representation, including a `"status"` field wherever mc's
    /// struct has one.
    fn json(&self) -> serde_json::Value;
    /// Rendering style for [`render_json`]; most messages use the default.
    fn json_style(&self) -> JsonStyle {
        JsonStyle::Standard
    }
}

/// Render `v` to a JSON string per `style` and whether stdout is a TTY.
/// Pure and unit-testable; does not consult global state.
pub(crate) fn render_json(v: &serde_json::Value, style: JsonStyle, tty: bool) -> String {
    match style {
        JsonStyle::Standard if tty => pretty_with_indent(v, b" "),
        JsonStyle::Standard => v.to_string(),
        JsonStyle::AlwaysCompact => v.to_string(),
        JsonStyle::EmptyIndent if tty => pretty_with_indent(v, b""),
        JsonStyle::EmptyIndent => v.to_string(),
    }
}

fn pretty_with_indent(v: &serde_json::Value, indent: &[u8]) -> String {
    let formatter = serde_json::ser::PrettyFormatter::with_indent(indent);
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
    // `Value` serialization cannot fail.
    serde::Serialize::serialize(v, &mut ser).expect("Value serialization is infallible");
    String::from_utf8(buf).expect("serde_json output is valid UTF-8")
}

/// Print `msg` to stdout: human text if `!out().json`, else JSON per
/// `msg.json_style()`. Mirrors mc's `printMsg` choke point.
///
/// Written through [`crate::progress::suspend_bars`] so a command that
/// prints while a progress display is live (any standalone command's
/// tasks-only spinner lines) lands above the bars rather than corrupting
/// their redraw accounting.
pub(crate) fn print_msg(msg: &dyn McMessage) {
    let opts = out();
    let line = if !opts.json {
        msg.human()
    } else {
        let v = msg.json();
        render_json(&v, msg.json_style(), opts.stdout_tty)
    };
    crate::progress::suspend_bars(|| println!("{line}"));
}

/// mc's punctuation rule for joining an error's context message with its
/// cause text (`cmd/error.go`'s `fatalIf`/`errorIf`):
/// - if `msg` doesn't already end in `:`/`.`, append `.` when `cause`
///   starts uppercase, else `:`
/// - ensure `cause` ends with `.`
/// - if `cause` is empty, `msg` is returned unchanged
pub(crate) fn join_error_text(msg: &str, cause: &str) -> String {
    let msg = msg.trim();
    let cause = cause.trim();
    if cause.is_empty() {
        return msg.to_string();
    }
    let mut msg = msg.to_string();
    if !msg.ends_with(':') && !msg.ends_with('.') {
        if cause.chars().next().is_some_and(|c| c.is_uppercase()) {
            msg.push('.');
        } else {
            msg.push(':');
        }
    }
    let mut cause = cause.to_string();
    if !cause.ends_with('.') {
        cause.push('.');
    }
    format!("{msg} {cause}")
}

/// Print an mc-shaped error. Human mode writes `{argv0}: <ERROR> <text>` to
/// stderr; `--json` mode writes the `{"status":"error","error":{...}}`
/// envelope (research doc §1.3) to stdout instead.
pub(crate) fn print_error(context_msg: &str, cause: &str, fatal: bool) {
    let opts = out();
    if !opts.json {
        let text = join_error_text(context_msg, cause);
        let prog = std::env::args()
            .next()
            .and_then(|p| {
                std::path::Path::new(&p)
                    .file_name()
                    .map(|f| f.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| "rs3".to_string());
        crate::progress::ui_eprintln!("{prog}: <ERROR> {text}");
    } else {
        let envelope = serde_json::json!({
            "status": "error",
            "error": {
                "message": context_msg,
                "cause": {
                    "message": cause,
                    "error": cause,
                },
                "type": if fatal { "fatal" } else { "error" },
            }
        });
        let line = render_json(&envelope, JsonStyle::Standard, opts.stdout_tty);
        crate::progress::suspend_bars(|| println!("{line}"));
    }
}

/// mc's `humanize.IBytes`-equivalent: binary (1024-based) byte
/// humanization with mc's exact formatting rules (`{:.1}` under 10 units,
/// `{:.0}` otherwise, plain integer bytes under 1024).
pub(crate) fn humanize_ibytes(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if bytes < 1024 {
        return format!("{bytes}B");
    }
    let mut value = bytes as f64;
    let mut unit_idx = 0;
    while value >= 1024.0 && unit_idx < UNITS.len() - 1 {
        value /= 1024.0;
        unit_idx += 1;
    }
    if value < 10.0 {
        format!("{value:.1}{}", UNITS[unit_idx])
    } else {
        format!("{value:.0}{}", UNITS[unit_idx])
    }
}

/// mc's local-time display format for timestamps (`stat`, `ls`, etc.).
pub(crate) fn print_date(t: chrono::DateTime<chrono::Utc>) -> String {
    let local = chrono::DateTime::<chrono::Local>::from(t);
    local.format("%Y-%m-%d %H:%M:%S %Z").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ibytes_matches_mc() {
        assert_eq!(humanize_ibytes(0), "0B");
        assert_eq!(humanize_ibytes(5), "5B");
        assert_eq!(humanize_ibytes(1024), "1.0KiB");
        assert_eq!(humanize_ibytes(15 * 1024), "15KiB");
        assert_eq!(humanize_ibytes(82_854_982), "79MiB");
        assert_eq!(humanize_ibytes(5 * 1024 * 1024 * 1024), "5.0GiB");
    }

    #[test]
    fn render_json_styles() {
        let v = serde_json::json!({"status":"success","bucket":"b"});
        assert_eq!(
            render_json(&v, JsonStyle::Standard, false),
            r#"{"status":"success","bucket":"b"}"#
        );
        let pretty = render_json(&v, JsonStyle::Standard, true);
        assert!(
            pretty.contains("\n \"bucket\""),
            "one-space indent, got: {pretty}"
        );
        assert_eq!(
            render_json(&v, JsonStyle::AlwaysCompact, true),
            r#"{"status":"success","bucket":"b"}"#
        );
        let empty = render_json(&v, JsonStyle::EmptyIndent, true);
        assert!(empty.contains("\n\"bucket\""), "empty indent, got: {empty}");
    }

    #[test]
    fn error_punctuation_rule() {
        // msg no terminal punct + cause starts uppercase => msg gets '.'
        assert_eq!(
            join_error_text("Unable to stat `x`", "Access Denied"),
            "Unable to stat `x`. Access Denied."
        );
        // cause starts lowercase => msg gets ':'
        assert_eq!(
            join_error_text("Unable to stat `x`", "no such key"),
            "Unable to stat `x`: no such key."
        );
        // msg already ends with '.' => untouched
        assert_eq!(join_error_text("Failed.", "boom"), "Failed. boom.");
        // empty cause => msg unchanged
        assert_eq!(join_error_text("Failed hard", ""), "Failed hard");
    }
}
