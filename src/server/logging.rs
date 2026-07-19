//! Logging setup.
//!
//! Two modes, chosen by config:
//! - **Split folder** (`logging.dir`): four purpose-built files —
//!   [`TARGET_AUTH`] → `auth.log` (who authenticated), [`TARGET_AUTHZ`] →
//!   `authz.log` (allow/deny decisions), [`TARGET_AUDIT`] → `audit.log` (who
//!   did what, and the outcome), and everything else → `server.log`. Each file
//!   shares the rotation/compression settings.
//! - **Single file / stdout** (`logging.file` or neither): the legacy behavior.
//!
//! Routing is by log *target*: call `log::info!(target: TARGET_AUTH, …)` and it
//! lands in `auth.log`. The category targets are non-additive, so their lines
//! do not also duplicate into `server.log` — but they still echo to the
//! console, so stdout remains a complete stream.

use std::path::Path;

use log::LevelFilter;
use log4rs::{
    append::{
        console::ConsoleAppender,
        rolling_file::{
            policy::compound::{
                roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
            },
            RollingFileAppender,
        },
    },
    config::{Appender, Config, Logger, Root},
    encode::pattern::PatternEncoder,
};

use super::config::LoggingConfig;

/// Log target for authentication events (who authenticated, how, request id).
pub const TARGET_AUTH: &str = "rusts3::auth";
/// Log target for authorization decisions (who is allowed/denied to do what).
pub const TARGET_AUTHZ: &str = "rusts3::authz";
/// Log target for the operation audit trail (who did what, success or failure).
pub const TARGET_AUDIT: &str = "rusts3::audit";

const PATTERN: &str = "{d(%Y-%m-%d %H:%M:%S%.3f)} {l:<5} {m}{n}";

fn encoder() -> Box<PatternEncoder> {
    Box::new(PatternEncoder::new(PATTERN))
}

/// Builds a size-rolling file appender that keeps `keep_files` archives and,
/// when `compress` is set, gzips them.
fn rolling_appender(
    path: &Path,
    rotation_bytes: u64,
    keep_files: u32,
    compress: bool,
) -> Result<RollingFileAppender, Box<dyn std::error::Error>> {
    let path_str = path.to_string_lossy();
    let stem = path_str
        .strip_suffix(".log")
        .map(str::to_string)
        .unwrap_or_else(|| path_str.to_string());
    let ext = if compress { "gz" } else { "log" };
    let archive_pattern = format!("{stem}.{{}}.{ext}");
    let roller = FixedWindowRoller::builder().build(&archive_pattern, keep_files)?;
    let trigger = SizeTrigger::new(rotation_bytes);
    let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));
    Ok(RollingFileAppender::builder()
        .encoder(encoder())
        .build(&*path_str, Box::new(policy))?)
}

pub fn init_logging(config: &LoggingConfig) -> Result<(), Box<dyn std::error::Error>> {
    let level: LevelFilter = config.level.parse().unwrap_or(LevelFilter::Info);
    let rotation_bytes = config.rotation_size_mb * 1024 * 1024;

    let console = ConsoleAppender::builder().encoder(encoder()).build();
    let mut builder =
        Config::builder().appender(Appender::builder().build("console", Box::new(console)));

    if let Some(dir) = &config.dir {
        // Split-folder mode: one file per category plus a catch-all server.log.
        let dir = Path::new(dir);
        std::fs::create_dir_all(dir)?;
        for (name, file) in [
            ("auth", "auth.log"),
            ("authz", "authz.log"),
            ("audit", "audit.log"),
            ("server", "server.log"),
        ] {
            let appender =
                rolling_appender(&dir.join(file), rotation_bytes, config.keep_files, config.compress)?;
            builder = builder.appender(Appender::builder().build(name, Box::new(appender)));
        }
        // Category targets route to their own file + console, and do NOT fall
        // through to server.log (additive = false).
        let categories = [
            (TARGET_AUTH, "auth"),
            (TARGET_AUTHZ, "authz"),
            (TARGET_AUDIT, "audit"),
        ];
        for (target, appender) in categories {
            builder = builder.logger(
                Logger::builder()
                    .appender(appender)
                    .appender("console")
                    .additive(false)
                    .build(target, level),
            );
        }
        let root = Root::builder()
            .appender("server")
            .appender("console")
            .build(level);
        log4rs::init_config(builder.build(root)?)?;
    } else if let Some(log_file) = &config.file {
        // Single combined file (legacy).
        let appender = rolling_appender(
            Path::new(log_file),
            rotation_bytes,
            config.keep_files,
            config.compress,
        )?;
        builder = builder.appender(Appender::builder().build("file", Box::new(appender)));
        let root = Root::builder()
            .appender("console")
            .appender("file")
            .build(level);
        log4rs::init_config(builder.build(root)?)?;
    } else {
        let root = Root::builder().appender("console").build(level);
        log4rs::init_config(builder.build(root)?)?;
    }

    Ok(())
}
