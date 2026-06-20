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
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
};

use super::config::LoggingConfig;

const PATTERN: &str = "{d(%Y-%m-%d %H:%M:%S%.3f)} {l:<5} {m}{n}";

pub fn init_logging(config: &LoggingConfig) -> Result<(), Box<dyn std::error::Error>> {
    let level: LevelFilter = config.level.parse().unwrap_or(LevelFilter::Info);
    let encoder = Box::new(PatternEncoder::new(PATTERN));

    let console = ConsoleAppender::builder().encoder(encoder.clone()).build();

    let mut builder =
        Config::builder().appender(Appender::builder().build("console", Box::new(console)));

    if let Some(log_file) = &config.file {
        let rotation_bytes = config.rotation_size_mb * 1024 * 1024;

        let archive_pattern = if log_file.ends_with(".log") {
            format!("{}.{{}}.gz", &log_file[..log_file.len() - 4])
        } else {
            format!("{}.{{}}.gz", log_file)
        };

        let roller = FixedWindowRoller::builder().build(&archive_pattern, config.keep_files)?;
        let trigger = SizeTrigger::new(rotation_bytes);
        let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

        let file_appender = RollingFileAppender::builder()
            .encoder(Box::new(PatternEncoder::new(PATTERN)))
            .build(log_file, Box::new(policy))?;

        builder = builder.appender(Appender::builder().build("file", Box::new(file_appender)));
        let root = Root::builder()
            .appender("console")
            .appender("file")
            .build(level);
        let config = builder.build(root)?;
        log4rs::init_config(config)?;
    } else {
        let root = Root::builder().appender("console").build(level);
        let config = builder.build(root)?;
        log4rs::init_config(config)?;
    }

    Ok(())
}
