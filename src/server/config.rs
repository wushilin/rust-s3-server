//! Configuration types loaded from `config.yaml`.
//!
//! All fields have sane defaults so an empty (or absent) config file is valid.
//! Use [`AppConfig::from_file`] to load from disk, or [`AppConfig::default`]
//! for in-process use (tests, embeddings).

use serde::{Deserialize, Serialize};

pub use crate::storage::config::StorageConfig;

/// Network and storage settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
    #[serde(default = "default_bind_port")]
    pub bind_port: u16,
    #[serde(default = "default_base_dir")]
    pub base_dir: String,
}

/// Log rotation and output settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Path to the log file. If absent, logs go to stdout only.
    pub file: Option<String>,
    /// Minimum log level: "trace", "debug", "info", "warn", "error".
    #[serde(default = "default_log_level")]
    pub level: String,
    /// Emit aggregate bandwidth totals and rates every 10 seconds.
    #[serde(default = "default_enable_bandwidth_report")]
    pub enable_bandwidth_report: bool,
    /// Rotate when the log file reaches this size (MiB).
    #[serde(default = "default_rotation_size_mb")]
    pub rotation_size_mb: u64,
    /// Maximum number of archived log files to keep.
    #[serde(default = "default_keep_files")]
    pub keep_files: u32,
    /// Compress archived log files with gzip.
    #[serde(default)]
    pub compress: bool,
}

/// A single access-key / secret-key credential pair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Credential {
    pub access_key: String,
    pub secret_key: String,
}

/// SigV4 authentication settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthConfig {
    /// When false the server accepts all requests without validation.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub credentials: Vec<Credential>,
    /// The hostname (and optional port) that S3 clients are configured to use,
    /// e.g. `"mys3.company.com"` or `"localhost:8002"`.
    ///
    /// When set, presigned URL verification substitutes this value for the
    /// `host` signed header instead of reading it from the incoming HTTP request.
    /// This makes signature verification proxy-safe: a reverse proxy may rewrite
    /// the `Host` header, but both the client and server still agree on the
    /// configured public hostname.
    ///
    /// When absent the incoming `Host` header is used (direct-access mode).
    #[serde(default)]
    pub public_hostname: Option<String>,
}

/// Background sweeper settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweeperConfig {
    /// How often the sweeper runs (seconds).
    #[serde(default = "default_sweep_interval_secs")]
    pub interval_secs: u64,
    /// Yield after this many SQLite index entries are checked.
    #[serde(default = "default_max_objects_per_pass")]
    pub max_objects_per_pass: usize,
    /// Minimum age of a stale SQLite row before it is removed (seconds).
    #[serde(default = "default_orphan_grace_period_secs")]
    pub orphan_grace_period_secs: u64,
    /// Minimum idle age of a staging directory before it is removed (seconds).
    #[serde(default = "default_staging_expiry_secs")]
    pub staging_expiry_secs: u64,
}

impl Default for SweeperConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_sweep_interval_secs(),
            max_objects_per_pass: default_max_objects_per_pass(),
            orphan_grace_period_secs: default_orphan_grace_period_secs(),
            staging_expiry_secs: default_staging_expiry_secs(),
        }
    }
}

/// Root configuration object, deserialised from `config.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub sweeper: SweeperConfig,
}

impl AppConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let text = std::fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&text)?;
        Ok(config)
    }

    pub fn find_secret(&self, access_key: &str) -> Option<&str> {
        self.auth
            .credentials
            .iter()
            .find(|c| c.access_key == access_key)
            .map(|c| c.secret_key.as_str())
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            logging: LoggingConfig::default(),
            auth: AuthConfig::default(),
            sweeper: SweeperConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            bind_port: default_bind_port(),
            base_dir: default_base_dir(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            file: None,
            level: default_log_level(),
            enable_bandwidth_report: default_enable_bandwidth_report(),
            rotation_size_mb: default_rotation_size_mb(),
            keep_files: default_keep_files(),
            compress: false,
        }
    }
}

fn default_bind_address() -> String {
    "0.0.0.0".to_string()
}
fn default_bind_port() -> u16 {
    8002
}
fn default_base_dir() -> String {
    "./rusts3-data".to_string()
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_enable_bandwidth_report() -> bool {
    true
}
fn default_rotation_size_mb() -> u64 {
    100
}
fn default_keep_files() -> u32 {
    5
}
fn default_sweep_interval_secs() -> u64 {
    300
}
fn default_max_objects_per_pass() -> usize {
    100
}
fn default_orphan_grace_period_secs() -> u64 {
    300
}
fn default_staging_expiry_secs() -> u64 {
    86400
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bandwidth_report_defaults_to_enabled() {
        assert!(AppConfig::default().logging.enable_bandwidth_report);

        let config: AppConfig = serde_yaml::from_str("logging: {}\n").unwrap();
        assert!(config.logging.enable_bandwidth_report);
    }

    #[test]
    fn bandwidth_report_can_be_disabled() {
        let config: AppConfig =
            serde_yaml::from_str("logging:\n  enable_bandwidth_report: false\n").unwrap();
        assert!(!config.logging.enable_bandwidth_report);
    }

    #[test]
    fn sqlite_pool_size_defaults_and_can_be_overridden() {
        assert_eq!(AppConfig::default().storage.sqlite_max_connections, 50);

        let config: AppConfig =
            serde_yaml::from_str("storage:\n  sqlite_max_connections: 12\n").unwrap();
        assert_eq!(config.storage.sqlite_max_connections, 12);
    }
}
