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

/// An API key pair belonging to a built-in admin user.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiKeyPair {
    pub ak: String,
    pub secret: String,
}

/// A built-in admin user defined in the config file. Built-in users are
/// held in memory, are **not alterable at runtime**, and are unrestricted
/// (no policy applies): they are the bootstrap identities that always work,
/// even against a bare data directory. Runtime-managed IAM users live in
/// `admin.sqlite` instead.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuiltinUser {
    pub user: String,
    /// Web-UI login password. Absent → this user cannot log into the UI
    /// (api_keys still work on the S3 API).
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub api_keys: Vec<ApiKeyPair>,
}

/// Scheme used when constructing externally reachable S3 URLs.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PublicScheme {
    Http,
    Https,
}

impl PublicScheme {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

impl Default for PublicScheme {
    fn default() -> Self {
        Self::Http
    }
}

/// SigV4 authentication and public S3 endpoint settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthConfig {
    /// When false the server accepts all requests without validation.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub credentials: Vec<Credential>,
    /// Built-in admin users (in-memory, non-alterable at runtime).
    #[serde(default)]
    pub users: Vec<BuiltinUser>,
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
    /// URL scheme used for generated public S3 links. SigV4 verification
    /// itself is scheme-independent. Defaults to `http`.
    #[serde(default)]
    pub public_scheme: PublicScheme,
}

/// Background maintenance settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweeperConfig {
    /// How often maintenance runs (seconds).
    #[serde(default = "default_sweep_interval_secs")]
    pub interval_secs: u64,
    /// Stale-intent resolution batch size. One maintenance pass drains all
    /// eligible intents in batches of this size.
    #[serde(
        default = "default_intent_batch_size",
        alias = "visibility_repair_batch_size",
        alias = "visibility_repair_max_per_pass",
        alias = "max_objects_per_pass"
    )]
    pub intent_batch_size: usize,
    /// Minimum age of an intent before the background resolver may act on it
    /// — an in-flight operation's intent must never look abandoned.
    #[serde(
        default = "default_intent_grace_period_secs",
        alias = "visibility_repair_grace_period_secs",
        alias = "orphan_grace_period_secs"
    )]
    pub intent_grace_period_secs: u64,
    /// Minimum idle age of a staging directory before it is removed (seconds).
    #[serde(default = "default_staging_expiry_secs")]
    pub staging_expiry_secs: u64,
    /// Minimum idle age of a trash directory before it is removed (seconds).
    #[serde(default = "default_trash_expiry_secs")]
    pub trash_expiry_secs: u64,
}

impl Default for SweeperConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_sweep_interval_secs(),
            intent_batch_size: default_intent_batch_size(),
            intent_grace_period_secs: default_intent_grace_period_secs(),
            staging_expiry_secs: default_staging_expiry_secs(),
            trash_expiry_secs: default_trash_expiry_secs(),
        }
    }
}

/// Management-UI settings. The UI listens on its own port, entirely
/// separate from the S3 API: web logins use username/password only, the S3
/// API uses access keys only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiConfig {
    #[serde(default = "default_ui_enabled")]
    pub enabled: bool,
    /// Defaults to the S3 server's bind address.
    #[serde(default)]
    pub bind_address: Option<String>,
    #[serde(default = "default_ui_port")]
    pub bind_port: u16,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            enabled: default_ui_enabled(),
            bind_address: None,
            bind_port: default_ui_port(),
        }
    }
}

fn default_ui_enabled() -> bool {
    true
}
fn default_ui_port() -> u16 {
    8003
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
    #[serde(default)]
    pub ui: UiConfig,
}

impl AppConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let text = std::fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&text)?;
        config.validate().map_err(|message| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
        })?;
        Ok(config)
    }

    pub fn validate(&self) -> std::result::Result<(), String> {
        format!("{}:{}", self.server.bind_address, self.server.bind_port)
            .parse::<std::net::SocketAddr>()
            .map_err(|err| format!("invalid server bind address: {err}"))?;
        if let Some(address) = self.ui.bind_address.as_deref() {
            format!("{}:{}", address, self.ui.bind_port)
                .parse::<std::net::SocketAddr>()
                .map_err(|err| format!("invalid ui bind address: {err}"))?;
        }
        if let Some(host) = self.auth.public_hostname.as_deref() {
            if host.is_empty() || host.contains("://") || host.contains('/') {
                return Err(
                    "auth.public_hostname must contain only a hostname and optional port"
                        .to_string(),
                );
            }
        }

        let mut usernames = std::collections::HashSet::new();
        let mut access_keys = std::collections::HashSet::new();
        for user in &self.auth.users {
            if user.user.trim().is_empty() {
                return Err("auth.users contains an empty user name".to_string());
            }
            if !usernames.insert(user.user.as_str()) {
                return Err(format!("duplicate built-in user: {}", user.user));
            }
            if let Some(password) = user.password.as_deref() {
                if password.starts_with("$2") && bcrypt::verify("", password).is_err() {
                    return Err(format!("invalid bcrypt password for user {}", user.user));
                }
            }
            // api_keys are optional: a built-in user may be console-only.
            for key in &user.api_keys {
                if key.ak.is_empty() || key.secret.is_empty() {
                    return Err(format!("empty api key or secret for user {}", user.user));
                }
                if !access_keys.insert(key.ak.as_str()) {
                    return Err(format!("duplicate S3 access key: {}", key.ak));
                }
            }
        }
        for credential in &self.auth.credentials {
            if credential.access_key.is_empty() || credential.secret_key.is_empty() {
                return Err("auth.credentials contains an empty access key or secret".to_string());
            }
            if !access_keys.insert(credential.access_key.as_str()) {
                return Err(format!(
                    "duplicate S3 access key: {}",
                    credential.access_key
                ));
            }
        }
        Ok(())
    }

    /// Resolves a root access key: legacy flat `credentials` entries plus
    /// every built-in user's `api_keys`. All are unrestricted.
    pub fn find_secret(&self, access_key: &str) -> Option<&str> {
        if let Some(secret) = self
            .auth
            .credentials
            .iter()
            .find(|c| c.access_key == access_key)
            .map(|c| c.secret_key.as_str())
        {
            return Some(secret);
        }
        self.auth
            .users
            .iter()
            .flat_map(|u| u.api_keys.iter())
            .find(|k| k.ak == access_key)
            .map(|k| k.secret.as_str())
    }

    pub fn find_builtin_user(&self, username: &str) -> Option<&BuiltinUser> {
        self.auth.users.iter().find(|u| u.user == username)
    }

    /// First available root key pair (for root presigned-URL generation).
    pub fn first_root_key(&self) -> Option<(&str, &str)> {
        self.auth
            .credentials
            .first()
            .map(|c| (c.access_key.as_str(), c.secret_key.as_str()))
            .or_else(|| {
                self.auth
                    .users
                    .iter()
                    .flat_map(|u| u.api_keys.iter())
                    .next()
                    .map(|k| (k.ak.as_str(), k.secret.as_str()))
            })
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
            ui: UiConfig::default(),
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
fn default_intent_batch_size() -> usize {
    100
}
fn default_intent_grace_period_secs() -> u64 {
    60 * 60
}
fn default_staging_expiry_secs() -> u64 {
    86400
}
fn default_trash_expiry_secs() -> u64 {
    600
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

    #[test]
    fn intent_config_accepts_new_names_and_legacy_aliases() {
        let config: AppConfig = serde_yaml::from_str(
            "sweeper:\n  intent_batch_size: 7\n  intent_grace_period_secs: 9\n",
        )
        .unwrap();
        assert_eq!(config.sweeper.intent_batch_size, 7);
        assert_eq!(config.sweeper.intent_grace_period_secs, 9);

        let renamed: AppConfig =
            serde_yaml::from_str("sweeper:\n  visibility_repair_max_per_pass: 10\n").unwrap();
        assert_eq!(renamed.sweeper.intent_batch_size, 10);

        let legacy: AppConfig = serde_yaml::from_str(
            "sweeper:\n  max_objects_per_pass: 11\n  orphan_grace_period_secs: 13\n",
        )
        .unwrap();
        assert_eq!(legacy.sweeper.intent_batch_size, 11);
        assert_eq!(legacy.sweeper.intent_grace_period_secs, 13);
    }

    #[test]
    fn public_scheme_defaults_to_http_and_accepts_https() {
        let defaulted: AppConfig =
            serde_yaml::from_str("auth:\n  public_hostname: s3.example.com\n").unwrap();
        assert_eq!(defaulted.auth.public_scheme, PublicScheme::Http);

        let https: AppConfig = serde_yaml::from_str(
            "auth:\n  public_hostname: s3.example.com\n  public_scheme: https\n",
        )
        .unwrap();
        assert_eq!(https.auth.public_scheme, PublicScheme::Https);
    }

    #[test]
    fn public_scheme_rejects_unsupported_values() {
        let result = serde_yaml::from_str::<AppConfig>("auth:\n  public_scheme: ftp\n");
        assert!(result.is_err());
    }

    #[test]
    fn builtin_console_admin_does_not_require_an_api_key() {
        let config: AppConfig = serde_yaml::from_str(
            "auth:\n  users:\n    - user: admin\n      password: console-only\n",
        )
        .unwrap();
        assert!(config.validate().is_ok());
        assert!(config.auth.users[0].api_keys.is_empty());
    }
}
