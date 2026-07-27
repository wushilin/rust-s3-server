//! Configuration types loaded from `config.yaml`.
//!
//! All fields have sane defaults so an empty (or absent) config file is valid.
//! Use [`AppConfig::from_file`] to load from disk, or [`AppConfig::default`]
//! for in-process use (tests, embeddings).

use serde::{Deserialize, Serialize};

pub const MIN_TRASH_RETENTION_SECS: u64 = 3 * 60 * 60;

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
    /// Directory for split log files. When set, logs are written to
    /// `auth.log`, `authz.log`, `audit.log`, and `server.log` inside this
    /// folder, each with the rotation/compression settings below. Takes
    /// precedence over `file`. The directory is created if it does not exist.
    pub dir: Option<String>,
    /// Path to a single combined log file. If both this and `dir` are absent,
    /// logs go to stdout only.
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
/// `admin.rocksdb` instead.
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
    ///
    /// Setting this also enables virtual-hosted-style addressing: a request
    /// whose `Host` is `<bucket>.<public_hostname>` is served as an access to
    /// that bucket (the bucket is folded into the path before routing), while
    /// requests to the bare `public_hostname` remain path-style. DNS and TLS
    /// for `*.<public_hostname>` must be provided by the fronting proxy.
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
    /// Minimum idle age of a single-PUT staging directory before it is removed
    /// (seconds). Single-PUT staging is orphaned temp data with no client-visible
    /// handle, so this cleanup is invisible to clients.
    #[serde(default = "default_staging_expiry_secs")]
    pub staging_expiry_secs: u64,
    /// Minimum idle age of an *incomplete multipart upload* before it is removed
    /// (seconds). Unlike single-PUT staging, an in-progress multipart upload is
    /// client-visible (ListMultipartUploads/ListParts) and resumable, so S3 keeps
    /// it until aborted or a lifecycle rule fires. We keep it for this long since
    /// its last part activity (uploading any part refreshes the window); `0`
    /// disables cleanup entirely (S3's "keep forever" behavior).
    #[serde(default = "default_multipart_upload_expiry_secs")]
    pub multipart_upload_expiry_secs: u64,
    /// Minimum idle age of a trash directory before it is removed (seconds).
    #[serde(default = "default_trash_expiry_secs")]
    pub trash_expiry_secs: u64,
    /// How often (seconds) the empty-directory reclaimer idles between full
    /// drains. It starts immediately and, while it is still removing dirs,
    /// re-runs on a short internal cadence; once a drain removes nothing it
    /// idles for this interval before checking again.
    #[serde(default = "default_reclaim_interval_secs")]
    pub reclaim_interval_secs: u64,
}

impl SweeperConfig {
    /// The per-pass storage-layer sweep settings derived from this
    /// (seconds-based) server config. The single place the seconds→millis
    /// conversion happens; every sweep job uses this.
    pub fn sweep_pass(&self) -> crate::storage::sweeper::SweepConfig {
        crate::storage::sweeper::SweepConfig {
            intent_batch_size: self.intent_batch_size,
            intent_grace_period_ms: self.intent_grace_period_secs as i64 * 1000,
            staging_expiry_ms: self.staging_expiry_secs as i64 * 1000,
            multipart_expiry_ms: self.multipart_upload_expiry_secs as i64 * 1000,
            trash_expiry_ms: self.trash_expiry_secs as i64 * 1000,
        }
    }
}

impl Default for SweeperConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_sweep_interval_secs(),
            intent_batch_size: default_intent_batch_size(),
            intent_grace_period_secs: default_intent_grace_period_secs(),
            staging_expiry_secs: default_staging_expiry_secs(),
            multipart_upload_expiry_secs: default_multipart_upload_expiry_secs(),
            trash_expiry_secs: default_trash_expiry_secs(),
            reclaim_interval_secs: default_reclaim_interval_secs(),
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
    /// Public console hostname (and optional port), used as the sole allowed
    /// browser origin for presigned uploads to the separate S3 endpoint.
    #[serde(default)]
    pub public_hostname: Option<String>,
    #[serde(default)]
    pub public_scheme: PublicScheme,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            enabled: default_ui_enabled(),
            bind_address: None,
            bind_port: default_ui_port(),
            public_hostname: None,
            public_scheme: PublicScheme::default(),
        }
    }
}

fn default_ui_enabled() -> bool {
    true
}
fn default_ui_port() -> u16 {
    8003
}

/// Runtime-stats sampling. The sampler snapshots CPU / memory / disk-IO (from
/// `/proc`) and S3 throughput every `sample_secs` into a small RocksDB, pruned
/// to `retention_days`. All optional so an absent `stats:` section just uses
/// the defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsConfig {
    #[serde(default = "default_stats_enabled")]
    pub enabled: bool,
    /// Sampling period in seconds. Clamped to a minimum of 1 at runtime.
    #[serde(default = "default_stats_sample_secs")]
    pub sample_secs: u64,
    /// How long samples are kept before the retention prune removes them.
    #[serde(default = "default_stats_retention_days")]
    pub retention_days: u64,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            enabled: default_stats_enabled(),
            sample_secs: default_stats_sample_secs(),
            retention_days: default_stats_retention_days(),
        }
    }
}

fn default_stats_enabled() -> bool {
    true
}
fn default_stats_sample_secs() -> u64 {
    5
}
fn default_stats_retention_days() -> u64 {
    7
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
    #[serde(default)]
    pub stats: StatsConfig,
}

/// Deserializes a list of strings, dropping entries that were never filled in.
///
/// For any future list field that a container config templates one slot per
/// line, e.g.
///
/// ```yaml
/// whitelist:
///   - "{{RUSTS3_ALLOW_1:}}"
///   - "{{RUSTS3_ALLOW_2:}}"
/// ```
///
/// With neither variable set this must produce an empty list, not a list of
/// two empty strings — and it must not fail to parse, which is what a bare
/// (unquoted) unfilled slot would otherwise do, since YAML reads it as `null`.
/// Both cases are handled here: `null` and blank entries are skipped.
///
/// Use it as `#[serde(default, deserialize_with = "list_of_filled_strings")]`.
pub fn list_of_filled_strings<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: Vec<Option<String>> = serde::Deserialize::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .flatten()
        .filter(|value| !value.trim().is_empty())
        .collect())
}

impl AppConfig {
    /// Reads a config file, expanding `{{ENV_VAR:default}}` placeholders from
    /// the environment first. That is what lets one image be configured by
    /// `-e RUSTS3_PORT=9000` without maintaining a parallel set of env-var
    /// bindings for every field — the file stays the single description of
    /// what is configurable.
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let text = std::fs::read_to_string(path)?;
        let text = super::template::expand(&text)?;
        let mut config: Self = serde_yaml::from_str(&text)?;
        config.normalize();
        config.validate().map_err(|message| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
        })?;
        Ok(config)
    }

    /// Serializes this config to YAML. `rusts3 init` writes the serialization
    /// of [`AppConfig::default`], so the generated file can never drift out of
    /// sync with the parser — every supported field is emitted from the struct
    /// itself rather than a hand-maintained template.
    pub fn to_yaml(&self) -> std::result::Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }

    /// Folds unfilled values away, so a templated config can be *flat*.
    ///
    /// Placeholder expansion cannot express "if this then not that": a
    /// container config has to list every slot it might use and leave the
    /// unused ones to expand to nothing. Which means an unfilled slot must be
    /// indistinguishable from an absent one — otherwise the shape of the file
    /// would have to change with the deployment, which is the thing templating
    /// exists to avoid.
    ///
    /// So:
    ///
    /// * an empty optional string becomes `None`, as if the key were absent;
    /// * a list entry that was never filled in is dropped rather than becoming
    ///   a real, empty entry.
    ///
    /// The second is not merely tidiness. `verify_builtin_password` compares
    /// the configured password to the candidate, so a user left with an empty
    /// password would *authenticate against an empty password*. An empty access
    /// key or secret is the same class of problem. A half-filled credential is
    /// never usable and never safe, so it is discarded.
    ///
    /// Anything dropped is reported on stderr: silently discarding
    /// configuration is its own kind of trap.
    fn normalize(&mut self) {
        fn blank(value: &str) -> bool {
            value.trim().is_empty()
        }
        fn blank_to_none(value: &mut Option<String>) {
            if value.as_deref().is_some_and(blank) {
                *value = None;
            }
        }
        let mut dropped: Vec<String> = Vec::new();

        blank_to_none(&mut self.auth.public_hostname);
        blank_to_none(&mut self.ui.bind_address);
        blank_to_none(&mut self.ui.public_hostname);

        for user in &mut self.auth.users {
            // An empty password is not "no password" to the verifier — it is a
            // password that the empty string matches. Treat it as absent, which
            // means "this user cannot log into the console".
            blank_to_none(&mut user.password);
            let before = user.api_keys.len();
            user.api_keys
                .retain(|key| !blank(&key.ak) && !blank(&key.secret));
            for _ in user.api_keys.len()..before {
                dropped.push(format!("an unfilled api_key of user {:?}", user.user));
            }
        }

        let before = self.auth.users.len();
        self.auth.users.retain(|user| !blank(&user.user));
        for _ in self.auth.users.len()..before {
            dropped.push("an unfilled auth.users entry".to_string());
        }

        let before = self.auth.credentials.len();
        self.auth
            .credentials
            .retain(|credential| !blank(&credential.access_key) && !blank(&credential.secret_key));
        for _ in self.auth.credentials.len()..before {
            dropped.push("an unfilled auth.credentials entry".to_string());
        }

        if !dropped.is_empty() {
            // Logging is not initialised yet at config-load time, so this goes
            // straight to stderr — which is where a container's output lands.
            eprintln!(
                "config: ignoring {} unfilled entr{} ({})",
                dropped.len(),
                if dropped.len() == 1 { "y" } else { "ies" },
                dropped.join(", ")
            );
        }
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
        // An empty value means "not configured": a container config expands an
        // unset {{RUSTS3_PUBLIC_HOSTNAME:}} to the empty string, and that has
        // to be indistinguishable from omitting the key.
        if let Some(host) = self.auth.public_hostname.as_deref().filter(|v| !v.is_empty()) {
            if host.contains("://") || host.contains('/') {
                return Err(
                    "auth.public_hostname must contain only a hostname and optional port"
                        .to_string(),
                );
            }
        }
        if let Some(host) = self.ui.public_hostname.as_deref().filter(|v| !v.is_empty()) {
            if host.contains("://") || host.contains('/') {
                return Err(
                    "ui.public_hostname must contain only a hostname and optional port"
                        .to_string(),
                );
            }
        }
        if self.sweeper.trash_expiry_secs < MIN_TRASH_RETENTION_SECS {
            return Err(format!(
                "sweeper.trash_expiry_secs must be at least {MIN_TRASH_RETENTION_SECS} seconds (3 hours)"
            ));
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
            stats: StatsConfig::default(),
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
            dir: None,
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
fn default_reclaim_interval_secs() -> u64 {
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
fn default_multipart_upload_expiry_secs() -> u64 {
    // 30 days of inactivity. S3 keeps incomplete uploads forever (until aborted
    // or a lifecycle rule), so this is a conservative disk safety net that a
    // well-behaved client never trips; `0` restores S3's keep-forever behavior.
    30 * 24 * 60 * 60
}
fn default_trash_expiry_secs() -> u64 {
    24 * 60 * 60
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a container gets when RUSTS3_PUBLIC_HOSTNAME is not supplied:
    /// the placeholder expands to "", which must read as "not configured".
    #[test]
    fn an_empty_templated_optional_is_treated_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "server:\n  bind_address: \"0.0.0.0\"\n  bind_port: {{T_PORT:8002}}\n\
             auth:\n  enabled: true\n  public_hostname: \"{{T_HOST:}}\"\n\
             ui:\n  bind_address: \"{{T_UI_ADDR:}}\"\n",
        )
        .unwrap();
        let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
        assert_eq!(config.server.bind_port, 8002);
        assert_eq!(config.auth.public_hostname, None, "empty host must read as unset");
        assert_eq!(config.ui.bind_address, None);
    }

    #[test]
    fn a_templated_config_takes_values_from_the_environment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "server:\n  bind_port: {{T_PORT_SET:8002}}\n").unwrap();
        std::env::set_var("T_PORT_SET", "9123");
        let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
        std::env::remove_var("T_PORT_SET");
        assert_eq!(config.server.bind_port, 9123);
    }

    #[test]
    fn a_placeholder_with_no_default_and_no_variable_fails_the_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "server:\n  bind_port: {{T_UNSET_REQUIRED}}\n").unwrap();
        let err = AppConfig::from_file(path.to_str().unwrap()).unwrap_err();
        assert!(
            err.to_string().contains("T_UNSET_REQUIRED"),
            "the error must name the variable: {err}"
        );
    }

    /// A templated config is flat: it lists every slot it might use, and the
    /// unused ones expand to nothing. Those must vanish, not become entries.
    #[test]
    fn unfilled_list_slots_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "auth:\n  enabled: true\n  users:\n\
             \x20   - user: \"{{T_USER_1:admin}}\"\n      password: \"{{T_PASS_1:secret}}\"\n\
             \x20     api_keys:\n\
             \x20       - ak: \"{{T_AK_1:key1}}\"\n          secret: \"{{T_SK_1:sec1}}\"\n\
             \x20       - ak: \"{{T_AK_2:}}\"\n          secret: \"{{T_SK_2:}}\"\n\
             \x20   - user: \"{{T_USER_2:}}\"\n      password: \"{{T_PASS_2:}}\"\n",
        )
        .unwrap();
        let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();

        // The filled user survives with its one filled key; the empty slots are
        // gone entirely.
        assert_eq!(config.auth.users.len(), 1, "{:?}", config.auth.users);
        assert_eq!(config.auth.users[0].user, "admin");
        assert_eq!(config.auth.users[0].api_keys.len(), 1);
        assert_eq!(config.auth.users[0].api_keys[0].ak, "key1");
    }

    /// The one that actually matters: an unfilled password must not become a
    /// password that the empty string authenticates against.
    #[test]
    fn an_unfilled_password_becomes_no_password_at_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "auth:\n  enabled: true\n  users:\n    - user: \"admin\"\n      password: \"{{T_PW:}}\"\n",
        )
        .unwrap();
        let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
        assert_eq!(config.auth.users.len(), 1);
        assert_eq!(
            config.auth.users[0].password, None,
            "an empty password must read as absent, or the empty string logs in"
        );
    }

    /// A half-filled credential is never usable and never safe.
    #[test]
    fn a_credential_missing_either_half_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "auth:\n  enabled: true\n  credentials:\n\
             \x20   - access_key: \"AK1\"\n      secret_key: \"SK1\"\n\
             \x20   - access_key: \"AK2\"\n      secret_key: \"{{T_SK:}}\"\n\
             \x20   - access_key: \"{{T_AK:}}\"\n      secret_key: \"SK3\"\n",
        )
        .unwrap();
        let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
        assert_eq!(config.auth.credentials.len(), 1);
        assert_eq!(config.auth.credentials[0].access_key, "AK1");
    }

    /// A discriminated shape: every branch is present, the unused ones blank.
    #[test]
    fn unused_branches_of_a_flat_config_are_harmless() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            "server:\n  bind_address: \"{{T_ADDR:0.0.0.0}}\"\n  bind_port: {{T_PORT:8002}}\n\
             auth:\n  enabled: true\n  public_hostname: \"{{T_HOST:}}\"\n\
             ui:\n  bind_address: \"{{T_UI:}}\"\n",
        )
        .unwrap();
        let config = AppConfig::from_file(path.to_str().unwrap()).unwrap();
        assert_eq!(config.auth.public_hostname, None);
        assert_eq!(config.ui.bind_address, None);
        assert_eq!(config.server.bind_port, 8002);
    }

    #[test]
    fn a_templated_string_list_drops_unfilled_and_null_slots() {
        #[derive(serde::Deserialize)]
        struct Holder {
            #[serde(default, deserialize_with = "list_of_filled_strings")]
            whitelist: Vec<String>,
        }
        // Two filled, one blank, one bare (which YAML reads as null).
        let yaml = "whitelist:\n  - \"10.0.0.1\"\n  - \"\"\n  - \n  - \"10.0.0.2\"\n";
        let holder: Holder = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(holder.whitelist, vec!["10.0.0.1", "10.0.0.2"]);

        // …and a list where nothing was filled in is simply empty.
        let yaml = "whitelist:\n  - \"\"\n  - \n";
        let holder: Holder = serde_yaml::from_str(yaml).unwrap();
        assert!(holder.whitelist.is_empty());
    }

    #[test]
    fn bandwidth_report_defaults_to_enabled() {
        assert!(AppConfig::default().logging.enable_bandwidth_report);

        let config: AppConfig = serde_yaml::from_str("logging: {}\n").unwrap();
        assert!(config.logging.enable_bandwidth_report);
    }

    #[test]
    fn trash_retention_defaults_to_one_day() {
        assert_eq!(AppConfig::default().sweeper.trash_expiry_secs, 86_400);
        let config: AppConfig = serde_yaml::from_str("sweeper: {}\n").unwrap();
        assert_eq!(config.sweeper.trash_expiry_secs, 86_400);
    }

    #[test]
    fn trash_retention_enforces_three_hour_minimum() {
        let below: AppConfig =
            serde_yaml::from_str("sweeper:\n  trash_expiry_secs: 10799\n").unwrap();
        assert_eq!(
            below.validate().unwrap_err(),
            "sweeper.trash_expiry_secs must be at least 10800 seconds (3 hours)"
        );

        let boundary: AppConfig =
            serde_yaml::from_str("sweeper:\n  trash_expiry_secs: 10800\n").unwrap();
        assert!(boundary.validate().is_ok());
    }

    #[test]
    fn bandwidth_report_can_be_disabled() {
        let config: AppConfig =
            serde_yaml::from_str("logging:\n  enable_bandwidth_report: false\n").unwrap();
        assert!(!config.logging.enable_bandwidth_report);
    }

    #[test]
    fn storage_defaults_and_ignores_removed_sqlite_knobs() {
        assert_eq!(AppConfig::default().storage.meta_cache_capacity, 200_000);

        // Legacy SQLite knobs are gone; configs that still carry them must
        // parse (unknown fields ignored) and fall back to defaults.
        let config: AppConfig =
            serde_yaml::from_str("storage:\n  sqlite_max_connections: 12\n").unwrap();
        assert_eq!(config.storage.meta_cache_capacity, 200_000);
    }

    #[test]
    fn default_config_round_trips_through_yaml() {
        // `rusts3 init` writes exactly this; it must parse and validate.
        let yaml = AppConfig::default().to_yaml().unwrap();
        let parsed: AppConfig = serde_yaml::from_str(&yaml).unwrap();
        parsed.validate().unwrap();
        assert_eq!(
            parsed.storage.meta_cache_capacity,
            AppConfig::default().storage.meta_cache_capacity
        );
        assert!(!yaml.contains("sqlite"));
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
