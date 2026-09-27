use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::SharedCredentialsProvider;
use serde::{Deserialize, Serialize};
use tokio::fs;

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct McConfig {
    #[serde(default)]
    pub(crate) version: String,
    #[serde(default)]
    pub(crate) aliases: BTreeMap<String, Alias>,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_json::Value>,
}

/// One alias entry, wire-compatible with mc's `aliasConfigV10`
/// (camelCase keys: url/accessKey/secretKey/api/path). `region` is an
/// rs3-only extension mc ignores; mc's own optional fields
/// (sessionToken, license, apiKey, src) round-trip through `extra` so an
/// `rs3 alias set` never destroys them in a shared `~/.mc/config.json`.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Alias {
    pub(crate) url: String,
    #[serde(rename = "accessKey")]
    pub(crate) access_key: String,
    #[serde(rename = "secretKey")]
    pub(crate) secret_key: String,
    #[serde(default = "default_api")]
    pub(crate) api: String,
    #[serde(default = "default_path")]
    pub(crate) path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) region: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_json::Value>,
}

pub(crate) fn default_api() -> String {
    "S3v4".into()
}

pub(crate) fn default_path() -> String {
    "auto".into()
}

pub(crate) async fn client_for_alias(alias_name: &str) -> Result<(Client, Alias)> {
    let cfg = load_config().await?;
    let alias = cfg
        .aliases
        .get(alias_name)
        .cloned()
        .or_else(|| env_alias(alias_name))
        .ok_or_else(|| anyhow!("alias `{alias_name}` not found; run `rs3 alias set` first"))?;
    let creds = Credentials::new(
        alias.access_key.clone(),
        alias.secret_key.clone(),
        None,
        None,
        "rs3",
    );
    let region = resolve_region(&alias);
    let sdk_cfg = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region))
        .credentials_provider(SharedCredentialsProvider::new(creds))
        .load()
        .await;
    let force_path_style = matches!(alias.path.as_str(), "on" | "auto" | "");
    let s3_cfg = aws_sdk_s3::config::Builder::from(&sdk_cfg)
        .endpoint_url(alias.url.clone())
        .force_path_style(force_path_style)
        .build();
    Ok((Client::from_conf(s3_cfg), alias))
}

/// Resolves the effective SigV4 region for an alias: an explicit
/// per-alias `region` wins, then `AWS_S3_REGION`/`AWS_REGION`, then the
/// `us-east-1` default. Shared by [`client_for_alias`] (SDK client config)
/// and `share.rs`'s hand-rolled POST-policy signer, which needs the same
/// value outside of an SDK `Client`.
pub(crate) fn resolve_region(alias: &Alias) -> String {
    alias
        .region
        .clone()
        .or_else(|| std::env::var("AWS_S3_REGION").ok())
        .or_else(|| std::env::var("AWS_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string())
}

pub(crate) fn env_alias(name: &str) -> Option<Alias> {
    let suffix = name.to_ascii_uppercase().replace('-', "_");
    let value = std::env::var(format!("RS3_HOST_{suffix}"))
        .or_else(|_| std::env::var(format!("MC_HOST_{suffix}")))
        .ok()?;
    let value = value.trim_end_matches('/');
    let (scheme, rest) = value.split_once("://")?;
    // The host never contains `@`, but a secret key may: split the
    // userinfo off at the *last* `@`, then access/secret at the first `:`,
    // and percent-decode both (mc parses this with Go's `url.Parse`).
    let (userinfo, host) = rest.rsplit_once('@')?;
    let (access_key, secret_key) = userinfo.split_once(':')?;
    let decode = |s: &str| {
        percent_encoding::percent_decode_str(s)
            .decode_utf8()
            .map(|c| c.into_owned())
            .ok()
    };
    Some(Alias {
        url: format!("{scheme}://{host}"),
        access_key: decode(access_key)?,
        secret_key: decode(secret_key)?,
        api: default_api(),
        path: default_path(),
        extra: BTreeMap::new(),
        region: std::env::var("AWS_S3_REGION")
            .ok()
            .or_else(|| std::env::var("AWS_REGION").ok()),
    })
}

/// Whether `name` is a configured alias (config file or `MC_HOST_*` /
/// `RS3_HOST_*`). An unreadable config answers `true`, so the operand keeps
/// its alias meaning and the real config error surfaces when it is used.
pub(crate) async fn alias_is_configured(name: &str) -> bool {
    if env_alias(name).is_some() {
        return true;
    }
    match load_config().await {
        Ok(cfg) => cfg.aliases.contains_key(name),
        Err(_) => true,
    }
}

pub(crate) async fn load_config() -> Result<McConfig> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(McConfig::default());
    }
    let data = fs::read(&path).await?;
    serde_json::from_slice(&data).with_context(|| format!("parse {}", path.display()))
}

pub(crate) async fn save_config(cfg: &McConfig) -> Result<()> {
    let path = config_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let data = serde_json::to_vec_pretty(cfg)?;
    write_private_file(&path, &data).await
}

/// Atomically replaces `path` with `data`, readable by the owner only:
/// the bytes go to a mode-0600 temp file in the same directory, are
/// fsynced, and are then renamed over `path`. The file holds secret keys
/// (config) or live presigned credentials (share DB), so it must never be
/// world-readable -- not even briefly -- and a crash mid-write must never
/// leave a truncated file behind.
pub(crate) async fn write_private_file(path: &Path, data: &[u8]) -> Result<()> {
    let path = path.to_path_buf();
    let data = data.to_vec();
    tokio::task::spawn_blocking(move || write_private_file_sync(&path, &data)).await?
}

fn write_private_file_sync(path: &Path, data: &[u8]) -> Result<()> {
    use std::io::Write;

    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let name = path
        .file_name()
        .ok_or_else(|| anyhow!("invalid file path `{}`", path.display()))?
        .to_string_lossy();
    let mut attempt = 0u32;
    let (tmp, mut file) = loop {
        let tmp = dir.join(format!(".{name}.tmp-{}-{attempt}", std::process::id()));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        match opts.open(&tmp) {
            Ok(f) => break (tmp, f),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && attempt < 100 => {
                attempt += 1;
            }
            Err(e) => return Err(e).with_context(|| format!("create {}", tmp.display())),
        }
    };
    let result = (|| -> Result<()> {
        #[cfg(unix)]
        {
            // `mode` is filtered through the umask; force 0600 exactly.
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(data)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result.with_context(|| format!("write {}", path.display()))
}

pub(crate) fn config_path() -> Result<PathBuf> {
    // rs3-specific variables win over their mc-compatible equivalents.
    if let Ok(file) = std::env::var("RS3_CONFIG_FILE") {
        return Ok(PathBuf::from(file));
    }
    if let Ok(dir) = std::env::var("RS3_CONFIG_DIR") {
        return Ok(PathBuf::from(dir).join("config.json"));
    }
    if let Ok(file) = std::env::var("MC_CONFIG_FILE") {
        return Ok(PathBuf::from(file));
    }
    if let Ok(dir) = std::env::var("MC_CONFIG_DIR") {
        return Ok(PathBuf::from(dir).join("config.json"));
    }
    let home = dirs::home_dir().ok_or_else(|| anyhow!("unable to locate home directory"))?;
    Ok(home.join(".mc").join("config.json"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Verbatim shape of a config.json written by real mc (RELEASE.2025-08-13):
    // camelCase keys, optional sessionToken/license/apiKey/src fields.
    const MC_WRITTEN_CONFIG: &str = r#"{
        "version": "10",
        "aliases": {
            "play": {
                "url": "https://play.min.io",
                "accessKey": "Q3AM3UQ867SPQQA43P2F",
                "secretKey": "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
                "api": "S3v4",
                "path": "auto",
                "sessionToken": "tok123"
            }
        }
    }"#;

    #[test]
    fn parses_real_mc_written_config() {
        let cfg: McConfig = serde_json::from_str(MC_WRITTEN_CONFIG).unwrap();
        let play = &cfg.aliases["play"];
        assert_eq!(play.access_key, "Q3AM3UQ867SPQQA43P2F");
        assert_eq!(play.secret_key, "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG");
        assert_eq!(play.api, "S3v4");
        assert_eq!(play.path, "auto");
    }

    #[test]
    fn saves_camelcase_and_preserves_mc_fields() {
        let cfg: McConfig = serde_json::from_str(MC_WRITTEN_CONFIG).unwrap();
        let out = serde_json::to_string_pretty(&cfg).unwrap();
        assert!(
            out.contains("\"accessKey\""),
            "must write mc's camelCase: {out}"
        );
        assert!(
            out.contains("\"secretKey\""),
            "must write mc's camelCase: {out}"
        );
        assert!(
            !out.contains("access_key"),
            "no snake_case in saved config: {out}"
        );
        assert!(
            out.contains("\"sessionToken\": \"tok123\""),
            "mc's optional fields must survive a roundtrip: {out}"
        );
        // rs3's own extension must not pollute configs that never set it
        assert!(
            !out.contains("\"region\""),
            "region must be omitted when None: {out}"
        );
    }

    #[test]
    fn env_alias_splits_at_last_at_and_percent_decodes() {
        // SAFETY: test-only env mutation with a unique variable name.
        unsafe {
            std::env::set_var(
                "MC_HOST_RS3TESTATSIGN",
                "https://AK%2Fx:se@cr%3Aet@example.com:9000",
            );
        }
        let alias = env_alias("rs3testatsign").unwrap();
        assert_eq!(alias.url, "https://example.com:9000");
        assert_eq!(alias.access_key, "AK/x");
        assert_eq!(alias.secret_key, "se@cr:et");
    }

    #[test]
    fn write_private_file_is_owner_only_and_replaces() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, b"old").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
        write_private_file_sync(&path, b"new").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let leftovers: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(leftovers.len(), 1, "no temp files left behind");
    }
}
