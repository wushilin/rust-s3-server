use std::collections::BTreeMap;
use std::path::PathBuf;

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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Alias {
    pub(crate) url: String,
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    #[serde(default = "default_api")]
    pub(crate) api: String,
    #[serde(default = "default_path")]
    pub(crate) path: String,
    #[serde(default)]
    pub(crate) region: Option<String>,
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
    let region = alias
        .region
        .clone()
        .or_else(|| std::env::var("AWS_S3_REGION").ok())
        .or_else(|| std::env::var("AWS_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());
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

pub(crate) fn env_alias(name: &str) -> Option<Alias> {
    let suffix = name.to_ascii_uppercase().replace('-', "_");
    let value = std::env::var(format!("RS3_HOST_{suffix}"))
        .or_else(|_| std::env::var(format!("MC_HOST_{suffix}")))
        .ok()?;
    let value = value.trim_end_matches('/');
    let (scheme, rest) = value.split_once("://")?;
    let (access_key, rest) = rest.split_once(':')?;
    let (secret_key, host) = rest.split_once('@')?;
    Some(Alias {
        url: format!("{scheme}://{host}"),
        access_key: access_key.into(),
        secret_key: secret_key.into(),
        api: default_api(),
        path: default_path(),
        region: std::env::var("AWS_S3_REGION")
            .ok()
            .or_else(|| std::env::var("AWS_REGION").ok()),
    })
}

pub(crate) async fn load_config() -> Result<McConfig> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(McConfig::default());
    }
    let data = fs::read(&path).await?;
    Ok(serde_json::from_slice(&data).with_context(|| format!("parse {}", path.display()))?)
}

pub(crate) async fn save_config(cfg: &McConfig) -> Result<()> {
    let path = config_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let data = serde_json::to_vec_pretty(cfg)?;
    fs::write(path, data).await?;
    Ok(())
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
