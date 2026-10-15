use std::{
    env, fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const DEFAULT_CONFIG_PATH: &str = "merge-app.toml";
const DEFAULT_RECONCILE_INTERVAL: &str = "1h";
const DEFAULT_WEBHOOK_LISTEN_ADDR: &str = "127.0.0.1:8080";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default = "default_reconcile_interval")]
    reconcile_interval: String,
    #[serde(default = "default_webhook_listen_addr")]
    webhook_listen_addr: String,
    allowed_accounts: Vec<u64>,
}

pub struct AppConfig {
    pub app_id: u64,
    pub private_key: PrivateKey,
    pub reconcile_interval: Duration,
    pub allowed_accounts: Vec<u64>,
    pub webhook_listen_addr: SocketAddr,
    pub webhook_secret: WebhookSecret,
}

pub struct WebhookSecret(String);

impl WebhookSecret {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for WebhookSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WebhookSecret([REDACTED])")
    }
}

pub struct PrivateKey(String);

impl PrivateKey {
    pub fn as_pem(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PrivateKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PrivateKey([REDACTED])")
    }
}

pub async fn load() -> Result<AppConfig> {
    let config_path = env::var_os("MERGE_APP_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    let app_id = required_env("GITHUB_APP_ID")?
        .parse::<u64>()
        .context("GITHUB_APP_ID must be a positive integer")?;
    if app_id == 0 {
        bail!("GITHUB_APP_ID must be greater than zero");
    }
    let private_key_path = PathBuf::from(required_env("GITHUB_APP_PRIVATE_KEY_PATH")?);
    let webhook_secret = required_env("GITHUB_WEBHOOK_SECRET")?;
    if webhook_secret.is_empty() {
        bail!("GITHUB_WEBHOOK_SECRET must not be empty");
    }

    load_from(&config_path, app_id, &private_key_path, webhook_secret).await
}

pub(crate) async fn load_from(
    config_path: &Path,
    app_id: u64,
    private_key_path: &Path,
    webhook_secret: String,
) -> Result<AppConfig> {
    if app_id == 0 {
        bail!("GITHUB_APP_ID must be greater than zero");
    }
    let config_contents = tokio::fs::read_to_string(config_path)
        .await
        .with_context(|| format!("failed to read config file {}", config_path.display()))?;
    let file_config: FileConfig = toml::from_str(&config_contents)
        .with_context(|| format!("failed to parse config file {}", config_path.display()))?;
    if file_config.allowed_accounts.contains(&0) {
        bail!("allowed_accounts entries must be greater than zero");
    }
    let reconcile_interval = humantime::parse_duration(&file_config.reconcile_interval)
        .context("reconcile_interval must be a duration such as `1h` or `30s`")?;
    if reconcile_interval.is_zero() {
        bail!("reconcile_interval must be greater than zero");
    }
    let webhook_listen_addr = file_config
        .webhook_listen_addr
        .parse::<SocketAddr>()
        .context("webhook_listen_addr must be a socket address such as `127.0.0.1:8080`")?;

    let private_key = tokio::fs::read_to_string(private_key_path)
        .await
        .with_context(|| {
            format!(
                "failed to read GitHub App private key file {}",
                private_key_path.display()
            )
        })?;
    if private_key.trim().is_empty() {
        bail!("GitHub App private key file is empty");
    }

    Ok(AppConfig {
        app_id,
        private_key: PrivateKey(private_key),
        reconcile_interval,
        allowed_accounts: file_config.allowed_accounts,
        webhook_listen_addr,
        webhook_secret: WebhookSecret(webhook_secret),
    })
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("environment variable {name} is required"))
}

fn default_reconcile_interval() -> String {
    DEFAULT_RECONCILE_INTERVAL.to_owned()
}

fn default_webhook_listen_addr() -> String {
    DEFAULT_WEBHOOK_LISTEN_ADDR.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_key_debug_output_is_redacted() {
        let key = PrivateKey("secret key material".to_owned());

        assert_eq!(format!("{key:?}"), "PrivateKey([REDACTED])");
        assert!(!format!("{key:?}").contains("secret key material"));
    }

    #[test]
    fn private_key_exposes_pem_without_exposing_it_in_debug() {
        let key = PrivateKey("PEM key data".to_owned());

        assert_eq!(key.as_pem(), "PEM key data");
        assert!(!format!("{key:?}").contains("PEM key data"));
    }

    #[test]
    fn config_uses_one_hour_default_and_numeric_account_ids() {
        let file_config: FileConfig = toml::from_str("allowed_accounts = [123, 456]").unwrap();
        let interval = humantime::parse_duration(&file_config.reconcile_interval).unwrap();

        assert_eq!(interval, Duration::from_secs(3600));
        assert_eq!(file_config.allowed_accounts, vec![123, 456]);
    }

    #[test]
    fn config_parses_a_custom_reconciliation_interval() {
        let file_config: FileConfig =
            toml::from_str("reconcile_interval = \"30s\"\nallowed_accounts = []").unwrap();

        assert_eq!(
            humantime::parse_duration(&file_config.reconcile_interval).unwrap(),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn config_uses_local_webhook_listen_address_by_default() {
        let file_config: FileConfig = toml::from_str("allowed_accounts = []").unwrap();

        assert_eq!(file_config.webhook_listen_addr, DEFAULT_WEBHOOK_LISTEN_ADDR);
    }

    #[test]
    fn config_accepts_a_custom_webhook_listen_address() {
        let file_config: FileConfig =
            toml::from_str("webhook_listen_addr = \"0.0.0.0:9000\"\nallowed_accounts = []")
                .unwrap();

        assert_eq!(
            file_config
                .webhook_listen_addr
                .parse::<SocketAddr>()
                .unwrap(),
            "0.0.0.0:9000".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn webhook_secret_debug_output_is_redacted() {
        let secret = WebhookSecret("secret value".to_owned());

        assert_eq!(format!("{secret:?}"), "WebhookSecret([REDACTED])");
        assert!(!format!("{secret:?}").contains("secret value"));
    }

    #[test]
    fn config_rejects_unknown_fields() {
        let result = toml::from_str::<FileConfig>(
            "reconciliation_interval = \"30s\"\nallowed_accounts = []",
        );

        assert!(result.is_err());
    }
}
