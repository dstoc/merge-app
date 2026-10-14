use std::{
    env, fmt,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const DEFAULT_CONFIG_PATH: &str = "merge-app.toml";
const DEFAULT_RECONCILE_INTERVAL: &str = "5m";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default = "default_reconcile_interval")]
    reconcile_interval: String,
    allowed_accounts: Vec<u64>,
}

pub struct AppConfig {
    pub app_id: u64,
    pub private_key: PrivateKey,
    pub reconcile_interval: Duration,
    pub allowed_accounts: Vec<u64>,
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

    load_from(&config_path, app_id, &private_key_path).await
}

pub(crate) async fn load_from(
    config_path: &Path,
    app_id: u64,
    private_key_path: &Path,
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
        .context("reconcile_interval must be a duration such as `5m` or `30s`")?;
    if reconcile_interval.is_zero() {
        bail!("reconcile_interval must be greater than zero");
    }

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
    })
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("environment variable {name} is required"))
}

fn default_reconcile_interval() -> String {
    DEFAULT_RECONCILE_INTERVAL.to_owned()
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
    fn config_uses_five_minute_default_and_numeric_account_ids() {
        let file_config: FileConfig = toml::from_str("allowed_accounts = [123, 456]").unwrap();
        let interval = humantime::parse_duration(&file_config.reconcile_interval).unwrap();

        assert_eq!(interval, Duration::from_secs(300));
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
    fn config_rejects_unknown_fields() {
        let result = toml::from_str::<FileConfig>(
            "reconciliation_interval = \"30s\"\nallowed_accounts = []",
        );

        assert!(result.is_err());
    }
}
