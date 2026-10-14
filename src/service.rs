use std::future::Future;

use anyhow::{Context, Result};
use tokio::time;
use tracing::{debug, info};

use crate::config::AppConfig;

pub async fn run(config: AppConfig) -> Result<()> {
    run_until(config, shutdown_signal()).await
}

async fn run_until<F>(config: AppConfig, shutdown: F) -> Result<()>
where
    F: Future<Output = Result<&'static str>>,
{
    tokio::pin!(shutdown);
    let interval = config.reconcile_interval;
    info!(
        event = "service_started",
        app_id = config.app_id,
        allowed_account_count = config.allowed_accounts.len(),
        reconcile_interval_seconds = interval.as_secs_f64(),
    );

    let mut ticker = time::interval(interval);
    let shutdown_reason = loop {
        tokio::select! {
            reason = &mut shutdown => break reason?,
            _ = ticker.tick() => {
                debug!(
                    event = "reconciliation_timer_elapsed",
                    reconcile_interval_seconds = interval.as_secs_f64(),
                );
            }
        }
    };

    info!(event = "service_stopping", reason = shutdown_reason);
    info!(event = "service_stopped");
    Ok(())
}

async fn shutdown_signal() -> Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut interrupt =
            signal(SignalKind::interrupt()).context("failed to listen for SIGINT")?;
        let mut terminate =
            signal(SignalKind::terminate()).context("failed to listen for SIGTERM")?;
        tokio::select! {
            _ = interrupt.recv() => Ok("SIGINT"),
            _ = terminate.recv() => Ok("SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for Ctrl+C")?;
        Ok("Ctrl+C")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    use super::*;

    static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn service_returns_after_a_shutdown_signal() {
        let temp_dir = std::env::temp_dir().join(format!(
            "merge-app-service-test-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let config_path = temp_dir.join("merge-app.toml");
        let key_path = temp_dir.join("app.pem");
        tokio::fs::write(&config_path, "allowed_accounts = [123]\n")
            .await
            .unwrap();
        tokio::fs::write(&key_path, "test private key\n")
            .await
            .unwrap();
        let config = crate::config::load_from(&config_path, 456, &key_path)
            .await
            .unwrap();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let service = tokio::spawn(run_until(config, async move {
            shutdown_rx
                .await
                .map(|()| "test signal")
                .map_err(anyhow::Error::from)
        }));
        shutdown_tx.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(2), service)
            .await
            .expect("service should stop promptly")
            .expect("service task should not panic")
            .expect("service should return successfully");
        tokio::fs::remove_dir_all(temp_dir).await.unwrap();
    }
}
