use std::{collections::HashSet, future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{net::TcpListener, sync::Notify, time};
use tracing::{debug, error, info, warn};

use crate::{
    auth::{
        AppInstallation, GitHubAppAuthenticator, InstallationCredentials, InstallationRepository,
    },
    config::AppConfig,
    coordinator::RepositoryWorkCoordinator,
    github::RepositoryLocator,
    webhook::{self, WebhookState},
};

trait InstallationDiscovery {
    fn app_installations(&self) -> impl Future<Output = Result<Vec<AppInstallation>>> + Send;

    fn installation_listing_token(
        &self,
        installation_id: u64,
    ) -> impl Future<Output = Result<InstallationCredentials>> + Send;

    fn installation_repositories<'a>(
        &'a self,
        credentials: &'a InstallationCredentials,
    ) -> impl Future<Output = Result<Vec<InstallationRepository>>> + Send + 'a;
}

impl InstallationDiscovery for GitHubAppAuthenticator {
    fn app_installations(&self) -> impl Future<Output = Result<Vec<AppInstallation>>> + Send {
        async move {
            GitHubAppAuthenticator::app_installations(self)
                .await
                .map_err(Into::into)
        }
    }

    fn installation_listing_token(
        &self,
        installation_id: u64,
    ) -> impl Future<Output = Result<InstallationCredentials>> + Send {
        async move {
            GitHubAppAuthenticator::installation_listing_token(self, installation_id)
                .await
                .map_err(Into::into)
        }
    }

    fn installation_repositories<'a>(
        &'a self,
        credentials: &'a InstallationCredentials,
    ) -> impl Future<Output = Result<Vec<InstallationRepository>>> + Send + 'a {
        async move {
            GitHubAppAuthenticator::installation_repositories(self, credentials)
                .await
                .map_err(Into::into)
        }
    }
}

trait Reconciler {
    async fn reconcile(&self) -> Result<usize>;
}

struct ServiceReconciler<F> {
    authenticator: Arc<GitHubAppAuthenticator>,
    allowed_accounts: Vec<u64>,
    coordinator: RepositoryWorkCoordinator<F>,
}

impl<F, Fut> Reconciler for ServiceReconciler<F>
where
    F: Fn(RepositoryLocator) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    async fn reconcile(&self) -> Result<usize> {
        reconcile_once(
            self.authenticator.as_ref(),
            &self.allowed_accounts,
            &self.coordinator,
        )
        .await
    }
}

pub async fn run(config: AppConfig) -> Result<()> {
    let authenticator = Arc::new(GitHubAppAuthenticator::new(
        config.app_id,
        config.app_id.to_string(),
        config.private_key.as_pem().as_bytes(),
    )?);
    let app = authenticator.verify_app().await?;
    info!(event = "github_app_authenticated", app_id = app.id, app_name = %app.name);

    let queries = crate::github::GitHubQueries::new(authenticator.http_client());
    let mutations = crate::queue::GitHubQueueMutations::new(authenticator.http_client());
    let worker_authenticator = Arc::clone(&authenticator);
    let coordinator = RepositoryWorkCoordinator::new(move |repository: RepositoryLocator| {
        let authenticator = Arc::clone(&worker_authenticator);
        let queries = queries.clone();
        let mutations = mutations.clone();
        async move {
            let credentials = authenticator
                .installation_token(
                    repository.installation_id,
                    &repository.owner,
                    &repository.name,
                )
                .await?;
            let outcome =
                crate::queue::advance(&repository, &credentials, &queries, &mutations).await?;
            info!(
                event = "repository_advance_completed",
                installation_id = repository.installation_id,
                repository = %format!("{}/{}", repository.owner, repository.name),
                ?outcome,
            );
            Ok(())
        }
    });
    let reconciler = ServiceReconciler {
        authenticator,
        allowed_accounts: config.allowed_accounts.clone(),
        coordinator: coordinator.clone(),
    };

    let listener = TcpListener::bind(config.webhook_listen_addr)
        .await
        .with_context(|| {
            format!(
                "failed to bind GitHub webhook listener at {}",
                config.webhook_listen_addr
            )
        })?;
    let reconcile_signal = Arc::new(Notify::new());
    let webhook_coordinator = coordinator.clone();
    let webhook_state = Arc::new(WebhookState::new(
        config.webhook_secret.as_bytes(),
        &config.allowed_accounts,
        move |repository| webhook_coordinator.mark_dirty(repository),
        Arc::clone(&reconcile_signal),
    ));
    let mut webhook_server = tokio::spawn(async move {
        axum::serve(listener, webhook::router(webhook_state))
            .await
            .context("GitHub webhook server stopped")
    });
    info!(event = "github_webhook_server_started", listen_addr = %config.webhook_listen_addr);

    tokio::select! {
        result = run_until(
        config.app_id,
        config.allowed_accounts.len(),
        config.reconcile_interval,
        &reconciler,
        &reconcile_signal,
        shutdown_signal(),
        ) => {
            webhook_server.abort();
            let _ = webhook_server.await;
            result
        }
        server_result = &mut webhook_server => {
            server_result.context("GitHub webhook server task failed")??;
            Ok(())
        }
    }
}

async fn reconcile_once<S, F, Fut>(
    source: &S,
    allowed_accounts: &[u64],
    coordinator: &RepositoryWorkCoordinator<F>,
) -> Result<usize>
where
    S: InstallationDiscovery,
    F: Fn(RepositoryLocator) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let installations = source
        .app_installations()
        .await
        .context("failed to list GitHub App installations")?;
    let allowed_accounts = allowed_accounts.iter().copied().collect::<HashSet<_>>();
    let mut marked_repositories = 0;

    for installation in installations {
        let Some(account) = installation.account else {
            warn!(
                event = "installation_ignored",
                installation_id = installation.id,
                reason = "account_missing",
            );
            continue;
        };
        if !allowed_accounts.contains(&account.id) {
            debug!(
                event = "installation_ignored",
                installation_id = installation.id,
                account_id = account.id,
                reason = "account_not_allowlisted",
            );
            continue;
        }

        let credentials = match source.installation_listing_token(installation.id).await {
            Ok(credentials) => credentials,
            Err(discovery_error) => {
                warn!(
                    event = "installation_reconciliation_failed",
                    installation_id = installation.id,
                    error = %discovery_error,
                );
                continue;
            }
        };
        let repositories = match source.installation_repositories(&credentials).await {
            Ok(repositories) => repositories,
            Err(discovery_error) => {
                warn!(
                    event = "installation_reconciliation_failed",
                    installation_id = installation.id,
                    error = %discovery_error,
                );
                continue;
            }
        };

        let repository_count = repositories.len();
        for repository in repositories {
            let locator = match RepositoryLocator::new(
                installation.id,
                repository.owner.login,
                repository.name,
            ) {
                Ok(locator) => locator,
                Err(locator_error) => {
                    warn!(
                        event = "repository_ignored",
                        installation_id = installation.id,
                        error = %locator_error,
                    );
                    continue;
                }
            };
            coordinator.mark_dirty(locator)?;
            marked_repositories += 1;
        }
        info!(
            event = "installation_reconciled",
            installation_id = installation.id,
            account_id = account.id,
            repository_count,
        );
    }

    info!(
        event = "reconciliation_scheduled",
        repository_count = marked_repositories
    );
    Ok(marked_repositories)
}

async fn run_until<R, F>(
    app_id: u64,
    allowed_account_count: usize,
    interval: Duration,
    reconciler: &R,
    reconcile_signal: &Notify,
    shutdown: F,
) -> Result<()>
where
    R: Reconciler,
    F: Future<Output = Result<&'static str>>,
{
    tokio::pin!(shutdown);
    info!(
        event = "service_started",
        app_id,
        allowed_account_count,
        reconcile_interval_seconds = interval.as_secs_f64(),
    );

    let mut ticker = time::interval_at(time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let shutdown_reason = tokio::select! {
        biased;
        reason = &mut shutdown => reason?,
        result = reconciler.reconcile() => {
            log_reconciliation(result);
            loop {
                tokio::select! {
                    biased;
                    reason = &mut shutdown => break reason?,
                    _ = reconcile_signal.notified() => {
                        let result = tokio::select! {
                            biased;
                            reason = &mut shutdown => break reason?,
                            result = reconciler.reconcile() => result,
                        };
                        log_reconciliation(result);
                    }
                    _ = ticker.tick() => {
                        let result = tokio::select! {
                            biased;
                            reason = &mut shutdown => break reason?,
                            result = reconciler.reconcile() => result,
                        };
                        log_reconciliation(result);
                    }
                }
            }
        }
    };

    info!(event = "service_stopping", reason = shutdown_reason);
    info!(event = "service_stopped");
    Ok(())
}

fn log_reconciliation(result: Result<usize>) {
    match result {
        Ok(repository_count) => {
            info!(event = "reconciliation_pass_completed", repository_count);
        }
        Err(reconciliation_error) => {
            error!(event = "reconciliation_pass_failed", error = %reconciliation_error);
        }
    }
}

async fn shutdown_signal() -> Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut interrupt = tokio::signal::unix::signal(SignalKind::interrupt())
            .context("failed to listen for SIGINT")?;
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
        collections::{HashMap, HashSet},
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::{
        sync::{Notify, mpsc},
        time::timeout,
    };

    use super::*;
    use crate::auth::{InstallationAccount, RepositoryOwner};

    struct FakeDiscovery {
        installations: Vec<AppInstallation>,
        repositories: HashMap<u64, Vec<InstallationRepository>>,
        failing_repository_installs: HashSet<u64>,
        token_requests: Mutex<Vec<u64>>,
        repository_requests: Mutex<Vec<u64>>,
    }

    impl InstallationDiscovery for FakeDiscovery {
        async fn app_installations(&self) -> Result<Vec<AppInstallation>> {
            Ok(self.installations.clone())
        }

        async fn installation_listing_token(
            &self,
            installation_id: u64,
        ) -> Result<InstallationCredentials> {
            self.token_requests.lock().unwrap().push(installation_id);
            Ok(InstallationCredentials {
                installation_id,
                token: format!("listing-token-{installation_id}"),
                expires_at: "2030-01-01T00:00:00Z".to_owned(),
                permissions: Default::default(),
            })
        }

        async fn installation_repositories(
            &self,
            credentials: &InstallationCredentials,
        ) -> Result<Vec<InstallationRepository>> {
            self.repository_requests
                .lock()
                .unwrap()
                .push(credentials.installation_id);
            if self
                .failing_repository_installs
                .contains(&credentials.installation_id)
            {
                anyhow::bail!("simulated unavailable installation");
            }
            Ok(self
                .repositories
                .get(&credentials.installation_id)
                .cloned()
                .unwrap_or_default())
        }
    }

    fn installation(id: u64, account_id: Option<u64>) -> AppInstallation {
        AppInstallation {
            id,
            account: account_id.map(|id| InstallationAccount { id }),
        }
    }

    fn repository(owner: &str, name: &str) -> InstallationRepository {
        InstallationRepository {
            name: name.to_owned(),
            owner: RepositoryOwner {
                login: owner.to_owned(),
            },
        }
    }

    #[tokio::test]
    async fn reconciliation_marks_only_allowlisted_repositories_and_continues_after_failure() {
        let discovery = FakeDiscovery {
            installations: vec![
                installation(10, Some(100)),
                installation(20, Some(200)),
                installation(30, None),
                installation(11, Some(100)),
            ],
            repositories: HashMap::from([(
                10,
                vec![
                    repository("Allowed", "first"),
                    repository("Allowed", "second"),
                ],
            )]),
            failing_repository_installs: HashSet::from([11]),
            token_requests: Mutex::new(Vec::new()),
            repository_requests: Mutex::new(Vec::new()),
        };
        let (marked_tx, mut marked_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new(move |repository: RepositoryLocator| {
            let marked_tx = marked_tx.clone();
            async move {
                marked_tx
                    .send((
                        repository.installation_id,
                        repository.owner,
                        repository.name,
                    ))
                    .unwrap();
                Ok(())
            }
        });

        let count = reconcile_once(&discovery, &[100], &coordinator)
            .await
            .unwrap();
        assert_eq!(count, 2);

        let mut marked = vec![
            timeout(Duration::from_secs(2), marked_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            timeout(Duration::from_secs(2), marked_rx.recv())
                .await
                .unwrap()
                .unwrap(),
        ];
        marked.sort();
        assert_eq!(
            marked,
            vec![
                (10, "Allowed".to_owned(), "first".to_owned()),
                (10, "Allowed".to_owned(), "second".to_owned()),
            ]
        );
        assert_eq!(*discovery.token_requests.lock().unwrap(), vec![10, 11]);
        assert_eq!(*discovery.repository_requests.lock().unwrap(), vec![10, 11]);
    }

    struct CountingReconciler {
        calls: AtomicUsize,
        shutdown: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl Reconciler for CountingReconciler {
        async fn reconcile(&self) -> Result<usize> {
            let calls = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if calls == 3 {
                self.shutdown
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
            }
            Ok(0)
        }
    }

    #[tokio::test]
    async fn service_reconciles_on_startup_and_on_the_periodic_timer() {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let reconciler = CountingReconciler {
            calls: AtomicUsize::new(0),
            shutdown: Mutex::new(Some(shutdown_tx)),
        };

        run_until(
            1,
            0,
            Duration::from_millis(1),
            &reconciler,
            &Notify::new(),
            async move {
                shutdown_rx
                    .await
                    .map(|()| "test signal")
                    .map_err(anyhow::Error::from)
            },
        )
        .await
        .unwrap();

        assert_eq!(reconciler.calls.load(Ordering::SeqCst), 3);
    }
}
