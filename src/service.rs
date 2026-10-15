use std::{collections::HashSet, future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{net::TcpListener, sync::Notify, time};
use tracing::{error, info, warn};

use crate::{
    auth::{
        AppInstallation, GitHubAppAuthenticator, InstallationCredentials, InstallationRepository,
    },
    config::AppConfig,
    coordinator::{IntoWorkerDirective, RepositoryWorkCoordinator, RetryReason, WorkerDirective},
    github::RepositoryLocator,
    pull_request::RebaseOutcome,
    queue::{AdvanceOutcome, WaitingReason},
    webhook::{self, LifecycleChange, WebhookState},
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

    fn uninstall_installation(
        &self,
        installation_id: u64,
    ) -> impl Future<Output = Result<()>> + Send;
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

    fn uninstall_installation(
        &self,
        installation_id: u64,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            GitHubAppAuthenticator::uninstall_installation(self, installation_id)
                .await
                .map_err(Into::into)
        }
    }
}

trait Reconciler {
    async fn reconcile(&self) -> Result<usize>;
}

#[derive(Clone, Copy)]
enum ReconciliationTrigger {
    Startup,
    Periodic,
    WebhookLifecycle,
}

impl ReconciliationTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Periodic => "periodic",
            Self::WebhookLifecycle => "webhook_lifecycle",
        }
    }
}

struct ServiceReconciler<F> {
    authenticator: Arc<GitHubAppAuthenticator>,
    allowed_accounts: Vec<u64>,
    coordinator: RepositoryWorkCoordinator<F>,
}

impl<F, Fut> Reconciler for ServiceReconciler<F>
where
    F: Fn(RepositoryLocator) -> Fut + Send + Sync + 'static,
    Fut: Future + Send + 'static,
    Fut::Output: IntoWorkerDirective,
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
    crate::auth::validate_worker_permissions(&app.permissions, "GitHub App")
        .map_err(anyhow::Error::msg)?;
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
            Ok(worker_directive_for_outcome(&outcome))
        }
    });
    let reconciler = ServiceReconciler {
        authenticator: Arc::clone(&authenticator),
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
    let removal_authenticator = Arc::clone(&authenticator);
    let webhook_state = Arc::new(WebhookState::new(
        config.webhook_secret.as_bytes(),
        &config.allowed_accounts,
        move |repository| webhook_coordinator.mark_dirty(repository),
        Arc::clone(&reconcile_signal),
        move |installation_id| {
            let authenticator = Arc::clone(&removal_authenticator);
            async move {
                authenticator
                    .uninstall_installation(installation_id)
                    .await
                    .map_err(Into::into)
            }
        },
        {
            let coordinator = coordinator.clone();
            move |change| match change {
                LifecycleChange::InstallationRemoved(installation_id) => {
                    coordinator.block_installation(installation_id);
                }
                LifecycleChange::InstallationRestored(installation_id) => {
                    coordinator.unblock_installation(installation_id);
                }
                LifecycleChange::RepositoryRemoved(repository) => {
                    coordinator.block_repository(&repository);
                }
                LifecycleChange::RepositoryRestored(repository) => {
                    coordinator.unblock_repository(&repository);
                }
                LifecycleChange::RepositoryAccessChanged(installation_id) => {
                    coordinator.require_repository_refresh(installation_id);
                }
            }
        },
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
    Fut: Future + Send + 'static,
    Fut::Output: IntoWorkerDirective,
{
    let scheduling_epoch = coordinator.scheduling_epoch();
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
            info!(
                event = "installation_uninstall_requested",
                installation_id = installation.id,
                account_id = account.id,
                reason = "account_not_allowlisted",
            );
            if let Err(removal_error) = source.uninstall_installation(installation.id).await {
                warn!(
                    event = "installation_removal_failed",
                    installation_id = installation.id,
                    account_id = account.id,
                    retry = "reconciliation",
                    error = %removal_error,
                );
            } else {
                info!(
                    event = "installation_uninstalled",
                    installation_id = installation.id,
                    account_id = account.id,
                    reason = "account_not_allowlisted",
                );
            }
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
        let mut repository_locators = Vec::with_capacity(repository_count);
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
            repository_locators.push(locator);
        }
        if !coordinator.apply_authoritative_repository_listing(
            installation.id,
            &repository_locators,
            scheduling_epoch,
        ) {
            warn!(
                event = "installation_reconciliation_stale",
                installation_id = installation.id,
                reason = "lifecycle_changed",
            );
            continue;
        }
        for locator in repository_locators {
            if coordinator.mark_dirty_at_epoch(locator, scheduling_epoch)? {
                marked_repositories += 1;
            }
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

fn worker_directive_for_outcome(outcome: &AdvanceOutcome) -> WorkerDirective {
    match outcome {
        AdvanceOutcome::Waiting {
            reason: WaitingReason::MergeabilityUnknown,
            ..
        } => WorkerDirective::Retry(RetryReason::MergeabilityUnknown),
        AdvanceOutcome::Waiting {
            reason: WaitingReason::BaseRelationshipUnknown,
            ..
        } => WorkerDirective::Retry(RetryReason::BaseRelationshipUnknown),
        AdvanceOutcome::RebaseAttempted {
            outcome: RebaseOutcome::StaleHead { .. },
            ..
        } => WorkerDirective::Retry(RetryReason::StaleHead),
        AdvanceOutcome::NoActionablePullRequests
        | AdvanceOutcome::AlreadyCurrent { .. }
        | AdvanceOutcome::RebaseAttempted { .. } => WorkerDirective::Complete,
    }
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
            log_reconciliation(ReconciliationTrigger::Startup, result);
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
                        log_reconciliation(ReconciliationTrigger::WebhookLifecycle, result);
                    }
                    _ = ticker.tick() => {
                        let result = tokio::select! {
                            biased;
                            reason = &mut shutdown => break reason?,
                            result = reconciler.reconcile() => result,
                        };
                        log_reconciliation(ReconciliationTrigger::Periodic, result);
                    }
                }
            }
        }
    };

    info!(event = "service_stopping", reason = shutdown_reason);
    info!(event = "service_stopped");
    Ok(())
}

fn log_reconciliation(trigger: ReconciliationTrigger, result: Result<usize>) {
    match result {
        Ok(repository_count) => {
            info!(
                event = "reconciliation_pass_completed",
                trigger = trigger.as_str(),
                repository_count
            );
        }
        Err(reconciliation_error) => {
            error!(
                event = "reconciliation_pass_failed",
                trigger = trigger.as_str(),
                error = %reconciliation_error
            );
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
        io,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::{
        sync::{Notify, mpsc, oneshot},
        time::{advance, timeout},
    };
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
    use crate::auth::{InstallationAccount, RepositoryOwner};

    struct FakeDiscovery {
        installations: Vec<AppInstallation>,
        repositories: HashMap<u64, Vec<InstallationRepository>>,
        failing_repository_installs: HashSet<u64>,
        failing_removal_attempts: Mutex<HashMap<u64, usize>>,
        removal_requests: Mutex<Vec<u64>>,
        token_requests: Mutex<Vec<u64>>,
        repository_requests: Mutex<Vec<u64>>,
        on_repository_listing: Option<Arc<dyn Fn(u64) + Send + Sync>>,
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
            if let Some(on_repository_listing) = &self.on_repository_listing {
                on_repository_listing(credentials.installation_id);
            }
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

        async fn uninstall_installation(&self, installation_id: u64) -> Result<()> {
            self.removal_requests.lock().unwrap().push(installation_id);
            let mut failing_attempts = self.failing_removal_attempts.lock().unwrap();
            if let Some(remaining) = failing_attempts.get_mut(&installation_id)
                && *remaining > 0
            {
                *remaining -= 1;
                anyhow::bail!("simulated installation removal failure");
            }
            Ok(())
        }
    }

    struct DiscoveryReconciler<'a, F> {
        discovery: &'a FakeDiscovery,
        allowed_accounts: &'a [u64],
        coordinator: &'a RepositoryWorkCoordinator<F>,
    }

    impl<F, Fut> Reconciler for DiscoveryReconciler<'_, F>
    where
        F: Fn(RepositoryLocator) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: IntoWorkerDirective,
    {
        async fn reconcile(&self) -> Result<usize> {
            reconcile_once(self.discovery, self.allowed_accounts, self.coordinator).await
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

    #[test]
    fn queue_outcomes_retry_only_transient_states_and_stale_heads() {
        assert_eq!(
            worker_directive_for_outcome(&AdvanceOutcome::Waiting {
                pull_request_number: 1,
                reason: WaitingReason::MergeabilityUnknown,
            }),
            WorkerDirective::Retry(RetryReason::MergeabilityUnknown)
        );
        assert_eq!(
            worker_directive_for_outcome(&AdvanceOutcome::Waiting {
                pull_request_number: 1,
                reason: WaitingReason::BaseRelationshipUnknown,
            }),
            WorkerDirective::Retry(RetryReason::BaseRelationshipUnknown)
        );
        assert_eq!(
            worker_directive_for_outcome(&AdvanceOutcome::RebaseAttempted {
                pull_request_number: 1,
                outcome: RebaseOutcome::StaleHead {
                    message: "head changed".to_owned(),
                },
            }),
            WorkerDirective::Retry(RetryReason::StaleHead)
        );

        for stable in [
            AdvanceOutcome::NoActionablePullRequests,
            AdvanceOutcome::AlreadyCurrent {
                pull_request_number: 1,
            },
            AdvanceOutcome::RebaseAttempted {
                pull_request_number: 1,
                outcome: RebaseOutcome::Rebased,
            },
            AdvanceOutcome::RebaseAttempted {
                pull_request_number: 1,
                outcome: RebaseOutcome::Conflict {
                    message: "conflict".to_owned(),
                },
            },
        ] {
            assert_eq!(
                worker_directive_for_outcome(&stable),
                WorkerDirective::Complete
            );
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
            failing_removal_attempts: Mutex::new(HashMap::new()),
            removal_requests: Mutex::new(Vec::new()),
            token_requests: Mutex::new(Vec::new()),
            repository_requests: Mutex::new(Vec::new()),
            on_repository_listing: None,
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
        assert_eq!(*discovery.removal_requests.lock().unwrap(), vec![20]);
    }

    #[tokio::test]
    async fn reconciliation_restores_access_from_the_authoritative_listing() {
        let discovery = FakeDiscovery {
            installations: vec![installation(10, Some(100))],
            repositories: HashMap::from([(10, vec![repository("Allowed", "restored")])]),
            failing_repository_installs: HashSet::new(),
            failing_removal_attempts: Mutex::new(HashMap::new()),
            removal_requests: Mutex::new(Vec::new()),
            token_requests: Mutex::new(Vec::new()),
            repository_requests: Mutex::new(Vec::new()),
            on_repository_listing: None,
        };
        let (scheduled_tx, mut scheduled_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new(move |repository: RepositoryLocator| {
            let scheduled_tx = scheduled_tx.clone();
            async move {
                scheduled_tx
                    .send((repository.owner, repository.name))
                    .unwrap();
                Ok(())
            }
        });
        let restored = RepositoryLocator::new(10, "Allowed", "restored").unwrap();
        coordinator.block_repository(&restored);

        assert_eq!(
            reconcile_once(&discovery, &[100], &coordinator)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            timeout(Duration::from_secs(2), scheduled_rx.recv())
                .await
                .unwrap(),
            Some(("Allowed".to_owned(), "restored".to_owned()))
        );
    }

    #[tokio::test]
    async fn reconciliation_restores_a_blocked_installation_from_successful_discovery() {
        let discovery = FakeDiscovery {
            installations: vec![installation(10, Some(100))],
            repositories: HashMap::from([(10, vec![repository("Allowed", "restored")])]),
            failing_repository_installs: HashSet::new(),
            failing_removal_attempts: Mutex::new(HashMap::new()),
            removal_requests: Mutex::new(Vec::new()),
            token_requests: Mutex::new(Vec::new()),
            repository_requests: Mutex::new(Vec::new()),
            on_repository_listing: None,
        };
        let (scheduled_tx, mut scheduled_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new(move |repository: RepositoryLocator| {
            let scheduled_tx = scheduled_tx.clone();
            async move {
                scheduled_tx
                    .send((repository.owner, repository.name))
                    .unwrap();
                Ok(())
            }
        });
        coordinator.block_installation(10);

        assert_eq!(
            reconcile_once(&discovery, &[100], &coordinator)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            timeout(Duration::from_secs(2), scheduled_rx.recv())
                .await
                .unwrap(),
            Some(("Allowed".to_owned(), "restored".to_owned()))
        );
    }

    #[tokio::test]
    async fn failed_installation_discovery_keeps_the_installation_blocked() {
        let discovery = FakeDiscovery {
            installations: vec![installation(10, Some(100))],
            repositories: HashMap::new(),
            failing_repository_installs: HashSet::from([10]),
            failing_removal_attempts: Mutex::new(HashMap::new()),
            removal_requests: Mutex::new(Vec::new()),
            token_requests: Mutex::new(Vec::new()),
            repository_requests: Mutex::new(Vec::new()),
            on_repository_listing: None,
        };
        let coordinator =
            RepositoryWorkCoordinator::new(|_repository: RepositoryLocator| async { Ok(()) });
        coordinator.block_installation(10);

        assert_eq!(
            reconcile_once(&discovery, &[100], &coordinator)
                .await
                .unwrap(),
            0
        );
        let repository = RepositoryLocator::new(10, "Allowed", "restored").unwrap();
        assert!(
            !coordinator
                .mark_dirty_at_epoch(repository, coordinator.scheduling_epoch())
                .unwrap()
        );
    }

    #[tokio::test]
    async fn stale_installation_discovery_keeps_the_installation_blocked() {
        let (scheduled_tx, mut scheduled_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new(move |repository: RepositoryLocator| {
            let scheduled_tx = scheduled_tx.clone();
            async move {
                scheduled_tx.send(repository.name).unwrap();
                Ok(())
            }
        });
        let lifecycle_coordinator = coordinator.clone();
        let discovery = FakeDiscovery {
            installations: vec![installation(10, Some(100))],
            repositories: HashMap::from([(10, vec![repository("Allowed", "restored")])]),
            failing_repository_installs: HashSet::new(),
            failing_removal_attempts: Mutex::new(HashMap::new()),
            removal_requests: Mutex::new(Vec::new()),
            token_requests: Mutex::new(Vec::new()),
            repository_requests: Mutex::new(Vec::new()),
            on_repository_listing: Some(Arc::new(move |installation_id| {
                lifecycle_coordinator.block_installation(installation_id);
            })),
        };

        assert_eq!(
            reconcile_once(&discovery, &[100], &coordinator)
                .await
                .unwrap(),
            0
        );
        let repository = RepositoryLocator::new(10, "Allowed", "restored").unwrap();
        assert!(
            !coordinator
                .mark_dirty_at_epoch(repository, coordinator.scheduling_epoch())
                .unwrap()
        );
        assert!(scheduled_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn reconciliation_retries_unallowlisted_installation_removal_on_the_next_pass() {
        let discovery = FakeDiscovery {
            installations: vec![installation(20, Some(200))],
            repositories: HashMap::new(),
            failing_repository_installs: HashSet::new(),
            failing_removal_attempts: Mutex::new(HashMap::from([(20, 1)])),
            removal_requests: Mutex::new(Vec::new()),
            token_requests: Mutex::new(Vec::new()),
            repository_requests: Mutex::new(Vec::new()),
            on_repository_listing: None,
        };
        let coordinator =
            RepositoryWorkCoordinator::new(|_repository: RepositoryLocator| async { Ok(()) });

        assert_eq!(
            reconcile_once(&discovery, &[100], &coordinator)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            reconcile_once(&discovery, &[100], &coordinator)
                .await
                .unwrap(),
            0
        );

        assert_eq!(*discovery.removal_requests.lock().unwrap(), vec![20, 20]);
    }

    struct CountingReconciler {
        calls: AtomicUsize,
        call_tx: mpsc::UnboundedSender<usize>,
    }

    impl Reconciler for CountingReconciler {
        async fn reconcile(&self) -> Result<usize> {
            let calls = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.call_tx.send(calls).unwrap();
            Ok(0)
        }
    }

    #[derive(Clone)]
    struct SharedLogBuffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for SharedLogBuffer {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedLogBuffer {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn service_reconciles_immediately_on_startup_and_lifecycle_and_on_timer() {
        let interval = Duration::from_secs(60);
        let (call_tx, mut call_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let reconcile_signal = Arc::new(Notify::new());
        let log_buffer = SharedLogBuffer(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(log_buffer.clone())
            .finish();
        let reconciler = Arc::new(CountingReconciler {
            calls: AtomicUsize::new(0),
            call_tx,
        });

        let service_task = tokio::spawn({
            let reconciler = Arc::clone(&reconciler);
            let reconcile_signal = Arc::clone(&reconcile_signal);
            async move {
                run_until(
                    1,
                    0,
                    interval,
                    reconciler.as_ref(),
                    reconcile_signal.as_ref(),
                    async move {
                        shutdown_rx
                            .await
                            .map(|()| "test signal")
                            .map_err(anyhow::Error::from)
                    },
                )
                .with_subscriber(subscriber)
                .await
            }
        });

        assert_eq!(call_rx.recv().await, Some(1));
        assert_eq!(reconciler.calls.load(Ordering::SeqCst), 1);

        advance(interval - Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(reconciler.calls.load(Ordering::SeqCst), 1);

        reconcile_signal.notify_one();
        assert_eq!(call_rx.recv().await, Some(2));

        advance(Duration::from_secs(1)).await;
        assert_eq!(call_rx.recv().await, Some(3));

        shutdown_tx.send(()).unwrap();
        service_task.await.unwrap().unwrap();

        let logs = String::from_utf8(log_buffer.0.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("trigger=\"startup\""));
        assert!(logs.contains("trigger=\"webhook_lifecycle\""));
        assert!(logs.contains("trigger=\"periodic\""));
    }

    struct StartupGateReconciler {
        calls: AtomicUsize,
        call_tx: mpsc::UnboundedSender<usize>,
        gate: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    }

    impl Reconciler for StartupGateReconciler {
        async fn reconcile(&self) -> Result<usize> {
            let calls = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.call_tx.send(calls).unwrap();
            if calls == 1 {
                self.gate
                    .lock()
                    .await
                    .take()
                    .expect("startup gate is present")
                    .await?;
            }
            Ok(0)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn missed_periodic_ticks_are_delayed_without_a_catch_up_burst() {
        let interval = Duration::from_secs(60);
        let (call_tx, mut call_rx) = mpsc::unbounded_channel();
        let (gate_tx, gate_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let reconcile_signal = Arc::new(Notify::new());
        let reconciler = Arc::new(StartupGateReconciler {
            calls: AtomicUsize::new(0),
            call_tx,
            gate: tokio::sync::Mutex::new(Some(gate_rx)),
        });

        let service_task = tokio::spawn({
            let reconciler = Arc::clone(&reconciler);
            let reconcile_signal = Arc::clone(&reconcile_signal);
            async move {
                run_until(
                    1,
                    0,
                    interval,
                    reconciler.as_ref(),
                    reconcile_signal.as_ref(),
                    async move {
                        shutdown_rx
                            .await
                            .map(|()| "test signal")
                            .map_err(anyhow::Error::from)
                    },
                )
                .await
            }
        });

        assert_eq!(call_rx.recv().await, Some(1));
        advance(interval * 5).await;
        gate_tx.send(()).unwrap();
        assert_eq!(call_rx.recv().await, Some(2));

        advance(interval - Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(reconciler.calls.load(Ordering::SeqCst), 2);

        advance(Duration::from_secs(1)).await;
        assert_eq!(call_rx.recv().await, Some(3));
        shutdown_tx.send(()).unwrap();
        service_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn periodic_reconciliation_repairs_a_missed_webhook() {
        let discovery = FakeDiscovery {
            installations: vec![installation(10, Some(100))],
            repositories: HashMap::from([(10, vec![repository("Allowed", "missed-event")])]),
            failing_repository_installs: HashSet::new(),
            failing_removal_attempts: Mutex::new(HashMap::new()),
            removal_requests: Mutex::new(Vec::new()),
            token_requests: Mutex::new(Vec::new()),
            repository_requests: Mutex::new(Vec::new()),
            on_repository_listing: None,
        };
        let allowed_accounts = [100];
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let shutdown = Arc::new(Mutex::new(Some(shutdown_tx)));
        let advances = Arc::new(AtomicUsize::new(0));
        let coordinator = RepositoryWorkCoordinator::new({
            let advances = Arc::clone(&advances);
            let shutdown = Arc::clone(&shutdown);
            move |_repository: RepositoryLocator| {
                let advances = Arc::clone(&advances);
                let shutdown = Arc::clone(&shutdown);
                async move {
                    if advances.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                        shutdown.lock().unwrap().take().unwrap().send(()).unwrap();
                    }
                    Ok(())
                }
            }
        });
        let reconciler = DiscoveryReconciler {
            discovery: &discovery,
            allowed_accounts: &allowed_accounts,
            coordinator: &coordinator,
        };

        // No webhook is sent. The startup and periodic GitHub listings must
        // both reach the repository worker.
        run_until(
            1,
            allowed_accounts.len(),
            Duration::from_millis(10),
            &reconciler,
            &Notify::new(),
            async move {
                shutdown_rx
                    .await
                    .map(|()| "second repository advance")
                    .map_err(anyhow::Error::from)
            },
        )
        .await
        .unwrap();

        assert_eq!(advances.load(Ordering::SeqCst), 2);
        assert_eq!(*discovery.repository_requests.lock().unwrap(), vec![10, 10]);
    }
}
