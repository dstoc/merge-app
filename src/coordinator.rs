//! In-memory scheduling and serialization for repository queue work.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::time::Instant;
use tracing::{error, info};

use crate::github::RepositoryLocator;

/// Schedules at most one worker for each repository and coalesces new work
/// into a dirty bit while that worker is running.
///
/// Each worker runs `advance` once, then repeats while a notification marked
/// its repository dirty. Work for different repositories runs in separate
/// Tokio tasks and can proceed concurrently.
pub struct RepositoryWorkCoordinator<F> {
    inner: Arc<CoordinatorInner<F>>,
}

struct CoordinatorInner<F> {
    state: Mutex<CoordinatorState>,
    advance: F,
}

#[derive(Default)]
struct CoordinatorState {
    /// The presence of a key means a worker is scheduled or running. Its value
    /// is true when the worker has work to perform.
    repositories: HashMap<RepositoryKey, bool>,
    blocked_installations: HashSet<u64>,
    installations_requiring_repository_refresh: HashSet<u64>,
    blocked_repositories: HashSet<RepositoryKey>,
    known_repositories: HashMap<u64, HashSet<RepositoryKey>>,
    pending_retries: HashMap<RepositoryKey, PendingRetry>,
    retry_backoff: HashMap<RepositoryKey, RetryBackoff>,
    next_retry_generation: u64,
    scheduling_epoch: u64,
}

impl CoordinatorState {
    fn is_blocked(&self, key: &RepositoryKey) -> bool {
        self.blocked_installations.contains(&key.installation_id)
            || self
                .installations_requiring_repository_refresh
                .contains(&key.installation_id)
            || self.blocked_repositories.contains(key)
    }

    fn mark_dirty(&mut self, key: &RepositoryKey) -> bool {
        let start_worker = match self.repositories.get_mut(key) {
            Some(dirty) => {
                *dirty = true;
                false
            }
            None => {
                self.repositories.insert(key.clone(), true);
                true
            }
        };
        self.known_repositories
            .entry(key.installation_id)
            .or_default()
            .insert(key.clone());
        start_worker
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingRetry {
    generation: u64,
    deadline: Instant,
    reason: RetryReason,
    stage: Option<u8>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RetryBackoff {
    queue_state_stage: u8,
    api_failure_stage: u8,
}

/// The reason to recompute a repository queue after a delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryReason {
    MergeabilityUnknown,
    BaseRelationshipUnknown,
    StaleHead,
    GitHubApiFailure,
}

/// The result of one serialized repository worker operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerDirective {
    Complete,
    Retry(RetryReason),
}

/// Convert a worker result to the coordinator's scheduling directive.
pub trait IntoWorkerDirective {
    fn into_worker_directive(self) -> Result<WorkerDirective>;
}

impl IntoWorkerDirective for Result<()> {
    fn into_worker_directive(self) -> Result<WorkerDirective> {
        self.map(|()| WorkerDirective::Complete)
    }
}

impl IntoWorkerDirective for Result<WorkerDirective> {
    fn into_worker_directive(self) -> Result<WorkerDirective> {
        self
    }
}

/// GitHub repository names are case-insensitive. Normalize them here so that
/// differently cased webhook and reconciliation payloads share one worker.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RepositoryKey {
    installation_id: u64,
    owner: String,
    name: String,
}

impl From<&RepositoryLocator> for RepositoryKey {
    fn from(repository: &RepositoryLocator) -> Self {
        Self {
            installation_id: repository.installation_id,
            owner: repository.owner.to_ascii_lowercase(),
            name: repository.name.to_ascii_lowercase(),
        }
    }
}

impl<F> Clone for RepositoryWorkCoordinator<F> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<F> RepositoryWorkCoordinator<F> {
    /// Create a coordinator with the asynchronous operation to run for each
    /// dirty repository.
    pub fn new(advance: F) -> Self {
        Self {
            inner: Arc::new(CoordinatorInner {
                state: Mutex::new(CoordinatorState::default()),
                advance,
            }),
        }
    }

    /// Mark a repository dirty and start a worker if one is not already
    /// scheduled or running for it.
    ///
    /// Calls for a repository that already has a worker only set its dirty
    /// bit. They do not create another task or queue another copy of the key.
    pub fn mark_dirty<Fut>(&self, repository: RepositoryLocator) -> Result<()>
    where
        F: Fn(RepositoryLocator) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: IntoWorkerDirective,
    {
        let epoch = self.scheduling_epoch();
        self.mark_dirty_at_epoch(repository, epoch).map(|_| ())
    }

    /// Mark a repository dirty only if no lifecycle event changed the
    /// authoritative scheduling view since the caller began reconciliation.
    pub fn mark_dirty_at_epoch<Fut>(
        &self,
        repository: RepositoryLocator,
        expected_epoch: u64,
    ) -> Result<bool>
    where
        F: Fn(RepositoryLocator) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: IntoWorkerDirective,
    {
        let runtime = tokio::runtime::Handle::try_current()
            .context("repository work must be scheduled from a Tokio runtime")?;
        let key = RepositoryKey::from(&repository);

        let start_worker = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("repository coordinator lock poisoned");
            if state.scheduling_epoch != expected_epoch || state.is_blocked(&key) {
                return Ok(false);
            }
            state.pending_retries.remove(&key);
            state.mark_dirty(&key)
        };

        if start_worker {
            runtime.spawn(run_worker(Arc::clone(&self.inner), key, repository));
        }
        Ok(true)
    }

    fn mark_retry_dirty<Fut>(
        &self,
        repository: RepositoryLocator,
        pending_retry: PendingRetry,
    ) -> Result<bool>
    where
        F: Fn(RepositoryLocator) -> Fut + Send + Sync + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: IntoWorkerDirective,
    {
        let runtime = tokio::runtime::Handle::try_current()
            .context("repository work must be scheduled from a Tokio runtime")?;
        let key = RepositoryKey::from(&repository);

        let start_worker = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("repository coordinator lock poisoned");
            if state.pending_retries.get(&key) != Some(&pending_retry) {
                return Ok(false);
            }
            state.pending_retries.remove(&key);
            if state.is_blocked(&key) {
                return Ok(false);
            }
            state.mark_dirty(&key)
        };

        if start_worker {
            runtime.spawn(run_worker(Arc::clone(&self.inner), key, repository));
        }
        Ok(true)
    }

    /// Capture the scheduling view before an authoritative GitHub read.
    pub fn scheduling_epoch(&self) -> u64 {
        self.inner
            .state
            .lock()
            .expect("repository coordinator lock poisoned")
            .scheduling_epoch
    }

    /// Stop future work for an installation and clear any coalesced work.
    pub fn block_installation(&self, installation_id: u64) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("repository coordinator lock poisoned");
        state.scheduling_epoch = state.scheduling_epoch.wrapping_add(1);
        state.blocked_installations.insert(installation_id);
        state
            .pending_retries
            .retain(|key, _| key.installation_id != installation_id);
        state
            .retry_backoff
            .retain(|key, _| key.installation_id != installation_id);
        for (key, dirty) in &mut state.repositories {
            if key.installation_id == installation_id {
                *dirty = false;
            }
        }
    }

    /// Re-enable an installation after an authorized installation event.
    pub fn unblock_installation(&self, installation_id: u64) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("repository coordinator lock poisoned");
        state.scheduling_epoch = state.scheduling_epoch.wrapping_add(1);
        state.blocked_installations.remove(&installation_id);
    }

    /// Stop future work for a repository removed from an installation.
    pub fn block_repository(&self, repository: &RepositoryLocator) {
        let key = RepositoryKey::from(repository);
        let mut state = self
            .inner
            .state
            .lock()
            .expect("repository coordinator lock poisoned");
        state.scheduling_epoch = state.scheduling_epoch.wrapping_add(1);
        state.blocked_repositories.insert(key.clone());
        state.pending_retries.remove(&key);
        state.retry_backoff.remove(&key);
        if let Some(dirty) = state.repositories.get_mut(&key) {
            *dirty = false;
        }
    }

    /// Re-enable a repository after GitHub reports that access was restored.
    pub fn unblock_repository(&self, repository: &RepositoryLocator) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("repository coordinator lock poisoned");
        state.scheduling_epoch = state.scheduling_epoch.wrapping_add(1);
        state
            .blocked_repositories
            .remove(&RepositoryKey::from(repository));
    }

    /// Pause repository work until GitHub confirms the installation's current
    /// repository access after an access-change event.
    pub fn require_repository_refresh(&self, installation_id: u64) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("repository coordinator lock poisoned");
        state.scheduling_epoch = state.scheduling_epoch.wrapping_add(1);
        state
            .installations_requiring_repository_refresh
            .insert(installation_id);
        state
            .pending_retries
            .retain(|key, _| key.installation_id != installation_id);
        state
            .retry_backoff
            .retain(|key, _| key.installation_id != installation_id);
        for (key, dirty) in &mut state.repositories {
            if key.installation_id == installation_id {
                *dirty = false;
            }
        }
    }

    /// Apply a successful authoritative repository listing if no lifecycle
    /// event changed the scheduling view while GitHub was being queried.
    /// Repositories that disappeared from the listing are blocked and any
    /// coalesced work for them is retired. Repositories in the listing become
    /// eligible again, including after missed restoration webhooks. A
    /// successful listing also restores an installation blocked by suspension.
    pub fn apply_authoritative_repository_listing(
        &self,
        installation_id: u64,
        repositories: &[RepositoryLocator],
        expected_epoch: u64,
    ) -> bool {
        let current_repositories = repositories
            .iter()
            .filter(|repository| repository.installation_id == installation_id)
            .map(RepositoryKey::from)
            .collect::<HashSet<_>>();
        let mut state = self
            .inner
            .state
            .lock()
            .expect("repository coordinator lock poisoned");
        if state.scheduling_epoch != expected_epoch {
            return false;
        }

        state.blocked_installations.remove(&installation_id);
        let previously_known = state
            .known_repositories
            .get(&installation_id)
            .cloned()
            .unwrap_or_default();
        for key in previously_known.difference(&current_repositories) {
            state.blocked_repositories.insert(key.clone());
            state.pending_retries.remove(key);
            state.retry_backoff.remove(key);
            if let Some(dirty) = state.repositories.get_mut(key) {
                *dirty = false;
            }
        }
        for key in &current_repositories {
            state.blocked_repositories.remove(key);
        }
        state
            .known_repositories
            .insert(installation_id, current_repositories);
        state
            .installations_requiring_repository_refresh
            .remove(&installation_id);
        true
    }
}

async fn run_worker<F, Fut>(
    inner: Arc<CoordinatorInner<F>>,
    key: RepositoryKey,
    repository: RepositoryLocator,
) where
    F: Fn(RepositoryLocator) -> Fut + Send + Sync + 'static,
    Fut: Future + Send + 'static,
    Fut::Output: IntoWorkerDirective,
{
    loop {
        let has_work = {
            let mut state = inner
                .state
                .lock()
                .expect("repository coordinator lock poisoned");
            let blocked = state.is_blocked(&key);
            if blocked {
                state.repositories.remove(&key);
                false
            } else {
                match state.repositories.get_mut(&key) {
                    Some(dirty) if *dirty => {
                        *dirty = false;
                        true
                    }
                    Some(_) => {
                        state.repositories.remove(&key);
                        false
                    }
                    None => false,
                }
            }
        };

        if !has_work {
            break;
        }

        match (inner.advance)(repository.clone())
            .await
            .into_worker_directive()
        {
            Ok(WorkerDirective::Complete) => {
                let mut state = inner
                    .state
                    .lock()
                    .expect("repository coordinator lock poisoned");
                state.retry_backoff.remove(&key);
            }
            Ok(WorkerDirective::Retry(reason)) => {
                if let Some(backoff) = inner
                    .state
                    .lock()
                    .expect("repository coordinator lock poisoned")
                    .retry_backoff
                    .get_mut(&key)
                {
                    backoff.api_failure_stage = 0;
                    if reason == RetryReason::StaleHead {
                        backoff.queue_state_stage = 0;
                    }
                }
                schedule_retry(Arc::clone(&inner), key.clone(), repository.clone(), reason);
            }
            Err(work_error) => {
                error!(
                    event = "repository_advance_failed",
                    installation_id = repository.installation_id,
                    repository = %format!("{}/{}", repository.owner, repository.name),
                    error = %format!("{work_error:#}"),
                );
                schedule_retry(
                    Arc::clone(&inner),
                    key.clone(),
                    repository.clone(),
                    RetryReason::GitHubApiFailure,
                );
            }
        }
    }
}

fn schedule_retry<F, Fut>(
    inner: Arc<CoordinatorInner<F>>,
    key: RepositoryKey,
    repository: RepositoryLocator,
    reason: RetryReason,
) where
    F: Fn(RepositoryLocator) -> Fut + Send + Sync + 'static,
    Fut: Future + Send + 'static,
    Fut::Output: IntoWorkerDirective,
{
    let (pending_retry, delay) = {
        let mut state = inner
            .state
            .lock()
            .expect("repository coordinator lock poisoned");
        if state.is_blocked(&key) || state.repositories.get(&key).copied().unwrap_or(false) {
            return;
        }

        if let Some(pending) = state.pending_retries.get(&key) {
            if pending.reason == reason {
                return;
            }
        }

        let current_backoff = state.retry_backoff.get(&key).copied().unwrap_or_default();
        let mut next_backoff = current_backoff;
        let (delay, stage) = match reason {
            RetryReason::MergeabilityUnknown | RetryReason::BaseRelationshipUnknown => {
                next_backoff.queue_state_stage =
                    next_backoff.queue_state_stage.saturating_add(1).min(3);
                (
                    queue_state_retry_delay(next_backoff.queue_state_stage),
                    Some(next_backoff.queue_state_stage),
                )
            }
            RetryReason::StaleHead => (Duration::from_secs(1), Some(1)),
            RetryReason::GitHubApiFailure => {
                next_backoff.api_failure_stage =
                    next_backoff.api_failure_stage.saturating_add(1).min(3);
                (
                    api_failure_retry_delay(next_backoff.api_failure_stage),
                    Some(next_backoff.api_failure_stage),
                )
            }
        };

        let deadline = Instant::now() + delay;
        if state
            .pending_retries
            .get(&key)
            .is_some_and(|pending| pending.deadline <= deadline)
        {
            return;
        }

        if next_backoff == RetryBackoff::default() {
            state.retry_backoff.remove(&key);
        } else {
            state.retry_backoff.insert(key.clone(), next_backoff);
        }
        state.next_retry_generation = state.next_retry_generation.wrapping_add(1);
        let pending_retry = PendingRetry {
            generation: state.next_retry_generation,
            deadline,
            reason,
            stage,
        };
        state.pending_retries.insert(key.clone(), pending_retry);
        (pending_retry, delay)
    };

    info!(
        event = "repository_retry_scheduled",
        installation_id = repository.installation_id,
        repository = %format!("{}/{}", repository.owner, repository.name),
        reason = ?pending_retry.reason,
        delay_ms = delay.as_millis() as u64,
        backoff_stage = pending_retry.stage,
    );

    tokio::spawn(async move {
        tokio::time::sleep_until(pending_retry.deadline).await;
        let coordinator = RepositoryWorkCoordinator {
            inner: Arc::clone(&inner),
        };
        match coordinator.mark_retry_dirty(repository, pending_retry) {
            Ok(true) | Ok(false) => {}
            Err(error) => tracing::warn!(
                event = "repository_retry_schedule_failed",
                installation_id = key.installation_id,
                repository = %format!("{}/{}", key.owner, key.name),
                error = %format!("{error:#}"),
            ),
        }
    });
}

fn queue_state_retry_delay(stage: u8) -> Duration {
    match stage {
        1 => Duration::from_secs(5),
        2 => Duration::from_secs(15),
        _ => Duration::from_secs(30),
    }
}

fn api_failure_retry_delay(stage: u8) -> Duration {
    match stage {
        1 => Duration::from_secs(30),
        2 => Duration::from_secs(2 * 60),
        _ => Duration::from_secs(5 * 60),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::{
        sync::{Semaphore, mpsc},
        time::timeout,
    };

    use super::*;

    fn repository(name: &str) -> RepositoryLocator {
        RepositoryLocator::new(1, "owner", name).unwrap()
    }

    async fn wait_for_call(receiver: &mut mpsc::UnboundedReceiver<usize>) -> usize {
        for _ in 0..100 {
            if let Ok(call) = receiver.try_recv() {
                return call;
            }
            tokio::task::yield_now().await;
        }
        panic!("repository worker did not run");
    }

    async fn wait_for_pending_retry<F>(
        coordinator: &RepositoryWorkCoordinator<F>,
        repository: &RepositoryLocator,
    ) -> PendingRetry {
        let key = RepositoryKey::from(repository);
        for _ in 0..100 {
            if let Some(pending) = coordinator
                .inner
                .state
                .lock()
                .unwrap()
                .pending_retries
                .get(&key)
                .copied()
            {
                return pending;
            }
            tokio::task::yield_now().await;
        }
        panic!("repository retry was not scheduled");
    }

    #[tokio::test]
    async fn lifecycle_blocks_retire_work_and_fence_stale_reconciliation_results() {
        let (scheduled_tx, mut scheduled_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new(move |repository: RepositoryLocator| {
            let scheduled_tx = scheduled_tx.clone();
            async move {
                scheduled_tx.send(repository.name).unwrap();
                Ok(())
            }
        });
        let removed_repository = repository("removed");
        let stale_epoch = coordinator.scheduling_epoch();

        coordinator.block_repository(&removed_repository);
        assert!(
            !coordinator
                .mark_dirty_at_epoch(removed_repository.clone(), stale_epoch)
                .unwrap()
        );
        assert!(
            !coordinator
                .mark_dirty_at_epoch(removed_repository.clone(), coordinator.scheduling_epoch())
                .unwrap()
        );
        coordinator.block_installation(1);
        assert!(
            !coordinator
                .mark_dirty_at_epoch(repository("another"), coordinator.scheduling_epoch())
                .unwrap()
        );
        assert!(scheduled_rx.try_recv().is_err());

        coordinator.unblock_installation(1);
        coordinator.unblock_repository(&removed_repository);
        assert!(
            coordinator
                .mark_dirty_at_epoch(removed_repository, coordinator.scheduling_epoch())
                .unwrap()
        );
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), scheduled_rx.recv())
                .await
                .unwrap(),
            Some("removed".to_owned())
        );
    }

    #[tokio::test]
    async fn access_refresh_fences_stale_results_and_retires_excluded_work() {
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (finished_tx, mut finished_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            let gate = Arc::clone(&gate);
            move |_repository: RepositoryLocator| {
                let calls = Arc::clone(&calls);
                let gate = Arc::clone(&gate);
                let started_tx = started_tx.clone();
                let finished_tx = finished_tx.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    started_tx.send(()).unwrap();
                    gate.acquire().await.unwrap().forget();
                    finished_tx.send(()).unwrap();
                    Ok(())
                }
            }
        });
        let excluded = repository("excluded");
        let initial_epoch = coordinator.scheduling_epoch();

        assert!(
            coordinator
                .mark_dirty_at_epoch(excluded.clone(), initial_epoch)
                .unwrap()
        );
        timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            coordinator
                .mark_dirty_at_epoch(excluded.clone(), initial_epoch)
                .unwrap()
        );

        coordinator.require_repository_refresh(1);
        assert!(!coordinator.apply_authoritative_repository_listing(
            1,
            std::slice::from_ref(&excluded),
            initial_epoch,
        ));
        assert!(
            !coordinator
                .mark_dirty_at_epoch(excluded.clone(), coordinator.scheduling_epoch())
                .unwrap()
        );

        let refresh_epoch = coordinator.scheduling_epoch();
        assert!(coordinator.apply_authoritative_repository_listing(1, &[], refresh_epoch));
        assert!(
            !coordinator
                .mark_dirty_at_epoch(excluded, refresh_epoch)
                .unwrap()
        );
        gate.add_permits(1);

        timeout(Duration::from_secs(2), finished_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn coalesces_notifications_and_serializes_work_for_one_repository() {
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();

        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            let gate = Arc::clone(&gate);
            move |_repository| {
                let calls = Arc::clone(&calls);
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let gate = Arc::clone(&gate);
                let started_tx = started_tx.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    started_tx.send(call).unwrap();
                    gate.acquire().await.unwrap().forget();
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        });

        let repository = repository("serialized");
        coordinator.mark_dirty(repository.clone()).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .unwrap(),
            Some(1)
        );

        for _ in 0..64 {
            coordinator.mark_dirty(repository.clone()).unwrap();
        }
        coordinator
            .mark_dirty(RepositoryLocator::new(1, "OWNER", "SERIALIZED").unwrap())
            .unwrap();
        assert!(
            timeout(Duration::from_millis(30), started_rx.recv())
                .await
                .is_err()
        );

        gate.add_permits(1);
        assert_eq!(
            timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        gate.add_permits(1);
        assert!(
            timeout(Duration::from_millis(50), started_rx.recv())
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn different_repositories_can_run_concurrently() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (finished_tx, mut finished_rx) = mpsc::unbounded_channel();

        let coordinator = RepositoryWorkCoordinator::new({
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            let gate = Arc::clone(&gate);
            move |repository: RepositoryLocator| {
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let gate = Arc::clone(&gate);
                let started_tx = started_tx.clone();
                let finished_tx = finished_tx.clone();
                async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    started_tx.send(repository.name.clone()).unwrap();
                    gate.acquire().await.unwrap().forget();
                    active.fetch_sub(1, Ordering::SeqCst);
                    finished_tx.send(repository.name).unwrap();
                    Ok(())
                }
            }
        });

        coordinator.mark_dirty(repository("first")).unwrap();
        coordinator.mark_dirty(repository("second")).unwrap();
        let first = timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let second = timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(max_active.load(Ordering::SeqCst), 2);

        gate.add_permits(2);
        let mut finished = [
            timeout(Duration::from_secs(2), finished_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            timeout(Duration::from_secs(2), finished_rx.recv())
                .await
                .unwrap()
                .unwrap(),
        ];
        finished.sort();
        assert_eq!(finished, ["first", "second"]);
    }

    #[tokio::test]
    async fn a_notification_at_worker_completion_is_not_lost() {
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (first_returning_tx, mut first_returning_rx) = mpsc::unbounded_channel();

        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            let gate = Arc::clone(&gate);
            move |_repository| {
                let calls = Arc::clone(&calls);
                let gate = Arc::clone(&gate);
                let started_tx = started_tx.clone();
                let first_returning_tx = first_returning_tx.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    started_tx.send(call).unwrap();
                    if call == 1 {
                        gate.acquire().await.unwrap().forget();
                        first_returning_tx.send(()).unwrap();
                    }
                    Ok(())
                }
            }
        });

        let repository = repository("completion-race");
        coordinator.mark_dirty(repository.clone()).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .unwrap(),
            Some(1)
        );
        gate.add_permits(1);
        timeout(Duration::from_secs(2), first_returning_rx.recv())
            .await
            .unwrap()
            .unwrap();

        coordinator.mark_dirty(repository).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_queue_states_retry_with_bounded_backoff() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (call_tx, mut call_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            move |_repository: RepositoryLocator| {
                let calls = Arc::clone(&calls);
                let call_tx = call_tx.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    call_tx.send(call).unwrap();
                    Ok::<_, anyhow::Error>(match call {
                        1 => WorkerDirective::Retry(RetryReason::MergeabilityUnknown),
                        2 => WorkerDirective::Retry(RetryReason::BaseRelationshipUnknown),
                        _ => WorkerDirective::Complete,
                    })
                }
            }
        });

        let repository = repository("unknown-mergeability");
        coordinator.mark_dirty(repository.clone()).unwrap();
        assert_eq!(wait_for_call(&mut call_rx).await, 1);
        let pending = wait_for_pending_retry(&coordinator, &repository).await;
        assert_eq!(pending.reason, RetryReason::MergeabilityUnknown);
        assert_eq!(pending.deadline - Instant::now(), Duration::from_secs(5));

        tokio::time::advance(Duration::from_secs(4)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(wait_for_call(&mut call_rx).await, 2);
        let pending = wait_for_pending_retry(&coordinator, &repository).await;
        assert_eq!(pending.reason, RetryReason::BaseRelationshipUnknown);
        assert_eq!(pending.deadline - Instant::now(), Duration::from_secs(15));
        tokio::time::advance(Duration::from_secs(15)).await;
        assert_eq!(wait_for_call(&mut call_rx).await, 3);
        assert!(
            coordinator
                .inner
                .state
                .lock()
                .unwrap()
                .pending_retries
                .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stale_head_recomputes_after_one_second() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (call_tx, mut call_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            move |_repository: RepositoryLocator| {
                let calls = Arc::clone(&calls);
                let call_tx = call_tx.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    call_tx.send(call).unwrap();
                    Ok::<_, anyhow::Error>(if call == 1 {
                        WorkerDirective::Retry(RetryReason::StaleHead)
                    } else {
                        WorkerDirective::Complete
                    })
                }
            }
        });

        let repository = repository("stale-head");
        coordinator.mark_dirty(repository.clone()).unwrap();
        assert_eq!(wait_for_call(&mut call_rx).await, 1);
        let pending = wait_for_pending_retry(&coordinator, &repository).await;
        assert_eq!(pending.deadline - Instant::now(), Duration::from_secs(1));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(wait_for_call(&mut call_rx).await, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_retry_requests_keep_one_deadline_and_one_worker() {
        let (call_tx, mut call_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new(move |_repository: RepositoryLocator| {
            let call_tx = call_tx.clone();
            async move {
                call_tx.send(1).unwrap();
                Ok::<_, anyhow::Error>(WorkerDirective::Complete)
            }
        });
        let repository = repository("coalesced-retry");
        let key = RepositoryKey::from(&repository);

        schedule_retry(
            Arc::clone(&coordinator.inner),
            key.clone(),
            repository.clone(),
            RetryReason::BaseRelationshipUnknown,
        );
        let first = coordinator.inner.state.lock().unwrap().pending_retries[&key];
        schedule_retry(
            Arc::clone(&coordinator.inner),
            key.clone(),
            repository.clone(),
            RetryReason::BaseRelationshipUnknown,
        );
        let second = coordinator.inner.state.lock().unwrap().pending_retries[&key];
        assert_eq!(first, second);
        assert_eq!(
            coordinator.inner.state.lock().unwrap().retry_backoff[&key].queue_state_stage,
            1
        );

        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(wait_for_call(&mut call_rx).await, 1);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(call_rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn webhook_work_replaces_pending_retry_without_concurrent_execution() {
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            let gate = Arc::clone(&gate);
            move |_repository: RepositoryLocator| {
                let calls = Arc::clone(&calls);
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let gate = Arc::clone(&gate);
                let started_tx = started_tx.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    started_tx.send(call).unwrap();
                    if call == 1 {
                        gate.acquire().await.unwrap().forget();
                    }
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, anyhow::Error>(WorkerDirective::Complete)
                }
            }
        });

        let repository = repository("webhook-replaces-retry");
        coordinator.mark_dirty(repository.clone()).unwrap();
        assert_eq!(wait_for_call(&mut started_rx).await, 1);
        let key = RepositoryKey::from(&repository);
        schedule_retry(
            Arc::clone(&coordinator.inner),
            key.clone(),
            repository.clone(),
            RetryReason::MergeabilityUnknown,
        );
        coordinator.mark_dirty(repository.clone()).unwrap();
        assert!(
            !coordinator
                .inner
                .state
                .lock()
                .unwrap()
                .pending_retries
                .contains_key(&key)
        );

        gate.add_permits(1);
        assert_eq!(wait_for_call(&mut started_rx).await, 2);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn lifecycle_blocks_retire_pending_retry_timers() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (call_tx, mut call_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            move |_repository: RepositoryLocator| {
                let calls = Arc::clone(&calls);
                let call_tx = call_tx.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    call_tx.send(call).unwrap();
                    Ok::<_, anyhow::Error>(WorkerDirective::Retry(RetryReason::MergeabilityUnknown))
                }
            }
        });

        let removed_repository = repository("removed-before-retry");
        let suspended_installation_repository =
            RepositoryLocator::new(2, "owner", "suspended-before-retry").unwrap();
        coordinator.mark_dirty(removed_repository.clone()).unwrap();
        assert_eq!(wait_for_call(&mut call_rx).await, 1);
        coordinator
            .mark_dirty(suspended_installation_repository.clone())
            .unwrap();
        assert_eq!(wait_for_call(&mut call_rx).await, 2);
        wait_for_pending_retry(&coordinator, &removed_repository).await;
        wait_for_pending_retry(&coordinator, &suspended_installation_repository).await;
        coordinator.block_repository(&removed_repository);
        coordinator.block_installation(2);

        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            coordinator
                .inner
                .state
                .lock()
                .unwrap()
                .pending_retries
                .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn api_failures_use_slow_capped_backoff() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (call_tx, mut call_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            move |_repository: RepositoryLocator| {
                let calls = Arc::clone(&calls);
                let call_tx = call_tx.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    call_tx.send(call).unwrap();
                    if call <= 4 {
                        anyhow::bail!("simulated GitHub API failure");
                    }
                    Ok(WorkerDirective::Complete)
                }
            }
        });
        let repository = repository("api-failure-backoff");
        let delays = [
            Duration::from_secs(30),
            Duration::from_secs(120),
            Duration::from_secs(300),
            Duration::from_secs(300),
        ];

        coordinator.mark_dirty(repository.clone()).unwrap();
        for (attempt, delay) in delays.into_iter().enumerate() {
            assert_eq!(wait_for_call(&mut call_rx).await, attempt + 1);
            let pending = wait_for_pending_retry(&coordinator, &repository).await;
            assert_eq!(pending.reason, RetryReason::GitHubApiFailure);
            assert_eq!(pending.deadline - Instant::now(), delay);
            tokio::time::advance(delay).await;
        }
        assert_eq!(wait_for_call(&mut call_rx).await, 5);
        assert_eq!(calls.load(Ordering::SeqCst), 5);
        assert!(
            coordinator
                .inner
                .state
                .lock()
                .unwrap()
                .pending_retries
                .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unrelated_lifecycle_changes_do_not_discard_pending_retries() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (call_tx, mut call_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            move |_repository: RepositoryLocator| {
                let calls = Arc::clone(&calls);
                let call_tx = call_tx.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    call_tx.send(call).unwrap();
                    if call == 1 {
                        Ok(WorkerDirective::Retry(RetryReason::MergeabilityUnknown))
                    } else {
                        Ok(WorkerDirective::Complete)
                    }
                }
            }
        });

        let repository_a = RepositoryLocator::new(1, "owner", "repo-a").unwrap();
        let repository_b = RepositoryLocator::new(2, "owner", "repo-b").unwrap();
        let repository_c = RepositoryLocator::new(1, "owner", "repo-c").unwrap();

        coordinator.mark_dirty(repository_a.clone()).unwrap();
        assert_eq!(wait_for_call(&mut call_rx).await, 1);
        wait_for_pending_retry(&coordinator, &repository_a).await;

        // Lifecycle changes for an unrelated repository/installation and for another
        // repository in the same installation must not discard repository A's retry.
        coordinator.block_repository(&repository_b);
        coordinator.block_installation(2);
        coordinator.block_repository(&repository_c);

        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(wait_for_call(&mut call_rx).await, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            coordinator
                .inner
                .state
                .lock()
                .unwrap()
                .pending_retries
                .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unblocking_does_not_revive_superseded_retry_timer() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (call_tx, mut call_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            move |_repository: RepositoryLocator| {
                let calls = Arc::clone(&calls);
                let call_tx = call_tx.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    call_tx.send(call).unwrap();
                    Ok(WorkerDirective::Retry(RetryReason::MergeabilityUnknown))
                }
            }
        });

        let repository = repository("unblock-revival-check");
        coordinator.mark_dirty(repository.clone()).unwrap();
        assert_eq!(wait_for_call(&mut call_rx).await, 1);
        wait_for_pending_retry(&coordinator, &repository).await;

        // Block and then unblock the repository before the timer expires.
        coordinator.block_repository(&repository);
        coordinator.unblock_repository(&repository);

        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(call_rx.try_recv().is_err());
        assert!(
            coordinator
                .inner
                .state
                .lock()
                .unwrap()
                .pending_retries
                .is_empty()
        );
    }
}
