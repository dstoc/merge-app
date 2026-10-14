//! In-memory scheduling and serialization for repository queue work.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use tracing::error;

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
    /// The presence of a key means a worker is scheduled or running. Its value
    /// is true when the worker has work to perform.
    repositories: Mutex<HashMap<RepositoryKey, bool>>,
    advance: F,
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
                repositories: Mutex::new(HashMap::new()),
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
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let runtime = tokio::runtime::Handle::try_current()
            .context("repository work must be scheduled from a Tokio runtime")?;
        let key = RepositoryKey::from(&repository);

        let start_worker = {
            let mut repositories = self
                .inner
                .repositories
                .lock()
                .expect("repository coordinator lock poisoned");
            match repositories.get_mut(&key) {
                Some(dirty) => {
                    *dirty = true;
                    false
                }
                None => {
                    repositories.insert(key.clone(), true);
                    true
                }
            }
        };

        if start_worker {
            runtime.spawn(run_worker(Arc::clone(&self.inner), key, repository));
        }
        Ok(())
    }
}

async fn run_worker<F, Fut>(
    inner: Arc<CoordinatorInner<F>>,
    key: RepositoryKey,
    repository: RepositoryLocator,
) where
    F: Fn(RepositoryLocator) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    loop {
        let has_work = {
            let mut repositories = inner
                .repositories
                .lock()
                .expect("repository coordinator lock poisoned");
            match repositories.get_mut(&key) {
                Some(dirty) if *dirty => {
                    *dirty = false;
                    true
                }
                Some(_) => {
                    repositories.remove(&key);
                    false
                }
                None => false,
            }
        };

        if !has_work {
            break;
        }

        if let Err(work_error) = (inner.advance)(repository.clone()).await {
            error!(
                event = "repository_advance_failed",
                installation_id = repository.installation_id,
                repository = %format!("{}/{}", repository.owner, repository.name),
                error = %work_error,
            );
        }
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
}
