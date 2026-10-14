use std::{
    future::Future,
    io::{self, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use hmac::{Hmac, KeyInit, Mac};
use merge_app::{
    auth::InstallationCredentials,
    coordinator::RepositoryWorkCoordinator,
    github::{
        AutoMergeRequest, MergeMethod, MergeStateStatus, Mergeability, PullRequestId,
        PullRequestState, PullRequestStatus, RepositoryIdentity, RepositoryLocator,
        RepositoryState, ReviewDecision,
    },
    pull_request::RebaseOutcome,
    queue::{self, AdvanceOutcome, QueueMutations, QueueQueries},
    webhook::{self, LifecycleChange, WebhookState},
};
use sha2::Sha256;
use tokio::sync::{Notify, Semaphore, mpsc};
use tower::ServiceExt;
use tracing_subscriber::fmt::MakeWriter;

const SECRET: &[u8] = b"phase 2 integration webhook secret";
const PULL_REQUEST_PAYLOAD: &str = r#"{"action":"opened","installation":{"id":17},"repository":{"name":"merge-app","owner":{"id":1,"login":"dstoc"}},"pull_request":{"number":999,"state":"closed","draft":true}}"#;
const PUSH_PAYLOAD: &str = r#"{"installation":{"id":17},"repository":{"name":"merge-app","owner":{"id":1,"login":"dstoc"}},"ref":"refs/heads/main","commits":[]}"#;
const TEST_REPOSITORY: &str = "dstoc/merge-app";

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
struct MockGitHubQueries {
    state: RepositoryState,
    calls: Arc<Mutex<Vec<QueryCall>>>,
    next_call: Arc<AtomicUsize>,
    first_query_gate: Arc<Semaphore>,
    first_query_started: mpsc::UnboundedSender<()>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueryCall {
    installation_id: u64,
    owner: String,
    repository: String,
    token: String,
}

impl QueueQueries for MockGitHubQueries {
    fn repository_state<'a>(
        &'a self,
        repository: &'a RepositoryLocator,
        credentials: &'a InstallationCredentials,
    ) -> impl Future<Output = anyhow::Result<RepositoryState>> + Send + 'a {
        async move {
            let call_number = self.next_call.fetch_add(1, Ordering::SeqCst) + 1;
            self.calls.lock().unwrap().push(QueryCall {
                installation_id: repository.installation_id,
                owner: repository.owner.clone(),
                repository: repository.name.clone(),
                token: credentials.token.clone(),
            });
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);

            if call_number == 1 {
                let _ = self.first_query_started.send(());
                self.first_query_gate
                    .acquire()
                    .await
                    .expect("the first query gate remains open")
                    .forget();
                self.active.fetch_sub(1, Ordering::SeqCst);
                anyhow::bail!("mock GitHub GraphQL query failed");
            }

            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(self.state.clone())
        }
    }
}

#[derive(Clone, Default)]
struct MockGitHubMutations {
    calls: Arc<Mutex<Vec<MutationCall>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MutationCall {
    installation_id: u64,
    token: String,
    pull_request_id: String,
    expected_head_oid: String,
}

impl QueueMutations for MockGitHubMutations {
    fn rebase<'a>(
        &'a self,
        credentials: &'a InstallationCredentials,
        pull_request_id: &'a PullRequestId,
        expected_head_oid: &'a str,
    ) -> impl Future<Output = anyhow::Result<RebaseOutcome>> + Send + 'a {
        async move {
            self.calls.lock().unwrap().push(MutationCall {
                installation_id: credentials.installation_id,
                token: credentials.token.clone(),
                pull_request_id: pull_request_id.as_str().to_owned(),
                expected_head_oid: expected_head_oid.to_owned(),
            });
            Ok(RebaseOutcome::Rebased)
        }
    }
}

#[derive(Clone)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for LogBuffer {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(Arc::clone(&self.0))
    }
}

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn verified_webhooks_drive_serialized_queue_work_and_lifecycle_guards() {
    let log_bytes = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_writer(LogBuffer(Arc::clone(&log_bytes)))
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("this integration test owns its tracing subscriber");

    let repository = RepositoryLocator::new(17, "dstoc", "merge-app").unwrap();
    let (query_started_tx, mut query_started_rx) = mpsc::unbounded_channel();
    let queries = MockGitHubQueries {
        state: queue_state(),
        calls: Arc::default(),
        next_call: Arc::default(),
        first_query_gate: Arc::new(Semaphore::new(0)),
        first_query_started: query_started_tx,
        active: Arc::default(),
        max_active: Arc::default(),
    };
    let mutations = MockGitHubMutations::default();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let coordinator = RepositoryWorkCoordinator::new({
        let queries = queries.clone();
        let mutations = mutations.clone();
        move |repository: RepositoryLocator| {
            let queries = queries.clone();
            let mutations = mutations.clone();
            let completed_tx = completed_tx.clone();
            async move {
                let credentials = InstallationCredentials {
                    installation_id: repository.installation_id,
                    token: format!("installation-token-{}", repository.installation_id),
                    expires_at: "2030-01-01T00:00:00Z".to_owned(),
                    permissions: Default::default(),
                };
                let outcome =
                    queue::advance(&repository, &credentials, &queries, &mutations).await?;
                completed_tx
                    .send((repository, outcome))
                    .map_err(anyhow::Error::msg)?;
                Ok(())
            }
        }
    });

    // Model the repository as known, then begin with its installation blocked.
    let epoch = coordinator.scheduling_epoch();
    assert!(coordinator.apply_authoritative_repository_listing(17, &[repository.clone()], epoch));
    coordinator.block_installation(17);

    let reconcile = Arc::new(Notify::new());
    let (removed_tx, mut removed_rx) = mpsc::unbounded_channel();
    let state = Arc::new(WebhookState::new(
        SECRET,
        &[1],
        {
            let coordinator = coordinator.clone();
            move |repository| coordinator.mark_dirty(repository)
        },
        Arc::clone(&reconcile),
        move |installation_id| {
            let removed_tx = removed_tx.clone();
            async move { removed_tx.send(installation_id).map_err(anyhow::Error::msg) }
        },
        {
            let coordinator = coordinator.clone();
            move |change| match change {
                LifecycleChange::InstallationRemoved(id) => coordinator.block_installation(id),
                LifecycleChange::InstallationRestored(id) => coordinator.unblock_installation(id),
                LifecycleChange::RepositoryRemoved(repository) => {
                    coordinator.block_repository(&repository)
                }
                LifecycleChange::RepositoryRestored(repository) => {
                    coordinator.unblock_repository(&repository)
                }
                LifecycleChange::RepositoryAccessChanged(id) => {
                    coordinator.require_repository_refresh(id)
                }
            }
        },
    ));
    let app = webhook::router(state);

    // An allowlisted installation is retained and unblocks its repository.
    let authorized_installation =
        br#"{"action":"created","installation":{"id":17,"account":{"id":1}}}"#;
    assert_eq!(
        app.clone()
            .oneshot(signed_request(
                "installation",
                "installation-authorized",
                authorized_installation,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    tokio::time::timeout(Duration::from_secs(1), reconcile.notified())
        .await
        .expect("authorized installation should request reconciliation");
    assert_eq!(coordinator.scheduling_epoch(), epoch + 2);

    // An unallowlisted installation is blocked and removed through the API seam.
    let unauthorized_installation =
        br#"{"action":"created","installation":{"id":88,"account":{"id":2}}}"#;
    assert_eq!(
        app.clone()
            .oneshot(signed_request(
                "installation",
                "installation-unauthorized",
                unauthorized_installation,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(removed_rx.try_recv().unwrap(), 88);
    assert!(
        !coordinator
            .mark_dirty_at_epoch(
                RepositoryLocator::new(88, "other", "repo").unwrap(),
                coordinator.scheduling_epoch(),
            )
            .unwrap()
    );
    assert_eq!(
        app.clone()
            .oneshot(signed_request(
                "push",
                "blocked-installation-push",
                PUSH_PAYLOAD.replace("\"id\":17", "\"id\":88").as_bytes(),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    assert!(queries.calls.lock().unwrap().is_empty());

    // Repository removal blocks scheduling until reconciliation confirms access.
    let removed_repository = br#"{"action":"removed","installation":{"id":17,"account":{"id":1}},"repositories_removed":[{"name":"merge-app","full_name":"dstoc/merge-app"}]}"#;
    assert_eq!(
        app.clone()
            .oneshot(signed_request(
                "installation_repositories",
                "repository-removed",
                removed_repository,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    assert!(
        !coordinator
            .mark_dirty_at_epoch(repository.clone(), coordinator.scheduling_epoch())
            .unwrap()
    );
    assert_eq!(
        app.clone()
            .oneshot(signed_request(
                "push",
                "removed-repository-push",
                PUSH_PAYLOAD.as_bytes(),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    assert!(queries.calls.lock().unwrap().is_empty());
    let epoch = coordinator.scheduling_epoch();
    assert!(coordinator.apply_authoritative_repository_listing(17, &[], epoch));
    assert!(
        !coordinator
            .mark_dirty_at_epoch(repository.clone(), coordinator.scheduling_epoch())
            .unwrap()
    );

    // A later access grant and authoritative listing restore the repository.
    let added_repository = br#"{"action":"added","installation":{"id":17,"account":{"id":1}},"repositories_added":[{"name":"merge-app","full_name":"dstoc/merge-app"}]}"#;
    assert_eq!(
        app.clone()
            .oneshot(signed_request(
                "installation_repositories",
                "repository-added",
                added_repository,
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    let epoch = coordinator.scheduling_epoch();
    assert!(coordinator.apply_authoritative_repository_listing(17, &[repository.clone()], epoch));

    // The signed PR event reaches the queue worker. Duplicate deliveries and
    // event bursts set one dirty bit while the first GitHub query is in flight.
    assert_eq!(
        app.clone()
            .oneshot(signed_request(
                "pull_request",
                "delivery-first",
                PULL_REQUEST_PAYLOAD.as_bytes(),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    tokio::time::timeout(Duration::from_secs(1), query_started_rx.recv())
        .await
        .unwrap()
        .expect("worker should query current GitHub state");

    assert_eq!(
        app.clone()
            .oneshot(signed_request(
                "pull_request",
                "delivery-first",
                PULL_REQUEST_PAYLOAD.as_bytes(),
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::ACCEPTED
    );
    for index in 0..64 {
        let (event, body) = match index % 3 {
            0 => ("push", PUSH_PAYLOAD.to_owned()),
            1 => (
                "pull_request",
                PULL_REQUEST_PAYLOAD.replace("\"opened\"", "\"assigned\""),
            ),
            _ => (
                "pull_request_review",
                PULL_REQUEST_PAYLOAD.replace("\"opened\"", "\"edited\""),
            ),
        };
        assert_eq!(
            app.clone()
                .oneshot(signed_request(
                    event,
                    &format!("delivery-burst-{index}"),
                    body.as_bytes(),
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::ACCEPTED
        );
    }

    let mut invalid_signature = signed_request(
        "pull_request",
        "delivery-invalid-signature",
        PULL_REQUEST_PAYLOAD.as_bytes(),
    );
    invalid_signature.headers_mut().insert(
        "x-hub-signature-256",
        "sha256=0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap(),
    );
    assert_eq!(
        app.clone()
            .oneshot(invalid_signature)
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(signed_request(
                "pull_request",
                "delivery-oversized",
                &vec![b'x'; webhook::MAX_WEBHOOK_BODY_BYTES + 1],
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );

    queries.first_query_gate.add_permits(1);
    let (completed_repository, outcome) =
        tokio::time::timeout(Duration::from_secs(2), completed_rx.recv())
            .await
            .unwrap()
            .expect("coalesced retry should complete queue advancement");
    assert_eq!(completed_repository, repository);
    assert!(matches!(
        outcome,
        AdvanceOutcome::RebaseAttempted {
            pull_request_number: 42,
            outcome: RebaseOutcome::Rebased,
        }
    ));

    assert_eq!(
        *queries.calls.lock().unwrap(),
        vec![
            QueryCall {
                installation_id: 17,
                owner: "dstoc".to_owned(),
                repository: "merge-app".to_owned(),
                token: "installation-token-17".to_owned(),
            },
            QueryCall {
                installation_id: 17,
                owner: "dstoc".to_owned(),
                repository: "merge-app".to_owned(),
                token: "installation-token-17".to_owned(),
            },
        ]
    );
    assert_eq!(queries.max_active.load(Ordering::SeqCst), 1);
    assert_eq!(
        *mutations.calls.lock().unwrap(),
        vec![MutationCall {
            installation_id: 17,
            token: "installation-token-17".to_owned(),
            pull_request_id: "PR_node_42".to_owned(),
            expected_head_oid: "head-42".to_owned(),
        }]
    );

    let logs = String::from_utf8(log_bytes.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("\"event\":\"repository_advance_failed\""));
    assert!(logs.contains("mock GitHub GraphQL query failed"));
    assert!(logs.contains("\"reason\":\"signature_invalid\""));
    assert!(logs.contains(TEST_REPOSITORY));
}

fn queue_state() -> RepositoryState {
    RepositoryState {
        repository: RepositoryIdentity {
            installation_id: 17,
            node_id: "R_repository".to_owned(),
            name_with_owner: TEST_REPOSITORY.to_owned(),
        },
        pull_requests: vec![PullRequestState {
            id: PullRequestId("PR_node_42".to_owned()),
            number: 42,
            created_at: "2026-10-14T12:00:00Z".to_owned(),
            state: PullRequestStatus::Open,
            is_draft: false,
            review_decision: ReviewDecision::Approved,
            auto_merge_request: Some(AutoMergeRequest {
                enabled_at: "2026-10-14T12:01:00Z".to_owned(),
                merge_method: MergeMethod::Rebase,
            }),
            mergeability: Mergeability::Mergeable,
            merge_state_status: MergeStateStatus::Behind,
            head_ref_oid: "head-42".to_owned(),
            base_ref_oid: "base-42".to_owned(),
        }],
    }
}

fn signed_request(event: &str, delivery: &str, body: &[u8]) -> Request<Body> {
    let mut mac = HmacSha256::new_from_slice(SECRET).unwrap();
    mac.update(body);
    let signature = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    Request::builder()
        .method("POST")
        .uri("/github/webhook")
        .header("x-hub-signature-256", format!("sha256={signature}"))
        .header("x-github-delivery", delivery)
        .header("x-github-event", event)
        .body(Body::from(body.to_vec()))
        .unwrap()
}
