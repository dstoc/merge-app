//! Authenticated, bounded ingress for GitHub App webhook deliveries.

use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, de::Error as _};
use serde_json::Value;
use sha2::Sha256;
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::github::RepositoryLocator;

pub const MAX_WEBHOOK_BODY_BYTES: usize = 1_000_000;
const MAX_DELIVERY_ID_BYTES: usize = 128;
const MAX_REMEMBERED_DELIVERIES: usize = 10_000;

type HmacSha256 = Hmac<Sha256>;
type ScheduleRepository = dyn Fn(RepositoryLocator) -> anyhow::Result<()> + Send + Sync;
type RemovalFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
type RemoveInstallation = dyn Fn(u64) -> RemovalFuture + Send + Sync;
type ApplyLifecycleChange = dyn Fn(LifecycleChange) + Send + Sync;

#[derive(Clone, Debug)]
pub enum LifecycleChange {
    InstallationRemoved(u64),
    InstallationRestored(u64),
    RepositoryRemoved(RepositoryLocator),
    RepositoryRestored(RepositoryLocator),
}

pub struct WebhookState {
    secret: Vec<u8>,
    allowed_accounts: HashSet<u64>,
    schedule_repository: Box<ScheduleRepository>,
    remove_installation: Box<RemoveInstallation>,
    apply_lifecycle_change: Box<ApplyLifecycleChange>,
    reconcile: Arc<Notify>,
    deliveries: Mutex<DeliveryCache>,
}

impl WebhookState {
    pub fn new<F, R, Fut, A>(
        secret: &[u8],
        allowed_accounts: &[u64],
        schedule_repository: F,
        reconcile: Arc<Notify>,
        remove_installation: R,
        apply_lifecycle_change: A,
    ) -> Self
    where
        F: Fn(RepositoryLocator) -> anyhow::Result<()> + Send + Sync + 'static,
        R: Fn(u64) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
        A: Fn(LifecycleChange) + Send + Sync + 'static,
    {
        Self {
            secret: secret.to_vec(),
            allowed_accounts: allowed_accounts.iter().copied().collect(),
            schedule_repository: Box::new(schedule_repository),
            remove_installation: Box::new(move |installation_id| {
                Box::pin(remove_installation(installation_id))
            }),
            apply_lifecycle_change: Box::new(apply_lifecycle_change),
            reconcile,
            deliveries: Mutex::new(DeliveryCache::default()),
        }
    }
}

#[derive(Default)]
struct DeliveryCache {
    ids: HashSet<String>,
    order: VecDeque<String>,
}

impl DeliveryCache {
    fn contains(&self, id: &str) -> bool {
        self.ids.contains(id)
    }

    fn insert(&mut self, id: String) {
        if self.order.len() == MAX_REMEMBERED_DELIVERIES {
            if let Some(oldest) = self.order.pop_front() {
                self.ids.remove(&oldest);
            }
        }
        self.ids.insert(id.clone());
        self.order.push_back(id);
    }

    fn remove(&mut self, id: &str) {
        self.ids.remove(id);
        self.order.retain(|remembered| remembered != id);
    }
}

pub fn router(state: Arc<WebhookState>) -> Router {
    Router::new()
        .route("/github/webhook", post(receive_webhook))
        .layer(DefaultBodyLimit::max(MAX_WEBHOOK_BODY_BYTES))
        .with_state(state)
}

async fn receive_webhook(
    State(state): State<Arc<WebhookState>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let delivery_id = headers
        .get("x-github-delivery")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let event_type = headers
        .get("x-github-event")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    let Some(delivery_id) = delivery_id
        .as_deref()
        .filter(|id| valid_header_token(id))
        .map(str::to_owned)
    else {
        warn!(
            event = "github_webhook_rejected",
            delivery_id = ?delivery_id,
            event_type = ?event_type,
            reason = "missing_or_invalid_delivery_id",
        );
        return StatusCode::BAD_REQUEST;
    };
    let Some(event_type) = event_type
        .as_deref()
        .filter(|event| valid_header_token(event))
        .map(str::to_owned)
    else {
        warn!(
            event = "github_webhook_rejected",
            delivery_id = %delivery_id,
            event_type = ?event_type,
            reason = "missing_or_invalid_event_type",
        );
        return StatusCode::BAD_REQUEST;
    };

    let Some(signature) = headers.get("x-hub-signature-256") else {
        warn!(
            event = "github_webhook_rejected",
            delivery_id = %delivery_id,
            event_type = %event_type,
            reason = "signature_missing",
        );
        return StatusCode::UNAUTHORIZED;
    };
    if !verify_signature(&state.secret, signature.as_bytes(), &body) {
        warn!(
            event = "github_webhook_rejected",
            delivery_id = %delivery_id,
            event_type = %event_type,
            reason = "signature_invalid",
        );
        return StatusCode::UNAUTHORIZED;
    }

    let payload = match serde_json::from_slice::<Value>(&body) {
        Ok(payload) => payload,
        Err(_) => {
            warn!(
                event = "github_webhook_rejected",
                delivery_id = %delivery_id,
                event_type = %event_type,
                reason = "payload_malformed",
            );
            return StatusCode::BAD_REQUEST;
        }
    };
    let action = match decode_action(&event_type, payload, &state.allowed_accounts) {
        Ok(action) => action,
        Err(_) => {
            warn!(
                event = "github_webhook_rejected",
                delivery_id = %delivery_id,
                event_type = %event_type,
                reason = "payload_invalid_for_event",
            );
            return StatusCode::BAD_REQUEST;
        }
    };

    // Claim the delivery before processing. Failures remove the claim below
    // so GitHub can retry them.
    let duplicate = {
        let mut deliveries = state
            .deliveries
            .lock()
            .expect("webhook delivery cache lock poisoned");
        if deliveries.contains(&delivery_id) {
            true
        } else {
            deliveries.insert(delivery_id.clone());
            false
        }
    };
    if duplicate {
        info!(
            event = "github_webhook_received",
            delivery_id = %delivery_id,
            event_type = %event_type,
            duplicate = true,
        );
        return StatusCode::ACCEPTED;
    }

    let result = match action {
        EventAction::Repositories(repositories) => {
            let mut schedule_result = Ok(());
            for repository in repositories {
                if let Err(error) = (state.schedule_repository)(repository) {
                    warn!(
                        event = "github_webhook_schedule_failed",
                        delivery_id = %delivery_id,
                        event_type = %event_type,
                        error = %error,
                    );
                    schedule_result = Err(error);
                    break;
                }
            }
            schedule_result
        }
        EventAction::RemoveInstallation {
            installation_id,
            account_id,
        } => {
            (state.apply_lifecycle_change)(LifecycleChange::InstallationRemoved(installation_id));
            match (state.remove_installation)(installation_id).await {
                Ok(()) => {
                    info!(
                        event = "installation_uninstalled",
                        delivery_id = %delivery_id,
                        installation_id,
                        account_id,
                        reason = "account_not_allowlisted",
                    );
                    Ok(())
                }
                Err(error) => {
                    warn!(
                        event = "installation_removal_failed",
                        delivery_id = %delivery_id,
                        installation_id,
                        account_id,
                        retry = "webhook_or_reconciliation",
                        error = %error,
                    );
                    Err(error)
                }
            }
        }
        EventAction::Lifecycle { changes, reconcile } => {
            for change in changes {
                (state.apply_lifecycle_change)(change);
            }
            if reconcile {
                state.reconcile.notify_one();
            }
            Ok(())
        }
        EventAction::Ignore => Ok(()),
    };

    if result.is_err() {
        state
            .deliveries
            .lock()
            .expect("webhook delivery cache lock poisoned")
            .remove(&delivery_id);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    info!(
        event = "github_webhook_received",
        delivery_id = %delivery_id,
        event_type = %event_type,
        duplicate = false,
    );
    StatusCode::ACCEPTED
}

fn valid_header_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_DELIVERY_ID_BYTES
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn verify_signature(secret: &[u8], signature: &[u8], body: &[u8]) -> bool {
    let Some(hex_digest) = signature.strip_prefix(b"sha256=") else {
        return false;
    };
    if hex_digest.len() != 64 {
        return false;
    }

    let mut digest = [0_u8; 32];
    for (index, pair) in hex_digest.chunks_exact(2).enumerate() {
        let Some(high) = hex_value(pair[0]) else {
            return false;
        };
        let Some(low) = hex_value(pair[1]) else {
            return false;
        };
        digest[index] = (high << 4) | low;
    }

    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&digest).is_ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

enum EventAction {
    Repositories(Vec<RepositoryLocator>),
    RemoveInstallation {
        installation_id: u64,
        account_id: u64,
    },
    Lifecycle {
        changes: Vec<LifecycleChange>,
        reconcile: bool,
    },
    Ignore,
}

#[derive(Deserialize)]
struct InstallationEvent {
    action: String,
    installation: InstallationPayload,
}

#[derive(Deserialize)]
struct RepositoryEvent {
    installation: RepositoryInstallationPayload,
    repository: RepositoryPayload,
}

#[derive(Deserialize)]
struct InstallationRepositoriesEvent {
    action: String,
    installation: InstallationPayload,
    #[serde(default)]
    repositories_added: Vec<InstallationRepositoryPayload>,
    #[serde(default)]
    repositories_removed: Vec<InstallationRepositoryPayload>,
}

#[derive(Deserialize)]
struct InstallationRepositoryPayload {
    name: String,
    full_name: String,
}

#[derive(Deserialize)]
struct InstallationPayload {
    id: u64,
    account: InstallationAccountPayload,
}

#[derive(Deserialize)]
struct RepositoryInstallationPayload {
    id: u64,
}

#[derive(Deserialize)]
struct InstallationAccountPayload {
    id: u64,
}

#[derive(Deserialize)]
struct RepositoryPayload {
    name: String,
    owner: RepositoryOwnerPayload,
}

#[derive(Deserialize)]
struct RepositoryOwnerPayload {
    id: u64,
    login: String,
}

fn decode_action(
    event_type: &str,
    payload: Value,
    allowed_accounts: &HashSet<u64>,
) -> serde_json::Result<EventAction> {
    match event_type {
        "pull_request" | "pull_request_review" => {
            let action = payload
                .get("action")
                .and_then(Value::as_str)
                .ok_or_else(|| serde_json::Error::custom("event action is missing or invalid"))?;
            let relevant = match event_type {
                "pull_request" => matches!(
                    action,
                    "opened"
                        | "reopened"
                        | "closed"
                        | "synchronize"
                        | "converted_to_draft"
                        | "ready_for_review"
                        | "auto_merge_enabled"
                        | "auto_merge_disabled"
                ),
                "pull_request_review" => matches!(action, "submitted" | "dismissed"),
                _ => false,
            };
            if !relevant {
                return Ok(EventAction::Ignore);
            }

            let event: RepositoryEvent = serde_json::from_value(payload)?;
            if event.installation.id == 0 || event.repository.owner.id == 0 {
                return Err(serde_json::Error::custom(
                    "installation and repository owner IDs must be positive",
                ));
            }
            if allowed_accounts.contains(&event.repository.owner.id) {
                Ok(EventAction::Repositories(vec![repository_locator(
                    event.installation.id,
                    event.repository,
                )?]))
            } else {
                Ok(EventAction::Ignore)
            }
        }
        "installation_repositories" => {
            let event: InstallationRepositoriesEvent = serde_json::from_value(payload)?;
            if event.installation.id == 0 || event.installation.account.id == 0 {
                return Err(serde_json::Error::custom(
                    "installation and account IDs must be positive",
                ));
            }
            if !allowed_accounts.contains(&event.installation.account.id) {
                return Ok(EventAction::Ignore);
            }
            let mut changes = Vec::new();
            for repository in event.repositories_removed {
                changes.push(LifecycleChange::RepositoryRemoved(
                    repository_locator_from_full_name(event.installation.id, repository)?,
                ));
            }
            for repository in event.repositories_added {
                changes.push(LifecycleChange::RepositoryRestored(
                    repository_locator_from_full_name(event.installation.id, repository)?,
                ));
            }
            match event.action.as_str() {
                "added" | "removed" => Ok(EventAction::Lifecycle {
                    changes,
                    reconcile: true,
                }),
                _ => Ok(EventAction::Ignore),
            }
        }
        "installation" => {
            let event: InstallationEvent = serde_json::from_value(payload)?;
            if event.installation.id == 0 || event.installation.account.id == 0 {
                return Err(serde_json::Error::custom(
                    "installation and account IDs must be positive",
                ));
            }
            let installation_id = event.installation.id;
            let account_id = event.installation.account.id;
            let allowed = allowed_accounts.contains(&account_id);
            match event.action.as_str() {
                "created" if allowed => Ok(EventAction::Lifecycle {
                    changes: vec![LifecycleChange::InstallationRestored(installation_id)],
                    reconcile: true,
                }),
                "created" => Ok(EventAction::RemoveInstallation {
                    installation_id,
                    account_id,
                }),
                "deleted" | "suspended" => Ok(EventAction::Lifecycle {
                    changes: vec![LifecycleChange::InstallationRemoved(installation_id)],
                    reconcile: true,
                }),
                "unsuspended" if allowed => Ok(EventAction::Lifecycle {
                    changes: vec![LifecycleChange::InstallationRestored(installation_id)],
                    reconcile: true,
                }),
                _ => Ok(EventAction::Ignore),
            }
        }
        _ => Ok(EventAction::Ignore),
    }
}

fn repository_locator(
    installation_id: u64,
    repository: RepositoryPayload,
) -> serde_json::Result<RepositoryLocator> {
    RepositoryLocator::new(installation_id, repository.owner.login, repository.name)
        .map_err(serde_json::Error::custom)
}

fn repository_locator_from_full_name(
    installation_id: u64,
    repository: InstallationRepositoryPayload,
) -> serde_json::Result<RepositoryLocator> {
    let (owner, name) = repository
        .full_name
        .split_once('/')
        .ok_or_else(|| serde_json::Error::custom("repository full_name must contain owner/name"))?;
    if name != repository.name || owner.is_empty() || name.is_empty() || name.contains('/') {
        return Err(serde_json::Error::custom(
            "repository full_name does not match its name",
        ));
    }
    RepositoryLocator::new(installation_id, owner, name).map_err(serde_json::Error::custom)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use axum::{body::Body, http::Request};
    use hmac::{KeyInit, Mac};
    use tokio::sync::{Notify, Semaphore, mpsc};
    use tower::ServiceExt;

    use crate::{coordinator::RepositoryWorkCoordinator, github::RepositoryLocator};

    use super::*;

    const TEST_SECRET: &[u8] = b"webhook test secret";
    const REPOSITORY_PAYLOAD: &str = r#"{"action":"opened","installation":{"id":17,"node_id":"I_kgDOExample"},"repository":{"name":"merge-app","owner":{"id":1,"login":"dstoc"}}}"#;

    fn test_state() -> (
        Arc<WebhookState>,
        mpsc::UnboundedReceiver<RepositoryLocator>,
    ) {
        let (scheduled_tx, scheduled_rx) = mpsc::unbounded_channel();
        let state = Arc::new(WebhookState::new(
            TEST_SECRET,
            &[1],
            move |repository| scheduled_tx.send(repository).map_err(anyhow::Error::msg),
            Arc::new(Notify::new()),
            |_| async { Ok(()) },
            |_| {},
        ));
        (state, scheduled_rx)
    }

    fn signed_request(event: &str, delivery: &str, body: &[u8]) -> Request<Body> {
        let mut mac = HmacSha256::new_from_slice(TEST_SECRET).unwrap();
        mac.update(body);
        let digest = mac.finalize().into_bytes();
        let signature = digest
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

    #[tokio::test]
    async fn valid_signature_schedules_repository_and_duplicate_delivery_is_ignored() {
        let (state, mut scheduled_rx) = test_state();
        let app = router(state);

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(signed_request(
                    "pull_request",
                    "delivery-1",
                    REPOSITORY_PAYLOAD.as_bytes(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED);
        }

        let repository = scheduled_rx.try_recv().unwrap();
        assert_eq!(repository.installation_id, 17);
        assert_eq!(repository.owner, "dstoc");
        assert_eq!(repository.name, "merge-app");
        assert!(scheduled_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn relevant_pull_request_actions_schedule_the_repository() {
        let (state, mut scheduled_rx) = test_state();
        let app = router(state);
        let actions = [
            "opened",
            "auto_merge_enabled",
            "auto_merge_disabled",
            "synchronize",
            "converted_to_draft",
            "ready_for_review",
            "reopened",
            "closed",
        ];

        for (index, action) in actions.iter().enumerate() {
            let body = REPOSITORY_PAYLOAD.replace("\"opened\"", &format!("\"{action}\""));
            let response = app
                .clone()
                .oneshot(signed_request(
                    "pull_request",
                    &format!("delivery-pull-{index}"),
                    body.as_bytes(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED, "action {action}");
        }

        for _ in actions {
            let repository = scheduled_rx.try_recv().unwrap();
            assert_eq!(repository.installation_id, 17);
            assert_eq!(repository.owner, "dstoc");
            assert_eq!(repository.name, "merge-app");
        }
        assert!(scheduled_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn submitted_and_dismissed_reviews_schedule_the_repository() {
        let (state, mut scheduled_rx) = test_state();
        let app = router(state);

        for (index, action) in ["submitted", "dismissed"].into_iter().enumerate() {
            let body = REPOSITORY_PAYLOAD.replace("\"opened\"", &format!("\"{action}\""));
            let response = app
                .clone()
                .oneshot(signed_request(
                    "pull_request_review",
                    &format!("delivery-review-{index}"),
                    body.as_bytes(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED, "action {action}");
            let repository = scheduled_rx.try_recv().unwrap();
            assert_eq!(repository.installation_id, 17);
            assert_eq!(repository.owner, "dstoc");
            assert_eq!(repository.name, "merge-app");
        }
        assert!(scheduled_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn unrelated_pull_request_and_review_actions_do_not_schedule_work() {
        let (state, mut scheduled_rx) = test_state();
        let app = router(state);

        for (index, (event, action)) in [
            ("pull_request", "assigned"),
            ("pull_request", "labeled"),
            ("pull_request", "edited"),
            ("pull_request_review", "edited"),
        ]
        .into_iter()
        .enumerate()
        {
            let body = REPOSITORY_PAYLOAD.replace("\"opened\"", &format!("\"{action}\""));
            let response = app
                .clone()
                .oneshot(signed_request(
                    event,
                    &format!("delivery-irrelevant-{index}"),
                    body.as_bytes(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED);
        }

        assert!(scheduled_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn webhook_burst_coalesces_through_the_repository_coordinator() {
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let first_call_gate = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let coordinator = RepositoryWorkCoordinator::new({
            let calls = Arc::clone(&calls);
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            let first_call_gate = Arc::clone(&first_call_gate);
            move |_repository: RepositoryLocator| {
                let calls = Arc::clone(&calls);
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let first_call_gate = Arc::clone(&first_call_gate);
                let started_tx = started_tx.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    started_tx.send(call).unwrap();
                    if call == 1 {
                        first_call_gate.acquire().await.unwrap().forget();
                    }
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        });
        let state = Arc::new(WebhookState::new(
            TEST_SECRET,
            &[1],
            move |repository| coordinator.mark_dirty(repository),
            Arc::new(Notify::new()),
        ));
        let app = router(state);

        let response = app
            .clone()
            .oneshot(signed_request(
                "pull_request",
                "delivery-burst-0",
                REPOSITORY_PAYLOAD.as_bytes(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .unwrap(),
            Some(1)
        );

        let body = REPOSITORY_PAYLOAD.replace("\"opened\"", "\"synchronize\"");
        for index in 1..=64 {
            let response = app
                .clone()
                .oneshot(signed_request(
                    "pull_request",
                    &format!("delivery-burst-{index}"),
                    body.as_bytes(),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED);
        }

        first_call_gate.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .unwrap(),
            Some(2)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), started_rx.recv())
                .await
                .is_err(),
            "one event burst should schedule only one follow-up advance"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn installation_repositories_event_with_abbreviated_repositories_reconciles() {
        let reconcile = Arc::new(Notify::new());
        let state = Arc::new(WebhookState::new(
            TEST_SECRET,
            &[1],
            |_| Ok(()),
            Arc::clone(&reconcile),
            |_| async { Ok(()) },
            |_| {},
        ));
        let app = router(state);
        let body = br#"{"action":"removed","installation":{"id":17,"account":{"id":1}},"repositories_removed":[{"id":99,"name":"merge-app","full_name":"dstoc/merge-app"}]}"#;
        let request = signed_request("installation_repositories", "delivery-repositories", body);

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        tokio::time::timeout(std::time::Duration::from_millis(10), reconcile.notified())
            .await
            .expect("installation repositories event should wake reconciliation");
    }

    #[tokio::test]
    async fn invalid_or_missing_signature_does_not_schedule_work() {
        let (state, mut scheduled_rx) = test_state();
        let app = router(state);
        let mut invalid = signed_request(
            "pull_request",
            "delivery-invalid",
            REPOSITORY_PAYLOAD.as_bytes(),
        );
        invalid.headers_mut().insert(
            "x-hub-signature-256",
            "sha256=0000000000000000000000000000000000000000000000000000000000000000"
                .parse()
                .unwrap(),
        );
        let response = app.clone().oneshot(invalid).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let mut missing = signed_request(
            "pull_request",
            "delivery-missing",
            REPOSITORY_PAYLOAD.as_bytes(),
        );
        missing.headers_mut().remove("x-hub-signature-256");
        let response = app.oneshot(missing).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(scheduled_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn oversized_request_is_rejected_before_scheduling() {
        let (state, mut scheduled_rx) = test_state();
        let app = router(state);
        let body = vec![b'x'; MAX_WEBHOOK_BODY_BYTES + 1];
        let request = signed_request("pull_request", "delivery-large", &body);

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(scheduled_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn unknown_event_is_accepted_without_scheduling() {
        let (state, mut scheduled_rx) = test_state();
        let app = router(state);
        let request = signed_request("workflow_job", "delivery-unknown", b"{}");

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(scheduled_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn repository_events_for_non_allowlisted_installations_are_ignored() {
        let (state, mut scheduled_rx) = test_state();
        let app = router(state);
        let body = REPOSITORY_PAYLOAD.replace("\"id\":1", "\"id\":2");
        let request = signed_request("pull_request", "delivery-unlisted", body.as_bytes());

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(scheduled_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn allowed_installation_event_requests_immediate_reconciliation() {
        let reconcile = Arc::new(Notify::new());
        let state = Arc::new(WebhookState::new(
            TEST_SECRET,
            &[1],
            |_| Ok(()),
            Arc::clone(&reconcile),
            |_| async { Ok(()) },
            |_| {},
        ));
        let app = router(state);
        let body = br#"{"action":"created","installation":{"id":17,"account":{"id":1}}}"#;
        let request = signed_request("installation", "delivery-installation", body);

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        tokio::time::timeout(std::time::Duration::from_millis(10), reconcile.notified())
            .await
            .expect("installation event should wake reconciliation");
    }

    #[tokio::test]
    async fn unallowlisted_installation_creation_is_removed() {
        let (removed_tx, mut removed_rx) = mpsc::unbounded_channel();
        let (changes_tx, mut changes_rx) = mpsc::unbounded_channel();
        let state = Arc::new(WebhookState::new(
            TEST_SECRET,
            &[1],
            |_| Ok(()),
            Arc::new(Notify::new()),
            move |installation_id| {
                let removed_tx = removed_tx.clone();
                async move { removed_tx.send(installation_id).map_err(anyhow::Error::msg) }
            },
            move |change| changes_tx.send(change).unwrap(),
        ));
        let app = router(state);
        let body = br#"{"action":"created","installation":{"id":17,"account":{"id":2}}}"#;

        let response = app
            .oneshot(signed_request("installation", "delivery-unlisted", body))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(removed_rx.try_recv().unwrap(), 17);
        assert!(matches!(
            changes_rx.try_recv().unwrap(),
            LifecycleChange::InstallationRemoved(17)
        ));
    }

    #[tokio::test]
    async fn failed_installation_removal_returns_500_and_keeps_delivery_retryable() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let removal_calls = Arc::clone(&calls);
        let state = Arc::new(WebhookState::new(
            TEST_SECRET,
            &[1],
            |_| Ok(()),
            Arc::new(Notify::new()),
            move |_| {
                let call = removal_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call == 0 {
                        anyhow::bail!("simulated GitHub API failure");
                    }
                    Ok(())
                }
            },
            |_| {},
        ));
        let app = router(state);
        let body = br#"{"action":"created","installation":{"id":17,"account":{"id":2}}}"#;

        let first = app
            .clone()
            .oneshot(signed_request("installation", "delivery-retry", body))
            .await
            .unwrap();
        let retry = app
            .oneshot(signed_request("installation", "delivery-retry", body))
            .await
            .unwrap();

        assert_eq!(first.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(retry.status(), StatusCode::ACCEPTED);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn installation_removal_blocks_work_and_reconciles() {
        let reconcile = Arc::new(Notify::new());
        let (changes_tx, mut changes_rx) = mpsc::unbounded_channel();
        let state = Arc::new(WebhookState::new(
            TEST_SECRET,
            &[1],
            |_| Ok(()),
            Arc::clone(&reconcile),
            |_| async { Ok(()) },
            move |change| changes_tx.send(change).unwrap(),
        ));
        let app = router(state);
        let body = br#"{"action":"deleted","installation":{"id":17,"account":{"id":1}}}"#;

        let response = app
            .oneshot(signed_request("installation", "delivery-deleted", body))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(matches!(
            changes_rx.try_recv().unwrap(),
            LifecycleChange::InstallationRemoved(17)
        ));
        tokio::time::timeout(std::time::Duration::from_millis(10), reconcile.notified())
            .await
            .expect("installation deletion should wake reconciliation");
    }

    #[tokio::test]
    async fn repository_access_changes_remove_and_restore_repository_scheduling() {
        let reconcile = Arc::new(Notify::new());
        let (changes_tx, mut changes_rx) = mpsc::unbounded_channel();
        let state = Arc::new(WebhookState::new(
            TEST_SECRET,
            &[1],
            |_| Ok(()),
            Arc::clone(&reconcile),
            |_| async { Ok(()) },
            move |change| changes_tx.send(change).unwrap(),
        ));
        let app = router(state);
        let removed = br#"{"action":"removed","installation":{"id":17,"account":{"id":1}},"repositories_removed":[{"id":99,"name":"old","full_name":"dstoc/old"}]}"#;
        let response = app
            .clone()
            .oneshot(signed_request(
                "installation_repositories",
                "delivery-repo-removed",
                removed,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(matches!(
            changes_rx.try_recv().unwrap(),
            LifecycleChange::RepositoryRemoved(repository)
                if repository.installation_id == 17
                    && repository.owner == "dstoc"
                    && repository.name == "old"
        ));
        tokio::time::timeout(std::time::Duration::from_millis(10), reconcile.notified())
            .await
            .expect("repository removal should wake reconciliation");

        let added = br#"{"action":"added","installation":{"id":17,"account":{"id":1}},"repositories_added":[{"id":100,"name":"new","full_name":"dstoc/new"}]}"#;
        let response = app
            .oneshot(signed_request(
                "installation_repositories",
                "delivery-repo-added",
                added,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(matches!(
            changes_rx.try_recv().unwrap(),
            LifecycleChange::RepositoryRestored(repository)
                if repository.installation_id == 17
                    && repository.owner == "dstoc"
                    && repository.name == "new"
        ));
        tokio::time::timeout(std::time::Duration::from_millis(10), reconcile.notified())
            .await
            .expect("repository addition should wake reconciliation");
    }

    #[tokio::test]
    async fn malformed_signed_payload_is_rejected() {
        let (state, mut scheduled_rx) = test_state();
        let app = router(state);
        let request = signed_request("pull_request", "delivery-malformed", b"{");

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(scheduled_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn missing_action_or_repository_identity_is_rejected_without_scheduling() {
        let (state, mut scheduled_rx) = test_state();
        let app = router(state);
        let missing_action = br#"{"installation":{"id":17},"repository":{"name":"merge-app","owner":{"id":1,"login":"dstoc"}}}"#;
        let missing_repository_identity = br#"{"action":"opened","installation":{"id":17},"repository":{"name":"merge-app","owner":{"login":"dstoc"}}}"#;

        for (index, body) in [
            missing_action.as_slice(),
            missing_repository_identity.as_slice(),
        ]
        .into_iter()
        .enumerate()
        {
            let response = app
                .clone()
                .oneshot(signed_request(
                    "pull_request",
                    &format!("delivery-invalid-shape-{index}"),
                    body,
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        assert!(scheduled_rx.try_recv().is_err());
    }

    #[test]
    fn delivery_cache_evicts_old_ids_at_its_fixed_limit() {
        let mut cache = DeliveryCache::default();
        for id in 0..MAX_REMEMBERED_DELIVERIES {
            cache.insert(format!("delivery-{id}"));
        }
        assert_eq!(cache.order.len(), MAX_REMEMBERED_DELIVERIES);

        cache.insert("new-delivery".to_owned());

        assert_eq!(cache.order.len(), MAX_REMEMBERED_DELIVERIES);
        assert!(!cache.contains("delivery-0"));
        assert!(cache.contains("new-delivery"));
    }
}
