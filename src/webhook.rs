//! Authenticated, bounded ingress for GitHub App webhook deliveries.

use std::{
    collections::{HashSet, VecDeque},
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

pub struct WebhookState {
    secret: Vec<u8>,
    allowed_accounts: HashSet<u64>,
    schedule_repository: Box<ScheduleRepository>,
    reconcile: Arc<Notify>,
    deliveries: Mutex<DeliveryCache>,
}

impl WebhookState {
    pub fn new<F>(
        secret: &[u8],
        allowed_accounts: &[u64],
        schedule_repository: F,
        reconcile: Arc<Notify>,
    ) -> Self
    where
        F: Fn(RepositoryLocator) -> anyhow::Result<()> + Send + Sync + 'static,
    {
        Self {
            secret: secret.to_vec(),
            allowed_accounts: allowed_accounts.iter().copied().collect(),
            schedule_repository: Box::new(schedule_repository),
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

    // Keep the short critical section around scheduling and recording the ID.
    // A failed schedule must remain retryable, and concurrent duplicate
    // deliveries must not both pass the deduplication check.
    let mut deliveries = state
        .deliveries
        .lock()
        .expect("webhook delivery cache lock poisoned");
    if deliveries.contains(&delivery_id) {
        info!(
            event = "github_webhook_received",
            delivery_id = %delivery_id,
            event_type = %event_type,
            duplicate = true,
        );
        return StatusCode::ACCEPTED;
    }

    match action {
        EventAction::Repositories(repositories) => {
            for repository in repositories {
                if let Err(error) = (state.schedule_repository)(repository) {
                    warn!(
                        event = "github_webhook_schedule_failed",
                        delivery_id = %delivery_id,
                        event_type = %event_type,
                        error = %error,
                    );
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
        }
        EventAction::Reconcile => state.reconcile.notify_one(),
        EventAction::Ignore => {}
    }
    deliveries.insert(delivery_id.clone());
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
    Reconcile,
    Ignore,
}

#[derive(Deserialize)]
struct InstallationEvent {
    installation: InstallationPayload,
}

#[derive(Deserialize)]
struct RepositoryEvent {
    installation: InstallationPayload,
    repository: RepositoryPayload,
}

#[derive(Deserialize)]
struct InstallationRepositoriesEvent {
    installation: InstallationPayload,
    #[serde(default)]
    repositories_added: Vec<RepositoryPayload>,
}

#[derive(Deserialize)]
struct InstallationPayload {
    id: u64,
    account: InstallationAccountPayload,
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
    login: String,
}

fn decode_action(
    event_type: &str,
    payload: Value,
    allowed_accounts: &HashSet<u64>,
) -> serde_json::Result<EventAction> {
    match event_type {
        "pull_request" | "pull_request_review" => {
            let event: RepositoryEvent = serde_json::from_value(payload)?;
            if allowed_accounts.contains(&event.installation.account.id) {
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
            if event.installation.id == 0 {
                return Err(serde_json::Error::custom(
                    "installation ID must be positive",
                ));
            }
            if !allowed_accounts.contains(&event.installation.account.id) {
                return Ok(EventAction::Ignore);
            }
            let mut repositories = Vec::with_capacity(event.repositories_added.len());
            for repository in event.repositories_added {
                repositories.push(repository_locator(event.installation.id, repository)?);
            }
            Ok(EventAction::Repositories(repositories))
        }
        "installation" => {
            let event: InstallationEvent = serde_json::from_value(payload)?;
            if event.installation.id == 0 {
                return Err(serde_json::Error::custom(
                    "installation ID must be positive",
                ));
            }
            if allowed_accounts.contains(&event.installation.account.id) {
                Ok(EventAction::Reconcile)
            } else {
                Ok(EventAction::Ignore)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{body::Body, http::Request};
    use hmac::{KeyInit, Mac};
    use tokio::sync::{Notify, mpsc};
    use tower::ServiceExt;

    use super::*;

    const TEST_SECRET: &[u8] = b"webhook test secret";
    const REPOSITORY_PAYLOAD: &str = r#"{"installation":{"id":17,"account":{"id":1}},"repository":{"name":"merge-app","owner":{"login":"dstoc"}}}"#;

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
        ));
        let app = router(state);
        let body = br#"{"installation":{"id":17,"account":{"id":1}}}"#;
        let request = signed_request("installation", "delivery-installation", body);

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        tokio::time::timeout(std::time::Duration::from_millis(10), reconcile.notified())
            .await
            .expect("installation event should wake reconciliation");
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
