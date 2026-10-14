//! Queue decisions and orchestration for advancing one repository's PR queue.

use anyhow::{Context, Result};
use reqwest::Client;
use std::future::Future;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    auth::InstallationCredentials,
    github::{
        BaseRelationship, GitHubQueries, MergeStateStatus, Mergeability, PullRequestId,
        PullRequestState, PullRequestStatus, RepositoryLocator, RepositoryState, ReviewDecision,
    },
    pull_request::{RebaseOutcome, rebase_pull_request},
};

/// The read interface used to obtain the current repository queue snapshot.
pub trait QueueQueries {
    fn repository_state<'a>(
        &'a self,
        repository: &'a RepositoryLocator,
        credentials: &'a InstallationCredentials,
    ) -> impl Future<Output = Result<RepositoryState>> + Send + 'a;
}

impl QueueQueries for GitHubQueries {
    fn repository_state<'a>(
        &'a self,
        repository: &'a RepositoryLocator,
        credentials: &'a InstallationCredentials,
    ) -> impl Future<Output = Result<RepositoryState>> + Send + 'a {
        async move { GitHubQueries::repository_state(self, repository, credentials).await }
    }
}

/// The mutation interface used to rebase the selected PR.
pub trait QueueMutations {
    fn rebase<'a>(
        &'a self,
        credentials: &'a InstallationCredentials,
        pull_request_id: &'a PullRequestId,
        expected_head_oid: &'a str,
    ) -> impl Future<Output = Result<RebaseOutcome>> + Send + 'a;
}

/// GitHub's implementation of the queue mutation interface.
#[derive(Clone)]
pub struct GitHubQueueMutations {
    client: Client,
}

impl GitHubQueueMutations {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl QueueMutations for GitHubQueueMutations {
    fn rebase<'a>(
        &'a self,
        credentials: &'a InstallationCredentials,
        pull_request_id: &'a PullRequestId,
        expected_head_oid: &'a str,
    ) -> impl Future<Output = Result<RebaseOutcome>> + Send + 'a {
        async move {
            rebase_pull_request(
                &self.client,
                &credentials.token,
                pull_request_id.as_str(),
                expected_head_oid,
            )
            .await
            .map_err(anyhow::Error::from)
        }
    }
}

/// Why the oldest unresolved candidate prevents the queue from advancing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitingReason {
    MergeabilityUnknown,
    BaseRelationshipUnknown,
}

/// The action selected from one fresh repository snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdvanceDecision {
    NoActionablePullRequests,
    Waiting {
        pull_request_number: u64,
        reason: WaitingReason,
    },
    AlreadyCurrent {
        pull_request_number: u64,
    },
    Rebase {
        pull_request_number: u64,
        pull_request_id: PullRequestId,
        expected_head_oid: String,
    },
}

/// The result of advancing a repository queue once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdvanceOutcome {
    NoActionablePullRequests,
    Waiting {
        pull_request_number: u64,
        reason: WaitingReason,
    },
    AlreadyCurrent {
        pull_request_number: u64,
    },
    RebaseAttempted {
        pull_request_number: u64,
        outcome: RebaseOutcome,
    },
}

/// Select the one queue action to take from a repository snapshot.
///
/// Eligible pull requests are considered oldest first. Conflicting pull
/// requests are skipped. Unknown mergeability or base relationship stops the
/// queue so a younger pull request cannot overtake an unresolved candidate.
pub fn decide_advance(state: &RepositoryState) -> Result<AdvanceDecision> {
    let mut candidates = state
        .pull_requests
        .iter()
        .filter(|pull_request| is_candidate(pull_request))
        .map(|pull_request| {
            let created_at = OffsetDateTime::parse(&pull_request.created_at, &Rfc3339)
                .with_context(|| {
                    format!(
                        "pull request #{} has an invalid creation timestamp",
                        pull_request.number
                    )
                })?;
            Ok((created_at, pull_request))
        })
        .collect::<Result<Vec<_>>>()?;
    candidates.sort_by_key(|(created_at, _)| *created_at);

    for (_, pull_request) in candidates {
        if pull_request.mergeability == Mergeability::Conflicting
            || pull_request.merge_state_status == MergeStateStatus::Conflicting
        {
            continue;
        }

        if pull_request.mergeability == Mergeability::Unknown {
            return Ok(AdvanceDecision::Waiting {
                pull_request_number: pull_request.number,
                reason: WaitingReason::MergeabilityUnknown,
            });
        }

        return Ok(match pull_request.base_relationship() {
            BaseRelationship::Behind => AdvanceDecision::Rebase {
                pull_request_number: pull_request.number,
                pull_request_id: pull_request.id.clone(),
                expected_head_oid: pull_request.head_ref_oid.clone(),
            },
            BaseRelationship::Current => AdvanceDecision::AlreadyCurrent {
                pull_request_number: pull_request.number,
            },
            BaseRelationship::Unresolved => AdvanceDecision::Waiting {
                pull_request_number: pull_request.number,
                reason: WaitingReason::BaseRelationshipUnknown,
            },
        });
    }

    Ok(AdvanceDecision::NoActionablePullRequests)
}

/// Query current repository state and perform at most one queue mutation.
///
/// This function never merges pull requests, polls for CI, or creates reviews.
pub async fn advance<Q, M>(
    repository: &RepositoryLocator,
    credentials: &InstallationCredentials,
    queries: &Q,
    mutations: &M,
) -> Result<AdvanceOutcome>
where
    Q: QueueQueries,
    M: QueueMutations,
{
    let state = queries
        .repository_state(repository, credentials)
        .await
        .context("failed to query repository queue state")?;

    match decide_advance(&state)? {
        AdvanceDecision::NoActionablePullRequests => Ok(AdvanceOutcome::NoActionablePullRequests),
        AdvanceDecision::Waiting {
            pull_request_number,
            reason,
        } => Ok(AdvanceOutcome::Waiting {
            pull_request_number,
            reason,
        }),
        AdvanceDecision::AlreadyCurrent {
            pull_request_number,
        } => Ok(AdvanceOutcome::AlreadyCurrent {
            pull_request_number,
        }),
        AdvanceDecision::Rebase {
            pull_request_number,
            pull_request_id,
            expected_head_oid,
        } => {
            let outcome = mutations
                .rebase(credentials, &pull_request_id, &expected_head_oid)
                .await
                .with_context(|| format!("failed to rebase PR #{pull_request_number}"))?;
            Ok(AdvanceOutcome::RebaseAttempted {
                pull_request_number,
                outcome,
            })
        }
    }
}

fn is_candidate(pull_request: &PullRequestState) -> bool {
    pull_request.state == PullRequestStatus::Open
        && !pull_request.is_draft
        && pull_request.review_decision == ReviewDecision::Approved
        && pull_request.auto_merge_request.is_some()
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex};

    use super::*;
    use crate::github::{AutoMergeRequest, MergeMethod, RepositoryIdentity};

    fn pull_request(number: u64, created_at: &str) -> PullRequestState {
        PullRequestState {
            id: PullRequestId(format!("PR_node_{number}")),
            number,
            created_at: created_at.to_owned(),
            state: PullRequestStatus::Open,
            is_draft: false,
            review_decision: ReviewDecision::Approved,
            auto_merge_request: Some(AutoMergeRequest {
                enabled_at: "2026-10-14T00:00:00Z".to_owned(),
                merge_method: MergeMethod::Squash,
            }),
            mergeability: Mergeability::Mergeable,
            merge_state_status: MergeStateStatus::Behind,
            head_ref_oid: format!("head_{number}"),
            base_ref_oid: format!("base_{number}"),
        }
    }

    fn repository_state(pull_requests: Vec<PullRequestState>) -> RepositoryState {
        RepositoryState {
            repository: RepositoryIdentity {
                installation_id: 1,
                node_id: "repo_node".to_owned(),
                name_with_owner: "owner/repo".to_owned(),
            },
            pull_requests,
        }
    }

    fn assert_rebase(decision: AdvanceDecision, expected_number: u64) {
        assert!(matches!(
            decision,
            AdvanceDecision::Rebase {
                pull_request_number,
                ..
            } if pull_request_number == expected_number
        ));
    }

    #[test]
    fn excludes_ineligible_pull_requests_and_orders_candidates_by_creation_time() {
        let mut draft = pull_request(1, "2026-10-14T00:00:00Z");
        draft.is_draft = true;
        let mut unapproved = pull_request(2, "2026-10-14T00:01:00Z");
        unapproved.review_decision = ReviewDecision::ReviewRequired;
        let mut auto_merge_disabled = pull_request(3, "2026-10-14T00:02:00Z");
        auto_merge_disabled.auto_merge_request = None;
        let mut closed = pull_request(4, "2026-10-14T00:03:00Z");
        closed.state = PullRequestStatus::Closed;
        let newer = pull_request(6, "2026-10-14T00:06:00Z");
        let older = pull_request(5, "2026-10-14T00:05:00Z");

        assert_rebase(
            decide_advance(&repository_state(vec![
                newer,
                draft,
                unapproved,
                auto_merge_disabled,
                closed,
                older,
            ]))
            .unwrap(),
            5,
        );
    }

    #[test]
    fn skips_conflicting_candidates_and_rebases_the_next_usable_candidate() {
        let mut conflicting = pull_request(1, "2026-10-14T00:00:00Z");
        conflicting.mergeability = Mergeability::Conflicting;
        let mut status_conflicting = pull_request(2, "2026-10-14T00:01:00Z");
        status_conflicting.merge_state_status = MergeStateStatus::Conflicting;
        let next = pull_request(3, "2026-10-14T00:02:00Z");

        assert_rebase(
            decide_advance(&repository_state(vec![
                conflicting,
                status_conflicting,
                next,
            ]))
            .unwrap(),
            3,
        );
    }

    #[test]
    fn unknown_mergeability_stops_before_a_younger_candidate() {
        let mut unresolved = pull_request(1, "2026-10-14T00:00:00Z");
        unresolved.mergeability = Mergeability::Unknown;
        let younger = pull_request(2, "2026-10-14T00:01:00Z");

        assert_eq!(
            decide_advance(&repository_state(vec![unresolved, younger])).unwrap(),
            AdvanceDecision::Waiting {
                pull_request_number: 1,
                reason: WaitingReason::MergeabilityUnknown,
            }
        );
    }

    #[test]
    fn current_candidate_stops_before_a_younger_candidate() {
        let mut current = pull_request(1, "2026-10-14T00:00:00Z");
        current.merge_state_status = MergeStateStatus::Clean;
        let younger = pull_request(2, "2026-10-14T00:01:00Z");

        assert_eq!(
            decide_advance(&repository_state(vec![current, younger])).unwrap(),
            AdvanceDecision::AlreadyCurrent {
                pull_request_number: 1,
            }
        );
    }

    #[test]
    fn all_known_current_merge_states_stop_the_queue() {
        for status in [
            MergeStateStatus::Clean,
            MergeStateStatus::HasHooks,
            MergeStateStatus::Unstable,
        ] {
            let mut current = pull_request(1, "2026-10-14T00:00:00Z");
            current.merge_state_status = status;

            assert_eq!(
                decide_advance(&repository_state(vec![current])).unwrap(),
                AdvanceDecision::AlreadyCurrent {
                    pull_request_number: 1,
                }
            );
        }
    }

    #[test]
    fn unresolved_base_relationship_stops_before_a_younger_candidate() {
        let mut unresolved = pull_request(1, "2026-10-14T00:00:00Z");
        unresolved.merge_state_status = MergeStateStatus::Blocked;
        let younger = pull_request(2, "2026-10-14T00:01:00Z");

        assert_eq!(
            decide_advance(&repository_state(vec![unresolved, younger])).unwrap(),
            AdvanceDecision::Waiting {
                pull_request_number: 1,
                reason: WaitingReason::BaseRelationshipUnknown,
            }
        );
    }

    #[test]
    fn returns_no_actionable_pull_requests_when_only_ineligible_prs_exist() {
        let mut draft = pull_request(1, "2026-10-14T00:00:00Z");
        draft.is_draft = true;

        assert_eq!(
            decide_advance(&repository_state(vec![draft])).unwrap(),
            AdvanceDecision::NoActionablePullRequests
        );
    }

    struct MockQueries(RepositoryState);

    impl QueueQueries for MockQueries {
        fn repository_state<'a>(
            &'a self,
            _repository: &'a RepositoryLocator,
            _credentials: &'a InstallationCredentials,
        ) -> impl Future<Output = Result<RepositoryState>> + Send + 'a {
            async move { Ok(self.0.clone()) }
        }
    }

    struct MockMutations {
        calls: Mutex<Vec<(String, String)>>,
        outcome: RebaseOutcome,
    }

    impl QueueMutations for MockMutations {
        fn rebase<'a>(
            &'a self,
            _credentials: &'a InstallationCredentials,
            pull_request_id: &'a PullRequestId,
            expected_head_oid: &'a str,
        ) -> impl Future<Output = Result<RebaseOutcome>> + Send + 'a {
            async move {
                self.calls.lock().unwrap().push((
                    pull_request_id.as_str().to_owned(),
                    expected_head_oid.to_owned(),
                ));
                Ok(self.outcome.clone())
            }
        }
    }

    #[tokio::test]
    async fn advance_makes_exactly_one_rebase_attempt_with_the_observed_head() {
        let queries = MockQueries(repository_state(vec![pull_request(
            1,
            "2026-10-14T00:00:00Z",
        )]));
        let mutations = MockMutations {
            calls: Mutex::new(Vec::new()),
            outcome: RebaseOutcome::StaleHead {
                message: "head changed".to_owned(),
            },
        };
        let repository = RepositoryLocator::new(1, "owner", "repo").unwrap();
        let credentials = InstallationCredentials {
            installation_id: 1,
            token: "test-token".to_owned(),
            expires_at: "2026-10-14T01:00:00Z".to_owned(),
            permissions: BTreeMap::new(),
        };

        let outcome = advance(&repository, &credentials, &queries, &mutations)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            AdvanceOutcome::RebaseAttempted {
                pull_request_number: 1,
                outcome: RebaseOutcome::StaleHead {
                    message: "head changed".to_owned(),
                },
            }
        );
        assert_eq!(
            *mutations.calls.lock().unwrap(),
            vec![("PR_node_1".to_owned(), "head_1".to_owned())]
        );
    }
}
