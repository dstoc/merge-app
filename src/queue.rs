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
    pull_request::{BranchUpdateMethod, BranchUpdateOutcome, update_pull_request_branch},
};

/// The read interface used to obtain the current repository queue snapshot.
pub trait QueueQueries {
    fn pull_request_state<'a>(
        &'a self,
        repository: &'a RepositoryLocator,
        number: u64,
        credentials: &'a InstallationCredentials,
    ) -> impl Future<Output = Result<PullRequestState>> + Send + 'a;

    fn repository_state<'a>(
        &'a self,
        repository: &'a RepositoryLocator,
        credentials: &'a InstallationCredentials,
    ) -> impl Future<Output = Result<RepositoryState>> + Send + 'a;
}

impl QueueQueries for GitHubQueries {
    fn pull_request_state<'a>(
        &'a self,
        repository: &'a RepositoryLocator,
        number: u64,
        credentials: &'a InstallationCredentials,
    ) -> impl Future<Output = Result<PullRequestState>> + Send + 'a {
        async move { GitHubQueries::pull_request_state(self, repository, number, credentials).await }
    }

    fn repository_state<'a>(
        &'a self,
        repository: &'a RepositoryLocator,
        credentials: &'a InstallationCredentials,
    ) -> impl Future<Output = Result<RepositoryState>> + Send + 'a {
        async move { GitHubQueries::repository_state(self, repository, credentials).await }
    }
}

/// The mutation interface used to update the selected PR working branch.
pub trait QueueMutations {
    fn update_branch<'a>(
        &'a self,
        credentials: &'a InstallationCredentials,
        pull_request_id: &'a PullRequestId,
        expected_head_oid: &'a str,
        method: BranchUpdateMethod,
    ) -> impl Future<Output = Result<BranchUpdateOutcome>> + Send + 'a;
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
    fn update_branch<'a>(
        &'a self,
        credentials: &'a InstallationCredentials,
        pull_request_id: &'a PullRequestId,
        expected_head_oid: &'a str,
        method: BranchUpdateMethod,
    ) -> impl Future<Output = Result<BranchUpdateOutcome>> + Send + 'a {
        async move {
            update_pull_request_branch(
                &self.client,
                &credentials.token,
                pull_request_id.as_str(),
                expected_head_oid,
                method,
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
    BranchUpdateAttempted {
        pull_request_number: u64,
        method: BranchUpdateMethod,
        outcome: BranchUpdateOutcome,
    },
    /// Inputs changed or eligibility was lost; recompute without carrying a conflict forward.
    StateChanged {
        pull_request_number: u64,
    },
    /// An accepted update has not yet produced a visible head change.
    UpdatePending {
        pull_request_number: u64,
        method: BranchUpdateMethod,
    },
    /// A fresh read observed a changed head after acceptance; another actor may have pushed.
    BranchUpdated {
        pull_request_number: u64,
        method: BranchUpdateMethod,
        previous_head_oid: String,
        head_oid: String,
        base_oid: String,
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

/// Update one candidate: rebase first, then merge-update only after a confirmed conflict.
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
            let observed = state
                .pull_requests
                .iter()
                .find(|pr| pr.id == pull_request_id)
                .context("selected PR missing from queue snapshot")?;
            let mut fresh = queries
                .pull_request_state(repository, pull_request_number, credentials)
                .await
                .context("failed to recheck PR before branch update")?;
            if !same_update_inputs(observed, &fresh) || !can_update(&fresh) {
                return Ok(AdvanceOutcome::StateChanged {
                    pull_request_number,
                });
            }
            for method in [BranchUpdateMethod::Rebase, BranchUpdateMethod::Merge] {
                let outcome = mutations
                    .update_branch(credentials, &pull_request_id, &expected_head_oid, method)
                    .await
                    .with_context(|| {
                        format!("failed to update PR #{pull_request_number} using {method:?}")
                    })?;
                if matches!(outcome, BranchUpdateOutcome::StaleHead { .. }) {
                    return Ok(AdvanceOutcome::BranchUpdateAttempted {
                        pull_request_number,
                        method,
                        outcome,
                    });
                }
                let after = queries
                    .pull_request_state(repository, pull_request_number, credentials)
                    .await
                    .context("failed to recheck PR after branch update")?;
                match outcome {
                    BranchUpdateOutcome::Accepted { .. } => {
                        // GitHub may return the OLD head in an accepted mutation payload.
                        // Only a fresh read can establish visible completion. We never
                        // reuse approval/check state from the pre-update head.
                        if !same_branch_identity(&fresh, &after)
                            || after.head_ref_oid.is_empty()
                            || fresh.base_ref_oid != after.base_ref_oid
                        {
                            return Ok(AdvanceOutcome::StateChanged {
                                pull_request_number,
                            });
                        }
                        if after.head_ref_oid == expected_head_oid {
                            return Ok(AdvanceOutcome::UpdatePending {
                                pull_request_number,
                                method,
                            });
                        }
                        return Ok(AdvanceOutcome::BranchUpdated {
                            pull_request_number,
                            method,
                            previous_head_oid: expected_head_oid,
                            head_oid: after.head_ref_oid,
                            base_oid: after.base_ref_oid,
                        });
                    }
                    BranchUpdateOutcome::Conflict { .. } => {
                        tracing::info!(event = "branch_update_conflict",
                            installation_id = repository.installation_id,
                            repository = %format!("{}/{}", repository.owner, repository.name),
                            pull_request_number,
                            ?method, head_oid = %expected_head_oid, base_oid = %fresh.base_ref_oid,
                            base_ref = %fresh.base_ref_name, message = ?outcome);
                        if !same_update_inputs(&fresh, &after) {
                            return Ok(AdvanceOutcome::StateChanged {
                                pull_request_number,
                            });
                        }
                        if method == BranchUpdateMethod::Merge {
                            // Both methods conflicted. Leave the branch; no immediate retry.
                            return Ok(AdvanceOutcome::BranchUpdateAttempted {
                                pull_request_number,
                                method,
                                outcome,
                            });
                        }
                        if !can_update(&after) {
                            return Ok(AdvanceOutcome::StateChanged {
                                pull_request_number,
                            });
                        }
                        fresh = after;
                    }
                    BranchUpdateOutcome::StaleHead { .. } => unreachable!(),
                }
            }
            unreachable!("the merge attempt always returns")
        }
    }
}

// The query is scoped to the same target repository. Names detect retargets
// even when two branches point at the same commit. A deleted head repo fails closed.
fn same_branch_identity(before: &PullRequestState, after: &PullRequestState) -> bool {
    before.id == after.id
        && before.base_ref_name == after.base_ref_name
        && before.head_ref_name == after.head_ref_name
        && before
            .head_repository_id
            .as_ref()
            .is_some_and(|id| !id.is_empty())
        && before.head_repository_id == after.head_repository_id
}

fn same_update_inputs(before: &PullRequestState, after: &PullRequestState) -> bool {
    same_branch_identity(before, after)
        && before.head_ref_oid == after.head_ref_oid
        && before.base_ref_oid == after.base_ref_oid
        && before.auto_merge_request == after.auto_merge_request
}

fn can_update(pr: &PullRequestState) -> bool {
    is_candidate(pr)
        && pr.mergeability == Mergeability::Mergeable
        && pr.base_relationship() == BaseRelationship::Behind
        && !pr.head_ref_oid.is_empty()
        && !pr.base_ref_oid.is_empty()
        && !pr.head_ref_name.is_empty()
        && !pr.base_ref_name.is_empty()
}

fn is_candidate(pull_request: &PullRequestState) -> bool {
    pull_request.state == PullRequestStatus::Open
        && !pull_request.is_draft
        && pull_request.review_decision == ReviewDecision::Approved
        && pull_request.auto_merge_request.is_some()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

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
            base_ref_name: "main".to_owned(),
            head_ref_name: format!("feature-{number}"),
            head_repository_id: Some("repo_node".to_owned()),
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

    struct ScriptQueries {
        reads: Mutex<std::collections::VecDeque<Result<PullRequestState>>>,
    }

    impl QueueQueries for ScriptQueries {
        async fn repository_state(
            &self,
            _: &RepositoryLocator,
            _: &InstallationCredentials,
        ) -> Result<RepositoryState> {
            Ok(repository_state(vec![fixture()]))
        }
        async fn pull_request_state(
            &self,
            _: &RepositoryLocator,
            number: u64,
            _: &InstallationCredentials,
        ) -> Result<PullRequestState> {
            assert_eq!(number, 1);
            self.reads
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected PR read")
        }
    }

    type UpdateCall = (BranchUpdateMethod, String, String);
    struct ScriptMutations {
        outcomes: Mutex<std::collections::VecDeque<Result<BranchUpdateOutcome>>>,
        calls: Mutex<Vec<UpdateCall>>,
    }

    impl QueueMutations for ScriptMutations {
        async fn update_branch(
            &self,
            _: &InstallationCredentials,
            id: &PullRequestId,
            head: &str,
            method: BranchUpdateMethod,
        ) -> Result<BranchUpdateOutcome> {
            self.calls
                .lock()
                .unwrap()
                .push((method, id.as_str().to_owned(), head.to_owned()));
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected branch update")
        }
    }

    fn fixture() -> PullRequestState {
        pull_request(1, "2026-10-14T00:00:00Z")
    }
    fn conflict() -> Result<BranchUpdateOutcome> {
        Ok(BranchUpdateOutcome::Conflict {
            message: "confirmed content conflict".to_owned(),
        })
    }
    fn accepted() -> Result<BranchUpdateOutcome> {
        Ok(BranchUpdateOutcome::Accepted {
            head_oid: "head_1".to_owned(),
        })
    }
    fn updated() -> PullRequestState {
        let mut pr = fixture();
        pr.head_ref_oid = "updated-head".to_owned();
        pr.review_decision = ReviewDecision::ReviewRequired;
        pr.merge_state_status = MergeStateStatus::Unknown;
        pr
    }

    async fn run_script(
        reads: Vec<Result<PullRequestState>>,
        outcomes: Vec<Result<BranchUpdateOutcome>>,
    ) -> (Result<AdvanceOutcome>, Vec<UpdateCall>) {
        let queries = ScriptQueries {
            reads: Mutex::new(reads.into()),
        };
        let mutations = ScriptMutations {
            calls: Mutex::new(Vec::new()),
            outcomes: Mutex::new(outcomes.into()),
        };
        let repository = RepositoryLocator::new(1, "owner", "repo").unwrap();
        let credentials = InstallationCredentials {
            installation_id: 1,
            token: "token".to_owned(),
            expires_at: "unused".to_owned(),
            permissions: Default::default(),
        };
        let result = advance(&repository, &credentials, &queries, &mutations).await;
        assert!(queries.reads.lock().unwrap().is_empty(), "unused PR reads");
        assert!(
            mutations.outcomes.lock().unwrap().is_empty(),
            "unused mutation outcomes"
        );
        let calls = mutations.calls.into_inner().unwrap();
        (result, calls)
    }

    fn assert_methods(calls: Vec<UpdateCall>, methods: &[BranchUpdateMethod]) {
        assert_eq!(
            calls,
            methods
                .iter()
                .map(|method| (*method, "PR_node_1".to_owned(), "head_1".to_owned()))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn confirmed_rebase_conflict_then_merge_updates_only_the_selected_working_branch() {
        let (outcome, calls) = run_script(
            vec![Ok(fixture()), Ok(fixture()), Ok(updated())],
            vec![conflict(), accepted()],
        )
        .await;
        assert_eq!(
            outcome.unwrap(),
            AdvanceOutcome::BranchUpdated {
                pull_request_number: 1,
                method: BranchUpdateMethod::Merge,
                previous_head_oid: "head_1".to_owned(),
                head_oid: "updated-head".to_owned(),
                base_oid: "base_1".to_owned(),
            }
        );
        assert_methods(
            calls,
            &[BranchUpdateMethod::Rebase, BranchUpdateMethod::Merge],
        );
    }

    #[tokio::test]
    async fn accepted_rebase_never_falls_back_and_uses_a_fresh_head_read() {
        let (outcome, calls) =
            run_script(vec![Ok(fixture()), Ok(updated())], vec![accepted()]).await;
        assert!(
            matches!(outcome.unwrap(), AdvanceOutcome::BranchUpdated { method: BranchUpdateMethod::Rebase, head_oid, .. } if head_oid == "updated-head")
        );
        assert_methods(calls, &[BranchUpdateMethod::Rebase]);
    }

    #[tokio::test]
    async fn accepted_updates_with_no_visible_head_change_are_pending_for_either_method() {
        for method in [BranchUpdateMethod::Rebase, BranchUpdateMethod::Merge] {
            let mut reads = vec![Ok(fixture()), Ok(fixture())];
            let mut outcomes = vec![accepted()];
            let methods = if method == BranchUpdateMethod::Merge {
                reads.push(Ok(fixture()));
                outcomes.insert(0, conflict());
                vec![BranchUpdateMethod::Rebase, BranchUpdateMethod::Merge]
            } else {
                vec![BranchUpdateMethod::Rebase]
            };
            let (outcome, calls) = run_script(reads, outcomes).await;
            assert_eq!(
                outcome.unwrap(),
                AdvanceOutcome::UpdatePending {
                    pull_request_number: 1,
                    method
                }
            );
            assert_methods(calls, &methods);
        }
    }

    #[tokio::test]
    async fn two_confirmed_conflicts_leave_the_pr_without_another_mutation() {
        let (outcome, calls) = run_script(
            vec![Ok(fixture()), Ok(fixture()), Ok(fixture())],
            vec![conflict(), conflict()],
        )
        .await;
        assert!(matches!(
            outcome.unwrap(),
            AdvanceOutcome::BranchUpdateAttempted {
                method: BranchUpdateMethod::Merge,
                outcome: BranchUpdateOutcome::Conflict { .. },
                ..
            }
        ));
        assert_methods(
            calls,
            &[BranchUpdateMethod::Rebase, BranchUpdateMethod::Merge],
        );
    }

    fn invalidated_snapshots() -> Vec<PullRequestState> {
        let changes: Vec<fn(&mut PullRequestState)> = vec![
            |pr| pr.head_ref_oid = "pushed-head".to_owned(),
            |pr| pr.base_ref_oid = "advanced-base".to_owned(),
            |pr| pr.base_ref_name = "retargeted-same-oid".to_owned(),
            |pr| pr.head_ref_name = "renamed-head".to_owned(),
            |pr| pr.head_repository_id = Some("different-fork".to_owned()),
            |pr| pr.head_repository_id = None,
            |pr| pr.id = PullRequestId("different-pr".to_owned()),
            |pr| pr.auto_merge_request = None,
            |pr| pr.auto_merge_request.as_mut().unwrap().enabled_at = "reenabled".to_owned(),
            |pr| pr.review_decision = ReviewDecision::ChangesRequested,
            |pr| pr.review_decision = ReviewDecision::Unknown,
            |pr| pr.is_draft = true,
            |pr| pr.state = PullRequestStatus::Closed,
            |pr| pr.state = PullRequestStatus::Merged,
            |pr| pr.mergeability = Mergeability::Unknown,
            |pr| pr.mergeability = Mergeability::Conflicting,
            |pr| pr.merge_state_status = MergeStateStatus::Clean,
            |pr| pr.merge_state_status = MergeStateStatus::Conflicting,
            |pr| pr.merge_state_status = MergeStateStatus::Unknown,
        ];
        changes
            .into_iter()
            .map(|change| {
                let mut pr = fixture();
                change(&mut pr);
                pr
            })
            .collect()
    }

    #[tokio::test]
    async fn changes_before_rebase_or_between_methods_require_recomputation() {
        for changed in invalidated_snapshots() {
            let (outcome, calls) = run_script(vec![Ok(changed.clone())], vec![]).await;
            assert_eq!(
                outcome.unwrap(),
                AdvanceOutcome::StateChanged {
                    pull_request_number: 1
                }
            );
            assert!(calls.is_empty());
            let (outcome, calls) =
                run_script(vec![Ok(fixture()), Ok(changed)], vec![conflict()]).await;
            assert_eq!(
                outcome.unwrap(),
                AdvanceOutcome::StateChanged {
                    pull_request_number: 1
                }
            );
            assert_methods(calls, &[BranchUpdateMethod::Rebase]);
        }
    }

    #[tokio::test]
    async fn a_target_race_during_either_update_recomputes_without_further_mutation() {
        for method in [BranchUpdateMethod::Rebase, BranchUpdateMethod::Merge] {
            for accepted_result in [false, true] {
                let mut changed = if accepted_result {
                    updated()
                } else {
                    fixture()
                };
                changed.base_ref_oid = "new-target".to_owned();
                let mut reads = vec![Ok(fixture())];
                let mut outcomes = Vec::new();
                let mut methods = vec![BranchUpdateMethod::Rebase];
                if method == BranchUpdateMethod::Merge {
                    reads.push(Ok(fixture()));
                    outcomes.push(conflict());
                    methods.push(BranchUpdateMethod::Merge);
                }
                reads.push(Ok(changed));
                outcomes.push(if accepted_result {
                    accepted()
                } else {
                    conflict()
                });
                let (outcome, calls) = run_script(reads, outcomes).await;
                assert_eq!(
                    outcome.unwrap(),
                    AdvanceOutcome::StateChanged {
                        pull_request_number: 1
                    }
                );
                assert_methods(calls, &methods);
            }
        }
    }

    #[tokio::test]
    async fn stale_and_operational_failures_never_authorize_the_next_method() {
        for method in [BranchUpdateMethod::Rebase, BranchUpdateMethod::Merge] {
            for stale in [false, true] {
                let mut reads = vec![Ok(fixture())];
                let mut outcomes = Vec::new();
                let mut methods = vec![BranchUpdateMethod::Rebase];
                if method == BranchUpdateMethod::Merge {
                    reads.push(Ok(fixture()));
                    outcomes.push(conflict());
                    methods.push(BranchUpdateMethod::Merge);
                }
                outcomes.push(if stale {
                    Ok(BranchUpdateOutcome::StaleHead {
                        message: "head mismatch".to_owned(),
                    })
                } else {
                    Err(anyhow::anyhow!("uncertain/operational request failure"))
                });
                let (outcome, calls) = run_script(reads, outcomes).await;
                if stale {
                    assert!(matches!(
                        outcome.unwrap(),
                        AdvanceOutcome::BranchUpdateAttempted {
                            outcome: BranchUpdateOutcome::StaleHead { .. },
                            ..
                        }
                    ));
                } else {
                    assert!(outcome.is_err());
                }
                assert_methods(calls, &methods);
            }
        }
    }

    #[tokio::test]
    async fn failed_freshness_or_completion_reads_stop_further_mutation() {
        for stage in 0..3 {
            let mut reads = vec![];
            let mut outcomes = vec![];
            let mut methods = vec![];
            if stage > 0 {
                reads.push(Ok(fixture()));
                outcomes.push(conflict());
                methods.push(BranchUpdateMethod::Rebase);
            }
            if stage > 1 {
                reads.push(Ok(fixture()));
                outcomes.push(accepted());
                methods.push(BranchUpdateMethod::Merge);
            }
            reads.push(Err(anyhow::anyhow!("GitHub read failed")));
            let (outcome, calls) = run_script(reads, outcomes).await;
            assert!(outcome.is_err());
            assert_methods(calls, &methods);
        }
    }
}
