//! Read-only GitHub queries and the application model for repository queue state.

use std::{fmt, str::FromStr};

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::OffsetDateTime;

const GRAPHQL_URL: &str = "https://api.github.com/graphql";
const PAGE_SIZE: u8 = 100;

/// Identifies a repository together with the installation that grants access to it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryLocator {
    pub installation_id: u64,
    pub owner: String,
    pub name: String,
}

impl RepositoryLocator {
    pub fn new(
        installation_id: u64,
        owner: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self> {
        let repository = Self {
            installation_id,
            owner: owner.into(),
            name: name.into(),
        };
        repository.validate()?;
        Ok(repository)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.installation_id > 0,
            "installation ID must be greater than zero"
        );
        ensure!(!self.owner.is_empty(), "repository owner must not be empty");
        ensure!(!self.name.is_empty(), "repository name must not be empty");
        Ok(())
    }
}

/// GitHub's canonical identity for the repository queried under an installation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryIdentity {
    pub installation_id: u64,
    pub node_id: String,
    pub name_with_owner: String,
}

/// A fresh view of all open pull requests in one repository, ordered oldest first.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryState {
    pub repository: RepositoryIdentity,
    pub pull_requests: Vec<PullRequestState>,
}

/// An application-level snapshot of the queue-relevant state of one pull request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PullRequestState {
    pub id: PullRequestId,
    pub number: u64,
    /// GitHub returns this timestamp in RFC 3339 format.
    pub created_at: String,
    pub state: PullRequestStatus,
    pub is_draft: bool,
    pub review_decision: ReviewDecision,
    pub auto_merge_request: Option<AutoMergeRequest>,
    pub mergeability: Mergeability,
    pub merge_state_status: MergeStateStatus,
    pub head_ref_oid: String,
    pub base_ref_oid: String,
}

impl PullRequestState {
    /// Return the base relationship that GitHub's merge status establishes.
    ///
    /// GitHub reports `BEHIND` explicitly. `CLEAN`, `HAS_HOOKS`, and `UNSTABLE`
    /// describe a mergeable PR (including one with failing checks); under the
    /// proposal's strict up-to-date branch rule, these states mean the PR is
    /// current. Other states do not establish the relationship, so callers
    /// must treat them as unresolved.
    pub fn base_relationship(&self) -> BaseRelationship {
        match self.merge_state_status {
            MergeStateStatus::Behind => BaseRelationship::Behind,
            MergeStateStatus::Clean | MergeStateStatus::HasHooks | MergeStateStatus::Unstable => {
                BaseRelationship::Current
            }
            MergeStateStatus::Blocked
            | MergeStateStatus::Conflicting
            | MergeStateStatus::Draft
            | MergeStateStatus::Unknown => BaseRelationship::Unresolved,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PullRequestId(pub String);

impl PullRequestId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PullRequestStatus {
    Open,
    Closed,
    Merged,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReviewDecision {
    Approved,
    ChangesRequested,
    ReviewRequired,
    /// GitHub returned null or a value this client does not recognize.
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Mergeability {
    Mergeable,
    Conflicting,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MergeStateStatus {
    Behind,
    Blocked,
    Clean,
    Conflicting,
    Draft,
    HasHooks,
    Unknown,
    Unstable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BaseRelationship {
    Behind,
    Current,
    Unresolved,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoMergeRequest {
    pub enabled_at: String,
    pub merge_method: MergeMethod,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MergeMethod {
    Merge,
    Rebase,
    Squash,
    Unknown,
}

/// Queries GitHub's current repository and pull-request state.
#[derive(Clone)]
pub struct GitHubQueries {
    client: Client,
    graphql_url: String,
}

impl GitHubQueries {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            graphql_url: GRAPHQL_URL.to_owned(),
        }
    }

    /// Fetch every open PR, in creation order, plus the canonical repository identity.
    pub async fn repository_state(
        &self,
        repository: &RepositoryLocator,
        token: &str,
    ) -> Result<RepositoryState> {
        repository.validate()?;
        validate_token(token)?;
        let mut cursor = None;
        let mut state = None::<RepositoryState>;

        loop {
            let page = self
                .repository_page(repository, token, cursor.as_deref())
                .await?;
            let has_next_page = page.page_info.has_next_page;
            let end_cursor = page.page_info.end_cursor.clone();
            match &mut state {
                Some(state) => {
                    ensure!(
                        state.repository == page.repository,
                        "GitHub repository identity changed while paging open pull requests"
                    );
                    state.pull_requests.extend(page.pull_requests);
                }
                None => {
                    state = Some(RepositoryState {
                        repository: page.repository,
                        pull_requests: page.pull_requests,
                    })
                }
            }

            if !has_next_page {
                break;
            }
            let next =
                end_cursor.context("GitHub reported another PR page without an end cursor")?;
            ensure!(
                cursor.as_deref() != Some(next.as_str()),
                "GitHub repeated the PR pagination cursor"
            );
            cursor = Some(next);
        }

        state.context("GitHub returned no repository state")
    }

    /// Fetch one PR by number, including its current eligibility and merge state.
    pub async fn pull_request_state(
        &self,
        repository: &RepositoryLocator,
        number: u64,
        token: &str,
    ) -> Result<PullRequestState> {
        repository.validate()?;
        validate_token(token)?;
        let number = i32::try_from(number).context("PR number is too large for GitHub GraphQL")?;
        let response = self
            .graphql_post(
                token,
                PULL_REQUEST_QUERY,
                json!({
                    "owner": repository.owner,
                    "repo": repository.name,
                    "number": number,
                }),
            )
            .await?;
        let pull_request = response["repository"]["pullRequest"].clone();
        ensure!(
            !pull_request.is_null(),
            "GitHub did not return the requested pull request"
        );
        decode_pull_request(pull_request)
    }

    async fn repository_page(
        &self,
        repository: &RepositoryLocator,
        token: &str,
        cursor: Option<&str>,
    ) -> Result<RepositoryPage> {
        let response = self
            .graphql_post(
                token,
                REPOSITORY_QUERY,
                json!({
                    "owner": repository.owner,
                    "repo": repository.name,
                    "first": PAGE_SIZE,
                    "after": cursor,
                }),
            )
            .await?;
        let repo = &response["repository"];
        ensure!(
            !repo.is_null(),
            "GitHub did not return the requested repository"
        );
        let node_id = required_string(&repo["id"], "repository ID")?;
        let name_with_owner = required_string(&repo["nameWithOwner"], "repository name")?;
        let pull_requests = repo["pullRequests"].clone();
        ensure!(
            !pull_requests.is_null(),
            "GitHub did not return the PR connection"
        );
        let page_info: GraphQlPageInfo = serde_json::from_value(pull_requests["pageInfo"].clone())
            .context("could not decode pull-request page information")?;
        let nodes = pull_requests["nodes"]
            .as_array()
            .context("GitHub returned an invalid pull-request page")?;
        let pull_requests = nodes
            .iter()
            .cloned()
            .map(decode_pull_request)
            .collect::<Result<Vec<_>>>()?;

        Ok(RepositoryPage {
            repository: RepositoryIdentity {
                installation_id: repository.installation_id,
                node_id,
                name_with_owner,
            },
            pull_requests,
            page_info,
        })
    }

    async fn graphql_post(&self, token: &str, query: &str, variables: Value) -> Result<Value> {
        let response = self
            .client
            .post(&self.graphql_url)
            .bearer_auth(token)
            .header("Accept", "application/vnd.github+json")
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await
            .context("GitHub GraphQL request failed")?;
        let response = ensure_success(response, token).await?;
        let body: GraphQlEnvelope = response
            .json()
            .await
            .context("could not decode GitHub GraphQL response")?;
        if !body.errors.is_empty() {
            let messages = body
                .errors
                .into_iter()
                .map(|error| error.message.replace(token, "[REDACTED]"))
                .collect::<Vec<_>>();
            bail!("GitHub GraphQL query failed: {}", messages.join("; "));
        }
        body.data
            .context("GitHub GraphQL response did not contain data")
    }
}

impl fmt::Debug for GitHubQueries {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GitHubQueries")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct RepositoryPage {
    repository: RepositoryIdentity,
    pull_requests: Vec<PullRequestState>,
    page_info: GraphQlPageInfo,
}

#[derive(Debug, Deserialize)]
struct GraphQlEnvelope {
    data: Option<Value>,
    #[serde(default)]
    errors: Vec<GraphQlError>,
}

#[derive(Debug, Deserialize)]
struct GraphQlError {
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphQlPageInfo {
    has_next_page: bool,
    end_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphQlPullRequest {
    id: String,
    number: u64,
    created_at: String,
    state: String,
    is_draft: bool,
    review_decision: Option<String>,
    mergeable: String,
    merge_state_status: String,
    head_ref_oid: String,
    base_ref_oid: String,
    auto_merge_request: Option<GraphQlAutoMergeRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphQlAutoMergeRequest {
    enabled_at: String,
    merge_method: String,
}

fn decode_pull_request(value: Value) -> Result<PullRequestState> {
    let pull_request: GraphQlPullRequest =
        serde_json::from_value(value).context("could not decode GitHub pull-request state")?;
    OffsetDateTime::parse(
        &pull_request.created_at,
        &time::format_description::well_known::Rfc3339,
    )
    .context("GitHub returned an invalid PR creation timestamp")?;
    Ok(PullRequestState {
        id: PullRequestId(pull_request.id),
        number: pull_request.number,
        created_at: pull_request.created_at,
        state: PullRequestStatus::from_str(&pull_request.state)
            .unwrap_or(PullRequestStatus::Unknown),
        is_draft: pull_request.is_draft,
        review_decision: pull_request
            .review_decision
            .as_deref()
            .map(|value| ReviewDecision::from_str(value).unwrap_or(ReviewDecision::Unknown))
            .unwrap_or(ReviewDecision::Unknown),
        auto_merge_request: pull_request
            .auto_merge_request
            .map(|auto_merge| AutoMergeRequest {
                enabled_at: auto_merge.enabled_at,
                merge_method: MergeMethod::from_str(&auto_merge.merge_method)
                    .unwrap_or(MergeMethod::Unknown),
            }),
        mergeability: Mergeability::from_str(&pull_request.mergeable)
            .unwrap_or(Mergeability::Unknown),
        merge_state_status: MergeStateStatus::from_str(&pull_request.merge_state_status)
            .unwrap_or(MergeStateStatus::Unknown),
        head_ref_oid: pull_request.head_ref_oid,
        base_ref_oid: pull_request.base_ref_oid,
    })
}

impl FromStr for PullRequestStatus {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "OPEN" => Self::Open,
            "CLOSED" => Self::Closed,
            "MERGED" => Self::Merged,
            _ => return Err(()),
        })
    }
}

impl FromStr for ReviewDecision {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "APPROVED" => Self::Approved,
            "CHANGES_REQUESTED" => Self::ChangesRequested,
            "REVIEW_REQUIRED" => Self::ReviewRequired,
            _ => return Err(()),
        })
    }
}

impl FromStr for Mergeability {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "MERGEABLE" => Self::Mergeable,
            "CONFLICTING" => Self::Conflicting,
            "UNKNOWN" => Self::Unknown,
            _ => return Err(()),
        })
    }
}

impl FromStr for MergeStateStatus {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "BEHIND" => Self::Behind,
            "BLOCKED" => Self::Blocked,
            "CLEAN" => Self::Clean,
            "DIRTY" => Self::Conflicting,
            "DRAFT" => Self::Draft,
            "HAS_HOOKS" => Self::HasHooks,
            "UNKNOWN" => Self::Unknown,
            "UNSTABLE" => Self::Unstable,
            _ => return Err(()),
        })
    }
}

impl FromStr for MergeMethod {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "MERGE" => Self::Merge,
            "REBASE" => Self::Rebase,
            "SQUASH" => Self::Squash,
            _ => return Err(()),
        })
    }
}

fn required_string(value: &Value, field: &str) -> Result<String> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .with_context(|| format!("GitHub returned no {field}"))
}

fn validate_token(token: &str) -> Result<()> {
    ensure!(
        !token.is_empty(),
        "GitHub installation token must not be empty"
    );
    Ok(())
}

async fn ensure_success(response: Response, secret: &str) -> Result<Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response.text().await.unwrap_or_default();
    let message = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|json| json["message"].as_str().map(str::to_owned))
        .unwrap_or_default()
        .replace(secret, "[REDACTED]");
    if message.is_empty() {
        bail!("GitHub GraphQL returned HTTP {status}");
    }
    bail!("GitHub GraphQL returned HTTP {status}: {message}");
}

const PULL_REQUEST_QUERY: &str = "query($owner: String!, $repo: String!, $number: Int!) { repository(owner: $owner, name: $repo) { pullRequest(number: $number) { id number createdAt state isDraft reviewDecision mergeable mergeStateStatus headRefOid baseRefOid autoMergeRequest { enabledAt mergeMethod } } } }";
const REPOSITORY_QUERY: &str = "query($owner: String!, $repo: String!, $first: Int!, $after: String) { repository(owner: $owner, name: $repo) { id nameWithOwner pullRequests(states: [OPEN], first: $first, after: $after, orderBy: { field: CREATED_AT, direction: ASC }) { pageInfo { hasNextPage endCursor } nodes { id number createdAt state isDraft reviewDecision mergeable mergeStateStatus headRefOid baseRefOid autoMergeRequest { enabledAt mergeMethod } } } } }";

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    fn query_client(url: String) -> GitHubQueries {
        GitHubQueries {
            client: Client::new(),
            graphql_url: url,
        }
    }

    fn sample_pull_request(overrides: Value) -> Value {
        let mut value = json!({
            "id": "PR_kwDO-test",
            "number": 7,
            "createdAt": "2026-10-14T00:00:00Z",
            "state": "OPEN",
            "isDraft": false,
            "reviewDecision": "APPROVED",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "BEHIND",
            "headRefOid": "head-oid",
            "baseRefOid": "base-oid",
            "autoMergeRequest": {
                "enabledAt": "2026-10-14T00:01:00Z",
                "mergeMethod": "SQUASH"
            }
        });
        for (key, replacement) in overrides.as_object().unwrap() {
            value[key] = replacement.clone();
        }
        value
    }

    fn response_page(nodes: Vec<Value>, has_next_page: bool, end_cursor: Option<&str>) -> Value {
        json!({
            "data": {
                "repository": {
                    "id": "R_kgDOrepo",
                    "nameWithOwner": "Example/queue",
                    "pullRequests": {
                        "pageInfo": {"hasNextPage": has_next_page, "endCursor": end_cursor},
                        "nodes": nodes
                    }
                }
            }
        })
    }

    #[tokio::test]
    async fn repository_query_paginates_oldest_first_and_returns_typed_state() {
        let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let (url, server) = mock_graphql_server(
            vec![
                response_page(
                    vec![sample_pull_request(json!({"number": 4}))],
                    true,
                    Some("cursor-1"),
                ),
                response_page(
                    vec![sample_pull_request(json!({
                        "id": "PR_kwDO-test-2",
                        "number": 7,
                        "createdAt": "2026-10-14T00:02:00Z",
                        "mergeStateStatus": "UNSTABLE",
                        "autoMergeRequest": null
                    }))],
                    false,
                    Some("cursor-2"),
                ),
            ],
            Arc::clone(&requests),
        );
        let queries = query_client(url);
        let repository = RepositoryLocator::new(19, "example", "queue").unwrap();

        let state = queries
            .repository_state(&repository, "installation-token")
            .await
            .unwrap();
        server.join().unwrap();

        assert_eq!(state.repository.installation_id, 19);
        assert_eq!(state.repository.node_id, "R_kgDOrepo");
        assert_eq!(state.repository.name_with_owner, "Example/queue");
        assert_eq!(state.pull_requests.len(), 2);
        assert_eq!(state.pull_requests[0].number, 4);
        assert_eq!(state.pull_requests[1].number, 7);
        assert_eq!(
            state.pull_requests[0].review_decision,
            ReviewDecision::Approved
        );
        assert_eq!(
            state.pull_requests[0]
                .auto_merge_request
                .as_ref()
                .unwrap()
                .merge_method,
            MergeMethod::Squash
        );
        assert_eq!(
            state.pull_requests[0].base_relationship(),
            BaseRelationship::Behind
        );
        assert_eq!(
            state.pull_requests[1].base_relationship(),
            BaseRelationship::Current
        );
        assert!(state.pull_requests[1].auto_merge_request.is_none());

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0]["variables"]["after"].is_null());
        assert_eq!(requests[1]["variables"]["after"], "cursor-1");
        assert!(
            requests[0]["query"]
                .as_str()
                .unwrap()
                .contains("orderBy: { field: CREATED_AT, direction: ASC }")
        );
    }

    #[tokio::test]
    async fn pull_request_query_returns_the_requested_typed_snapshot() {
        let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let (url, server) = mock_graphql_server(
            vec![json!({
                "data": {
                    "repository": {
                        "pullRequest": sample_pull_request(json!({"number": 23}))
                    }
                }
            })],
            Arc::clone(&requests),
        );
        let queries = query_client(url);
        let repository = RepositoryLocator::new(19, "example", "queue").unwrap();

        let pull_request = queries
            .pull_request_state(&repository, 23, "installation-token")
            .await
            .unwrap();
        server.join().unwrap();

        assert_eq!(pull_request.number, 23);
        assert_eq!(pull_request.state, PullRequestStatus::Open);
        assert_eq!(pull_request.review_decision, ReviewDecision::Approved);
        assert_eq!(pull_request.mergeability, Mergeability::Mergeable);
        assert_eq!(pull_request.base_ref_oid, "base-oid");
        assert_eq!(pull_request.head_ref_oid, "head-oid");
        assert_eq!(requests.lock().unwrap()[0]["variables"]["number"], 23);
    }

    #[test]
    fn missing_review_decision_and_unrecognized_states_remain_unknown() {
        let pull_request = decode_pull_request(sample_pull_request(json!({
            "reviewDecision": null,
            "mergeable": "FUTURE_VALUE",
            "mergeStateStatus": "FUTURE_VALUE"
        })))
        .unwrap();

        assert_eq!(pull_request.review_decision, ReviewDecision::Unknown);
        assert_eq!(pull_request.mergeability, Mergeability::Unknown);
        assert_eq!(pull_request.merge_state_status, MergeStateStatus::Unknown);
        assert_eq!(
            pull_request.base_relationship(),
            BaseRelationship::Unresolved
        );
    }

    #[test]
    fn repository_locator_requires_installation_owner_and_name() {
        assert!(RepositoryLocator::new(0, "owner", "repo").is_err());
        assert!(RepositoryLocator::new(1, "", "repo").is_err());
        assert!(RepositoryLocator::new(1, "owner", "").is_err());
    }

    fn mock_graphql_server(
        responses: Vec<Value>,
        requests: Arc<Mutex<Vec<Value>>>,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(false).unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let body = read_request_body(&mut stream);
                requests
                    .lock()
                    .unwrap()
                    .push(serde_json::from_slice(&body).unwrap());
                let response = serde_json::to_vec(&response).unwrap();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response.len()
                )
                .unwrap();
                stream.write_all(&response).unwrap();
            }
        });
        (format!("http://{address}/graphql"), server)
    }

    fn read_request_body(stream: &mut TcpStream) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut buffer = [0; 1024];
        let body_start;
        let content_length;
        loop {
            let count = stream.read(&mut buffer).unwrap();
            assert_ne!(count, 0, "client closed before sending request");
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                body_start = position + 4;
                let headers = String::from_utf8_lossy(&bytes[..position]);
                content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= body_start + content_length {
                    break;
                }
                loop {
                    let count = stream.read(&mut buffer).unwrap();
                    assert_ne!(count, 0, "client closed before sending request body");
                    bytes.extend_from_slice(&buffer[..count]);
                    if bytes.len() >= body_start + content_length {
                        break;
                    }
                }
                break;
            }
        }
        bytes[body_start..body_start + content_length].to_vec()
    }
}
