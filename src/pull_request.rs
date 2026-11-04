use std::{error::Error, fmt};

use reqwest::{Client, Response, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};

const GRAPHQL_URL: &str = "https://api.github.com/graphql";
const API_VERSION: &str = "2022-11-28";
const USER_AGENT: &str = "merge-app";
const UPDATE_BRANCH_MUTATION: &str = "mutation($input: UpdatePullRequestBranchInput!) { updatePullRequestBranch(input: $input) { pullRequest { id headRefOid } } }";

/// How GitHub should update the PR working branch (never the target branch).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BranchUpdateMethod {
    Rebase,
    Merge,
}

/// The result of asking GitHub to update a pull request onto its base branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchUpdateOutcome {
    /// GitHub accepted the update; callers must re-read state to verify completion.
    Accepted { head_oid: String },
    /// The pull request head no longer matches the OID supplied by the caller.
    StaleHead { message: String },
    /// GitHub reported a confirmed content conflict for the requested method.
    Conflict { message: String },
}

/// An unexpected failure while requesting a pull request branch update.
#[derive(Debug)]
pub enum BranchUpdateError {
    InvalidInput(&'static str),
    Request(String),
    Http { status: StatusCode, message: String },
    InvalidResponse(String),
    GitHub(Vec<String>),
}

impl fmt::Display for BranchUpdateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => {
                write!(formatter, "invalid branch update input: {message}")
            }
            Self::Request(message) => write!(formatter, "GitHub GraphQL request failed: {message}"),
            Self::Http { status, message } if message.is_empty() => {
                write!(formatter, "GitHub returned HTTP {status}")
            }
            Self::Http { status, message } => {
                write!(formatter, "GitHub returned HTTP {status}: {message}")
            }
            Self::InvalidResponse(message) => {
                write!(formatter, "invalid GitHub GraphQL response: {message}")
            }
            Self::GitHub(messages) => {
                write!(
                    formatter,
                    "GitHub GraphQL mutation failed: {}",
                    messages.join("; ")
                )
            }
        }
    }
}

impl Error for BranchUpdateError {}

#[derive(Deserialize)]
struct GraphQlResponse {
    #[serde(default)]
    data: Option<Value>,
    #[serde(default)]
    errors: Vec<GraphQlError>,
}

#[derive(Deserialize)]
struct GraphQlError {
    message: String,
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    path: Option<Vec<Value>>,
}

/// Ask GitHub to update a pull request using the caller's observed head OID.
///
/// The supplied installation token must have Contents: write and Pull
/// requests: write access. This function performs one mutation and does not
/// query or retry with a newer head. A [`BranchUpdateOutcome::StaleHead`] means the
/// caller must recompute repository state before it attempts another mutation.
pub async fn update_pull_request_branch(
    client: &Client,
    installation_token: &str,
    pull_request_id: &str,
    expected_head_oid: &str,
    method: BranchUpdateMethod,
) -> Result<BranchUpdateOutcome, BranchUpdateError> {
    update_pull_request_branch_at(
        client,
        GRAPHQL_URL,
        installation_token,
        pull_request_id,
        expected_head_oid,
        method,
    )
    .await
}

async fn update_pull_request_branch_at(
    client: &Client,
    graphql_url: &str,
    installation_token: &str,
    pull_request_id: &str,
    expected_head_oid: &str,
    method: BranchUpdateMethod,
) -> Result<BranchUpdateOutcome, BranchUpdateError> {
    if installation_token.is_empty() {
        return Err(BranchUpdateError::InvalidInput(
            "installation token must not be empty",
        ));
    }
    if pull_request_id.is_empty() {
        return Err(BranchUpdateError::InvalidInput(
            "pull request ID must not be empty",
        ));
    }
    if expected_head_oid.is_empty() {
        return Err(BranchUpdateError::InvalidInput(
            "expected head OID must not be empty",
        ));
    }

    let response = client
        .post(graphql_url)
        .bearer_auth(installation_token)
        .header("User-Agent", USER_AGENT)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", API_VERSION)
        .json(&json!({
            "query": UPDATE_BRANCH_MUTATION,
            "variables": {
                "input": {
                    "pullRequestId": pull_request_id,
                    "expectedHeadOid": expected_head_oid,
                    "updateMethod": method
                }
            }
        }))
        .send()
        .await
        .map_err(|error| BranchUpdateError::Request(error.to_string()))?;

    let response = ensure_success(response, installation_token).await?;
    let body: GraphQlResponse = response.json().await.map_err(|error| {
        BranchUpdateError::InvalidResponse(format!("could not decode response body: {error}"))
    })?;
    let mut errors = body.errors;
    for error in &mut errors {
        error.message = error.message.replace(installation_token, "[REDACTED]");
    }
    // Partial data plus errors is uncertain completion, never permission to fall back.
    let mutation = body
        .data
        .as_ref()
        .and_then(|data| data.get("updatePullRequestBranch"));
    if !errors.is_empty() {
        if mutation.is_some_and(Value::is_null)
            && let Some(outcome) = expected_outcome(&errors, method)
        {
            return Ok(outcome);
        }
        return Err(BranchUpdateError::GitHub(
            errors.into_iter().map(|e| e.message).collect(),
        ));
    }

    let pull_request = mutation.and_then(|mutation| mutation.get("pullRequest"));
    let id = pull_request
        .and_then(|pr| pr.get("id"))
        .and_then(Value::as_str);
    let head_oid = pull_request
        .and_then(|pr| pr.get("headRefOid"))
        .and_then(Value::as_str);
    match (id, head_oid) {
        (Some(id), Some(head_oid)) if id == pull_request_id && !head_oid.is_empty() => {
            Ok(BranchUpdateOutcome::Accepted {
                head_oid: head_oid.to_owned(),
            })
        }
        _ => Err(BranchUpdateError::InvalidResponse(
            "updatePullRequestBranch returned no matching pull request and head".to_owned(),
        )),
    }
}

fn expected_outcome(
    errors: &[GraphQlError],
    method: BranchUpdateMethod,
) -> Option<BranchUpdateOutcome> {
    if errors.is_empty() {
        return None;
    }
    let message = errors
        .iter()
        .map(|e| e.message.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    if errors.iter().all(|e| is_stale_head_message(&e.message)) {
        return Some(BranchUpdateOutcome::StaleHead { message });
    }
    // Exact signatures captured from disposable GitHub PRs. Unknown wording,
    // wrong paths/types and mixed errors fail closed. See tests/fixtures/branch-update.
    let conflict_messages: &[&str] = match method {
        BranchUpdateMethod::Rebase => &[
            "rebase conflict between base and head",
            "PullRequest::RebaseConflictError",
        ],
        BranchUpdateMethod::Merge => &["merge conflict between base and head"],
    };
    if errors.iter().all(|e| {
        e.kind.as_deref() == Some("UNPROCESSABLE")
            && e.path.as_deref() == Some(&[json!("updatePullRequestBranch")])
            && conflict_messages.contains(&e.message.as_str())
    }) {
        return Some(BranchUpdateOutcome::Conflict { message });
    }
    None
}

fn is_stale_head_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    let describes_expected_head =
        message.contains("expected") && (message.contains("head") || message.contains("oid"));
    let describes_mismatch = [
        "does not match",
        "doesn't match",
        "did not match",
        "didn't match",
        "mismatch",
    ]
    .iter()
    .any(|phrase| message.contains(phrase));

    (describes_expected_head && describes_mismatch)
        || message.contains("head sha didn't match the current head ref")
}

async fn ensure_success(
    response: Response,
    installation_token: &str,
) -> Result<Response, BranchUpdateError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    let body = response.text().await.unwrap_or_default();
    let message = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|body| body["message"].as_str().map(str::to_owned))
        .unwrap_or_default()
        .replace(installation_token, "[REDACTED]");
    Err(BranchUpdateError::Http { status, message })
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::mpsc,
        thread,
    };

    use super::*;

    fn mock_server(status: &'static str, body: &'static str) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();

        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let request = read_request(&mut stream);
                let _ = request_tx.send(request);
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        (format!("http://{address}/graphql"), request_rx)
    }

    fn read_request(stream: &mut TcpStream) -> String {
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        let mut chunk = [0; 4096];
        let mut body_end = None;
        loop {
            let count = stream.read(&mut chunk).unwrap();
            if count == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..count]);
            if body_end.is_none()
                && let Some(header_end) =
                    request.windows(4).position(|window| window == b"\r\n\r\n")
            {
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                body_end = Some(header_end + 4 + content_length);
            }
            if body_end.is_some_and(|end| request.len() >= end) {
                break;
            }
        }
        String::from_utf8(request).unwrap()
    }

    fn request_body(request: &str) -> Value {
        let body = request.split_once("\r\n\r\n").unwrap().1;
        serde_json::from_str(body).unwrap()
    }

    #[tokio::test]
    async fn reports_a_successful_rebase() {
        let (url, _) = mock_server(
            "200 OK",
            r#"{"data":{"updatePullRequestBranch":{"pullRequest":{"id":"PR_kwDO123","headRefOid":"rebased-head-oid"}}}}"#,
        );
        let result = update_pull_request_branch_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
            BranchUpdateMethod::Rebase,
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            BranchUpdateOutcome::Accepted {
                head_oid: "rebased-head-oid".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn sends_one_rebase_using_the_observed_head_and_reports_stale_head() {
        let (url, requests) = mock_server(
            "200 OK",
            r#"{"data":{"updatePullRequestBranch":null},"errors":[{"message":"The expected head OID does not match the current pull request head"}]}"#,
        );
        let result = update_pull_request_branch_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
            BranchUpdateMethod::Rebase,
        )
        .await
        .unwrap();

        assert!(matches!(result, BranchUpdateOutcome::StaleHead { .. }));
        let request = requests.recv().unwrap();
        assert!(request.starts_with("POST /graphql "));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer installation-token")
        );
        let body = request_body(&request);
        assert!(
            body["query"]
                .as_str()
                .unwrap()
                .contains("updatePullRequestBranch")
        );
        assert_eq!(body["variables"]["input"]["updateMethod"], "REBASE");
        assert_eq!(
            body["variables"]["input"]["expectedHeadOid"],
            "observed-head-oid"
        );
        assert_eq!(body["variables"]["input"]["pullRequestId"], "PR_kwDO123");
    }

    #[tokio::test]
    async fn recognizes_github_head_sha_mismatch_wording() {
        let (url, _) = mock_server(
            "200 OK",
            include_str!("../tests/fixtures/branch-update/stale-head.json"),
        );
        let result = update_pull_request_branch_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
            BranchUpdateMethod::Rebase,
        )
        .await
        .unwrap();

        assert!(matches!(result, BranchUpdateOutcome::StaleHead { .. }));
    }

    #[tokio::test]
    async fn mixed_stale_head_and_unrelated_errors_remain_unexpected() {
        let (url, _) = mock_server(
            "200 OK",
            r#"{"data":{"updatePullRequestBranch":null},"errors":[{"message":"head sha didn't match the current head ref."},{"message":"Resource not accessible by integration"}]}"#,
        );
        let error = update_pull_request_branch_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
            BranchUpdateMethod::Rebase,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, BranchUpdateError::GitHub(_)));
    }

    #[tokio::test]
    async fn reports_conflicts_as_an_expected_outcome() {
        let (url, _) = mock_server(
            "200 OK",
            include_str!("../tests/fixtures/branch-update/rebase-conflict.json"),
        );
        let result = update_pull_request_branch_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
            BranchUpdateMethod::Rebase,
        )
        .await
        .unwrap();

        assert!(matches!(result, BranchUpdateOutcome::Conflict { .. }));
    }

    #[tokio::test]
    async fn reports_unexpected_graphql_errors_as_failures() {
        let (url, _) = mock_server(
            "200 OK",
            r#"{"data":{"updatePullRequestBranch":null},"errors":[{"message":"Resource not accessible by integration"}]}"#,
        );
        let error = update_pull_request_branch_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
            BranchUpdateMethod::Rebase,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, BranchUpdateError::GitHub(_)));
        assert!(
            error
                .to_string()
                .contains("Resource not accessible by integration")
        );
    }

    #[tokio::test]
    async fn reports_http_failures_separately() {
        let (url, _) = mock_server(
            "403 Forbidden",
            r#"{"message":"Resource not accessible by integration"}"#,
        );
        let error = update_pull_request_branch_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
            BranchUpdateMethod::Rebase,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            BranchUpdateError::Http {
                status: StatusCode::FORBIDDEN,
                ..
            }
        ));
    }

    #[test]
    fn does_not_classify_mixed_expected_and_unexpected_errors_as_expected() {
        let errors = serde_json::from_str::<GraphQlResponse>(r#"{"errors":[{"message":"The expected head OID does not match"},{"message":"Resource not accessible by integration"}]}"#).unwrap().errors;
        assert!(expected_outcome(&errors, BranchUpdateMethod::Rebase).is_none());
    }
    #[tokio::test]
    async fn sends_explicit_merge_with_expected_head_and_accepts_old_head_response() {
        let (url, requests) = mock_server(
            "200 OK",
            r#"{"data":{"updatePullRequestBranch":{"pullRequest":{"id":"PR_kwDO123","headRefOid":"observed-head-oid"}}}}"#,
        );
        let result = update_pull_request_branch_at(
            &Client::new(),
            &url,
            "token",
            "PR_kwDO123",
            "observed-head-oid",
            BranchUpdateMethod::Merge,
        )
        .await
        .unwrap();
        assert_eq!(
            result,
            BranchUpdateOutcome::Accepted {
                head_oid: "observed-head-oid".to_owned()
            }
        );
        let body = request_body(&requests.recv().unwrap());
        assert_eq!(
            body["variables"]["input"],
            json!({"pullRequestId":"PR_kwDO123", "expectedHeadOid":"observed-head-oid", "updateMethod":"MERGE"})
        );
    }

    #[test]
    fn classifies_only_captured_conflicts_for_the_requested_method() {
        for (fixture, method) in [
            (
                include_str!("../tests/fixtures/branch-update/rebase-conflict.json"),
                BranchUpdateMethod::Rebase,
            ),
            (
                include_str!("../tests/fixtures/branch-update/rebase-conflict-class.json"),
                BranchUpdateMethod::Rebase,
            ),
            (
                include_str!("../tests/fixtures/branch-update/merge-conflict.json"),
                BranchUpdateMethod::Merge,
            ),
        ] {
            let response: GraphQlResponse = serde_json::from_str(fixture).unwrap();
            assert!(matches!(
                expected_outcome(&response.errors, method),
                Some(BranchUpdateOutcome::Conflict { .. })
            ));
            let other = if method == BranchUpdateMethod::Rebase {
                BranchUpdateMethod::Merge
            } else {
                BranchUpdateMethod::Rebase
            };
            assert!(expected_outcome(&response.errors, other).is_none());
        }
    }

    #[tokio::test]
    async fn ambiguous_or_operational_responses_never_authorize_fallback() {
        for (status, body) in [
            (
                "200 OK",
                r#"{"data":{"updatePullRequestBranch":null},"errors":[{"message":"The pull request branch has conflicts"}]}"#,
            ),
            (
                "200 OK",
                r#"{"data":{"updatePullRequestBranch":null},"errors":[{"type":"UNPROCESSABLE","path":["otherMutation"],"message":"rebase conflict between base and head"}]}"#,
            ),
            (
                "200 OK",
                r#"{"data":{"updatePullRequestBranch":null},"errors":[{"type":"FORBIDDEN","path":["updatePullRequestBranch"],"message":"rebase conflict between base and head"}]}"#,
            ),
            (
                "200 OK",
                r#"{"data":{"updatePullRequestBranch":null},"errors":[{"type":"UNPROCESSABLE","path":["updatePullRequestBranch"],"message":"rebase conflict between base and head"},{"message":"Resource not accessible by integration"}]}"#,
            ),
            (
                "200 OK",
                r#"{"data":{"updatePullRequestBranch":null},"errors":[{"type":"UNPROCESSABLE","path":["updatePullRequestBranch"],"message":"rebase conflict between base and head"},{"message":"head sha didn't match the current head ref."}]}"#,
            ),
            (
                "200 OK",
                r#"{"data":{"updatePullRequestBranch":{"pullRequest":{"id":"PR_kwDO123","headRefOid":"new-head"}}},"errors":[{"type":"UNPROCESSABLE","path":["updatePullRequestBranch"],"message":"rebase conflict between base and head"}]}"#,
            ),
            (
                "200 OK",
                r#"{"errors":[{"type":"UNPROCESSABLE","path":["updatePullRequestBranch"],"message":"rebase conflict between base and head"}]}"#,
            ),
            (
                "200 OK",
                r#"{"data":{"updatePullRequestBranch":{"pullRequest":{"id":"wrong-pr","headRefOid":"new-head"}}}}"#,
            ),
            (
                "200 OK",
                r#"{"data":{"updatePullRequestBranch":{"pullRequest":{"id":"PR_kwDO123"}}}}"#,
            ),
            ("200 OK", "invalid json"),
            ("401 Unauthorized", r#"{"message":"Bad credentials"}"#),
            (
                "429 Too Many Requests",
                r#"{"message":"rate limit exceeded"}"#,
            ),
            (
                "422 Unprocessable Entity",
                r#"{"message":"rebase conflict between base and head"}"#,
            ),
            (
                "500 Internal Server Error",
                r#"{"message":"conflict in backend operation"}"#,
            ),
        ] {
            let (url, _) = mock_server(status, body);
            let result = update_pull_request_branch_at(
                &Client::new(),
                &url,
                "token",
                "PR_kwDO123",
                "head",
                BranchUpdateMethod::Rebase,
            )
            .await;
            assert!(result.is_err(), "{status}: {body}: {result:?}");
        }
    }
    #[tokio::test]
    async fn a_request_timeout_is_an_uncertain_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/graphql", listener.local_addr().unwrap());
        let (release, hold) = mpsc::channel::<()>();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            let _ = hold.recv_timeout(std::time::Duration::from_secs(5));
            request
        });
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(100))
            .build()
            .unwrap();
        let result = update_pull_request_branch_at(
            &client,
            &url,
            "token",
            "PR_test",
            "head",
            BranchUpdateMethod::Rebase,
        )
        .await;
        release.send(()).unwrap();
        assert!(server.join().unwrap().contains("REBASE"));
        assert!(matches!(result, Err(BranchUpdateError::Request(_))));
    }
}
