use std::{error::Error, fmt};

use reqwest::{Client, Response, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};

const GRAPHQL_URL: &str = "https://api.github.com/graphql";
const API_VERSION: &str = "2022-11-28";
const USER_AGENT: &str = "merge-app";
const UPDATE_BRANCH_MUTATION: &str = "mutation($input: UpdatePullRequestBranchInput!) { updatePullRequestBranch(input: $input) { pullRequest { id headRefOid } } }";

/// The result of asking GitHub to rebase a pull request onto its base branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebaseOutcome {
    /// GitHub accepted the rebase mutation.
    Rebased,
    /// The pull request head no longer matches the OID supplied by the caller.
    StaleHead { message: String },
    /// GitHub cannot rebase the pull request because it has conflicts.
    Conflict { message: String },
}

/// An unexpected failure while requesting a pull request rebase.
#[derive(Debug)]
pub enum RebaseError {
    InvalidInput(&'static str),
    Request(String),
    Http { status: StatusCode, message: String },
    InvalidResponse(String),
    GitHub(Vec<String>),
}

impl fmt::Display for RebaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(formatter, "invalid rebase input: {message}"),
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

impl Error for RebaseError {}

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
}

/// Ask GitHub to rebase a pull request using the caller's observed head OID.
///
/// The supplied installation token must have Pull requests: write access. This
/// function performs one mutation and does not query or retry with a newer head.
/// A [`RebaseOutcome::StaleHead`] means the caller must recompute repository
/// state before it attempts another mutation.
pub async fn rebase_pull_request(
    client: &Client,
    installation_token: &str,
    pull_request_id: &str,
    expected_head_oid: &str,
) -> Result<RebaseOutcome, RebaseError> {
    rebase_pull_request_at(
        client,
        GRAPHQL_URL,
        installation_token,
        pull_request_id,
        expected_head_oid,
    )
    .await
}

async fn rebase_pull_request_at(
    client: &Client,
    graphql_url: &str,
    installation_token: &str,
    pull_request_id: &str,
    expected_head_oid: &str,
) -> Result<RebaseOutcome, RebaseError> {
    if installation_token.is_empty() {
        return Err(RebaseError::InvalidInput(
            "installation token must not be empty",
        ));
    }
    if pull_request_id.is_empty() {
        return Err(RebaseError::InvalidInput(
            "pull request ID must not be empty",
        ));
    }
    if expected_head_oid.is_empty() {
        return Err(RebaseError::InvalidInput(
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
                    "updateMethod": "REBASE"
                }
            }
        }))
        .send()
        .await
        .map_err(|error| RebaseError::Request(error.to_string()))?;

    let response = ensure_success(response, installation_token).await?;
    let body: GraphQlResponse = response.json().await.map_err(|error| {
        RebaseError::InvalidResponse(format!("could not decode response body: {error}"))
    })?;
    let messages = body
        .errors
        .into_iter()
        .map(|error| error.message.replace(installation_token, "[REDACTED]"))
        .collect::<Vec<_>>();

    if !messages.is_empty() {
        if let Some(outcome) = expected_outcome(&messages) {
            return Ok(outcome);
        }
        return Err(RebaseError::GitHub(messages));
    }

    let pull_request = body
        .data
        .as_ref()
        .and_then(|data| data.get("updatePullRequestBranch"))
        .and_then(|mutation| mutation.get("pullRequest"));
    if pull_request.is_some_and(Value::is_object) {
        Ok(RebaseOutcome::Rebased)
    } else {
        Err(RebaseError::InvalidResponse(
            "updatePullRequestBranch returned no pull request".to_owned(),
        ))
    }
}

fn expected_outcome(messages: &[String]) -> Option<RebaseOutcome> {
    if messages.is_empty() {
        return None;
    }

    let classifications = messages
        .iter()
        .map(|message| {
            if is_stale_head_message(message) {
                Some(ExpectedFailure::StaleHead)
            } else if message.to_ascii_lowercase().contains("conflict") {
                Some(ExpectedFailure::Conflict)
            } else {
                None
            }
        })
        .collect::<Option<Vec<_>>>()?;

    let message = messages.join("; ");
    match classifications.first()? {
        ExpectedFailure::StaleHead
            if classifications
                .iter()
                .all(|kind| *kind == ExpectedFailure::StaleHead) =>
        {
            Some(RebaseOutcome::StaleHead { message })
        }
        ExpectedFailure::Conflict
            if classifications
                .iter()
                .all(|kind| *kind == ExpectedFailure::Conflict) =>
        {
            Some(RebaseOutcome::Conflict { message })
        }
        _ => None,
    }
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExpectedFailure {
    StaleHead,
    Conflict,
}

async fn ensure_success(
    response: Response,
    installation_token: &str,
) -> Result<Response, RebaseError> {
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
    Err(RebaseError::Http { status, message })
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
        let result = rebase_pull_request_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
        )
        .await
        .unwrap();

        assert_eq!(result, RebaseOutcome::Rebased);
    }

    #[tokio::test]
    async fn sends_one_rebase_using_the_observed_head_and_reports_stale_head() {
        let (url, requests) = mock_server(
            "200 OK",
            r#"{"data":{"updatePullRequestBranch":null},"errors":[{"message":"The expected head OID does not match the current pull request head"}]}"#,
        );
        let result = rebase_pull_request_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
        )
        .await
        .unwrap();

        assert!(matches!(result, RebaseOutcome::StaleHead { .. }));
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
            r#"{"data":{"updatePullRequestBranch":null},"errors":[{"message":"head sha didn't match the current head ref."}]}"#,
        );
        let result = rebase_pull_request_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
        )
        .await
        .unwrap();

        assert!(matches!(result, RebaseOutcome::StaleHead { .. }));
    }

    #[tokio::test]
    async fn mixed_stale_head_and_unrelated_errors_remain_unexpected() {
        let (url, _) = mock_server(
            "200 OK",
            r#"{"data":{"updatePullRequestBranch":null},"errors":[{"message":"head sha didn't match the current head ref."},{"message":"Resource not accessible by integration"}]}"#,
        );
        let error = rebase_pull_request_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
        )
        .await
        .unwrap_err();

        assert!(matches!(error, RebaseError::GitHub(_)));
    }

    #[tokio::test]
    async fn reports_conflicts_as_an_expected_outcome() {
        let (url, _) = mock_server(
            "200 OK",
            r#"{"data":{"updatePullRequestBranch":null},"errors":[{"message":"The pull request branch has conflicts"}]}"#,
        );
        let result = rebase_pull_request_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
        )
        .await
        .unwrap();

        assert!(matches!(result, RebaseOutcome::Conflict { .. }));
    }

    #[tokio::test]
    async fn reports_unexpected_graphql_errors_as_failures() {
        let (url, _) = mock_server(
            "200 OK",
            r#"{"data":{"updatePullRequestBranch":null},"errors":[{"message":"Resource not accessible by integration"}]}"#,
        );
        let error = rebase_pull_request_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
        )
        .await
        .unwrap_err();

        assert!(matches!(error, RebaseError::GitHub(_)));
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
        let error = rebase_pull_request_at(
            &Client::new(),
            &url,
            "installation-token",
            "PR_kwDO123",
            "observed-head-oid",
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            RebaseError::Http {
                status: StatusCode::FORBIDDEN,
                ..
            }
        ));
    }

    #[test]
    fn does_not_classify_mixed_expected_and_unexpected_errors_as_expected() {
        let messages = [
            "The expected head OID does not match".to_owned(),
            "Resource not accessible by integration".to_owned(),
        ];

        assert!(expected_outcome(&messages).is_none());
    }
}
