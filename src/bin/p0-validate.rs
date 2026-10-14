use std::{collections::BTreeMap, env, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use merge_app::auth::GitHubAppAuthenticator;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const GRAPHQL_URL: &str = "https://api.github.com/graphql";

const HELP: &str = "Usage: p0-validate --app-id ID --private-key PEM --owner OWNER --repo REPO --pr NUMBER [--issuer ID_OR_CLIENT_ID] [--execute-rebase]\n\n\
Runs GitHub App authentication, installation-token, and pull-request read checks.\n\
By default this command is read-only. --execute-rebase probes a mismatched\n\
expectedHeadOid, then updates an eligible pull request with REBASE and checks\n\
that native auto-merge remains enabled. Use a disposable test PR.\n\
The private key is read from the PEM path and credentials are never printed.\n";

#[derive(Debug)]
struct Args {
    app_id: u64,
    issuer: String,
    private_key: PathBuf,
    owner: String,
    repo: String,
    pr_number: u64,
    execute_rebase: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PullRequest {
    id: String,
    number: u64,
    state: String,
    is_draft: bool,
    review_decision: Option<String>,
    mergeable: String,
    merge_state_status: String,
    head_ref_oid: String,
    base_ref_oid: String,
    auto_merge_request: Option<AutoMergeRequest>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct AutoMergeRequest {
    enabled_at: String,
    merge_method: String,
}

#[derive(Debug)]
struct GraphQlResult {
    data: Value,
    errors: Vec<String>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("p0-validate: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = parse_args(env::args().skip(1))?;
    let pem = tokio::fs::read_to_string(&args.private_key)
        .await
        .with_context(|| {
            format!(
                "could not read private key file {}",
                args.private_key.display()
            )
        })?;
    let authenticator =
        GitHubAppAuthenticator::new(args.app_id, args.issuer.clone(), pem.as_bytes())?;
    let client = authenticator.http_client();

    let app = authenticator.verify_app().await?;
    validate_permissions(&app.permissions, "GitHub App")?;

    let installation_id = authenticator
        .repository_installation(&args.owner, &args.repo)
        .await?;

    // Ask GitHub to mint the narrowest token needed for this spike. GitHub also
    // returns the effective permissions, which are checked before any PR calls.
    let installation_token = authenticator
        .installation_token(installation_id, &args.owner, &args.repo)
        .await?;
    validate_permissions(&installation_token.permissions, "installation token")?;

    let initial = query_pull_request(&client, &installation_token.token, &args).await?;
    let mut evidence = json!({
        "app_authentication": {
            "get_app_succeeded": true,
            "app_id": app.id,
            "app_name": app.name,
            "permissions": app.permissions,
        },
        "installation_lookup": {
            "succeeded": true,
            "installation_id": installation_token.installation_id,
            "repository": format!("{}/{}", args.owner, args.repo),
        },
        "installation_token": {
            "minted": true,
            "expires_at": installation_token.expires_at,
            "repository_scope": format!("{}/{}", args.owner, args.repo),
            "permissions": installation_token.permissions,
        },
        "pull_request_query": {
            "succeeded": true,
            "repository": format!("{}/{}", args.owner, args.repo),
            "pull_request": initial,
        },
        "rebase_validation": if args.execute_rebase {
            json!({"requested": true, "completed": false})
        } else {
            json!({"requested": false, "completed": false, "reason": "read-only mode"})
        },
    });

    if args.execute_rebase {
        let readiness = validate_rebase_target(&initial);
        if let Err(error) = readiness {
            evidence["rebase_validation"]["reason"] = json!(format!("{error:#}"));
            print_evidence(&evidence)?;
            return Err(error);
        }

        // Send a valid but deliberately non-matching OID while the PR is still
        // behind. This tests the compare-and-swap guard before a successful
        // update can make the PR no longer eligible for a branch update.
        let stale_probe = update_pull_request_branch_raw(
            &client,
            &installation_token.token,
            &initial.id,
            &initial.base_ref_oid,
        )
        .await;
        let fresh = query_pull_request(&client, &installation_token.token, &args).await;
        let (stale_errors, fresh) = match (stale_probe, fresh) {
            (Ok(result), Ok(fresh)) => (result.errors, fresh),
            (Err(error), _) => {
                evidence["rebase_validation"]["stale_head_probe_error"] =
                    json!(format!("{error:#}"));
                print_evidence(&evidence)?;
                return Err(error);
            }
            (_, Err(error)) => {
                evidence["rebase_validation"]["post_probe_query_error"] =
                    json!(format!("{error:#}"));
                print_evidence(&evidence)?;
                return Err(error);
            }
        };
        let expected_head_rejected = is_expected_head_mismatch(&stale_errors);
        let head_unchanged = fresh.head_ref_oid == initial.head_ref_oid;
        let still_behind = fresh.merge_state_status == "BEHIND";
        let automerge_preserved = fresh.auto_merge_request.is_some();
        let stale_probe_evidence = json!({
            "mismatched_expected_head_oid": initial.base_ref_oid,
            "rejected_as_head_mismatch": expected_head_rejected,
            "errors": stale_errors,
            "head_unchanged": head_unchanged,
            "still_behind": still_behind,
            "auto_merge_preserved": automerge_preserved,
            "pull_request_after_probe": fresh,
        });
        evidence["rebase_validation"]["stale_expected_head_probe"] = stale_probe_evidence.clone();

        if !expected_head_rejected || !head_unchanged || !still_behind || !automerge_preserved {
            print_evidence(&evidence)?;
            bail!(
                "the deliberately mismatched expectedHeadOid probe did not reject as a head mismatch while leaving the PR unchanged"
            );
        }

        let rebase = update_pull_request_branch(
            &client,
            &installation_token.token,
            &initial.id,
            &initial.head_ref_oid,
        )
        .await;
        if let Err(error) = rebase {
            evidence["rebase_validation"]["rebase_mutation_error"] = json!(format!("{error:#}"));
            print_evidence(&evidence)?;
            return Err(error);
        }

        let after_rebase = wait_for_head_change(
            &client,
            &installation_token.token,
            &args,
            &initial.head_ref_oid,
        )
        .await;
        let after_rebase = match after_rebase {
            Ok(snapshot) => snapshot,
            Err(error) => {
                evidence["rebase_validation"]["rebase_mutation_succeeded"] = json!(true);
                evidence["rebase_validation"]["post_rebase_query_error"] =
                    json!(format!("{error:#}"));
                print_evidence(&evidence)?;
                return Err(error);
            }
        };

        let automerge_preserved = after_rebase.auto_merge_request.is_some();
        evidence["rebase_validation"] = json!({
            "requested": true,
            "completed": false,
            "rebase_mutation_succeeded": true,
            "update_method": "REBASE",
            "expected_head_oid": initial.head_ref_oid,
            "head_oid_after_rebase": after_rebase.head_ref_oid,
            "head_changed": after_rebase.head_ref_oid != initial.head_ref_oid,
            "auto_merge_preserved": automerge_preserved,
            "stale_expected_head_probe": stale_probe_evidence,
            "pull_request_after_rebase": after_rebase,
        });

        if !automerge_preserved {
            print_evidence(&evidence)?;
            bail!("GitHub completed the rebase but native auto-merge is no longer enabled");
        }

        evidence["rebase_validation"]["completed"] = json!(true);
    }

    print_evidence(&evidence)
}

async fn query_pull_request(client: &Client, token: &str, args: &Args) -> Result<PullRequest> {
    const QUERY: &str = "query($owner: String!, $repo: String!, $number: Int!) { repository(owner: $owner, name: $repo) { pullRequest(number: $number) { id number state isDraft reviewDecision mergeable mergeStateStatus headRefOid baseRefOid autoMergeRequest { enabledAt mergeMethod } } } }";
    let number = i32::try_from(args.pr_number).context("PR number is too large")?;
    let response = graphql_post(
        client,
        token,
        QUERY,
        json!({"owner": args.owner, "repo": args.repo, "number": number}),
    )
    .await?;
    ensure!(
        response.errors.is_empty(),
        "pull request query failed: {}",
        response.errors.join("; ")
    );
    let pull_request = &response.data["repository"]["pullRequest"];
    ensure!(
        !pull_request.is_null(),
        "GitHub did not return the requested pull request"
    );
    serde_json::from_value(pull_request.clone()).context("could not decode pull request snapshot")
}

async fn update_pull_request_branch(
    client: &Client,
    token: &str,
    pull_request_id: &str,
    expected_head_oid: &str,
) -> Result<()> {
    let response =
        update_pull_request_branch_raw(client, token, pull_request_id, expected_head_oid).await?;
    ensure!(
        response.errors.is_empty(),
        "updatePullRequestBranch(REBASE) failed: {}",
        response.errors.join("; ")
    );
    ensure!(
        !response.data["updatePullRequestBranch"].is_null(),
        "updatePullRequestBranch(REBASE) returned no pull request"
    );
    Ok(())
}

async fn update_pull_request_branch_raw(
    client: &Client,
    token: &str,
    pull_request_id: &str,
    expected_head_oid: &str,
) -> Result<GraphQlResult> {
    const MUTATION: &str = "mutation($input: UpdatePullRequestBranchInput!) { updatePullRequestBranch(input: $input) { pullRequest { id number state isDraft reviewDecision mergeable mergeStateStatus headRefOid baseRefOid autoMergeRequest { enabledAt mergeMethod } } } }";
    graphql_post(
        client,
        token,
        MUTATION,
        json!({
            "input": {
                "pullRequestId": pull_request_id,
                "expectedHeadOid": expected_head_oid,
                "updateMethod": "REBASE"
            }
        }),
    )
    .await
}

async fn graphql_post(
    client: &Client,
    token: &str,
    query: &str,
    variables: Value,
) -> Result<GraphQlResult> {
    let response = client
        .post(GRAPHQL_URL)
        .bearer_auth(token)
        .header("Accept", "application/vnd.github+json")
        .json(&json!({"query": query, "variables": variables}))
        .send()
        .await
        .context("GitHub GraphQL request failed")?;
    let response = ensure_success(response, token).await?;
    let body: Value = response
        .json()
        .await
        .context("could not decode GraphQL response")?;
    let errors = body["errors"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|error| {
            error["message"]
                .as_str()
                .map(|message| message.replace(token, "[REDACTED]"))
        })
        .collect();
    Ok(GraphQlResult {
        data: body["data"].clone(),
        errors,
    })
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
        bail!("GitHub returned HTTP {status}");
    }
    bail!("GitHub returned HTTP {status}: {message}");
}

async fn wait_for_head_change(
    client: &Client,
    token: &str,
    args: &Args,
    original_head: &str,
) -> Result<PullRequest> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let snapshot = query_pull_request(client, token, args).await?;
        if snapshot.head_ref_oid != original_head {
            return Ok(snapshot);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "GitHub accepted the REBASE mutation, but the head OID did not change within 120 seconds; stale-head validation was not run"
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn validate_rebase_target(pull_request: &PullRequest) -> Result<()> {
    ensure!(
        pull_request.state == "OPEN",
        "the pull request must be open"
    );
    ensure!(
        !pull_request.is_draft,
        "the pull request must not be a draft"
    );
    ensure!(
        pull_request.review_decision.as_deref() == Some("APPROVED"),
        "the pull request must have reviewDecision APPROVED"
    );
    ensure!(
        pull_request.auto_merge_request.is_some(),
        "native auto-merge must already be enabled on the pull request"
    );
    ensure!(
        pull_request.mergeable == "MERGEABLE",
        "the pull request must have mergeable state MERGEABLE"
    );
    ensure!(
        pull_request.merge_state_status == "BEHIND",
        "the pull request must have mergeStateStatus BEHIND"
    );
    ensure!(
        pull_request.head_ref_oid != pull_request.base_ref_oid,
        "the PR head and base OIDs must differ for the expectedHeadOid mismatch probe"
    );
    Ok(())
}

fn is_expected_head_mismatch(errors: &[String]) -> bool {
    let message = errors.join(" ").to_ascii_lowercase();
    !errors.is_empty()
        && message.contains("expected")
        && (message.contains("head") || message.contains("oid"))
}

fn validate_permissions(permissions: &BTreeMap<String, String>, source: &str) -> Result<()> {
    ensure!(
        permissions.get("pull_requests").map(String::as_str) == Some("write"),
        "{source} must have Pull requests: write; received permissions: {}",
        format_permissions(permissions)
    );
    let unexpected = permissions
        .iter()
        .filter(|(name, access)| {
            !(name.as_str() == "metadata" && access.as_str() == "read")
                && !(name.as_str() == "pull_requests" && access.as_str() == "write")
        })
        .map(|(name, access)| format!("{name}: {access}"))
        .collect::<Vec<_>>();
    ensure!(
        unexpected.is_empty(),
        "{source} has permissions outside Metadata: read and Pull requests: write: {}",
        unexpected.join(", ")
    );
    Ok(())
}

fn format_permissions(permissions: &BTreeMap<String, String>) -> String {
    if permissions.is_empty() {
        return "none".to_owned();
    }
    permissions
        .iter()
        .map(|(name, access)| format!("{name}: {access}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn print_evidence(evidence: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(evidence)?);
    Ok(())
}

fn parse_args(arguments: impl Iterator<Item = String>) -> Result<Args> {
    let mut values = BTreeMap::<String, String>::new();
    let mut execute_rebase = false;
    let mut arguments = arguments.peekable();
    while let Some(argument) = arguments.next() {
        if argument == "--help" || argument == "-h" {
            println!("{HELP}");
            std::process::exit(0);
        }
        if argument == "--execute-rebase" {
            ensure!(
                !execute_rebase,
                "--execute-rebase was specified more than once"
            );
            execute_rebase = true;
            continue;
        }
        ensure!(
            argument.starts_with("--"),
            "unexpected argument: {argument}"
        );
        let value = arguments
            .next()
            .with_context(|| format!("missing value for {argument}"))?;
        ensure!(!value.starts_with("--"), "missing value for {argument}");
        ensure!(
            matches!(
                argument.as_str(),
                "--app-id" | "--issuer" | "--private-key" | "--owner" | "--repo" | "--pr"
            ),
            "unknown option: {argument}"
        );
        ensure!(
            values.insert(argument, value).is_none(),
            "an option was specified more than once"
        );
    }

    let app_id = required_positive_integer(&values, "--app-id")?;
    let issuer = values
        .get("--issuer")
        .cloned()
        .unwrap_or_else(|| app_id.to_string());
    let private_key = PathBuf::from(required_value(&values, "--private-key")?);
    let owner = required_value(&values, "--owner")?.to_owned();
    let repo = required_value(&values, "--repo")?.to_owned();
    let pr_number = required_positive_integer(&values, "--pr")?;

    Ok(Args {
        app_id,
        issuer,
        private_key,
        owner,
        repo,
        pr_number,
        execute_rebase,
    })
}

fn required_value<'a>(values: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str> {
    values
        .get(name)
        .map(String::as_str)
        .with_context(|| format!("required option {name} is missing\n\n{HELP}"))
}

fn required_positive_integer(values: &BTreeMap<String, String>, name: &str) -> Result<u64> {
    let value = required_value(values, name)?;
    let integer = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    ensure!(integer > 0, "{name} must be a positive integer");
    Ok(integer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> Result<Args> {
        parse_args(arguments.iter().map(|argument| (*argument).to_owned()))
    }

    #[test]
    fn parse_args_defaults_issuer_to_app_id_and_is_read_only() {
        let args = parse(&[
            "--app-id",
            "5312678",
            "--private-key",
            "/tmp/app.pem",
            "--owner",
            "dstoc",
            "--repo",
            "merge-app",
            "--pr",
            "42",
        ])
        .unwrap();

        assert_eq!(args.issuer, "5312678");
        assert!(!args.execute_rebase);
        assert_eq!(args.pr_number, 42);
    }

    #[test]
    fn parse_args_accepts_client_id_and_explicit_rebase_flag() {
        let args = parse(&[
            "--app-id",
            "5312678",
            "--issuer",
            "Iv23clientid",
            "--private-key",
            "/tmp/app.pem",
            "--owner",
            "dstoc",
            "--repo",
            "merge-app",
            "--pr",
            "42",
            "--execute-rebase",
        ])
        .unwrap();

        assert_eq!(args.issuer, "Iv23clientid");
        assert!(args.execute_rebase);
    }

    #[test]
    fn permission_check_allows_only_implicit_metadata_and_pull_request_write() {
        let permissions = BTreeMap::from([
            ("metadata".to_owned(), "read".to_owned()),
            ("pull_requests".to_owned(), "write".to_owned()),
        ]);

        assert!(validate_permissions(&permissions, "test token").is_ok());
    }

    #[test]
    fn permission_check_rejects_contents_write_and_extra_permissions() {
        let permissions = BTreeMap::from([
            ("contents".to_owned(), "write".to_owned()),
            ("pull_requests".to_owned(), "write".to_owned()),
        ]);

        assert!(validate_permissions(&permissions, "test App").is_err());
    }

    #[test]
    fn stale_head_probe_requires_an_expected_head_mismatch_error() {
        assert!(is_expected_head_mismatch(&[
            "The expected head OID does not match the current pull request head".to_owned()
        ]));
        assert!(!is_expected_head_mismatch(&[
            "Pull request is not behind its base branch".to_owned()
        ]));
    }

    #[test]
    fn rebase_requires_a_suitable_test_pull_request() {
        let pull_request = PullRequest {
            id: "PR_kwDO-test".to_owned(),
            number: 42,
            state: "OPEN".to_owned(),
            is_draft: false,
            review_decision: Some("APPROVED".to_owned()),
            mergeable: "MERGEABLE".to_owned(),
            merge_state_status: "BEHIND".to_owned(),
            head_ref_oid: "head-before".to_owned(),
            base_ref_oid: "base".to_owned(),
            auto_merge_request: Some(AutoMergeRequest {
                enabled_at: "2026-10-14T00:00:00Z".to_owned(),
                merge_method: "SQUASH".to_owned(),
            }),
        };

        assert!(validate_rebase_target(&pull_request).is_ok());

        let mut ineligible = pull_request;
        ineligible.merge_state_status = "CLEAN".to_owned();
        assert!(validate_rebase_target(&ineligible).is_err());

        let ineligible = PullRequest {
            id: "PR_kwDO-test".to_owned(),
            number: 42,
            state: "OPEN".to_owned(),
            is_draft: false,
            review_decision: Some("APPROVED".to_owned()),
            mergeable: "CONFLICTING".to_owned(),
            merge_state_status: "BEHIND".to_owned(),
            head_ref_oid: "head-before".to_owned(),
            base_ref_oid: "base".to_owned(),
            auto_merge_request: Some(AutoMergeRequest {
                enabled_at: "2026-10-14T00:00:00Z".to_owned(),
                merge_method: "SQUASH".to_owned(),
            }),
        };
        assert!(validate_rebase_target(&ineligible).is_err());
    }
}
