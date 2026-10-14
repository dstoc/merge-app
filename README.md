# merge-app

`merge-app` is a Rust service foundation for the GitHub App described in
[`docs/merge-app-proposal.md`](docs/merge-app-proposal.md).

## Configuration

Create `merge-app.toml` in the working directory:

```toml
reconcile_interval = "5m"
webhook_listen_addr = "127.0.0.1:8080"
allowed_accounts = [1234567, 7654321]
```

`reconcile_interval` defaults to `5m`. Set `MERGE_APP_CONFIG` to use a different
configuration file. Supply the App ID and private key path through the
environment:

```sh
export GITHUB_APP_ID=123456
export GITHUB_APP_PRIVATE_KEY_PATH=/path/to/private-key.pem
export GITHUB_WEBHOOK_SECRET='shared secret configured in the GitHub App'
cargo run
```

The service reads the private key file and webhook secret at startup. It serves
`POST /github/webhook` at `webhook_listen_addr`, which defaults to
`127.0.0.1:8080`. Set the address in `merge-app.toml` to match the local network
and ingress setup. The service verifies the GitHub HMAC signature and rejects
bodies larger than 1,000,000 bytes before it parses an event. It writes
structured JSON logs to standard error and exits cleanly on SIGINT or SIGTERM.
Set `RUST_LOG` to change the log filter.

The service verifies its App credentials, then reconciles immediately and every
`reconcile_interval`. Each pass lists App installations, keeps only accounts in
`allowed_accounts`, lists their repositories, and marks each repository dirty
through the work coordinator. Repository workers use repository-scoped
installation tokens to run the PR queue. Failures for one installation are
logged and do not stop reconciliation of the other allowlisted installations.

Configure the GitHub App webhook subscription for `installation`,
`installation_repositories`, `pull_request`, and `pull_request_review`. Installation
events request an immediate reconciliation. Pull request events schedule repository
work for `opened`, `auto_merge_enabled`, `auto_merge_disabled`, `synchronize`,
`converted_to_draft`, `ready_for_review`, `reopened`, and `closed` actions. Review
events schedule work for `submitted` and `dismissed` actions. Other pull request and
review actions are acknowledged without scheduling work. Webhook payloads only
signal that state changed; workers query GitHub for current queue state before they
advance it. Periodic reconciliation remains the recovery path for missed webhook
deliveries.

## Manual Phase 0 validation

The `p0-validate` binary runs the remaining Phase 0 checks against a GitHub App
installation. It reads the private key from a local PEM file. It does not print
the private key, App JWT, or installation token.

Configure the test App with only **Pull requests: read/write**. Install it on a
test repository. The tool verifies that the App and the minted installation
token have only that permission, plus GitHub's implicit **Metadata: read**.
The tool finds the installation for the selected repository. Supply the App ID,
test repository, and a suitable test PR. Use the App Client ID as the JWT issuer
when required with `--issuer`.

Start with the read-only check:

```sh
cargo run --locked --bin p0-validate -- \
  --app-id 5312678 \
  --issuer APP_CLIENT_ID \
  --private-key /secure/path/test-app.pem \
  --owner OWNER \
  --repo REPOSITORY \
  --pr PULL_REQUEST_NUMBER
```

This validates App JWT authentication with `GET /app`, mints an installation
token scoped to the named repository with Pull requests: write, and queries the
PR's state, draft status, review decision, mergeability, merge state, head and
base OIDs, and native auto-merge request. It prints a JSON evidence report to
standard output.

To run the mutation checks, add `--execute-rebase` only after confirming the
read-only report describes the intended test PR:

```sh
cargo run --locked --bin p0-validate -- \
  --app-id 5312678 \
  --issuer APP_CLIENT_ID \
  --private-key /secure/path/test-app.pem \
  --owner OWNER \
  --repo REPOSITORY \
  --pr PULL_REQUEST_NUMBER \
  --execute-rebase
```

Mutation mode requires an open, approved, non-draft, mergeable PR with native
auto-merge enabled and `mergeStateStatus: BEHIND`. It calls GraphQL
`updatePullRequestBranch` with `updateMethod: REBASE` and the observed head OID.
After GitHub updates the branch, the tool checks that native auto-merge remains
enabled. Before the successful rebase, it sends an update request with the
observed base OID as a deliberately mismatched `expectedHeadOid`. It checks that
GitHub reports an expected-head mismatch and leaves the PR unchanged while it is
still behind. The successful update then uses the observed PR head OID. The tool
waits up to 120 seconds for the rebase to appear in the PR query.

Use a disposable test PR with a required check that is still pending. Rebase
updates the PR branch and GitHub native auto-merge remains in control of the
final merge. Do not run mutation mode against a production PR.

If GitHub rejects the rebase with these permissions, keep the permission set
unchanged and save the JSON report and error for maintainer review. Do not add
Contents: write. If auto-merge is disabled, report that result as well.

## Pull request rebase API

The library exposes `merge_app::pull_request::rebase_pull_request`. Pass the
pull request's GraphQL node ID and the head OID from the current repository
snapshot. The function sends one `updatePullRequestBranch` mutation with
`updateMethod: REBASE` and that `expectedHeadOid`.

The function returns `RebaseOutcome::StaleHead` when the head OID is stale and
`RebaseOutcome::Conflict` when GitHub reports merge conflicts. Recompute
repository state before taking another action after a stale-head result. Other
GitHub and API failures return `RebaseError`.
