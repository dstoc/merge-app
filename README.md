# merge-app

`merge-app` is a Rust service foundation for the GitHub App described in
[`docs/merge-app-proposal.md`](docs/merge-app-proposal.md).

## Configuration

Create `merge-app.toml` in the working directory:

```toml
reconcile_interval = "5m"
allowed_accounts = [1234567, 7654321]
```

`reconcile_interval` defaults to `5m`. Set `MERGE_APP_CONFIG` to use a different
configuration file. Supply the App ID and private key path through the
environment:

```sh
export GITHUB_APP_ID=123456
export GITHUB_APP_PRIVATE_KEY_PATH=/path/to/private-key.pem
cargo run
```

The service reads the private key file at startup. It writes structured JSON
logs to standard error and exits cleanly on SIGINT or SIGTERM. Set `RUST_LOG` to
change the log filter.

This phase starts the service lifecycle and reconciliation timer. It does not
make GitHub API calls or start an HTTP server.

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
