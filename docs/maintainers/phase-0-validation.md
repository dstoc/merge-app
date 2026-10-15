# Maintainer Guide: Manual Phase 0 Validation

This procedure runs the remaining Phase 0 checks against a GitHub App
installation. It is a one-off maintainer validation, not a normal operator
setup step. Use a disposable test repository and pull request. The `p0-validate`
binary reads the private key from a local PEM file and does not print the
private key, App JWT, or installation token.

Configure the test App with **Contents: read/write** and **Pull requests:
read/write**. GitHub grants **Metadata: read** automatically. Install the App
on the test repository. The validator checks both App-level permissions and
the minted installation token's permissions. It finds the installation for the
selected repository.

Supply the App ID, test repository, and a suitable test pull request. Use the
App Client ID as the JWT issuer when required with `--issuer`.

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
token scoped to the named repository with Contents: write and Pull requests:
write, and queries the pull request's state, draft status, review decision,
mergeability, merge state, head and base OIDs, and native auto-merge request.
It prints a JSON evidence report to standard output.

Run mutation checks only after confirming that the read-only report describes
the intended test pull request. Add `--execute-rebase`:

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

Mutation mode requires an open, approved, non-draft, mergeable pull request
with native auto-merge enabled and `mergeStateStatus: BEHIND`. It calls GraphQL
`updatePullRequestBranch` with `updateMethod: REBASE` and the observed head OID.
After GitHub updates the branch, the tool checks that native auto-merge remains
enabled. Before the successful rebase, it sends an update request with the
observed base OID as a deliberately mismatched `expectedHeadOid`. It checks
that GitHub reports an expected-head mismatch and leaves the pull request
unchanged while it is still behind. The successful update then uses the
observed pull request head OID. The tool waits up to 120 seconds for the rebase
to appear in the pull request query.

Use a disposable test pull request with a required check that is still pending.
Rebase updates its branch, while GitHub native auto-merge remains in control of
the final merge. Do not run mutation mode against a production pull request.

If GitHub rejects the rebase with these permissions, keep the permission set
unchanged and save the JSON report and error for maintainer review. Do not add
other permissions. Report if auto-merge is disabled.
