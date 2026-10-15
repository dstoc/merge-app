# merge-app

`merge-app` is a self-hosted GitHub App for operators who want a serialized
pull request merge queue. It selects the oldest eligible pull request and, when
needed, rebases it onto the latest base branch. GitHub remains responsible for
approvals, required checks, branch rules, auto-merge, and the final merge. See
the [proposal](docs/merge-app-proposal.md) for design context.

## Requirements

Create a GitHub App and install it on the repositories to manage. Configure
these repository permissions:

| Permission | Access |
| --- | --- |
| Metadata | Read (GitHub grants this automatically) |
| Contents | Write |
| Pull requests | Write |

Set a webhook secret and subscribe the App to these events:

- `installation`
- `installation_repositories`
- `pull_request`
- `pull_request_review`
- `push`

Route the App's public webhook URL to `POST /github/webhook`. The service
verifies GitHub's HMAC signature and rejects request bodies larger than
1,000,000 bytes. The listener defaults to `127.0.0.1:8080`; configure a public
TLS ingress or reverse proxy to reach it, or set `webhook_listen_addr` to the
address used by your ingress.

Enable GitHub auto-merge on pull requests. Protect each managed base branch with
your required review and CI rules. The queue requires branches to be up to date
before GitHub can merge them. In classic branch protection, enable **Require
branches to be up to date before merging**. In a ruleset, enable the equivalent
strict required checks rule. GitHub then runs required checks on the rebased
branch before auto-merge can complete.

## Build and run

From the repository root, build and run the service with Cargo:

```sh
cargo build --locked
cargo run --locked
```

The service binary is the default Cargo target. `cargo run --locked` starts the
service.

## Configuration

By default, the service reads `merge-app.toml` from its working directory. Set
`MERGE_APP_CONFIG` to use another file. `allowed_accounts` is required and must
list the numeric GitHub user or organization IDs allowed to install the App.
The other fields below are optional:

```toml
allowed_accounts = [1234567, 7654321]
webhook_listen_addr = "127.0.0.1:8080"
reconcile_interval = "1h"
```

The defaults are `127.0.0.1:8080` for the webhook listener and `1h` for
`reconcile_interval`. Use an address reachable from your ingress when the
service runs outside the ingress network namespace.

Set the App credentials in the environment. The private key must be a PEM file
readable by the service process:

```sh
export GITHUB_APP_ID=123456
export GITHUB_APP_PRIVATE_KEY_PATH=/path/to/private-key.pem
export GITHUB_WEBHOOK_SECRET='the secret configured in the GitHub App'
cargo run --locked
```

The service reads the private key and webhook secret at startup. It fails to
start if a required environment variable or the configuration file is missing
or invalid.

## Runtime behavior and operations

The service validates its App credentials and reconciles installations and
repositories at startup. Installation lifecycle webhooks request another full
reconciliation. Pull request, review, and push webhooks are the normal
low-latency queue trigger; they mark an authorized repository for a fresh
GitHub query. The worker reads current pull request and review state before
advancing the queue.

The service retries transient queue states, stale-head races, and GitHub API
failures for the affected repository. A full reconciliation runs every hour by
default as a safety sweep for missed webhook deliveries, interrupted work, and
installation or repository access changes. Configure `reconcile_interval` to
change that interval. The periodic sweep is not the normal queue trigger.

Logs are structured JSON written to standard error. Set `RUST_LOG` to change
the log filter; for example, `RUST_LOG=debug` enables debug-level logs. The
service exits cleanly on `SIGINT` or `SIGTERM`.

## Maintainer documentation

The [manual Phase 0 GitHub App validation procedure](docs/maintainers/phase-0-validation.md)
is for maintainers using a disposable test repository. It is not part of normal
service setup or operation.
