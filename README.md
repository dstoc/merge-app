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
