# Releasing Arancini Crates

## One-time setup

1. Create a crates.io API token with publish scope.
2. Add it to GitHub repository secrets as `CARGO_REGISTRY_TOKEN`.

## Publish flow

1. Ensure `arancini/Cargo.toml` workspace version is set to the target release version.
2. Commit and push changes.
3. Run GitHub Actions workflow `Publish Crates`:
   - First run with `dry_run=true`.
   - Then run with `dry_run=false` to publish.

The workflow publishes in order:
1. `arancini-lib`
2. `arancini`

This order is required because `arancini` depends on `arancini-lib`.

## Tag-based publish

Pushing a tag like `v0.7.1` also triggers the same workflow and performs a real publish.
