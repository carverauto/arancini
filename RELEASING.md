# Releasing Arancini Crates

## One-time setup

1. Create a crates.io API token with publish scope.
2. Add it to the Forgejo repository or organization Actions secrets as
   `CARGO_REGISTRY_TOKEN`.

## Publish flow

1. Set the root `Cargo.toml` workspace version and Arancini's `arancini-lib`
   dependency version to the target release version.
2. Commit and push changes.
3. After the release commit is merged, run Forgejo Actions workflow
   `Publish Crates` on that exact merged revision with `dry_run=true`.
4. Create and push the corresponding version tag. The tag is the sole real
   publish trigger; do not also dispatch the workflow with `dry_run=false`.

The workflow publishes in order:
1. `arancini-lib`
2. `arancini`

This order is required because `arancini` depends on `arancini-lib`.

## Tag-based publish

Pushing a version tag such as `v0.7.3` also triggers the same workflow and
performs a real publish.
