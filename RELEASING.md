# Releasing Arancini Crates

Collaboration is on GitHub: https://github.com/carverauto/arancini

## One-time setup

1. Create a crates.io API token with publish scope.
2. Add it to the GitHub repository `main` environment as `CARGO_REGISTRY_TOKEN`.
3. For tagged container publishes, set `HARBOR_ROBOT_USERNAME` and
   `HARBOR_ROBOT_SECRET` on the GitHub repository or `carverauto` org.

## Publish flow

1. Set the root `Cargo.toml` workspace version and Arancini's `arancini-lib`
   dependency version to the target release version.
2. Open a pull request against `main` and merge it.
3. After the release commit is on `main`, run the GitHub Actions workflow
   `Publish Crates` on that revision with `dry_run=true`.
4. Create and push the corresponding version tag. The tag is the sole real
   publish trigger; do not also dispatch the workflow with `dry_run=false`.

The workflow publishes in order:
1. `arancini-lib`
2. `arancini`

This order is required because `arancini` depends on `arancini-lib`.

Tagged `v*` pushes also build and push
`registry.carverauto.dev/serviceradar/arancini`.

## Tag-based publish

Pushing a version tag such as `v0.7.6` triggers crate publish and the container
build.
