# Grafana Plugin Workflows

These workflow files define the CI/CD logic for the Grafana plugin. They are **reusable workflows** that must be called from the repository root's `.github/workflows/` directory.

## Why This Structure?

GitHub Actions only executes workflows from the repository's root `.github/workflows/` directory. To keep the plugin's CI configuration close to its source code while still being executable, we use a **reusable workflow** pattern:

1. **Source of Truth** (this directory): Contains the reusable workflow definitions with `workflow_call` triggers
2. **Executable Wrappers** (root `.github/workflows/`): Thin wrappers that define triggers (push, PR, schedule) and call the reusable workflows

## Workflow Files

| File | Purpose | Root Wrapper |
|------|---------|--------------|
| `ci.yml` | Build, lint, test, E2E | `grafana-plugin-ci.yml` |
| `bundle-stats.yml` | Bundle size comparison | `grafana-plugin-bundle-stats.yml` |
| `cp-update.yml` | Create plugin updates (monthly) | `grafana-plugin-cp-update.yml` |
| `is-compatible.yml` | Grafana API compatibility check | `grafana-plugin-is-compatible.yml` |

## Syncing Changes

When you modify workflows in this directory, copy them to the root:

```bash
# From repository root
cp src/grafana-plugin/.github/workflows/ci.yml .github/workflows/grafana-plugin-ci-reusable.yml
cp src/grafana-plugin/.github/workflows/bundle-stats.yml .github/workflows/grafana-plugin-bundle-stats-reusable.yml
cp src/grafana-plugin/.github/workflows/cp-update.yml .github/workflows/grafana-plugin-cp-update-reusable.yml
cp src/grafana-plugin/.github/workflows/is-compatible.yml .github/workflows/grafana-plugin-is-compatible-reusable.yml
```

## Release

Plugin releases are handled by `release-please` in the root workflow. When changes to `src/grafana-plugin/` are merged with conventional commits, release-please will:
1. Create a release PR with changelog
2. On merge, create a `grafana-plugin-v*` tag
3. Trigger the release jobs to build and publish the plugin
