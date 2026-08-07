## 1. Configure release-please

- [x] 1.1 In `release-please-config.json`, add a `linked-versions` plugin entry: group name `signaldb-core`, components `src/signaldb-bin`, `src/signaldb-cli`, `src/acceptor`, `src/router`, `src/writer`, `src/querier`, `src/compactor`, `src/common`.
- [x] 1.2 In `release-please-config.json`, scope the existing `cargo-workspace` plugin to `"merge": false` for the same 8 packages, per design.md's plugin-interaction decision.
- [x] 1.3 In `.release-please-manifest.json`, set all 8 grouped package entries to `0.2.2` (current group maximum).
- [x] 1.4 Confirm `signal-producer`'s manifest/config entries are untouched (stays independently versioned, not added to the linked group).

## 2. Verify workflow compatibility

- [x] 2.1 Re-read `.github/workflows/release-please.yml`'s use of per-component `steps.release.outputs[...]` keys (e.g. `src/signaldb-bin--release_created`, `src/signaldb-bin--tag_name`) and confirm against release-please's plugin docs that linked components still emit these per-original-path outputs. **Confirmed**: outputs are keyed by manifest _path_ (`src/<pkg>--release_created`), which the `linked-versions`/`cargo-workspace` plugins don't change — plugins only influence which version number a path's package entry gets, not whether that path still produces its own release/PR/output entry. A real-world combined `cargo-workspace(merge:false)` + `linked-versions` config still lists each package by its own path. Confidence is high but not 100% (no live release-please run available to test against in this environment) — final confirmation happens naturally at task 3.2/3.3 against a real generated PR.
- [x] 2.2 If outputs are affected, update the workflow's `outputs:` block and any downstream `if:`/`needs.release-please.outputs.*` references accordingly. **Not applicable** — per 2.1, outputs are not expected to be affected; no workflow changes made.

## 3. Validate against a real release-please run

- [ ] 3.1 Merge the config/manifest change to `main`.
- [ ] 3.2 Inspect the next auto-generated release-please PR: confirm all 8 linked packages show the identical target version, confirm `signal-producer` and all independent packages (tempo-api, loki-api, prometheus-api, pyroscope-api, signaldb-sdk, signaldb-api, mcp-server, ui, grafana-plugin) are unaffected.
- [ ] 3.3 Confirm the PR's diff touches only the expected `Cargo.toml`/`CHANGELOG.md`/manifest files for the 8 linked packages plus whatever independent package(s) actually had commits.
- [ ] 3.4 Merge that release-please PR and confirm `build-release`/`build-musl-*` jobs trigger correctly off the resulting tag(s).

## 4. Document

- [ ] 4.1 Add a short note to the merged release's notes/CHANGELOG entry (for `signaldb-bin` and `signaldb-cli`) explaining the one-time version-line jump from `0.1.x` to `0.2.x`+, per proposal.md's BREAKING callout.
