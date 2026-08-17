## 1. Configure release-please

- [x] 1.1 In `release-please-config.json`, add a `linked-versions` plugin entry: group name `signaldb-core`, components `src/signaldb-bin`, `src/signaldb-cli`, `src/acceptor`, `src/router`, `src/writer`, `src/querier`, `src/compactor`, `src/common`.
- [x] 1.2 In `release-please-config.json`, scope the existing `cargo-workspace` plugin to `"merge": false` for the same 8 packages, per design.md's plugin-interaction decision.
- [x] 1.3 ~~In `.release-please-manifest.json`, set all 8 grouped package entries to `0.2.2` (current group maximum).~~ **Corrected in review (CodeRabbit)**: verified against real repo tags that only `router-v0.2.2` actually exists — `acceptor-v0.2.2`/`common-v0.2.2`/`compactor-v0.2.2`/`querier-v0.2.2`/`writer-v0.2.2`/`signaldb-cli-v0.2.2`/`v0.2.2` do not. Writing `0.2.2` for those 7 would point release-please's previous-release tag lookup at tags that don't exist. Manifest now left untouched — every grouped entry stays at its real last-tagged version; `linked-versions` computes the actual alignment on the next real commit to any group member instead. See design.md's updated Decisions/Migration Plan.
- [x] 1.4 Confirm `signal-producer`'s manifest/config entries are untouched (stays independently versioned, not added to the linked group).

## 2. Verify workflow compatibility

- [x] 2.1 Re-read `.github/workflows/release-please.yml`'s use of per-component `steps.release.outputs[...]` keys (e.g. `src/signaldb-bin--release_created`, `src/signaldb-bin--tag_name`) and confirm against release-please's plugin docs that linked components still emit these per-original-path outputs. **Confirmed**: outputs are keyed by manifest _path_ (`src/<pkg>--release_created`), which the `linked-versions`/`cargo-workspace` plugins don't change — plugins only influence which version number a path's package entry gets, not whether that path still produces its own release/PR/output entry. A real-world combined `cargo-workspace(merge:false)` + `linked-versions` config still lists each package by its own path. Confidence is high but not 100% (no live release-please run available to test against in this environment) — final confirmation happens naturally at task 3.2/3.3 against a real generated PR.
- [x] 2.2 If outputs are affected, update the workflow's `outputs:` block and any downstream `if:`/`needs.release-please.outputs.*` references accordingly. **Not applicable** — per 2.1, outputs are not expected to be affected; no workflow changes made.

## 3. Validate against a real release-please run

- [x] 3.1 Merge the config/manifest change to `main`. Merged via PR #1043 (`2efecc39`), plus a same-day follow-up fix commit (`ad18a5d5`) for the CodeRabbit-caught manifest issue.
- [x] 3.2 Inspect the next auto-generated release-please PR: confirm all 8 linked packages show the identical target version, confirm `signal-producer` and all independent packages (tempo-api, loki-api, prometheus-api, pyroscope-api, signaldb-sdk, signaldb-api, mcp-server, ui, grafana-plugin) are unaffected. **Partial pass, bug found**: PR #841 updated within a minute of merge. `acceptor`/`common`/`compactor`/`querier`/`router`/`writer` and `signaldb-cli` all aligned correctly to `0.3.0`; `signal-producer` and every independent package moved on their own, unaffected — but `signaldb-bin` diverged (`0.1.4`, not `0.3.0`). Root cause investigated and fixed — see section 5 below.
- [x] 3.3 Confirm the PR's diff touches only the expected `Cargo.toml`/`CHANGELOG.md`/manifest files for the 8 linked packages plus whatever independent package(s) actually had commits. **Verified on the merged PR #841 (`7d2dce06`)**: all 8 linked packages at `0.3.0` (`signaldb-bin` included, tag `signaldb-bin-v0.3.0`); files touched are `Cargo.toml`+`CHANGELOG.md` for the linked and the independently-bumped packages, `Cargo.lock`, `.release-please-manifest.json`, `otel/registry/manifest.yaml` (common's `extra-files` entry) and `tests-integration/Cargo.toml` (cargo-workspace dependent bump). Nothing unexpected.
- [x] 3.4 Merge that release-please PR and confirm `build-release`/`build-musl-*` jobs trigger correctly off the resulting tag(s). **Done, with one detour**: run 32064545499 attempt 1 failed at release creation (`body is too long`, see 5.16 below — a release-please overflow bug, not this change); after neutralizing the offending text on the `release-please--branches--main--release-notes` branch and re-running (attempt 2), all 19 configured releases were created and `build-release`, `build-musl-{amd64,arm64}`, Docker and grafana-plugin jobs all triggered off `src/signaldb-bin--release_created`/`grafana-plugin--release_created`. Only `Package and Release Grafana Plugin` failed (Grafana's `plugin-validator-cli` `backendbinary` analyzer rejects a Rust backend — unrelated to versioning; validation made informational in the same wrap-up PR).

## 4. Document

- [x] 4.1 Add a short note to the merged release's notes/CHANGELOG entry (for `signaldb-bin` and `signaldb-cli`) explaining the one-time version-line jump from `0.1.x` to `0.3.0`. Note added under the `0.3.0` heading of `src/signaldb-bin/CHANGELOG.md` and `src/signaldb-cli/CHANGELOG.md`, and to the bodies of GitHub releases `signaldb-bin-v0.3.0` / `signaldb-cli-v0.3.0`.

## 5. Fix signaldb-bin's linked-versions divergence

**Attempt 1 (superseded — root-package split), abandoned before merge:**

- [x] ~~5.1 Create root `version.txt` containing `0.1.3`.~~
- [x] ~~5.2 Add a `"."` package: `release-type: "simple"`, `component: "signaldb"`, `include-component-in-tag: false`.~~
- [x] ~~5.3 Update the `signaldb-core` group's `components`: remove `"signaldb-bin"`, add `"signaldb"`.~~
- [x] ~~5.6 Switch top-level `release_created`/`tag_name` outputs to the root package's unprefixed outputs.~~
- [x] ~~5.7 Add a `version.txt` doc note to README.~~

**Reverted in review** (PR #1047, CodeRabbit): verified against release-please v17.6.0 source that `getComponent()` unconditionally returns `undefined` when `include-component-in-tag` is `false`, and `linked-versions` silently skips any such package — a hard rule, not specific to which package holds the flag. The `.`-package would have hit the identical bug, just relocated, while also removing `signaldb-bin` from the group entirely. All five tasks above reverted (files deleted/restored to pre-attempt state).

**Attempt 2 (this one — keeps `signaldb-bin` a real linked-versions member):**

- [x] 5.10 In `release-please-config.json`, flip `src/signaldb-bin`'s `include-component-in-tag` to `true` (kept from attempt 1 — this part was always correct).
- [x] 5.11 In `release-please-config.json`, keep `"signaldb-bin"` in the `signaldb-core` `linked-versions` group's `components` list (undo the attempt-1 swap).
- [x] 5.12 In `.github/workflows/release-please.yml`, add a `version` output (`steps.release.outputs['src/signaldb-bin--version']`) alongside the unchanged `release_created`/`tag_name` outputs.
- [x] 5.13 In `.github/workflows/release-please.yml`, switch the three Docker `type=semver` tag rules from `needs.release-please.outputs.tag_name` to `needs.release-please.outputs.version`; leave the GitHub Release step's `tag_name` usage unchanged.
- [x] 5.14 Create and push git tag `signaldb-bin-v0.1.3` at the same commit as the existing `v0.1.3` (`dc218d8f...`), bootstrapping the tag lineage under the new `include-component-in-tag: true` convention.
- [x] 5.15 Validate config JSON and workflow YAML are well-formed; commit and push.
- [x] 5.16 Observe the next release-please PR. **Confirmed on PR #841 / run 32064545499**: `signaldb-bin` tagged `signaldb-bin-v0.3.0`, aligned with the other 7 `signaldb-core` members at `0.3.0`; Docker images tagged from the `version` output (`ghcr.io/cedricziel/signaldb:0.3.0-{amd64,arm64}` + multi-arch `0.3.0`/`0.3`); GitHub Release `signaldb-bin: v0.3.0` created with the prefixed `tag_name` and release assets uploaded to it. Side finding, unrelated to this change: release-please's PR-body overflow handler (`release-notes.md` on a side branch, used because the 20-package notes exceeded 65,536 chars) round-trips the notes through an HTML parser, so `&lt;lang&gt;` from commit `92a439e` ("query --<lang>") became a literal `<lang>` element that swallowed every following `<details>` block: `signaldb-ui`'s body grew to 125,812 chars (> GitHub's 125,000 limit) and `router`/`signaldb-cli`/`signaldb-sdk`/`mcp-server` were not found at all. Fixed one-off by rewriting the text on the notes branch and re-running; if a future release PR overflows again, avoid raw `<...>` in commit subjects.
