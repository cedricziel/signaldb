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
- [ ] 3.3 Confirm the PR's diff touches only the expected `Cargo.toml`/`CHANGELOG.md`/manifest files for the 8 (now 9, incl. `.`) linked packages plus whatever independent package(s) actually had commits — re-verify after section 5 lands and PR #841 (or its successor) recomputes.
- [ ] 3.4 Merge that release-please PR and confirm `build-release`/`build-musl-*` jobs trigger correctly off the resulting tag(s) — blocked on 3.3.

## 4. Document

- [ ] 4.1 Add a short note to the merged release's notes/CHANGELOG entry (for `signaldb-bin` and `signaldb-cli`) explaining the one-time version-line jump from `0.1.x` to `0.2.x`+/`0.3.x`+, per proposal.md's BREAKING callout.

## 5. Fix signaldb-bin's linked-versions divergence (root-package split)

- [x] 5.1 Create root `version.txt` containing `0.1.3` (matches the existing `v0.1.3` tag).
- [x] 5.2 In `release-please-config.json`, add a `"."` package: `release-type: "simple"`, `component: "signaldb"`, `include-component-in-tag: false`.
- [x] 5.3 In `release-please-config.json`, update the `signaldb-core` `linked-versions` group's `components`: remove `"signaldb-bin"`, add `"signaldb"`.
- [x] 5.4 In `release-please-config.json`, flip `src/signaldb-bin`'s `include-component-in-tag` to `true`.
- [x] 5.5 In `.release-please-manifest.json`, add `"." : "0.1.3"` (replacing the stale, disconnected `"0.1.0"`).
- [x] 5.6 In `.github/workflows/release-please.yml`, switch the top-level `release_created`/`tag_name` outputs from `steps.release.outputs['src/signaldb-bin--release_created']`/`['src/signaldb-bin--tag_name']` to the root package's unprefixed `steps.release.outputs.release_created`/`.tag_name`.
- [x] 5.7 Add a one-line comment to `version.txt` context (README or adjacent doc note) clarifying it's release-please bookkeeping for the aggregate project version, not a build input.
- [x] 5.8 Validate both JSON config files are well-formed; commit and push.
- [ ] 5.9 Observe the next release-please PR: confirm `.` appears, tags unprefixed, aligns with the rest of `signaldb-core`; confirm `signaldb-bin` now tags `signaldb-bin-vX.Y.Z` and also aligns; confirm Docker image tagging / GitHub Release creation still resolve a valid tag from the new output source.
