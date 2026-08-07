## Why

SignalDB's crates are versioned fully independently via release-please's `cargo-workspace` plugin, driven by conventional commits scoped by path. That plugin only cascades a patch bump to a dependent when convenient — it does not guarantee equality. Today `acceptor`/`common`/`compactor`/`querier`/`writer` sit at `0.2.1` while `router` drifted to `0.2.2` on its own, and `signaldb-bin` — the monolithic binary that ships as "SignalDB" and depends on every one of those crates — is stuck at `0.1.3`, on a completely different line. There is no number today that means "the SignalDB release."

## What Changes

- Add a `linked-versions` release-please plugin group (name: `signaldb-core`) covering `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`. Whenever any member bumps, all members release at the same, highest resulting version.
- Set `"merge": false` on the existing `cargo-workspace` plugin for these packages, per release-please's documented guidance for combining `cargo-workspace` with `linked-versions` (avoids the two plugins double-managing the same packages).
- Remove `signal-producer` from incidental lockstep — it currently sits in the `0.2.x` cluster purely because `cargo-workspace` cascaded it as a dependent of `common`; it is a dev/test data generator, not a shipped runtime component, and should version independently going forward.
- `.release-please-manifest.json` keeps every grouped package at its real, actually-tagged last-released version (`acceptor`/`common`/`compactor`/`querier`/`writer` at `0.2.1`, `signaldb-bin`/`signaldb-cli` at `0.1.3`, `router` at `0.2.2`) — **not** pre-written to `0.2.2` for packages that were never tagged there. See design.md for why: the manifest is release-please's source of truth for finding each package's previous-release tag to compute the next diff/changelog boundary; writing a version with no matching tag breaks that lookup.
- **BREAKING** (release-visible, not code-breaking): the first time any linked-group member gets a real commit after this lands, `linked-versions` will compute each member's true next version from its real baseline, take the max across the group, and align all 8 to it in one release PR — `signaldb-bin`/`signaldb-cli` will jump from the `0.1.x` line to `0.2.x`+ with no corresponding new features, pure harmonization. Call this out explicitly in that release's notes so it doesn't read as unexplained scope. This happens on the next organic release-please cycle, not immediately on merging this config change.
- No changes to crates outside the group: `tempo-api`, `loki-api`, `prometheus-api`, `pyroscope-api`, `signaldb-sdk`, `signaldb-api`, `mcp-server`, `ui`, `grafana-plugin` keep independent versioning exactly as today.
- **Follow-up, discovered post-merge**: the first real release-please PR generated under this config (#841) showed 7 of the 8 grouped packages aligning correctly, but `signaldb-bin` didn't — an upstream release-please bug (`googleapis/release-please#1750`) where `linked-versions` doesn't sync a package that has `include-component-in-tag: false`, which only `signaldb-bin` used among the group. Fix: split the unprefixed-tag "project release" identity out of `signaldb-bin` onto a new repo-root `.` package (`release-type: simple`, backed by a root `version.txt`), which joins the linked group in `signaldb-bin`'s place; `signaldb-bin` itself flips to `include-component-in-tag: true` and tags like its 7 siblings (`signaldb-bin-vX.Y.Z`), sidestepping the bug entirely. `.github/workflows/release-please.yml`'s `release_created`/`tag_name` outputs (which feed Docker image tagging and GitHub Releases) move from `signaldb-bin`'s outputs to the root package's — see design.md's Decisions/Migration Plan for the full rationale, including why flipping `signaldb-bin`'s tag format directly was rejected (breaks Docker's `type=semver` tag parsing).

## Capabilities

This is a release-tooling/CI configuration change only (`release-please-config.json`, `.release-please-manifest.json`). It changes no OTLP ingest, query, storage, or API behavior — no runtime capability is added or modified, so this change sets `skip_specs: true` and carries no spec deltas.

### New Capabilities

None.

### Modified Capabilities

None.

## Impact

- `release-please-config.json`: add `linked-versions` plugin config; adjust `cargo-workspace` plugin config (`merge: false` scope); add a `"."` root package (`release-type: simple`); move `signaldb-bin` out of / add `.` into the `linked-versions` group; flip `signaldb-bin`'s `include-component-in-tag` to `true`.
- `.release-please-manifest.json`: no version values changed for the 7 remaining directly-grouped packages; `"."` gets a real, tag-matched bootstrap value (`0.1.3`). `signal-producer` keeps tracking its own version independently (no manifest change).
- New file: root `version.txt` (release-please's `simple` release-type update target for the `.` package).
- `.github/workflows/release-please.yml`: `release_created`/`tag_name` top-level outputs repointed from `signaldb-bin`'s to the root `.` package's (per release-please-action's convention, root-package outputs are unprefixed) — everything downstream that consumes these (Docker image tagging, GitHub Release creation) keeps working unchanged since the unprefixed `vX.Y.Z` tag shape is preserved, just sourced from a different package.
- No changes to application source code in `src/*` crates — version fields and release-tooling config only.
- Affected workspace crates (version field edits at release time, no logic changes): `.` (new), `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`.
