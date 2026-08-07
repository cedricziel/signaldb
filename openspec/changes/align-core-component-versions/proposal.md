## Why

SignalDB's crates are versioned fully independently via release-please's `cargo-workspace` plugin, driven by conventional commits scoped by path. That plugin only cascades a patch bump to a dependent when convenient — it does not guarantee equality. Today `acceptor`/`common`/`compactor`/`querier`/`writer` sit at `0.2.1` while `router` drifted to `0.2.2` on its own, and `signaldb-bin` — the monolithic binary that ships as "SignalDB" and depends on every one of those crates — is stuck at `0.1.3`, on a completely different line. There is no number today that means "the SignalDB release."

## What Changes

- Add a `linked-versions` release-please plugin group (name: `signaldb-core`) covering `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`. Whenever any member bumps, all members release at the same, highest resulting version.
- Set `"merge": false` on the existing `cargo-workspace` plugin for these packages, per release-please's documented guidance for combining `cargo-workspace` with `linked-versions` (avoids the two plugins double-managing the same packages).
- Remove `signal-producer` from incidental lockstep — it currently sits in the `0.2.x` cluster purely because `cargo-workspace` cascaded it as a dependent of `common`; it is a dev/test data generator, not a shipped runtime component, and should version independently going forward.
- `.release-please-manifest.json` keeps every grouped package at its real, actually-tagged last-released version (`acceptor`/`common`/`compactor`/`querier`/`writer` at `0.2.1`, `signaldb-bin`/`signaldb-cli` at `0.1.3`, `router` at `0.2.2`) — **not** pre-written to `0.2.2` for packages that were never tagged there. See design.md for why: the manifest is release-please's source of truth for finding each package's previous-release tag to compute the next diff/changelog boundary; writing a version with no matching tag breaks that lookup.
- **BREAKING** (release-visible, not code-breaking): the first time any linked-group member gets a real commit after this lands, `linked-versions` will compute each member's true next version from its real baseline, take the max across the group, and align all 8 to it in one release PR — `signaldb-bin`/`signaldb-cli` will jump from the `0.1.x` line to `0.2.x`+ with no corresponding new features, pure harmonization. Call this out explicitly in that release's notes so it doesn't read as unexplained scope. This happens on the next organic release-please cycle, not immediately on merging this config change.
- No changes to crates outside the group: `tempo-api`, `loki-api`, `prometheus-api`, `pyroscope-api`, `signaldb-sdk`, `signaldb-api`, `mcp-server`, `ui`, `grafana-plugin` keep independent versioning exactly as today.
- **Follow-up, discovered post-merge**: the first real release-please PR generated under this config (#841) showed 7 of the 8 grouped packages aligning correctly, but `signaldb-bin` didn't — an upstream release-please bug (`googleapis/release-please#1750`) where `linked-versions` never syncs _any_ package that has `include-component-in-tag: false` (confirmed by reading release-please's own source: `getComponent()` unconditionally returns `undefined` for such a package, and `linked-versions` silently skips packages it can't get a component for). Fix: flip `signaldb-bin`'s `include-component-in-tag` to `true` (keeping it in the linked group — an earlier attempt that instead introduced a parallel repo-root `.` package to own the unprefixed tag was reviewed, found to hit the identical bug just relocated, and abandoned before merge). Docker's image-tagging rules, which previously parsed `signaldb-bin`'s tag string directly and would otherwise choke on the new `signaldb-bin-vX.Y.Z` prefix, now consume a plain `version` output (`0.3.0`, no prefix) that release-please-action already exposes per package — a smaller fix than changing tag production at all. `signaldb-bin`'s tag-lineage was also bootstrapped (`signaldb-bin-v0.1.3` created at the same commit as the existing `v0.1.3`) so release-please can find its previous-release boundary under the new convention. See design.md's Decisions/Migration Plan for the full investigation, including the rejected first attempt.

## Capabilities

This is a release-tooling/CI configuration change only (`release-please-config.json`, `.release-please-manifest.json`). It changes no OTLP ingest, query, storage, or API behavior — no runtime capability is added or modified, so this change sets `skip_specs: true` and carries no spec deltas.

### New Capabilities

None.

### Modified Capabilities

None.

## Impact

- `release-please-config.json`: add `linked-versions` plugin config; adjust `cargo-workspace` plugin config (`merge: false` scope); flip `signaldb-bin`'s `include-component-in-tag` to `true` (stays in the `linked-versions` group throughout — no swap).
- `.release-please-manifest.json`: no version values changed for any of the 8 grouped packages.
- `.github/workflows/release-please.yml`: adds a `version` output (`signaldb-bin`'s plain semver, no tag prefix) alongside the unchanged `release_created`/`tag_name` outputs; the three Docker `type=semver` tag rules switch from consuming `tag_name` to consuming `version`, so they don't have to parse a business-prefixed ref. The GitHub Release step keeps using `tag_name` unchanged.
- New git tag `signaldb-bin-v0.1.3`, created at the same commit as the existing `v0.1.3`, so release-please can find `signaldb-bin`'s previous-release boundary under its new tag convention.
- No changes to application source code in `src/*` crates — version fields and release-tooling config only.
- Affected workspace crates (version field edits at release time, no logic changes): `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`.
