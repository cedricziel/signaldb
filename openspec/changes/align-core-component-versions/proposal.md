## Why

SignalDB's crates are versioned fully independently via release-please's `cargo-workspace` plugin, driven by conventional commits scoped by path. That plugin only cascades a patch bump to a dependent when convenient — it does not guarantee equality. Today `acceptor`/`common`/`compactor`/`querier`/`writer` sit at `0.2.1` while `router` drifted to `0.2.2` on its own, and `signaldb-bin` — the monolithic binary that ships as "SignalDB" and depends on every one of those crates — is stuck at `0.1.3`, on a completely different line. There is no number today that means "the SignalDB release."

## What Changes

- Add a `linked-versions` release-please plugin group (name: `signaldb-core`) covering `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`. Whenever any member bumps, all members release at the same, highest resulting version.
- Set `"merge": false` on the existing `cargo-workspace` plugin for these packages, per release-please's documented guidance for combining `cargo-workspace` with `linked-versions` (avoids the two plugins double-managing the same packages).
- Remove `signal-producer` from incidental lockstep — it currently sits in the `0.2.x` cluster purely because `cargo-workspace` cascaded it as a dependent of `common`; it is a dev/test data generator, not a shipped runtime component, and should version independently going forward.
- `.release-please-manifest.json` keeps every grouped package at its real, actually-tagged last-released version (`acceptor`/`common`/`compactor`/`querier`/`writer` at `0.2.1`, `signaldb-bin`/`signaldb-cli` at `0.1.3`, `router` at `0.2.2`) — **not** pre-written to `0.2.2` for packages that were never tagged there. See design.md for why: the manifest is release-please's source of truth for finding each package's previous-release tag to compute the next diff/changelog boundary; writing a version with no matching tag breaks that lookup.
- **BREAKING** (release-visible, not code-breaking): the first time any linked-group member gets a real commit after this lands, `linked-versions` will compute each member's true next version from its real baseline, take the max across the group, and align all 8 to it in one release PR — `signaldb-bin`/`signaldb-cli` will jump from the `0.1.x` line to `0.2.x`+ with no corresponding new features, pure harmonization. Call this out explicitly in that release's notes so it doesn't read as unexplained scope. This happens on the next organic release-please cycle, not immediately on merging this config change.
- No changes to crates outside the group: `tempo-api`, `loki-api`, `prometheus-api`, `pyroscope-api`, `signaldb-sdk`, `signaldb-api`, `mcp-server`, `ui`, `grafana-plugin` keep independent versioning exactly as today.

## Capabilities

This is a release-tooling/CI configuration change only (`release-please-config.json`, `.release-please-manifest.json`). It changes no OTLP ingest, query, storage, or API behavior — no runtime capability is added or modified, so this change sets `skip_specs: true` and carries no spec deltas.

### New Capabilities

None.

### Modified Capabilities

None.

## Impact

- `release-please-config.json`: add `linked-versions` plugin config; adjust `cargo-workspace` plugin config (`merge: false` scope).
- `.release-please-manifest.json`: no version values changed for the 8 grouped packages — each stays at its real last-tagged version; only the eventual alignment mechanism changes (via `linked-versions`, on the next real release). `signal-producer` keeps tracking its own version independently going forward (no manifest change needed there beyond leaving it as-is).
- `.github/workflows/release-please.yml`: verify no assumptions break — it already reads `outputs['src/signaldb-bin--release_created']` etc. per-component; confirm those output keys are unaffected by grouping (release-please still emits per-component outputs even when linked).
- No source code changes in `src/*` crates themselves — version fields only.
- Affected workspace crates (version field edits at release time, no logic changes): `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`.
