## Why

SignalDB's crates are versioned fully independently via release-please's `cargo-workspace` plugin, driven by conventional commits scoped by path. That plugin only cascades a patch bump to a dependent when convenient — it does not guarantee equality. Today `acceptor`/`common`/`compactor`/`querier`/`writer` sit at `0.2.1` while `router` drifted to `0.2.2` on its own, and `signaldb-bin` — the monolithic binary that ships as "SignalDB" and depends on every one of those crates — is stuck at `0.1.3`, on a completely different line. There is no number today that means "the SignalDB release."

## What Changes

- Add a `linked-versions` release-please plugin group (name: `signaldb-core`) covering `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`. Whenever any member bumps, all members release at the same, highest resulting version.
- Set `"merge": false` on the existing `cargo-workspace` plugin for these packages, per release-please's documented guidance for combining `cargo-workspace` with `linked-versions` (avoids the two plugins double-managing the same packages).
- Remove `signal-producer` from incidental lockstep — it currently sits in the `0.2.x` cluster purely because `cargo-workspace` cascaded it as a dependent of `common`; it is a dev/test data generator, not a shipped runtime component, and should version independently going forward.
- Bootstrap `.release-please-manifest.json` so the 8 grouped crates start from a single aligned version (the current group maximum, `0.2.2`, unless a deliberate bump to signal the new scheme is preferred — see design.md).
- **BREAKING** (release-visible, not code-breaking): `signaldb-bin` and `signaldb-cli` will jump from the `0.1.x` line to `0.2.x`+ in the first release after this lands, without corresponding new features — pure harmonization. Call this out explicitly in that release's notes so it doesn't read as unexplained scope.
- No changes to crates outside the group: `tempo-api`, `loki-api`, `prometheus-api`, `pyroscope-api`, `signaldb-sdk`, `signaldb-api`, `mcp-server`, `ui`, `grafana-plugin` keep independent versioning exactly as today.

## Capabilities

This is a release-tooling/CI configuration change only (`release-please-config.json`, `.release-please-manifest.json`). It changes no OTLP ingest, query, storage, or API behavior — no runtime capability is added or modified, so this change sets `skip_specs: true` and carries no spec deltas.

### New Capabilities

None.

### Modified Capabilities

None.

## Impact

- `release-please-config.json`: add `linked-versions` plugin config; adjust `cargo-workspace` plugin config (`merge: false` scope).
- `.release-please-manifest.json`: bootstrap aligned starting version for the 8 grouped packages; `signal-producer` keeps tracking its own version independently going forward (no manifest change needed there beyond leaving it as-is).
- `.github/workflows/release-please.yml`: verify no assumptions break — it already reads `outputs['src/signaldb-bin--release_created']` etc. per-component; confirm those output keys are unaffected by grouping (release-please still emits per-component outputs even when linked).
- No source code changes in `src/*` crates themselves — version fields only.
- Affected workspace crates (version field edits at release time, no logic changes): `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`.
