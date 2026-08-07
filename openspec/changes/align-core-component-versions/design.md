## Context

See proposal.md - Why/What Changes for motivation and scope. Relevant mechanics:

- release-please runs in manifest mode (`release-please-config.json` + `.release-please-manifest.json`), one `packages` entry per crate path, each independently computing its own bump from conventional commits scoped to that path.
- The `cargo-workspace` plugin is already enabled workspace-wide. It builds the internal dependency graph and patch-bumps a package when a workspace dependency it references was itself bumped — but this is a one-way cascade with no equality guarantee, and it does not touch `signaldb-bin`/`signaldb-cli` reliably today (they depend on the core crates via bare `{ path = "../x" }` with no `version` requirement, so there is nothing for the plugin to reconcile against).
- `version.workspace = true` (Cargo-native shared-field inheritance via `[workspace.package].version`) was considered and rejected as the mechanism: release-please's Cargo.toml updater has no clearly documented/verified support for writing to a shared `[workspace.package].version` field consumed via inheritance (see `googleapis/release-please#1094`, which is in this exact territory but its resolution doesn't confirm inheritance-write support). `linked-versions` is release-please's documented, first-class answer to "force a subset of independently-versioned packages to share one number."
- **The manifest is not just a display value — it's how release-please finds the previous-release boundary.** For each package, release-please resolves its manifest entry to a corresponding git tag (following that package's tag convention, e.g. `acceptor-v{version}`) to locate the commit marking the end of the last release; that's the diff boundary for computing the next release's commits/changelog/bump. Verified against this repo's actual tags: only `router-v0.2.2` exists among the group — `acceptor-v0.2.2`, `common-v0.2.2`, `compactor-v0.2.2`, `querier-v0.2.2`, `writer-v0.2.2`, `signaldb-cli-v0.2.2`, and `v0.2.2` (signaldb-bin) do not. Writing `0.2.2` into the manifest for those 7 packages would point release-please at tags that were never created, which per its own troubleshooting docs risks losing the release boundary entirely (replaying a much larger commit range than intended) — and even short of that failure mode, it would falsely record a release that never happened, shifting the _real_ next patch release to read `0.2.3`.

## Goals / Non-Goals

**Goals:**

- Every release, `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common` carry the identical version number, computed automatically from conventional commits — no manual sync step.
- Everything else keeps releasing independently, unaffected.
- No change to how contributors write commits — the linked group still derives its bump from the same conventional-commit scan release-please already does per path.

**Non-Goals:**

- Not switching to Cargo workspace version inheritance (`version.workspace = true`) — out of scope per the mechanism decision above; may be revisited later if release-please's support for it is verified.
- Not changing `signaldb-sdk`/`signaldb-api`/`tempo-api`/`loki-api`/`prometheus-api`/`pyroscope-api`/`mcp-server`/`ui`/`grafana-plugin` versioning at all.
- Not cleaning up the orphaned `.` → `"0.1.0"` manifest entry / vestigial `[workspace.package] version = "0.1.0"` in the root `Cargo.toml` — dead, unwired, harmless, unrelated to this mechanism; a one-line note only.
- Not adding build-metadata (git commit hash) to version output — tracked as a separate change (`report-build-commit-hash` or similar).

## Decisions

**Use `linked-versions`, not `version.workspace = true`.** Covered in Context — the deciding factor is that `linked-versions` is a proven, documented release-please pattern for exactly this shape of requirement, while workspace-field inheritance support is unverified. Revisit only if a future spike confirms release-please writes `[workspace.package].version` correctly.

**Group membership: `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`.** These are the FDAP runtime (monolith + the services composing it) plus the shared substrate library every one of them is built from, plus the CLI that ships in the same release artifacts. Rejected alternatives: dropping `common` (it's not a binary, but every grouped binary is compiled from it in lockstep anyway, so excluding it would let "the version" diverge from what actually built the binaries); dropping `signaldb-cli` (ships in the same GitHub release/archives as the server binaries, same audience, same cadence — including it was the explicit call made when scoping this).

**Pull `signal-producer` out of the incidental cluster.** It's currently at `0.2.1` purely because `cargo-workspace` cascaded it as a `common` dependent — not a deliberate grouping. It's a dev/test data generator, not a shipped runtime component, so it goes back to plain independent versioning (no config change needed beyond _not_ adding it to the `linked-versions` group — it already isn't).

**`cargo-workspace` plugin needs `"merge": false` scoped to the linked group.** Per release-please's own guidance for combining `cargo-workspace` with `linked-versions`: without this, both plugins try to own version-setting for the same packages and can conflict. `cargo-workspace` keeps doing its job (Cargo.lock updates, dependency-graph-driven cascades) for packages outside the linked group.

**Don't pre-write a bootstrap version into the manifest at all — keep every entry at its real, actually-tagged last release.** An earlier draft of this design set all 8 grouped manifest entries to `0.2.2` (the group's current maximum, `router`'s version) on the theory that SemVer's factual trail makes an aligned starting point harmless. That was wrong, caught in PR review (CodeRabbit) and confirmed against this repo's real tags: `router-v0.2.2` exists, but `acceptor-v0.2.2`/`common-v0.2.2`/`compactor-v0.2.2`/`querier-v0.2.2`/`writer-v0.2.2`/`signaldb-cli-v0.2.2`/`v0.2.2` do not — see the Context note above on why the manifest is a tag-lookup key, not just a display number. Fabricating those 7 values doesn't "harmlessly" pre-align anything; it risks release-please losing the release boundary for those packages and, even in the best case, permanently misrecords a release that never happened. The correct approach: leave the manifest untouched (each of the 8 stays at its real baseline — `0.2.1` for `acceptor`/`common`/`compactor`/`querier`/`writer`, `0.1.3` for `signaldb-bin`/`signaldb-cli`, `0.2.2` for `router`) and let `linked-versions` compute the alignment for real the next time any group member gets a commit — see Migration Plan.

## Risks / Trade-offs

- **[Risk]** `linked-versions` + `cargo-workspace` interaction is a less common combination than either plugin alone; misconfiguration could produce a broken or no-op release-please PR. → **Mitigation**: land this change, then verify by inspecting the next release-please PR's diff (all 8 manifest entries move together, no unrelated packages touched) before merging it; treat the first PR as the acceptance test, not a step to auto-merge.
- **[Risk]** The `signaldb-bin`/`signaldb-cli` version jump (`0.1.x` → `0.2.x`+) could be mistaken by downstream consumers (Docker tag watchers, changelog scrapers) for a major feature drop. → **Mitigation**: explicit release-notes callout (already in proposal.md); no code changes accompany the jump, so the diff itself is self-explanatory to anyone who looks.
- **[Risk]** `.github/workflows/release-please.yml` reads specific per-component outputs (e.g. `steps.release.outputs['src/signaldb-bin--release_created']`). If linking changes how/whether those keys are emitted, the release workflow silently stops triggering binary builds. → **Mitigation**: release-please still tracks and emits outputs per original component path even when linked (linking affects the version number chosen, not the component identity) — but this must be confirmed against the next real release-please PR output before relying on it in production; add a task to verify this explicitly rather than assume it.
- **[Risk]** Manifest entries could be pre-written to a version with no matching tag (exactly what happened here initially — see the Decisions entry above). → **Mitigation**: caught in PR review before merge by validating manifest versions against actual `git tag`/`git ls-remote --tags` output; fixed by leaving the manifest untouched. General lesson for any future manifest edit: never write a version into `.release-please-manifest.json` without confirming its tag actually exists.

## Migration Plan

1. Edit `release-please-config.json`: add the `linked-versions` plugin block (group `signaldb-core`, the 8 paths), add `"merge": false` scoping for `cargo-workspace` on those same paths.
2. `.release-please-manifest.json`: no edits — every grouped entry stays at its real, tagged last-released version.
3. Merge to `main` behind normal PR review (no runtime deploy involved — this only affects release-please's next computation on a future push to `main`).
4. Wait for the first real commit to land that touches any linked-group member's path. On that push, release-please computes each touched (and, via the group, every) member's next version from its real baseline and aligns all 8 to the group's max in one release PR.
5. Observe that auto-generated release-please PR: confirm all 8 packages appear with the same target version, confirm `signal-producer` and every independent package are untouched, confirm the `release-please.yml` job outputs used by `build-release`/`build-musl-*` still populate correctly, confirm the diff's per-package tags/changelogs look sane (not an unexpectedly large replayed commit range).
6. If immediate alignment (rather than waiting for an organic commit) is wanted, that's a separate, deliberate operational decision — e.g. a `Release-As:` commit footer scoped to force specific packages' next version — not something to fold into a manifest pre-edit. Out of scope for this change; revisit only if waiting turns out to be impractical.
7. Rollback: revert the config edit commit — release-please is stateless between runs (its only persisted state is the manifest file, which this change no longer touches), so reverting fully restores independent-versioning behavior with no cleanup needed.

## Open Questions

None — bootstrap version, group membership, and plugin interaction are all decided above.
