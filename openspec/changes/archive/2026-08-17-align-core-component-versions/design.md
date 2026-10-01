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
- Docker image tags and GitHub Releases keep working exactly as before.

**Non-Goals:**

- Not switching to Cargo workspace version inheritance (`version.workspace = true`) — out of scope per the mechanism decision above; may be revisited later if release-please's support for it is verified.
- Not changing `signaldb-sdk`/`signaldb-api`/`tempo-api`/`loki-api`/`prometheus-api`/`pyroscope-api`/`mcp-server`/`ui`/`grafana-plugin` versioning at all.
- Not adding build-metadata (git commit hash) to version output — tracked as a separate change (`report-build-commit-hash` or similar).

## Decisions

**Use `linked-versions`, not `version.workspace = true`.** Covered in Context — the deciding factor is that `linked-versions` is a proven, documented release-please pattern for exactly this shape of requirement, while workspace-field inheritance support is unverified. Revisit only if a future spike confirms release-please writes `[workspace.package].version` correctly.

**Group membership: `signaldb-bin`, `signaldb-cli`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `common`.** These are the FDAP runtime (monolith + the services composing it) plus the shared substrate library every one of them is built from, plus the CLI that ships in the same release artifacts. Rejected alternatives: dropping `common` (it's not a binary, but every grouped binary is compiled from it in lockstep anyway, so excluding it would let "the version" diverge from what actually built the binaries); dropping `signaldb-cli` (ships in the same GitHub release/archives as the server binaries, same audience, same cadence — including it was the explicit call made when scoping this).

**Pull `signal-producer` out of the incidental cluster.** It's currently at `0.2.1` purely because `cargo-workspace` cascaded it as a `common` dependent — not a deliberate grouping. It's a dev/test data generator, not a shipped runtime component, so it goes back to plain independent versioning (no config change needed beyond _not_ adding it to the `linked-versions` group — it already isn't).

**`cargo-workspace` plugin needs `"merge": false` scoped to the linked group.** Per release-please's own guidance for combining `cargo-workspace` with `linked-versions`: without this, both plugins try to own version-setting for the same packages and can conflict. `cargo-workspace` keeps doing its job (Cargo.lock updates, dependency-graph-driven cascades) for packages outside the linked group.

**Don't pre-write a bootstrap version into the manifest at all — keep every entry at its real, actually-tagged last release.** An earlier draft of this design set all 8 grouped manifest entries to `0.2.2` (the group's current maximum, `router`'s version) on the theory that SemVer's factual trail makes an aligned starting point harmless. That was wrong, caught in PR review (CodeRabbit) and confirmed against this repo's real tags: `router-v0.2.2` exists, but `acceptor-v0.2.2`/`common-v0.2.2`/`compactor-v0.2.2`/`querier-v0.2.2`/`writer-v0.2.2`/`signaldb-cli-v0.2.2`/`v0.2.2` do not — see the Context note above on why the manifest is a tag-lookup key, not just a display number. Fabricating those 7 values doesn't "harmlessly" pre-align anything; it risks release-please losing the release boundary for those packages and, even in the best case, permanently misrecords a release that never happened. The correct approach: leave the manifest untouched (each of the 8 stays at its real baseline — `0.2.1` for `acceptor`/`common`/`compactor`/`querier`/`writer`, `0.1.3` for `signaldb-bin`/`signaldb-cli`, `0.2.2` for `router`) and let `linked-versions` compute the alignment for real the next time any group member gets a commit — see Migration Plan.

**Fix `signaldb-bin`'s divergence by flipping its own `include-component-in-tag` to `true` and keeping it in the linked group — and fix Docker's tag _consumption_, not release-please's tag _production_.** Landing Phase 1 surfaced a second, upstream bug once the first real post-merge release-please PR (#841) computed live: 7 of the 8 grouped packages aligned correctly to the same version, but `signaldb-bin` didn't — it kept its own independently-computed patch bump instead. Root cause, confirmed against release-please's own issue tracker: [`googleapis/release-please#1750`](https://github.com/googleapis/release-please/issues/1750), "linked-versions plugin and includeComponentInTag=false don't work together." Both of the issue's fix attempts (`PR #1749`, `PR #2208`) are closed _unmerged_ — years of continued cross-references into 2026 indicate the underlying incompatibility is still live.

**First attempt at this fix was wrong and is worth recording.** The initial approach introduced a repo-root `.` package (`release-type: "simple"`, a `version.txt`) to take over `signaldb-bin`'s unprefixed tag, on the theory that moving `include-component-in-tag: false` to a different, non-Cargo package would avoid the bug. It didn't — caught in PR review (CodeRabbit), which pulled release-please v17.6.0's actual source. `strategies/base.ts`:

```ts
async getComponent(): Promise<string | undefined> {
    if (!this.includeComponentInTag) {
      return undefined;
    }
    return this.component || (await this.getDefaultComponent());
}
```

and `plugins/linked-versions.ts`'s `preconfigure()`:

```ts
const component = await strategy.getComponent();
if (!component) {
  continue; // silently excluded from the group
}
```

`getComponent()` returns `undefined` whenever `includeComponentInTag` is false — **unconditionally, regardless of which package or what `component` name is configured.** `linked-versions` silently skips any such package. This is not a `signaldb-bin`-specific quirk; it's a hard rule that _no_ `include-component-in-tag: false` package can ever be a `linked-versions` member. The `.`-package attempt would have hit the identical bug, just relocated — and additionally, by swapping `signaldb-bin` _out_ of the `components` list entirely (replaced by the new root component), it stopped `signaldb-bin`'s own Cargo.toml version from ever being forced to match the group at all, which is strictly worse than Phase 1. Verified independently against release-please's real source (not just taking the review comment on faith) before redoing the fix.

**The corrected fix**: flip `signaldb-bin`'s `include-component-in-tag` to `true` — this is what actually resolves its `getComponent()` return value and makes it correctly discoverable — and _keep it in_ the `signaldb-core` `linked-versions` group (undo the erroneous swap). This was always understood to be the "direct fix" (see the original, since-superseded rejection below); what changed is realizing the blocker it was rejected for doesn't actually require inventing a new package to solve.

**The real blocker — Docker's `type=semver` rule consuming a business-prefixed tag — has a much smaller fix.** `docker/metadata-action`'s `type=semver,pattern={{version}},value=<ref>` rule doesn't require `value` to be a git tag string at all; it accepts any semver-shaped value directly. release-please-action already exposes a plain, prefix-free `<path>--version` output per package (`grafana-plugin--version` is already consumed elsewhere in this same workflow) — so pointing Docker's `value` input at `signaldb-bin`'s `version` output (e.g. `0.3.0`) instead of its `tag_name` output (`signaldb-bin-v0.3.0`) sidesteps the prefix-parsing problem entirely, with no new package, no `version.txt`, and no tag-lineage migration to invent. The GitHub Release step keeps using `tag_name` as before (it just names which git tag the release corresponds to; nothing there parses it as semver), so `signaldb-bin-v0.3.0` is fine for that purpose.

**Migrating `signaldb-bin`'s own tag convention still needs its history bootstrapped.** CodeRabbit's second finding on the same PR: release-please resolves a package's previous-release tag using its _current_ `include-component-in-tag` setting — once `signaldb-bin` flips to `true`, release-please looks for `signaldb-bin-v0.1.3` (matching its `0.1.3` manifest entry) to find the last-release boundary, but only the old-convention `v0.1.3` exists. This is the same class of mistake as the original CodeRabbit catch on PR #1043 (manifest pointing at a tag that doesn't exist) — caught before merge again, this time by creating `signaldb-bin-v0.1.3` as a new tag at the exact same commit as the existing `v0.1.3` (verified: `dc218d8f...`, a lightweight tag, trivially replicated), rather than by fabricating a manifest value.

Rejected alternatives (superseded): the `.`-package split (see above — doesn't actually work); dropping `signaldb-bin` from the linked group and reconciling it manually via periodic `Release-As:` commits (keeps the exact package this whole change exists to fix permanently unautomated, and unnecessary now that the real blocker has a proper fix).

## Risks / Trade-offs

- **[Risk]** `linked-versions` + `cargo-workspace` interaction is a less common combination than either plugin alone; misconfiguration could produce a broken or no-op release-please PR. → **Mitigation**: land this change, then verify by inspecting the next release-please PR's diff (all 8 manifest entries move together, no unrelated packages touched) before merging it; treat the first PR as the acceptance test, not a step to auto-merge.
- **[Risk]** The `signaldb-bin`/`signaldb-cli` version jump (`0.1.x` → `0.2.x`+) could be mistaken by downstream consumers (Docker tag watchers, changelog scrapers) for a major feature drop. → **Mitigation**: explicit release-notes callout (already in proposal.md); no code changes accompany the jump, so the diff itself is self-explanatory to anyone who looks.
- **[Risk]** `.github/workflows/release-please.yml` reads specific per-component outputs (e.g. `steps.release.outputs['src/signaldb-bin--release_created']`). If linking changes how/whether those keys are emitted, the release workflow silently stops triggering binary builds. → **Mitigation**: confirmed live against PR #841 (post-merge) that path-keyed outputs are unaffected by linking for the 7 packages that synced correctly. `signaldb-bin` stays the source of `release_created`/`tag_name` (unchanged path/key), plus a new `version` output added alongside it — no output key is removed or renamed.
- **[Risk]** Manifest entries could be pre-written to a version with no matching tag (exactly what happened here initially — see the Decisions entry above). → **Mitigation**: caught in PR review before merge by validating manifest versions against actual `git tag`/`git ls-remote --tags` output; fixed by leaving the manifest untouched. General lesson for any future manifest edit: never write a version into `.release-please-manifest.json` without confirming its tag actually exists.
- **[Risk]** `include-component-in-tag: false` + `linked-versions` silently fails to sync a package instead of erroring, with no warning from release-please — confirmed live in PR #841 (`signaldb-bin` diverged) and confirmed structurally by reading release-please's actual source (`getComponent()` unconditionally returns `undefined` when the flag is false). → **Mitigation**: the fix is to never combine `include-component-in-tag: false` with `linked-versions` membership for _any_ package, full stop — not to find a package for which the combination is "safe" (there isn't one). `signaldb-bin` now uses `include-component-in-tag: true` and is a real group member.
- **[Risk]** Migrating `signaldb-bin`'s tag convention (unprefixed → prefixed) could make release-please lose its previous-release boundary if the new-convention tag doesn't exist at the right commit — same failure mode as the earlier manifest-fabrication bug, different trigger (a _config_ change altering which tag name release-please looks for, rather than a fabricated manifest value). → **Mitigation**: created `signaldb-bin-v0.1.3` as a real tag at the exact commit `v0.1.3` already points to (`dc218d8f...`) before merging this config change, so the lookup succeeds under the new convention immediately.

## Migration Plan

**Phase 1 (done, merged in #1043, corrected in a follow-up commit same day):**

1. Edit `release-please-config.json`: add the `linked-versions` plugin block (group `signaldb-core`, 8 paths incl. `signaldb-bin`), add `"merge": false` scoping for `cargo-workspace`.
2. `.release-please-manifest.json`: no version edits — every grouped entry stays at its real, tagged last-released version.
3. Merged to `main`. First real release-please PR (#841) confirmed 7/8 packages aligned correctly; `signaldb-bin` diverged — the bug this Decisions entry addresses.

**Phase 2, attempt 1 (superseded, never merged — see Decisions above):** introduced a root `.` package + `version.txt`. Caught in review as structurally broken before merge; abandoned in favor of attempt 2 below. Recorded here, not deleted, so the reasoning isn't lost.

**Phase 2, attempt 2 (this entry — fixes `signaldb-bin`'s divergence):**

4. In `release-please-config.json`: flip `src/signaldb-bin`'s `include-component-in-tag` to `true`; keep it in the `signaldb-core` `linked-versions` group's `components` list (no swap).
5. In `.github/workflows/release-please.yml`: add a `version` output (`steps.release.outputs['src/signaldb-bin--version']`) alongside the existing `release_created`/`tag_name` outputs (both unchanged); repoint the three Docker `type=semver` tag rules from `needs.release-please.outputs.tag_name` to `needs.release-please.outputs.version`. The GitHub Release step keeps using `tag_name`.
6. Bootstrap `signaldb-bin`'s tag lineage: create and push `signaldb-bin-v0.1.3` at the same commit as the existing `v0.1.3` (`dc218d8f...`), so release-please can find the previous-release boundary under the new tag convention.
7. Merge to `main`, then observe the next release-please PR: confirm `signaldb-bin` now tags as `signaldb-bin-vX.Y.Z` and aligns with the rest of `signaldb-core`; confirm Docker image tagging still resolves correctly from the `version` output; confirm the GitHub Release still gets created with the (now-prefixed) `tag_name`.
8. Rollback: revert the commit. `signaldb-bin-v0.1.3` is an additive tag pointing at pre-existing history — harmless to leave in place even if the surrounding config reverts, but can be deleted (`git push origin --delete signaldb-bin-v0.1.3`) if a full rollback is wanted.

## Open Questions

None — bootstrap version, group membership, the `signaldb-bin` tag-convention fix, and plugin interaction are all decided above.
