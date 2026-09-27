# Design

## Context

`schema-model` parses a model tree with `RegistryDocument::from_dir`. Each
non-manifest file is deserialised into a `ModelFile { groups }` whose `groups`
defaults to empty, so a `definition/2` file parses cleanly into nothing. Resolution
(`resolve.rs`) works entirely on v1 `Group`s: `attribute_group`, `entity`,
`metric`, `span` and `event` types, with attributes addressed by `id` / `ref`. The
bundled snapshot is built in `src/common/build.rs` (otel, then signaldb with otel as
its dependency) and embedded as JSON. The vendoring tool is
`cargo xtask vendor-semconv`, which clones one repo at `v<pin>`.

Upstream GenAI (commit `e57c543`, 2026-09-24) uses `definition/2` in every model
file, uses `manifest.yaml` with `dependencies` pointing at core `v1.44.0`, and has no
release tag. Core semconv v1.44.0 still uses v1 `groups`.

## Goals / Non-Goals

**Goals:**
- Read `definition/2` without changing the resolved model or any API response shape.
- Make unreadable model files loud.
- Bundle GenAI as its own registry, pinned to an upstream commit.

**Non-Goals:**
- Writing `definition/2` back out. We normalise to `groups`.
- Re-expressing `otel/registry/signaldb.yaml` in `definition/2`.
- Moving SignalDB's self-monitoring registry dependency from `otel` to
  `otel-genai`. Its `mcp.*` / `gen_ai.tool.name` refs keep resolving against the
  deprecated shells until a follow-up.
- Supporting Weaver features we don't resolve today (templates, imports,
  visibility filtering beyond dropping `internal` groups from listings).

## Decisions

- **Lower at parse time into v1 `Group`s.** `definition/2` constructs are converted
  into the existing `Group` model when a file is read:
  - top-level `attributes` (`key:`) becomes one synthetic `attribute_group` per file
    with `id: <key>`;
  - `attribute_groups` become `attribute_group` groups;
  - `entities`, `metrics`, `spans` and `events` become groups of the matching
    `type`, taking the name from `type:`/`name:`;
  - `*_refinements` become groups that `extends` their base.

  The resolver, the snapshot format, the storage format for custom registries and
  every API stay unchanged.
  *Alternative:* a second native model that the resolver handles directly.
  Rejected because it doubles the resolver and every consumer for no user-visible
  gain.
- **Dispatch on `file_format`.** A file without `file_format` that has `groups` is
  v1. A file with `file_format: definition/2` is v2. Anything else is a
  `ParseError` naming the file. A v1 file without `groups` that is not a manifest
  becomes an error too, which closes the silent-empty hole.
- **Separate bundled namespace `otel-genai`.** It is its own registry with its own
  version (the upstream tag), not merged into `otel`. That keeps provenance honest,
  and the "moved" deprecated shells in `otel` remain reachable as alternatives.
  Precedence is custom → `signaldb` → `otel-genai` → `otel`, so current definitions
  beat the shells. `otel-genai` is resolved with `otel` as its dependency, mirroring
  upstream's manifest.
- **Pin a commit, not a tag.** Upstream has no release. `cargo xtask
  vendor-semconv-genai <sha>` clones `semantic-conventions-genai` at that commit into
  `vendor/otel-semconv-genai/<sha>/`, with a `VERSION` file, `LICENSE` and a README
  like the core vendor tree. The registry version is the short SHA. Moving to a
  tag once one exists is a re-vendor.
- **No core-version coupling.** GenAI declares core v1.44.0, and we pin v1.43.0.
  `otel-genai` is resolved against the bundled 1.43.0 `otel`, and `build.rs` fails on
  any unresolved ref. That is the check that matters to SignalDB. Weaver (CI) checks
  SignalDB's GenAI-dependent registry against 1.44.0, pulled in transitively.
  *Alternative:* bump the core pin first. Deferred, because it changes
  self-monitoring semconv for no gain here.
- **The self-monitoring registry does not depend on GenAI.** A spike showed Weaver
  live-check (`--include-unreferenced`) fails once core's `gen_ai.*` shells and
  GenAI's real definitions load together: 58 "declared multiple times" errors,
  upstream's overlap and not ours. SignalDB-owned groups that need GenAI live in
  `otel/registry-genai/`, with its own manifest depending on GenAI at the pinned
  commit, and a Weaver v0.26.1 `registry check` step in CI. `build.rs` folds that
  directory into the bundled `signaldb` registry.
- **Reconcile with `genai-agent-entity`.** If the vendored release defines its own
  agent entity, remove the `signaldb` one in the same change. If it doesn't, keep
  ours, and its refs start resolving to the `otel-genai` attributes because
  precedence handles lookup.
- No Flight, WAL, Iceberg or Arrow types are touched, so the FDAP
  version-alignment and schema-transform concerns do not apply.

## Risks / Trade-offs

- [`definition/2` is still evolving and our lowering drifts from Weaver] → Keep a
  fixture corpus of real upstream files under `src/schema-model/tests/fixtures/`,
  and run `weaver registry check` on the same fixtures in CI
  (`scripts/weaver-check-fixtures.sh`) with an image that supports the format.
- [Pinned commit is not a release, and upstream may rename things before tagging] →
  Re-vendoring is one xtask run. Tests pin the definitions we depend on
  (`gen_ai.agent.*`), so a breaking re-vendor fails loudly.
- [GenAI refs a core attribute added after 1.43.0] → `build.rs` fails naming the
  ref. Then bump the core pin (task group 3) before re-vendoring.
- [Custom registries round-trip in a different layout than uploaded] → This is
  accepted and documented. The resolved content is identical.

## Migration Plan

This is additive. Existing v1 registries are untouched. Rollback means reverting the
change. The embedded snapshot is rebuilt at compile time, and stored custom
registries are already in the `groups` form.
