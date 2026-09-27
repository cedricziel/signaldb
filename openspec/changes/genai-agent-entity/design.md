# Design

## Context

Entity types reach tenants only through bundled or custom registries. The four
`gen_ai.agent.*` attributes exist in the vendored core semconv (1.43.0) only as
shells annotated `dependency_resolution: exclude`. A spike confirmed that Weaver
rejects a `ref` to them ("marked as excluded from dependency resolution"), and that a
local redeclaration fails live-check ("declared multiple times"). The comment in
`otel/registry/signaldb.yaml` (MCP section) records the same limit for
`gen_ai.tool.name`.

`semconv-definition-v2` bundles the GenAI conventions as `otel-genai` at a pinned
commit and adds `otel/registry-genai/`, a SignalDB-owned Weaver registry that depends
on GenAI. Attribute lookups already collect entity roles across every visible
registry (`entity_roles` in `common::schema_registry`).

## Goals / Non-Goals

**Goals:**
- One `gen_ai.agent` entity, resolvable on every existing surface, that passes Weaver.

**Non-Goals:**
- Agent-aware service map, trace grouping or UI views.
- Deriving agent identity at ingest (e.g. a `resource.identity`-style column).
- Moving the self-monitoring registry onto GenAI.

## Decisions

- **The entity lives in `otel/registry-genai/`.** Its manifest depends on GenAI at the
  pinned commit, so the `ref`s point at real, non-excluded definitions and
  `weaver registry check` passes (spike: exit 0 with Weaver v0.26.1).
  *Alternatives:* `ref` the core shells, which Weaver rejects. Redeclare them
  locally, which live-check rejects. Make the self-monitoring registry depend on
  GenAI, which makes live-check `--include-unreferenced` fail on upstream's own
  overlap between the shells and GenAI.
- **Bundled under the `signaldb` namespace.** `build.rs` folds
  `otel/registry-genai/` into the `signaldb` document, resolved against `otel` and
  `otel-genai`. Tenants see one SignalDB registry, not two.
- **Entity name `gen_ai.agent`.** This follows the attribute prefix, like `k8s.pod`
  and `container`. If upstream later ships an entity with the same name, tenants get
  both hits, with `signaldb` first by precedence. We then remove ours.
- No Flight, WAL, Iceberg or Arrow types are touched, so the FDAP
  version-alignment and schema-transform concerns do not apply.

## Risks / Trade-offs

- [Upstream ships its own agent entity, possibly with a different identifying
  attribute] → Check on every GenAI re-vendor, and drop ours if upstream covers it.
- [The pinned GenAI commit changes `gen_ai.agent.*` before release] → The tests pin
  the four keys, so a re-vendor that breaks them fails the build.
