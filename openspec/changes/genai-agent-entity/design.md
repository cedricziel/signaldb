# Design

## Context

Entity types reach tenants only through bundled or custom registries. The bundled
`signaldb` registry (`otel/registry/signaldb.yaml`) is resolved in
`src/common/build.rs` with the vendored `otel` registry as its only dependency, so its
groups can `ref:` any `otel` attribute. It already does this for the deprecated
`mcp.session.id`. The four `gen_ai.agent.*` attributes sit in
`vendor/otel-semconv/1.43.0/model/gen-ai/deprecated/registry-deprecated.yaml`, and
upstream kept the same keys when it moved them. Attribute lookups already collect entity roles
across every visible registry (`entity_roles` in `common::schema_registry`), so an
entity in `signaldb` that refs an `otel` attribute shows up on that attribute's hit.

## Goals / Non-Goals

**Goals:**
- One `gen_ai.agent` entity, resolvable on every existing surface, with no code
  change outside the registry YAML and tests.

**Non-Goals:**
- Un-deprecating or redefining the `gen_ai.*` attributes (that comes with
  `semconv-definition-v2`).
- Agent-aware service map, trace grouping or UI views.
- Deriving agent identity at ingest (e.g. a `resource.identity`-style column).

## Decisions

- **Reference, don't redefine.** The entity `ref:`s the `otel` attributes. Defining
  `gen_ai.agent.*` again in `signaldb` would pass `schema-model` (its duplicate check
  is per document), but lookups would then show two definitions of the same key, and
  `weaver registry check` may reject redefining an attribute that a dependency
  already provides. Referencing keeps one definition, and resolution stays honest
  that upstream calls these attributes "moved".
  *Alternative:* redefine them as non-deprecated attributes in `signaldb`. Rejected
  because the upstream definition is still `development` and has no release.
- **Entity name `gen_ai.agent`.** This follows the attribute prefix, like `k8s.pod`
  and `container`. If upstream later ships an entity with the same name, tenants get
  both hits, with `signaldb` first by precedence and `otel` returned as an
  alternative. We then remove ours (see Risks).
- **Group lives in `signaldb.yaml`**, next to the existing groups, with
  `stability: development`. It is not placed in a separate file, which keeps the
  registry to one document per concern.
- No Flight, WAL, Iceberg or Arrow types are touched, so the FDAP
  version-alignment and schema-transform concerns do not apply.

## Risks / Trade-offs

- [Upstream later ships its own agent entity, possibly with a different identifying
  attribute] → When `semconv-definition-v2` vendors the GenAI repo, compare
  definitions and drop ours if upstream covers it. Precedence already means nothing
  breaks in the meantime.
- [Weaver's policy check rejects an entity that refs deprecated attributes] → Run
  `weaver registry check` locally first (task 1.3). The fallback is to mark the entity
  `development` with a note; the existing `mcp.session.id` ref shows that deprecated
  refs pass today.
- [Lookups show the attributes as deprecated while the entity is not] → This is
  accepted and documented. It goes away when the GenAI conventions are vendored.
