# Proposal

## Why

AI agents have no entity type anywhere in SignalDB, so `resolve_entity gen_ai.agent`
returns nothing. The `gen_ai.agent.*` attributes exist in the bundled `otel` registry
only as deprecated entries ("moved to the GenAI semantic-conventions repository"), and
that upstream repository defines them as span attributes on `create_agent` /
`invoke_agent` spans but ships no entity. Tenants instrumenting agents have no
canonical way to learn which attribute identifies an agent. Agents are now common
producers of telemetry, so this is worth fixing now.

## What Changes

- Add a `gen_ai.agent` entity to SignalDB's own bundled registry, defined in a new
  `otel/registry-genai/` directory whose manifest depends on the GenAI conventions.
  `gen_ai.agent.id` is the identifying attribute. `gen_ai.agent.name`,
  `gen_ai.agent.description` and `gen_ai.agent.version` are descriptive.
- The entity refers to the GenAI attribute definitions that `semconv-definition-v2`
  bundles as `otel-genai`. It does not redefine them, and it ships in or after that
  change. The deprecated copies in core semconv can't be used: upstream marks them
  `dependency_resolution: exclude`, so Weaver rejects any `ref` to them.
- The entity's brief states the upstream meaning of `gen_ai.agent.id`: the
  provider-assigned, stable identifier of a hosted agent (e.g. a Bedrock agent ARN),
  not an in-memory instance id.
- No new endpoints, tools or UI: every surface that already resolves entities
  (HTTP, CLI `signaldb schema entity get`, MCP `resolve_entity`, the Explore schema
  view) picks the entity up from the bundled registry.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `schema-registry`: the bundled `signaldb` registry gains a requirement to define
  the `gen_ai.agent` entity.

## Impact

- New `otel/registry-genai/` (manifest + one `entity` group), folded into the bundled
  `signaldb` registry by `src/common/build.rs`, and Weaver-checked in CI.
- Depends on `semconv-definition-v2` (the `definition/2` parser and the vendored
  GenAI conventions).
- `common` crate: bundled snapshot (built by `src/common/build.rs`) now contains the
  entity; new assertion in `src/common/tests/schema_registry.rs`.
- The self-monitoring registry (`otel/registry/`) and its live-check are unchanged.
- Docs: `docs/users/schema-registry.md` lists the entity.
- No change to OTLP ingest, query surfaces, Flight schemas, WAL or Iceberg layout.
  Not breaking.
