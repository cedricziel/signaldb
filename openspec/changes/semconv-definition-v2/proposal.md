# Proposal

## Why

OpenTelemetry moved the GenAI and MCP conventions (`gen_ai.*`, `mcp.*`) out of core
semconv into [semantic-conventions-genai](https://github.com/open-telemetry/semantic-conventions-genai).
The vendored core model now carries them only as deprecated shells whose note says
"moved". That repo is written in Weaver's newer `file_format: definition/2` layout
(top-level `attributes:` keyed by `key:`, plus `attribute_groups:`, `spans:`,
`metrics:`, `events:`, `entities:` and `*_refinements:`). `schema-model` only reads
the v1 `groups:` layout. Worse, it quietly skips any file without `groups:`, so
pointing it at a definition/2 tree produces an empty registry instead of an error.
Until the parser understands the new format, tenants resolving `gen_ai.*` keys get
deprecated definitions. They also can't upload registries written in the format
Weaver is moving to.

## What Changes

- `schema-model` reads Weaver `definition/2` files alongside v1 `groups:` files and
  lowers both into the same resolved model (attributes, entities, metrics, spans,
  events).
- A model file whose layout the parser can't read is a parse error. It is no longer
  skipped.
- Tenant custom registries may be uploaded in either layout. They are stored and
  returned in the normalised form.
- Once upstream tags a release, vendor `semantic-conventions-genai` and ship it as a
  new bundled, read-only registry `otel-genai`. It takes precedence over `otel`, so
  its current definitions win over the deprecated shells. Those shells stay
  available as alternatives.
- The namespace `otel-genai` becomes reserved.
- `cargo xtask vendor-semconv` learns to vendor the GenAI repo at a pinned tag, and
  the build fails if its declared core-semconv dependency differs from the core pin.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `schema-registry`: bundled registries gain `otel-genai`. The reserved namespaces
  and resolution precedence change accordingly. Custom registries accept the
  `definition/2` layout.

## Impact

- `schema-model` crate: parser and lowering for `definition/2`, plus
  unreadable-file errors.
- `common` crate: `build.rs` bundles a third registry, the precedence order in
  `common::schema_registry`, and reserved-namespace validation.
- `xtask`: vendoring for the GenAI repo. New `vendor/otel-semconv-genai/` tree.
- Upstream GenAI currently depends on core semconv v1.44.0, and we pin v1.43.0. The
  vendoring step needs the core pin (`common::self_monitoring::SEMCONV_SCHEMA_URL`)
  bumped to match first.
- Router HTTP API: the schema-registry upload accepts the new layout. The OpenAPI
  request description changes, but the response shapes do not.
- CI: the `weaver registry check` image must understand `definition/2` (Weaver
  ≥ v0.26 per upstream `versions.env`; CI uses v0.25.1).
- Docs: `docs/users/schema-registry.md`.
- No change to OTLP ingest, query surfaces, Flight schemas, WAL or Iceberg layout.
  Not breaking for existing v1 registries.
