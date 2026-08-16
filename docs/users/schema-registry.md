---
audience: user
type: how-to
status: living
sources:
  - src/router/src/endpoints/schema.rs
  - src/common/src/schema_registry/**
  - src/schema-model/src/**
  - src/signaldb-cli/src/commands/**
  - src/mcp-server/src/server.rs
---

# Schema registry — semantic conventions and custom registries

SignalDB knows what your telemetry _means_, not just what it contains. The
**schema registry** holds semantic-convention registries — attribute keys with
their descriptions and types, entity types (`service`, `k8s.pod`, `host`, …)
with the attributes that identify them, and metric definitions with their
instrument, unit, and the entities they describe — and resolves any attribute
key, entity type, or metric name to its definitions for your tenant.

Three kinds of registry exist:

| Namespace       | Source  | What it is                                                                                         |
| --------------- | ------- | -------------------------------------------------------------------------------------------------- |
| `otel`          | bundled | The OpenTelemetry semantic conventions, vendored at the version SignalDB itself emits (`1.43.0`)   |
| `signaldb`      | bundled | SignalDB's own `signaldb.*` conventions (its self-monitoring telemetry)                            |
| _anything else_ | custom  | Registries **you** upload for your tenant, in the same OTel Weaver model, versioned `name@version` |

Bundled registries are visible to every tenant and read-only. Custom registries
are private to the tenant that owns them. Registries are **namespaced**, so a
custom registry may describe a key that `otel` also describes — both
definitions coexist; lookups return every one, ordered by precedence:

```
your custom registries (namespace A→Z, newest version first) → signaldb → otel
```

The first hit is the _primary_ definition; the rest are alternatives and are
never hidden.

## Prerequisites

- An API key (or a signed-in session). Reading the registry needs the
  `schema:read` scope; creating, replacing, validating, or deleting custom
  registries needs `schema:write` (sessions: any tenant role reads, tenant
  admins write). See [Authentication](authentication.md#api-key-scopes) for
  the full scope vocabulary (which also includes the unrelated
  `tenant:manage` management scope).
- For custom registries: a registry document in the
  [OpenTelemetry Weaver semantic-convention model](https://github.com/open-telemetry/weaver)
  — the same YAML you would run `weaver registry check` on.

## Look up what a key means

```bash
# HTTP
curl -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme" \
  http://localhost:3000/api/v1/schema/attributes/k8s.pod.uid

# CLI
signaldb schema attribute get k8s.pod.uid
signaldb schema entity get k8s.pod
signaldb schema metric get k8s.pod.cpu.time
signaldb schema attribute search k8s.pod. --limit 20
```

The response lists every visible definition:

```json
{
  "key": "k8s.pod.uid",
  "primary": {
    "namespace": "otel",
    "version": "1.43.0",
    "source": "bundled",
    "key": "k8s.pod.uid",
    "type": "string",
    "stability": "stable",
    "group_display_name": "Kubernetes Attributes",
    "brief": "The UID of the Pod.",
    "examples": ["275ecb36-5aa8-4c2a-9c47-d8bb681b9aff"],
    "entity_roles": [
      { "namespace": "otel", "entity": "k8s.pod", "role": "identifying" }
    ]
  },
  "hits": ["…the primary, then alternatives…"]
}
```

Deprecated keys carry their replacement (`"deprecated": {"reason": "renamed",
"renamed_to": "http.response.status_code"}`). Entity lookups list identifying
and descriptive attributes, the metrics associated with the entity, and any
custom entities that extend it; metric lookups include instrument, unit, and
`entity_associations`. An unknown name returns an empty result, not an error.

Prefix search (`GET /api/v1/schema/attributes?prefix=http.re&limit=20`, also
`/entities` and `/metrics`) powers autocomplete; `?keys=a,b,c` resolves several
attribute keys in one call.

The MCP server exposes the same lookups as tools (`resolve_attribute`,
`resolve_entity`, `resolve_metric`, `search_schema`, `list_schema_registries`,
`get_schema_registry`), so an AI agent can learn what a key means before
building a query. `validate_schema_registry` checks a document without
storing it, mirroring `signaldb-cli admin schema validate` — see
[the MCP tool catalogue](mcp.md#what-it-exposes) for the full list. Like
every MCP tool call, these are subject to the server's total per-call
deadline — see [Running it](mcp.md#running-it).

## Add your own conventions

Write a registry document. It is a Weaver semantic-convention file with the
manifest fields at the top; a minimal one:

```yaml
name: acme # namespace — anything but otel/signaldb
version: 1.0.0
schema_url: https://acme.example/schemas/1.0.0
dependencies:
  - name: otel # lets you `ref` upstream attributes; default when omitted
groups:
  - id: registry.acme.order
    type: attribute_group
    display_name: Acme Order Attributes
    brief: Attributes describing an Acme order.
    attributes:
      - id: acme.order.id
        type: string
        stability: development
        brief: Internal order identifier (see the order-service runbook).
        examples: ["ord_8f21a"]
  - id: entity.acme.order
    type: entity
    name: acme.order
    stability: development
    brief: A customer order flowing through Acme's checkout.
    attributes:
      - ref: acme.order.id
        role: identifying
  - id: metric.acme.checkout.latency
    type: metric
    metric_name: acme.checkout.latency
    instrument: histogram
    unit: "s"
    stability: development
    brief: End-to-end checkout latency per order.
    entity_associations: [acme.order]
```

Validate, then create:

```bash
signaldb admin schema validate --file acme.yaml
signaldb admin schema create --file acme.yaml
# later
signaldb admin schema replace acme 1.0.0 --file acme.yaml
signaldb admin schema delete acme 1.0.0
```

or over HTTP (`Content-Type: application/yaml` for YAML, `application/json`
for JSON):

```bash
curl -X POST -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme" \
  -H "Content-Type: application/yaml" --data-binary @acme.yaml \
  http://localhost:3000/api/v1/schema/registries:validate   # nothing stored
curl -X POST … http://localhost:3000/api/v1/schema/registries            # 201
curl -X PUT  … http://localhost:3000/api/v1/schema/registries/acme/1.0.0 # replace
curl -X DELETE … http://localhost:3000/api/v1/schema/registries/acme/1.0.0
```

Validation enforces: unique group and attribute ids; every `ref`/`extends`
resolves in your document or a dependency; known attribute types
(`string`, `int`, `double`, `boolean`, arrays, `template[...]`, or an enum);
entity attribute roles `identifying`/`descriptive`; metrics carry
`metric_name`, `instrument`, `unit`; every `entity_associations` target is a
known entity; and an entity that `extends` another may add descriptive
attributes but never new identifying ones. Errors name the offending path
(`groups[2].attributes[0].ref: unresolved ref …`). Replace is all-or-nothing —
an invalid document leaves the previous one served.

Registries are documents: replacing uploads the whole file (a
`weaver`-managed repo can push its files unchanged). Namespaces `otel` and
`signaldb` are reserved. Group types other than `attribute_group`, `entity`,
and `metric` are stored but not resolved.

## Where the registry shows up

- The Explore UI resolves attribute keys in span/log detail panels, field
  sidebars, filter autocomplete, and facet headers to their title and
  description, and hosts the **Schema → Conventions** hub for browsing and
  managing registries ([Explore UI](explore-ui.md)).
- The MCP tools above.
- `GET /api/v1/schema/registries` lists everything visible to the tenant with
  attribute/entity/metric counts; `GET …/registries/{ns}/{version}` returns
  the document verbatim.

## Troubleshooting

- **`403 missing schema:read scope`** — the key carries explicit scopes
  without `schema:read`; create a key with it (or use a session).
- **`409 … is bundled and read-only`** — you tried to mutate `otel` or
  `signaldb`; upload a custom registry that `ref`s or `extends` them instead.
- **`422` with `dependencies[0]: unknown dependency namespace`** — the
  document names a dependency you have not uploaded; only `otel`, `signaldb`,
  and your own custom registries can be dependencies.
- **My tenant's definition should win but `otel` is primary** — the key is
  spelled differently (dotted OTel keys, e.g. `k8s.pod.uid`, not the
  underscore form Loki labels use), or the custom registry belongs to another
  tenant.
