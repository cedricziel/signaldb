---
audience: user
type: how-to
status: living
sources:
  - src/router/src/endpoints/schema.rs
  - src/common/src/schema_registry/**
  - src/common/src/schema/type_authority.rs
  - src/common/src/schema/type_authority/**
  - src/schema-model/src/**
  - src/signaldb-cli/src/commands/**
  - src/mcp-server/src/server.rs
  - otel/registry-genai/**
---

# Schema registry — semantic conventions and custom registries

SignalDB knows what your telemetry _means_, not just what it contains. The
**schema registry** holds semantic-convention registries — attribute keys with
their descriptions and types, entity types (`service`, `k8s.pod`, `host`, …)
with the attributes that identify them, and metric definitions with their
instrument, unit, and the entities they describe — and resolves any attribute
key, entity type, or metric name to its definitions for your tenant.

Four kinds of registry exist:

| Namespace       | Source  | What it is                                                                                         |
| --------------- | ------- | -------------------------------------------------------------------------------------------------- |
| `otel`          | bundled | The OpenTelemetry semantic conventions, vendored at the version SignalDB itself emits (`1.43.0`)   |
| `otel-genai`    | bundled | The OpenTelemetry GenAI conventions (`gen_ai.*`, `mcp.*`), vendored at a pinned upstream commit    |
| `signaldb`      | bundled | SignalDB's own conventions: its `signaldb.*` self-monitoring telemetry and the `gen_ai.agent` entity |
| _anything else_ | custom  | Registries **you** upload for your tenant, in the same OTel Weaver model, versioned `name@version` |

Bundled registries are visible to every tenant and read-only. Custom registries
are private to the tenant that owns them. Registries are **namespaced**, so a
custom registry may describe a key that `otel` also describes — both
definitions coexist; lookups return every one, ordered by precedence:

```
your custom registries (namespace A→Z, newest version first) → signaldb → otel-genai → otel
```

The first hit is the _primary_ definition; the rest are alternatives and are
never hidden.

OpenTelemetry moved the GenAI and MCP conventions out of core semconv into
[their own repository](https://github.com/open-telemetry/semantic-conventions-genai);
the `otel` registry keeps only deprecated copies. That is why `otel-genai`
comes before `otel`: resolving `gen_ai.agent.id` returns the current
`otel-genai` definition first and the deprecated `otel` one as an alternative.
Upstream has no GenAI release yet, so its version is the vendored commit.

The `signaldb` registry's source is
[`otel/registry/`](https://github.com/cedricziel/signaldb/tree/main/otel/registry)
in the repository, and its version is the SignalDB release. SignalDB's own
telemetry declares it on every instrumentation scope as
`schema_url: https://cedricziel.github.io/signaldb/schemas/<version>`; the
[telemetry-schema files](https://cedricziel.github.io/signaldb/schemas/) at
those URLs are published with this site, one per release. See
[Self-Monitoring Trace Model](../operations/self-monitoring-traces.md) for
what that telemetry contains.

## Prerequisites

- An API key (or a signed-in session — established with a password or, where
  the operator configured it, via [SSO](authentication.md#signing-in-with-sso-oidc);
  either resolves to the same session). Reading the registry needs the
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
signaldb-cli schema attribute get k8s.pod.uid
signaldb-cli schema entity get k8s.pod
signaldb-cli schema metric get k8s.pod.cpu.time
signaldb-cli schema entity get gen_ai.agent
signaldb-cli schema attribute search k8s.pod. --limit 20
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
  "hits": ["…the primary, then alternatives…"],
  "canonical_types": [
    {
      "dataset": "production",
      "signal": "traces",
      "level": "resource",
      "canonical_type": "string",
      "source": "observed",
      "hint_schema_url": null,
      "off_type_count": 0
    }
  ]
}
```

`canonical_types` appears only once data for the key has been stored; see
[Canonical types](#canonical-types).

Deprecated keys carry their replacement (`"deprecated": {"reason": "renamed",
"renamed_to": "http.response.status_code"}`). Entity lookups list identifying
and descriptive attributes, the metrics associated with the entity, and any
custom entities that extend it; metric lookups include instrument, unit, and
`entity_associations`. An unknown name returns an empty result, not an error.

AI agents are an entity too. `gen_ai.agent` (from the `signaldb` registry) is
identified by `gen_ai.agent.id` — the provider-assigned, stable id of a hosted
agent such as an AWS Bedrock agent ARN, not an in-memory instance id — and
described by `gen_ai.agent.name`, `gen_ai.agent.description`, and
`gen_ai.agent.version`. Upstream defines these only as span attributes (on
`create_agent` and `invoke_agent` spans), so SignalDB supplies the entity; a
custom registry can `extends: entity.gen_ai.agent` to add its own descriptive
attributes.

Prefix search (`GET /api/v1/schema/attributes?prefix=http.re&limit=20`, also
`/entities` and `/metrics`) powers autocomplete; `?keys=a,b,c` resolves several
attribute keys in one call, and the same parameter on `/metrics` batch-resolves
an exact metric name set.

The MCP server exposes the same lookups as tools (`resolve_attribute`,
`resolve_entity`, `resolve_metric`, `search_schema`, `list_schema_registries`,
`get_schema_registry`), so an AI agent can learn what a key means before
building a query. `validate_schema_registry` checks a document without
storing it, mirroring `signaldb-cli admin schema validate` — see
[the MCP tool catalogue](mcp.md#what-it-exposes) for the full list. Like
every MCP tool call, these are subject to the server's total per-call
deadline — see [Running it](mcp.md#running-it).

## Canonical types

A registry entry's `type` says what the convention _declares_. It is a hint.
The type SignalDB actually stores and filters a key by is the **canonical
type**: one of `string`, `int64`, `float64` or `bool`, scoped per tenant,
dataset, signal (`traces`, `logs`, `metrics`, `profiles`), attribute level
(`resource`, `scope`, `record`) and key. The same key can therefore have
different canonical types in different datasets or at different levels, and one
tenant's data never affects another's.

The canonical type comes from, in order: a config pin, then the registry's
semconv hint, then the type of the first value SignalDB stored for the key.

- **Pin**: the `[[schema.attribute_types]]` entry below.
- **Semconv hint**: applies only when the sender's `schema_url` names a registry
  visible to the tenant. A resource-level key uses the resource's `schema_url`; a
  scope- or record-level key prefers the scope's `schema_url` and falls back to
  the resource's. A URL starting `https://opentelemetry.io/schemas/` selects the
  bundled `otel` registry; any other URL matches a registry's exact
  `schema_url`. Only scalar declared types count: `string`, `int`, `double`,
  `boolean`, and an enum whose members are all strings or all integers.
- **First observed**: the first scalar value stored. Arrays, key-value lists and
  bytes never set a type.

Once set, the type does not change because later data disagrees. What happens
to a value that does not fit depends on its shape:

- A **scalar of another type** is kept exactly as sent but cannot be filtered as
  a typed value. It increments the key's `off_type_count`, and the sender's OTLP
  response carries a `partial_success` warning naming the keys.
- An **array, key-value list or bytes value** is kept as sent and can be read
  back, but cannot be filtered. It is not counted and does not trigger the
  warning.

Because a declared type wins over what you send, an `int` convention with a
sender that emits strings makes every one of those values off-type, so pin the
type you actually send if it differs.

The lookup shows the committed types and how many values arrived off-type:

```bash
curl -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme" \
  "http://localhost:3000/api/v1/schema/attributes?keys=retry.count,http.route"
```

Each entry in `canonical_types` has `dataset`, `signal`, `level`,
`canonical_type`, `source` (`config`, `semconv` or `observed`),
`hint_schema_url` and `off_type_count`. A key restricted to some datasets only
sees those datasets' entries.

**Pin a type** in the server configuration (an operator setting, not an API):

```toml
[[schema.attribute_types]]
signal = "logs"      # logs | traces | metrics | profiles
level = "record"     # resource | scope | record
key = "retry.count"
type = "int64"       # string | int64 | float64 | bool
dataset = "prod"     # optional; omitted applies to every dataset of the tenant
```

A dataset entry wins over a tenant-wide one. Pinning retypes a field that data
had already typed, for values written from then on; stored values are not
rewritten. A changed pin applies after the writer restarts. A tenant with its
own schema block (`[tenants.tenants.<id>.schema]`) uses only that block's pins,
so repeat any global pin it needs. `signaldb.dist.toml` documents the block.

## Add your own conventions

Write a registry document. It is a Weaver semantic-convention file with the
manifest fields at the top; a minimal one:

```yaml
name: acme # namespace — anything but otel/otel-genai/signaldb
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

Weaver's newer `file_format: definition/2` layout works too: keep the manifest
fields at the top and list definitions under `attributes`, `attribute_groups`,
`entities`, `metrics`, `spans`, `events`, `span_refinements` and
`metric_refinements` instead of `groups`. The same registry as above:

```yaml
file_format: definition/2
name: acme
version: 1.0.0
schema_url: https://acme.example/schemas/1.0.0
dependencies:
  - name: otel
attributes:
  - key: acme.order.id
    type: string
    stability: development
    brief: Internal order identifier (see the order-service runbook).
    examples: ["ord_8f21a"]
entities:
  - name: acme.order
    stability: development
    brief: A customer order flowing through Acme's checkout.
    attributes:
      - ref: acme.order.id
        role: identifying
metrics:
  - name: acme.checkout.latency
    instrument: histogram
    unit: "s"
    stability: development
    brief: End-to-end checkout latency per order.
    entity_associations: [acme.order]
```

SignalDB converts a `definition/2` upload to `groups` when it stores it, and
reading the registry back returns the `groups` form: metrics become
`metric.<name>`, entities `entity.<name>`, and the top-level `attributes` one
group named `registry.<name>`. The attributes, entities and metrics resolve the
same as in the `groups` version; only the group an attribute is listed under
differs. Any other `file_format` value, or `ref_group` references that loop
back on themselves, are rejected with `422` and nothing is stored.

Validate, then create:

```bash
signaldb-cli admin schema validate --file acme.yaml
signaldb-cli admin schema create --file acme.yaml
# later
signaldb-cli admin schema replace acme 1.0.0 --file acme.yaml
signaldb-cli admin schema delete acme 1.0.0
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
`weaver`-managed repo can push its files unchanged). Namespaces `otel`,
`otel-genai`, and `signaldb` are reserved. Group types other than `attribute_group`, `entity`,
and `metric` are stored but not resolved.

## Where the registry shows up

- The Explore UI resolves attribute keys in span/log detail panels, field
  sidebars, filter autocomplete, and facet headers to their title and
  description, and hosts the **Schema → Conventions** hub for browsing and
  managing registries ([Explore UI](explore-ui.md)).
- The MCP tools above.
- `GET /api/v1/schema/registries` lists everything visible to the tenant with
  attribute/entity/metric counts; `GET …/registries/{ns}/{version}` returns
  the stored document (a `definition/2` upload comes back in the `groups`
  form).

## Troubleshooting

- **`403 missing schema:read scope`** — the key carries explicit scopes
  without `schema:read`; create a key with it (or use a session).
- **`409 … is bundled and read-only`** — you tried to mutate `otel`,
  `otel-genai`, or `signaldb`; upload a custom registry that `ref`s or `extends` them instead.
- **`422 … unsupported file_format`** — the document declares a
  `file_format` other than `definition/2`. Drop the key for the `groups`
  layout, or convert the file to `definition/2`.
- **`422` with `dependencies[0]: unknown dependency namespace`** — the
  document names a dependency you have not uploaded; only `otel`, `otel-genai`,
  `signaldb`, and your own custom registries can be dependencies.
- **My tenant's definition should win but `otel` is primary** — the key is
  spelled differently (dotted OTel keys, e.g. `k8s.pod.uid`, not the
  underscore form Loki labels use), or the custom registry belongs to another
  tenant.
