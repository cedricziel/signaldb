# Spec Delta

## ADDED Requirements

### Requirement: Unreadable model files are rejected, not skipped

When SignalDB reads a registry model (a bundled model tree at build time or a
custom registry upload), every non-manifest model file SHALL be in a layout it
understands, either `groups` or `file_format: definition/2`. A file in any
other layout, including an unknown `file_format` value, SHALL fail the build or
the write with an error naming the file. It SHALL NOT contribute zero
definitions without an error.

#### Scenario: Unknown file format fails the build

- **WHEN** a vendored model tree contains a file declaring
  `file_format: definition/3`
- **THEN** the build fails with an error naming that file and the unsupported
  format

#### Scenario: Unknown file format is rejected on upload

- **WHEN** a tenant uploads a registry declaring an unsupported `file_format`
- **THEN** the write is rejected with a validation error naming the format and
  no registry is created or changed

## MODIFIED Requirements

### Requirement: Registries are identified by namespace and version

Every registry SHALL be identified by a `namespace` and a `version`
(`<namespace>@<version>`), carry a `source` of `bundled`, `custom`, or `remote`,
and a `schema_url`. Definitions inside a registry SHALL be addressed by their
namespaced id (`<namespace>/<definition-id>`, e.g. `otel/k8s.pod.uid`,
`otel/k8s.pod`, `otel/k8s.pod.cpu.time`), so two registries MAY define the same
wire key, entity name, or metric name without conflict. The namespaces `otel`,
`otel-genai` and `signaldb` are reserved for bundled registries in every tenant.

#### Scenario: Same wire key in two namespaces coexists

- **WHEN** a tenant's custom registry `acme@1.0.0` defines an attribute with id
  `service.name` while `otel@1.43.0` also defines `service.name`
- **THEN** both definitions exist as `acme/service.name` and `otel/service.name`
  and neither operation is rejected

#### Scenario: Reserved namespace cannot be claimed by a custom registry

- **WHEN** a client creates a custom registry whose manifest `name` is `otel`,
  `otel-genai` or `signaldb`
- **THEN** the request is rejected with a validation error naming the reserved
  namespace

### Requirement: Bundled OpenTelemetry and SignalDB registries are always present and read-only

SignalDB SHALL ship the OpenTelemetry semantic conventions as the bundled
registry `otel` at the semantic-conventions version pinned for self-monitoring
(`common::self_monitoring::SEMCONV_SCHEMA_URL`), the OpenTelemetry GenAI semantic
conventions as the bundled registry `otel-genai` at a pinned release whose declared
core semantic-conventions dependency equals that pin, and SignalDB's own conventions
(`otel/registry/`) as the bundled registry `signaldb`. Bundled registries SHALL be
visible to every tenant, SHALL be available without any tenant configuration, and
SHALL reject create, replace, and delete requests.

#### Scenario: OTel registry is listed for a fresh tenant

- **WHEN** a newly created tenant lists registries
- **THEN** the list contains `otel`, `otel-genai` and `signaldb` with `source: bundled`, their
  attribute/entity/metric counts, and the `otel` version equals the
  self-monitoring semantic-conventions pin

#### Scenario: Mutating a bundled registry is refused

- **WHEN** a client attempts to replace or delete `otel@<version>`,
  `otel-genai@<version>` or `signaldb@<version>`
- **THEN** the request is rejected with a client error stating the registry is
  bundled and read-only, and the registry is unchanged

#### Scenario: Vendored conventions match the pinned version

- **WHEN** the vendored OpenTelemetry semantic-conventions model differs from the
  self-monitoring pin
- **THEN** the build/CI check fails, so the two versions cannot drift

#### Scenario: GenAI conventions depend on the pinned core version

- **WHEN** the vendored GenAI conventions declare a core semantic-conventions
  dependency that differs from the self-monitoring pin
- **THEN** the build/CI check fails

#### Scenario: Moved GenAI attributes resolve to their current definition

- **WHEN** a tenant with no custom registries resolves `gen_ai.agent.id`
- **THEN** the primary hit is `otel-genai/gen_ai.agent.id` without a deprecation,
  and the deprecated `otel/gen_ai.agent.id` is returned as an alternative

### Requirement: Custom registries are tenant-scoped documents in the Weaver semantic-convention model

An authenticated tenant SHALL be able to create, read, replace, and delete
custom registries. A custom registry SHALL be submitted and returned as a
document in the OpenTelemetry Weaver semantic-convention model (a manifest with
`name`, `version`, `schema_url`, optional `dependencies`, and `groups`; JSON on
the wire, YAML accepted on upload). Uploads MAY instead use Weaver's
`file_format: definition/2` layout (top-level `attributes`, `attribute_groups`,
`entities`, `metrics`, `spans`, `events` and their refinements); such a document
SHALL resolve to the same definitions as its `groups` equivalent and SHALL be
returned in the `groups` form. Custom registries SHALL be visible only to
the tenant that owns them; the same namespace MAY exist in different tenants.
Replacing a registry SHALL be atomic: the previous version is served until the
new document is fully validated and stored.

#### Scenario: A Weaver-authored file is uploaded unchanged

- **WHEN** a tenant uploads a semantic-convention YAML file that passes
  `weaver registry check` against upstream semconv, containing `attribute_group`,
  `entity`, and `metric` groups
- **THEN** the registry is created under the manifest's `name` and `version` and
  every attribute, entity, and metric it declares is resolvable for that tenant

#### Scenario: Custom registries never cross tenants

- **WHEN** tenant A creates `infra@1.0.0` and tenant B lists registries or looks
  up a key defined only in A's `infra`
- **THEN** B sees neither the registry nor its definitions

#### Scenario: Replace is all-or-nothing

- **WHEN** a tenant replaces `acme@1.0.0` with a document that fails validation
- **THEN** the request is rejected and lookups continue to serve the previous
  `acme@1.0.0` content

#### Scenario: Deleting a custom registry removes its definitions

- **WHEN** a tenant deletes `acme@1.0.0`
- **THEN** subsequent lookups no longer return any `acme/*` definition and the
  registry is absent from the list

#### Scenario: A definition/2 file is uploaded

- **WHEN** a tenant uploads a registry in the `definition/2` layout declaring an
  attribute, an entity and a metric associated with that entity
- **THEN** the registry is created, all three definitions resolve for that
  tenant exactly as they would from the equivalent `groups` document, and reading
  the registry back returns the `groups` form

### Requirement: Definitions resolve by wire key, entity type, and metric name

SignalDB SHALL expose a resolved lookup for the calling tenant that accepts an
attribute wire key (`k8s.pod.uid`), an entity type name (`k8s.pod`), or a metric
name (`k8s.pod.cpu.time`) — bare, as it appears on the wire — and returns every
matching definition across the registries visible to that tenant. Each hit SHALL
be tagged with its `namespace`, `version`, and `source`. Hits SHALL be ordered by
precedence: the tenant's custom registries first, then `signaldb`, then
`otel-genai`, then `otel`;
the first hit is the primary definition and the rest are returned as
alternatives, never hidden. Lookups SHALL support prefix search over attribute
keys, entity names, and metric names for autocomplete.

#### Scenario: Attribute lookup returns semantics and provenance

- **WHEN** a tenant with no custom registries resolves `http.response.status_code`
- **THEN** the response contains one hit tagged `otel@<pinned>` with the
  attribute's brief, note, type, examples, stability, requirement information,
  and the entities (if any) in which it plays a role

#### Scenario: Tenant definition takes precedence but does not hide upstream

- **WHEN** a tenant whose `acme@1.0.0` defines `service.name` resolves
  `service.name`
- **THEN** the primary hit is `acme/service.name` and `otel/service.name` is
  returned as an alternative

#### Scenario: Deprecated attribute carries its replacement

- **WHEN** a client resolves an attribute that the registry marks deprecated with
  a rename (e.g. `http.status_code`)
- **THEN** the hit states it is deprecated and names the replacement key
  (`http.response.status_code`)

#### Scenario: Entity lookup lists roles and associated metrics

- **WHEN** a client resolves entity type `k8s.pod`
- **THEN** the response lists its identifying attributes and descriptive
  attributes with their roles, and the metric names whose definitions declare
  `entity_associations` including `k8s.pod`

#### Scenario: Metric lookup names its entities

- **WHEN** a client resolves metric name `k8s.pod.cpu.time`
- **THEN** the response contains its brief, instrument, unit, stability, declared
  attributes, and the entity types it is associated with

#### Scenario: Unknown key resolves to an empty result, not an error

- **WHEN** a client resolves a key no visible registry defines
- **THEN** the response is a successful, empty result

#### Scenario: Prefix search for autocomplete

- **WHEN** a client searches attributes with prefix `k8s.pod.`
- **THEN** every attribute key visible to the tenant that starts with that
  prefix is returned with its primary brief, bounded by a documented page size
