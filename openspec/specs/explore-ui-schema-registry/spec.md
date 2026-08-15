# explore-ui-schema-registry Specification

## Purpose

The Explore UI's `/schema` Conventions hub: where tenant users inspect the
semantic-convention registries visible to them (bundled and custom) and tenant
admins manage custom registries — upload or edit a Weaver-format document,
validate it, replace or delete it.

## Requirements

### Requirement: The schema hub lists every visible registry

The `/schema` route SHALL present a Conventions tab, reachable by every
authenticated tenant user, that lists all registries visible to the active
tenant with namespace, version, source (`bundled` / `custom` / `remote`),
counts of attributes, entities, and metrics, and last-updated time. Bundled
registries SHALL be marked read-only. The existing storage schema explorer SHALL
remain available under a Storage tab of the same route, visible only to users
who could see it before.

#### Scenario: Fresh tenant sees the bundled registries

- **WHEN** a user of a tenant with no custom registries opens `/schema`
- **THEN** the Conventions tab lists `otel` and `signaldb` with `bundled` source,
  their versions, and non-zero attribute counts, both marked read-only

#### Scenario: Storage explorer stays where it was

- **WHEN** an instance admin opens `/schema` and switches to the Storage tab
- **THEN** the storage schema explorer renders as before; a non-instance-admin
  user does not see the Storage tab

### Requirement: A registry can be browsed and searched

Opening a registry SHALL show a browser with a search box and three sections —
attributes, entities, metrics — filtered live by prefix or substring. Selecting
an item SHALL show its full definition: for an attribute the key, type, group
title, brief, note, examples, stability, requirement information, deprecation
with replacement, and the entities in which it plays a role; for an entity its
brief, identifying and descriptive attributes with roles and requirement level,
the metrics associated with it, and any entities that extend it; for a metric its
brief, instrument, unit, stability, declared attributes, and associated
entities. Every definition SHALL show its namespace and version and, when the
same key/name is defined in other visible registries, link to those alternatives.

#### Scenario: Searching a bundled registry

- **WHEN** a user opens `otel@1.43.0` and types `k8s.pod`
- **THEN** the attributes section lists the `k8s.pod.*` attributes, the entities
  section lists `k8s.pod`, and the metrics section lists the `k8s.pod.*` metrics

#### Scenario: Entity page shows roles and associations

- **WHEN** a user opens the entity `k8s.pod`
- **THEN** the page shows `k8s.pod.uid` as identifying, `k8s.pod.name` as
  descriptive, and lists metrics such as `k8s.pod.cpu.time` as associated

#### Scenario: Alternatives are linked

- **WHEN** a user views `otel/service.name` while the tenant's custom registry
  also defines `service.name`
- **THEN** the definition links to `acme/service.name` and marks which one is
  primary under the tenant's precedence

### Requirement: Global lookup across registries

The hub SHALL offer a lookup box that resolves an attribute key, entity name, or
metric name across all visible registries and shows the precedence-ordered hits
with their namespace, version, source, and brief, the first marked primary.

#### Scenario: Lookup shows primary and alternatives

- **WHEN** a user of a tenant whose custom registry defines `service.name` looks
  up `service.name`
- **THEN** the custom hit is shown first as primary and the `otel` hit below as
  an alternative, each with its brief

### Requirement: Tenant admins manage custom registries in the hub

For a tenant admin the Conventions tab SHALL offer: creating a registry by
uploading a Weaver-format YAML or JSON file or by pasting/editing source; a
Validate action that reports the server's validation result (per-path errors,
resulting attribute/entity/metric counts) without storing anything; Save that
creates or replaces the registry at the document's `namespace@version` (with a
choice to save under a new version); and Delete with confirmation. Non-admin
users SHALL see the management actions disabled or hidden and bundled registries
SHALL never expose them.

#### Scenario: Upload creates a registry

- **WHEN** a tenant admin uploads a valid Weaver-format YAML file with
  `name: acme` and `version: 1.0.0`
- **THEN** after Save the registry list shows `acme@1.0.0` as `custom` and its
  definitions are browsable and resolvable

#### Scenario: Validation errors are shown at their path

- **WHEN** a tenant admin edits a document so that an attribute `ref` is
  unresolvable and clicks Validate
- **THEN** the editor shows the error with its path (group and attribute) and the
  message, and Save is blocked until validation passes

#### Scenario: Replace shows a diff before saving

- **WHEN** a tenant admin edits an existing custom registry and validates
- **THEN** the panel summarizes added, changed, and removed definitions relative
  to the stored document before Save

#### Scenario: Non-admin cannot mutate

- **WHEN** a non-admin tenant user opens a custom registry
- **THEN** Edit, Replace, and Delete are unavailable and any attempt is
  rejected by the API

#### Scenario: Bundled registries are read-only in the UI

- **WHEN** any user opens `otel@1.43.0`
- **THEN** no Edit, Replace, or Delete action is offered

### Requirement: Registry pages are addressable

Registry list, a registry's browser, and a definition (attribute, entity,
metric) SHALL each have a URL under `/schema/…` so they can be linked from
labels, tooltips, MCP results, and shared, and the browser back button SHALL
return to the previous view.

#### Scenario: Deep link to a definition

- **WHEN** a user opens `/schema/conventions/otel/1.43.0/entities/k8s.pod`
- **THEN** the entity page for `k8s.pod` in `otel@1.43.0` renders directly

#### Scenario: Tooltip links into the hub

- **WHEN** a user clicks the definition link in an attribute tooltip in a span
  detail panel
- **THEN** the hub opens on that attribute's definition page
