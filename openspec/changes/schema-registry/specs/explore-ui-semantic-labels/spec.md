## Purpose

Wherever the Explore UI shows an attribute key as a label, it resolves the key
through the schema registry so users see what the key means — its semantic title
and description — instead of a bare string, with graceful fallback for keys no
registry knows.

## ADDED Requirements

### Requirement: Attribute labels resolve to semantic title and description

Every UI element that renders an attribute wire key as a label — the logs field
sidebar, filter chips and their autocomplete, span and log detail attribute
tables, and trace facet headers — SHALL resolve the key through the schema
registry for the active tenant and present the resolved semantic title (the
owning group's display name, e.g. "Kubernetes Attributes") and description (the
attribute brief) alongside the key. The raw key SHALL remain visible and
copyable; the semantics are additive (secondary text or tooltip), never a
replacement of the key.

#### Scenario: Registered key shows its meaning

- **WHEN** a span detail panel lists the resource attribute `k8s.pod.uid`
- **THEN** the row shows the key `k8s.pod.uid`, its description "The UID of the
  Pod.", and its title/group "Kubernetes Attributes", tagged with the defining
  namespace (`otel`)

#### Scenario: Unregistered key falls back to the raw key

- **WHEN** a log detail panel lists the attribute `app.order.id` and no visible
  registry defines it
- **THEN** the row shows `app.order.id` with no description and no error, and the
  panel renders no differently from today for that row

#### Scenario: Tenant definition wins in the UI

- **WHEN** the tenant's custom registry defines `service.name` and the UI renders
  that key
- **THEN** the description shown is the tenant's, tagged with the custom
  namespace, and the upstream `otel` description is reachable as an alternative

#### Scenario: Deprecated key is flagged with its replacement

- **WHEN** a rendered key is deprecated in the resolved registry with a rename
- **THEN** the label carries a deprecation marker and names the replacement key

### Requirement: Autocomplete surfaces descriptions

Attribute-key autocomplete in filter builders SHALL query the registry's prefix
search and show each suggestion's key with its primary description, merged with
keys observed in the tenant's data so that observed-but-unregistered keys remain
suggestible.

#### Scenario: Suggestions carry meaning

- **WHEN** a user types `http.re` in a filter chip's key input
- **THEN** suggestions include `http.request.method` and
  `http.response.status_code`, each with its brief, and any observed key with
  that prefix that the registry does not know appears without a brief

### Requirement: Resolution never blocks rendering

Registry resolution SHALL be asynchronous and cached per session; attribute rows
and labels SHALL render immediately with the raw key and be enriched when the
resolution arrives. A failing or slow registry endpoint SHALL degrade to
raw-key rendering without surfacing an error in the affected panel.

#### Scenario: Registry unavailable

- **WHEN** the schema endpoint returns an error while a span detail panel is open
- **THEN** all attribute rows render with their raw keys and the panel shows no
  error state
