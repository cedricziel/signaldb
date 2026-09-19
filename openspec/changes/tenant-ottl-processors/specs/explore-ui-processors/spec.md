## ADDED Requirements

### Requirement: Processors page lists and manages the tenant's processors

The Explore UI SHALL provide a `/processors` route, reachable from the user
menu, listing the current tenant's processors with name, signal, dataset,
enabled state, priority, status (`ok`/`invalid`), and last update. Tenant
admins SHALL be able to create, edit, enable/disable, and delete processors
from this page; other members see it read-only. All requests SHALL go through
the generated TypeScript client.

#### Scenario: List renders

- **WHEN** a tenant user opens `/processors`
- **THEN** every processor of the tenant is listed with the fields above and
  a disabled one is visually distinct

### Requirement: Editor validates statements as they are written

The processor editor SHALL take name, description, signal, dataset (a picker
of the tenant's datasets plus "all datasets"), enabled, priority, error mode,
and statements (one per line). On blur or on an explicit "Validate" action it
SHALL call the validate endpoint and annotate each failing line with its
message and column; saving SHALL be disabled while errors are present. After a
successful save the editor SHALL show the "applies within N seconds" hint.

#### Scenario: Invalid line is annotated

- **WHEN** the user types `merge_maps(attributes, resource.attributes,
  "upsert")` on line 2
- **THEN** line 2 is marked with the server's error message and Save is
  disabled

### Requirement: Dry-run panel shows before and after

The editor SHALL offer a "Test" panel preloaded with a sample OTLP JSON
payload for the selected signal, editable by the user, that submits the
current (unsaved) processor to the test endpoint and renders a before/after
diff of the payload and the per-statement match and error counts.

#### Scenario: Diff after test

- **WHEN** the user tests `set(attributes["user.email"], "[redacted]")`
  against the default logs sample
- **THEN** the diff highlights the changed `user.email` value and statement 1
  shows one match
