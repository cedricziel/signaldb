## ADDED Requirements

### Requirement: Real users page is reachable from the sidebar

The sidebar's Monitor group SHALL list "Real users" after Catalog, linking
to `/rum` with the explore context (tenant, dataset, range) carried over.
Any `/rum/...` path SHALL highlight it and title the breadcrumb
"Monitor / Real users". An unknown tab segment SHALL resolve to
`/rum/overview`, preserving the query string.

#### Scenario: Sidebar link

- **WHEN** a user on `/logs?tenant=acme&dataset=prod` clicks "Real users"
- **THEN** the browser navigates to `/rum?tenant=acme&dataset=prod` (plus the
  current range) and the entry is highlighted

#### Scenario: Unknown tab

- **WHEN** a user opens `/rum/nope?app=storefront-web`
- **THEN** the URL becomes `/rum/overview?app=storefront-web`
