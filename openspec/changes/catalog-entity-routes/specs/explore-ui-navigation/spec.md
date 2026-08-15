## ADDED Requirements

### Requirement: Catalog entity selection via URL path

The catalog SHALL address the selected entity type and any drilled-into entity
in the URL path, not in query parameters: `/catalog/:entity` shows the list for
entity type `:entity` (an entity type id such as `service`, `database`, `host`,
`k8s_pod`), `/catalog/:entity/:primary` shows that entity's detail, and
`/catalog/:entity/:primary/:secondary` the breakdown row drilled into within
it. `:primary` and `:secondary` SHALL encode their identity values as
comma-separated, percent-encoded segments (a value containing `,` or `/` is
percent-encoded, so the split is unambiguous), and a not-set identity value
SHALL round-trip. `/catalog` with no further segment SHALL show the default
entity type's list. Time range and tenant/dataset context SHALL remain query
parameters as on every other view.

#### Scenario: Drilling into an entity navigates to its route

- **WHEN** a user on `/catalog/service?tenant=acme` opens the entity whose
  `service.name` is `checkout` and `service.namespace` is `shop`
- **THEN** the URL becomes `/catalog/service/checkout,shop?tenant=acme` and
  the entity detail renders

#### Scenario: An entity route is directly addressable

- **WHEN** a user opens `/catalog/host/db-01?tenant=acme` directly
- **THEN** the catalog renders the `host` entity type with `db-01`'s detail
  view, and the browser back button returns to the previous view

#### Scenario: Identity values with reserved characters round-trip

- **WHEN** an entity's identity value is `a/b,c`
- **THEN** its route segment is `a%2Fb%2Cc` and opening that URL selects the
  same entity

#### Scenario: Legacy query parameters are not honoured

- **WHEN** a user opens `/catalog?entity=service&primary=x`
- **THEN** the default entity list renders (the query parameters are ignored)
