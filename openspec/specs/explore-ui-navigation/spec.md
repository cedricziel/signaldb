# explore-ui-navigation Specification

## Purpose

Defines how the explore UI's client-side URLs map to on-screen views, so
every reachable screen — a signal view, the tenant management panel, the
OAuth consent screen — has a distinct, bookmarkable, shareable address and
participates correctly in browser back/forward history.
## Requirements
### Requirement: Signal selection via URL path

The explore UI SHALL expose each signal view at its own path — `/logs`,
`/traces`, `/metrics`, `/profiles`, `/query` — rather than behind a query
parameter on a single path. Selecting a different signal (e.g. clicking a
signal tab) SHALL navigate to that signal's path.

#### Scenario: Switching signal tabs updates the path

- **WHEN** a user on `/logs` clicks the "Traces" tab
- **THEN** the browser URL path becomes `/traces`

#### Scenario: Navigating directly to a signal path renders that view

- **WHEN** a user opens `/metrics` directly (fresh load or external link)
- **THEN** the metrics view renders with the "Metrics" tab selected

### Requirement: Unrecognized paths resolve to the logs view

A path whose signal segment is not one of the known signals SHALL redirect
to `/logs`, preserving any query string from the original URL.

#### Scenario: Unknown path redirects preserving query params

- **WHEN** a user opens `/bogus?range=15m`
- **THEN** the browser URL becomes `/logs?range=15m` and the logs view renders

### Requirement: Root path redirects to the logs view

Navigating to the site root SHALL redirect to `/logs`.

#### Scenario: Root redirects to logs

- **WHEN** a user opens `/`
- **THEN** the browser URL becomes `/logs` and the logs view renders

### Requirement: Tenant management is reachable via a dedicated URL

The tenant/API-key management panel SHALL be reachable at its own URL,
`/manage`, rather than as component state with no URL representation. Only
users who are a tenant admin or instance admin SHALL see the panel;
everyone else navigating to `/manage` SHALL be redirected to `/logs`.
Because it is a real route, navigating to it creates browser history, so
using the browser back button after opening it SHALL return to the
previous view instead of leaving the panel open.

#### Scenario: Admin opens /manage directly

- **WHEN** a tenant admin or instance admin navigates to `/manage`
- **THEN** the management panel renders

#### Scenario: Non-admin is redirected away from /manage

- **WHEN** a user who is neither a tenant admin nor an instance admin
  navigates to `/manage`
- **THEN** the browser URL becomes `/logs` and the management panel does not
  render

#### Scenario: Back button closes the management panel

- **WHEN** a user opens `/manage` from a signal view and then uses the
  browser back button
- **THEN** the browser returns to the signal view they came from and the
  management panel is no longer shown

### Requirement: OAuth consent screen is a standalone route

`/oauth/consent` SHALL render the OAuth connector consent screen on its own,
outside the explore shell (no top bar, no signal tabs), independent of any
explore-view query state.

#### Scenario: Consent screen renders standalone

- **WHEN** a user is redirected to `/oauth/consent` with valid authorization
  parameters
- **THEN** the consent screen renders without the explore shell's top bar or
  signal tabs

### Requirement: Non-signal state stays in the query string

Time range, filters, search text, live-tail mode, trace/group selection,
grouping dimension, grouping grain, PromQL expression, profile type/service
selectors, and tenant/dataset context SHALL remain represented as URL query
parameters, independent of which signal path is active, so a view (including a
specific trace or a specific PromQL query) remains bookmarkable and shareable.

#### Scenario: Query parameters survive a signal switch

- **WHEN** a user on `/logs?tenant=acme&dataset=prod` switches to the
  traces signal
- **THEN** the resulting URL is `/traces?tenant=acme&dataset=prod`

#### Scenario: A shared link reproduces the grouping grain

- **WHEN** a user shares a traces view whose group table counts spans rather
  than traces
- **THEN** opening that link presents the table at the same grain

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
